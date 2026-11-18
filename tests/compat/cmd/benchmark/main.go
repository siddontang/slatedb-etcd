// Measure the same gRPC workload against each service.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func mix(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func percentiles(samples []float64) map[string]float64 {
	sort.Float64s(samples)
	result := map[string]float64{}
	for name, fraction := range map[string]float64{"p50": .5, "p95": .95, "p99": .99, "p999": .999, "max": 1} {
		if len(samples) > 0 {
			result[name] = samples[int(float64(len(samples)-1)*fraction)]
		}
	}
	return result
}

func barrier(directory, notify, release string) error {
	if directory == "" {
		return nil
	}
	if err := os.WriteFile(filepath.Join(directory, notify), nil, 0600); err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Minute)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(directory, release)); err == nil {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return fmt.Errorf("controller did not release %s", notify)
}

func main() {
	endpoints := flag.String("endpoints", "", "Comma-separated client endpoints")
	requests := flag.Int("requests", 10000, "Measured operations")
	concurrency := flag.Int("concurrency", 8, "Concurrent requests")
	rate := flag.Int("rate", 0, "Target operations per second during measurement; zero runs at capacity")
	reads := flag.Int("read-percent", 50, "Linearizable read percentage")
	keys := flag.Int("keys", 1000, "Preloaded keys")
	pattern := flag.String("value-pattern", "repeat", "Value pattern: repeat, binary, or metadata")
	size := flag.Int("value-size", 256, "Value size in bytes")
	warmup := flag.Int("warmup", 100, "Unmeasured operations after preload")
	seed := flag.Uint64("seed", 42, "Deterministic workload seed")
	control := flag.String("control-dir", "", "Optional controller barrier directory")
	output := flag.String("output", "", "JSON result file")
	flag.Parse()
	if *endpoints == "" || *requests < 1 || *rate < 0 || *concurrency < 1 || *keys < 1 || *size < 1 || *warmup < 0 || *reads < 0 || *reads > 100 {
		fmt.Fprintln(os.Stderr, "invalid benchmark arguments")
		os.Exit(2)
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(*endpoints, ","),
		DialTimeout: 5 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	deadline := time.Now().Add(30 * time.Second)
	for _, endpoint := range client.Endpoints() {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			status, err := client.Status(ctx, endpoint)
			cancel()
			if err == nil && status.Leader != 0 {
				break
			}
			if time.Now().After(deadline) {
				panic("cluster readiness timed out")
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	values, err := makeValues(*pattern, *size, *keys+*requests+*warmup)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	for i := 0; i < *keys; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := client.Put(ctx, fmt.Sprintf("bench/%08d", i), values[i%len(values)])
		cancel()
		if err != nil {
			panic(err)
		}
	}
	isRead := func(i int) bool { return mix(uint64(i)^*seed)%100 < uint64(*reads) }
	operation := func(i int) error {
		keyIndex := int(mix(uint64(i)^*seed^0x51ed270b) % uint64(*keys))
		key := fmt.Sprintf("bench/%08d", keyIndex)
		value := values[keyIndex%len(values)]
		if *reads == 0 {
			value = values[(*keys+i)%len(values)]
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if isRead(i) {
			response, err := client.Get(ctx, key)
			if err == nil && (len(response.Kvs) != 1 || string(response.Kvs[0].Value) != value) {
				return fmt.Errorf("read returned an unexpected value")
			}
			return err
		}
		_, err := client.Put(ctx, key, value)
		return err
	}
	for i := 0; i < *warmup; i++ {
		if err := operation(*requests + i); err != nil {
			panic(err)
		}
	}
	durations := make([]float64, *requests)
	failures := make([]string, *requests)
	var next atomic.Int64
	var wg sync.WaitGroup
	var measurementStart time.Time
	start := make(chan struct{})
	for worker := 0; worker < *concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				i := int(next.Add(1) - 1)
				if i >= *requests {
					return
				}
				began := time.Now()
				if *rate > 0 {
					// Include time spent waiting for a free worker after the scheduled arrival.
					began = measurementStart.Add(time.Duration(float64(i) / float64(*rate) * float64(time.Second)))
					time.Sleep(time.Until(began))
				}
				err := operation(i)
				durations[i] = float64(time.Since(began).Nanoseconds()) / 1e6
				if err != nil {
					failures[i] = err.Error()
				}
			}
		}()
	}
	if err := barrier(*control, "ready", "start"); err != nil {
		panic(err)
	}
	measurementStart = time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(measurementStart).Seconds()
	if err := barrier(*control, "done", "finish"); err != nil {
		panic(err)
	}
	samples, getSamples, putSamples, errors := []float64{}, []float64{}, []float64{}, map[string]int{}
	readCount := 0
	for i, failure := range failures {
		if isRead(i) {
			readCount++
		}
		if failure != "" {
			errors[failure]++
		} else {
			samples = append(samples, durations[i])
			if isRead(i) {
				getSamples = append(getSamples, durations[i])
			} else {
				putSamples = append(putSamples, durations[i])
			}
		}
	}
	report := map[string]any{"passed": len(samples) == *requests, "requests": *requests,
		"successes": len(samples), "errors": errors, "concurrency": *concurrency,
		"read_percent": *reads, "read_requests": readCount, "write_requests": *requests - readCount,
		"keys": *keys, "value_bytes": *size, "value_pattern": *pattern, "warmup_operations": *warmup, "seed": *seed, "target_ops_per_second": *rate,
		"seconds": elapsed, "successful_ops_per_second": float64(len(samples)) / elapsed,
		"latency_ms": percentiles(samples), "read_latency_ms": percentiles(getSamples),
		"write_latency_ms": percentiles(putSamples),
		"model":            "bounded concurrency; seeded uniform keys and reads; paced latency starts at the scheduled arrival"}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		panic(err)
	}
	if *output != "" {
		if err := os.WriteFile(*output, data, 0600); err != nil {
			panic(err)
		}
	}
	fmt.Println(string(data))
	if len(samples) != *requests {
		os.Exit(1)
	}
}
