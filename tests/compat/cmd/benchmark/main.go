// Measure the same gRPC workload against either three-member service.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func main() {
	endpoints := flag.String("endpoints", "", "Comma-separated client endpoints")
	requests := flag.Int("requests", 10000, "Measured operations")
	concurrency := flag.Int("concurrency", 8, "Concurrent requests")
	reads := flag.Int("read-percent", 50, "Linearizable read percentage")
	keys := flag.Int("keys", 1000, "Preloaded keys")
	size := flag.Int("value-size", 256, "Value size in bytes")
	output := flag.String("output", "", "JSON result file")
	flag.Parse()
	if *endpoints == "" || *requests < 1 || *concurrency < 1 || *keys < 1 || *size < 1 || *reads < 0 || *reads > 100 {
		fmt.Fprintln(os.Stderr, "invalid benchmark arguments")
		os.Exit(2)
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: strings.Split(*endpoints, ","),
		DialTimeout: 5 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		panic(err)
	}
	defer client.Close()
	value := strings.Repeat("v", *size)
	for i := 0; i < *keys; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := client.Put(ctx, fmt.Sprintf("bench/%08d", i), value)
		cancel()
		if err != nil {
			panic(err)
		}
	}
	durations := make([]float64, *requests)
	failures := make([]string, *requests)
	var next atomic.Int64
	var wg sync.WaitGroup
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
				key := fmt.Sprintf("bench/%08d", i%*keys)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				began := time.Now()
				var err error
				if i%100 < *reads {
					var response *clientv3.GetResponse
					response, err = client.Get(ctx, key)
					if err == nil && (len(response.Kvs) != 1 || string(response.Kvs[0].Value) != value) {
						err = fmt.Errorf("read returned an unexpected value")
					}
				} else {
					_, err = client.Put(ctx, key, value)
				}
				durations[i] = float64(time.Since(began).Nanoseconds()) / 1e6
				cancel()
				if err != nil {
					failures[i] = err.Error()
				}
			}
		}()
	}
	began := time.Now()
	close(start)
	wg.Wait()
	elapsed := time.Since(began).Seconds()
	samples, errors := []float64{}, map[string]int{}
	for i, failure := range failures {
		if failure != "" {
			errors[failure]++
		} else {
			samples = append(samples, durations[i])
		}
	}
	sort.Float64s(samples)
	percentile := func(fraction float64) float64 {
		if len(samples) == 0 {
			return 0
		}
		index := int(float64(len(samples)-1) * fraction)
		return samples[index]
	}
	report := map[string]any{"passed": len(samples) == *requests, "requests": *requests,
		"successes": len(samples), "errors": errors, "concurrency": *concurrency,
		"read_percent": *reads, "keys": *keys, "value_bytes": *size,
		"seconds": elapsed, "successful_ops_per_second": float64(len(samples)) / elapsed,
		"latency_ms": map[string]float64{"p50": percentile(.5), "p95": percentile(.95),
			"p99": percentile(.99), "p999": percentile(.999), "max": percentile(1)},
		"model": "closed loop; warmup excluded; latency includes successful operations only"}
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
