//go:build robustness && (linux || darwin)

package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	recording "go.etcd.io/etcd/tests/v3/robustness/client"
	"go.etcd.io/etcd/tests/v3/robustness/identity"
	"go.etcd.io/etcd/tests/v3/robustness/model"
)

type jepsenEvent struct {
	Time                  int64
	Process               int
	Type, Function, Value string
}

func saveJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err == nil {
		err = os.WriteFile(path, append(data, '\n'), 0600)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestRobustness(t *testing.T) {
	seed := int64(1)
	if value := os.Getenv("SLATEDB_TEST_SEED"); value != "" {
		var err error
		seed, err = strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	c := newCluster(t)
	base := time.Now()
	ids := identity.NewIDProvider()
	const workers = 3
	recorders := make([]*recording.RecordingClient, workers)
	events := make([][]jepsenEvent, workers)
	appendEvents := make([][]jepsenEvent, workers)
	counts := make([]int, workers)
	for i := range recorders {
		var err error
		recorders[i], err = recording.NewRecordingClient([]string{c.addresses[i]}, ids, base)
		if err != nil {
			t.Fatal(err)
		}
		defer recorders[i].Close()
	}
	watchCtx, stopWatch := context.WithTimeout(context.Background(), 25*time.Second)
	defer stopWatch()
	watchResults := startFaultWatches(t, c, watchCtx)
	stop := make(chan struct{})
	var done sync.WaitGroup
	for i, recorder := range recorders {
		done.Add(1)
		go func(i int, recorder *recording.RecordingClient) {
			defer done.Done()
			rng := rand.New(rand.NewSource(seed + int64(i)*1009))
			process := i
			var leaseID int64
			leaseAttached := false
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
				value := fmt.Sprintf("worker-%d-op-%d", i, n)
				callTime := time.Since(base).Nanoseconds()
				function, input, output := "", "nil", "nil"
				var err error
				switch rng.Intn(8) {
				case 0:
					function, input = "write", strconv.Quote(value)
					_, err = recorder.Put(ctx, "register", value)
					output = input
				case 1:
					function = "read"
					resp, readErr := recorder.Get(ctx, "register")
					err = readErr
					if err == nil && len(resp.Kvs) != 0 {
						output = strconv.Quote(string(resp.Kvs[0].Value))
					}
				case 2:
					function = "write"
					_, err = recorder.Delete(ctx, "register")
				case 3:
					_, err = recorder.Txn(ctx).Then(clientv3.OpPut("pair/a", value),
						clientv3.OpPut("pair/b", value)).Commit()
				case 4:
					_, err = recorder.Range(ctx, "pair/", "pair0", 0, 0)
				case 5:
					resp, readErr := recorder.Get(ctx, "pair/a")
					err = readErr
					if err == nil {
						var revision int64
						if len(resp.Kvs) != 0 {
							revision = resp.Kvs[0].ModRevision
						}
						_, err = recorder.Txn(ctx).
							If(clientv3.Compare(clientv3.ModRevision("pair/a"), "=", revision)).
							Then(clientv3.OpPut("pair/a", value), clientv3.OpPut("pair/b", value)).Commit()
					}
				case 7:
					if leaseID == 0 {
						var lease *clientv3.LeaseGrantResponse
						lease, err = recorder.LeaseGrant(ctx, 300)
						if err == nil {
							leaseID = int64(lease.ID)
						}
					} else if !leaseAttached {
						_, err = recorder.PutWithLease(ctx, fmt.Sprintf("lease/%d", i), value, leaseID)
						if err == nil {
							leaseAttached = true
						}
					} else {
						_, err = recorder.LeaseRevoke(ctx, leaseID)
						leaseID, leaseAttached = 0, false
					}

				case 6:
					var invoke, result string
					var kind string
					invoke, result, kind, err = appendTransaction(ctx, recorder, n*workers+i+1)
					appendEvents[i] = append(appendEvents[i],
						jepsenEvent{callTime, process, "invoke", "txn", invoke},
						jepsenEvent{time.Since(base).Nanoseconds(), process, kind, "txn", result})
					if kind == "info" {
						process += workers
					}
				}
				cancel()
				if err == nil {
					counts[i]++
				}
				if function != "" {
					kind := "ok"
					if err != nil {
						kind = "info"
						if function == "read" {
							kind = "fail"
						}
					}
					events[i] = append(events[i],
						jepsenEvent{callTime, process, "invoke", function, input},
						jepsenEvent{time.Since(base).Nanoseconds(), process, kind, function, output})
					// An uncertain write can finish after this worker starts another operation.
					if kind == "info" {
						process += workers
					}
				}
				if err != nil {
					time.Sleep(500 * time.Millisecond)
				} else {
					time.Sleep(50 * time.Millisecond)
				}
			}
		}(i, recorder)
	}
	// Cleanup also joins workers if a fault assertion fails.
	stopped := false
	defer func() {
		if !stopped {
			close(stop)
			done.Wait()
		}
	}()
	faults := []map[string]any{}
	mark := func(name string, member int) {
		faults = append(faults, map[string]any{"fault": name, "member": member,
			"time_ns": time.Since(base).Nanoseconds()})
	}
	time.Sleep(300 * time.Millisecond)
	// Each history uses one fault so unknown writes do not span unrelated campaigns.
	fault := (seed - 1) % 4
	if fault < 0 {
		fault += 4
	}
	switch fault {
	case 0:
		leader := c.leader()
		mark("partition-leader", leader)
		for id := range c.processes {
			blocked := []int{leader}
			if id == leader {
				blocked = []int{leader%3 + 1, (leader+1)%3 + 1}
			}
			if _, err := c.http(id, "/debug/partition", map[string]any{"blocked": blocked}); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(1500 * time.Millisecond)
		for id := range c.processes {
			if _, err := c.http(id, "/debug/partition", map[string]any{"blocked": []int{}}); err != nil {
				t.Fatal(err)
			}
		}
		mark("heal", 0)
	case 1:
		leader := c.leader()
		mark("kill-leader", leader)
		c.stop(leader)
		c.leader()
		c.start(leader)
	case 2:
		leader := c.leader()
		mark("pause-leader", leader)
		if err := c.processes[leader].Process.Signal(syscall.SIGSTOP); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1500 * time.Millisecond)
		if err := c.processes[leader].Process.Signal(syscall.SIGCONT); err != nil {
			t.Fatal(err)
		}
		mark("resume", leader)
	case 3:
		mark("kill-all", 0)
		for id := range c.processes {
			c.stop(id)
		}
		time.Sleep(200 * time.Millisecond)
		for id := 1; id <= 3; id++ {
			c.start(id)
		}
		c.leader()
		mark("restart-all", 0)
	}
	time.Sleep(3 * time.Second)
	time.Sleep(500 * time.Millisecond)
	close(stop)
	done.Wait()
	stopped = true
	for i, recorder := range recorders {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := recorder.Range(ctx, "\x00", "\xff", 0, 0)
		cancel()
		if err != nil {
			t.Fatal("No final read after recovery:", err)
		}
		// Use fresh process IDs for final reads after all uncertain operations.
		process := 1000000 + i
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		start := time.Since(base).Nanoseconds()
		response, err := recorder.Get(ctx, "register")
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		value := "nil"
		if len(response.Kvs) != 0 {
			value = strconv.Quote(string(response.Kvs[0].Value))
		}
		events[i] = append(events[i], jepsenEvent{start, process, "invoke", "read", "nil"},
			jepsenEvent{time.Since(base).Nanoseconds(), process, "ok", "read", value})
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		start = time.Since(base).Nanoseconds()
		_, value, _, err = appendTransaction(ctx, recorder, 0)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		appendEvents[i] = append(appendEvents[i],
			jepsenEvent{start, process, "invoke", "txn", "[[:r :a nil] [:r :b nil]]"},
			jepsenEvent{time.Since(base).Nanoseconds(), process, "ok", "txn", value})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	barrier, err := recorders[0].Put(ctx, "__watch_barrier", "done")
	require.NoError(t, err)
	final, err := recorders[0].Range(ctx, "\x00", "\xff", 0, 0)
	cancel()
	require.NoError(t, err)
	watchHistories := finishFaultWatches(t, watchResults, barrier.Header.Revision, final.Kvs)
	stopWatch()
	globalEnd := time.Since(base).Nanoseconds() + int64(time.Second)
	var history []porcupine.Operation
	for _, recorder := range recorders {
		for _, op := range recorder.Report().KeyValue {
			request := op.Input.(model.EtcdRequest)
			normalizeRecordedRequest(&request)
			op.Input = request
			if op.Output.(model.MaybeEtcdResponse).Error != "" && !request.IsRead() {
				op.Return = globalEnd
			}
			history = append(history, op)
		}
	}
	root := os.Getenv("SLATEDB_TEST_RESULTS")
	if root == "" {
		root = c.root
	}
	root = filepath.Join(root, fmt.Sprintf("robustness-%d", seed))
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	saveJSON(t, filepath.Join(root, "operations.json"), history)
	saveJSON(t, filepath.Join(root, "faults.json"), faults)
	saveJSON(t, filepath.Join(root, "watches.json"), watchHistories)
	saveEDN(t, filepath.Join(root, "jepsen.edn"), events)
	saveEDN(t, filepath.Join(root, "append.edn"), appendEvents)
	result, info := porcupine.CheckOperationsVerbose(model.NonDeterministicModel, history, 30*time.Second)
	if err := porcupine.VisualizePath(model.NonDeterministicModel, info,
		filepath.Join(root, "history.html")); err != nil {
		t.Fatal(err)
	}
	saveJSON(t, filepath.Join(root, "result.json"), map[string]any{
		"seed": seed, "checker": "etcd v3.6.5 NonDeterministicModel with Porcupine v0.1.4",
		"result": result, "operations": len(history), "successful_operations": counts,
		"process_logs": c.root, "faults": faults, "watch_replicas": len(watchHistories),
	})
	t.Log("Fault history:", root)
	for _, count := range counts {
		if count < 10 {
			t.Fatal("Insufficient successful operations:", counts)
		}
	}
	if result != porcupine.Ok {
		t.Fatalf("Linearizability check did not pass: %s", result)
	}
}

func saveEDN(t *testing.T, path string, workers [][]jepsenEvent) {
	t.Helper()
	var events []jepsenEvent
	for _, worker := range workers {
		events = append(events, worker...)
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].Time < events[j].Time })
	var edn strings.Builder
	edn.WriteString("[\n")
	for _, event := range events {
		fmt.Fprintf(&edn, "{:process %d :type :%s :f :%s :value %s :time %d}\n",
			event.Process, event.Type, event.Function, event.Value, event.Time)
	}
	edn.WriteString("]\n")
	if err := os.WriteFile(path, []byte(edn.String()), 0600); err != nil {
		t.Fatal(err)
	}
}
