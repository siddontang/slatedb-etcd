//go:build failpoints

package compat

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Crash real processes at the Rust equivalents of etcd persistence and apply hooks.
func TestFailpointRecovery(t *testing.T) {
	points := []string{
		"wal-before-sync", "wal-after-sync",
		"raft-before-advance", "raft-before-leader-send", "raft-before-follower-send",
		"snapshot-before-save", "snapshot-after-save", "apply-before-entry", "apply-after-entry",
		"apply-batch-before-write", "apply-batch-after-write", "apply-after-early-reply",
		"checkpoint-after-file-sync", "checkpoint-file-before-reference", "checkpoint-before-save",
		"checkpoint-after-save", "checkpoint-after-gc",
		"snapshot-before-ready", "snapshot-before-install", "snapshot-after-install-write",
		"snapshot-after-install-flush",
		"compact-before-apply", "compact-after-apply", "compact-before-flush", "compact-after-flush",
		"watch-before-send", "lease-before-expire",
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			// Replicated mode only: shared state mode does not reach these hooks. They are in
			// the local checkpoint, Raft snapshot install, and compaction flush paths. The
			// removal of replicated mode must replace these cases.
			c := newCluster(t)
			leader := c.leader()
			target := leader%3 + 1
			if point == "raft-before-leader-send" || point == "apply-after-early-reply" {
				target = leader
			}
			api := client(t, []string{c.addresses[leader-1]})
			anchor := "acknowledged"
			if strings.HasPrefix(point, "snapshot-") || strings.HasPrefix(point, "checkpoint-") {
				anchor = strings.Repeat("checkpoint", 8192)
			}
			if strings.HasPrefix(point, "compact-") {
				mustCall(t, api, clientv3.OpPut("anchor", strings.Repeat("x", 4096)))
				mustCall(t, api, clientv3.OpDelete("anchor"))
			}
			base := mustCall(t, api, clientv3.OpPut("anchor", anchor)).(*pb.PutResponse)
			for _, endpoint := range c.addresses {
				mustCall(t, client(t, []string{endpoint}), clientv3.OpGet("anchor"))
			}
			trigger := filepath.Join(c.root, fmt.Sprintf("fault-%d", target))
			witness := trigger + ".reached"
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			batched := map[string]int64{}
			var priorRevision int64
			const priorValue = "\x00\xffbefore"
			if strings.HasPrefix(point, "snapshot-") {
				c.stop(target)
				for i := 0; i < 8; i++ {
					mustCall(t, api, clientv3.OpPut("later", fmt.Sprint(i)))
				}
				for id := range c.processes {
					_, err := c.http(id, "/checkpoint", map[string]any{})
					require.NoError(t, err)
				}
				require.NoError(t, os.WriteFile(trigger, []byte(point), 0600))
				c.start(target)
			} else if strings.HasPrefix(point, "apply-batch-") {
				prior := mustCall(t, api, clientv3.OpPut("batch/00", priorValue)).(*pb.PutResponse)
				priorRevision = prior.Header.Revision
				applyDelay(t, c, target, 2500)
				mustCall(t, api, clientv3.OpPut("batch-blocker", "ready"))
				for i := 0; i < 8; i++ {
					key := fmt.Sprintf("batch/%02d", i)
					if i%2 == 0 {
						reply := mustCall(t, api, clientv3.OpPut(key, point)).(*pb.PutResponse)
						batched[key] = reply.Header.Revision
						continue
					}
					// The compare reads the put that the same apply group wrote just before.
					previous := fmt.Sprintf("batch/%02d", i-1)
					reply := mustCall(t, api, clientv3.OpTxn(
						[]clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(previous), "=", batched[previous])},
						[]clientv3.Op{clientv3.OpPut(key, point)},
						[]clientv3.Op{clientv3.OpPut("batch/never", "stale compare")},
					)).(*pb.TxnResponse)
					require.True(t, reply.Succeeded)
					batched[key] = reply.Header.Revision
				}
				// A failed transaction in the group writes nothing and keeps the revision.
				failed := mustCall(t, api, clientv3.OpTxn(
					[]clientv3.Cmp{clientv3.Compare(clientv3.Value("batch/01"), "=", "wrong")},
					[]clientv3.Op{clientv3.OpPut("batch/never", "failed compare")},
					[]clientv3.Op{clientv3.OpGet("batch/01")},
				)).(*pb.TxnResponse)
				require.False(t, failed.Succeeded)
				require.Equal(t, batched["batch/07"], failed.Header.Revision)
				last, err := c.http(leader, "/status", nil)
				require.NoError(t, err)
				// Arm only after a delayed job contains the complete multi-entry suffix.
				require.Eventually(t, func() bool {
					state, err := c.http(target, "/status", nil)
					return err == nil && state["apply_busy"] == true &&
						state["apply_target"].(float64) >= last["applied_index"].(float64) &&
						state["apply_target"].(float64)-state["applied_index"].(float64) > 1
				}, 10*time.Second, 10*time.Millisecond, "no multi-entry apply job")
				require.NoError(t, os.WriteFile(trigger, []byte(point), 0600))
			} else if point == "apply-after-early-reply" {
				// The leader replies after prepare. The delay lets the reply reach the client
				// before the publish job crashes ahead of the state write.
				applyDelay(t, c, target, 500)
				require.NoError(t, os.WriteFile(trigger, []byte(point), 0600))
				reply := mustCall(t, api, clientv3.OpPut("early", point)).(*pb.PutResponse)
				batched["early"] = reply.Header.Revision
			} else if point == "lease-before-expire" {
				// Only the leader proposes expiration. Its follower must replay that entry.
				grant, err := api.Grant(ctx, 2)
				require.NoError(t, err)
				_, err = api.Put(ctx, "expiring", "lease", clientv3.WithLease(grant.ID))
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(trigger, []byte(point), 0600))
			} else if point == "watch-before-send" {
				watcher := client(t, []string{c.addresses[target-1]})
				stream := watcher.Watch(ctx, "watched", clientv3.WithCreatedNotify())
				select {
				case created := <-stream:
					require.True(t, created.Created)
				case <-ctx.Done():
					t.Fatal("watch creation timed out")
				}
				require.NoError(t, os.WriteFile(trigger, []byte(point), 0600))
				mustCall(t, api, clientv3.OpPut("watched", "event"))
			} else {
				require.NoError(t, os.WriteFile(trigger, []byte(point), 0600))
				switch {
				case strings.HasPrefix(point, "checkpoint-"):
					_, _ = c.http(target, "/checkpoint", map[string]any{})
				case strings.HasPrefix(point, "compact-"):
					_, err := compact(api, base.Header.Revision, true)
					require.NoError(t, err)
				default:
					if point == "raft-before-leader-send" {
						_, _ = api.Put(ctx, "trigger", "uncertain-on-target")
					} else {
						mustCall(t, api, clientv3.OpPut("trigger", "uncertain-on-target"))
					}
				}
			}
			require.Eventually(t, func() bool {
				data, err := os.ReadFile(witness)
				return err == nil && string(data) == point
			}, 15*time.Second, 20*time.Millisecond, "fault hook was not reached")
			require.NoError(t, os.Remove(trigger))
			c.stop(target)
			c.start(target)
			c.waitReady()
			recovered := client(t, []string{c.addresses[target-1]})
			got := mustCall(t, recovered, clientv3.OpGet("anchor")).(*pb.RangeResponse)
			require.Len(t, got.Kvs, 1)
			require.Equal(t, anchor, string(got.Kvs[0].Value))
			require.GreaterOrEqual(t, got.Header.Revision, base.Header.Revision)
			for key, revision := range batched {
				got := mustCall(t, recovered, clientv3.OpGet(key)).(*pb.RangeResponse)
				require.Len(t, got.Kvs, 1)
				require.Equal(t, point, string(got.Kvs[0].Value))
				require.Equal(t, revision, got.Kvs[0].ModRevision)
			}
			if point == "apply-after-early-reply" {
				// Every member, including the replayed leader, has the replied revision.
				for _, endpoint := range c.addresses {
					got := mustCall(t, client(t, []string{endpoint}),
						clientv3.OpGet("early")).(*pb.RangeResponse)
					require.Len(t, got.Kvs, 1)
					require.Equal(t, batched["early"], got.Kvs[0].ModRevision)
				}
			}
			if priorRevision != 0 {
				never := mustCall(t, recovered, clientv3.OpGet("batch/never")).(*pb.RangeResponse)
				require.Empty(t, never.Kvs)
				old := mustCall(t, recovered, clientv3.OpGet("batch/00",
					clientv3.WithRev(priorRevision))).(*pb.RangeResponse)
				require.Len(t, old.Kvs, 1)
				require.Equal(t, priorValue, string(old.Kvs[0].Value))
				stream := recovered.Watch(ctx, "batch/00",
					clientv3.WithRev(batched["batch/00"]), clientv3.WithPrevKV())
				select {
				case replay := <-stream:
					require.NoError(t, replay.Err())
					require.Len(t, replay.Events, 1)
					require.Equal(t, old.Kvs[0], replay.Events[0].PrevKv)
					require.Equal(t, point, string(replay.Events[0].Kv.Value))
				case <-ctx.Done():
					t.Fatal("batch watch replay timed out")
				}
			}
			mustCall(t, recovered, clientv3.OpPut("after-fault", point))
			if point == "watch-before-send" {
				replay := recovered.Watch(ctx, "watched", clientv3.WithRev(base.Header.Revision+1))
				select {
				case response := <-replay:
					require.NoError(t, response.Err())
					require.Len(t, response.Events, 1)
					require.Equal(t, "event", string(response.Events[0].Kv.Value))
				case <-ctx.Done():
					t.Fatal("watch replay timed out")
				}
			}
			if point == "lease-before-expire" {
				gone := mustCall(t, recovered, clientv3.OpGet("expiring")).(*pb.RangeResponse)
				require.Empty(t, gone.Kvs)
			}
		})
	}
}
