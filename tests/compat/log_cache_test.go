package compat

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestBoundedLogCacheRecoversLaggingFollower(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "delayed-read"
		if fail {
			name = "failed-read"
		}
		t.Run(name, func(t *testing.T) {
			args := []string{"--log-cache-bytes", "1024"}
			if fail {
				// Replicated mode only. The test expects the lagging follower to catch up from
				// the log, with `snapshot_index` at 0 on each member. In shared state mode a
				// member can install a base after the leader restart: on Linux CI it reported
				// `snapshot_index` 15. On macOS it caught up from the log in 5 of 5 runs, so an
				// install is not a stable expectation either. The removal of replicated mode
				// must replace this case.
				args = append(args, "--state-mode", "replicated")
			}
			c := newCluster(t, args...)
			leader := c.leader()
			offline := leader%3 + 1
			c.stop(offline)
			api := client(t, []string{c.addresses[leader-1]})
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			value := strings.Repeat("x", 32*1024)
			for n := 0; n < 12; n++ {
				_, err := api.Put(ctx, "cache-recovery", value)
				require.NoError(t, err)
			}
			before, err := c.http(leader, "/status", nil)
			require.NoError(t, err)
			require.EqualValues(t, 0, before["snapshot_index"])
			memory := before["log_memory"].(map[string]any)
			require.LessOrEqual(t, memory["cache_bytes"].(float64), float64(1024))
			require.EqualValues(t, 0, memory["pending_bytes"])
			delay := 2500
			if fail {
				delay = 0
			}
			_, err = c.http(leader, "/debug/log-read-fault", map[string]any{
				"delay_ms": delay, "fail": fail,
			})
			require.NoError(t, err)
			c.start(offline)
			if fail {
				require.Eventually(t, func() bool {
					_, err := c.http(leader, "/status", nil)
					return err != nil
				}, 5*time.Second, 20*time.Millisecond)
				c.stop(leader)
				c.start(leader)
				c.leader()
			} else {
				require.Eventually(t, func() bool {
					state, err := c.http(leader, "/status", nil)
					return err == nil && state["log_memory"].(map[string]any)["read_reserved"].(float64) > 0
				}, 2*time.Second, 10*time.Millisecond)
				for n := 0; n < 10; n++ {
					start := time.Now()
					state, err := c.http(leader, "/status", nil)
					require.NoError(t, err)
					require.Less(t, time.Since(start), time.Second)
					require.Equal(t, "Leader", state["role"])
					require.Equal(t, before["term"], state["term"])
					time.Sleep(150 * time.Millisecond)
				}
				_, err := c.http(leader, "/debug/log-read-fault", map[string]any{
					"delay_ms": 0, "fail": false,
				})
				require.NoError(t, err)
			}
			recovered := client(t, []string{c.addresses[offline-1]})
			require.Eventually(t, func() bool {
				result, err := recovered.Get(ctx, "cache-recovery", clientv3.WithSerializable())
				return err == nil && len(result.Kvs) == 1 && string(result.Kvs[0].Value) == value
			}, 10*time.Second, 20*time.Millisecond)
			for id := 1; id <= 3; id++ {
				state, err := c.http(id, "/status", nil)
				require.NoError(t, err)
				memory := state["log_memory"].(map[string]any)
				for _, pool := range []string{"cache", "pending", "read", "term"} {
					require.LessOrEqual(t, memory[pool+"_bytes"].(float64), memory[pool+"_limit"].(float64))
				}
				require.EqualValues(t, 0, state["snapshot_index"])
			}
		})
	}
}

func TestFollowerReadIndexWaitsForLocalApply(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	follower := leader%3 + 1
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := c.http(follower, "/debug/apply-delay", map[string]any{"delay_ms": 2500})
	require.NoError(t, err)
	api := client(t, []string{c.addresses[leader-1]})
	written, err := api.Put(ctx, "follower-barrier", "value")
	require.NoError(t, err)
	local := client(t, []string{c.addresses[follower-1]})
	short, stop := context.WithTimeout(ctx, 400*time.Millisecond)
	_, err = local.Get(short, "follower-barrier")
	stop()
	require.Error(t, err, "follower returned a read before local apply")
	_, err = c.http(follower, "/debug/apply-delay", map[string]any{"delay_ms": 0})
	require.NoError(t, err)
	result, err := local.Get(ctx, "follower-barrier")
	require.NoError(t, err)
	require.Equal(t, written.Header.Revision, result.Kvs[0].ModRevision)
	historical, err := local.Get(ctx, "follower-barrier",
		clientv3.WithSerializable(), clientv3.WithRev(result.Header.Revision))
	require.NoError(t, err)
	require.Equal(t, "value", string(historical.Kvs[0].Value))
}

func TestForwardedWriteWaitsForLocalApply(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	follower := leader%3 + 1
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	api := client(t, []string{c.addresses[follower-1]})
	initial, err := api.Put(ctx, "forwarded-barrier", "before")
	require.NoError(t, err)
	next := follower%3 + 1
	sender := client(t, []string{c.addresses[leader-1]})
	_, err = sender.MoveLeader(ctx, uint64(next))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		status, err := api.Status(ctx, c.addresses[follower-1])
		return err == nil && status.Leader == uint64(next)
	}, 5*time.Second, 10*time.Millisecond)
	_, err = c.http(follower, "/debug/apply-delay", map[string]any{"delay_ms": 2500})
	require.NoError(t, err)
	replies := make(chan *clientv3.PutResponse, 1)
	errors := make(chan error, 1)
	go func() {
		response, err := api.Put(ctx, "forwarded-barrier", "value", clientv3.WithPrevKV())
		replies <- response
		errors <- err
	}()
	require.Eventually(t, func() bool {
		state, err := c.http(follower, "/status", nil)
		return err == nil && state["commit_index"].(float64) > state["applied_index"].(float64) &&
			state["pending_revision_waiters"].(float64) > 0
	}, time.Second, 10*time.Millisecond)
	state, err := c.http(follower, "/status", nil)
	require.NoError(t, err)
	require.Zero(t, state["pending_read_indexes"].(float64))
	select {
	case reply := <-replies:
		t.Fatalf("forwarded write returned before local apply: %v", reply)
	case <-time.After(300 * time.Millisecond):
	}
	_, err = c.http(follower, "/debug/apply-delay", map[string]any{"delay_ms": 0})
	require.NoError(t, err)
	require.NoError(t, <-errors)
	written := <-replies
	require.NotNil(t, written.PrevKv)
	require.Equal(t, initial.Header.Revision, written.PrevKv.ModRevision)
	require.Equal(t, uint64(follower), written.Header.MemberId)
	result, err := api.Get(ctx, "forwarded-barrier",
		clientv3.WithSerializable(), clientv3.WithRev(written.Header.Revision))
	require.NoError(t, err)
	require.Len(t, result.Kvs, 1)
	require.Equal(t, "value", string(result.Kvs[0].Value))
	c.stop(follower)
	c.start(follower)
	recovered, err := api.Get(ctx, "forwarded-barrier")
	require.NoError(t, err)
	require.Len(t, recovered.Kvs, 1)
	require.Equal(t, "value", string(recovered.Kvs[0].Value))

}
