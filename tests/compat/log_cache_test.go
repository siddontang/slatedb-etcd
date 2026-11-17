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
			c := newCluster(t, "--log-cache-bytes", "1024")
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
	follower := c.leader()%3 + 1
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := c.http(follower, "/debug/apply-delay", map[string]any{"delay_ms": 2500})
	require.NoError(t, err)
	api := client(t, []string{c.addresses[follower-1]})
	replies := make(chan *clientv3.PutResponse, 1)
	errors := make(chan error, 1)
	go func() {
		response, err := api.Put(ctx, "forwarded-barrier", "value")
		replies <- response
		errors <- err
	}()
	require.Eventually(t, func() bool {
		state, err := c.http(follower, "/status", nil)
		return err == nil && state["commit_index"].(float64) > state["applied_index"].(float64)
	}, time.Second, 10*time.Millisecond)
	select {
	case reply := <-replies:
		t.Fatalf("forwarded write returned before local apply: %v", reply)
	case <-time.After(300 * time.Millisecond):
	}
	_, err = c.http(follower, "/debug/apply-delay", map[string]any{"delay_ms": 0})
	require.NoError(t, err)
	require.NoError(t, <-errors)
	written := <-replies
	result, err := api.Get(ctx, "forwarded-barrier",
		clientv3.WithSerializable(), clientv3.WithRev(written.Header.Revision))
	require.NoError(t, err)
	require.Len(t, result.Kvs, 1)
	require.Equal(t, "value", string(result.Kvs[0].Value))
}
