package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func applyDelay(t *testing.T, c *cluster, id int, milliseconds int) {
	t.Helper()
	_, err := c.http(id, "/debug/apply-delay", map[string]any{"delay_ms": milliseconds})
	require.NoError(t, err)
}

func TestSlowApplyKeepsRaftLive(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	follower := leader%3 + 1
	api := client(t, []string{c.addresses[leader-1]})
	witness := client(t, []string{c.addresses[follower-1]})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := api.Put(ctx, "before", "ready")
	require.NoError(t, err)
	before, err := c.http(leader, "/status", nil)
	require.NoError(t, err)
	// Only the leader applies slowly. A shared Put can take two delayed jobs (prepare and
	// apply), so both must end within the 5 s request timeout.
	applyDelay(t, c, leader, 1500)
	// A shared Put can reply after prepare and commit, before apply. So the test does not time
	// the write reply. It needs only success.
	written := make(chan error, 1)
	go func() { _, err := api.Put(ctx, "slow", "committed"); written <- err }()
	// A follower applies at normal speed. Its serializable read proves that the Put committed,
	// and it gives the revision of the Put. The leader must still apply below that revision.
	var revision float64
	require.Eventually(t, func() bool {
		attempt, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		r, err := witness.Get(attempt, "slow", clientv3.WithSerializable())
		if err != nil || len(r.Kvs) != 1 {
			return false
		}
		revision = float64(r.Kvs[0].ModRevision)
		state, err := c.http(leader, "/status", nil)
		return err == nil && state["revision"].(float64) < revision
	}, 4*time.Second, 10*time.Millisecond, "the follower must apply the Put before the leader")
	checkpoint := make(chan map[string]any, 1)
	checkpointErr := make(chan error, 1)
	go func() {
		state, err := c.http(leader, "/checkpoint", map[string]any{})
		checkpoint <- state
		checkpointErr <- err
	}()
	read := make(chan error, 1)
	go func() {
		r, err := api.Get(ctx, "slow")
		if err == nil && (len(r.Kvs) != 1 || string(r.Kvs[0].Value) != "committed") {
			err = fmt.Errorf("linearizable read missed the committed write: %v", r)
		}
		read <- err
	}()
	// While the leader applies below the Put, Raft stays live: same term, same leader.
	for {
		require.NoError(t, ctx.Err(), "the leader did not apply the Put")
		state, err := c.http(leader, "/status", nil)
		require.NoError(t, err)
		require.Equal(t, before["term"], state["term"])
		require.Equal(t, "Leader", state["role"])
		if state["revision"].(float64) >= revision {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	applyDelay(t, c, leader, 0)
	require.NoError(t, <-written)
	require.NoError(t, <-read)
	// The reply of a manual checkpoint is the status at the reply. It must include the Put, and
	// its base must be above the last log index before the Put.
	replied := <-checkpoint
	require.NoError(t, <-checkpointErr)
	require.GreaterOrEqual(t, replied["revision"].(float64), revision)
	base := replied["shared_state"].(map[string]any)["base_applied_index"].(float64)
	require.Greater(t, base, before["last_log_index"].(float64))
	after, err := c.http(leader, "/status", nil)
	require.NoError(t, err)
	require.Equal(t, before["term"], after["term"])
}

func TestCommittedApplyBacklogSurvivesRestart(t *testing.T) {
	c := newCluster(t)
	api := client(t, c.addresses)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	lease, err := api.Grant(ctx, 60)
	require.NoError(t, err)
	for id := 1; id <= 3; id++ {
		applyDelay(t, c, id, 5000)
	}
	writeCtx, stopWrite := context.WithCancel(ctx)
	defer stopWrite()
	written := make(chan error, 1)
	go func() {
		_, err := api.Put(writeCtx, "backlog", "survives", clientv3.WithLease(lease.ID))
		written <- err
	}()
	leader := c.leader()
	require.Eventually(t, func() bool {
		state, err := c.http(leader, "/status", nil)
		return err == nil && state["apply_lag"].(float64) > 0
	}, time.Second, 10*time.Millisecond)
	select {
	case err := <-written:
		t.Fatalf("write replied before apply: %v", err)
	default:
	}
	stopWrite()
	require.Error(t, <-written)
	for id := range c.processes {
		c.stop(id)
	}
	for id := 1; id <= 3; id++ {
		c.start(id)
	}
	c.leader()
	fresh := client(t, c.addresses)
	response, err := fresh.Get(ctx, "backlog")
	require.NoError(t, err)
	require.Len(t, response.Kvs, 1)
	require.Equal(t, "survives", string(response.Kvs[0].Value))
	require.Equal(t, int64(lease.ID), response.Kvs[0].Lease)
	events := collectWatch(t, fresh, "backlog", 1, clientv3.WithRev(1))
	require.Equal(t, response.Kvs[0].ModRevision, events[0].Kv.ModRevision)
}

func TestSnapshotApplyKeepsPeerReplicationLive(t *testing.T) {
	// In shared state mode the lagging follower installed a snapshot record in 2 of 3 runs. In the
	// third, it did not reach `snapshot_index > applied_index` in time.
	t.Skip("needs a deterministic snapshot trigger; see #149 " +
		"TestSnapshotApplyKeepsPeerReplicationLive")
	c := newCluster(t)
	leader := c.leader()
	lagging := leader%3 + 1
	api := client(t, []string{c.addresses[leader-1]})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for id := range c.processes {
		if id != lagging {
			_, err := c.http(id, "/debug/partition", map[string]any{"blocked": []int{lagging}})
			require.NoError(t, err)
		}
	}
	c.stop(lagging)
	for n := 0; n < 40; n++ {
		_, err := api.Put(ctx, "snapshot-apply", fmt.Sprint(n))
		require.NoError(t, err)
	}
	for id := range c.processes {
		_, err := c.http(id, "/checkpoint", map[string]any{})
		require.NoError(t, err)
	}
	c.start(lagging)
	require.Eventually(t, func() bool { _, err := c.http(lagging, "/status", nil); return err == nil },
		3*time.Second, 10*time.Millisecond)
	applyDelay(t, c, lagging, 2500)
	for id := range c.processes {
		_, err := c.http(id, "/debug/partition", map[string]any{"blocked": []int{}})
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		state, err := c.http(lagging, "/status", nil)
		return err == nil && state["snapshot_index"].(float64) > state["applied_index"].(float64)
	}, 2*time.Second, 10*time.Millisecond)
	for n := 40; n < 45; n++ {
		_, err := api.Put(ctx, "snapshot-apply", fmt.Sprint(n))
		require.NoError(t, err)
	}
	latest, err := c.http(leader, "/status", nil)
	require.NoError(t, err)
	target := latest["commit_index"].(float64)
	require.Eventually(t, func() bool {
		state, err := c.http(lagging, "/status", nil)
		return err == nil && state["commit_index"].(float64) >= target &&
			state["installed_snapshots"].(float64) == 0
	}, time.Second, 10*time.Millisecond, "peer replication waited for state installation")
	applyDelay(t, c, lagging, 0)
	require.Eventually(t, func() bool {
		state, err := c.http(lagging, "/status", nil)
		return err == nil && state["applied_index"].(float64) >= target &&
			state["installed_snapshots"].(float64) > 0
	}, 5*time.Second, 20*time.Millisecond)
	follower := client(t, []string{c.addresses[lagging-1]})
	response, err := follower.Get(ctx, "snapshot-apply", clientv3.WithSerializable())
	require.NoError(t, err)
	require.Equal(t, "44", string(response.Kvs[0].Value))
	collectWatch(t, follower, "snapshot-apply", 45, clientv3.WithRev(1))
}
