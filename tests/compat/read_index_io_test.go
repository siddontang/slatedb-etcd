package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func logFault(t *testing.T, c *cluster, id, delay int, fail bool) {
	t.Helper()
	_, err := c.http(id, "/debug/log-fault", map[string]any{"delay_ms": delay, "fail": fail})
	require.NoError(t, err)
}

func TestReadIndexAndNestedReadTxnDoNotWriteLogs(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	api := client(t, []string{c.addresses[leader-1]})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := api.Put(ctx, "read-index", "value")
	require.NoError(t, err)
	state, err := c.http(leader, "/status", nil)
	require.NoError(t, err)
	target := state["commit_index"].(float64)
	before := make(map[int]map[string]any)
	for id := 1; id <= 3; id++ {
		require.Eventually(t, func() bool {
			state, err := c.http(id, "/status", nil)
			if err == nil && state["applied_index"].(float64) >= target {
				before[id] = state
				return true
			}
			return false
		}, 3*time.Second, 10*time.Millisecond)
	}
	for id := 1; id <= 3; id++ {
		api := client(t, []string{c.addresses[id-1]})
		for n := 0; n < 5; n++ {
			result, err := api.Get(ctx, "read-index")
			require.NoError(t, err)
			require.Len(t, result.Kvs, 1)
			require.Equal(t, "value", string(result.Kvs[0].Value))
			txn, err := api.Txn(ctx).If(clientv3.Compare(clientv3.Value("read-index"), "=", "value")).
				Then(clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpGet("read-index")}, nil)).
				Else(clientv3.OpGet("read-index")).Commit()
			require.NoError(t, err)
			require.True(t, txn.Succeeded)
		}
	}
	for id := 1; id <= 3; id++ {
		after, err := c.http(id, "/status", nil)
		require.NoError(t, err)
		require.Equal(t, before[id]["term"], after["term"])
		require.Equal(t, before[id]["last_log_index"], after["last_log_index"])
		require.Equal(t, before[id]["sync_writes"], after["sync_writes"])
	}
}

func TestReadIndexRejectsMinorityLeader(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	minority := client(t, []string{c.addresses[leader-1]})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := minority.Put(ctx, "fenced-read", "before")
	require.NoError(t, err)
	var blocked []int
	for id := 1; id <= 3; id++ {
		if id != leader {
			blocked = append(blocked, id)
			_, err := c.http(id, "/debug/partition", map[string]any{"blocked": []int{leader}})
			require.NoError(t, err)
		}
	}
	_, err = c.http(leader, "/debug/partition", map[string]any{"blocked": blocked})
	require.NoError(t, err)
	short, stop := context.WithTimeout(ctx, 600*time.Millisecond)
	_, err = minority.Get(short, "fenced-read")
	stop()
	require.Error(t, err, "isolated leader returned a read without a quorum")
	var newLeader int
	require.Eventually(t, func() bool {
		for id := 1; id <= 3; id++ {
			if id == leader {
				continue
			}
			state, err := c.http(id, "/status", nil)
			if err == nil && state["role"] == "Leader" {
				newLeader = id
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)
	healthy := client(t, []string{c.addresses[newLeader-1]})
	_, err = healthy.Put(ctx, "fenced-read", "after")
	require.NoError(t, err)
	short, stop = context.WithTimeout(ctx, 600*time.Millisecond)
	_, err = minority.Get(short, "fenced-read")
	stop()
	require.Error(t, err)
	for id := 1; id <= 3; id++ {
		_, err := c.http(id, "/debug/partition", map[string]any{"blocked": []int{}})
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool {
		result, err := minority.Get(ctx, "fenced-read")
		return err == nil && len(result.Kvs) == 1 && string(result.Kvs[0].Value) == "after"
	}, 5*time.Second, 20*time.Millisecond)
}

func TestSlowLogKeepsRaftLiveAndFencesReplies(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	api := client(t, []string{c.addresses[leader-1]})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := api.Put(ctx, "disk", "before")
	require.NoError(t, err)
	before, err := c.http(leader, "/status", nil)
	require.NoError(t, err)
	logFault(t, c, leader, 2500, false)
	written := make(chan error, 1)
	go func() { _, err := api.Put(ctx, "disk", "after"); written <- err }()
	var target float64
	require.Eventually(t, func() bool {
		state, err := c.http(leader, "/status", nil)
		if err != nil {
			return false
		}
		target = state["raft_commit_index"].(float64)
		return target > state["commit_index"].(float64)
	}, time.Second, 10*time.Millisecond)
	checkpoint := make(chan error, 1)
	go func() { _, err := c.http(leader, "/checkpoint", map[string]any{}); checkpoint <- err }()
	read := make(chan error, 1)
	go func() {
		result, err := api.Get(ctx, "disk")
		if err == nil && (len(result.Kvs) != 1 || string(result.Kvs[0].Value) != "after") {
			err = fmt.Errorf("read crossed the persistence barrier: %v", result)
		}
		read <- err
	}()
	for n := 0; n < 10; n++ {
		start := time.Now()
		state, err := c.http(leader, "/status", nil)
		require.NoError(t, err)
		require.Less(t, time.Since(start), time.Second)
		require.Equal(t, "Leader", state["role"])
		require.Equal(t, before["term"], state["term"])
		require.Equal(t, before["applied_index"], state["applied_index"])
		select {
		case err := <-written:
			t.Fatalf("write crossed the fsync barrier: %v", err)
		default:
		}
		select {
		case err := <-read:
			t.Fatalf("read crossed the fsync barrier: %v", err)
		default:
		}
		time.Sleep(150 * time.Millisecond)
	}
	logFault(t, c, leader, 0, false)
	require.NoError(t, <-written)
	require.NoError(t, <-read)
	require.NoError(t, <-checkpoint)
	after, err := c.http(leader, "/status", nil)
	require.NoError(t, err)
	require.Equal(t, before["term"], after["term"])
	require.GreaterOrEqual(t, after["snapshot_index"].(float64), target)
}

func TestLogWriteFailureStopsNodeAndRecoversAcknowledgedState(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	api := client(t, []string{c.addresses[leader-1]})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := api.Put(ctx, "durable-before-error", "safe")
	require.NoError(t, err)
	logFault(t, c, leader, 0, true)
	short, stop := context.WithTimeout(ctx, time.Second)
	_, err = api.Put(short, "unknown-after-error", "maybe")
	stop()
	require.Error(t, err)
	require.Eventually(t, func() bool { _, err := c.http(leader, "/status", nil); return err != nil },
		3*time.Second, 10*time.Millisecond)
	c.stop(leader)
	c.start(leader)
	recoveredLeader := c.leader()
	fresh := client(t, []string{c.addresses[recoveredLeader-1]})
	result, err := fresh.Get(ctx, "durable-before-error")
	require.NoError(t, err)
	require.Len(t, result.Kvs, 1)
	require.Equal(t, "safe", string(result.Kvs[0].Value))
	events := collectWatch(t, fresh, "durable-before-error", 1, clientv3.WithRev(1))
	require.Equal(t, result.Kvs[0].ModRevision, events[0].Kv.ModRevision)
}

func TestSlowCheckpointPublicationKeepsRaftLive(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	api := client(t, []string{c.addresses[leader-1]})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := api.Put(ctx, "checkpoint-io", "value")
	require.NoError(t, err)
	before, err := c.http(leader, "/status", nil)
	require.NoError(t, err)
	logFault(t, c, leader, 2500, false)
	checkpoint := make(chan error, 1)
	go func() { _, err := c.http(leader, "/checkpoint", map[string]any{}); checkpoint <- err }()
	require.Eventually(t, func() bool {
		state, err := c.http(leader, "/status", nil)
		return err == nil && state["log_busy"] == true && state["queued_snapshot_bytes"].(float64) > 0
	}, time.Second, 10*time.Millisecond)
	for n := 0; n < 10; n++ {
		state, err := c.http(leader, "/status", nil)
		require.NoError(t, err)
		require.Equal(t, before["term"], state["term"])
		require.Equal(t, "Leader", state["role"])
		require.Equal(t, before["snapshot_index"], state["snapshot_index"])
		select {
		case err := <-checkpoint:
			t.Fatalf("checkpoint replied before fsync: %v", err)
		default:
		}
		time.Sleep(150 * time.Millisecond)
	}
	logFault(t, c, leader, 0, false)
	require.NoError(t, <-checkpoint)
	c.stop(leader)
	c.start(leader)
	recoveredLeader := c.leader()
	result, err := client(t, []string{c.addresses[recoveredLeader-1]}).Get(ctx, "checkpoint-io")
	require.NoError(t, err)
	require.Equal(t, "value", string(result.Kvs[0].Value))
}

func TestForwardedWriteWaitsForLocalApply(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	follower := leader%3 + 1
	api := client(t, []string{c.addresses[follower-1]})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := api.Put(ctx, "forward-apply", "before")
	require.NoError(t, err)
	_, err = c.http(follower, "/debug/apply-delay", map[string]any{"delay_ms": 1500})
	require.NoError(t, err)
	written := make(chan error, 1)
	go func() { _, err := api.Put(ctx, "forward-apply", "after"); written <- err }()
	require.Eventually(t, func() bool {
		state, err := c.http(follower, "/status", nil)
		return err == nil && state["pending_revision_waiters"].(float64) > 0
	}, time.Second, 10*time.Millisecond)
	state, err := c.http(follower, "/status", nil)
	require.NoError(t, err)
	require.Zero(t, state["pending_read_indexes"].(float64))
	select {
	case err := <-written:
		t.Fatalf("forwarded write preceded local apply: %v", err)
	default:
	}
	_, err = c.http(follower, "/debug/apply-delay", map[string]any{"delay_ms": 0})
	require.NoError(t, err)
	require.NoError(t, <-written)
	result, err := api.Get(ctx, "forward-apply", clientv3.WithSerializable())
	require.NoError(t, err)
	require.Len(t, result.Kvs, 1)
	require.Equal(t, "after", string(result.Kvs[0].Value))
	c.stop(follower)
	c.start(follower)
	result, err = api.Get(ctx, "forward-apply")
	require.NoError(t, err)
	require.Equal(t, "after", string(result.Kvs[0].Value))
}
