//go:build failpoints

package compat

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Crash the leader while it applies a step of an automatic compaction. The new leader must
// finish the removal, and the restarted member must reach the same compacted state.
func TestFailpointAutoCompaction(t *testing.T) {
	c := newCluster(t, "--auto-compaction-mode=periodic", "--auto-compaction-retention=1s")
	leader := c.leader()
	api := client(t, c.addresses)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Many versions of a few keys give the removal several steps.
	var first int64
	for i := 0; i < 600; i++ {
		reply, err := api.Put(ctx, fmt.Sprintf("auto/%d", i%5), fmt.Sprint(i))
		require.NoError(t, err)
		if first == 0 {
			first = reply.Header.Revision
		}
	}
	// The removed versions and Watch journal rows that the interrupted target must reclaim.
	before := logicalSizes(t, c.addresses[leader-1])
	require.Greater(t, before["history"], float64(0))
	require.Greater(t, before["watch_journal"], float64(0))
	trigger := filepath.Join(c.root, fmt.Sprintf("fault-%d", leader))
	require.NoError(t, os.WriteFile(trigger, []byte("compact-step-before-apply"), 0600))
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(trigger + ".reached")
		return err == nil && string(data) == "compact-step-before-apply"
	}, 20*time.Second, 20*time.Millisecond, "the automatic compaction did not reach the leader step")
	require.NoError(t, os.Remove(trigger))
	c.stop(leader)
	var others []string
	for id, address := range c.addresses {
		if id+1 != leader {
			others = append(others, address)
		}
	}
	survivors := client(t, others)
	// Retry while the survivors elect a leader.
	put := func(key, value string) *clientv3.PutResponse {
		var reply *clientv3.PutResponse
		require.Eventually(t, func() bool {
			attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			var err error
			reply, err = survivors.Put(attempt, key, value)
			return err == nil
		}, 20*time.Second, 100*time.Millisecond, "put %s", key)
		return reply
	}
	// The survivors keep writing and compacting.
	last := put("auto/last", "after-crash")
	c.start(leader)
	c.waitReady()
	// Stop writing, then wait until every member reports the same boundary and hash.
	revision := put("auto/final", "done").Header.Revision
	require.Eventually(t, func() bool {
		var hashes []uint32
		var boundaries []int64
		for _, endpoint := range c.addresses {
			member := client(t, []string{endpoint})
			got, err := member.Get(ctx, "auto/final", clientv3.WithSerializable())
			if err != nil || got.Header.Revision < revision {
				return false
			}
			hash, err := member.HashKV(ctx, endpoint, revision)
			if err != nil {
				return false
			}
			hashes = append(hashes, hash.Hash)
			boundaries = append(boundaries, hash.CompactRevision)
		}
		for i := 1; i < len(hashes); i++ {
			if hashes[i] != hashes[0] || boundaries[i] != boundaries[0] {
				return false
			}
		}
		return boundaries[0] >= first
	}, 30*time.Second, 200*time.Millisecond, "members did not converge")
	// HashKV skips rows below the boundary, so equal hashes and boundaries also hold when
	// removal stalls. With writes stopped, the boundary reaches the last writes, so every member
	// must physically drop nearly all old versions and journal rows.
	for _, endpoint := range c.addresses {
		require.Eventually(t, func() bool {
			after := logicalSizes(t, endpoint)
			return after["history"] < before["history"]/4 &&
				after["watch_journal"] < before["watch_journal"]/4
		}, 30*time.Second, 200*time.Millisecond, "%s did not remove compacted rows: before %v",
			endpoint, before)
	}
	for _, endpoint := range c.addresses {
		member := client(t, []string{endpoint})
		_, err := member.Get(ctx, "auto/0", clientv3.WithRev(first), clientv3.WithSerializable())
		require.True(t, errors.Is(err, rpctypes.ErrCompacted), "%s: %v", endpoint, err)
		got, err := member.Get(ctx, "auto/last", clientv3.WithSerializable())
		require.NoError(t, err)
		require.Len(t, got.Kvs, 1)
		require.Equal(t, last.Header.Revision, got.Kvs[0].ModRevision)
	}
}

// The logical size gauges of one member by row kind.
func logicalSizes(t *testing.T, address string) map[string]float64 {
	t.Helper()
	kinds := map[string]float64{}
	for _, metric := range scrapeMetrics(t, address)["slatedb_etcd_logical_size_bytes"].GetMetric() {
		kinds[metric.GetLabel()[0].GetValue()] = metric.GetGauge().GetValue()
	}
	return kinds
}
