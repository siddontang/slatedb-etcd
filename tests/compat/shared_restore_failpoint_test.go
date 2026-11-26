//go:build failpoints

package compat

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/snapshot"
	"go.uber.org/zap"
)

// checkLargeValues replays the seeded values of the large keys and reads one key at a time, so
// memory stays bounded. Every key must hold its acknowledged value.
func checkLargeValues(ctx context.Context, t *testing.T, api *clientv3.Client, mib int) {
	t.Helper()
	random := rand.New(rand.NewSource(1))
	value := make([]byte, 1024*1024)
	for i := 0; i < mib; i++ {
		random.Read(value)
		key := fmt.Sprintf("large/%06d", i)
		result, err := api.Get(ctx, key)
		require.NoError(t, err)
		require.Len(t, result.Kvs, 1, key)
		require.True(t, bytes.Equal(value, result.Kvs[0].Value), "%s has another value", key)
	}
}

// Save a shared-mode backup above 64 MiB and restore it into a new shared cluster. The restore
// crashes after its first durable rows. The same command waits for the stale lease, continues
// after the stored rows. Every key keeps its acknowledged value, and the cluster accepts a write.
func TestFailpointSharedRestore(t *testing.T) {
	const mib = 72
	source := newClusterSize(t, 1, "--state-mode", "shared")
	api := client(t, source.addresses)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	random := rand.New(rand.NewSource(1))
	value := make([]byte, 1024*1024)
	for i := 0; i < mib; i++ {
		random.Read(value)
		_, err := putRetrying(ctx, api, fmt.Sprintf("large/%06d", i), string(value))
		require.NoError(t, err)
	}
	checkLargeValues(ctx, t, api, mib)
	file := filepath.Join(t.TempDir(), "shared.snapshot")
	_, err := snapshot.SaveWithVersion(ctx, zap.NewNop(), clientv3.Config{Endpoints: source.addresses}, file)
	require.NoError(t, err)
	info, err := os.Stat(file)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(64)<<20)

	restored := newClusterConfigured(t, 1, func(c *cluster) {
		c.readyTimeout = 10 * time.Minute
		c.deferStart = true
	}, "--state-mode", "shared", "--restore-snapshot", file)
	trigger := filepath.Join(restored.root, "fault-1")
	require.NoError(t, os.WriteFile(trigger, []byte("shared-restore-after-flush"), 0600))
	restored.start(1)
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(trigger + ".reached")
		return err == nil && string(data) == "shared-restore-after-flush"
	}, 5*time.Minute, 20*time.Millisecond, "the restore did not flush its rows")
	require.NoError(t, os.Remove(trigger))
	restored.stop(1)
	restored.start(1)
	restored.waitReady()
	log, err := os.ReadFile(filepath.Join(restored.root, "node-1.log"))
	require.NoError(t, err)
	require.Contains(t, string(log), "continue the partial snapshot restore")

	check := client(t, restored.addresses)
	checkLargeValues(ctx, t, check, mib)
	result, err := check.Get(ctx, fmt.Sprintf("large/%06d", mib-1))
	require.NoError(t, err)
	count, err := check.Get(ctx, "large/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	require.Equal(t, int64(mib), count.Count)
	after, err := putRetrying(ctx, check, "after/restore", "value")
	require.NoError(t, err)
	require.Equal(t, result.Header.Revision+1, after.Header.Revision)
}
