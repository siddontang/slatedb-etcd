package compat

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/snapshot"
	"go.uber.org/zap"
)

// processMemory returns the current and peak resident set size of a process in bytes. It reads
// /proc, so callers check memory on Linux only.
func processMemory(t *testing.T, pid int) (rss, peak int64) {
	file, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
	require.NoError(t, err)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		kib, _ := strconv.ParseInt(fields[1], 10, 64)
		switch fields[0] {
		case "VmRSS:":
			rss = kib * 1024
		case "VmHWM:":
			peak = kib * 1024
		}
	}
	require.NoError(t, scanner.Err())
	return rss, peak
}

// putRetrying writes a key and retries the admission rejections of a busy member.
func putRetrying(ctx context.Context, api *clientv3.Client, key, value string) (*clientv3.PutResponse, error) {
	for {
		response, err := api.Put(ctx, key, value)
		if !errors.Is(err, rpctypes.ErrTooManyRequests) {
			return response, err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestNativeSnapshotStreamsLargeState saves more than the former 64 MiB limit through the
// etcdctl save path while writes continue, then restores it. SLATEDB_SNAPSHOT_TEST_MIB sets the
// state size.
func TestNativeSnapshotStreamsLargeState(t *testing.T) {
	mib := 80
	if value := os.Getenv("SLATEDB_SNAPSHOT_TEST_MIB"); value != "" {
		var err error
		mib, err = strconv.Atoi(value)
		require.NoError(t, err)
	}
	source := newClusterSize(t, 1)
	api := client(t, source.addresses)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	random := rand.New(rand.NewSource(1))
	value := make([]byte, 1024*1024)
	var last []byte
	for i := 0; i < mib; i++ {
		random.Read(value)
		_, err := putRetrying(ctx, api, fmt.Sprintf("large/%06d", i), string(value))
		require.NoError(t, err)
		last = append(last[:0], value...)
	}
	pid := source.processes[1].Process.Pid

	// An interrupted stream releases its checkpoint and does not block a later save.
	interrupted, stop := context.WithCancel(ctx)
	stream, err := api.Snapshot(interrupted)
	require.NoError(t, err)
	_, err = io.ReadFull(stream, make([]byte, 1024*1024))
	require.NoError(t, err)
	stop()
	stream.Close()

	// Flush the loaded rows first. The save would otherwise also pay for that memtable flush.
	_, err = source.http(1, "/checkpoint", map[string]any{})
	require.NoError(t, err)
	save := func(name string) (string, time.Duration) {
		file := filepath.Join(t.TempDir(), name)
		started := time.Now()
		_, err := snapshot.SaveWithVersion(ctx, zap.NewNop(), clientv3.Config{Endpoints: source.addresses[:1]}, file)
		require.NoError(t, err)
		return file, time.Since(started)
	}

	// Without other writes, the save adds a few response messages and checkpoint reads to RSS.
	linux := runtime.GOOS == "linux"
	var baseline int64
	if linux {
		baseline, _ = processMemory(t, pid)
		require.NoError(t, os.WriteFile(fmt.Sprintf("/proc/%d/clear_refs", pid), []byte("5"), 0))
	}
	file, elapsed := save("idle.snapshot")
	info, err := os.Stat(file)
	require.NoError(t, err)
	t.Logf("saved %d MiB in %s", info.Size()>>20, elapsed)
	require.Greater(t, info.Size(), int64(mib)<<20)
	if linux {
		_, peak := processMemory(t, pid)
		t.Logf("server RSS %d MiB before, peak %d MiB during the save", baseline>>20, peak>>20)
		require.Less(t, peak-baseline, int64(64)<<20)
	}

	// Writes continue during this save. The image holds the applied state at one boundary.
	var revisions sync.Map
	done := make(chan struct{})
	writer := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-done:
				writer <- nil
				return
			default:
			}
			response, err := putRetrying(ctx, api, fmt.Sprintf("concurrent/%06d", i), "during save")
			if err != nil {
				writer <- err
				return
			}
			revisions.Store(fmt.Sprintf("concurrent/%06d", i), response.Header.Revision)
		}
	}()
	file, _ = save("concurrent.snapshot")
	close(done)
	require.NoError(t, <-writer)

	started := time.Now()
	restored := newClusterConfigured(t, 1, func(c *cluster) { c.readyTimeout = 10 * time.Minute },
		"--restore-snapshot", file)
	t.Logf("restored %d MiB and elected a leader in %s", info.Size()>>20, time.Since(started))
	if linux {
		_, restorePeak := processMemory(t, restored.processes[1].Process.Pid)
		t.Logf("restored member peak RSS %d MiB", restorePeak>>20)
	}
	check := client(t, restored.addresses)
	result, err := check.Get(ctx, fmt.Sprintf("large/%06d", mib-1))
	require.NoError(t, err)
	require.Equal(t, last, result.Kvs[0].Value)
	boundary := result.Header.Revision
	count, err := check.Get(ctx, "large/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	require.NoError(t, err)
	require.Equal(t, int64(mib), count.Count)
	// The image holds exactly the writes at or below its revision.
	revisions.Range(func(key, revision any) bool {
		result, err := check.Get(ctx, key.(string))
		require.NoError(t, err)
		require.Equal(t, revision.(int64) <= boundary, len(result.Kvs) == 1, key)
		return true
	})
}
