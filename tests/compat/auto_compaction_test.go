package compat

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// startsWith reports whether a process accepts its flags: a rejected flag exits at once.
func startsWith(t *testing.T, binary string, args ...string) (bool, string) {
	t.Helper()
	cmd := exec.Command(binary, args...)
	var output strings.Builder
	cmd.Stdout = &output
	cmd.Stderr = &output
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
		return false, output.String()
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		return true, output.String()
	}
}

// The service accepts and rejects the same --auto-compaction-* values as etcd v3.6.5.
func TestAutoCompactionFlagsMatchEtcd(t *testing.T) {
	dir := t.TempDir()
	reference := filepath.Join(dir, "etcd")
	output, err := exec.Command("go", "build", "-mod=readonly", "-o", reference, "./cmd/reference-etcd").CombinedOutput()
	require.NoError(t, err, string(output))
	binary := os.Getenv("SLATEDB_ETCD_BINARY")
	if binary == "" {
		binary = "../../target/release/slatedb-etcd"
	}
	for _, tc := range []struct{ mode, retention string }{
		{"periodic", "0"},
		{"periodic", "1"},
		{"periodic", "30m"},
		{"periodic", "1h30m"},
		{"periodic", "1.5h"},
		{"periodic", "500ms"},
		{"periodic", "abc"},
		{"periodic", "1x"},
		{"periodic", "h"},
		{"revision", "0"},
		{"revision", "1000"},
		{"revision", "1us"},
		{"revision", "abc"},
		{"daily", "1"},
	} {
		t.Run(tc.mode+"="+tc.retention, func(t *testing.T) {
			flags := []string{"--auto-compaction-mode", tc.mode, "--auto-compaction-retention", tc.retention}
			api, peer := "http://"+address(t), "http://"+address(t)
			expected, refOutput := startsWith(t, reference, append([]string{"--data-dir", filepath.Join(t.TempDir(), "data"),
				"--name", "ref", "--listen-client-urls", api, "--advertise-client-urls", api,
				"--listen-peer-urls", peer, "--initial-advertise-peer-urls", peer,
				"--initial-cluster", "ref=" + peer, "--log-level", "error"}, flags...)...)
			listen := address(t)
			actual, ourOutput := startsWith(t, binary, append([]string{"--id", "1", "--listen", listen,
				"--peer", "1=" + listen, "--dir", filepath.Join(t.TempDir(), "node")}, flags...)...)
			require.Equal(t, expected, actual, "etcd: %s\nservice: %s", refOutput, ourOutput)
		})
	}
}

func compactRevision(t *testing.T, cli *clientv3.Client, revision int64) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := cli.Get(ctx, "auto", clientv3.WithRev(revision))
	// Other errors, such as an election in progress, mean "not yet"; callers retry.
	return errors.Is(err, rpctypes.ErrCompacted)
}

// Periodic retention compacts history from the leader. After the leader stops, the new leader
// keeps compacting with its own revision samples.
func TestAutoCompactionPeriodicAcrossLeaderChange(t *testing.T) {
	c := newCluster(t, "--auto-compaction-mode=periodic", "--auto-compaction-retention=1s")
	cli := client(t, c.addresses)
	// Retry while a new leader is elected.
	put := func() int64 {
		var err error
		for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			var response *clientv3.PutResponse
			response, err = cli.Put(ctx, "auto", "value")
			cancel()
			if err == nil {
				return response.Header.Revision
			}
			time.Sleep(100 * time.Millisecond)
		}
		require.NoError(t, err)
		return 0
	}
	first := put()
	deadline := time.Now().Add(15 * time.Second)
	for !compactRevision(t, cli, first) {
		require.True(t, time.Now().Before(deadline), "revision %d was not compacted", first)
		put()
		time.Sleep(50 * time.Millisecond)
	}
	// The newest revision stays readable: compaction keeps one second of history.
	latest := put()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := cli.Get(ctx, "auto", clientv3.WithRev(latest))
	cancel()
	require.NoError(t, err)

	leader := c.leader()
	c.stop(leader)
	var others []string
	for id, address := range c.addresses {
		if id+1 != leader {
			others = append(others, address)
		}
	}
	cli = client(t, others)
	second := put()
	deadline = time.Now().Add(20 * time.Second)
	for !compactRevision(t, cli, second) {
		require.True(t, time.Now().Before(deadline), "the new leader did not compact %d", second)
		put()
		time.Sleep(50 * time.Millisecond)
	}
	// The stopped member catches up to the same boundary after it restarts.
	c.start(leader)
	restarted := client(t, []string{c.addresses[leader-1]})
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := restarted.Get(ctx, "auto", clientv3.WithRev(second), clientv3.WithSerializable())
		return errors.Is(err, rpctypes.ErrCompacted)
	}, 20*time.Second, 100*time.Millisecond)
}

// The quota gauges split logical bytes by row kind, and the early warning reports the threshold.
func TestQuotaWarningAndUsageGauges(t *testing.T) {
	c := newClusterSize(t, 1, "--quota-backend-bytes=262144", "--quota-warning-fraction=0.5")
	cli := client(t, c.addresses)
	gauge := func(m map[string]*dto.MetricFamily, name string) float64 {
		require.NotNil(t, m[name], name)
		return m[name].Metric[0].GetGauge().GetValue()
	}
	m := scrapeMetrics(t, c.addresses[0])
	require.Equal(t, float64(131072), gauge(m, "slatedb_etcd_quota_warning_bytes"))
	require.Equal(t, float64(0), gauge(m, "slatedb_etcd_quota_warning"))
	value := strings.Repeat("v", 8192)
	for i := 0; ; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := cli.Put(ctx, "quota", value)
		cancel()
		require.NoError(t, err)
		m = scrapeMetrics(t, c.addresses[0])
		if gauge(m, "slatedb_etcd_quota_warning") == 1 {
			break
		}
		require.Less(t, i, 40, "the warning gauge did not rise")
	}
	kinds := map[string]float64{}
	for _, metric := range m["slatedb_etcd_logical_size_bytes"].Metric {
		kinds[metric.Label[0].GetValue()] = metric.GetGauge().GetValue()
	}
	require.Len(t, kinds, 4)
	total := kinds["current"] + kinds["history"] + kinds["watch_journal"] + kinds["other"]
	require.Equal(t, gauge(m, "etcd_mvcc_db_total_size_in_use_in_bytes"), total)
	// One current row and many history versions of the same key.
	require.Greater(t, kinds["history"], kinds["current"])
	require.Greater(t, kinds["current"], float64(8192))
	require.Eventually(t, func() bool {
		data, _ := os.ReadFile(filepath.Join(c.root, "node-1.log"))
		return strings.Contains(string(data), "Quota usage reached the warning threshold")
	}, 5*time.Second, 100*time.Millisecond)
}
