package compat

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type metricShape struct {
	Type      string
	Labels    []string
	Buckets   []float64
	Coverage  string
	Condition string
}

func scrapeMetrics(t *testing.T, address string) map[string]*dto.MetricFamily {
	t.Helper()
	if !strings.Contains(address, "://") {
		address = "http://" + address
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(address + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "text/plain")
	var reader expfmt.TextParser
	families, err := reader.TextToMetricFamilies(resp.Body)
	require.NoError(t, err)
	return families
}

func metricShapeOf(t *testing.T, f *dto.MetricFamily) metricShape {
	t.Helper()
	require.NotNil(t, f)
	require.NotEmpty(t, f.Metric)
	shape := metricShape{Type: strings.ToLower(f.GetType().String()), Labels: []string{}}
	for _, label := range f.Metric[0].Label {
		shape.Labels = append(shape.Labels, label.GetName())
	}
	sort.Strings(shape.Labels)
	for _, bucket := range f.Metric[0].GetHistogram().GetBucket() {
		if !math.IsInf(bucket.GetUpperBound(), 1) {
			shape.Buckets = append(shape.Buckets, bucket.GetUpperBound())
		}
	}
	return shape
}

// Upstream gRPC metrics use a process registry. Start a fresh reference process for each test.
func metricsReference(t *testing.T, options ...string) *clientv3.Client {
	t.Helper()
	dir := t.TempDir()
	binary := filepath.Join(dir, "etcd")
	output, err := exec.Command("go", "build", "-mod=readonly", "-o", binary, "./cmd/reference-etcd").CombinedOutput()
	require.NoError(t, err, string(output))
	api, peer := "http://"+address(t), "http://"+address(t)
	args := []string{"--data-dir", filepath.Join(dir, "data"), "--name", "ref", "--listen-client-urls", api,
		"--advertise-client-urls", api, "--listen-peer-urls", peer, "--initial-advertise-peer-urls", peer,
		"--initial-cluster", "ref=" + peer, "--log-level", "error"}
	cmd := exec.Command(binary, append(args, options...)...)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	cli := client(t, []string{api})
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		result, err := cli.Status(ctx, api)
		return err == nil && result.Leader != 0
	}, 12*time.Second, 50*time.Millisecond)
	return cli
}

func exerciseMetrics(t *testing.T, cli *clientv3.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	before := scrapeMetrics(t, cli.Endpoints()[0])
	stream := cli.Watch(ctx, "metric-key", clientv3.WithCreatedNotify())
	require.True(t, (<-stream).Created)
	p, err := cli.Put(ctx, "metric-key", "value")
	require.NoError(t, err)
	event := <-stream
	require.Len(t, event.Events, 1)
	_, err = cli.Get(ctx, "metric-key")
	require.NoError(t, err)
	lease, err := cli.Grant(ctx, 12)
	require.NoError(t, err)
	_, err = cli.Revoke(ctx, lease.ID)
	require.NoError(t, err)
	_, err = cli.Compact(ctx, p.Header.Revision)
	require.NoError(t, err)
	_, err = cli.Defragment(ctx, cli.Endpoints()[0])
	require.NoError(t, err)
	after := scrapeMetrics(t, cli.Endpoints()[0])
	for _, name := range []string{"etcd_debugging_lease_granted_total", "etcd_debugging_lease_revoked_total", "etcd_debugging_mvcc_events_total", "etcd_network_client_grpc_received_bytes_total", "etcd_network_client_grpc_sent_bytes_total"} {
		require.Greater(t, after[name].Metric[0].GetCounter().GetValue(), before[name].Metric[0].GetCounter().GetValue(), name)
	}
	for _, name := range []string{"etcd_debugging_mvcc_watch_stream_total", "etcd_debugging_mvcc_watcher_total"} {
		require.Greater(t, after[name].Metric[0].GetGauge().GetValue(), before[name].Metric[0].GetGauge().GetValue(), name)
	}
	for _, name := range []string{"etcd_disk_wal_fsync_duration_seconds", "etcd_disk_backend_defrag_duration_seconds", "etcd_debugging_lease_ttl_total"} {
		require.Greater(t, after[name].Metric[0].GetHistogram().GetSampleCount(), before[name].Metric[0].GetHistogram().GetSampleCount(), name)
	}
	require.Equal(t, float64(p.Header.Revision), after["etcd_debugging_mvcc_current_revision"].Metric[0].GetGauge().GetValue())
}

func TestMetricsMatchEtcd(t *testing.T) {
	extra := "http://" + address(t)
	c := newClusterSize(t, 1, "--listen-metrics-urls", extra)
	api := client(t, c.addresses)
	ref := metricsReference(t)
	exerciseMetrics(t, ref)
	exerciseMetrics(t, api)
	expected := scrapeMetrics(t, ref.Endpoints()[0])
	actual := scrapeMetrics(t, extra)
	data, err := os.ReadFile("testdata/metrics-v3.6.5.json")
	require.NoError(t, err)
	var inventory map[string]metricShape
	require.NoError(t, json.Unmarshal(data, &inventory))
	for name, shape := range inventory {
		if shape.Coverage != "exported" || shape.Condition != "" || name == "etcd_network_active_peers" ||
			name == "etcd_network_peer_sent_bytes_total" || name == "etcd_network_peer_received_bytes_total" {
			continue
		}
		shape.Coverage = ""
		require.Equal(t, shape, metricShapeOf(t, actual[name]), name)
		require.Equal(t, shape, metricShapeOf(t, expected[name]), name)
	}
	if runtime.GOOS == "linux" {
		rss := actual["process_resident_memory_bytes"]
		require.Equal(t, "gauge", metricShapeOf(t, rss).Type)
		require.Greater(t, rss.Metric[0].GetGauge().GetValue(), float64(0))
	}
	require.Nil(t, actual["grpc_server_handling_seconds"])
	require.Nil(t, expected["grpc_server_handling_seconds"])
	for name := range actual {
		if strings.HasPrefix(name, "go_") {
			t.Fatalf("unexpected Go runtime metric %s", name)
		}
	}
	resp, err := http.Get(extra + "/status")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestMetricsFollowLeaderChanges(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	for i, addr := range c.addresses {
		metrics := scrapeMetrics(t, addr)
		isLeader := float64(0)
		if i+1 == leader {
			isLeader = 1
		}
		require.Equal(t, isLeader, metrics["etcd_server_is_leader"].Metric[0].GetGauge().GetValue())
		require.Equal(t, 1.0, metrics["etcd_server_has_leader"].Metric[0].GetGauge().GetValue())
		require.Len(t, metrics["etcd_network_known_peers"].Metric, 3)
		require.Len(t, metrics["etcd_network_active_peers"].Metric, 2)
	}
	require.Eventually(t, func() bool {
		m := scrapeMetrics(t, c.addresses[leader-1])
		for name, label := range map[string]string{"etcd_network_peer_sent_bytes_total": "To", "etcd_network_peer_received_bytes_total": "From"} {
			family := m[name]
			require.NotNil(t, family)
			for _, peer := range family.Metric {
				require.Equal(t, label, peer.Label[0].GetName())
				if peer.GetCounter().GetValue() == 0 {
					return false
				}
			}
			if len(family.Metric) != 2 {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond)
	c.stop(leader)
	replacement := c.leader()
	metrics := scrapeMetrics(t, c.addresses[replacement-1])
	require.GreaterOrEqual(t, metrics["etcd_server_leader_changes_seen_total"].Metric[0].GetCounter().GetValue(), 2.0)
	require.Equal(t, 1.0, metrics["etcd_server_is_leader"].Metric[0].GetGauge().GetValue())
}

func TestMetricsUseClientTLS(t *testing.T) {
	extra := "https://" + address(t)
	c := newClusterConfigured(t, 1, func(c *cluster) { configureTLS(c, true, true, true) },
		"--listen-metrics-urls", extra)
	trusted := &http.Client{Transport: &http.Transport{TLSClientConfig: c.tlsClient}, Timeout: 4 * time.Second}
	for _, addr := range []string{c.addresses[0], extra} {
		resp, err := trusted.Get(addr + "/metrics")
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		missingCert := c.tlsClient.Clone()
		missingCert.Certificates = nil
		noCert := &http.Client{Transport: &http.Transport{TLSClientConfig: missingCert}, Timeout: 4 * time.Second}
		_, err = noCert.Get(addr + "/metrics")
		require.Error(t, err)
	}
}

func rpcCount(t *testing.T, family *dto.MetricFamily, code string) float64 {
	t.Helper()
	for _, sample := range family.Metric {
		labels := map[string]string{}
		for _, label := range sample.Label {
			labels[label.GetName()] = label.GetValue()
		}
		if labels["grpc_service"] == "etcdserverpb.KV" && labels["grpc_method"] == "Put" &&
			labels["grpc_type"] == "unary" && labels["grpc_code"] == code {
			return sample.GetCounter().GetValue()
		}
	}
	t.Fatal("missing Put counter for " + code)
	return 0
}

func TestMetricsCountAppliedWrites(t *testing.T) {
	c := newClusterSize(t, 1)
	for _, cli := range []*clientv3.Client{metricsReference(t), client(t, c.addresses)} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		baseline := scrapeMetrics(t, cli.Endpoints()[0])
		lease, err := cli.Grant(ctx, 20)
		require.NoError(t, err)
		_, err = cli.Txn(ctx).Then(clientv3.OpPut("a", "11"), clientv3.OpPut("b", "22", clientv3.WithLease(lease.ID))).Commit()
		require.NoError(t, err)
		_, err = cli.Put(ctx, "a", "333", clientv3.WithPrevKV())
		require.NoError(t, err)
		_, err = cli.Delete(ctx, "missing")
		require.NoError(t, err)
		_, err = cli.Revoke(ctx, lease.ID)
		require.NoError(t, err)
		_, err = cli.Put(ctx, "", "invalid")
		require.Error(t, err)
		after := scrapeMetrics(t, cli.Endpoints()[0])
		for _, code := range []string{"OK", "InvalidArgument"} {
			const handled = "grpc_server_handled_total"
			require.Equal(t, 1.0, rpcCount(t, after[handled], code)-rpcCount(t, baseline[handled], code), code)
		}
		for name, delta := range map[string]float64{"etcd_mvcc_put_total": 3, "etcd_mvcc_delete_total": 2} {
			require.Equal(t, delta, after[name].Metric[0].GetCounter().GetValue()-baseline[name].Metric[0].GetCounter().GetValue(), name)
		}
		const bytes = "etcd_debugging_mvcc_total_put_size_in_bytes"
		require.Equal(t, 10.0, after[bytes].Metric[0].GetGauge().GetValue()-baseline[bytes].Metric[0].GetGauge().GetValue())
		cancel()
	}
}

func alertMetricShape(t *testing.T, m map[string]*dto.MetricFamily, name string) {
	t.Helper()
	data, err := os.ReadFile("testdata/metrics-v3.6.5.json")
	require.NoError(t, err)
	var inventory map[string]metricShape
	require.NoError(t, json.Unmarshal(data, &inventory))
	expected := inventory[name]
	require.NotEmpty(t, expected.Condition)
	expected.Coverage, expected.Condition = "", ""
	require.Equal(t, expected, metricShapeOf(t, m[name]), name)
}

func TestMetricsExtensiveMatchesEtcd(t *testing.T) {
	c := newClusterSize(t, 1, "--metrics=extensive")
	for _, cli := range []*clientv3.Client{metricsReference(t, "--metrics=extensive"), client(t, c.addresses)} {
		const name = "grpc_server_handling_seconds"
		putCount := func() uint64 {
			m := scrapeMetrics(t, cli.Endpoints()[0])
			alertMetricShape(t, m, name)
			for _, row := range m[name].Metric {
				for _, l := range row.Label {
					if l.GetName() == "grpc_method" && l.GetValue() == "Put" {
						return row.GetHistogram().GetSampleCount()
					}
				}
			}
			t.Fatal("missing Put duration")
			return 0
		}
		before := putCount()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := cli.Put(ctx, "timed", "value")
		require.NoError(t, err)
		_, err = cli.Put(ctx, "", "invalid")
		require.Error(t, err)
		cancel()
		require.Equal(t, before+2, putCount())
	}
}

func TestMetricsReportPeerFailure(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	follower := leader%3 + 1
	target := strconv.FormatInt(int64(follower), 16)
	addr := c.addresses[leader-1]
	const rtt = "etcd_network_peer_round_trip_time_seconds"
	const failed = "etcd_network_peer_sent_failures_total"
	const disconnected = "etcd_network_disconnected_peers_total"
	metric := func(m map[string]*dto.MetricFamily, name, label string) *dto.Metric {
		for _, row := range m[name].GetMetric() {
			for _, v := range row.Label {
				if v.GetName() == label && v.GetValue() == target {
					return row
				}
			}
		}
		return nil
	}
	require.Eventually(t, func() bool {
		row := metric(scrapeMetrics(t, addr), rtt, "To")
		return row.GetHistogram().GetSampleCount() > 0 && row.GetHistogram().GetSampleSum() > 0
	}, 10*time.Second, 50*time.Millisecond)
	before := scrapeMetrics(t, addr)
	failures := metric(before, failed, "To").GetCounter().GetValue()
	losses := metric(before, disconnected, "Remote").GetCounter().GetValue()
	c.stop(follower)
	require.Eventually(t, func() bool {
		m := scrapeMetrics(t, addr)
		return metric(m, failed, "To").GetCounter().GetValue() > failures &&
			metric(m, disconnected, "Remote").GetCounter().GetValue() > losses
	}, 10*time.Second, 50*time.Millisecond)
	after := scrapeMetrics(t, addr)
	for _, name := range []string{rtt, failed, disconnected} {
		alertMetricShape(t, after, name)
	}
}
