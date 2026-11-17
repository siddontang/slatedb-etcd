//go:build s3

package compat

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type s3Proxy struct {
	mu      sync.Mutex
	mode    string
	member  int
	entered chan struct{}
	release chan struct{}
	target  *url.URL
}

func (p *s3Proxy) arm(mode string, member int) (<-chan struct{}, chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mode, p.member = mode, member
	p.entered, p.release = make(chan struct{}, 1), make(chan struct{})
	return p.entered, p.release
}

func (p *s3Proxy) clear() { p.mu.Lock(); p.mode = ""; p.mu.Unlock() }

func (p *s3Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	mode, entered, release := p.mode, p.entered, p.release
	matched := strings.Contains(r.URL.Path, fmt.Sprintf("/member-%d/", p.member))
	if mode == "get" {
		matched = matched && r.Method == "GET"
	} else {
		matched = matched && r.Method == "PUT" && strings.HasSuffix(r.URL.Path, ".sst")
	}
	if matched && (mode == "pause" || mode == "lost-reply") {
		p.mode = ""
	}
	p.mu.Unlock()
	if matched && mode != "" {
		select {
		case entered <- struct{}{}:
		default:
		}
		switch mode {
		case "pause":
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		case "put", "get":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, "<Error><Code>ServiceUnavailable</Code></Error>")
			return
		}
	}
	request := r.Clone(r.Context())
	request.RequestURI = ""
	request.URL.Scheme, request.URL.Host = p.target.Scheme, p.target.Host
	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil {
		http.Error(w, "backend unavailable", 502)
		return
	}
	defer response.Body.Close()
	if matched && mode == "lost-reply" && response.StatusCode/100 == 2 {
		io.Copy(io.Discard, response.Body)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(503)
		fmt.Fprint(w, "<Error><Code>ServiceUnavailable</Code></Error>")
		return
	}
	for key, values := range response.Header {
		w.Header()[key] = values
	}
	w.WriteHeader(response.StatusCode)
	io.Copy(w, response.Body)
}

func waitS3Fault(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("S3 fault was not reached")
	}
}

func TestS3Recovery(t *testing.T) {
	endpoint, bucket := os.Getenv("SLATEDB_S3_TEST_ENDPOINT"), os.Getenv("SLATEDB_S3_TEST_BUCKET")
	require.NotEmpty(t, endpoint, "run scripts/check-s3.py")
	require.NotEmpty(t, bucket)
	target, err := url.Parse(endpoint)
	require.NoError(t, err)
	proxy := &s3Proxy{target: target}
	server := httptest.NewServer(proxy)
	defer server.Close()
	c := newCluster(t, "--s3-bucket", bucket, "--s3-endpoint", server.URL, "--s3-allow-http")
	leader := c.leader()
	follower := leader%3 + 1
	api := client(t, []string{c.addresses[leader-1]})
	local := client(t, []string{c.addresses[follower-1]})
	write := func(key, value string) {
		t.Helper()
		mustCall(t, api, clientv3.OpPut(key, value))
		mustCall(t, local, clientv3.OpGet(key))
	}
	check := func() {
		t.Helper()
		for _, address := range c.addresses {
			result := mustCall(t, client(t, []string{address}), clientv3.OpGet("anchor")).(*pb.RangeResponse)
			require.Len(t, result.Kvs, 1)
			require.Equal(t, "acknowledged", string(result.Kvs[0].Value))
		}
	}
	write("anchor", "acknowledged")
	t.Run("delayed-put-keeps-raft-active", func(t *testing.T) {
		before, err := c.http(follower, "/status", nil)
		require.NoError(t, err)
		entered, release := proxy.arm("pause", follower)
		done := make(chan error, 1)
		go func() { _, err := c.http(follower, "/checkpoint", map[string]any{}); done <- err }()
		waitS3Fault(t, entered)
		write("during-upload", "committed")
		current, err := c.http(follower, "/status", nil)
		require.NoError(t, err)
		require.Equal(t, before["snapshot_index"], current["snapshot_index"])
		require.Equal(t, before["first_log_index"], current["first_log_index"])
		close(release)
		require.NoError(t, <-done)
		check()
	})
	t.Run("lost-put-response", func(t *testing.T) {
		write("lost-reply", "committed")
		entered, _ := proxy.arm("lost-reply", follower)
		_, err := c.http(follower, "/checkpoint", map[string]any{})
		require.NoError(t, err)
		waitS3Fault(t, entered)
		c.stop(follower)
		c.start(follower)
		c.waitReady()
		check()
	})
	t.Run("outage-crash-and-recovery", func(t *testing.T) {
		write("outage", "committed")
		before, err := c.http(follower, "/status", nil)
		require.NoError(t, err)
		entered, _ := proxy.arm("put", follower)
		done := make(chan error, 1)
		go func() { _, err := c.http(follower, "/checkpoint", map[string]any{}); done <- err }()
		waitS3Fault(t, entered)
		write("during-outage", "committed")
		current, err := c.http(follower, "/status", nil)
		require.NoError(t, err)
		require.Equal(t, before["snapshot_index"], current["snapshot_index"])
		require.Equal(t, before["first_log_index"], current["first_log_index"])
		c.stop(follower)
		require.Error(t, <-done)
		proxy.clear()
		c.start(follower)
		c.waitReady()
		check()
		result := mustCall(t, local, clientv3.OpGet("during-outage")).(*pb.RangeResponse)
		require.Len(t, result.Kvs, 1)
		require.Equal(t, "committed", string(result.Kvs[0].Value))
	})
	t.Run("read-outage-at-restart", func(t *testing.T) {
		c.stop(follower)
		entered, _ := proxy.arm("get", follower)
		c.start(follower)
		waitS3Fault(t, entered)
		// A failed recovery read must not create an empty replacement state.
		c.stop(follower)
		proxy.clear()
		c.start(follower)
		c.waitReady()
		check()
	})
	t.Run("snapshot-transfer-and-full-restart", func(t *testing.T) {
		c.stop(follower)
		for i := 0; i < 8; i++ {
			mustCall(t, api, clientv3.OpPut("bulk", fmt.Sprint(i)))
		}
		for id := range c.processes {
			_, err := c.http(id, "/checkpoint", map[string]any{})
			require.NoError(t, err)
		}
		c.start(follower)
		require.Eventually(t, func() bool {
			state, err := c.http(follower, "/status", nil)
			return err == nil && state["installed_snapshots"].(float64) > 0
		}, 15*time.Second, 50*time.Millisecond)
		check()
		for id := 1; id <= 3; id++ {
			c.stop(id)
		}
		for id := 1; id <= 3; id++ {
			c.start(id)
		}
		c.waitReady()
		check()
	})
}
