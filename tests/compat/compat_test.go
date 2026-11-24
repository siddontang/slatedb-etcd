package compat

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc/status"
)

type cluster struct {
	reserved      map[string]net.Listener
	peerAddresses []string
	tlsClient     *tls.Config
	httpClient    *http.Client
	snapshotEvery uint64
	t             *testing.T
	root, binary  string
	addresses     []string
	processes     map[int]*exec.Cmd
	logs          map[int]*os.File
	args          []string
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func address(t *testing.T) string {
	t.Helper()
	listener := listenLocal(t)
	defer listener.Close()
	return listener.Addr().String()
}

func (c *cluster) reserveAddress() string {
	c.t.Helper()
	listener := listenLocal(c.t)
	addr := listener.Addr().String()
	c.reserved[addr] = listener
	return addr
}

func (c *cluster) releaseAddress(endpoint string) {
	addr := endpointHost(endpoint)
	if listener := c.reserved[addr]; listener != nil {
		if err := listener.Close(); err != nil {
			c.t.Fatal(err)
		}
		delete(c.reserved, addr)
	}
}

func newCluster(t *testing.T, extraArgs ...string) *cluster {
	return newClusterSize(t, 3, extraArgs...)
}

func newClusterSize(t *testing.T, size int, extraArgs ...string) *cluster {
	return newClusterConfigured(t, size, nil, extraArgs...)
}

func newClusterConfigured(t *testing.T, size int, configure func(*cluster), extraArgs ...string) *cluster {
	t.Helper()
	binary := os.Getenv("SLATEDB_ETCD_BINARY")
	if binary == "" {
		binary = "../../target/release/slatedb-etcd"
	}
	binary, err := filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(binary); err != nil {
		t.Fatal("Build the service first:", err)
	}
	root, err := os.MkdirTemp("", "slatedb-etcd-compat-")
	if err != nil {
		t.Fatal(err)
	}
	c := &cluster{t: t, root: root, binary: binary, args: extraArgs, processes: map[int]*exec.Cmd{}, logs: map[int]*os.File{}}
	c.reserved = make(map[string]net.Listener)
	t.Cleanup(func() {
		for _, listener := range c.reserved {
			listener.Close()
		}
	})
	for i := 0; i < size; i++ {
		c.addresses = append(c.addresses, c.reserveAddress())
	}
	if configure != nil {
		configure(c)
	}
	t.Log("Process logs and data:", root)
	t.Cleanup(func() {
		for id := range c.processes {
			c.stop(id)
		}
		if t.Failed() {
			for i := 1; i <= size; i++ {
				data, _ := os.ReadFile(filepath.Join(root, fmt.Sprintf("node-%d.log", i)))
				if len(data) > 4000 {
					data = data[len(data)-4000:]
				}
				t.Logf("node %d: %s", i, data)
			}
		} else if os.Getenv("SLATEDB_KEEP_DATA") != "1" && t.Name() != "TestRobustness" {
			os.RemoveAll(root)
		}
	})
	for i := 1; i <= size; i++ {
		c.start(i)
	}
	c.waitReady()
	return c
}

func (c *cluster) start(id int) {
	c.t.Helper()
	args := []string{"--id", strconv.Itoa(id), "--listen", endpointHost(c.addresses[id-1]), "--dir", filepath.Join(c.root, fmt.Sprintf("node-%d", id)), "--cluster-id", filepath.Base(c.root), "--snapshot-every", fmt.Sprint(c.snapshotEvery)}
	args = append(args, c.args...)
	peers := c.addresses
	if len(c.peerAddresses) != 0 {
		peers = c.peerAddresses
		args = append(args, "--peer-listen", endpointHost(peers[id-1]))
		for i, addr := range c.addresses {
			args = append(args, "--client-peer", fmt.Sprintf("%d=%s", i+1, addr))
		}
	}
	for i, addr := range peers {
		args = append(args, "--peer", fmt.Sprintf("%d=%s", i+1, addr))
	}
	file, err := os.OpenFile(filepath.Join(c.root, fmt.Sprintf("node-%d.log", id)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		c.t.Fatal(err)
	}
	cmd := exec.Command(c.binary, args...)
	cmd.Env = append(os.Environ(), "RUST_LOG=slatedb_etcd=info,slatedb=warn",
		"SLATEDB_FAILPOINT_FILE="+filepath.Join(c.root, fmt.Sprintf("fault-%d", id)))
	cmd.Stdout = file
	cmd.Stderr = file
	// Keep both ports reserved until this member can bind them.
	c.releaseAddress(c.addresses[id-1])
	c.releaseAddress(peers[id-1])
	if err := cmd.Start(); err != nil {
		file.Close()
		c.t.Fatal(err)
	}
	c.processes[id] = cmd
	c.logs[id] = file
}

func (c *cluster) stop(id int) {
	if cmd := c.processes[id]; cmd != nil {
		cmd.Process.Kill()
		cmd.Wait()
		c.logs[id].Close()
		delete(c.processes, id)
		delete(c.logs, id)
	}
}

func (c *cluster) http(id int, path string, body any) (map[string]any, error) {
	address := c.addresses[id-1]
	if len(c.peerAddresses) != 0 {
		address = c.peerAddresses[id-1]
	}
	if !strings.Contains(address, "://") {
		address = "http://" + address
	}
	var req *http.Request
	var err error
	if body == nil {
		req, err = http.NewRequest("GET", address+path, nil)
	} else {
		data, _ := json.Marshal(body)
		req, err = http.NewRequest("POST", address+path, bytes.NewReader(data))
		req.Header.Set("Content-Type", "application/json")
	}
	if err != nil {
		return nil, err
	}
	httpClient := c.httpClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 12 * time.Second}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result map[string]any
	err = json.NewDecoder(resp.Body).Decode(&result)
	if resp.StatusCode != 200 {
		return result, fmt.Errorf("HTTP %d: %v", resp.StatusCode, result)
	}
	return result, err
}

func (c *cluster) leader() int {
	c.t.Helper()
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		for id := range c.processes {
			s, err := c.http(id, "/status", nil)
			if err == nil && s["role"] == "Leader" {
				return id
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("No leader elected")
	return 0
}

func (c *cluster) waitReady() {
	c.t.Helper()
	// Initial clients can select any member. Each member must know the elected leader.
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		var leader, term any
		ready, sawLeader := true, false
		for id := range c.processes {
			s, err := c.http(id, "/status", nil)
			if err != nil || s["id"] != float64(id) || s["cluster_id"] != filepath.Base(c.root) || s["leader_id"] == float64(0) {
				ready = false
				break
			}
			if leader == nil {
				leader, term = s["leader_id"], s["term"]
			}
			if s["leader_id"] != leader || s["term"] != term {
				ready = false
				break
			}
			sawLeader = sawLeader || (leader == float64(id) && s["role"] == "Leader")
		}
		if ready && sawLeader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("Initial members did not agree on a leader")
}

func client(t *testing.T, addresses []string) *clientv3.Client {
	t.Helper()
	c, err := clientv3.New(clientv3.Config{Endpoints: addresses, DialTimeout: 3 * time.Second, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func reference(t *testing.T) *clientv3.Client {
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.LogLevel = "error"
	peer, _ := url.Parse("http://" + address(t))
	api, _ := url.Parse("http://" + address(t))
	cfg.ListenPeerUrls = []url.URL{*peer}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cfg.ListenClientUrls = []url.URL{*api}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		t.Fatal("Reference etcd startup timed out")
	}
	return client(t, []string{api.String()})
}

func call(c *clientv3.Client, op clientv3.Op) (proto.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.Do(ctx, op)
	if err != nil {
		return nil, err
	}
	switch {
	case r.Get() != nil:
		return (*pb.RangeResponse)(r.Get()), nil
	case r.Put() != nil:
		return (*pb.PutResponse)(r.Put()), nil
	case r.Del() != nil:
		return (*pb.DeleteRangeResponse)(r.Del()), nil
	default:
		return (*pb.TxnResponse)(r.Txn()), nil
	}
}

func responseHeader(m proto.Message) *pb.ResponseHeader {
	switch r := m.(type) {
	case *pb.RangeResponse:
		return r.Header
	case *pb.PutResponse:
		return r.Header
	case *pb.DeleteRangeResponse:
		return r.Header
	case *pb.TxnResponse:
		return r.Header
	case *pb.CompactionResponse:
		return r.Header
	default:
		panic("unknown response")
	}
}

func normalize(m proto.Message) {
	h := responseHeader(m)
	if h != nil {
		h.ClusterId = 0
		h.MemberId = 0
		h.RaftTerm = 0
	}
	if txn, ok := m.(*pb.TxnResponse); ok {
		for _, op := range txn.Responses {
			switch r := op.Response.(type) {
			case *pb.ResponseOp_ResponseRange:
				normalize(r.ResponseRange)
			case *pb.ResponseOp_ResponsePut:
				normalize(r.ResponsePut)
			case *pb.ResponseOp_ResponseDeleteRange:
				normalize(r.ResponseDeleteRange)
			case *pb.ResponseOp_ResponseTxn:
				normalize(r.ResponseTxn)
			}
		}
	}
}

func TestDifferentialKV(t *testing.T) {
	cluster := newCluster(t)
	actual := client(t, []string{cluster.addresses[cluster.leader()-1]})
	expected := reference(t)
	check := func(name string, op clientv3.Op) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			a, ae := call(actual, op)
			b, be := call(expected, op)
			if ae != nil || be != nil {
				if status.Code(ae) != status.Code(be) || status.Convert(ae).Message() != status.Convert(be).Message() {
					t.Fatalf("Errors differ: service=%v; etcd=%v", ae, be)
				}
				return
			}
			h := responseHeader(a)
			if h == nil || h.ClusterId == 0 || h.MemberId == 0 || h.RaftTerm == 0 {
				t.Fatalf("Missing response identity: %v", h)
			}
			normalize(a)
			normalize(b)
			if !proto.Equal(a, b) {
				t.Fatalf("Responses differ:\nservice: %s\netcd:    %s", a, b)
			}
		})
		if t.Failed() {
			t.FailNow()
		}
	}
	check("initial_revision", clientv3.OpGet("missing"))
	check("create", clientv3.OpPut("key", "first", clientv3.WithPrevKV()))
	check("same_value_increments_version", clientv3.OpPut("key", "first", clientv3.WithPrevKV()))
	check("update_prev_kv", clientv3.OpPut("key", "second", clientv3.WithPrevKV()))
	check("history", clientv3.OpGet("key", clientv3.WithRev(2)))
	check("negative_revision_reads_latest", clientv3.OpGet("key", clientv3.WithRev(-1)))
	check("future_revision", clientv3.OpGet("key", clientv3.WithRev(999999)))
	check("binary_put", clientv3.OpPut("\x00\xff", "\x00\xff\x80"))
	check("binary_get", clientv3.OpGet("\x00\xff"))
	check("empty_value", clientv3.OpPut("empty", ""))
	check("prefix_a", clientv3.OpPut("p/a", "z"))
	check("prefix_b", clientv3.OpPut("p/b", "a"))
	check("prefix_c", clientv3.OpPut("p/c", "m"))
	check("prefix_limit_more", clientv3.OpGet("p/", clientv3.WithPrefix(), clientv3.WithLimit(2)))
	check("sort_value", clientv3.OpGet("p/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByValue, clientv3.SortAscend)))
	check("sort_key_desc", clientv3.OpGet("p/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend)))
	check("sort_mod", clientv3.OpGet("p/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend)))
	check("keys_only", clientv3.OpGet("p/", clientv3.WithPrefix(), clientv3.WithKeysOnly()))
	check("count_only", clientv3.OpGet("p/", clientv3.WithPrefix(), clientv3.WithCountOnly(), clientv3.WithLimit(1)))
	check("revision_filter_count", clientv3.OpGet("p/", clientv3.WithPrefix(), clientv3.WithMinModRev(9)))
	check("create_revision_filters", clientv3.OpGet("p/", clientv3.WithPrefix(), clientv3.WithMinCreateRev(8), clientv3.WithMaxCreateRev(9)))
	check("all_keys", clientv3.OpGet("\x00", clientv3.WithFromKey()))
	check("history_prefix", clientv3.OpGet("p/", clientv3.WithPrefix(), clientv3.WithRev(8)))
	check("history_sort_limit", clientv3.OpGet("p/", clientv3.WithPrefix(), clientv3.WithRev(9), clientv3.WithSort(clientv3.SortByValue, clientv3.SortDescend), clientv3.WithLimit(2)))
	check("history_from_key_keys_only", clientv3.OpGet("\x00", clientv3.WithFromKey(), clientv3.WithRev(8), clientv3.WithKeysOnly(), clientv3.WithLimit(3)))
	check("history_filter_count", clientv3.OpGet("p/", clientv3.WithPrefix(), clientv3.WithRev(9), clientv3.WithMinModRev(8), clientv3.WithCountOnly()))
	check("range_end", clientv3.OpGet("p/a", clientv3.WithRange("p/c")))
	check("ignore_value", clientv3.OpPut("key", "", clientv3.WithIgnoreValue(), clientv3.WithPrevKV()))
	check("ignore_lease", clientv3.OpPut("key", "third", clientv3.WithIgnoreLease()))
	check("missing_ignore_value", clientv3.OpPut("missing", "", clientv3.WithIgnoreValue()))
	check("invalid_ignore_value", clientv3.OpPut("key", "bad", clientv3.WithIgnoreValue()))
	check("missing_lease", clientv3.OpPut("key", "bad", clientv3.WithLease(123)))
	check("invalid_ignore_lease", clientv3.OpPut("key", "bad", clientv3.WithIgnoreLease(), clientv3.WithLease(123)))
	check("txn_one_revision", clientv3.OpTxn([]clientv3.Cmp{clientv3.Compare(clientv3.Version("new/a"), "=", 0)}, []clientv3.Op{
		clientv3.OpPut("new/a", "one"), clientv3.OpPut("new/b", "two"), clientv3.OpGet("new/", clientv3.WithPrefix()),
	}, nil))
	check("txn_else", clientv3.OpTxn([]clientv3.Cmp{clientv3.Compare(clientv3.Value("new/a"), "=", "wrong")}, nil, []clientv3.Op{clientv3.OpPut("else", "selected")}))
	check("txn_read_before_write", clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpGet("key"), clientv3.OpPut("order", "value"), clientv3.OpGet("order")}, nil))
	check("txn_read_only", clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpGet("key")}, nil))
	check("txn_empty", clientv3.OpTxn(nil, nil, nil))
	check("missing_value_compare", clientv3.OpTxn([]clientv3.Cmp{clientv3.Compare(clientv3.Value("absent"), "!=", "anything")}, []clientv3.Op{clientv3.OpPut("wrong-branch", "bad")}, []clientv3.Op{clientv3.OpGet("key")}))
	check("range_compare", clientv3.OpTxn([]clientv3.Cmp{clientv3.Compare(clientv3.Version("p/").WithPrefix(), "=", 1)}, []clientv3.Op{clientv3.OpGet("p/", clientv3.WithPrefix())}, nil))
	check("nested_base_predicates", clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpPut("nested/key", "created"), clientv3.OpTxn([]clientv3.Cmp{clientv3.Compare(clientv3.Version("nested/key"), "=", 0)}, []clientv3.Op{clientv3.OpPut("nested/result", "base-view")}, []clientv3.Op{clientv3.OpPut("nested/result", "wrong")})}, nil))
	check("nested_mutually_exclusive_writes", clientv3.OpTxn(nil, []clientv3.Op{
		clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpPut("exclusive", "ok")}, []clientv3.Op{clientv3.OpDelete("exclusive")}),
	}, nil))
	check("nested_negative_revision", clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpGet("key", clientv3.WithRev(-10))}, nil))
	check("txn_rollback", clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpPut("rollback", "must-not-appear"), clientv3.OpPut("absent", "", clientv3.WithIgnoreValue())}, nil))
	check("rollback_absent", clientv3.OpGet("rollback"))
	check("txn_future_revision_rollback", clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpPut("rollback", "bad"), clientv3.OpGet("key", clientv3.WithRev(99999))}, nil))
	check("duplicate_put", clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpPut("same", "a"), clientv3.OpPut("same", "b")}, nil))
	check("overlapping_put_delete", clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpPut("p/a", "bad"), clientv3.OpDelete("p/", clientv3.WithPrefix())}, nil))
	check("unselected_invalid_branch", clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpGet("key")}, []clientv3.Op{clientv3.OpPut("same", "a"), clientv3.OpPut("same", "b")}))
	check("delete_previous", clientv3.OpDelete("key", clientv3.WithPrevKV()))
	check("deleted_history", clientv3.OpGet("key", clientv3.WithRev(2)))
	check("deleted_latest", clientv3.OpGet("key"))
	check("delete_missing_no_revision", clientv3.OpDelete("key"))
	check("recreate_resets_version", clientv3.OpPut("key", "reborn", clientv3.WithPrevKV()))
	check("recreated_metadata", clientv3.OpGet("key"))
	check("overlapping_deletes", clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpDelete("p/", clientv3.WithPrefix(), clientv3.WithPrevKV()), clientv3.OpDelete("p/a", clientv3.WithPrevKV())}, nil))
	check("history_before_deletes", clientv3.OpGet("p/", clientv3.WithPrefix(), clientv3.WithRev(9), clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortDescend)))
	check("empty_key", clientv3.OpPut("", "bad"))
	ops := make([]clientv3.Op, 129)
	for i := range ops {
		ops[i] = clientv3.OpGet("key")
	}
	check("too_many_operations", clientv3.OpTxn(nil, ops, nil))
	check("final_state", clientv3.OpGet("\x00", clientv3.WithFromKey()))
}

func mustCall(t *testing.T, c *clientv3.Client, op clientv3.Op) proto.Message {
	t.Helper()
	r, err := call(c, op)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestFailoverAndSnapshotRecovery(t *testing.T) {
	c := newCluster(t)
	clients := make([]*clientv3.Client, 3)
	for i, addr := range c.addresses {
		clients[i] = client(t, []string{addr})
	}
	leader := c.leader()
	follower := leader%3 + 1
	firstReply := mustCall(t, clients[follower-1], clientv3.OpPut("history", "first")).(*pb.PutResponse)
	if firstReply.Header.MemberId != uint64(follower) {
		t.Fatalf("Forwarded response has wrong member ID: %v", firstReply.Header)
	}
	first := firstReply.Header.Revision
	mustCall(t, clients[leader-1], clientv3.OpPut("history", "second"))
	c.stop(leader)
	leader = c.leader()
	check := func(c *clientv3.Client, revision int64, value string) {
		t.Helper()
		opts := []clientv3.OpOption{}
		if revision > 0 {
			opts = append(opts, clientv3.WithRev(revision))
		}
		got := mustCall(t, c, clientv3.OpGet("history", opts...)).(*pb.RangeResponse)
		if len(got.Kvs) != 1 || string(got.Kvs[0].Value) != value {
			t.Fatalf("Unexpected value: %v", got)
		}
	}
	check(clients[leader-1], 0, "second")
	check(clients[leader-1], first, "first")
	for id := 1; id <= 3; id++ {
		if c.processes[id] == nil {
			c.start(id)
		}
	}
	for id := range c.processes {
		c.stop(id)
	}
	for id := 1; id <= 3; id++ {
		c.start(id)
	}
	leader = c.leader()
	check(clients[leader-1], 0, "second")
	check(clients[leader-1], first, "first")
	lagging := leader%3 + 1
	c.stop(lagging)
	for i := 0; i < 80; i++ {
		mustCall(t, clients[leader-1], clientv3.OpPut("bulk", fmt.Sprintf("%d:%s", i, string(bytes.Repeat([]byte("x"), 4096)))))
	}
	for id := range c.processes {
		if _, err := c.http(id, "/checkpoint", map[string]any{}); err != nil {
			t.Fatal(err)
		}
	}
	c.start(lagging)
	installed := false
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		s, err := c.http(lagging, "/status", nil)
		if err == nil && s["installed_snapshots"].(float64) > 0 {
			installed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !installed {
		t.Fatal("Lagging follower did not install a snapshot")
	}
	check(clients[lagging-1], first, "first")
	local := mustCall(t, clients[lagging-1], clientv3.OpGet("history", clientv3.WithSerializable())).(*pb.RangeResponse)
	if len(local.Kvs) != 1 || string(local.Kvs[0].Value) != "second" {
		t.Fatalf("Bad local state: %v", local)
	}
	for id := range c.processes {
		c.stop(id)
	}
	for id := 1; id <= 3; id++ {
		c.start(id)
	}
	leader = c.leader()
	check(clients[leader-1], first, "first")
	response := mustCall(t, clients[leader-1], clientv3.OpPut("after-recovery", "ok")).(*pb.PutResponse)
	if response.Header.Revision != first+82 {
		t.Fatalf("Revision regressed: %d", response.Header.Revision)
	}
}

func TestConcurrentCompareAndSwap(t *testing.T) {
	c := newCluster(t)
	kv := client(t, c.addresses)
	mustCall(t, kv, clientv3.OpPut("counter", "0"))
	var workers sync.WaitGroup
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for n := 0; n < 10; n++ {
				for {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					r, err := kv.Get(ctx, "counter")
					if err != nil {
						cancel()
						errs <- err
						return
					}
					value, _ := strconv.Atoi(string(r.Kvs[0].Value))
					tx, err := kv.Txn(ctx).If(clientv3.Compare(clientv3.ModRevision("counter"), "=", r.Kvs[0].ModRevision)).Then(clientv3.OpPut("counter", strconv.Itoa(value+1))).Commit()
					cancel()
					if err != nil {
						errs <- err
						return
					}
					if tx.Succeeded {
						break
					}
				}
			}
		}()
	}
	workers.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	r := mustCall(t, kv, clientv3.OpGet("counter")).(*pb.RangeResponse)
	if string(r.Kvs[0].Value) != "120" || r.Header.Revision != 122 || r.Kvs[0].Version != 121 {
		t.Fatalf("Lost update or incorrect revision: %v", r)
	}
}

func TestPartitionReadModes(t *testing.T) {
	c := newCluster(t)
	isolated := c.leader()
	minority := client(t, []string{c.addresses[isolated-1]})
	initial := mustCall(t, minority, clientv3.OpPut("partition", "before")).(*pb.PutResponse).Header.Revision
	for id := range c.processes {
		blocked := []int{isolated}
		if id == isolated {
			blocked = nil
			for peer := range c.processes {
				if peer != isolated {
					blocked = append(blocked, peer)
				}
			}
		}
		if _, err := c.http(id, "/debug/partition", map[string]any{"blocked": blocked}); err != nil {
			t.Fatal(err)
		}
	}
	leader := 0
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline) && leader == 0; {
		for id := range c.processes {
			if id == isolated {
				continue
			}
			s, err := c.http(id, "/status", nil)
			if err == nil && s["role"] == "Leader" {
				leader = id
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leader == 0 {
		t.Fatal("Majority did not elect a leader")
	}
	majority := client(t, []string{c.addresses[leader-1]})
	mustCall(t, majority, clientv3.OpPut("partition", "after"))
	local := mustCall(t, minority, clientv3.OpGet("partition", clientv3.WithSerializable())).(*pb.RangeResponse)
	if len(local.Kvs) != 1 || string(local.Kvs[0].Value) != "before" {
		t.Fatalf("Unexpected local read: %v", local)
	}
	localCtx, localCancel := context.WithTimeout(context.Background(), time.Second)
	localTxn, localErr := minority.Txn(localCtx).If(clientv3.Compare(clientv3.Version("partition"), "=", 1)).Then(
		clientv3.OpGet("partition", clientv3.WithSerializable()),
	).Commit()
	localCancel()
	if localErr != nil || !localTxn.Succeeded || string(localTxn.Responses[0].GetResponseRange().Kvs[0].Value) != "before" {
		t.Fatalf("Serializable transaction failed on isolated replica: %v %v", localTxn, localErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, err := minority.Get(ctx, "partition")
	cancel()
	if err == nil {
		t.Fatal("An isolated replica served a linearizable read")
	}
	if _, err := compact(minority, initial, false); err == nil {
		t.Fatal("An isolated replica acknowledged compaction")
	}
	if _, err := call(minority, clientv3.OpGet("partition",
		clientv3.WithRev(initial-1), clientv3.WithSerializable())); err != nil {
		t.Fatalf("An isolated replica applied uncommitted compaction: %v", err)
	}
	for id := range c.processes {
		if _, err := c.http(id, "/debug/partition", map[string]any{"blocked": []int{}}); err != nil {
			t.Fatal(err)
		}
	}
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		r := mustCall(t, minority, clientv3.OpGet("partition", clientv3.WithSerializable())).(*pb.RangeResponse)
		if len(r.Kvs) == 1 && string(r.Kvs[0].Value) == "after" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("The healed replica did not catch up")
}

func endpointHost(address string) string {
	if !strings.Contains(address, "://") {
		return address
	}
	endpoint, err := url.Parse(address)
	if err != nil {
		panic(err)
	}
	return endpoint.Host
}
