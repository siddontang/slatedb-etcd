package compat

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

func compact(c *clientv3.Client, revision int64, physical bool) (*pb.CompactionResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return pb.NewKVClient(c.ActiveConnection()).Compact(ctx, &pb.CompactionRequest{
		Revision: revision,
		Physical: physical,
	})
}

func TestDifferentialCompaction(t *testing.T) {
	c := newCluster(t)
	follower := c.leader()%3 + 1
	actual := client(t, []string{c.addresses[follower-1]})
	expected := reference(t)
	compare := func(t *testing.T, a proto.Message, ae error, b proto.Message, be error) {
		t.Helper()
		if ae != nil || be != nil {
			if status.Code(ae) != status.Code(be) ||
				status.Convert(ae).Message() != status.Convert(be).Message() {
				t.Fatalf("Errors differ: service=%v; etcd=%v", ae, be)
			}
			return
		}
		h := responseHeader(a)
		if h == nil || h.ClusterId == 0 || h.MemberId != uint64(follower) || h.RaftTerm == 0 {
			t.Fatalf("Invalid response identity: %v", h)
		}
		normalize(a)
		normalize(b)
		if !proto.Equal(a, b) {
			t.Fatalf("Responses differ:\nservice: %s\netcd:    %s", a, b)
		}
	}
	check := func(name string, op clientv3.Op) int64 {
		t.Helper()
		var revision int64
		t.Run(name, func(t *testing.T) {
			a, ae := call(actual, op)
			b, be := call(expected, op)
			compare(t, a, ae, b, be)
			if ae == nil {
				revision = responseHeader(a).Revision
			}
		})
		if t.Failed() {
			t.FailNow()
		}
		return revision
	}
	checkCompact := func(name string, revision int64) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			a, ae := compact(actual, revision, false)
			b, be := compact(expected, revision, false)
			compare(t, a, ae, b, be)
		})
		if t.Failed() {
			t.FailNow()
		}
	}
	checkCompact("negative_revision", -2)
	checkCompact("initial_boundary", -1)
	checkCompact("zero_revision", 0)
	checkCompact("repeated_zero", 0)
	checkCompact("empty_store", 1)
	check("empty_boundary_read", clientv3.OpGet("absent", clientv3.WithRev(1)))
	check("stable_key", clientv3.OpPut("stable", "anchor"))
	old := check("first_version", clientv3.OpPut("key", "first"))
	boundary := check("second_version", clientv3.OpPut("key", "second"))
	deleted := check("delete", clientv3.OpDelete("key"))
	recreated := check("recreate", clientv3.OpPut("key", "third"))
	latest := check("newer_key", clientv3.OpPut("later", "new"))
	checkCompact("future_revision", latest+1)
	check("future_compact_keeps_history", clientv3.OpGet("key", clientv3.WithRev(old)))
	checkCompact("compact_middle", boundary)
	check("compacted_point", clientv3.OpGet("key", clientv3.WithRev(old)))
	check("compacted_missing_key", clientv3.OpGet("absent", clientv3.WithRev(old)))
	check("compacted_count", clientv3.OpGet("", clientv3.WithFromKey(),
		clientv3.WithCountOnly(), clientv3.WithRev(old)))
	check("boundary_point", clientv3.OpGet("key", clientv3.WithRev(boundary)))
	check("boundary_range", clientv3.OpGet("", clientv3.WithFromKey(), clientv3.WithRev(boundary)))
	check("stable_anchor", clientv3.OpGet("stable", clientv3.WithRev(boundary)))
	check("delete_history", clientv3.OpGet("key", clientv3.WithRev(deleted)))
	check("recreated_history", clientv3.OpGet("key", clientv3.WithRev(recreated)))
	check("current", clientv3.OpGet("", clientv3.WithFromKey()))
	check("negative_direct_read", clientv3.OpGet("key", clientv3.WithRev(-1)))
	check("negative_txn_read", clientv3.OpTxn(nil,
		[]clientv3.Op{clientv3.OpGet("key", clientv3.WithRev(-1))}, nil))
	check("txn_rollback", clientv3.OpTxn(nil, []clientv3.Op{
		clientv3.OpPut("rollback", "must not appear"),
		clientv3.OpGet("key", clientv3.WithRev(old)),
	}, nil))
	check("rollback_absent", clientv3.OpGet("rollback"))
	check("inactive_compacted_branch", clientv3.OpTxn(nil,
		[]clientv3.Op{clientv3.OpGet("stable")},
		[]clientv3.Op{clientv3.OpGet("key", clientv3.WithRev(old))}))
	check("nested_boundary_read", clientv3.OpTxn(nil, []clientv3.Op{
		clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpGet("key", clientv3.WithRev(boundary))}, nil),
	}, nil))
	checkCompact("repeat_boundary", boundary)
	checkCompact("older_boundary", old)
	checkCompact("compact_deleted_version", deleted)
	check("deleted_boundary_range", clientv3.OpGet("", clientv3.WithFromKey(), clientv3.WithRev(deleted)))
	for _, api := range []*clientv3.Client{actual, expected} {
		if _, err := compact(api, latest, true); err != nil {
			t.Fatal(err)
		}
	}
	check("physical_compact_removes_history", clientv3.OpGet("key", clientv3.WithRev(deleted)))
	checkCompact("compact_current", latest)
	check("current_boundary", clientv3.OpGet("", clientv3.WithFromKey(), clientv3.WithRev(latest)))
	check("write_after_compact", clientv3.OpPut("key", "fourth", clientv3.WithPrevKV()))
	check("read_after_compact", clientv3.OpGet("key"))
}

func TestCompactionRecovery(t *testing.T) {
	// In shared state mode a restarted member adopts the newest shared checkpoint when it starts,
	// so it installs no Raft snapshot.
	t.Skip("needs a shared restart and pause shape; see #149 TestCompactionRecovery")
	c := newCluster(t)
	clients := make([]*clientv3.Client, 3)
	for i, addr := range c.addresses {
		clients[i] = client(t, []string{addr})
	}
	leader := c.leader()
	lagging := leader%3 + 1
	mustCall(t, clients[leader-1], clientv3.OpPut("stable", "anchor"))
	old := mustCall(t, clients[leader-1], clientv3.OpPut("key", "first")).(*pb.PutResponse).Header.Revision
	boundary := mustCall(t, clients[leader-1], clientv3.OpPut("key", "second")).(*pb.PutResponse).Header.Revision
	c.stop(lagging)
	if _, err := compact(clients[leader-1], boundary, false); err != nil {
		t.Fatal(err)
	}
	check := func(id int) {
		t.Helper()
		applied := false
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
			s, err := c.http(id, "/status", nil)
			if err == nil && s["compact_revision"].(float64) == float64(boundary) {
				applied = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if !applied {
			t.Fatalf("Node %d did not apply compaction", id)
		}
		for _, key := range []string{"key", "absent"} {
			_, err := call(clients[id-1], clientv3.OpGet(key,
				clientv3.WithRev(old), clientv3.WithSerializable()))
			if !errors.Is(err, rpctypes.ErrCompacted) {
				t.Fatalf("Node %d lost the compact boundary: %v", id, err)
			}
		}
		got := mustCall(t, clients[id-1], clientv3.OpGet("",
			clientv3.WithFromKey(), clientv3.WithRev(boundary),
			clientv3.WithSerializable())).(*pb.RangeResponse)
		if got.Header.Revision != boundary || len(got.Kvs) != 2 ||
			string(got.Kvs[0].Value) != "second" || string(got.Kvs[1].Value) != "anchor" {
			t.Fatalf("Node %d lost retained history: %v", id, got)
		}
	}
	// Kill all live nodes before an explicit SlateDB flush or checkpoint.
	for id := range c.processes {
		c.stop(id)
	}
	for id := 1; id <= 3; id++ {
		if id != lagging {
			c.start(id)
		}
	}
	leader = c.leader()
	for id := range c.processes {
		check(id)
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
		t.Fatal("Lagging node did not install a snapshot")
	}
	check(lagging)
	c.stop(leader)
	check(c.leader())
	for id := range c.processes {
		c.stop(id)
	}
	for id := 1; id <= 3; id++ {
		c.start(id)
	}
	leader = c.leader()
	check(leader)
	got := mustCall(t, clients[leader-1], clientv3.OpPut("key", "third")).(*pb.PutResponse)
	if got.Header.Revision != boundary+1 {
		t.Fatalf("Compaction changed the KV revision: %v", got.Header)
	}
}
