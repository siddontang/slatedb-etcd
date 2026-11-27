package compat

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

func TestDifferentialLease(t *testing.T) {
	cluster := newCluster(t)
	actual := client(t, []string{cluster.addresses[cluster.leader()%3]})
	expected := reference(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	check := func(name string, operation func(*clientv3.Client) (proto.Message, error)) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			a, ae := operation(actual)
			b, be := operation(expected)
			if ae != nil || be != nil {
				require.Equal(t, status.Code(be), status.Code(ae))
				require.Equal(t, status.Convert(be).Message(), status.Convert(ae).Message())
				return
			}
			clean := func(m proto.Message) {
				var h *pb.ResponseHeader
				switch r := m.(type) {
				case *pb.LeaseGrantResponse:
					h = r.Header
				case *pb.LeaseRevokeResponse:
					h = r.Header
				case *pb.LeaseLeasesResponse:
					h = r.Header
					sort.Slice(r.Leases, func(i, j int) bool { return r.Leases[i].ID < r.Leases[j].ID })
				case *pb.LeaseTimeToLiveResponse:
					h = r.Header
					if r.GrantedTTL != 0 {
						require.GreaterOrEqual(t, r.TTL, int64(0))
						require.LessOrEqual(t, r.TTL, r.GrantedTTL)
						r.TTL = 0
					}
					sort.Slice(r.Keys, func(i, j int) bool { return string(r.Keys[i]) < string(r.Keys[j]) })
				default:
					normalize(m)
					return
				}
				if h != nil {
					h.ClusterId, h.MemberId, h.RaftTerm = 0, 0, 0
				}
			}
			clean(a)
			clean(b)
			require.True(t, proto.Equal(a, b), "service %s; etcd %s", a, b)
		})
		if t.Failed() {
			t.FailNow()
		}
	}
	grant := func(id, ttl int64) func(*clientv3.Client) (proto.Message, error) {
		return func(c *clientv3.Client) (proto.Message, error) {
			return pb.NewLeaseClient(c.ActiveConnection()).LeaseGrant(ctx, &pb.LeaseGrantRequest{ID: id, TTL: ttl})
		}
	}
	ttl := func(id int64) func(*clientv3.Client) (proto.Message, error) {
		return func(c *clientv3.Client) (proto.Message, error) {
			return pb.NewLeaseClient(c.ActiveConnection()).LeaseTimeToLive(ctx, &pb.LeaseTimeToLiveRequest{ID: id, Keys: true})
		}
	}
	revoke := func(id int64) func(*clientv3.Client) (proto.Message, error) {
		return func(c *clientv3.Client) (proto.Message, error) {
			return pb.NewLeaseClient(c.ActiveConnection()).LeaseRevoke(ctx, &pb.LeaseRevokeRequest{ID: id})
		}
	}
	kv := func(op clientv3.Op) func(*clientv3.Client) (proto.Message, error) {
		return func(c *clientv3.Client) (proto.Message, error) { return call(c, op) }
	}
	check("grant", grant(1001, 30))
	check("duplicate", grant(1001, 30))
	check("second", grant(1002, 30))
	check("too_large", grant(1003, math.MaxInt64))
	check("negative_id", grant(-2001, 30))
	check("minimum", grant(1004, -100))
	check("revoke_minimum", revoke(1004))
	check("attach_a", kv(clientv3.OpPut("lease/a", "a", clientv3.WithLease(1001))))
	check("attach_b", kv(clientv3.OpPut("lease/b", "b", clientv3.WithLease(1001))))
	check("keys", ttl(1001))
	check("reassign", kv(clientv3.OpPut("lease/a", "new", clientv3.WithLease(1002), clientv3.WithPrevKV())))
	check("ignore_lease", kv(clientv3.OpPut("lease/a", "newer", clientv3.WithIgnoreLease())))
	check("old_keys", ttl(1001))
	check("new_keys", ttl(1002))
	check("compare", kv(clientv3.OpTxn([]clientv3.Cmp{clientv3.Compare(clientv3.LeaseValue("lease/a"), "=", 1002)},
		[]clientv3.Op{clientv3.OpPut("compared", "ok")}, nil)))
	check("rollback", kv(clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpPut("bad", "no"),
		clientv3.OpPut("unknown", "bad", clientv3.WithLease(99999))}, nil)))
	check("rollback_absent", kv(clientv3.OpGet("bad")))
	check("detach", kv(clientv3.OpPut("lease/b", "detached")))
	check("revoke_old", revoke(1001))
	check("detached_survives", kv(clientv3.OpGet("lease/b")))
	check("revoke_new", revoke(1002))
	check("attached_deleted", kv(clientv3.OpGet("lease/a")))
	check("missing_ttl", ttl(1002))
	check("missing_revoke", revoke(1002))
	check("list", func(c *clientv3.Client) (proto.Message, error) {
		return pb.NewLeaseClient(c.ActiveConnection()).LeaseLeases(ctx, &pb.LeaseLeasesRequest{})
	})
}

func collectWatch(t *testing.T, c *clientv3.Client, key string, count int, options ...clientv3.OpOption) []*mvccpb.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	stream := c.Watch(ctx, key, options...)
	var events []*mvccpb.Event
	var last int64
	for len(events) < count {
		select {
		case r, ok := <-stream:
			require.True(t, ok, "watch closed with %d of %d events", len(events), count)
			require.NoError(t, r.Err())
			require.GreaterOrEqual(t, r.Header.Revision, last)
			last = r.Header.Revision
			for _, e := range r.Events {
				require.LessOrEqual(t, e.Kv.ModRevision, r.Header.Revision)
				events = append(events, (*mvccpb.Event)(e))
			}
		case <-ctx.Done():
			t.Fatalf("watch received %d of %d events", len(events), count)
		}
	}
	require.Len(t, events, count)
	return events
}

func TestDifferentialWatch(t *testing.T) {
	cluster := newCluster(t)
	actual, expected := client(t, cluster.addresses), reference(t)
	for _, op := range []clientv3.Op{
		clientv3.OpPut("watch/b", "first"),
		clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpPut("watch/z", "z"), clientv3.OpPut("watch/a", "a")}, nil),
		clientv3.OpPut("watch/b", "second"), clientv3.OpDelete("watch/", clientv3.WithPrefix()),
		clientv3.OpDelete("watch/missing"),
	} {
		_, err := call(actual, op)
		require.NoError(t, err)
		_, err = call(expected, op)
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		name, key string
		count     int
		opts      []clientv3.OpOption
	}{
		{"order_and_previous", "watch/", 7, []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithPrevKV()}},
		{"no_put", "watch/", 3, []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithFilterPut(), clientv3.WithPrevKV()}},
		{"no_delete", "watch/", 4, []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithFilterDelete()}},
		{"exact", "watch/b", 3, nil},
		{"range", "watch/a", 5, []clientv3.OpOption{clientv3.WithRange("watch/c")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := append(tc.opts, clientv3.WithRev(1))
			a, b := collectWatch(t, actual, tc.key, tc.count, opts...), collectWatch(t, expected, tc.key, tc.count, opts...)
			for i := range a {
				require.True(t, proto.Equal(a[i], b[i]), "event %d: service %s; etcd %s", i, a[i], b[i])
			}
		})
	}
	for _, c := range []*clientv3.Client{actual, expected} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := c.Compact(ctx, 4)
		require.NoError(t, err)
		if c == actual {
			// The Watch stream can use a different member. Fence compaction on every endpoint.
			for _, address := range cluster.addresses {
				_, err := client(t, []string{address}).Get(ctx, "watch/", clientv3.WithPrefix())
				require.NoError(t, err)
			}
		}
		r := <-c.Watch(ctx, "watch/", clientv3.WithPrefix(), clientv3.WithRev(3))
		require.True(t, errors.Is(r.Err(), rpctypes.ErrCompacted), "%v", r.Err())
		require.Equal(t, int64(4), r.CompactRevision)
		cancel()
	}
	opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(4)}
	a, b := collectWatch(t, actual, "watch/", 4, opts...), collectWatch(t, expected, "watch/", 4, opts...)
	for i := range a {
		require.True(t, proto.Equal(a[i], b[i]), "compacted event %d: service %s; etcd %s", i, a[i], b[i])
	}
}

func TestWatchCompactionUsesLocalApply(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	follower := leader%3 + 1
	majority := client(t, []string{c.addresses[leader-1]})
	local := client(t, []string{c.addresses[follower-1]})
	first := mustCall(t, majority, clientv3.OpPut("local-watch", "first")).(*pb.PutResponse)
	last := mustCall(t, majority, clientv3.OpPut("local-watch", "last")).(*pb.PutResponse)
	mustCall(t, local, clientv3.OpGet("local-watch"))
	for id := range c.processes {
		blocked := []int{follower}
		if id == follower {
			blocked = []int{follower%3 + 1, (follower+1)%3 + 1}
		}
		_, err := c.http(id, "/debug/partition", map[string]any{"blocked": blocked})
		require.NoError(t, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := majority.Compact(ctx, last.Header.Revision)
	require.NoError(t, err)
	// A local Watch can replay history that the majority has already compacted.
	events := collectWatch(t, local, "local-watch", 2, clientv3.WithRev(first.Header.Revision))
	require.Equal(t, "first", string(events[0].Kv.Value))
	require.Equal(t, "last", string(events[1].Kv.Value))
	for id := range c.processes {
		_, err := c.http(id, "/debug/partition", map[string]any{"blocked": []int{}})
		require.NoError(t, err)
	}
	// A later linearizable read waits for the same member to apply compaction.
	mustCall(t, local, clientv3.OpGet("local-watch"))
	response := <-local.Watch(ctx, "local-watch", clientv3.WithRev(first.Header.Revision))
	require.ErrorIs(t, response.Err(), rpctypes.ErrCompacted)
	require.Equal(t, last.Header.Revision, response.CompactRevision)
}

func TestWatchLeaseRestartAndSnapshot(t *testing.T) {
	// In shared state mode a restarted member adopts the newest shared checkpoint when it starts,
	// so it installs no Raft snapshot.
	t.Skip("needs a shared restart and pause shape; see #149 TestWatchLeaseRestartAndSnapshot")
	c := newCluster(t)
	api := client(t, c.addresses)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	grant, err := api.Grant(ctx, 30)
	require.NoError(t, err)
	txn, err := api.Txn(ctx).Then(clientv3.OpPut("leased/a", "a", clientv3.WithLease(grant.ID)),
		clientv3.OpPut("leased/b", "b", clientv3.WithLease(grant.ID))).Commit()
	require.NoError(t, err)
	lagging := c.leader()%3 + 1
	c.stop(lagging)
	_, err = api.Put(ctx, "leased/a", "updated", clientv3.WithIgnoreLease())
	require.NoError(t, err)
	for id := range c.processes {
		_, err = c.http(id, "/checkpoint", map[string]any{})
		require.NoError(t, err)
	}
	c.start(lagging)
	require.Eventually(t, func() bool {
		status, err := c.http(lagging, "/status", nil)
		return err == nil && status["installed_snapshots"].(float64) > 0
	}, 10*time.Second, 50*time.Millisecond, "follower did not install a snapshot")
	follower := client(t, []string{c.addresses[lagging-1]})
	events := collectWatch(t, follower, "leased/", 3, clientv3.WithPrefix(), clientv3.WithRev(txn.Header.Revision), clientv3.WithPrevKV())
	require.Equal(t, []string{"leased/a", "leased/b", "leased/a"}, []string{
		string(events[0].Kv.Key), string(events[1].Kv.Key), string(events[2].Kv.Key)})
	for id := range c.processes {
		c.stop(id)
	}
	for id := 1; id <= 3; id++ {
		c.start(id)
	}
	c.leader()
	fresh := client(t, c.addresses)
	lease, err := fresh.TimeToLive(ctx, grant.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, int64(30), lease.GrantedTTL)
	require.Len(t, lease.Keys, 2)
	keep, err := fresh.KeepAliveOnce(ctx, grant.ID)
	require.NoError(t, err)
	require.Equal(t, int64(30), keep.TTL)
	_, err = fresh.Revoke(ctx, grant.ID)
	require.NoError(t, err)
	events = collectWatch(t, fresh, "leased/", 5, clientv3.WithPrefix(), clientv3.WithRev(txn.Header.Revision), clientv3.WithPrevKV())
	require.Equal(t, mvccpb.DELETE, events[3].Type)
	require.Equal(t, mvccpb.DELETE, events[4].Type)
	require.Equal(t, events[3].Kv.ModRevision, events[4].Kv.ModRevision)
	response, err := fresh.Get(ctx, "leased/", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Empty(t, response.Kvs)
	short, err := fresh.Grant(ctx, 2)
	require.NoError(t, err)
	put, err := fresh.Put(ctx, "expires", "value", clientv3.WithLease(short.ID))
	require.NoError(t, err)
	events = collectWatch(t, fresh, "expires", 1, clientv3.WithRev(put.Header.Revision+1), clientv3.WithPrevKV())
	require.Equal(t, mvccpb.DELETE, events[0].Type)
	require.Equal(t, "value", string(events[0].PrevKv.Value))
}

func TestWatchStreamControls(t *testing.T) {
	c := newCluster(t)
	for name, api := range map[string]*clientv3.Client{
		"service": client(t, c.addresses), "etcd": reference(t),
	} {
		t.Run(name, func(t *testing.T) { checkWatchStreamControls(t, api) })
	}
}

func checkWatchStreamControls(t *testing.T, api *clientv3.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := pb.NewWatchClient(api.ActiveConnection()).Watch(ctx)
	require.NoError(t, err)
	send := func(r *pb.WatchRequest) { t.Helper(); require.NoError(t, stream.Send(r)) }
	create := func(id, rev int64) {
		send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
			CreateRequest: &pb.WatchCreateRequest{Key: []byte("control"), WatchId: id, StartRevision: rev}}})
	}
	send(&pb.WatchRequest{})
	send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
		CreateRequest: &pb.WatchCreateRequest{Key: []byte("z"), RangeEnd: []byte("a")}}})
	invalid, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, invalid.Created && invalid.Canceled)
	require.Equal(t, "mvcc: watcher range is empty", invalid.CancelReason)
	create(-2, 0)
	negative, err := stream.Recv()
	require.NoError(t, err)
	require.False(t, negative.Canceled)
	require.Equal(t, int64(-2), negative.WatchId)
	send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CancelRequest{
		CancelRequest: &pb.WatchCancelRequest{WatchId: -2}}})
	negative, err = stream.Recv()
	require.NoError(t, err)
	require.True(t, negative.Canceled)
	create(77, 0)
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	create(77, 0)
	duplicate, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, duplicate.Canceled)
	require.Equal(t, int64(-1), duplicate.WatchId)
	require.Equal(t, "mvcc: duplicate watch ID provided on the WatchStream", duplicate.CancelReason)
	put, err := api.Put(ctx, "control", "one")
	require.NoError(t, err)
	event, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, event.Events, 1)
	require.Equal(t, put.Header.Revision, event.Events[0].Kv.ModRevision)
	send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{ProgressRequest: &pb.WatchProgressRequest{}}})
	progress, err := stream.Recv()
	require.NoError(t, err)
	require.Empty(t, progress.Events)
	require.Equal(t, int64(-1), progress.WatchId)
	require.GreaterOrEqual(t, progress.Header.Revision, put.Header.Revision)
	send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CancelRequest{CancelRequest: &pb.WatchCancelRequest{WatchId: 77}}})
	stopped, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, stopped.Canceled)
	create(88, put.Header.Revision+2)
	created, err = stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{
		ProgressRequest: &pb.WatchProgressRequest{}}})
	_, err = api.Put(ctx, "control", "skip")
	require.NoError(t, err)
	future, err := api.Put(ctx, "control", "future")
	require.NoError(t, err)
	event, err = stream.Recv()
	require.NoError(t, err)
	require.Len(t, event.Events, 1)
	require.Equal(t, "future", string(event.Events[0].Kv.Value))
	require.Equal(t, future.Header.Revision, event.Events[0].Kv.ModRevision)
	require.NoError(t, stream.CloseSend())
}

func TestWatchLeaseKeepAliveAndLeaderFailure(t *testing.T) {
	c := newCluster(t)
	api := client(t, c.addresses)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Allow leader election and client reconnection before the client lease deadline.
	lease, err := api.Grant(ctx, 8)
	require.NoError(t, err)
	put, err := api.Put(ctx, "kept", "alive", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	keepCtx, stopKeep := context.WithCancel(ctx)
	defer stopKeep()
	keep, err := api.KeepAlive(keepCtx, lease.ID)
	require.NoError(t, err)
	end := time.NewTimer(10 * time.Second)
	defer end.Stop()
	killed, replies := 0, 0
loop:
	for {
		select {
		case response, ok := <-keep:
			require.True(t, ok, "keepalive stream closed")
			require.Positive(t, response.TTL)
			replies++
			if killed == 0 {
				killed = c.leader()
				c.stop(killed)
			}
		case <-end.C:
			break loop
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	require.GreaterOrEqual(t, replies, 3)
	current, err := api.Get(ctx, "kept")
	require.NoError(t, err)
	require.Len(t, current.Kvs, 1)
	stopKeep()
	c.start(killed)
	events := collectWatch(t, api, "kept", 1, clientv3.WithRev(put.Header.Revision+1), clientv3.WithPrevKV())
	require.Equal(t, mvccpb.DELETE, events[0].Type)
	require.Equal(t, "alive", string(events[0].PrevKv.Value))
	_, err = api.KeepAliveOnce(ctx, lease.ID)
	require.ErrorIs(t, err, rpctypes.ErrLeaseNotFound)
}

func TestWatchLeaseMinorityCannotRenew(t *testing.T) {
	c := newCluster(t)
	leader := c.leader()
	minority := client(t, []string{c.addresses[leader-1]})
	var addresses []string
	for id := 1; id <= 3; id++ {
		if id != leader {
			addresses = append(addresses, c.addresses[id-1])
		}
	}
	majority := client(t, addresses)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	lease, err := minority.Grant(ctx, 2)
	require.NoError(t, err)
	put, err := minority.Put(ctx, "partitioned", "value", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	stream, err := pb.NewWatchClient(minority.ActiveConnection()).Watch(clientv3.WithRequireLeader(ctx))
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
		CreateRequest: &pb.WatchCreateRequest{Key: []byte("partitioned")}}}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	for id := range c.processes {
		blocked := []int{leader}
		if id == leader {
			blocked = []int{leader%3 + 1, (leader+1)%3 + 1}
		}
		_, err = c.http(id, "/debug/partition", map[string]any{"blocked": blocked})
		require.NoError(t, err)
	}
	short, stop := context.WithTimeout(ctx, 1500*time.Millisecond)
	_, err = minority.KeepAliveOnce(short, lease.ID)
	stop()
	require.Error(t, err, "minority acknowledged a renewal")
	_, err = stream.Recv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "no leader")
	deleted := collectWatch(t, majority, "partitioned", 1,
		clientv3.WithRev(put.Header.Revision+1), clientv3.WithPrevKV())
	require.Equal(t, mvccpb.DELETE, deleted[0].Type)
	for id := range c.processes {
		_, err = c.http(id, "/debug/partition", map[string]any{"blocked": []int{}})
		require.NoError(t, err)
	}
	caughtUp := collectWatch(t, minority, "partitioned", 1,
		clientv3.WithRev(put.Header.Revision+1), clientv3.WithPrevKV())
	require.True(t, proto.Equal(deleted[0], caughtUp[0]))
	current, err := majority.Get(ctx, "partitioned")
	require.NoError(t, err)
	require.Empty(t, current.Kvs)
}

func TestWatchCatchupProgressAndFragmentation(t *testing.T) {
	c := newCluster(t)
	api := client(t, c.addresses)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var revision int64
	for n := 0; n < 70; n++ {
		r, err := api.Put(ctx, "catchup", "value")
		require.NoError(t, err)
		revision = r.Header.Revision
	}
	// Progress reports local apply state. Wait for this member before asserting a cluster revision.
	watchClient := client(t, []string{c.addresses[0]})
	ready, err := watchClient.Get(ctx, "catchup")
	require.NoError(t, err)
	require.Equal(t, revision, ready.Header.Revision)
	stream, err := pb.NewWatchClient(watchClient.ActiveConnection()).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
		CreateRequest: &pb.WatchCreateRequest{Key: []byte("catchup"), StartRevision: 1}}}))
	created, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	require.NoError(t, stream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_ProgressRequest{
		ProgressRequest: &pb.WatchProgressRequest{}}}))
	for n := int64(2); n <= revision; n++ {
		r, err := stream.Recv()
		require.NoError(t, err)
		require.Len(t, r.Events, 1, "progress overtook history")
		require.Equal(t, n, r.Events[0].Kv.ModRevision)
	}
	progress, err := stream.Recv()
	require.NoError(t, err)
	require.Empty(t, progress.Events)
	require.Equal(t, int64(-1), progress.WatchId)
	require.Equal(t, revision, progress.Header.Revision)
	require.NoError(t, stream.CloseSend())

	var operations []clientv3.Op
	for n := 0; n < 120; n++ {
		operations = append(operations, clientv3.OpPut(fmt.Sprintf("large/%03d", n), strings.Repeat("x", 10000)))
	}
	txn, err := api.Txn(ctx).Then(operations...).Commit()
	require.NoError(t, err)
	stream, err = pb.NewWatchClient(watchClient.ActiveConnection()).Watch(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.WatchRequest{RequestUnion: &pb.WatchRequest_CreateRequest{
		CreateRequest: &pb.WatchCreateRequest{Key: []byte("large/"), RangeEnd: []byte("large0"),
			StartRevision: txn.Header.Revision, Fragment: true}}}))
	created, err = stream.Recv()
	require.NoError(t, err)
	require.True(t, created.Created)
	fragments, count := 0, 0
	for {
		r, err := stream.Recv()
		require.NoError(t, err)
		require.NotEmpty(t, r.Events)
		fragments++
		for _, event := range r.Events {
			require.Equal(t, txn.Header.Revision, event.Kv.ModRevision)
			require.Equal(t, fmt.Sprintf("large/%03d", count), string(event.Kv.Key))
			count++
		}
		if !r.Fragment {
			break
		}
	}
	require.Equal(t, 120, count)
	require.GreaterOrEqual(t, fragments, 2)
}
