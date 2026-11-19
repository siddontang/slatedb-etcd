package compat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

func TestAdminCompatibility(t *testing.T) {
	c := newCluster(t)
	actual, expected := client(t, c.addresses), reference(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, err := actual.AuthStatus(ctx)
	require.NoError(t, err)
	b, err := expected.AuthStatus(ctx)
	require.NoError(t, err)
	require.Equal(t, b.Enabled, a.Enabled)
	require.Equal(t, b.AuthRevision, a.AuthRevision)
	members, err := actual.MemberList(ctx)
	require.NoError(t, err)
	require.Len(t, members.Members, 3)
	for i, endpoint := range c.addresses {
		local := client(t, []string{endpoint})
		auth, err := local.AuthStatus(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), auth.Header.MemberId)
		members, err := local.MemberList(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), members.Header.MemberId)
		s, err := actual.Status(ctx, endpoint)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1), s.Header.MemberId)
		require.NotZero(t, s.Leader)
		require.GreaterOrEqual(t, s.RaftIndex, s.RaftAppliedIndex)
	}
	for _, api := range []*clientv3.Client{actual, expected} {
		result, err := api.Put(ctx, "/capability/compact", "old")
		require.NoError(t, err)
		_, err = api.Put(ctx, "/capability/compact", "new")
		require.NoError(t, err)
		_, err = api.Compact(ctx, result.Header.Revision+1, clientv3.WithCompactPhysical())
		require.NoError(t, err)
	}
	_, ae := actual.Get(ctx, "/capability/compact", clientv3.WithRev(2))
	_, be := expected.Get(ctx, "/capability/compact", clientv3.WithRev(2))
	require.Error(t, ae)
	require.Equal(t, status.Code(be), status.Code(ae))
}

func TestDifferentialHashKV(t *testing.T) {
	c := newCluster(t)
	follower := c.leader()%3 + 1
	actual, expected := client(t, []string{c.addresses[follower-1]}), reference(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	check := func(revision int64) {
		t.Helper()
		a, ae := actual.HashKV(ctx, actual.Endpoints()[0], revision)
		b, be := expected.HashKV(ctx, expected.Endpoints()[0], revision)
		require.Equal(t, status.Code(be), status.Code(ae))
		if ae == nil {
			require.Equal(t, b.Hash, a.Hash, "revision %d", revision)
			require.Equal(t, b.CompactRevision, a.CompactRevision)
			require.Equal(t, b.HashRevision, a.HashRevision)
		}
	}
	check(0)
	ops := []clientv3.Op{
		clientv3.OpPut("a", "one"), clientv3.OpPut("b", "two"),
		clientv3.OpPut("a", "three"), clientv3.OpDelete("b"),
		clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpPut("c", "c"), clientv3.OpPut("d", "d")}, nil),
		clientv3.OpDelete("c", clientv3.WithRange("e")), clientv3.OpPut("b", "again"),
	}
	for _, op := range ops {
		_, err := actual.Do(ctx, op)
		require.NoError(t, err)
		_, err = expected.Do(ctx, op)
		require.NoError(t, err)
		check(0)
	}
	for revision := int64(2); revision <= 8; revision++ {
		check(revision)
	}
	for _, revision := range []int64{4, 5, 7, 8} {
		for _, api := range []*clientv3.Client{actual, expected} {
			_, err := api.Compact(ctx, revision, clientv3.WithCompactPhysical())
			require.NoError(t, err)
		}
		check(0)
		check(revision)
		check(revision - 1)
	}
	// Hash an empty keyspace, then query that boundary after a key returns.
	var deleted int64
	for _, api := range []*clientv3.Client{actual, expected} {
		result, err := api.Delete(ctx, "a", clientv3.WithFromKey())
		require.NoError(t, err)
		deleted = result.Header.Revision
	}
	check(deleted)
	for _, api := range []*clientv3.Client{actual, expected} {
		_, err := api.Put(ctx, "a", "new generation")
		require.NoError(t, err)
		_, err = api.Compact(ctx, deleted, clientv3.WithCompactPhysical())
		require.NoError(t, err)
	}
	check(deleted)
	check(0)
}
