package compat

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestLearnerJoinPromoteRemoveAndRecover(t *testing.T) {
	t.Run("ThreeVoters", func(t *testing.T) { testLearnerLifecycle(t, 3, false) })
	t.Run("SingleVoterStrict", func(t *testing.T) { testLearnerLifecycle(t, 1, true) })
}

func testLearnerLifecycle(t *testing.T, size int, strict bool) {
	// The learner makes a second member, so a single voter also starts on RustFS.
	c := newClusterConfigured(t, size, func(c *cluster) { c.sharedStore = true },
		"--strict-reconfig-check", fmt.Sprint(strict))
	api := client(t, c.addresses)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := api.Put(ctx, "before-join", "snapshot")
	require.NoError(t, err)
	endpoint := address(t)
	added, err := api.MemberAddAsLearner(ctx, []string{"http://" + endpoint})
	require.NoError(t, err)
	require.True(t, added.Member.IsLearner)
	id := added.Member.ID
	_, err = api.MemberPromote(ctx, id)
	require.Error(t, err, "an absent learner cannot be promoted")
	var process *exec.Cmd
	var log *os.File
	stop := func() {
		if process != nil {
			process.Process.Kill()
			process.Wait()
			log.Close()
			process = nil
		}
	}
	t.Cleanup(stop)
	start := func() {
		args := []string{"--id", fmt.Sprint(id), "--listen", endpoint, "--join",
			"--dir", filepath.Join(c.root, "learner"), "--cluster-id", filepath.Base(c.root),
			"--strict-reconfig-check", fmt.Sprint(strict), "--snapshot-every", "0"}
		args = append(args, c.storeArgs...)
		for _, member := range added.Members {
			args = append(args, "--peer", fmt.Sprintf("%d=%s", member.ID, member.PeerURLs[0]))
		}
		var err error
		log, err = os.OpenFile(filepath.Join(c.root, "learner.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		require.NoError(t, err)
		process = exec.Command(c.binary, args...)
		process.Env = append(os.Environ(), "RUST_LOG=slatedb_etcd=info,slatedb=warn")
		process.Stdout, process.Stderr = log, log
		require.NoError(t, process.Start())
	}
	start()
	learner := client(t, []string{endpoint})
	require.Eventually(t, func() bool {
		attempt, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		result, err := learner.Get(attempt, "before-join", clientv3.WithSerializable())
		return err == nil && len(result.Kvs) == 1 && string(result.Kvs[0].Value) == "snapshot"
	}, 15*time.Second, 100*time.Millisecond, "learner must install the membership snapshot")
	require.Eventually(t, func() bool {
		members, err := api.MemberList(ctx)
		if err != nil {
			return false
		}
		for _, member := range members.Members {
			if member.ID == id {
				return member.Name != "" && len(member.ClientURLs) == 1
			}
		}
		return false
	}, 5*time.Second, 100*time.Millisecond)
	beforePromotion, err := learner.Status(ctx, endpoint)
	require.NoError(t, err)
	require.True(t, beforePromotion.IsLearner)
	require.Eventually(t, func() bool { _, err := api.MemberPromote(ctx, id); return err == nil },
		5*time.Second, 100*time.Millisecond)
	_, err = api.MoveLeader(ctx, id)
	require.NoError(t, err)
	s, err := api.Status(ctx, endpoint)
	require.NoError(t, err)
	require.Equal(t, id, s.Leader)
	require.False(t, s.IsLearner)
	removed := uint64(1)
	// A two-voter cluster needs both voters until removal commits.
	if size > 1 {
		c.stop(int(removed))
	}
	_, err = learner.MemberRemove(ctx, removed)
	require.NoError(t, err)
	if size == 1 {
		c.stop(int(removed))
	}
	_, err = learner.Put(ctx, "after-remove", "durable")
	require.NoError(t, err)
	// Restart with the old command line. Durable membership must take precedence.
	for i := 2; i <= size; i++ {
		c.stop(i)
	}
	stop()
	for i := 2; i <= size; i++ {
		c.start(i)
	}
	start()
	require.Eventually(t, func() bool {
		attempt, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		result, err := learner.Get(attempt, "after-remove")
		return err == nil && len(result.Kvs) == 1 && string(result.Kvs[0].Value) == "durable"
	}, 15*time.Second, 100*time.Millisecond)
	members, err := learner.MemberList(ctx)
	require.NoError(t, err)
	require.Len(t, members.Members, size)
	for _, member := range members.Members {
		require.NotEqual(t, removed, member.ID)
	}
	result, err := pb.NewMaintenanceClient(learner.ActiveConnection()).MoveLeader(ctx,
		&pb.MoveLeaderRequest{TargetID: removed})
	require.Nil(t, result)
	require.Error(t, err)
}

func TestSerializableMemberListDuringPartition(t *testing.T) {
	c := newCluster(t)
	isolated := c.leader()%3 + 1
	for id := range c.processes {
		blocked := []int{isolated}
		if id == isolated {
			blocked = nil
			for peer := range c.processes {
				if peer != id {
					blocked = append(blocked, peer)
				}
			}
		}
		_, err := c.http(id, "/debug/partition", map[string]any{"blocked": blocked})
		require.NoError(t, err)
	}
	api := client(t, []string{c.addresses[isolated-1]})
	cluster := pb.NewClusterClient(api.ActiveConnection())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	response, err := cluster.MemberList(ctx, &pb.MemberListRequest{Linearizable: false})
	cancel()
	require.NoError(t, err)
	require.Len(t, response.Members, 3)
	require.Equal(t, uint64(isolated), response.Header.MemberId)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	_, err = cluster.MemberList(ctx, &pb.MemberListRequest{Linearizable: true})
	cancel()
	require.Error(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	local, err := api.Status(ctx, c.addresses[isolated-1])
	cancel()
	require.NoError(t, err)
	require.Equal(t, uint64(isolated), local.Header.MemberId)

}
