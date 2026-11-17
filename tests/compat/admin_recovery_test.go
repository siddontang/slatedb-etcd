package compat

import (
	"context"
	"crypto/sha256"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func TestAdminSnapshotAndPhysicalRecovery(t *testing.T) {
	c := newCluster(t)
	api := client(t, c.addresses)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	first, err := api.Put(ctx, "history", "old")
	require.NoError(t, err)
	written, err := api.Put(ctx, "history", strings.Repeat("x", 96*1024))
	require.NoError(t, err)
	_, err = api.Compact(ctx, written.Header.Revision, clientv3.WithCompactPhysical())
	require.NoError(t, err)
	hash, err := api.HashKV(ctx, c.addresses[0], 0)
	require.NoError(t, err)
	maintenance := pb.NewMaintenanceClient(api.ActiveConnection())
	native, err := maintenance.Hash(ctx, &pb.HashRequest{})
	require.NoError(t, err)
	repeated, err := maintenance.Hash(ctx, &pb.HashRequest{})
	require.NoError(t, err)
	require.Equal(t, native.Hash, repeated.Hash)
	require.NotZero(t, native.Hash)
	stream, err := pb.NewMaintenanceClient(api.ActiveConnection()).Snapshot(ctx, &pb.SnapshotRequest{})
	require.NoError(t, err)
	var data []byte
	chunks := 0
	var remaining uint64
	for {
		part, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, written.Header.Revision, part.Header.Revision)
		if chunks > 0 {
			require.Equal(t, remaining, uint64(len(part.Blob))+part.RemainingBytes)
		}
		remaining = part.RemainingBytes
		data = append(data, part.Blob...)
		chunks++
	}
	require.Greater(t, chunks, 1)
	require.Zero(t, remaining)
	require.Greater(t, len(data), sha256.Size)
	digest := sha256.Sum256(data[:len(data)-sha256.Size])
	require.Equal(t, digest[:], data[len(data)-sha256.Size:])
	for id := range c.processes {
		c.stop(id)
	}
	for id := 1; id <= 3; id++ {
		c.start(id)
	}
	c.leader()
	_, err = api.Get(ctx, "history", clientv3.WithRev(first.Header.Revision))
	require.ErrorIs(t, err, rpctypes.ErrCompacted)
	result, err := api.Get(ctx, "history")
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("x", 96*1024), string(result.Kvs[0].Value))
	restored, err := api.HashKV(ctx, c.addresses[0], 0)
	require.NoError(t, err)
	require.Equal(t, hash.Hash, restored.Hash)
	require.Equal(t, hash.CompactRevision, restored.CompactRevision)
}

func TestAuthLeaseAttachmentDifferential(t *testing.T) {
	c := newCluster(t)
	for _, target := range []struct {
		name string
		api  *clientv3.Client
	}{
		{"slatedb", client(t, c.addresses)}, {"etcd", reference(t)},
	} {
		t.Run(target.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			api := target.api
			_, err := api.UserAdd(ctx, "root", "root-password")
			require.NoError(t, err)
			_, err = api.UserGrantRole(ctx, "root", "root")
			require.NoError(t, err)
			_, err = api.RoleAdd(ctx, "writer")
			require.NoError(t, err)
			_, err = api.RoleGrantPermission(ctx, "writer", "/allowed/", clientv3.GetPrefixRangeEnd("/allowed/"), clientv3.PermissionType(clientv3.PermReadWrite))
			require.NoError(t, err)
			_, err = api.UserAdd(ctx, "writer", "writer-password")
			require.NoError(t, err)
			_, err = api.UserGrantRole(ctx, "writer", "writer")
			require.NoError(t, err)
			lease, err := api.Grant(ctx, 60)
			require.NoError(t, err)
			_, err = api.Put(ctx, "/private", "root", clientv3.WithLease(lease.ID))
			require.NoError(t, err)
			_, err = api.AuthEnable(ctx)
			require.NoError(t, err)
			writer, err := clientv3.New(clientv3.Config{Endpoints: api.Endpoints(), Username: "writer",
				Password: "writer-password", Logger: zap.NewNop()})
			require.NoError(t, err)
			defer writer.Close()
			_, err = writer.Put(ctx, "/allowed/own", "ok")
			require.NoError(t, err)
			_, err = writer.Put(ctx, "/allowed/shared", "must fail", clientv3.WithLease(lease.ID))
			require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
			_, err = writer.Revoke(ctx, lease.ID)
			require.ErrorIs(t, err, rpctypes.ErrPermissionDenied)
			value, err := writer.Get(ctx, "/allowed/shared")
			require.NoError(t, err)
			require.Empty(t, value.Kvs)
		})
	}
}

func TestNativeSnapshotRestore(t *testing.T) {
	source := newCluster(t)
	api := client(t, source.addresses)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	lease, err := api.Grant(ctx, 60)
	require.NoError(t, err)
	_, err = api.Put(ctx, "restore", "old", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	written, err := api.Put(ctx, "restore", "saved", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	_, err = api.Compact(ctx, written.Header.Revision, clientv3.WithCompactPhysical())
	require.NoError(t, err)
	_, err = api.UserAdd(ctx, "root", "restore-password")
	require.NoError(t, err)
	_, err = api.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = api.AuthEnable(ctx)
	require.NoError(t, err)
	admin := func(endpoints []string) *clientv3.Client {
		authenticated, err := clientv3.New(clientv3.Config{Endpoints: endpoints, Username: "root",
			Password: "restore-password", Logger: zap.NewNop()})
		require.NoError(t, err)
		t.Cleanup(func() { authenticated.Close() })
		return authenticated
	}
	old := admin(source.addresses)
	oldToken, err := old.Authenticate(ctx, "root", "restore-password")
	require.NoError(t, err)
	stream, err := old.Snapshot(ctx)
	require.NoError(t, err)
	data, err := io.ReadAll(stream)
	stream.Close()
	require.NoError(t, err)
	file := filepath.Join(t.TempDir(), "native.snapshot")
	require.NoError(t, os.WriteFile(file, data, 0600))
	restored := newCluster(t, "--restore-snapshot", file)
	root := admin(restored.addresses)
	value, err := root.Get(ctx, "restore")
	require.NoError(t, err)
	require.Equal(t, "saved", string(value.Kvs[0].Value))
	require.Equal(t, lease.ID, clientv3.LeaseID(value.Kvs[0].Lease))
	require.Equal(t, written.Header.Revision, value.Header.Revision)
	require.NotEqual(t, written.Header.ClusterId, value.Header.ClusterId)
	_, err = root.Get(ctx, "restore", clientv3.WithRev(written.Header.Revision-1))
	require.ErrorIs(t, err, rpctypes.ErrCompacted)
	ttl, err := root.TimeToLive(ctx, lease.ID, clientv3.WithAttachedKeys())
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("restore")}, ttl.Keys)
	members, err := root.MemberList(ctx)
	require.NoError(t, err)
	for _, member := range members.Members {
		require.Equal(t, []string{"http://" + restored.addresses[member.ID-1]}, member.PeerURLs)
	}
	anonymous := client(t, restored.addresses)
	stale := metadata.AppendToOutgoingContext(ctx, "token", oldToken.Token)
	_, err = pb.NewKVClient(anonymous.ActiveConnection()).Range(stale, &pb.RangeRequest{Key: []byte("restore")})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	restored.args = nil
	for id := range restored.processes {
		restored.stop(id)
	}
	for id := 1; id <= 3; id++ {
		restored.start(id)
	}
	restored.leader()
	value, err = root.Get(ctx, "restore")
	require.NoError(t, err)
	require.Equal(t, "saved", string(value.Kvs[0].Value))
	for _, test := range []struct{ name, path, dir, errorText string }{
		{"existing-directory", file, filepath.Join(restored.root, "node-1"), "empty data directory"},
		{"bad-checksum", filepath.Join(t.TempDir(), "bad.snapshot"), filepath.Join(t.TempDir(), "fresh"), "checksum mismatch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.name == "bad-checksum" {
				damaged := append([]byte(nil), data...)
				damaged[0] ^= 1
				require.NoError(t, os.WriteFile(test.path, damaged, 0600))
			}
			endpoint := address(t)
			output, err := exec.Command(source.binary, "--id", "1", "--listen", endpoint,
				"--peer", "1="+endpoint, "--dir", test.dir, "--restore-snapshot", test.path).CombinedOutput()
			require.Error(t, err)
			require.Contains(t, string(output), test.errorText)
		})
	}
}

func TestMaintenanceAlarmDifferential(t *testing.T) {
	c := newCluster(t)
	for _, target := range []struct {
		name string
		api  *clientv3.Client
	}{
		{"slatedb", client(t, c.addresses)}, {"etcd", reference(t)},
	} {
		t.Run(target.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			api := target.api
			written, err := api.Put(ctx, "alarm", "safe")
			require.NoError(t, err)
			lease, err := api.Grant(ctx, 60)
			require.NoError(t, err)
			maintenance := pb.NewMaintenanceClient(api.ActiveConnection())
			for _, alarm := range []pb.AlarmType{pb.AlarmType_NOSPACE, pb.AlarmType_CORRUPT} {
				request := &pb.AlarmRequest{Action: pb.AlarmRequest_ACTIVATE,
					MemberID: written.Header.MemberId, Alarm: alarm}
				_, err = maintenance.Alarm(ctx, request)
				require.NoError(t, err)
				expected := rpctypes.ErrNoSpace
				if alarm == pb.AlarmType_CORRUPT {
					expected = rpctypes.ErrCorrupt
				}
				_, err = api.Put(ctx, "alarm", "blocked")
				require.ErrorIs(t, err, expected)
				_, err = api.Grant(ctx, 30)
				require.ErrorIs(t, err, expected)
				if alarm == pb.AlarmType_CORRUPT {
					_, err = api.Get(ctx, "alarm")
					require.NoError(t, err)
					_, err = api.Get(ctx, "alarm", clientv3.WithSerializable())
					require.NoError(t, err)
					_, err = api.Compact(ctx, written.Header.Revision)
					require.ErrorIs(t, err, expected)
					_, err = api.Revoke(ctx, lease.ID)
					require.ErrorIs(t, err, expected)
				}
				request.Action = pb.AlarmRequest_DEACTIVATE
				_, err = maintenance.Alarm(ctx, request)
				require.NoError(t, err)
			}
			value, err := api.Get(ctx, "alarm")
			require.NoError(t, err)
			require.Equal(t, "safe", string(value.Kvs[0].Value))
			_, err = api.Put(ctx, "alarm", "recovered")
			require.NoError(t, err)
		})
	}
}
