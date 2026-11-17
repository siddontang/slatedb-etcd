package compat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// These controls assert known differences. They do not establish compatibility.
func TestCapabilityDifferences(t *testing.T) {
	c := newCluster(t)
	actual, expected := client(t, c.addresses), reference(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	probes := []struct {
		name string
		run  func(*clientv3.Client) error
	}{
		{"auth", func(api *clientv3.Client) error { _, err := api.AuthStatus(ctx); return err }},
		{"members", func(api *clientv3.Client) error { _, err := api.MemberList(ctx); return err }},
		{"status", func(api *clientv3.Client) error {
			_, err := api.Status(ctx, api.Endpoints()[0])
			return err
		}},
		{"physical-compact", func(api *clientv3.Client) error {
			result, err := api.Put(ctx, "/capability/compact", "value")
			if err != nil {
				return err
			}
			_, err = api.Compact(ctx, result.Header.Revision, clientv3.WithCompactPhysical())
			return err
		}},
	}
	for _, probe := range probes {
		t.Run(probe.name, func(t *testing.T) {
			require.NoError(t, probe.run(expected), "reference etcd must support this operation")
			require.Equal(t, codes.Unimplemented, status.Code(probe.run(actual)))
		})
	}
}
