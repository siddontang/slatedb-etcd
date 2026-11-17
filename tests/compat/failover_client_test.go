package compat

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// Adapt the before/after server-loss cases from etcd v3.6.5 v3_failover_test.go.
func TestClientFailoverWithTLS(t *testing.T) {
	for _, timing := range []string{"client-before-failure", "client-after-failure"} {
		t.Run(timing, func(t *testing.T) {
			c := newClusterConfigured(t, 3, func(c *cluster) { configureTLS(c, true, true, true) })
			open := func() *clientv3.Client {
				api, err := clientv3.New(clientv3.Config{Endpoints: c.addresses, TLS: c.tlsClient,
					DialTimeout: 3 * time.Second, DialKeepAliveTime: 30 * time.Second,
					DialKeepAliveTimeout: 10 * time.Second, Logger: zap.NewNop()})
				require.NoError(t, err)
				t.Cleanup(func() { api.Close() })
				return api
			}
			api := open()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err := api.Put(ctx, "before-failure", "acknowledged")
			require.NoError(t, err)
			old := c.leader()
			c.stop(old)
			if timing == "client-after-failure" {
				api = open()
			}
			require.Eventually(t, func() bool {
				request, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				_, err := api.Put(request, "after-failure", "available")
				return err == nil
			}, 15*time.Second, 100*time.Millisecond)
			got, err := api.Get(ctx, "before-failure")
			require.NoError(t, err)
			require.Len(t, got.Kvs, 1)
			require.Equal(t, "acknowledged", string(got.Kvs[0].Value))
			c.start(old)
			c.waitReady()
			status, err := api.Status(ctx, c.addresses[old-1])
			require.NoError(t, err)
			require.NotZero(t, status.Leader)
		})
	}
}
