package downstream

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/coredns/coredns/plugin/etcd"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Use CoreDNS' etcd plugin to resolve leased DNS records through its normal client path.
func TestCoreDNSDiscovery(t *testing.T) {
	for _, kind := range []string{"service", "etcd"} {
		t.Run(kind, func(t *testing.T) {
			client := backend(t, kind)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			plugin := &etcd.Etcd{Client: client, PathPrefix: "/skydns", Zones: []string{"example.org."}}
			lease, err := client.Grant(ctx, 10)
			require.NoError(t, err)
			for i := 1; i <= 2; i++ {
				_, err := client.Put(ctx, fmt.Sprintf("/skydns/org/example/service/host%d", i),
					fmt.Sprintf(`{"host":"192.0.2.%d","port":8080,"ttl":30}`, i),
					clientv3.WithLease(lease.ID))
				require.NoError(t, err)
			}
			message := new(dns.Msg)
			message.SetQuestion("service.example.org.", dns.TypeA)
			records, err := plugin.Records(ctx, request.Request{Req: message}, false)
			require.NoError(t, err)
			require.Len(t, records, 2)
			require.ElementsMatch(t, []string{"192.0.2.1", "192.0.2.2"},
				[]string{records[0].Host, records[1].Host})
			_, err = client.Revoke(ctx, lease.ID)
			require.NoError(t, err)
			_, err = plugin.Records(ctx, request.Request{Req: message}, false)
			require.True(t, plugin.IsNameError(err))
		})
	}
}
