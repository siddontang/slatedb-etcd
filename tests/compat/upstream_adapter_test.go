//go:build upstream

package compat

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/config"
	intf "go.etcd.io/etcd/tests/v3/framework/interfaces"
	"go.uber.org/zap"
)

func init() {
	testRunner = slateRunner{}
	clusterTestCases = func() []testCase {
		return []testCase{{name: "ThreeVoters", config: config.DefaultClusterConfig()}}
	}
}

type slateRunner struct{}

func (slateRunner) TestMain(m *testing.M) { os.Exit(m.Run()) }

func (slateRunner) BeforeTest(t testing.TB) {
	if os.Getenv("SLATEDB_UPSTREAM_ALL") == "1" {
		return
	}
	name := strings.Split(t.Name(), "/")[0]
	if strings.HasPrefix(name, "TestKV") || strings.HasPrefix(name, "TestTxn") ||
		name == "TestCompact" || strings.HasPrefix(name, "TestWaitLeader") {
		return
	}
	t.Skip("unsupported capability: " + name + "; see tests/upstream/README.md")
}

func (r slateRunner) NewCluster(_ context.Context, tb testing.TB, opts ...config.ClusterOption) intf.Cluster {
	r.BeforeTest(tb)
	cfg := config.NewClusterConfig(opts...)
	if cfg.ClusterSize != 3 || cfg.PeerTLS != config.NoTLS || cfg.ClientTLS != config.NoTLS ||
		cfg.QuotaBackendBytes != 0 || cfg.SnapshotCount != 0 {
		if os.Getenv("SLATEDB_UPSTREAM_ALL") == "1" {
			tb.Fatal("unsupported cluster configuration")
		}
		tb.Skip("unsupported cluster configuration: requires three voters without TLS or quota")
	}
	t := tb.(*testing.T)
	return &slateCluster{cluster: newCluster(t)}
}

type slateCluster struct{ *cluster }

func (c *slateCluster) Endpoints() []string       { return c.addresses }
func (c *slateCluster) WaitLeader(testing.TB) int { return c.leader() - 1 }
func (c *slateCluster) Close() error {
	for id := range c.processes {
		c.stop(id)
	}
	return nil
}
func (c *slateCluster) Members() []intf.Member {
	return []intf.Member{slateMember{c, 1}, slateMember{c, 2}, slateMember{c, 3}}
}
func (c *slateCluster) Client(opts ...config.ClientOption) (intf.Client, error) {
	cfg := clientv3.Config{Endpoints: c.addresses, Logger: zap.NewNop()}
	for _, option := range opts {
		option(&cfg)
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	c.t.Cleanup(func() { client.Close() })
	return integrationClient{Client: client, tb: c.t}, nil
}

type slateMember struct {
	cluster *slateCluster
	id      int
}

func (m slateMember) Client() intf.Client {
	c, err := m.cluster.Client(WithEndpoints([]string{m.cluster.addresses[m.id-1]}))
	if err != nil {
		m.cluster.t.Fatal(err)
	}
	return c
}
func (m slateMember) Start(context.Context) error {
	if m.cluster.processes[m.id] != nil {
		return fmt.Errorf("member %d is already running", m.id)
	}
	m.cluster.start(m.id)
	return nil
}
func (m slateMember) Stop() { m.cluster.stop(m.id) }

func WithAuth(user, password string) config.ClientOption {
	return func(value any) { cfg := value.(*clientv3.Config); cfg.Username = user; cfg.Password = password }
}
func WithEndpoints(endpoints []string) config.ClientOption {
	return func(value any) { value.(*clientv3.Config).Endpoints = endpoints }
}
