//go:build upstream

package compat

import (
	"context"
	"fmt"
	"os"
	"testing"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/config"
	intf "go.etcd.io/etcd/tests/v3/framework/interfaces"
	"go.uber.org/zap"
)

func init() {
	testRunner = slateRunner{}
	clusterTestCases = func() []testCase {
		return []testCase{
			{name: "ThreeVoters", config: config.DefaultClusterConfig()},
			{name: "SingleVoter", config: config.ClusterConfig{ClusterSize: 1}},
			{name: "PeerTLS", config: config.ClusterConfig{ClusterSize: 3, PeerTLS: config.ManualTLS}},
			{name: "ClientTLS", config: config.ClusterConfig{ClusterSize: 1, ClientTLS: config.ManualTLS}},
		}
	}
}

type slateRunner struct{}

func (slateRunner) TestMain(m *testing.M) { os.Exit(m.Run()) }

func (slateRunner) BeforeTest(testing.TB) {}

func (r slateRunner) NewCluster(_ context.Context, tb testing.TB, opts ...config.ClusterOption) intf.Cluster {
	r.BeforeTest(tb)
	cfg := config.NewClusterConfig(opts...)
	t := tb.(*testing.T)
	args := []string{"--quota-backend-bytes", fmt.Sprint(cfg.QuotaBackendBytes), "--strict-reconfig-check", fmt.Sprint(cfg.StrictReconfigCheck)}
	if cfg.AuthToken != "" {
		args = append(args, "--auth-token", cfg.AuthToken)
	}
	return &slateCluster{cluster: newClusterConfigured(t, cfg.ClusterSize, func(c *cluster) {
		c.snapshotEvery = cfg.SnapshotCount
		if cfg.PeerTLS != config.NoTLS || cfg.ClientTLS != config.NoTLS {
			configureTLS(c, cfg.PeerTLS != config.NoTLS, cfg.ClientTLS != config.NoTLS, false)
		}
	}, args...)}
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
	members := make([]intf.Member, len(c.addresses))
	for i := range members {
		members[i] = slateMember{c, i + 1}
	}
	return members
}
func (c *slateCluster) Client(opts ...config.ClientOption) (intf.Client, error) {
	cfg := clientv3.Config{Endpoints: c.addresses, Logger: zap.NewNop(), TLS: c.tlsClient}
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
	m.cluster.leader()
	return nil
}
func (m slateMember) Stop() { m.cluster.stop(m.id) }

func WithAuth(user, password string) config.ClientOption {
	return func(value any) { cfg := value.(*clientv3.Config); cfg.Username = user; cfg.Password = password }
}
func WithEndpoints(endpoints []string) config.ClientOption {
	return func(value any) { value.(*clientv3.Config).Endpoints = endpoints }
}
