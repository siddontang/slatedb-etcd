package downstream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.etcd.io/etcd/server/v3/embed"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	"k8s.io/utils/clock"
)

func address(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := l.Addr().String()
	require.NoError(t, l.Close())
	return address
}

func backend(t *testing.T, kind string) *clientv3.Client {
	t.Helper()
	var endpoints []string
	if kind == "etcd" {
		config := embed.NewConfig()
		config.Dir = t.TempDir()
		config.LogLevel = "error"
		clientURL, _ := url.Parse("http://" + address(t))
		peerURL, _ := url.Parse("http://" + address(t))
		config.ListenClientUrls, config.AdvertiseClientUrls = []url.URL{*clientURL}, []url.URL{*clientURL}
		config.ListenPeerUrls, config.AdvertisePeerUrls = []url.URL{*peerURL}, []url.URL{*peerURL}
		config.InitialCluster = config.InitialClusterFromName(config.Name)
		server, err := embed.StartEtcd(config)
		require.NoError(t, err)
		t.Cleanup(server.Close)
		select {
		case <-server.Server.ReadyNotify():
		case <-time.After(15 * time.Second):
			t.Fatal("reference etcd did not start")
		}
		endpoints = []string{clientURL.String()}
	} else {
		if configured := os.Getenv("SLATEDB_DOWNSTREAM_ENDPOINTS"); configured != "" {
			endpoints = strings.Split(configured, ",")
		} else {
			binary := os.Getenv("SLATEDB_ETCD_BINARY")
			if binary == "" {
				binary = "../../target/release/slatedb-etcd"
			}
			binary, err := filepath.Abs(binary)
			require.NoError(t, err)
			root := t.TempDir()
			for i := 0; i < 3; i++ {
				endpoints = append(endpoints, address(t))
			}
			// Three members run in shared state mode on the RustFS server of
			// scripts/check-compat.py, with a prefix for this cluster only.
			s3, bucket := os.Getenv("SLATEDB_S3_TEST_ENDPOINT"), os.Getenv("SLATEDB_S3_TEST_BUCKET")
			require.True(t, s3 != "" && bucket != "", "run scripts/check-compat.py --suite downstream")
			// A temporary directory name repeats across tests, so the prefix takes a random part.
			random := make([]byte, 8)
			_, err = rand.Read(random)
			require.NoError(t, err)
			prefix := "downstream/" + hex.EncodeToString(random)
			// This module cannot import the compat test package. Its flags copy `storeFlags` in
			// tests/compat/compat_test.go and `shared_args` in scripts/rustfs_fixture.py.
			flags := []string{"--state-mode", "shared", "--s3-bucket", bucket, "--s3-prefix", prefix,
				"--s3-endpoint", s3}
			if strings.HasPrefix(s3, "http://") {
				flags = append(flags, "--s3-allow-http")
			}
			for i, endpoint := range endpoints {
				args := []string{"--id", fmt.Sprint(i + 1), "--listen", endpoint,
					"--cluster-id", filepath.Base(root), "--dir", filepath.Join(root, fmt.Sprint(i+1))}
				args = append(args, flags...)
				for j, peer := range endpoints {
					args = append(args, "--peer", fmt.Sprintf("%d=%s", j+1, peer))
				}
				cmd := exec.Command(binary, args...)
				log, err := os.Create(filepath.Join(root, fmt.Sprintf("node-%d.log", i+1)))
				require.NoError(t, err)
				cmd.Stdout, cmd.Stderr = log, log
				require.NoError(t, cmd.Start())
				t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait(); log.Close() })
			}
		}
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := client.Get(ctx, "ready")
		return err == nil
	}, 20*time.Second, 50*time.Millisecond)
	return client
}

// Call Kubernetes' original storage tests against its real etcd3 storage implementation.
func TestKubernetesStorage(t *testing.T) {
	for _, kind := range []string{"service", "etcd"} {
		t.Run(kind, func(t *testing.T) {
			api := backend(t, kind)
			scheme := runtime.NewScheme()
			metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
			require.NoError(t, example.AddToScheme(scheme))
			require.NoError(t, examplev1.AddToScheme(scheme))
			codec := serializer.NewCodecFactory(scheme).LegacyCodec(examplev1.SchemeGroupVersion)
			tests := []struct {
				name string
				run  func(context.Context, *testing.T, storage.Interface)
			}{
				{"create", func(ctx context.Context, t *testing.T, s storage.Interface) {
					storagetesting.RunTestCreate(ctx, t, s, func(context.Context, *testing.T, string) {})
				}},
				{"create-ttl", storagetesting.RunTestCreateWithTTL},
				{"create-existing", storagetesting.RunTestCreateWithKeyExist},
				{"get", storagetesting.RunTestGet},
				{"delete", storagetesting.RunTestUnconditionalDelete},
				{"conditional-delete", storagetesting.RunTestConditionalDelete},
				{"delete-conflict", storagetesting.RunTestDeleteWithConflict},
				{"recursive-list", storagetesting.RunTestGetListRecursivePrefix},
				{"list-paging", storagetesting.RunTestListPaging},
				{"namespace-list", storagetesting.RunTestNamespaceScopedList},
				{"update-ttl", storagetesting.RunTestGuaranteedUpdateWithTTL},
				{"update-conflict", storagetesting.RunTestGuaranteedUpdateWithConflict},
				{"watch", storagetesting.RunTestWatch},
				{"watch-delete", storagetesting.RunTestDeleteTriggerWatch},
				{"watch-resume", storagetesting.RunTestWatchFromNonZero},
				{"watch-delay", storagetesting.RunTestDelayedWatchDelivery},
				{"watch-cancel", storagetesting.RunTestWatchContextCancel},
				{"watch-delete-revision", storagetesting.RunTestWatchDeleteEventObjectHaveLatestRV},
				{"cluster-watch", storagetesting.RunTestClusterScopedWatch},
				{"namespace-watch", storagetesting.RunTestNamespaceScopedWatch},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					client, err := kubernetes.New(clientv3.Config{Endpoints: api.Endpoints(), DialTimeout: 5 * time.Second})
					require.NoError(t, err)
					t.Cleanup(func() { client.Close() })
					compactor := etcd3.NewCompactor(api, 0, clock.RealClock{}, nil)
					t.Cleanup(compactor.Stop)
					versioner := storage.APIObjectVersioner{}
					leases := etcd3.NewDefaultLeaseManagerConfig()
					leases.ReuseDurationSeconds = 1
					store := etcd3.New(client, compactor, codec,
						func() runtime.Object { return &example.Pod{} },
						func() runtime.Object { return &example.PodList{} },
						"/downstream/"+strings.ReplaceAll(t.Name(), "/", "-"), "pods",
						schema.GroupResource{Resource: "pods"},
						storagetesting.NewPrefixTransformer([]byte("test!"), false), leases,
						etcd3.NewDefaultDecoder(codec, versioner), versioner)
					t.Cleanup(store.Close)
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					t.Cleanup(cancel)
					test.run(ctx, t, store)
				})
			}
		})
	}
}
