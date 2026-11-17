//go:build multihost

package compat

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestThreeVirtualMachines(t *testing.T) {
	names := strings.Split(os.Getenv("SLATEDB_VM_NAMES"), ",")
	require.Len(t, names, 3)
	binary := os.Getenv("SLATEDB_LINUX_BINARY")
	require.NotEmpty(t, binary)
	endpoint := os.Getenv("SLATEDB_S3_TEST_ENDPOINT")
	parsed, err := url.Parse(endpoint)
	require.NoError(t, err)
	port := parsed.Port()
	root := t.TempDir()
	hosts := make([]string, 3)
	for i, name := range names {
		hosts[i] = "lima-" + name + ".internal"
	}
	peerTLS := makeTLS(t, root, "peer", "peer", hosts...)
	clientTLS := makeTLS(t, root, "client", "client", hosts...)
	run := func(args ...string) string {
		t.Helper()
		output, err := exec.Command("limactl", args...).CombinedOutput()
		require.NoError(t, err, "limactl failed: %s", output)
		return strings.TrimSpace(string(output))
	}
	guestDirs := make([]string, 3)
	endpoints, peerEndpoints := make([]string, 3), make([]string, 3)
	tunnels := make([]*exec.Cmd, 3)
	for i := range names {
		endpoints[i], peerEndpoints[i] = "https://"+address(t), "https://"+address(t)
	}
	tunnel := func(i int) {
		t.Helper()
		if tunnels[i] != nil {
			tunnels[i].Process.Kill()
			tunnels[i].Wait()
		}
		home, err := os.UserHomeDir()
		require.NoError(t, err)
		command := exec.Command("ssh", "-F", filepath.Join(home, ".lima", names[i], "ssh.config"),
			"-o", "ExitOnForwardFailure=yes", "-N",
			"-L", strings.TrimPrefix(endpoints[i], "https://")+":127.0.0.1:2379",
			"-L", strings.TrimPrefix(peerEndpoints[i], "https://")+":127.0.0.1:2380", "lima-"+names[i])
		require.NoError(t, command.Start())
		tunnels[i] = command
	}
	start := func(i int) {
		t.Helper()
		directory := guestDirs[i]
		args := []string{"shell", names[i], "--", "sudo", "systemd-run", "--quiet",
			"--unit=slatedb-validation", "--collect", "--property=EnvironmentFile=" + directory + "/environment",
			directory + "/slatedb-etcd", "--id", strconv.Itoa(i + 1), "--allow-remote",
			"--listen", "0.0.0.0:2379", "--peer-listen", "0.0.0.0:2380", "--dir", directory + "/data",
			"--cluster-id", filepath.Base(root), "--snapshot-every", "0",
			"--cert-file", directory + "/client-cert.pem", "--key-file", directory + "/client-key.pem",
			"--trusted-ca-file", directory + "/client-ca.pem", "--client-cert-auth",
			"--peer-cert-file", directory + "/peer-cert.pem", "--peer-key-file", directory + "/peer-key.pem",
			"--peer-trusted-ca-file", directory + "/peer-ca.pem",
			"--s3-bucket", os.Getenv("SLATEDB_S3_TEST_BUCKET"), "--s3-allow-http",
			"--s3-endpoint", "http://host.lima.internal:" + port}
		for j, host := range hosts {
			args = append(args, "--peer", fmt.Sprintf("%d=https://%s:2380", j+1, host),
				"--client-peer", fmt.Sprintf("%d=https://%s:2379", j+1, host))
		}
		run(args...)
	}
	environment := filepath.Join(root, "environment")
	require.NoError(t, os.WriteFile(environment, []byte(
		"AWS_ACCESS_KEY_ID="+os.Getenv("AWS_ACCESS_KEY_ID")+"\n"+
			"AWS_SECRET_ACCESS_KEY="+os.Getenv("AWS_SECRET_ACCESS_KEY")+"\n"), 0600))
	for i, name := range names {
		home := run("shell", name, "--", "printenv", "HOME")
		guestDirs[i] = home + "/slatedb-validation-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		run("shell", name, "--", "mkdir", "-p", guestDirs[i])
		for _, file := range []string{peerTLS.ca, peerTLS.cert, peerTLS.key,
			clientTLS.ca, clientTLS.cert, clientTLS.key, environment} {
			run("copy", file, name+":"+guestDirs[i]+"/"+filepath.Base(file))
		}
		run("copy", binary, name+":"+guestDirs[i]+"/slatedb-etcd")
		run("shell", name, "--", "chmod", "700", guestDirs[i]+"/slatedb-etcd")
		run("shell", name, "--", "sync")
		tunnel(i)
		start(i)
	}
	t.Cleanup(func() {
		for i, name := range names {
			exec.Command("limactl", "shell", name, "--", "sudo", "systemctl", "stop", "slatedb-validation").Run()
			if tunnels[i] != nil {
				tunnels[i].Process.Kill()
				tunnels[i].Wait()
			}
		}
	})
	newClient := func(addresses []string, tls *tls.Config) *clientv3.Client {
		c, err := clientv3.New(clientv3.Config{Endpoints: addresses, TLS: tls, DialTimeout: 3 * time.Second})
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })
		return c
	}
	api := newClient(endpoints, clientTLS.config)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	require.Eventually(t, func() bool {
		call, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, err := api.Get(call, "ready")
		return err == nil
	}, 30*time.Second, 100*time.Millisecond)
	leader := func() int {
		for i, endpoint := range endpoints {
			call, cancel := context.WithTimeout(ctx, time.Second)
			status, err := api.Status(call, endpoint)
			cancel()
			if err == nil && status.Header.MemberId == status.Leader {
				return i
			}
		}
		t.Fatal("no leader")
		return 0
	}
	peerHTTP := &http.Client{Transport: &http.Transport{TLSClientConfig: peerTLS.config}, Timeout: 15 * time.Second}
	diagnostic := func(i int, path string) (map[string]any, error) {
		var response *http.Response
		var err error
		if path == "/checkpoint" {
			response, err = peerHTTP.Post(peerEndpoints[i]+path, "application/json", strings.NewReader("{}"))
		} else {
			response, err = peerHTTP.Get(peerEndpoints[i] + path)
		}
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		var value map[string]any
		err = json.NewDecoder(response.Body).Decode(&value)
		if response.StatusCode != 200 {
			return value, fmt.Errorf("HTTP %d", response.StatusCode)
		}
		return value, err
	}
	written, err := api.Put(ctx, "anchor", "acknowledged")
	require.NoError(t, err)
	verify := func() {
		for _, endpoint := range endpoints {
			c := newClient([]string{endpoint}, clientTLS.config)
			require.Eventually(t, func() bool {
				call, cancel := context.WithTimeout(ctx, time.Second)
				defer cancel()
				result, err := c.Get(call, "anchor")
				return err == nil && len(result.Kvs) == 1 && string(result.Kvs[0].Value) == "acknowledged" &&
					result.Header.Revision >= written.Header.Revision
			}, 30*time.Second, 100*time.Millisecond)
		}
	}
	verify()
	if !t.Run("mutual-tls", func(t *testing.T) {
		anonymous := clientTLS.config.Clone()
		anonymous.Certificates = nil
		denied := newClient(endpoints[:1], anonymous)
		call, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, err := denied.Get(call, "anchor")
		require.Error(t, err)
	}) {
		return
	}
	if !t.Run("leader-machine-crash", func(t *testing.T) {
		old := leader()
		run("stop", "--force", names[old])
		survivors := append([]string{}, endpoints[:old]...)
		survivors = append(survivors, endpoints[old+1:]...)
		healthy := newClient(survivors, clientTLS.config)
		require.Eventually(t, func() bool {
			call, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			_, err := healthy.Put(call, "after-failover", "available")
			return err == nil
		}, 20*time.Second, 100*time.Millisecond)
		run("start", "--yes", names[old])
		tunnel(old)
		start(old)
		verify()
	}) {
		return
	}
	if !t.Run("kernel-peer-partition", func(t *testing.T) {
		isolated := (leader() + 1) % 3
		for _, chain := range []string{"INPUT", "OUTPUT"} {
			run("shell", names[isolated], "--", "sudo", "iptables", "-A", chain,
				"-p", "tcp", "--dport", "2380", "-j", "DROP")
		}
		defer func() {
			for _, chain := range []string{"INPUT", "OUTPUT"} {
				run("shell", names[isolated], "--", "sudo", "iptables", "-D", chain,
					"-p", "tcp", "--dport", "2380", "-j", "DROP")
			}
		}()
		majority := newClient([]string{endpoints[(isolated+1)%3], endpoints[(isolated+2)%3]}, clientTLS.config)
		_, err := majority.Put(ctx, "partition", "majority")
		require.NoError(t, err)
		minority := newClient([]string{endpoints[isolated]}, clientTLS.config)
		call, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err = minority.Get(call, "partition")
		require.Error(t, err)
	}) {
		return
	}
	verify()
	if !t.Run("object-outage-crash-and-snapshot", func(t *testing.T) {
		follower := (leader() + 1) % 3
		before, err := diagnostic(follower, "/status")
		require.NoError(t, err)
		run("shell", names[follower], "--", "sudo", "iptables", "-A", "OUTPUT",
			"-p", "tcp", "--dport", port, "-j", "REJECT")
		checkpoint := make(chan error, 1)
		go func() { _, err := diagnostic(follower, "/checkpoint"); checkpoint <- err }()
		for i := 0; i < 20; i++ {
			_, err := api.Put(ctx, "bulk", strconv.Itoa(i))
			require.NoError(t, err)
		}
		current, err := diagnostic(follower, "/status")
		require.NoError(t, err)
		require.Equal(t, before["snapshot_index"], current["snapshot_index"])
		require.Equal(t, before["first_log_index"], current["first_log_index"])
		run("stop", "--force", names[follower])
		require.Error(t, <-checkpoint)
		healthy := newClient([]string{endpoints[(follower+1)%3], endpoints[(follower+2)%3]}, clientTLS.config)
		for i := 0; i < 8; i++ {
			_, err := healthy.Put(ctx, "offline", strconv.Itoa(i))
			require.NoError(t, err)
		}
		for i := range names {
			if i != follower {
				_, err := diagnostic(i, "/checkpoint")
				require.NoError(t, err)
			}
		}
		run("start", "--yes", names[follower])
		tunnel(follower)
		start(follower)
		verify()
		require.Eventually(t, func() bool {
			status, err := diagnostic(follower, "/status")
			return err == nil && status["installed_snapshots"].(float64) > 0
		}, 30*time.Second, 100*time.Millisecond)
	}) {
		return
	}
	verify()
}
