package compat

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testTLS struct {
	ca, cert, key string
	config        *tls.Config
}

func makeTLS(t *testing.T, root, name, user string, hosts ...string) testTLS {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name + " CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: user},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: append([]string{"localhost"}, hosts...),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	result := testTLS{ca: filepath.Join(root, name+"-ca.pem"), cert: filepath.Join(root, name+"-cert.pem"), key: filepath.Join(root, name+"-key.pem")}
	write := func(path, kind string, der []byte) {
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(result.ca, "CERTIFICATE", caDER)
	write(result.cert, "CERTIFICATE", der)
	write(result.key, "PRIVATE KEY", keyDER)
	// Parse the signed CA to retain its public key for verification.
	signedCA, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(signedCA)
	identity, err := tls.LoadX509KeyPair(result.cert, result.key)
	if err != nil {
		t.Fatal(err)
	}
	result.config = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{identity}}
	return result
}

func configureTLS(c *cluster, peerTLS, clientTLS, mutual bool) {
	c.t.Helper()
	for range c.addresses {
		c.peerAddresses = append(c.peerAddresses, address(c.t))
	}
	if peerTLS {
		config := makeTLS(c.t, c.root, "peer", "peer")
		c.args = append(c.args, "--peer-cert-file", config.cert, "--peer-key-file", config.key, "--peer-trusted-ca-file", config.ca)
		for i := range c.peerAddresses {
			c.peerAddresses[i] = "https://" + c.peerAddresses[i]
		}
		c.httpClient = &http.Client{Timeout: 12 * time.Second, Transport: &http.Transport{TLSClientConfig: config.config}}
	}
	if clientTLS {
		config := makeTLS(c.t, c.root, "client", "root")
		c.args = append(c.args, "--cert-file", config.cert, "--key-file", config.key)
		if mutual {
			c.args = append(c.args, "--trusted-ca-file", config.ca, "--client-cert-auth")
		}
		for i := range c.addresses {
			c.addresses[i] = "https://" + c.addresses[i]
		}
		c.tlsClient = config.config
	}
}

func tlsClient(t *testing.T, endpoints []string, config *tls.Config) *clientv3.Client {
	t.Helper()
	api, err := clientv3.New(clientv3.Config{Endpoints: endpoints, TLS: config,
		DialTimeout: time.Second, Logger: zap.NewNop()})
	require.NoError(t, err)
	t.Cleanup(func() { api.Close() })
	return api
}

func TestTLSClientAndPeer(t *testing.T) {
	for _, mode := range []struct {
		name                 string
		peer, client, mutual bool
	}{
		{"client", false, true, false}, {"peer", true, false, false},
		{"mutual", true, true, true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			c := newClusterConfigured(t, 3, func(c *cluster) {
				configureTLS(c, mode.peer, mode.client, mode.mutual)
			})
			api := tlsClient(t, c.addresses, c.tlsClient)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := api.Put(ctx, "tls", "replicated")
			require.NoError(t, err)
			for _, endpoint := range c.addresses {
				follower := tlsClient(t, []string{endpoint}, c.tlsClient)
				result, err := follower.Get(ctx, "tls")
				require.NoError(t, err)
				require.Len(t, result.Kvs, 1)
				require.Equal(t, "replicated", string(result.Kvs[0].Value))
			}
			if mode.client {
				wrong := makeTLS(t, c.root, "wrong", "root")
				configs := []*tls.Config{wrong.config}
				hostname := c.tlsClient.Clone()
				hostname.ServerName = "wrong.example"
				configs = append(configs, hostname)
				if mode.mutual {
					noCert := c.tlsClient.Clone()
					noCert.Certificates = nil
					configs = append(configs, noCert)
				}
				for _, config := range configs {
					bad := tlsClient(t, c.addresses, config)
					ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
					_, err := bad.Get(ctx, "tls")
					cancel()
					require.Error(t, err)
				}
			}
			if mode.peer {
				// Client certificates must not authenticate to the peer listener.
				transport := &http.Transport{TLSClientConfig: c.tlsClient}
				httpClient := &http.Client{Timeout: time.Second, Transport: transport}
				_, err := httpClient.Get(c.peerAddresses[0] + "/status")
				transport.CloseIdleConnections()
				require.Error(t, err)
			}
		})
	}
}

func TestTLSCertificateIdentity(t *testing.T) {
	c := newClusterConfigured(t, 3, func(c *cluster) { configureTLS(c, true, true, true) })
	api := tlsClient(t, c.addresses, c.tlsClient)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := api.UserAddWithOptions(ctx, "root", "", &clientv3.UserAddOptions{NoPassword: true})
	require.NoError(t, err)
	_, err = api.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = api.AuthEnable(ctx)
	require.NoError(t, err)
	for _, endpoint := range c.addresses {
		authenticated := tlsClient(t, []string{endpoint}, c.tlsClient)
		_, err := authenticated.Put(ctx, "certificate", "root")
		require.NoError(t, err)
	}
}

func TestClientMetadataCannotImpersonate(t *testing.T) {
	c := newClusterConfigured(t, 3, func(c *cluster) { configureTLS(c, true, true, false) })
	api := tlsClient(t, c.addresses, c.tlsClient)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := api.UserAdd(ctx, "root", "secret")
	require.NoError(t, err)
	_, err = api.UserGrantRole(ctx, "root", "root")
	require.NoError(t, err)
	_, err = api.AuthEnable(ctx)
	require.NoError(t, err)
	forged := metadata.AppendToOutgoingContext(ctx, "x-slatedb-user", "root", "x-slatedb-forwarded", "1")
	_, err = pb.NewKVClient(api.ActiveConnection()).Put(forged, &pb.PutRequest{Key: []byte("forged"), Value: []byte("must fail")})
	require.Error(t, err)
	require.Contains(t, []codes.Code{codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument}, status.Code(err))
}
