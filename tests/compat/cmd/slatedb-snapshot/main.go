// Export the native snapshot format without etcdctl's bbolt size assumptions.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Snapshot export failed:", err)
		os.Exit(1)
	}
}

func run() error {
	endpoints := flag.String("endpoints", "http://127.0.0.1:7001", "Comma-separated client endpoints")
	output := flag.String("output", "", "New native backup file; existing files are never replaced")
	user := flag.String("user", "", "User name; read its password from SLATEDB_SNAPSHOT_PASSWORD")
	ca := flag.String("cacert", "", "Trusted CA certificate file")
	cert := flag.String("cert", "", "TLS client certificate file")
	key := flag.String("key", "", "TLS client private key file")
	timeout := flag.Duration("timeout", 30*time.Second, "Snapshot stream timeout")
	flag.Parse()
	if *output == "" || flag.NArg() != 0 || *timeout <= 0 {
		return errors.New("provide --output, a positive --timeout, and no positional arguments")
	}
	cfg := clientv3.Config{Endpoints: strings.Split(*endpoints, ","), DialTimeout: 5 * time.Second,
		Username: *user, Password: os.Getenv("SLATEDB_SNAPSHOT_PASSWORD")}
	if *ca != "" || *cert != "" || *key != "" {
		var err error
		cfg.TLS, err = (transport.TLSInfo{TrustedCAFile: *ca, CertFile: *cert, KeyFile: *key}).ClientConfig()
		if err != nil {
			return err
		}
	}
	client, err := clientv3.New(cfg)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	stream, err := client.Snapshot(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()
	if err := saveSnapshot(stream, *output, 64*1024*1024); err != nil {
		return err
	}
	fmt.Println("Saved checked native snapshot:", *output)
	return nil
}

func saveSnapshot(stream io.Reader, output string, limit int64) error {
	file, err := os.CreateTemp(filepath.Dir(output), ".native-snapshot-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	count, err := io.Copy(file, io.LimitReader(stream, limit+1))
	if err != nil {
		return err
	}
	if count > limit || count <= sha256.Size {
		return errors.New("native snapshot is too large or lacks its checksum")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	hash := sha256.New()
	if _, err := io.CopyN(hash, file, count-sha256.Size); err != nil {
		return err
	}
	checksum := make([]byte, sha256.Size)
	if _, err := io.ReadFull(file, checksum); err != nil {
		return err
	}
	if !bytes.Equal(checksum, hash.Sum(nil)) {
		return errors.New("native snapshot checksum mismatch")
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	// A hard link publishes the checked file atomically and refuses an existing destination.
	if err := os.Link(file.Name(), output); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(output))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
