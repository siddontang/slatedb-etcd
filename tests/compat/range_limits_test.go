package compat

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A read whose rows exceed --max-response-bytes fails with an explicit size error. Limits,
// counts, and keys-only reads still return.
func TestMaxResponseBytes(t *testing.T) {
	c := newClusterSize(t, 1, "--max-response-bytes", "4096")
	cli := client(t, c.addresses)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	value := strings.Repeat("v", 1000)
	for i := 0; i < 10; i++ {
		if _, err := cli.Put(ctx, fmt.Sprintf("big/%02d", i), value); err != nil {
			t.Fatal(err)
		}
	}
	tooLarge := func(name string, err error) {
		t.Helper()
		if status.Code(err) != codes.ResourceExhausted || status.Convert(err).Message() != "etcdserver: range response is too large" {
			t.Fatalf("%s: got %v, want the response size error", name, err)
		}
	}
	_, err := cli.Get(ctx, "big/", clientv3.WithPrefix())
	tooLarge("prefix", err)
	_, err = cli.Get(ctx, "big/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend))
	tooLarge("sorted", err)
	_, err = cli.Get(ctx, "big/", clientv3.WithPrefix(), clientv3.WithRev(8))
	tooLarge("historical", err)
	_, err = cli.Get(ctx, "big/", clientv3.WithPrefix(), clientv3.WithSerializable())
	tooLarge("serializable", err)
	_, err = cli.Txn(ctx).Then(clientv3.OpGet("big/0", clientv3.WithPrefix()), clientv3.OpGet("big/0", clientv3.WithPrefix())).Commit()
	tooLarge("txn", err)

	r, err := cli.Get(ctx, "big/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend), clientv3.WithLimit(3))
	if err != nil || len(r.Kvs) != 3 || !r.More || r.Count != 10 || string(r.Kvs[0].Key) != "big/09" {
		t.Fatalf("sorted limit: %v %v", r, err)
	}
	r, err = cli.Get(ctx, "big/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil || len(r.Kvs) != 10 || len(r.Kvs[0].Value) != 0 {
		t.Fatalf("keys only: %v %v", r, err)
	}
	r, err = cli.Get(ctx, "big/", clientv3.WithPrefix(), clientv3.WithCountOnly())
	if err != nil || r.Count != 10 {
		t.Fatalf("count only: %v %v", r, err)
	}
	// A write transaction ignores the member-local cap and returns every row.
	txn, err := cli.Txn(ctx).Then(clientv3.OpPut("other", "v"), clientv3.OpGet("big/", clientv3.WithPrefix())).Commit()
	if err != nil || len(txn.Responses[1].GetResponseRange().Kvs) != 10 {
		t.Fatalf("write txn: %v %v", txn, err)
	}
}
