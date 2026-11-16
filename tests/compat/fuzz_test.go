package compat

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/status"
)

// Each input defines a bounded API sequence against two independent clusters.
func FuzzKVSequence(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	f.Add([]byte{255, 0, 128, 31, 42, 9, 52, 66, 1})
	f.Add([]byte("revision-history-transaction-delete-recreate"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 || len(data) > 64 {
			t.Skip("sequence length must be 1 to 64")
		}
		cluster := newCluster(t)
		actual, expected := client(t, cluster.addresses), reference(t)
		check := func(op clientv3.Op) {
			t.Helper()
			a, ae := call(actual, op)
			b, be := call(expected, op)
			if ae != nil || be != nil {
				if status.Code(ae) != status.Code(be) ||
					status.Convert(ae).Message() != status.Convert(be).Message() {
					t.Fatalf("input %x: service error %v; etcd error %v", data, ae, be)
				}
				return
			}
			normalize(a)
			normalize(b)
			if !proto.Equal(a, b) {
				t.Fatalf("input %x: service %s; etcd %s", data, a, b)
			}
		}
		for i, b := range data {
			key, value := fmt.Sprintf("k%d", data[(i+1)%len(data)]%4), fmt.Sprintf("v%d-%d", i, b)
			var op clientv3.Op
			switch b % 8 {
			case 0:
				op = clientv3.OpPut(key, value, clientv3.WithPrevKV())
			case 1:
				op = clientv3.OpGet(key)
			case 2:
				op = clientv3.OpDelete(key, clientv3.WithPrevKV())
			case 3:
				op = clientv3.OpGet("k", clientv3.WithPrefix(), clientv3.WithLimit(int64(b%3+1)))
			case 4:
				op = clientv3.OpGet(key, clientv3.WithRev(int64(b%12+1)))
			case 5:
				op = clientv3.OpTxn([]clientv3.Cmp{clientv3.Compare(clientv3.Version(key), "=", 0)},
					[]clientv3.Op{clientv3.OpPut(key, value)}, []clientv3.Op{clientv3.OpGet(key)})
			case 6:
				op = clientv3.OpTxn(nil, []clientv3.Op{clientv3.OpPut(key, value),
					clientv3.OpGet("k", clientv3.WithPrefix())}, nil)
			case 7:
				op = clientv3.OpPut(key, "", clientv3.WithIgnoreValue(), clientv3.WithPrevKV())
			}
			check(op)
		}
		check(clientv3.OpGet("\x00", clientv3.WithFromKey()))
	})
}
