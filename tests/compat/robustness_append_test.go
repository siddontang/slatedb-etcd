//go:build robustness && (linux || darwin)

package compat

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
	recording "go.etcd.io/etcd/tests/v3/robustness/client"
)

// A revision guard makes the read and append sequence one logical transaction.
func appendTransaction(ctx context.Context, c *recording.RecordingClient, value int) (
	invoke, result, kind string, err error,
) {
	invoke = "[[:r :a nil] [:r :b nil]]"
	if value != 0 {
		invoke = fmt.Sprintf("[[:r :a nil] [:append :a %d] [:r :b nil] [:append :b %d]]", value, value)
	}
	response, err := c.Range(ctx, "list/", "list0", 0, 0)
	if err != nil {
		return invoke, invoke, "fail", err
	}
	lists, revisions := [2]string{}, [2]int64{}
	for _, kv := range response.Kvs {
		index := -1
		if string(kv.Key) == "list/a" {
			index = 0
		}
		if string(kv.Key) == "list/b" {
			index = 1
		}
		if index < 0 {
			return invoke, invoke, "fail", fmt.Errorf("unexpected list key: %q", kv.Key)
		}
		for _, item := range strings.Fields(string(kv.Value)) {
			if _, err := strconv.Atoi(item); err != nil {
				return invoke, invoke, "fail", err
			}
		}
		lists[index], revisions[index] = string(kv.Value), kv.ModRevision
	}
	result = fmt.Sprintf("[[:r :a [%s]] [:r :b [%s]]]", lists[0], lists[1])
	if value == 0 {
		return invoke, result, "ok", nil
	}
	result = fmt.Sprintf("[[:r :a [%s]] [:append :a %d] [:r :b [%s]] [:append :b %d]]",
		lists[0], value, lists[1], value)
	txn, err := c.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision("list/a"), "=", revisions[0]),
		clientv3.Compare(clientv3.ModRevision("list/b"), "=", revisions[1]),
	).Then(
		clientv3.OpPut("list/a", strings.TrimSpace(fmt.Sprintf("%s %d", lists[0], value))),
		clientv3.OpPut("list/b", strings.TrimSpace(fmt.Sprintf("%s %d", lists[1], value))),
	).Commit()
	if err != nil {
		return invoke, invoke, "info", err
	}
	if !txn.Succeeded {
		return invoke, invoke, "fail", fmt.Errorf("revision guard failed")
	}
	return invoke, result, "ok", nil
}
