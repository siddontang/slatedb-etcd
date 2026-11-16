//go:build robustness && (linux || darwin)

package compat

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type faultWatchResult struct {
	events []*mvccpb.Event
	err    error
}

func startFaultWatches(t *testing.T, c *cluster, ctx context.Context) []<-chan faultWatchResult {
	t.Helper()
	var results []<-chan faultWatchResult
	for _, address := range c.addresses {
		api := client(t, []string{address})
		output := make(chan faultWatchResult, 1)
		results = append(results, output)
		go func() {
			result := faultWatchResult{}
			defer func() { output <- result }()
			watch := api.Watch(ctx, "", clientv3.WithPrefix(), clientv3.WithRev(1), clientv3.WithPrevKV())
			tick := time.NewTicker(100 * time.Millisecond)
			defer tick.Stop()
			var lastEvent, progress int64 = 1, 0
			for {
				select {
				case <-tick.C:
					// Explicit progress also exercises bookmarks during fault recovery.
					_ = api.RequestProgress(ctx)
				case response, ok := <-watch:
					if !ok {
						result.err = fmt.Errorf("watch closed before the barrier")
						return
					}
					if response.Err() != nil {
						result.err = response.Err()
						return
					}
					if response.IsProgressNotify() {
						if response.Header.Revision < progress || response.Header.Revision < lastEvent {
							result.err = fmt.Errorf("progress moved backwards")
							return
						}
						progress = response.Header.Revision
					}
					if len(response.Events) > 0 && response.Events[0].Kv.ModRevision <= lastEvent {
						result.err = fmt.Errorf("duplicate or split revision")
						return
					}
					barrier := false
					for _, event := range response.Events {
						rev := event.Kv.ModRevision
						if rev <= progress || rev < lastEvent || rev > lastEvent+1 || rev > response.Header.Revision {
							result.err = fmt.Errorf("invalid revision %d after %d; progress %d", rev, lastEvent, progress)
							return
						}
						lastEvent = rev
						result.events = append(result.events, (*mvccpb.Event)(event))
						barrier = barrier || string(event.Kv.Key) == "__watch_barrier"
					}
					if barrier {
						return
					}
				case <-ctx.Done():
					result.err = ctx.Err()
					return
				}
			}
		}()
	}
	return results
}

func finishFaultWatches(t *testing.T, results []<-chan faultWatchResult, revision int64,
	final []*mvccpb.KeyValue) [][]*mvccpb.Event {
	t.Helper()
	var histories [][]*mvccpb.Event
	for _, output := range results {
		result := <-output
		require.NoError(t, result.err)
		require.NotEmpty(t, result.events)
		require.Equal(t, revision, result.events[len(result.events)-1].Kv.ModRevision)
		state := map[string]*mvccpb.KeyValue{}
		for _, event := range result.events {
			key := string(event.Kv.Key)
			previous := state[key]
			require.True(t, proto.Equal(previous, event.PrevKv), "wrong previous value for %q", key)
			if event.Type == mvccpb.DELETE {
				require.NotNil(t, previous, "delete for missing key %q", key)
				delete(state, key)
			} else {
				state[key] = event.Kv
			}
		}
		require.Len(t, state, len(final))
		for _, kv := range final {
			require.True(t, proto.Equal(kv, state[string(kv.Key)]))
		}
		if len(histories) > 0 {
			require.Equal(t, histories[0], result.events)
		}
		histories = append(histories, result.events)
	}
	return histories
}
