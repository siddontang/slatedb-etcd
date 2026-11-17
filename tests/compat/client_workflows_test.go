package compat

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Run the same client workflows against this service and the reference server.
func TestOfficialClientWorkflows(t *testing.T) {
	for _, backend := range []string{"service", "etcd"} {
		t.Run(backend, func(t *testing.T) {
			var api *clientv3.Client
			if backend == "etcd" {
				api = reference(t)
			} else {
				c := newCluster(t)
				api = client(t, c.addresses)
			}
			t.Run("mutex", func(t *testing.T) { mutexWorkflow(t, api) })
			t.Run("election", func(t *testing.T) { electionWorkflow(t, api) })
			for _, mode := range []concurrency.Isolation{
				concurrency.SerializableSnapshot, concurrency.Serializable,
				concurrency.RepeatableReads,
			} {
				t.Run(fmt.Sprintf("stm-%d", mode), func(t *testing.T) { stmWorkflow(t, api, mode) })
			}
		})
	}
}

func mutexWorkflow(t *testing.T, api *clientv3.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sessions := make([]*concurrency.Session, 3)
	for i := range sessions {
		session, err := concurrency.NewSession(api, concurrency.WithTTL(10), concurrency.WithContext(ctx))
		require.NoError(t, err)
		sessions[i] = session
		defer session.Close()
	}
	owner := concurrency.NewMutex(sessions[0], "/workflows/mutex")
	waiter := concurrency.NewMutex(sessions[1], "/workflows/mutex")
	require.NoError(t, owner.Lock(ctx))
	require.ErrorIs(t, waiter.TryLock(ctx), concurrency.ErrLocked)
	locked := make(chan error, 1)
	go func() { locked <- waiter.Lock(ctx) }()
	select {
	case err := <-locked:
		t.Fatalf("waiter passed an active owner: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	// Session revocation must delete the owner key and wake the waiting watch.
	require.NoError(t, sessions[0].Close())
	require.NoError(t, <-locked)
	require.NoError(t, waiter.Unlock(ctx))

	var active, completed atomic.Int32
	errors := make(chan error, 2)
	for _, session := range sessions[1:] {
		go func(session *concurrency.Session) {
			mutex := concurrency.NewMutex(session, "/workflows/mutex")
			for n := 0; n < 5; n++ {
				if err := mutex.Lock(ctx); err != nil {
					errors <- err
					return
				}
				if active.Add(1) != 1 {
					errors <- fmt.Errorf("two mutex owners")
					return
				}
				time.Sleep(time.Millisecond)
				completed.Add(1)
				active.Add(-1)
				if err := mutex.Unlock(ctx); err != nil {
					errors <- err
					return
				}
			}
			errors <- nil
		}(session)
	}
	require.NoError(t, <-errors)
	require.NoError(t, <-errors)
	require.EqualValues(t, 10, completed.Load())
}

func electionWorkflow(t *testing.T, api *clientv3.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	first, err := concurrency.NewSession(api, concurrency.WithTTL(10), concurrency.WithContext(ctx))
	require.NoError(t, err)
	defer first.Close()
	second, err := concurrency.NewSession(api, concurrency.WithTTL(10), concurrency.WithContext(ctx))
	require.NoError(t, err)
	defer second.Close()
	a, b := concurrency.NewElection(first, "/workflows/election"),
		concurrency.NewElection(second, "/workflows/election")
	require.NoError(t, a.Campaign(ctx, "first"))
	observed := b.Observe(ctx)
	next := func(value string) {
		t.Helper()
		select {
		case result, ok := <-observed:
			require.True(t, ok)
			require.Len(t, result.Kvs, 1)
			require.Equal(t, value, string(result.Kvs[0].Value))
		case <-ctx.Done():
			t.Fatal("election observation timed out")
		}
	}
	next("first")
	require.NoError(t, a.Proclaim(ctx, "updated"))
	next("updated")
	elected := make(chan error, 1)
	go func() { elected <- b.Campaign(ctx, "second") }()
	select {
	case err := <-elected:
		t.Fatalf("two elected leaders: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	require.NoError(t, a.Resign(ctx))
	require.NoError(t, <-elected)
	next("second")
	leader, err := a.Leader(ctx)
	require.NoError(t, err)
	require.Equal(t, "second", string(leader.Kvs[0].Value))
	require.NoError(t, second.Close())
	_, err = a.Leader(ctx)
	require.ErrorIs(t, err, concurrency.ErrElectionNoLeader)
}

func stmWorkflow(t *testing.T, api *clientv3.Client, mode concurrency.Isolation) {
	// Keep revision-bound serializable reads on the member that returned the revision.
	api = client(t, api.Endpoints()[:1])
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("/workflows/stm-%d/", mode)
	for n := 0; n < 4; n++ {
		_, err := api.Put(ctx, prefix+strconv.Itoa(n), "100")
		require.NoError(t, err)
	}
	var wg sync.WaitGroup
	errors := make(chan error, 4)
	var calls atomic.Int32
	// All first attempts read the same state before any attempt can commit.
	// This forces transaction conflicts and tests the client's retry path.
	var arrived atomic.Int32
	start := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for n := 0; n < 8; n++ {
				firstAttempt := true
				_, err := concurrency.NewSTM(api, func(stm concurrency.STM) error {
					calls.Add(1)
					from, to := prefix+strconv.Itoa(worker), prefix+strconv.Itoa((worker+1)%4)
					left, err := strconv.Atoi(stm.Get(from))
					if err != nil {
						return err
					}
					right, err := strconv.Atoi(stm.Get(to))
					if err != nil {
						return err
					}
					if n == 0 && firstAttempt {
						firstAttempt = false
						if arrived.Add(1) == 4 {
							close(start)
						}
						select {
						case <-start:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					stm.Put(from, strconv.Itoa(left-1))
					stm.Put(to, strconv.Itoa(right+1))
					return nil
				}, concurrency.WithAbortContext(ctx), concurrency.WithIsolation(mode))
				if err != nil {
					errors <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	result, err := api.Get(ctx, prefix, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, result.Kvs, 4)
	total := 0
	for _, kv := range result.Kvs {
		value, err := strconv.Atoi(string(kv.Value))
		require.NoError(t, err)
		require.Equal(t, 100, value)
		total += value
	}
	require.Equal(t, 400, total)
	require.Greater(t, calls.Load(), int32(32), "no conflict retry was observed")
}
