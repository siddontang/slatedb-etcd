// Measure physical compaction while the other client workers keep sending requests.
package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type compactionSchedule struct {
	every     int64
	retain    int64
	writes    atomic.Int64
	mu        sync.Mutex
	boundary  int64
	latencies []float64
}

func (s *compactionSchedule) afterPut(
	ctx context.Context,
	client *clientv3.Client,
	revision int64,
) error {
	if s.every == 0 || s.writes.Add(1)%s.every != 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	target := revision - s.retain
	if target <= s.boundary {
		return nil
	}
	start := time.Now()
	if _, err := client.Compact(ctx, target, clientv3.WithCompactPhysical()); err != nil {
		return err
	}
	s.latencies = append(s.latencies, float64(time.Since(start).Nanoseconds())/1e6)
	s.boundary = target
	return nil
}
