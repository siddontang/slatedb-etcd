package compat

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestWatchProgressKeepsAFutureStartAcrossAReconnect checks the resume revision of the official
// client. The client applies a stream-wide progress reply to each watch that it knows. A watch
// from a revision far ahead must then still resume there after a reconnect.
func TestWatchProgressKeepsAFutureStartAcrossAReconnect(t *testing.T) {
	c := newCluster(t)
	for name, api := range map[string]*clientv3.Client{
		"service": client(t, c.addresses), "etcd": reference(t),
	} {
		t.Run(name, func(t *testing.T) { checkProgressResume(t, api) })
	}
}

// watchProxy forwards Watch streams to a member. It ends the first stream on `cut`, and it
// reports the start revision of each later create of the key `future`.
type watchProxy struct {
	pb.UnimplementedWatchServer
	upstream pb.WatchClient
	future   string
	streams  atomic.Int32
	cut      chan struct{}
	resumed  chan int64
}

func (p *watchProxy) Watch(down pb.Watch_WatchServer) error {
	session := p.streams.Add(1)
	ctx, cancel := context.WithCancel(down.Context())
	defer cancel()
	up, err := p.upstream.Watch(ctx)
	if err != nil {
		return err
	}
	errs := make(chan error, 2)
	go func() {
		for {
			request, err := down.Recv()
			if err != nil {
				errs <- err
				return
			}
			create := request.GetCreateRequest()
			if session > 1 && create != nil && string(create.Key) == p.future {
				select {
				case p.resumed <- create.StartRevision:
				default:
				}
			}
			if err := up.Send(request); err != nil {
				errs <- err
				return
			}
		}
	}()
	go func() {
		for {
			response, err := up.Recv()
			if err != nil {
				errs <- err
				return
			}
			if err := down.Send(response); err != nil {
				errs <- err
				return
			}
		}
	}()
	if session == 1 {
		select {
		case <-p.cut:
			return status.Error(codes.Unavailable, "the proxy ends the stream")
		case err := <-errs:
			return err
		}
	}
	return <-errs
}

func checkProgressResume(t *testing.T, api *clientv3.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	put, err := api.Put(ctx, "progress-old", "1")
	require.NoError(t, err)
	proxy := &watchProxy{
		upstream: pb.NewWatchClient(api.ActiveConnection()),
		future:   "progress-future",
		cut:      make(chan struct{}),
		resumed:  make(chan int64, 1),
	}
	listener := listenLocal(t)
	server := grpc.NewServer()
	pb.RegisterWatchServer(server, proxy)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	watcher := client(t, []string{listener.Addr().String()})
	// One context, so the watches and the progress requests share one stream.
	streamCtx, stop := context.WithCancel(ctx)
	defer stop()
	old := watcher.Watch(streamCtx, "progress-old", clientv3.WithRev(1), clientv3.WithCreatedNotify())
	if created := <-old; !created.Created {
		t.Fatalf("the first watch was not created: %v", created.Err())
	}
	// A progress request before the create, and one after it. Neither reply may reach the
	// future watch with a revision below its start.
	require.NoError(t, watcher.RequestProgress(streamCtx))
	start := put.Header.Revision + 50
	future := watcher.Watch(streamCtx, proxy.future, clientv3.WithRev(start),
		clientv3.WithCreatedNotify())
	if created := <-future; !created.Created {
		t.Fatalf("the future watch was not created: %v", created.Err())
	}
	require.NoError(t, watcher.RequestProgress(streamCtx))
	select {
	case response := <-future:
		t.Fatalf("the future watch got a response before the cut: %+v", response)
	case <-time.After(500 * time.Millisecond):
	}
	close(proxy.cut)
	select {
	case resumed := <-proxy.resumed:
		if resumed < start {
			t.Fatalf("the future watch resumed at %d, below its start %d", resumed, start)
		}
	case <-ctx.Done():
		t.Fatal("the client did not create the future watch again")
	}
}
