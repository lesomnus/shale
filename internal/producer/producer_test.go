package producer

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// fakeCP is the part of a control plane a recording producer calls on
// (§12.6, §38.6): it agrees to every proposal as it was made, and answers
// every heartbeat and every allocation round at a profile version past the
// one agreed, so that each of them negotiates again.
type fakeCP struct {
	version atomic.Int64

	mu       sync.Mutex
	proposed []int64
	reports  []*api.SourceReport
}

// fakeSets and fakeProducers are the fake's SetService and ProducerService.
type (
	fakeSets struct {
		api.UnimplementedSetServiceServer
		*fakeCP
	}
	fakeProducers struct {
		api.UnimplementedProducerServiceServer
		*fakeCP
	}
)

func (f fakeSets) Negotiate(_ context.Context, req *api.SetNegotiateRequest) (*api.SetNegotiateResponse, error) {
	var agreed []*api.SourceAgreed
	f.mu.Lock()
	for _, sp := range req.GetSources() {
		f.proposed = append(f.proposed, sp.GetProfile().GetMaxBitrate())
		agreed = append(agreed, api.SourceAgreed_builder{
			SourceId: sp.GetSource().GetId(),
			Profile:  api.SegmentProfile_builder{MaxBitrate: sp.GetProfile().GetMaxBitrate(), DurationSeconds: 4, KeyframeIntervalMs: 2000}.Build(),
		}.Build())
	}
	f.mu.Unlock()

	return api.SetNegotiateResponse_builder{
		ProfileVersion: f.version.Load(),
		Link:           api.LinkProfile_builder{Mode: api.UploadMode_UPLOAD_MODE_LIVE, AllocationHorizonSeconds: 1}.Build(),
		Sources:        agreed,
	}.Build(), nil
}

func (f fakeSets) Allocate(context.Context, *api.SetAllocateRequest) (*api.SetAllocateResponse, error) {
	return api.SetAllocateResponse_builder{ProfileVersion: f.version.Add(1)}.Build(), nil
}

func (f fakeProducers) Heartbeat(_ context.Context, req *api.ProducerHeartbeatRequest) (*api.ProducerHeartbeatResponse, error) {
	f.mu.Lock()
	f.reports = append(f.reports, req.GetSources()...)
	f.mu.Unlock()

	return api.ProducerHeartbeatResponse_builder{ProfileVersion: f.version.Add(1)}.Build(), nil
}

// The producer's loops at once (§38.6, §12.6, §38.5): a capture cuts a
// recording while the heartbeat reports what it measured, the stall watch
// asks its keyframe interval, and every heartbeat, every allocation round
// and every early cut negotiates again. Each loop is a goroutine of its
// own and they share the source, so this is where sharing it without a
// lock shows under the race detector (CI's race job); what it checks
// besides is that the heartbeat says what the capture measured and that an
// early cut raises the ceiling.
func TestLoopsShareTheSource(t *testing.T) {
	cp := &fakeCP{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	api.RegisterSetServiceServer(srv, fakeSets{fakeCP: cp})
	api.RegisterProducerServiceServer(srv, fakeProducers{fakeCP: cp})
	go srv.Serve(lis)
	defer srv.Stop()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	// A ceiling well below what the recording carries: every segment is cut
	// early at the next keyframe (§38.2).
	const ceiling = 80_000
	p, err := New(Config{
		StateDir:          t.TempDir(),
		Sources:           []SourceConfig{{Alias: "door", Input: "raw:testdata/av.mp4", Fps: 30, MaxBitrate: ceiling}},
		HeartbeatInterval: 100 * time.Millisecond,
		Log:               slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	// What Run has by the time the loops start: a connection, the set, and
	// the source's row.
	p.conn.Store(conn)
	p.set = api.Set_builder{Id: pdid.New(pdid.Domain(20)).Bytes()}.Build()
	p.members = 1
	s := p.order[0]
	s.row = api.Source_builder{Id: pdid.New(pdid.Domain(23)).Bytes(), Ordinal: 1}.Build()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, p.negotiate(ctx))

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return p.capture(gctx, s) })
	g.Go(func() error { return p.heartbeats(gctx) })
	g.Go(func() error { return p.allocations(gctx) })
	g.Go(func() error { return p.ticks(gctx) })

	// The heartbeat says the input is up and what keyframe interval the
	// capture measured: the recording's, a keyframe every 2 s.
	var measured time.Duration
	require.Eventually(t, func() bool {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		for _, r := range cp.reports {
			if r.GetInputUp() && r.GetKeyframeIntervalMs() > 0 {
				measured = time.Duration(r.GetKeyframeIntervalMs()) * time.Millisecond
				return true
			}
		}

		return false
	}, 10*time.Second, 20*time.Millisecond, "a heartbeat reports the measured keyframe interval")
	require.InDelta(t, 2*time.Second, measured, float64(500*time.Millisecond))

	// The early cut is a ceiling that was wrong: the next negotiation
	// proposes a quarter more (§38.5).
	require.Eventually(t, func() bool {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		for _, v := range cp.proposed {
			if v == ceiling*5/4 {
				return true
			}
		}

		return false
	}, 10*time.Second, 20*time.Millisecond, "the ceiling raised after an early cut")

	cancel()
	require.NoError(t, g.Wait())
	cp.mu.Lock()
	negotiations := len(cp.proposed)
	cp.mu.Unlock()
	require.Greater(t, negotiations, 3, "the heartbeats and the allocation rounds negotiated again")
}
