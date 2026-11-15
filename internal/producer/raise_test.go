package producer

import (
	"context"
	"log/slog"
	"net"
	"os/exec"
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

// How an encoder spends its ceiling is what the CP's auto_raise goes by
// (§38.5): only a VBR source is raised for starvation.
func TestRateControlOf(t *testing.T) {
	for encoder, want := range map[string]api.RateControl{
		"":             api.RateControl_RATE_CONTROL_UNSPECIFIED,
		"copy":         api.RateControl_RATE_CONTROL_COPY,
		"h264_v4l2m2m": api.RateControl_RATE_CONTROL_CBR,
		"x264enc":      api.RateControl_RATE_CONTROL_CBR,
		"libx264":      api.RateControl_RATE_CONTROL_VBR,
		"h264_vaapi":   api.RateControl_RATE_CONTROL_VBR,
		"h264_nvenc":   api.RateControl_RATE_CONTROL_VBR,
		"v4l2h264enc":  api.RateControl_RATE_CONTROL_VBR,
	} {
		require.Equal(t, want, rateControlOf(encoder), encoder)
	}
}

// A cut on schedule asks Boundary (§38.5): true closes the segment there as
// any other cut, and the rest of the stream is not kept, the next stream
// opening the next segment. An early cut is the ceiling's guard and does
// not ask.
func TestCutterEndsAtBoundary(t *testing.T) {
	t0 := time.Date(2026, 11, 2, 6, 0, 0, 0, time.UTC)
	var out []*Segment
	asked, end := 0, false
	c := &Cutter{
		Schedule: func() Schedule { return Schedule{Duration: time.Second, Ceiling: 12} },
		Now:      func() time.Time { return t0.Add(time.Hour) },
		Prefix:   func() []byte { return []byte("HDR") },
		Out:      func(s *Segment) { out = append(out, s) },
		Boundary: func() bool { asked++; return end },
	}
	c.Feed(&Frame{Kind: FrameData, Time: t0, Payload: []byte("aaaa")})
	require.Zero(t, asked, "the first segment of a stream ends nothing")
	c.Feed(&Frame{Kind: FrameData, Time: t0.Add(300 * time.Millisecond), Payload: []byte("bbbbbbbb")})
	c.Feed(&Frame{Kind: FrameData, Time: t0.Add(600 * time.Millisecond), Payload: []byte("cc")})
	require.Zero(t, asked, "an early cut does not ask")
	require.Len(t, out, 1)
	require.True(t, out[0].Early)

	c.Feed(&Frame{Kind: FrameData, Time: t0.Add(1100 * time.Millisecond), Payload: []byte("dd")})
	require.Equal(t, 1, asked)
	require.False(t, c.Ended(), "nothing to take up: cut as always")
	require.Len(t, out, 2)

	end = true
	c.Feed(&Frame{Kind: FrameData, Time: t0.Add(2000 * time.Millisecond), Payload: []byte("ee")})
	require.True(t, c.Ended())
	require.Len(t, out, 3)
	last := out[2]
	require.Equal(t, "HDRdd", string(last.Bytes()))
	require.True(t, last.Ended.Equal(t0.Add(2000*time.Millisecond)), "closed at the boundary")
	require.False(t, last.Early)
	require.False(t, last.Stopped, "a segment cut on schedule, not a stopped source's")
	c.Feed(&Frame{Kind: FrameData, Time: t0.Add(2100 * time.Millisecond), Payload: []byte("ff")})
	c.Stop()
	require.Len(t, out, 3, "the old encoder's tail is not kept")
	require.Nil(t, c.Current())
}

// raisingCP agrees each proposal, or the floor it is told when that is
// more, as the CP does for a source auto_raise raised (§38.5, §12.6); and
// answers every heartbeat at a profile version past the one agreed.
type raisingCP struct {
	version atomic.Int64
	floor   atomic.Int64

	mu      sync.Mutex
	reports []*api.SourceReport
}

type (
	raisingSets struct {
		api.UnimplementedSetServiceServer
		*raisingCP
	}
	raisingProducers struct {
		api.UnimplementedProducerServiceServer
		*raisingCP
	}
)

func (f raisingSets) Negotiate(_ context.Context, req *api.SetNegotiateRequest) (*api.SetNegotiateResponse, error) {
	var agreed []*api.SourceAgreed
	for _, sp := range req.GetSources() {
		agreed = append(agreed, api.SourceAgreed_builder{
			SourceId: sp.GetSource().GetId(),
			Profile:  api.SegmentProfile_builder{MaxBitrate: max(sp.GetProfile().GetMaxBitrate(), f.floor.Load()), DurationSeconds: 2, KeyframeIntervalMs: 1000}.Build(),
		}.Build())
	}

	return api.SetNegotiateResponse_builder{
		ProfileVersion: f.version.Load(),
		Link:           api.LinkProfile_builder{Mode: api.UploadMode_UPLOAD_MODE_LIVE, AllocationHorizonSeconds: 1}.Build(),
		Sources:        agreed,
	}.Build(), nil
}

func (f raisingProducers) Heartbeat(_ context.Context, req *api.ProducerHeartbeatRequest) (*api.ProducerHeartbeatResponse, error) {
	f.mu.Lock()
	f.reports = append(f.reports, req.GetSources()...)
	f.mu.Unlock()

	return api.ProducerHeartbeatResponse_builder{ProfileVersion: f.version.Add(1)}.Build(), nil
}

// The producer's half of auto_raise (§38.5): a ceiling the CP raised is
// agreed at the negotiation the heartbeat's newer profile version starts,
// and the encoder is started again with it at the next segment boundary,
// a restart on purpose that the heartbeat does not count. The heartbeat
// says the encoder is VBR, which is what the CP raises.
func TestRaisedCeilingReachesTheEncoderAtTheBoundary(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("no ffmpeg on this host")
	}
	cp := &raisingCP{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	api.RegisterSetServiceServer(srv, raisingSets{raisingCP: cp})
	api.RegisterProducerServiceServer(srv, raisingProducers{raisingCP: cp})
	go srv.Serve(lis)
	defer srv.Stop()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	// Well above what the picture needs, so no early cut raises it first.
	const ceiling, raised = 2_000_000, 2_500_000
	p, err := New(Config{
		StateDir:          t.TempDir(),
		Ffmpeg:            ffmpeg,
		Sources:           []SourceConfig{{Alias: "door", Input: "demo:", Size: "320x240", Fps: 10, Encoder: "libx264", MaxBitrate: ceiling, Audio: &AudioConfig{Codec: "none"}}},
		HeartbeatInterval: 100 * time.Millisecond,
		Log:               slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
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

	encoding := func() int64 {
		s.capture.mu.Lock()
		defer s.capture.mu.Unlock()
		if !s.capture.up {
			return 0
		}
		return s.capture.running.ceiling
	}
	require.Eventually(t, func() bool { return encoding() == ceiling }, 20*time.Second, 20*time.Millisecond, "the encoder runs at the agreed ceiling")
	require.Eventually(t, func() bool {
		cp.mu.Lock()
		defer cp.mu.Unlock()
		for _, r := range cp.reports {
			if r.GetRateControl() == api.RateControl_RATE_CONTROL_VBR {
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond, "the heartbeat says libx264 is VBR")
	// A segment cut on schedule before the raise, so the restart has a
	// boundary to wait for.
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.pending) >= 2
	}, 20*time.Second, 20*time.Millisecond)

	cp.floor.Store(raised)
	require.Eventually(t, func() bool { return encoding() == raised }, 20*time.Second, 20*time.Millisecond, "the encoder runs at the raised ceiling")
	require.Zero(t, s.capture.Restarts(), "a restart on purpose is not a restart")
	require.False(t, s.capture.Outdated())

	// A raise the producer proposes itself after an early cut moves the
	// declaration and leaves the encoder alone until its next start.
	s.mu.Lock()
	s.raise = true
	segments := len(s.pending)
	s.mu.Unlock()
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.ceiling == raised*5/4 && len(s.pending) >= segments+2
	}, 20*time.Second, 20*time.Millisecond, "agreed, and a segment boundary passed since")
	require.Equal(t, int64(raised), encoding())
	require.True(t, s.capture.Outdated(), "taken at the encoder's next start")

	cancel()
	require.NoError(t, g.Wait())
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, seg := range s.pending[:len(s.pending)-1] {
		require.True(t, seg.Closed())
		require.False(t, seg.Stopped, "every segment before the last closed on schedule, the one before the restart at its boundary")
	}
}
