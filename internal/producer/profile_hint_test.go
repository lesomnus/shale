package producer

import (
	"context"
	"log/slog"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/token"
	"github.com/lesomnus/shale/server/core"
	catalog "github.com/lesomnus/shale/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const oldHintBitrate, newHintBitrate int64 = 4_128_000, 1_128_000

type profileHintGate struct {
	entered, released chan struct{}
	enter, release    sync.Once
}

func newProfileHintGate(t *testing.T) *profileHintGate {
	g := &profileHintGate{entered: make(chan struct{}), released: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}
func (g *profileHintGate) open() { g.release.Do(func() { close(g.released) }) }
func (g *profileHintGate) wait(ctx context.Context) error {
	g.enter.Do(func() { close(g.entered) })
	select {
	case <-g.released:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type profileHintCP struct {
	version, calls, bitrate, lastCatalog atomic.Int64
	hint                                 int64
	allocation                           *api.Allocation
	response                             *profileHintGate
	mu                                   sync.Mutex
	commit                               *profileHintGate
}
type profileHintSets struct {
	api.UnimplementedSetServiceServer
	*profileHintCP
}
type profileHintProducers struct {
	api.UnimplementedProducerServiceServer
	*profileHintCP
}

func (f profileHintSets) Negotiate(ctx context.Context, r *api.SetNegotiateRequest) (*api.SetNegotiateResponse, error) {
	f.calls.Add(1)
	if !r.GetPreview() {
		f.lastCatalog.Store(r.GetCatalog().GetCatalogRevision())
	}
	var agreed []*api.SourceAgreed
	for _, proposal := range r.GetSources() {
		profile := proto.Clone(proposal.GetProfile()).(*api.SegmentProfile)
		profile.SetDurationSeconds(4)
		if !r.GetPreview() {
			f.bitrate.Store(profile.GetMaxBitrate())
		}
		agreed = append(agreed, api.SourceAgreed_builder{SourceId: proposal.GetSource().GetId(), Profile: profile, MaxLength: core.MaxLength(profile)}.Build())
	}
	version := f.version.Load()
	if !r.GetPreview() {
		f.mu.Lock()
		gate := f.commit
		f.commit = nil
		f.mu.Unlock()
		if gate != nil {
			if err := gate.wait(ctx); err != nil {
				return nil, err
			}
		}
	}
	return api.SetNegotiateResponse_builder{ProfileVersion: version, Link: api.LinkProfile_builder{Mode: api.UploadMode_UPLOAD_MODE_LIVE, AllocationHorizonSeconds: 600}.Build(), Sources: agreed}.Build(), nil
}
func (f profileHintSets) Allocate(ctx context.Context, _ *api.SetAllocateRequest) (*api.SetAllocateResponse, error) {
	if err := f.response.wait(ctx); err != nil {
		return nil, err
	}
	return api.SetAllocateResponse_builder{ProfileVersion: f.hint, Allocations: []*api.Allocation{f.allocation}}.Build(), nil
}
func (f profileHintProducers) Heartbeat(ctx context.Context, _ *api.ProducerHeartbeatRequest) (*api.ProducerHeartbeatResponse, error) {
	if err := f.response.wait(ctx); err != nil {
		return nil, err
	}
	return api.ProducerHeartbeatResponse_builder{ProfileVersion: f.hint}.Build(), nil
}

type profileHintFixture struct {
	p         *Producer
	cp        *profileHintCP
	old, next *source
	snapshot  catalog.Snapshot
	verifier  *token.Verifier
	ctx       context.Context
}

func newProfileHintFixture(t *testing.T, hint int64) *profileHintFixture {
	t.Helper()
	p, err := New(Config{StateDir: t.TempDir(), Sources: []SourceConfig{{Alias: "camera", Input: "raw:unused", MaxBitrate: oldHintBitrate}}, Buffer: 64 << 20, Log: slog.New(slog.DiscardHandler)})
	require.NoError(t, err)
	p.id = pdid.New(DomProducer)
	p.set = api.Set_builder{Id: pdid.New(7).Bytes()}.Build()
	p.members = 1
	old := p.order[0]
	old.row = api.Source_builder{Id: pdid.New(8).Bytes(), Ordinal: 0}.Build()
	key, err := token.Generate("profile-hint-fixture")
	require.NoError(t, err)
	verifier := token.NewVerifier()
	verifier.Add(key.Kid, key.Public())
	profile := api.SegmentProfile_builder{MaxBitrate: oldHintBitrate, DurationSeconds: 4}.Build()
	started := time.Now().Truncate(4 * time.Second)
	budget := core.MaxLength(profile)
	node := pdid.New(24).Bytes()
	signed, err := key.Sign(api.TokenClaims_builder{Exp: timestamppb.New(time.Now().Add(time.Hour)), Iat: timestamppb.Now(), Aud: node, Op: api.TokenOp_TOKEN_OP_PUT, CaptureEpoch: "old-frozen-epoch", MaxLength: budget, Mode: api.UploadMode_UPLOAD_MODE_LIVE}.Build())
	require.NoError(t, err)
	allocation := api.Allocation_builder{SourceId: old.row.GetId(), LaminaId: pdid.New(9).Bytes(), ProfileVersion: 1, CaptureEpoch: "old-frozen-epoch", Profile: profile, MaxLength: budget, DateStarted: timestamppb.New(started), DateExpires: timestamppb.New(time.Now().Add(time.Hour)), Candidates: []*api.Candidate{api.Candidate_builder{Token: signed, NodeId: node}.Build()}}.Build()
	cp := &profileHintCP{hint: hint, allocation: allocation, response: newProfileHintGate(t)}
	cp.version.Store(1)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	api.RegisterSetServiceServer(server, profileHintSets{profileHintCP: cp})
	api.RegisterProducerServiceServer(server, profileHintProducers{profileHintCP: cp})
	go server.Serve(lis)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	p.conn.Store(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, p.negotiate(ctx))
	cfg := old.cfg
	cfg.MaxBitrate = newHintBitrate
	next, err := p.sourceFor(cfg)
	require.NoError(t, err)
	next.row = old.row
	snapshot := catalog.Snapshot{Revision: catalog.Revision{AssignmentID: pdid.New(28).String(), Generation: 1, Catalog: 2}}
	return &profileHintFixture{p: p, cp: cp, old: old, next: next, snapshot: snapshot, verifier: verifier, ctx: ctx}
}

// This wrapper is also a unique scheduling-barrier marker in goroutine stacks.
// No production hook or test-specific negotiation implementation is used.
func runProfileHintHandler(ctx context.Context, p *Producer, handler string, done chan<- error) {
	if handler == "heartbeat" {
		done <- p.heartbeat(ctx)
	} else {
		done <- p.allocations(ctx)
	}
}
func (f *profileHintFixture) start(t *testing.T, handler string) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(f.ctx)
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go runProfileHintHandler(ctx, f.p, handler, done)
	select {
	case <-f.cp.response.entered:
	case <-f.ctx.Done():
		t.Fatal("profile hint response did not reach its barrier")
	}
	return cancel, done
}
func (f *profileHintFixture) finish(t *testing.T, handler string, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	if handler == "allocations" {
		require.Eventually(t, func() bool {
			f.old.mu.Lock()
			defer f.old.mu.Unlock()
			return len(f.old.allocs) == 1
		}, 5*time.Second, time.Millisecond, "an old-version allocation must still reach the old FIFO")
		cancel()
	}
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-f.ctx.Done():
		t.Fatal("profile hint handler did not complete")
	}
	if handler == "allocations" {
		segment := &Segment{Started: f.cp.allocation.GetDateStarted().AsTime(), CaptureEpoch: "old-frozen-epoch"}
		got, err := f.p.allocationFor(f.ctx, f.old, segment)
		require.NoError(t, err)
		require.True(t, proto.Equal(f.cp.allocation, got), "the old FIFO retains its complete frozen allocation")
		claims, err := f.verifier.Verify(got.GetCandidates()[0].GetToken())
		require.NoError(t, err)
		require.NoError(t, token.Check(claims, got.GetCandidates()[0].GetNodeId(), api.TokenOp_TOKEN_OP_PUT))
		require.Equal(t, "old-frozen-epoch", claims.GetCaptureEpoch())
		require.Equal(t, f.cp.allocation.GetMaxLength(), claims.GetMaxLength())
		if f.next.profile != nil {
			require.Greater(t, claims.GetMaxLength(), core.MaxLength(f.next.profile), "an old signed budget remains larger than the replacement profile")
		}
	}
}

func TestDelayedProfileHintsDoNotRestoreDrainingProfile(t *testing.T) {
	for _, handler := range []string{"heartbeat", "allocations"} {
		for _, hint := range []int64{0, 1, 2} {
			t.Run(handler+"/"+string(rune('0'+hint)), func(t *testing.T) {
				f := newProfileHintFixture(t, hint)
				cancel, done := f.start(t, handler)
				// The lifecycle accepted the replacement catalog and committed its
				// smaller profile, while the old capture still drains as active.
				f.p.acceptCatalog(f.snapshot)
				f.cp.version.Store(2)
				require.NoError(t, f.p.negotiateSources(f.ctx, []*source{f.next}, f.snapshot, false))
				before := f.cp.calls.Load()
				require.Equal(t, newHintBitrate, f.cp.bitrate.Load())
				f.cp.response.open()
				f.finish(t, handler, cancel, done)
				assert.Equal(t, before, f.cp.calls.Load(), "zero, equal and delayed older hints must make no negotiation RPC")
				require.Equal(t, newHintBitrate, f.cp.bitrate.Load(), "a delayed response must not restore the draining capture's larger Source profile")
			})
		}
	}
}

func waitProfileHintNegotiationLock(t *testing.T, handler string) {
	t.Helper()
	// The stack is only a bounded scheduling barrier. Assertions use RPC count,
	// committed profile and signed allocation data; no stack text is logged.
	require.Eventually(t, func() bool {
		buf := make([]byte, 256<<10)
		n := runtime.Stack(buf, true)
		for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(stack, "runProfileHintHandler(") && strings.Contains(stack, "(*Producer)."+handler+"(") && strings.Contains(stack, "(*Producer).negotiate") && strings.Contains(stack, "(*Mutex).") {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond, "the actual profile hint handler must wait for the in-flight negotiation")
}

func TestProfileHintBecomesStaleWhileWaitingForNegotiation(t *testing.T) {
	for _, handler := range []string{"heartbeat", "allocations"} {
		t.Run(handler, func(t *testing.T) {
			f := newProfileHintFixture(t, 2)
			gate := newProfileHintGate(t)
			f.cp.mu.Lock()
			f.cp.commit = gate
			f.cp.mu.Unlock()
			f.p.acceptCatalog(f.snapshot)
			f.cp.version.Store(3)
			committed := make(chan error, 1)
			go func() { committed <- f.p.negotiateSources(f.ctx, []*source{f.next}, f.snapshot, false) }()
			select {
			case <-gate.entered:
			case <-f.ctx.Done():
				t.Fatal("newer profile commit did not reach its response barrier")
			}
			cancel, done := f.start(t, handler)
			f.cp.response.open()
			waitProfileHintNegotiationLock(t, handler)
			before := f.cp.calls.Load()
			gate.open()
			require.NoError(t, <-committed)
			f.finish(t, handler, cancel, done)
			require.Equal(t, int64(3), f.p.agreed().version)
			assert.Equal(t, before, f.cp.calls.Load(), "a hint which became stale while waiting must make no negotiation RPC")
			require.Equal(t, newHintBitrate, f.cp.bitrate.Load(), "queued hints must not re-propose a draining source after a newer commit")
		})
	}
}

func TestNewerProfileHintRefreshesTheCurrentProfile(t *testing.T) {
	for _, handler := range []string{"heartbeat", "allocations"} {
		t.Run(handler, func(t *testing.T) {
			f := newProfileHintFixture(t, 2)
			f.cp.version.Store(2)
			before := f.cp.calls.Load()
			cancel, done := f.start(t, handler)
			f.cp.response.open()
			f.finish(t, handler, cancel, done)
			require.Equal(t, before+1, f.cp.calls.Load(), "a genuinely newer hint refreshes the current profile once")
			require.Equal(t, int64(2), f.p.agreed().version)
			require.Equal(t, oldHintBitrate, f.cp.bitrate.Load())
		})
	}
}

func TestNonNewerProfileHintsDoNotWaitForNegotiation(t *testing.T) {
	for _, handler := range []string{"heartbeat", "allocations"} {
		for _, hint := range []int64{0, 1, 2} {
			t.Run(handler+"/"+string(rune('0'+hint)), func(t *testing.T) {
				f := newProfileHintFixture(t, hint)
				f.p.acceptCatalog(f.snapshot)
				f.cp.version.Store(2)
				require.NoError(t, f.p.negotiateSources(f.ctx, []*source{f.next}, f.snapshot, false))
				before := f.cp.calls.Load()
				// A slow lifecycle negotiation must not hold up routine reports
				// or an old FIFO response which cannot require another RPC.
				f.p.negotiating.Lock()
				defer f.p.negotiating.Unlock()
				cancel, done := f.start(t, handler)
				f.cp.response.open()
				f.finish(t, handler, cancel, done)
				require.Equal(t, before, f.cp.calls.Load())
			})
		}
	}
}

func TestNewerProfileHintSelectsCurrentSourcesAndCatalogAfterLock(t *testing.T) {
	for _, handler := range []string{"heartbeat", "allocations"} {
		t.Run(handler, func(t *testing.T) {
			f := newProfileHintFixture(t, 4)
			gate := newProfileHintGate(t)
			f.cp.mu.Lock()
			f.cp.commit = gate
			f.cp.mu.Unlock()
			f.p.acceptCatalog(f.snapshot)
			f.cp.version.Store(3)
			committed := make(chan error, 1)
			go func() { committed <- f.p.negotiateSources(f.ctx, []*source{f.next}, f.snapshot, false) }()
			select {
			case <-gate.entered:
			case <-f.ctx.Done():
				t.Fatal("current profile commit did not reach its response barrier")
			}
			cancel, done := f.start(t, handler)
			f.cp.response.open()
			waitProfileHintNegotiationLock(t, handler)
			latest := f.snapshot.Clone()
			latest.Revision.Catalog++
			f.p.acceptCatalog(latest)
			f.p.retireSource(f.old)
			require.NoError(t, f.p.installSource(f.next))
			f.cp.version.Store(4)
			gate.open()
			require.NoError(t, <-committed)
			if handler == "allocations" {
				require.Eventually(t, func() bool { return f.p.agreed().version == 4 }, 5*time.Second, time.Millisecond, "the newer allocation hint must finish negotiation")
				cancel()
			}
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-f.ctx.Done():
				t.Fatal("current profile hint handler did not complete")
			}
			require.Equal(t, int64(4), f.p.agreed().version)
			require.Equal(t, latest.Revision.Catalog, f.cp.lastCatalog.Load(), "a queued hint must use the catalog accepted after it began waiting")
			require.Equal(t, newHintBitrate, f.cp.bitrate.Load(), "a queued hint must propose the currently installed capture")
		})
	}
}

func TestProfileHintRechecksLoadingAfterNegotiationLock(t *testing.T) {
	f := newProfileHintFixture(t, 4)
	gate := newProfileHintGate(t)
	f.cp.mu.Lock()
	f.cp.commit = gate
	f.cp.mu.Unlock()
	f.p.acceptCatalog(f.snapshot)
	f.cp.version.Store(3)
	committed := make(chan error, 1)
	go func() { committed <- f.p.negotiateSources(f.ctx, []*source{f.next}, f.snapshot, false) }()
	select {
	case <-gate.entered:
	case <-f.ctx.Done():
		t.Fatal("loading profile commit did not reach its response barrier")
	}
	cancel, done := f.start(t, "heartbeat")
	f.cp.response.open()
	waitProfileHintNegotiationLock(t, "heartbeat")
	f.p.loading.Store(true)
	before := f.cp.calls.Load()
	gate.open()
	require.NoError(t, <-committed)
	f.finish(t, "heartbeat", cancel, done)
	require.Equal(t, before, f.cp.calls.Load(), "a queued newer hint cannot negotiate while sources are loading")
	require.Equal(t, newHintBitrate, f.cp.bitrate.Load())
}
