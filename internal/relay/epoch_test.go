package relay

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/fmp4"
	"github.com/lesomnus/shale/internal/token"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type epochStream struct {
	grpc.ServerStream
	ctx  context.Context
	mu   sync.Mutex
	sent []*api.AttachResponse
}

func (s *epochStream) Context() context.Context { return s.ctx }
func (s *epochStream) Send(v *api.AttachResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, proto.Clone(v).(*api.AttachResponse))
	return nil
}
func (*epochStream) Recv() (*api.AttachRequest, error) { return nil, io.EOF }

type epochFixture struct {
	t             *testing.T
	r             *Relay
	a             *attachment
	key           token.Key
	actor, tenant pdid.Id
	viewerActor   pdid.Id
	at            time.Time
}

func newEpochFixture(t *testing.T) epochFixture {
	t.Helper()
	r, err := New(Config{StateDir: t.TempDir(), Log: slog.New(slog.DiscardHandler)})
	require.NoError(t, err)
	r.id = pdid.New(24)
	w, err := newWhepServer(r)
	require.NoError(t, err)
	r.whep = w
	t.Cleanup(w.closeAll)
	key, err := token.Generate("epoch-fixture")
	require.NoError(t, err)
	r.keys.Verifier.Add(key.Kid, key.Public())
	stream := &epochStream{ctx: t.Context()}
	a := &attachment{r: r, stream: stream, sources: map[pdid.Id]*source{}, grants: map[pdid.Id]*api.CaptureEpochGrant{}, pending: map[pdid.Id]bool{}, updating: true}
	return epochFixture{t: t, r: r, a: a, key: key, actor: pdid.New(22), tenant: pdid.New(1), viewerActor: pdid.New(2), at: time.Now().UTC()}
}
func (f epochFixture) claims(sources []pdid.Id, grants ...*api.CaptureEpochGrant) *api.TokenClaims {
	ids := make([][]byte, 0, len(sources))
	for _, id := range sources {
		ids = append(ids, id.Bytes())
	}
	return api.TokenClaims_builder{Actor: f.actor.Bytes(), ActorTenant: f.tenant.Bytes(), Aud: f.r.id.Bytes(), Op: api.TokenOp_TOKEN_OP_PUBLISH, Iat: timestamppb.New(f.at), Exp: timestamppb.New(f.at.Add(time.Minute)), Sources: ids, CaptureEpochs: grants, Live: api.LivePolicy_LIVE_POLICY_ALWAYS, RewindSeconds: 30}.Build()
}
func epochGrant(id pdid.Id, epoch string, seq int64) *api.CaptureEpochGrant {
	return api.CaptureEpochGrant_builder{SourceId: id.Bytes(), CaptureEpoch: epoch, Sequence: seq}.Build()
}
func (f epochFixture) apply(c *api.TokenClaims) (bool, error) {
	f.t.Helper()
	signed, err := f.key.Sign(proto.Clone(c).(*api.TokenClaims))
	require.NoError(f.t, err)
	verified, err := f.r.verify(signed, api.TokenOp_TOKEN_OP_PUBLISH)
	if err != nil {
		return false, err
	}
	return f.a.authorize(verified)
}
func (f epochFixture) cachedViewer(id pdid.Id, key string) (*source, *viewer) {
	f.t.Helper()
	s := f.r.sources.get(id)
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	require.NoError(f.t, err)
	v := &viewer{key: key, source: s, pc: pc, done: make(chan struct{}), captureEpoch: s.epoch, captureSequence: s.epochSequence}
	s.mu.Lock()
	s.init = &fmp4.Init{Bytes: []byte("old-init")}
	s.initEpoch = s.epoch
	s.params = []byte("old-params")
	s.gop = []sample{{data: []byte("old-frame")}}
	s.ring.bytes = 99
	s.viewers[key] = v
	s.mu.Unlock()
	f.r.whep.mu.Lock()
	f.r.whep.sessions[key] = v
	f.r.whep.mu.Unlock()
	return s, v
}

func TestCaptureEpochGrantResetPreservesOtherSource(t *testing.T) {
	f := newEpochFixture(t)
	aID, bID := pdid.New(8), pdid.New(8)
	old := f.claims([]pdid.Id{aID, bID}, epochGrant(aID, "a-1", 1), epochGrant(bID, "b-1", 1))
	ok, err := f.apply(old)
	require.NoError(t, err)
	require.True(t, ok)
	a, av := f.cachedViewer(aID, "actor.source-a")
	b, bv := f.cachedViewer(bID, "actor.source-b")
	f.at = f.at.Add(time.Nanosecond)
	fresh := f.claims([]pdid.Id{aID, bID}, epochGrant(aID, "a-2", 2), epochGrant(bID, "b-1", 1))
	ok, err = f.apply(fresh)
	require.NoError(t, err)
	require.True(t, ok)
	select {
	case <-av.done:
	default:
		t.Fatal("changed source peer must close")
	}
	select {
	case <-bv.done:
		t.Fatal("unrelated source peer must remain connected")
	default:
	}
	a.mu.Lock()
	require.Nil(t, a.init)
	require.Nil(t, a.params)
	require.Empty(t, a.gop)
	require.Zero(t, a.ring.bytes)
	require.Equal(t, int64(2), a.epochSequence)
	a.mu.Unlock()
	b.mu.Lock()
	require.NotNil(t, b.init)
	require.Equal(t, "b-1", b.initEpoch)
	require.EqualValues(t, 99, b.ring.bytes)
	require.Len(t, b.viewers, 1)
	b.mu.Unlock()
	ok, err = f.apply(old)
	require.NoError(t, err)
	require.False(t, ok, "old still-valid signed Hello cannot roll back an epoch")
	require.Equal(t, "a-2", a.epoch)
	downgrade := proto.Clone(fresh).(*api.TokenClaims)
	downgrade.SetCaptureEpochs(nil)
	ok, err = f.apply(downgrade)
	require.NoError(t, err)
	require.False(t, ok, "source-only legacy grant cannot remove managed provenance")
	require.False(t, a.resetEpoch(f.a, epochGrant(aID, "a-1", 1), "a-1", false))
	a.feedEpoch(f.a, "", []byte("old-empty-epoch"))
	require.Nil(t, a.init)
	require.True(t, a.resetEpoch(f.a, f.a.grants[aID], "a-2", true))
	ok, err = f.apply(fresh)
	require.NoError(t, err)
	require.False(t, ok, "same sequence cannot resurrect a stopped capture")
	f.at = f.at.Add(time.Nanosecond)
	ok, err = f.apply(f.claims([]pdid.Id{aID, bID}, epochGrant(aID, "a-3", 3), epochGrant(bID, "b-1", 1)))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "a-3", a.epoch)
	require.False(t, a.epochDenied)
}

func TestCaptureEpochZeroMembershipAndEqualIatRetry(t *testing.T) {
	f := newEpochFixture(t)
	zero := f.claims(nil)
	ok, err := f.apply(zero)
	require.NoError(t, err)
	require.True(t, ok, "zero-source producer may remain attached")
	id := pdid.New(8)
	added := f.claims([]pdid.Id{id}, epochGrant(id, "new", 1))
	ok, err = f.apply(added)
	require.NoError(t, err)
	require.True(t, ok, "new CP Begin sequence can admit a source without reconnecting")
	ok, err = f.apply(zero)
	require.NoError(t, err)
	require.False(t, ok, "equal-Iat old empty Hello cannot erase newly admitted membership")
	require.Contains(t, f.a.sources, id)
	f.at = f.at.Add(time.Nanosecond)
	ok, err = f.apply(f.claims(nil))
	require.NoError(t, err)
	require.True(t, ok, "a fresh token retries legitimate membership removal")
	require.Empty(t, f.a.sources)
	s := f.r.sources.get(id)
	s.mu.Lock()
	require.True(t, s.managed)
	require.EqualValues(t, 1, s.epochSequence)
	require.True(t, s.epochDenied, "removal cannot be undone by replaying an old capability on another stream")
	s.mu.Unlock()
}

func TestCaptureEpochPendingDenyAndPublicationIdentity(t *testing.T) {
	f := newEpochFixture(t)
	id := pdid.New(8)
	pending := f.claims([]pdid.Id{id}, epochGrant(id, "", 0))
	ok, err := f.apply(pending)
	require.NoError(t, err)
	require.True(t, ok)
	s := f.r.sources.get(id)
	require.False(t, s.viewEpoch(""), "pending managed source never obtains legacy view behavior")
	require.False(t, s.resetEpoch(f.a, f.a.grants[id], "", false))
	s.feedEpoch(f.a, "", []byte("legacy"))
	require.Nil(t, s.init)
	wrong := proto.Clone(pending).(*api.TokenClaims)
	wrong.SetActor(pdid.New(22).Bytes())
	_, err = f.apply(wrong)
	require.Error(t, err)
	wrong = proto.Clone(pending).(*api.TokenClaims)
	wrong.SetActorTenant(pdid.New(1).Bytes())
	_, err = f.apply(wrong)
	require.Error(t, err)
	wrong = proto.Clone(pending).(*api.TokenClaims)
	wrong.SetExp(timestamppb.New(time.Now().Add(-time.Second)))
	ok, err = f.a.authorize(wrong)
	require.Error(t, err)
	require.False(t, ok, "expired publication cannot alter source state even within verifier skew")
}

func TestCaptureEpochMembershipFenceNeverMovesBackwards(t *testing.T) {
	f := newEpochFixture(t)
	aID, bID := pdid.New(8), pdid.New(8)
	latest := f.at
	ok, err := f.apply(f.claims([]pdid.Id{aID, bID}, epochGrant(aID, "a", 1), epochGrant(bID, "b", 1)))
	require.NoError(t, err)
	require.True(t, ok)
	f.at = latest.Add(-2 * time.Second)
	ok, err = f.apply(f.claims([]pdid.Id{aID, bID}, epochGrant(aID, "a", 1), epochGrant(bID, "b", 1)))
	require.NoError(t, err)
	require.True(t, ok, "equivalent older signed grant may refresh the same stream")
	f.at = latest.Add(-time.Second)
	ok, err = f.apply(f.claims([]pdid.Id{aID}, epochGrant(aID, "a", 1)))
	require.NoError(t, err)
	require.False(t, ok, "equivalent replay cannot lower the newest membership issuance fence")
	require.Contains(t, f.a.sources, bID)
	f.at = latest.Add(time.Nanosecond)
	ok, err = f.apply(f.claims([]pdid.Id{aID}, epochGrant(aID, "a", 1)))
	require.NoError(t, err)
	require.True(t, ok)
}

// A rejected initial attachment has no registered membership to detach. Its
// multi-source authorization must therefore commit all grants before closing
// any old peer or performing another externally blocking side effect.
func TestCaptureEpochConcurrentAttachCommitsBeforeClosingPeers(t *testing.T) {
	f := newEpochFixture(t)
	aID, bID := pdid.New(8), pdid.New(8)
	ok, err := f.apply(f.claims([]pdid.Id{aID, bID}, epochGrant(aID, "a-1", 1), epochGrant(bID, "b-1", 1)))
	require.NoError(t, err)
	require.True(t, ok)
	a, av := f.cachedViewer(aID, "actor.atomic-a")
	b, bv := f.cachedViewer(bID, "actor.atomic-b")
	viewers := map[pdid.Id]*viewer{aID: av, bID: bv}
	av.mu.Lock()
	bv.mu.Lock()
	locked := map[pdid.Id]bool{aID: true, bID: true}
	t.Cleanup(func() {
		for id, v := range viewers {
			if locked[id] {
				v.mu.Unlock()
			}
		}
	})
	attachmentFor := func() *attachment {
		return &attachment{r: f.r, stream: &epochStream{ctx: t.Context()}, sources: map[pdid.Id]*source{}, grants: map[pdid.Id]*api.CaptureEpochGrant{}, pending: map[pdid.Id]bool{}, updating: true}
	}
	left, right := attachmentFor(), attachmentFor()
	type result struct {
		ok  bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		ok, err := left.authorize(f.claims([]pdid.Id{aID, bID}, epochGrant(aID, "a-2", 2), epochGrant(bID, "b-2", 2)))
		done <- result{ok, err}
	}()
	var first pdid.Id
	require.Eventually(t, func() bool {
		f.r.whep.mu.Lock()
		defer f.r.whep.mu.Unlock()
		for id, v := range viewers {
			if f.r.whep.sessions[v.key] == nil {
				first = id
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond, "the first old peer must be closing")
	other := aID
	if first == aID {
		other = bID
	}
	viewers[other].mu.Unlock()
	locked[other] = false
	// A concurrent fresh stream advances only the other Source while the
	// first peer's Close is blocked. This may not turn left into a rejected
	// initial attachment that left the first Source owned by a dead feeder.
	ok, err = right.authorize(f.claims([]pdid.Id{other}, epochGrant(other, "concurrent-3", 3)))
	require.NoError(t, err)
	require.True(t, ok)
	viewers[first].mu.Unlock()
	locked[first] = false
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.True(t, got.ok, "all source grants must commit before closing peers")
	case <-time.After(time.Second):
		t.Fatal("authorization did not finish after the peer was released")
	}
	firstSource := a
	if first == bID {
		firstSource = b
	}
	firstSource.mu.Lock()
	require.Equal(t, left, firstSource.feeder)
	require.EqualValues(t, 2, firstSource.epochSequence)
	firstSource.mu.Unlock()
	require.Contains(t, left.sources, first, "the live feeder must own a registered source for input and cleanup")
}
