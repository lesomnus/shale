package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/payday/config"
	_ "github.com/lesomnus/payday/config/dbsqlite3"
	"github.com/protobuf-orm/ent/dialect/sql"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/attempt"
	"github.com/lesomnus/shale/internal/ent/enttest"
	"github.com/lesomnus/shale/internal/ent/lamina"
	"github.com/lesomnus/shale/server/bare"
	"github.com/lesomnus/shale/server/pd"
)

// capFixture is a set of three cameras recording 66-second segments with
// the default 10-minute horizon, over three sinks on three nodes, so every
// lamina gets three candidates (§12.1, §13).
type capFixture struct {
	t      *testing.T
	core   Core
	sink   pd.Sink
	client *ent.Client
	tenant *api.TenantRef
	set    *api.Set
	srcs   []*api.Source
	sinks  []pdid.Id
	nodes  []pdid.Id
	now    time.Time
	logs   *bytes.Buffer
	ctx    context.Context
}

const capSegment = 66 * time.Second

func newCapFixture(t *testing.T) *capFixture {
	return newCapFixtureOn(t, config.DbConfig{Driver: "sqlite3", Dsn: "file:" + filepath.Join(t.TempDir(), "shale.db") + "?_pragma=foreign_keys(1)"})
}

// newCapFixtureOn is the fixture over the database cfg names.
func newCapFixtureOn(t *testing.T, cfg config.DbConfig) *capFixture {
	ctx := t.Context()
	db, dialect, err := cfg.Open(ctx)
	require.NoError(t, err)
	driver := sql.OpenDB(dialect, db)
	client := enttest.NewClient(t, enttest.WithOptions(ent.Driver(driver)))
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	sink, err := pd.NewSink(client, bare.WithMinter(pd.Minter()))
	require.NoError(t, err)
	tenant, err := sink.Tenant().Add(ctx, api.TenantAddRequest_builder{Alias: "acme"}.Build())
	require.NoError(t, err)

	f := &capFixture{
		t: t, sink: sink, client: client, tenant: api.TenantRef_builder{Id: tenant.GetId()}.Build(),
		now: time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC), logs: &bytes.Buffer{},
	}
	now := func() time.Time { return f.now }
	kek := make(Kek, 32)
	_, err = rand.Read(kek)
	require.NoError(t, err)
	d := &Deps{
		Ent: client, Own: sink, Drv: driver, Now: now, Keys: NewKeys(kek, sink, now),
		// The test moves the clock; the nodes stay up.
		NodeDownAfter: 24 * time.Hour,
		Log:           slog.New(slog.NewTextHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	_, err = d.Keys.Create(ctx, sink, true)
	require.NoError(t, err)
	f.core = New(sink, d)

	for _, name := range []string{"n1", "n2", "n3"} {
		node := pdid.New(DomNode)
		_, err := sink.Node().Add(ctx, api.NodeAddRequest_builder{
			Id: node.Bytes(), Alias: name, HardwareId: name, State: api.HostState_HOST_STATE_ADOPTED, DateSeen: timestamppb.New(f.now),
		}.Build())
		require.NoError(t, err)
		dev, err := sink.Device().Add(ctx, api.DeviceAddRequest_builder{Alias: "d-" + name, HardwareId: "d-" + name, Node: api.NodeRef_builder{Id: node.Bytes()}.Build()}.Build())
		require.NoError(t, err)
		id := pdid.New(DomSink)
		_, err = sink.Sink().Add(ctx, api.SinkAddRequest_builder{
			Id: id.Bytes(), Alias: "s-" + name, Node: api.NodeRef_builder{Id: node.Bytes()}.Build(), Device: api.DeviceRef_builder{Id: dev.GetId()}.Build(),
			Path: "/" + name, Capacity: 4 << 40, Attachment: api.SinkAttachment_SINK_ATTACHMENT_ATTACHED, AcceptWrites: true, DateSeen: timestamppb.New(f.now),
		}.Build())
		require.NoError(t, err)
		f.nodes, f.sinks = append(f.nodes, node), append(f.sinks, id)
	}

	f.set, err = sink.Set().Add(ctx, api.SetAddRequest_builder{Tenant: f.tenant, Alias: "cams"}.Build())
	require.NoError(t, err)
	for i, alias := range []string{"door", "yard", "gate"} {
		src, err := sink.Source().Add(ctx, api.SourceAddRequest_builder{
			Tenant: f.tenant, Set: api.SetRef_builder{Id: f.set.GetId()}.Build(), Alias: alias, Ordinal: int32(i),
			Profile: api.SegmentProfile_builder{MaxBitrate: 4_000_000, DurationSeconds: int64(capSegment / time.Second), KeyframeIntervalMs: 2000}.Build(),
		}.Build())
		require.NoError(t, err)
		f.srcs = append(f.srcs, src)
	}
	f.ctx = frame.Into(context.Background(), frame.New(pdid.New(DomHolder), mustId(tenant.GetId()), frame.Whole()))

	return f
}

// slots are the slots a member's look-ahead covers at the fixture's now.
func (f *capFixture) slots(src *api.Source) []time.Time {
	var out []time.Time
	for at := slotOf(f.now, f.set.GetId(), int(src.GetOrdinal()), len(f.srcs), capSegment); !at.After(f.now.Add(DefaultHorizon)); at = at.Add(capSegment) {
		out = append(out, at)
	}

	return out
}

func (f *capFixture) slot(src *api.Source) time.Time {
	return slotOf(f.now, f.set.GetId(), int(src.GetOrdinal()), len(f.srcs), capSegment)
}

// allocate is SetService.Allocate, answered per source as the slots it
// covers, each checked to carry all three candidates.
func (f *capFixture) allocate() map[string][]time.Time {
	f.t.Helper()
	resp, err := f.core.Set().Allocate(f.ctx, api.SetAllocateRequest_builder{Ref: api.SetRef_builder{Id: f.set.GetId()}.Build()}.Build())
	require.NoError(f.t, err)
	out := map[string][]time.Time{}
	for _, al := range resp.GetAllocations() {
		require.Len(f.t, al.GetCandidates(), Candidates)
		out[string(al.GetSourceId())] = append(out[string(al.GetSourceId())], al.GetDateStarted().AsTime())
	}

	return out
}

// allocateOne is LaminaService.Allocate for a segment of `src` beginning at
// `at`.
func (f *capFixture) allocateOne(src *api.Source, at time.Time) (*api.Allocation, error) {
	return f.core.Lamina().Allocate(f.ctx, api.LaminaAllocateRequest_builder{
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(at),
	}.Build())
}

// pending adds a PENDING lamina of `src` beginning at `at` with one open
// attempt on every sink, as an allocation leaves it.
func (f *capFixture) pending(src *api.Source, at time.Time) pdid.Id {
	f.t.Helper()
	ctx := f.t.Context()
	id := pdid.New(DomLamina)
	_, err := f.sink.Lamina().Add(ctx, api.LaminaAddRequest_builder{
		Id: id.Bytes(), Tenant: f.tenant, Set: api.SetRef_builder{Id: f.set.GetId()}.Build(), Source: api.SourceRef_builder{Id: src.GetId()}.Build(),
		State: api.LaminaState_LAMINA_STATE_PENDING, DateStarted: timestamppb.New(at),
	}.Build())
	require.NoError(f.t, err)
	for i := range f.sinks {
		_, err := f.sink.Attempt().Add(ctx, api.AttemptAddRequest_builder{
			Tenant: f.tenant, Lamina: api.LaminaRef_builder{Id: id.Bytes()}.Build(),
			Sink: api.SinkRef_builder{Id: f.sinks[i].Bytes()}.Build(), Node: api.NodeRef_builder{Id: f.nodes[i].Bytes()}.Build(),
			State: api.AttemptState_ATTEMPT_STATE_ALLOCATED, DateExpires: timestamppb.New(f.now.Add(time.Hour)), Rank: int32(i),
		}.Build())
		require.NoError(f.t, err)
	}

	return id
}

// open counts the open attempts of a source.
func (f *capFixture) open(src *api.Source) int {
	f.t.Helper()
	n, err := f.client.Attempt.Query().
		Where(attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED)), attempt.DateExpiresGT(f.now),
			attempt.HasLaminaWith(lamina.SourceIdEQ(mustId(src.GetId()).Uuid()))).
		Count(f.t.Context())
	require.NoError(f.t, err)

	return n
}

// capped is how many times the look-ahead said it stopped at the cap.
func (f *capFixture) capped() int {
	return strings.Count(f.logs.String(), "look-ahead stops at the open-attempt cap")
}

// §12.1: the cap is raised to what the horizon needs; the policy's value
// is a floor.
func TestOpenAttemptCap(t *testing.T) {
	b := DefaultBounds()
	require.Equal(t, 36, openAttemptCap(b, 10*time.Minute, 66*time.Second), "3 × (⌈600 ÷ 66⌉ + 2)")
	require.Equal(t, DefaultMaxOpenAttempts, openAttemptCap(b, 10*time.Minute, 2*time.Minute), "3 × (5 + 2) = 21 is under the floor")
	require.Equal(t, 90, openAttemptCap(b, 30*time.Minute, 66*time.Second), "3 × (28 + 2)")
	require.Equal(t, DefaultMaxOpenAttempts, openAttemptCap(b, 10*time.Minute, 0))
	b.MaxOpenAttempts = 100
	require.Equal(t, 100, openAttemptCap(b, 10*time.Minute, 66*time.Second), "a larger policy value stands")
}

// §12.1: with 66-second segments, the default horizon and three candidates,
// a producer that keeps fetching ahead through slot after slot, while the
// segment before the one being recorded is still uploading, is answered
// every time, and every member keeps its whole look-ahead.
func TestLookAheadFitsTheHorizon(t *testing.T) {
	f := newCapFixture(t)
	limit := openAttemptCap(DefaultBounds(), DefaultHorizon, capSegment)
	for range 60 {
		got := f.allocate()
		for _, src := range f.srcs {
			require.Equal(t, f.slots(src), got[string(src.GetId())], "%s keeps its look-ahead at %s", src.GetAlias(), f.now)
			require.LessOrEqual(t, f.open(src), limit)
		}
		require.Zero(t, f.capped())

		// Time passes by a fraction of a segment, so the calls fall at
		// every phase of every member's slots. What ended a segment ago has
		// committed; the segment before the one being recorded has not.
		f.now = f.now.Add(13 * time.Second)
		_, err := f.client.Attempt.Update().
			Where(attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_ALLOCATED)), attempt.HasLaminaWith(lamina.DateStartedLTE(f.now.Add(-2*capSegment)))).
			SetState(int32(api.AttemptState_ATTEMPT_STATE_STORED)).
			Save(f.t.Context())
		require.NoError(t, err)
	}
}

// §12.1: a member at the cap stops its own look-ahead and keeps its
// nearest slots; the other members' look-ahead is not rolled back with it,
// and the call says so once.
func TestLookAheadStopsAtTheCapAlone(t *testing.T) {
	f := newCapFixture(t)
	door := f.srcs[0]
	// A backlog of nine segments not yet stored: 27 open attempts, so
	// three more slots fit under the cap of 36.
	for k := 1; k <= 9; k++ {
		f.pending(door, f.slot(door).Add(-time.Duration(k)*capSegment))
	}

	got := f.allocate()
	require.Equal(t, f.slots(door)[:3], got[string(door.GetId())], "the nearest slots, then no more")
	for _, src := range f.srcs[1:] {
		require.Equal(t, f.slots(src), got[string(src.GetId())], "%s is not held back", src.GetAlias())
		n, err := f.client.Lamina.Query().Where(lamina.SourceIdEQ(mustId(src.GetId()).Uuid())).Count(f.t.Context())
		require.NoError(t, err)
		require.Equal(t, len(f.slots(src)), n, "%s's laminae were committed with the call", src.GetAlias())
	}
	require.Equal(t, 36, f.open(door))
	require.Equal(t, 1, f.capped(), "said once for the member, not once a slot")

	// Asked again, the same: what the member holds is answered again.
	got = f.allocate()
	require.Equal(t, f.slots(door)[:3], got[string(door.GetId())])
	require.Equal(t, 2, f.capped())
}

// §12.1: allocations fetched ahead never crowd out a segment that exists.
// With the future laminae holding the whole cap, the slot being recorded
// and a segment of the backlog still get laminae; a new slot further ahead
// does not, and one the source already holds is answered. Once those
// laminae have begun unused, as for a camera that stopped, they still
// leave room for the segment of a camera that came back.
func TestLookAheadDoesNotCrowdOutSegments(t *testing.T) {
	f := newCapFixture(t)
	door := f.srcs[0]
	slot := f.slot(door)
	var ahead []pdid.Id
	for k := 1; k <= 12; k++ {
		ahead = append(ahead, f.pending(door, slot.Add(time.Duration(k)*capSegment)))
	}
	require.Equal(t, 36, f.open(door))

	// The segment being recorded, asked for as it begins by a producer
	// whose clock is a second ahead of the CP's.
	f.now = slot.Add(-time.Second)
	al, err := f.allocateOne(door, slot)
	require.NoError(t, err)
	require.Len(t, al.GetCandidates(), Candidates)
	f.now = slot

	// A segment of the backlog.
	al, err = f.allocateOne(door, slot.Add(-5*capSegment))
	require.NoError(t, err)
	require.Len(t, al.GetCandidates(), Candidates)

	// A slot already held is answered; a new one ahead is refused.
	al, err = f.allocateOne(door, slot.Add(2*capSegment))
	require.NoError(t, err)
	require.Equal(t, ahead[1].Bytes(), al.GetLaminaId())
	later := slot.Add(13 * capSegment)
	_, err = f.allocateOne(door, later)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Contains(t, err.Error(), "holds 42 open attempts; the cap is 36")

	// Nothing used what was fetched ahead, and all of it has begun.
	f.now = later
	require.Equal(t, 42, f.open(door))
	al, err = f.allocateOne(door, later)
	require.NoError(t, err, "the camera came back")
	require.Len(t, al.GetCandidates(), Candidates)
}

// §12.1: the cap still bounds how many segments that have begun a source
// can leave open. A lamina the source already holds is answered past it,
// and one whose attempts all failed gets fresh ones (§16).
func TestOpenAttemptCapBoundsSegments(t *testing.T) {
	f := newCapFixture(t)
	door := f.srcs[0]
	slot := f.slot(door)
	// Twice the cap of 36: the look-ahead's share and the segments'.
	var held [][]byte
	for k := 1; k <= 24; k++ {
		al, err := f.allocateOne(door, slot.Add(-time.Duration(k)*capSegment))
		require.NoError(t, err, "segment %d of the backlog", k)
		held = append(held, al.GetLaminaId())
	}
	_, err := f.allocateOne(door, slot.Add(-25*capSegment))
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Contains(t, err.Error(), "holds 72 open attempts of segments that have begun; the cap is 72")
	_, err = f.allocateOne(door, slot)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "however recent")

	// Idempotent per segment, past the cap.
	al, err := f.allocateOne(door, slot.Add(-capSegment))
	require.NoError(t, err)
	require.Equal(t, held[0], al.GetLaminaId())
	require.Len(t, al.GetCandidates(), Candidates)

	// A segment being retried.
	_, err = f.client.Attempt.Update().
		Where(attempt.HasLaminaWith(lamina.IdEQ(mustId(held[1]).Uuid()))).
		SetState(int32(api.AttemptState_ATTEMPT_STATE_FAILED)).
		Save(f.t.Context())
	require.NoError(t, err)
	al, err = f.allocateOne(door, slot.Add(-2*capSegment))
	require.NoError(t, err)
	require.Equal(t, held[1], al.GetLaminaId())
	require.Len(t, al.GetCandidates(), Candidates)

	// The set's look-ahead gets nothing new for it, and the call succeeds.
	got := f.allocate()
	require.Empty(t, got[string(door.GetId())])
	for _, src := range f.srcs[1:] {
		require.Equal(t, f.slots(src), got[string(src.GetId())])
	}
}
