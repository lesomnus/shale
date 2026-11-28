package core

import (
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
)

// The times below carry nanoseconds, as a wall clock and a producer's
// clock do; PostgreSQL keeps a microsecond of them, and the generated
// store now writes no more on SQLite either. What the core answers and
// what it writes past the store are what a read gives again, on both.
const nanos = 123_456_789 * time.Nanosecond

// nanoClock moves the fixture's clock off the microsecond.
func nanoClock(f *capFixture) {
	f.now = f.now.Add(nanos)
}

// requireTime says a time answered is the time a read gives.
func requireTime(t *testing.T, read, answered *timestamppb.Timestamp, what string) {
	t.Helper()
	require.NotNil(t, answered, what)
	require.True(t, proto.Equal(read, answered), "%s: answered %s, read %s", what, answered.AsTime().Format(time.RFC3339Nano), read.AsTime().Format(time.RFC3339Nano))
}

// An allocation answers its expiry from the core's clock and its start from
// the producer's request, beside the lamina and the attempts it has just
// written: both are what a Get of those rows gives. The same segment
// reported again, with the same nanoseconds, is the same lamina.
func TestAnAllocationAnswersWhatItWrote(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testAnAllocationAnswersWhatItWrote(t, newCapFixture(t)) })
	t.Run("PostgreSQL", func(t *testing.T) { testAnAllocationAnswersWhatItWrote(t, newCapFixtureOn(t, pgConfig(t))) })
}

func testAnAllocationAnswersWhatItWrote(t *testing.T, f *capFixture) {
	nanoClock(f)
	ctx := t.Context()
	src := f.srcs[0]
	at := f.slot(src).Add(1500*time.Millisecond + 789*time.Nanosecond)

	al, err := f.allocateOne(src, at)
	require.NoError(t, err)
	require.Len(t, al.GetCandidates(), Candidates)
	lam, err := f.sink.Lamina().Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())
	require.NoError(t, err)
	requireTime(t, lam.GetDateStarted(), al.GetDateStarted(), "the allocation's start")
	for _, c := range al.GetCandidates() {
		v, err := f.sink.Attempt().Get(ctx, api.AttemptGetRequest_builder{Ref: api.AttemptRef_builder{Id: c.GetAttemptId()}.Build()}.Build())
		require.NoError(t, err)
		requireTime(t, v.GetDateExpires(), al.GetDateExpires(), "the allocation's expiry")
	}

	again, err := f.allocateOne(src, at)
	require.NoError(t, err)
	require.Equal(t, pdid.Id(al.GetLaminaId()), pdid.Id(again.GetLaminaId()), "the segment reported twice is one lamina")
	requireTime(t, al.GetDateExpires(), again.GetDateExpires(), "the allocation answered again")
	requireTime(t, lam.GetDateStarted(), again.GetDateStarted(), "the start answered again")

	// The look-ahead, every slot of every member.
	resp, err := f.core.Set().Allocate(f.ctx, api.SetAllocateRequest_builder{Ref: api.SetRef_builder{Id: f.set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, resp.GetAllocations())
	for _, al := range resp.GetAllocations() {
		require.NotEmpty(t, al.GetCandidates())
		v, err := f.sink.Attempt().Get(ctx, api.AttemptGetRequest_builder{Ref: api.AttemptRef_builder{Id: al.GetCandidates()[0].GetAttemptId()}.Build()}.Build())
		require.NoError(t, err)
		requireTime(t, v.GetDateExpires(), al.GetDateExpires(), "a look-ahead's expiry")
	}
}

// A skipped segment's dates, the end from the producer and the finish from
// the core's clock, are answered as a Get gives them.
func TestASkipAnswersWhatItWrote(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testASkipAnswersWhatItWrote(t, newCapFixture(t)) })
	t.Run("PostgreSQL", func(t *testing.T) { testASkipAnswersWhatItWrote(t, newCapFixtureOn(t, pgConfig(t))) })
}

func testASkipAnswersWhatItWrote(t *testing.T, f *capFixture) {
	nanoClock(f)
	ctx := t.Context()
	src := f.srcs[0]
	at := f.slot(src).Add(time.Second + 789*time.Nanosecond)
	al, err := f.allocateOne(src, at)
	require.NoError(t, err)

	f.now = f.now.Add(10 * time.Second)
	got, err := f.core.Lamina().Skip(f.ctx, api.LaminaSkipRequest_builder{
		Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build(), Reason: api.LaminaSkipReason_LAMINA_SKIP_REASON_DARK,
		DateEnded: timestamppb.New(at.Add(5*time.Second + 321*time.Nanosecond)),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, api.LaminaState_LAMINA_STATE_SKIPPED, got.GetState())
	read, err := f.sink.Lamina().Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())
	require.NoError(t, err)
	requireTime(t, read.GetDateEnded(), got.GetDateEnded(), "the skip's end")
	requireTime(t, read.GetDateFinished(), got.GetDateFinished(), "the skip's finish")
	requireTime(t, read.GetDateUpdated(), got.GetDateUpdated(), "the skip's version")
	requireTime(t, timestamppb.New(f.now.Truncate(time.Microsecond)), read.GetDateFinished(), "the finish is the clock's, at the microsecond")
}

// Place writes the set's version and its producer's straight through ent,
// past the generated store: what it writes is the core's clock at the
// microsecond, which a read gives again and the answer gives.
func TestAPlacingWritesWhatTheDatabaseKeeps(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testAPlacingWritesWhatTheDatabaseKeeps(t, newCapFixture(t)) })
	t.Run("PostgreSQL", func(t *testing.T) { testAPlacingWritesWhatTheDatabaseKeeps(t, newCapFixtureOn(t, pgConfig(t))) })
}

func testAPlacingWritesWhatTheDatabaseKeeps(t *testing.T, cf *capFixture) {
	f := newPlaceFixture(t, cf)
	nanoClock(f.capFixture)
	ctx := t.Context()
	f.allocate()

	got, err := f.place(false)
	require.NoError(t, err)
	want := timestamppb.New(f.now.Truncate(time.Microsecond))
	set, err := f.sink.Set().Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: f.set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	requireTime(t, want, set.GetDateUpdated(), "the set's version as written")
	requireTime(t, set.GetDateUpdated(), got.GetSet().GetDateUpdated(), "the set as answered")
	p, err := f.sink.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: f.producer.Bytes()}.Build()}.Build())
	require.NoError(t, err)
	requireTime(t, want, p.GetDateUpdated(), "the producer's version as written")

	// The version a read gave is the one a patch is taken with.
	_, err = f.sink.Set().Patch(ctx, api.SetPatchRequest_builder{Ref: api.SetRef_builder{Id: f.set.GetId()}.Build(), Name: proto.String("cams"), DateUpdated: set.GetDateUpdated()}.Build())
	require.NoError(t, err)
}
