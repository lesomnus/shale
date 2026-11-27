package core

import (
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
)

// push is the node's batch as it pushes it: what the CP answered applied,
// and the call's error.
func (f *acceptFixture) push(evs ...*api.Event) (int32, error) {
	f.t.Helper()
	resp, err := f.core.Node().PushEvents(as(f.node), api.NodePushEventsRequest_builder{Events: evs}.Build())

	return resp.GetApplied(), err
}

// stored is a stored event of an attempt of a lamina, its file at key.
func (f *acceptFixture) stored(lamina, at pdid.Id, key string, size int64, incomplete bool) *api.Event {
	return api.Event_builder{Stored: api.LaminaStored_builder{
		LaminaId: lamina.Bytes(), AttemptId: at.Bytes(), SinkId: f.sinkId.Bytes(), LaminaKey: key, Size: size, Incomplete: incomplete,
	}.Build()}.Build()
}

// deleted is the node's word that the file at key, of an attempt, is
// unlinked.
func (f *acceptFixture) deleted(at pdid.Id, key string, size int64) *api.Event {
	return api.Event_builder{Deleted: api.LaminaDeleted_builder{
		SinkId: f.sinkId.Bytes(), LaminaKey: key, AttemptId: at.Bytes(), Size: size, DateDeleted: timestamppb.New(f.now),
	}.Build()}.Build()
}

// missing is the node's word that the file at key is gone.
func (f *acceptFixture) missing(key string) *api.Event {
	return api.Event_builder{Missing: api.LaminaMissing_builder{
		SinkId: f.sinkId.Bytes(), LaminaKey: key, Reason: api.MissingReason_MISSING_REASON_NOT_FOUND, DateObserved: timestamppb.New(f.now),
	}.Build()}.Build()
}

// another is a further open attempt of a lamina on the fixture's sink.
func (f *acceptFixture) another(lamina pdid.Id) pdid.Id {
	f.t.Helper()
	at := pdid.New(DomAttempt)
	_, err := f.sink.Attempt().Add(f.t.Context(), api.AttemptAddRequest_builder{
		Id: at.Bytes(), Tenant: f.tenant, Lamina: api.LaminaRef_builder{Id: lamina.Bytes()}.Build(),
		Sink: api.SinkRef_builder{Id: f.sinkId.Bytes()}.Build(), Node: api.NodeRef_builder{Id: f.node.Bytes()}.Build(),
		State: api.AttemptState_ATTEMPT_STATE_ALLOCATED, DateExpires: timestamppb.New(f.now.Add(time.Hour)),
	}.Build())
	require.NoError(f.t, err)

	return at
}

// storedBytes is the tenant's stored bytes as the CP counts them.
func (f *acceptFixture) storedBytes() int64 {
	f.t.Helper()
	v, err := f.sink.Tenant().Get(f.t.Context(), api.TenantGetRequest_builder{Ref: f.tenant}.Build())
	require.NoError(f.t, err)

	return v.GetStoredBytes()
}

// applies requires every event of a batch applied.
func (f *acceptFixture) applies(evs ...*api.Event) {
	f.t.Helper()
	n, err := f.push(evs...)
	require.NoError(f.t, err)
	require.Equal(f.t, int32(len(evs)), n)
}

// A tenant's stored bytes count the file that is a lamina's, once: added
// when its stored event wins, taken away once when it stops being the
// lamina's. A duplicate's never counts. Before, a duplicate that never won
// was taken away when its file was deleted, and taken away again each time
// that event was applied; a truncated winner replaced by a complete file
// stayed counted beside it; and a LOST lamina's deletion took its bytes
// away a second time. Found making the resend of #343 safe.
func TestStoredBytesCountTheLaminaFile(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testStoredBytesCountTheLaminaFile(t, newAcceptFixture(t)) })
	t.Run("PostgreSQL", func(t *testing.T) { testStoredBytesCountTheLaminaFile(t, newAcceptFixtureOn(t, pgConfig(t))) })
}

func testStoredBytesCountTheLaminaFile(t *testing.T, f *acceptFixture) {
	// Bytes of another lamina, so that a count taken below zero is seen
	// rather than stopped at zero.
	base, baseAt := f.lamina(time.Hour)
	f.applies(f.stored(base, baseAt, "base", 1000, false))
	require.Equal(t, int64(1000), f.storedBytes())

	// A duplicate that never won: stored, and its file deleted, twice.
	id, won := f.lamina(time.Hour)
	lost := f.another(id)
	f.applies(f.stored(id, won, "won", 100, false))
	f.applies(f.stored(id, lost, "lost", 50, false))
	require.Equal(t, api.AttemptState_ATTEMPT_STATE_DUPLICATE, f.attempt(lost).GetState())
	require.Equal(t, int64(1100), f.storedBytes())
	f.applies(f.deleted(lost, "lost", 50))
	f.applies(f.deleted(lost, "lost", 50))
	require.Equal(t, int64(1100), f.storedBytes(), "a duplicate's file never counted")

	// A truncated winner replaced by a complete file.
	id, truncated := f.lamina(time.Hour)
	complete := f.another(id)
	f.applies(f.stored(id, truncated, "truncated", 40, true))
	require.Equal(t, int64(1140), f.storedBytes())
	f.applies(f.stored(id, complete, "complete", 100, false))
	require.Equal(t, api.AttemptState_ATTEMPT_STATE_DUPLICATE, f.attempt(truncated).GetState())
	require.Equal(t, "complete", f.get(id).GetLaminaKey())
	require.Equal(t, int64(1200), f.storedBytes(), "the complete file in place of the truncated one")
	f.applies(f.stored(id, complete, "complete", 100, false))
	require.Equal(t, int64(1200), f.storedBytes(), "a redelivery moves nothing")
	f.applies(f.deleted(truncated, "truncated", 40))
	f.applies(f.deleted(truncated, "truncated", 40))
	require.Equal(t, int64(1200), f.storedBytes(), "the truncated file was taken away when it was replaced")
	f.applies(f.deleted(complete, "complete", 100))
	f.applies(f.deleted(complete, "complete", 100))
	require.Equal(t, api.LaminaState_LAMINA_STATE_DELETED, f.get(id).GetState())
	require.Equal(t, int64(1100), f.storedBytes(), "the lamina's file, taken away once")

	// A lamina lost, and then its file deleted.
	id, at := f.lamina(time.Hour)
	f.applies(f.stored(id, at, "gone", 200, false))
	require.Equal(t, int64(1300), f.storedBytes())
	f.applies(f.missing("gone"))
	f.applies(f.missing("gone"))
	require.Equal(t, api.LaminaState_LAMINA_STATE_LOST, f.get(id).GetState())
	require.Equal(t, int64(1100), f.storedBytes())
	f.applies(f.deleted(at, "gone", 200))
	require.Equal(t, api.LaminaState_LAMINA_STATE_DELETED, f.get(id).GetState())
	require.Equal(t, int64(1100), f.storedBytes(), "a lost lamina's bytes were taken away when it was lost")

	// A lost lamina stored again by a new attempt counts again, once.
	id, at = f.lamina(time.Hour)
	f.applies(f.stored(id, at, "first", 200, false))
	f.applies(f.missing("first"))
	require.Equal(t, int64(1100), f.storedBytes())
	again := f.another(id)
	f.applies(f.stored(id, again, "again", 150, false))
	f.applies(f.stored(id, again, "again", 150, false))
	require.Equal(t, api.LaminaState_LAMINA_STATE_COMMITTED, f.get(id).GetState())
	require.Equal(t, int64(1250), f.storedBytes())
}
