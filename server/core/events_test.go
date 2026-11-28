package core

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lesomnus/payday/config"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/z"
	"github.com/ncruces/go-sqlite3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent/attempt"
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

	// A lost lamina whose file the node still has, and proposes: approved,
	// it is DELETING and counts as every DELETING lamina does, so that the
	// deletion's confirmation takes its bytes away once and not a second
	// time (lesomnus/shale#345).
	id, at = f.lamina(time.Hour)
	f.applies(f.stored(id, at, "found", 300, false))
	require.Equal(t, int64(1550), f.storedBytes())
	f.applies(f.missing("found"))
	require.Equal(t, api.LaminaState_LAMINA_STATE_LOST, f.get(id).GetState())
	require.Equal(t, int64(1250), f.storedBytes())
	propose := func() {
		t.Helper()
		resp, err := f.core.Sink().ProposeGc(as(f.node), api.SinkProposeGcRequest_builder{
			Ref:        api.SinkRef_builder{Id: f.sinkId.Bytes()}.Build(),
			Reason:     api.GcReason_GC_REASON_SWEEP,
			Candidates: []*api.GcCandidate{api.GcCandidate_builder{LaminaKey: "found", Size: 300}.Build()},
		}.Build())
		require.NoError(t, err)
		require.Len(t, resp.GetDecisions(), 1)
		require.True(t, resp.GetDecisions()[0].GetApproved(), "the proposal was not approved, so this proves nothing")
	}
	// Due for deletion, so that a sweep approves it.
	_, err := f.sink.Lamina().Patch(t.Context(), api.LaminaPatchRequest_builder{
		Ref: api.LaminaRef_builder{Id: id.Bytes()}.Build(), DateDeleted: timestamppb.New(f.now.Add(-time.Minute)), DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.NoError(t, err)
	propose()
	require.Equal(t, api.LaminaState_LAMINA_STATE_DELETING, f.get(id).GetState())
	require.Equal(t, int64(1550), f.storedBytes(), "its file is there after all, and counts until it is deleted")
	propose()
	require.Equal(t, int64(1550), f.storedBytes(), "proposed again, it is DELETING already and nothing moves")
	f.applies(f.deleted(at, "found", 300))
	f.applies(f.deleted(at, "found", 300))
	require.Equal(t, api.LaminaState_LAMINA_STATE_DELETED, f.get(id).GetState())
	require.Equal(t, int64(1250), f.storedBytes(), "taken away once")
}

// logged gives the fixture's core a log to read.
func (f *acceptFixture) logged() *bytes.Buffer {
	b := &bytes.Buffer{}
	f.core.d.Log = slog.New(slog.NewTextHandler(b, &slog.HandlerOptions{Level: slog.LevelDebug}))

	return b
}

// deadlock is a refusal of PostgreSQL's, as pgx carries it.
var deadlock = &pgconn.PgError{Severity: "ERROR", Code: "40P01", Message: "deadlock detected"}

// A node's stored event whose transaction the store refused once is
// applied on the second try, and counted applied. Before, it was refused
// for good, and the node, answered, dropped it (#343).
func TestEventTriedAgain(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testEventTriedAgain(t, newAcceptFixture(t)) })
	t.Run("PostgreSQL", func(t *testing.T) { testEventTriedAgain(t, newAcceptFixtureOn(t, pgConfig(t))) })
}

func testEventTriedAgain(t *testing.T, f *acceptFixture) {
	logs := f.logged()
	id, at := f.lamina(time.Hour)
	tries := 0
	f.core.d.eventTried = func(context.Context, *api.Event) error {
		tries++
		if tries == 1 {
			return fmt.Errorf("apply: %w", deadlock)
		}
		return nil
	}
	n, err := f.push(f.stored(id, at, "k", 10, false))
	f.core.d.eventTried = nil
	require.NoError(t, err)
	require.Equal(t, int32(1), n)
	require.Equal(t, 2, tries)
	require.Equal(t, api.LaminaState_LAMINA_STATE_COMMITTED, f.get(id).GetState())
	require.Equal(t, api.AttemptState_ATTEMPT_STATE_STORED, f.attempt(at).GetState())
	require.Equal(t, int64(10), f.storedBytes())
	require.Contains(t, logs.String(), "event tried again")
}

// An event the store refuses on every try fails the call Unavailable: the
// events before it stay applied, and the same batch sent again applies
// every one, nothing twice.
func TestEventRefusedByTheStoreFailsTheCall(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testEventRefusedByTheStoreFailsTheCall(t, newAcceptFixture(t)) })
	t.Run("PostgreSQL", func(t *testing.T) { testEventRefusedByTheStoreFailsTheCall(t, newAcceptFixtureOn(t, pgConfig(t))) })
}

func testEventRefusedByTheStoreFailsTheCall(t *testing.T, f *acceptFixture) {
	logs := f.logged()
	var ids, ats []pdid.Id
	var batch []*api.Event
	for i := range 3 {
		id, at := f.lamina(time.Hour)
		ids, ats = append(ids, id), append(ats, at)
		batch = append(batch, f.stored(id, at, fmt.Sprintf("k%d", i), 10, false))
	}
	tries := 0
	f.core.d.eventTried = func(_ context.Context, ev *api.Event) error {
		if bytes.Equal(ev.GetStored().GetLaminaId(), ids[1].Bytes()) {
			tries++
			return deadlock
		}
		return nil
	}
	_, err := f.push(batch...)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NotContains(t, status.Convert(err).Message(), ids[1].String(), "the answer names nothing of the event")
	require.Equal(t, 3, tries, "three tries in all")
	require.Equal(t, api.LaminaState_LAMINA_STATE_COMMITTED, f.get(ids[0]).GetState(), "the event before it stays applied")
	require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, f.get(ids[1]).GetState())
	require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, f.get(ids[2]).GetState())
	require.Contains(t, logs.String(), `by="the store"`)

	// The node sends the batch again.
	f.core.d.eventTried = nil
	n, err := f.push(batch...)
	require.NoError(t, err)
	require.Equal(t, int32(3), n)
	for i := range ids {
		require.Equal(t, api.LaminaState_LAMINA_STATE_COMMITTED, f.get(ids[i]).GetState())
		require.Equal(t, api.AttemptState_ATTEMPT_STATE_STORED, f.attempt(ats[i]).GetState())
	}
	require.Equal(t, int64(30), f.storedBytes(), "nothing counted twice")
}

// A call runs ten transactions again in all: the refusal after that fails
// it at once, as an event that used its three tries does. A refusal once
// the call's context has ended is not tried again.
func TestEventRetriesAreBounded(t *testing.T) {
	f := newAcceptFixture(t)
	var batch []*api.Event
	var ids []pdid.Id
	for i := range eventRetries + 2 {
		id, at := f.lamina(time.Hour)
		ids = append(ids, id)
		batch = append(batch, f.stored(id, at, fmt.Sprintf("k%d", i), 1, false))
	}
	seen := map[string]bool{}
	f.core.d.eventTried = func(_ context.Context, ev *api.Event) error {
		k := string(ev.GetStored().GetLaminaId())
		if !seen[k] {
			seen[k] = true
			return deadlock
		}
		return nil
	}
	start := time.Now()
	_, err := f.push(batch...)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Less(t, time.Since(start), 3*time.Second)
	for i, id := range ids {
		want := api.LaminaState_LAMINA_STATE_COMMITTED
		if i >= eventRetries {
			want = api.LaminaState_LAMINA_STATE_PENDING
		}
		require.Equal(t, want, f.get(id).GetState(), "event %d", i)
	}

	// The call's context ends in the transaction: no second try.
	id, at := f.lamina(time.Hour)
	ctx, cancel := context.WithCancel(as(f.node))
	tries := 0
	f.core.d.eventTried = func(context.Context, *api.Event) error {
		tries++
		cancel()
		return deadlock
	}
	_, err = f.core.Node().PushEvents(ctx, api.NodePushEventsRequest_builder{Events: []*api.Event{f.stored(id, at, "late", 1, false)}}.Build())
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, 1, tries)
}

// An event refused for what it says (another node's sink) is skipped as
// before: the rest of the batch is applied, and the call answers.
func TestEventRefusedForWhatItSays(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { testEventRefusedForWhatItSays(t, newAcceptFixture(t)) })
	t.Run("PostgreSQL", func(t *testing.T) { testEventRefusedForWhatItSays(t, newAcceptFixtureOn(t, pgConfig(t))) })
}

func testEventRefusedForWhatItSays(t *testing.T, f *acceptFixture) {
	logs := f.logged()
	ctx := t.Context()
	other := pdid.New(DomNode)
	_, err := f.sink.Node().Add(ctx, api.NodeAddRequest_builder{Id: other.Bytes(), Alias: "n2", HardwareId: "n2", State: api.HostState_HOST_STATE_ADOPTED}.Build())
	require.NoError(t, err)
	dev, err := f.sink.Device().Add(ctx, api.DeviceAddRequest_builder{Alias: "d2", HardwareId: "d2", Node: api.NodeRef_builder{Id: other.Bytes()}.Build()}.Build())
	require.NoError(t, err)
	theirs := pdid.New(DomSink)
	_, err = f.sink.Sink().Add(ctx, api.SinkAddRequest_builder{
		Id: theirs.Bytes(), Alias: "s2", Node: api.NodeRef_builder{Id: other.Bytes()}.Build(), Device: api.DeviceRef_builder{Id: dev.GetId()}.Build(), Path: "/s2",
	}.Build())
	require.NoError(t, err)

	first, firstAt := f.lamina(time.Hour)
	wrong, _ := f.lamina(time.Hour)
	last, lastAt := f.lamina(time.Hour)
	foreign := f.stored(wrong, pdid.New(DomAttempt), "theirs", 10, false)
	foreign.GetStored().SetSinkId(theirs.Bytes())
	n, err := f.push(f.stored(first, firstAt, "a", 10, false), foreign, f.stored(last, lastAt, "c", 10, false))
	require.NoError(t, err)
	require.Equal(t, int32(2), n)
	require.Equal(t, api.LaminaState_LAMINA_STATE_COMMITTED, f.get(first).GetState())
	require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, f.get(wrong).GetState())
	require.Equal(t, api.LaminaState_LAMINA_STATE_COMMITTED, f.get(last).GetState())
	require.Contains(t, logs.String(), `by="the event"`)
	require.NotContains(t, logs.String(), "event tried again")
}

// storeRefused against errors made for real here: SQLite's busy, from a
// database another connection holds; a context that ended; a patch whose
// test did not hold. And against what each driver says, built by hand
// where it cannot be made here.
func TestStoreRefused(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "shale.db")
	// A short busy timeout, so that a held database refuses in
	// milliseconds rather than the minute the default DSN waits.
	f := newAcceptFixtureOn(t, config.DbConfig{Driver: "sqlite3", Dsn: "file:" + path + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(50)"})
	id, at := f.lamina(time.Hour)
	apply := func(ctx context.Context) error {
		return f.core.ownTx(ctx, func(ctx context.Context, own api.Server) error {
			return f.core.applyStored(ctx, own, f.node, f.stored(id, at, "k", 1, false).GetStored())
		})
	}

	t.Run("busy", func(t *testing.T) {
		db, _, err := config.DbConfig{Driver: "sqlite3", Dsn: "file:" + path + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(50)"}.Open(ctx)
		require.NoError(t, err)
		defer db.Close()
		held, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer held.Rollback()

		err = apply(ctx)
		require.ErrorIs(t, err, sqlite3.BUSY)
		require.True(t, storeRefused(err), "%v", err)

		_, err = f.push(f.stored(id, at, "k", 1, false))
		require.Equal(t, codes.Unavailable, status.Code(err), "busy on every try")
		require.NoError(t, held.Rollback())
		require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, f.get(id).GetState())
	})
	t.Run("a context that ended", func(t *testing.T) {
		ended, cancel := context.WithCancel(ctx)
		cancel()
		err := apply(ended)
		require.Error(t, err)
		require.True(t, storeRefused(err), "%v", err)

		// And one that ends inside the transaction.
		ending, cancel := context.WithCancel(ctx)
		err = f.core.ownTx(ending, func(ctx context.Context, own api.Server) error {
			cancel()
			_, err := own.Lamina().Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: id.Bytes()}.Build()}.Build())
			return err
		})
		require.Error(t, err)
		require.True(t, storeRefused(err), "%v", err)
	})
	t.Run("moved", func(t *testing.T) {
		v := f.get(id)
		_, err := f.sink.Lamina().Patch(ctx, api.LaminaPatchRequest_builder{Ref: api.LaminaRef_builder{Id: id.Bytes()}.Build(), Size: z.Ptr(int64(2)), DateUpdatedForce: z.Ptr(true)}.Build())
		require.NoError(t, err)
		_, err = f.sink.Lamina().Patch(ctx, api.LaminaPatchRequest_builder{Ref: api.LaminaRef_builder{Id: id.Bytes()}.Build(), Size: z.Ptr(int64(3)), DateUpdated: v.GetDateUpdated()}.Build())
		require.True(t, moved(err), "%v", err)
		require.True(t, storeRefused(err))
	})
	t.Run("the event's own", func(t *testing.T) {
		err := f.core.ownTx(ctx, func(ctx context.Context, own api.Server) error {
			ev := f.stored(id, at, "k", 1, false).GetStored()
			ev.SetSinkId(pdid.New(DomSink).Bytes())
			return f.core.applyStored(ctx, own, f.node, ev)
		})
		require.Error(t, err)
		require.False(t, storeRefused(err), "%v", err)
	})

	for _, c := range []struct {
		err  error
		want bool
	}{
		{&pgconn.PgError{Code: "40P01"}, true},                        // deadlock_detected
		{&pgconn.PgError{Code: "40001"}, true},                        // serialization_failure
		{fmt.Errorf("ent: %w", &pgconn.PgError{Code: "40001"}), true}, // wrapped
		{&pgconn.PgError{Code: "08006"}, true},                        // connection_failure
		{&pgconn.PgError{Code: "08003"}, true},                        // connection_does_not_exist
		{&pgconn.PgError{Code: "53300"}, true},                        // too_many_connections
		{&pgconn.PgError{Code: "53100"}, true},                        // disk_full
		{&pgconn.PgError{Code: "57P01"}, true},                        // admin_shutdown
		{&pgconn.PgError{Code: "57014"}, true},                        // query_canceled
		{&pgconn.PgError{Code: "23505"}, false},                       // unique_violation
		{&pgconn.PgError{Code: "23503"}, false},                       // foreign_key_violation
		{&pgconn.PgError{Code: "22P02"}, false},                       // invalid_text_representation
		{&pgconn.PgError{Code: "25P02"}, false},                       // in_failed_sql_transaction
		{&pgconn.PgError{Code: "40"}, false},
		{sqlite3.LOCKED, true},
		{sqlite3.BUSY, true},
		{sqlite3.CONSTRAINT, false},
		{driver.ErrBadConn, true},
		{sql.ErrConnDone, true},
		{fmt.Errorf("begin: %w", context.DeadlineExceeded), true},
		{status.Error(codes.Canceled, "context canceled"), true},
		{status.Error(codes.DeadlineExceeded, "deadline"), true},
		{status.Error(codes.FailedPrecondition, "a test in the patch did not hold"), true},
		{status.Error(codes.FailedPrecondition, "lamina is unknown and the event carries no record"), false},
		{status.Error(codes.PermissionDenied, "sink is not attached to this node"), false},
		{status.Error(codes.NotFound, "Set not found"), false},
		{status.Error(codes.InvalidArgument, "attempt_id"), false},
		{status.Error(codes.Internal, "record the write by x: deadlock detected"), false},
		{errors.New("deadlock detected"), false},
		{sql.ErrNoRows, false},
	} {
		require.Equal(t, c.want, storeRefused(c.err), "%#v", c.err)
	}
}

// A real deadlock on PostgreSQL, made by construction: ReportFailure holds
// the first of a lamina's two open attempts it wrote; the node's stored
// event of the other writes its attempt and the lamina, and waits for the
// held one; ReportFailure, released, waits for the stored event's
// attempt. The stored event waited first, so PostgreSQL's deadlock
// detection aborts it (40P01), and it is tried again once ReportFailure
// is through: the lamina is COMMITTED and the event applied. Before, the
// abort refused the event for good (#343). SQLite takes the database whole
// at BEGIN, so the two cannot interleave there.
func TestEventTriedAgainAfterADeadlock(t *testing.T) {
	f := newAcceptFixtureOn(t, pgConfig(t))
	logs := f.logged()
	id, a := f.lamina(time.Hour)
	b := f.another(id)

	written, release := make(chan pdid.Id, 1), make(chan struct{})
	f.core.d.failureHeld = func(ctx context.Context) {
		v, err := f.core.ent(ctx).Attempt.Query().Where(attempt.LaminaIdEQ(id.Uuid()), attempt.StateEQ(int32(api.AttemptState_ATTEMPT_STATE_FAILED))).Only(ctx)
		if err != nil {
			t.Error(err)
			written <- pdid.Nil
		} else {
			written <- pdid.Id(v.Id)
		}
		<-release
	}
	failed := make(chan error, 1)
	go func() {
		_, err := f.core.Lamina().ReportFailure(as(f.producer), api.LaminaReportFailureRequest_builder{Ref: api.LaminaRef_builder{Id: id.Bytes()}.Build(), Reason: "gave up"}.Build())
		failed <- err
	}()
	mine := a
	if <-written == a {
		mine = b
	}
	f.core.d.failureHeld = nil

	type pushed struct {
		n   int32
		err error
	}
	done := make(chan pushed, 1)
	go func() {
		n, err := f.push(f.stored(id, mine, "k", 10, false))
		done <- pushed{n, err}
	}()
	// The stored event reaches the held attempt and waits for it; it is
	// released inside the second PostgreSQL waits before it looks for a
	// deadlock.
	db, err := sql.Open("pgx", os.Getenv("SHALE_E2E_DB_DSN"))
	require.NoError(t, err)
	defer db.Close()
	require.Eventually(t, func() bool {
		var n int
		err := db.QueryRowContext(t.Context(), "select count(*) from pg_stat_activity where datname = current_database() and wait_event_type = 'Lock'").Scan(&n)
		return err == nil && n > 0
	}, 5*time.Second, 10*time.Millisecond, "the stored event waits for the held attempt")
	close(release)

	require.NoError(t, <-failed, "ReportFailure goes through")
	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, int32(1), got.n)
	require.Contains(t, logs.String(), "40P01", "the stored event was the one aborted")
	require.Contains(t, logs.String(), "event tried again")
	require.Equal(t, api.LaminaState_LAMINA_STATE_COMMITTED, f.get(id).GetState())
	require.Equal(t, api.AttemptState_ATTEMPT_STATE_STORED, f.attempt(mine).GetState())
}
