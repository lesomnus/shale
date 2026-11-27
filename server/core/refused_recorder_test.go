package core

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/lesomnus/payday/config"
	_ "github.com/lesomnus/payday/config/dbsqlite3"
	"github.com/protobuf-orm/ent/dialect/sql"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/enttest"
	"github.com/lesomnus/shale/server/bare"
	"github.com/lesomnus/shale/server/pd"
)

// recorderOf is a recorder that answers what a test tells it to.
type recorderOf func() error

func (r recorderOf) Record(context.Context, bare.Server, bare.Change) error { return r() }

// TestStoreRefusedReadsARecordersFailure: a refusal of the store's met
// while a recorder's row is written, the trail's or the outbox's, in a
// write's transaction is the store's too, and is tried again like one met
// on the entity's own statement. The generated store answers a recorder's
// failure Internal, in its own words, and since lesomnus/payday#325 that
// answer unwraps to what the recorder returned; before, it kept the
// words alone, and a deadlock on the trail's row read as the write's own
// fault.
func TestStoreRefusedReadsARecordersFailure(t *testing.T) {
	ctx := t.Context()
	db, dialect, err := config.DbConfig{Driver: "sqlite3", Dsn: "file:" + filepath.Join(t.TempDir(), "shale.db") + "?_pragma=foreign_keys(1)"}.Open(ctx)
	require.NoError(t, err)
	client := enttest.NewClient(t, enttest.WithOptions(ent.Driver(sql.OpenDB(dialect, db))))
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	var fail error
	sink, err := pd.NewSink(client, bare.WithMinter(pd.Minter()), bare.WithRecorder(recorderOf(func() error { return fail })))
	require.NoError(t, err)

	for _, c := range []struct {
		name    string
		err     error
		refused bool
	}{
		{"a deadlock on the recorder's row", fmt.Errorf("trail: %w", deadlock), true},
		{"a connection gone under the recorder", fmt.Errorf("trail: %w", driver.ErrBadConn), true},
		{"a failure of the recorder's own", errors.New("trail: a row it would not write"), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			fail = c.err
			_, err := sink.Tenant().Add(ctx, api.TenantAddRequest_builder{Alias: "acme"}.Build())
			require.Error(t, err)
			require.Equal(t, codes.Internal, status.Code(err), "the answer's code is as it was")
			require.ErrorIs(t, err, c.err, "the answer unwraps to what the recorder returned")
			require.Equal(t, c.refused, storeRefused(err), "%v", err)
		})
	}

	// A write the recorder lets through is written: the failures above left
	// nothing behind, each in a transaction that was rolled back.
	fail = nil
	_, err = sink.Tenant().Add(ctx, api.TenantAddRequest_builder{Alias: "acme"}.Build())
	require.NoError(t, err)
}
