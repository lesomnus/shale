package cli

import (
	"context"
	"os"
	"testing"
	"time"

	entsql "github.com/protobuf-orm/ent/dialect/sql"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/lesomnus/payday/config"

	"github.com/lesomnus/shale/cmd"
)

// pgServer opens its own pool on SHALE_E2E_DB_DSN, as each process of a
// deployment does.
func pgServer(t *testing.T, ctx context.Context, dsn string) *cmd.Server {
	t.Helper()
	db, dia, err := config.DbConfig{Driver: "pgx", Dsn: dsn}.Open(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	return &cmd.Server{Db: db, Drv: entsql.OpenDB(dia, db), Dialect: dia}
}

// Processes sharing one PostgreSQL database migrate it at startup at the
// same time, `serve control` and `serve cluster` of one upgrade (#165): an
// older schema, here one without `lamina.capture_epoch`, has the column
// added once, and none of them exits on a duplicate column. Run with
// SHALE_E2E_DB_DSN; the database is emptied first.
func TestConcurrentMigrationOnPostgreSQL(t *testing.T) {
	dsn := os.Getenv("SHALE_E2E_DB_DSN")
	if dsn == "" {
		t.Skip("SHALE_E2E_DB_DSN names no PostgreSQL database")
	}
	ctx := context.Background()
	admin := pgServer(t, ctx, dsn)
	reset := func() {
		_, err := admin.Db.ExecContext(ctx, "drop schema public cascade; create schema public")
		require.NoError(t, err)
	}
	concurrently := func() error {
		var g errgroup.Group
		for range 4 {
			s := pgServer(t, ctx, dsn)
			g.Go(func() error { return Migrate(ctx, s) })
		}

		return g.Wait()
	}

	for i := range 5 {
		reset()
		require.NoError(t, concurrently(), "an empty database, round %d", i)
	}
	// A pool held to one connection still migrates: the lock's is one more.
	reset()
	one := pgServer(t, ctx, dsn)
	one.Db.SetMaxOpenConns(1)
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, Migrate(bounded, one))
	require.Equal(t, 1, one.Db.Stats().MaxOpenConnections, "the pool's limit is given back")

	for i := range 5 {
		reset()
		require.NoError(t, Migrate(ctx, admin))
		_, err := admin.Db.ExecContext(ctx, "ALTER TABLE lamina DROP COLUMN capture_epoch")
		require.NoError(t, err)
		require.NoError(t, concurrently(), "an older schema, round %d", i)
		var n int
		require.NoError(t, admin.Db.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'lamina' AND column_name = 'capture_epoch'`).Scan(&n))
		require.Equal(t, 1, n)
	}
}
