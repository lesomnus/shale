package cli

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/protobuf-orm/ent/dialect"
)

// migrationLock is the PostgreSQL advisory lock Migrate holds, one key for
// the database: "shale/migrate" as eight bytes.
const migrationLock int64 = 0x7368616c656d6967

// lockMigration serializes Migrate across the processes that share one
// PostgreSQL database (#165). `serve control` and `serve cluster` of one
// upgrade start together, and each brings the schema up to date before
// it listens; two at once each saw a column missing and added it, and one
// exited on the duplicate (42701). The lock is the session's, on one
// connection held for the migration, so a process that dies lets it go
// with its connection. Waiting for it ends with the context. Other
// dialects have nothing to share: SQLite is one process's file.
func lockMigration(ctx context.Context, db *sql.DB, d string) (func(), error) {
	if d != dialect.Postgres {
		return func() {}, nil
	}
	// The lock's connection is one more than the migration uses, so a pool
	// held to `db.max_open_conns` gets one more while it lasts. Migrate runs
	// at startup, before anything else uses the pool.
	restore := func() {}
	if n := db.Stats().MaxOpenConnections; n > 0 {
		db.SetMaxOpenConns(n + 1)
		restore = func() { db.SetMaxOpenConns(n) }
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		restore()

		return nil, fmt.Errorf("migration lock: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrationLock); err != nil {
		conn.Close()
		restore()

		return nil, fmt.Errorf("migration lock: %w", err)
	}

	return func() {
		defer restore()
		// Whatever became of the migration's context.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", migrationLock); err != nil {
			// A connection that still holds the lock does not go back to
			// the pool: closed, its session ends, and the lock with it.
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		conn.Close()
	}, nil
}
