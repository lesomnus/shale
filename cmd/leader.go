package cmd

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
)

// The leader (§34.9): exactly one CP process runs the multi-step jobs and
// the directives. On PostgreSQL that is whoever holds an advisory lock on
// a dedicated connection; the lock is released the moment the process or
// its connection dies. SQLite serves one process, which is the leader.

// leaderKey is the advisory lock's key: "shale" as a number nobody else in
// the database is likely to use.
const leaderKey int64 = 0x7368616c65

// Leader is the lease.
type Leader struct {
	db  *sql.DB
	log *slog.Logger

	mu   sync.Mutex
	conn *sql.Conn
	held bool
}

// NewLeader makes a lease over the database.
func NewLeader(db *sql.DB, log *slog.Logger) *Leader {
	return &Leader{db: db, log: log}
}

// Is says whether this process holds the lock now, taking it when it is
// free. A connection that broke is dropped, and the lock with it: the next
// call tries again on a new one.
func (l *Leader) Is(ctx context.Context) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn != nil {
		if err := l.conn.PingContext(ctx); err != nil {
			l.log.Warn("leader: the lock's connection is gone", "err", err.Error())
			l.conn.Close()
			l.conn, l.held = nil, false
		}
	}
	if l.conn == nil {
		c, err := l.db.Conn(ctx)
		if err != nil {
			l.log.Warn("leader", "err", err.Error())
			return false
		}
		l.conn = c
	}
	if l.held {
		return true
	}
	var got bool
	if err := l.conn.QueryRowContext(ctx, "select pg_try_advisory_lock($1)", leaderKey).Scan(&got); err != nil {
		l.log.Warn("leader", "err", err.Error())
		l.conn.Close()
		l.conn = nil
		return false
	}
	if got {
		l.log.Info("leader: this process runs the jobs and the directives")
	}
	l.held = got

	return got
}

// Close gives the lock up.
func (l *Leader) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn != nil {
		l.conn.Close()
		l.conn, l.held = nil, false
	}
}
