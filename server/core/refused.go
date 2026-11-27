package core

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// storeRefused says that a transaction was refused for a reason of the
// store's: it could not be finished just then, and would have gone
// through a moment later. It is read positively, from what the database
// or the driver says where there is something to read:
//
//   - PostgreSQL's SQLSTATE classes 40 (transaction rollback: a deadlock,
//     a serialization failure), 08 (connection), 53 (insufficient
//     resources) and 57 (operator intervention: the server shutting down),
//     as pgx's *pgconn.PgError carries them; and an error pgx says
//     happened before anything was sent (pgconn.SafeToRetry);
//   - SQLite's busy and locked, as the driver's error carries them;
//   - a connection database/sql says is gone (driver.ErrBadConn,
//     sql.ErrConnDone);
//   - a context that ended, as the context's own error or as the status
//     it was turned into;
//   - a patch whose test did not hold (moved): the row moved under the
//     write, which is a reason to read it again.
//
// The generated servers and ent hand a driver's error up as it is, or
// wrapped with %w, so errors.As finds it. Anything else is the
// transaction's own: what it was asked to do is wrong, and doing it again
// would answer the same.
func storeRefused(err error) bool {
	if err == nil {
		return false
	}
	if moved(err) {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if v, ok := status.FromError(err); ok && (v.Code() == codes.Canceled || v.Code() == codes.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, sql.ErrConnDone) {
		return true
	}

	return driverRefused(err)
}

// pgRefused says a PostgreSQL SQLSTATE is of a class storeRefused takes
// for the store's.
func pgRefused(code string) bool {
	if len(code) != 5 {
		return false
	}
	switch code[:2] {
	case "40", "08", "53", "57":
		return true
	}

	return false
}
