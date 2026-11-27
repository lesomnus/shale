//go:build !js

package core

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ncruces/go-sqlite3"
)

// driverRefused reads the errors of the drivers a control plane runs on:
// pgx for PostgreSQL, and ncruces/go-sqlite3 for SQLite.
func driverRefused(err error) bool {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pgRefused(pg.Code)
	}
	if pgconn.SafeToRetry(err) {
		return true
	}

	return errors.Is(err, sqlite3.BUSY) || errors.Is(err, sqlite3.LOCKED)
}
