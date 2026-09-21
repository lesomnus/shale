// Package dsn is the one place a database DSN is made right before it is
// opened.
package dsn

import "strings"

// SQLiteTimeFormat is what a SQLite DSN of this app carries so that time
// values compare as times (§34.2). The driver's default writes RFC 3339
// text with the fractional seconds trimmed, and "…:42Z" sorts after
// "…:42.093Z", so a row dated on the second was missed by a window ending
// a few milliseconds later in that same second: the slot lookup of an
// allocation, a timeline's bound, a sweep's cutoff. Nanoseconds since the
// epoch, as an integer, compare numerically, keep every digit Go has, and
// carry no zone. The driver's `sqlite` format would sort too, but it
// keeps whole seconds only.
const SQLiteTimeFormat = "_timefmt=unixepoch_nano"

// SQLite gives a SQLite DSN the time format when it names none.
func SQLite(v string) string {
	if strings.Contains(v, "_timefmt=") {
		return v
	}
	switch {
	case strings.Contains(v, "?"):
		return v + "&" + SQLiteTimeFormat
	default:
		return v + "?" + SQLiteTimeFormat
	}
}

// Normalize gives the DSN of a driver what this app needs of it.
func Normalize(driver, v string) string {
	if driver == "sqlite3" {
		return SQLite(v)
	}

	return v
}
