package cmd_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
)

// Time values in SQLite must sort as they read (§34.2). The driver's text
// default trims fractional seconds, so a row dated on the second sorted
// after a bound a few milliseconds into that same second, and the slot
// lookup of an allocation missed the lamina it had just made: that was
// TestVerticalSlice's flake, one run in fifty. payday opens a SQLite DSN
// that names no format with the integer one its driver spells, and this
// is that reaching the rows of an app that says nothing about it.
func TestSQLiteTimesSortAsText(t *testing.T) {
	c := &cmd.Config{}
	cli.ApplyDev(c, t.TempDir())
	require.NotContains(t, c.Db.Dsn, "_timefmt", "the app names no time format; the driver's default is payday's business")

	ctx := context.Background()
	s, err := cmd.Build(ctx, *c)
	require.NoError(t, err)
	defer s.Close()

	_, err = s.Db.ExecContext(ctx, "CREATE TABLE t (at datetime)")
	require.NoError(t, err)
	onTheSecond := time.Date(2026, 9, 21, 5, 38, 42, 0, time.UTC)
	_, err = s.Db.ExecContext(ctx, "INSERT INTO t (at) VALUES (?)", onTheSecond)
	require.NoError(t, err)

	count := func(q string, arg time.Time) int {
		var n int
		require.NoError(t, s.Db.QueryRowContext(ctx, q, arg).Scan(&n))
		return n
	}
	require.Equal(t, 1, count("SELECT count(*) FROM t WHERE at < ?", onTheSecond.Add(93*time.Millisecond)), "a bound later in the same second is later")
	require.Equal(t, 0, count("SELECT count(*) FROM t WHERE at < ?", onTheSecond), "not before itself")
	require.Equal(t, 1, count("SELECT count(*) FROM t WHERE at >= ?", onTheSecond.Add(-time.Second+500*time.Millisecond)))
	require.Equal(t, 0, count("SELECT count(*) FROM t WHERE at > ?", onTheSecond.Add(7*time.Millisecond)))

	// And it reads back as the time it was.
	var back time.Time
	require.NoError(t, s.Db.QueryRowContext(ctx, "SELECT at FROM t").Scan(&back))
	require.True(t, back.Equal(onTheSecond), "%s", back)
}
