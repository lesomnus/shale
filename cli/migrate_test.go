package cli

import (
	"context"
	"path/filepath"
	"testing"

	entsql "github.com/protobuf-orm/ent/dialect/sql"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/config"

	"github.com/lesomnus/shale/cmd"
	entmigrate "github.com/lesomnus/shale/internal/ent/migrate"
)

// TestMovedIndexes holds the names Migrate moves to the schema: each is
// taken to an index this build declares on its table, from a name it does
// not declare.
func TestMovedIndexes(t *testing.T) {
	declared := map[string]map[string]bool{}
	for _, tb := range entmigrate.Tables {
		declared[tb.Name] = map[string]bool{}
		for _, idx := range tb.Indexes {
			declared[tb.Name][idx.Name] = true
		}
	}
	for _, v := range moved {
		require.True(t, declared[v.Table][v.To], "%s.%s is no index this schema declares", v.Table, v.To)
		require.False(t, declared[v.Table][v.From], "%s.%s is an index this schema declares", v.Table, v.From)
	}
}

// TestMigrateMovesAnIndexWhoseColumnsChanged is a database an earlier build
// made, as one index of it: the lamina's by source and time, which that
// build stored with the field first, under the name of those columns. The
// migration leaves the index the schema declares, in its order, and not
// the old one beside it.
func TestMigrateMovesAnIndexWhoseColumnsChanged(t *testing.T) {
	ctx := context.Background()
	c := config.DbConfig{Driver: "sqlite3", Dsn: "file:" + filepath.Join(t.TempDir(), "shale.db") + "?_pragma=foreign_keys(1)"}
	db, dia, err := c.Open(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	s := &cmd.Server{Db: db, Drv: entsql.OpenDB(dia, db), Dialect: dia}
	require.NoError(t, Migrate(ctx, s))

	for _, q := range []string{
		`DROP INDEX lamina_source_time`,
		`CREATE INDEX lamina_date_started_source_id ON lamina (date_started, source_id)`,
	} {
		_, err := s.Db.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	require.NoError(t, Migrate(ctx, s))

	rows, err := s.Db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'lamina'`)
	require.NoError(t, err)
	var names []string
	for rows.Next() {
		var v string
		require.NoError(t, rows.Scan(&v))
		names = append(names, v)
	}
	require.NoError(t, rows.Err())
	require.NotContains(t, names, "lamina_date_started_source_id")
	require.Contains(t, names, "lamina_source_time")

	rows, err = s.Db.QueryContext(ctx, `SELECT name FROM pragma_index_info('lamina_source_time') ORDER BY seqno`)
	require.NoError(t, err)
	var cols []string
	for rows.Next() {
		var v string
		require.NoError(t, rows.Scan(&v))
		cols = append(cols, v)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"source_id", "date_started"}, cols)
}
