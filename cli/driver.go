package cli

// The database engines this app runs on in a process (§34.2): SQLite for a
// single machine, PostgreSQL whenever more than one CP process runs. They
// are blank-imported here rather than in `cmd` so that nothing that only
// needs `cmd.Build` links an engine it never opens.
import (
	_ "github.com/lesomnus/payday/config/brokerpg"
	_ "github.com/lesomnus/payday/config/dbpgx"
	_ "github.com/lesomnus/payday/config/dbsqlite3"
)
