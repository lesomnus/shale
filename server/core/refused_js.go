//go:build js

package core

import (
	"github.com/lesomnus/sqlite3-wasm/binding"
)

// driverRefused reads the errors of the driver the sandbox runs on (§40):
// SQLite in the page, whose busy and locked are binding.IsBusy.
func driverRefused(err error) bool {
	return binding.IsBusy(err)
}
