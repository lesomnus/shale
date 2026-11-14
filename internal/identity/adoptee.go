package identity

import "github.com/lesomnus/payday/pdid"

// Adoptee is a person Adopt writes, as Shale knows them.
type Adoptee struct {
	Id    pdid.Id
	Alias string
	Name  string
	// Admin binds them to the role that administers the tenant at roster.
	Admin bool
}
