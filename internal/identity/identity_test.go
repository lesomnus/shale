package identity

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"
)

// The embedded roster (§33.1): seeded people sign in with the password
// shown once, a wrong one is refused and says nothing else, a person made
// later gets a password issued, and every call goes as the `shale` holder
// the store wrote into the tenant.
func TestEmbeddedRoster(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := Open(ctx, Config{}, t.TempDir(), nil)
	require.NoError(t, err)
	defer s.Close()
	go s.Run(ctx)
	require.True(t, s.Embedded())

	ops, opPass, err := s.Seed(ctx, "cluster", "ops")
	require.NoError(t, err)
	admin, adminPass, err := s.Seed(ctx, "acme", "admin")
	require.NoError(t, err)
	require.NotEmpty(t, adminPass)
	require.NotEqual(t, opPass, adminPass)
	require.Equal(t, "admin", admin.Alias)
	require.Equal(t, "acme", admin.TenantAlias)
	require.NotEqual(t, ops.Tenant, admin.Tenant)

	p, err := s.Verify(ctx, "acme", "admin", adminPass)
	require.NoError(t, err)
	require.Equal(t, admin.Id, p.Id)
	require.Equal(t, admin.Tenant, p.Tenant)

	_, err = s.Verify(ctx, "acme", "admin", "wrong")
	require.ErrorIs(t, err, ErrRefused)
	_, err = s.Verify(ctx, "acme", "nobody", adminPass)
	require.ErrorIs(t, err, ErrRefused)
	_, err = s.Verify(ctx, "nowhere", "admin", adminPass)
	require.ErrorIs(t, err, ErrNoTenant)
	// The operators' password opens nothing in the other tenant.
	_, err = s.Verify(ctx, "acme", "ops", opPass)
	require.ErrorIs(t, err, ErrRefused)

	// Looked up by name, with nothing to check.
	q, err := s.Lookup(ctx, "acme", "admin")
	require.NoError(t, err)
	require.Equal(t, admin.Id, q.Id)
	_, err = s.Lookup(ctx, "acme", "nobody")
	require.ErrorIs(t, err, ErrNoPerson)

	id, name, err := s.Tenant(ctx, "acme")
	require.NoError(t, err)
	require.Equal(t, admin.Tenant, id)
	require.Empty(t, name)
	_, _, err = s.Tenant(ctx, "nowhere")
	require.ErrorIs(t, err, ErrNoTenant)

	// A person made later, and the password issued to them.
	bob, err := s.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	require.Equal(t, admin.Tenant, bob.Tenant)
	_, err = s.Verify(ctx, "acme", "bob", "anything")
	require.ErrorIs(t, err, ErrRefused, "no password yet")
	bobPass, err := s.IssuePassword(ctx, "acme", "bob")
	require.NoError(t, err)
	p, err = s.Verify(ctx, "acme", "bob", bobPass)
	require.NoError(t, err)
	require.Equal(t, bob.Id, p.Id)
	require.NoError(t, s.SetPassword(ctx, "acme", "bob", "correct horse battery"))
	_, err = s.Verify(ctx, "acme", "bob", bobPass)
	require.ErrorIs(t, err, ErrRefused, "the old one is gone")
	_, err = s.Verify(ctx, "acme", "bob", "correct horse battery")
	require.NoError(t, err)

	// Enough wrong answers close the account for a while.
	var locked bool
	for range 12 {
		_, err = s.Verify(ctx, "acme", "bob", "wrong")
		var l Locked
		if errors.As(err, &l) {
			require.True(t, l.Until.After(time.Now()))
			locked = true
			break
		}
	}
	require.True(t, locked, "a lockout after repeated wrong answers")

	vs, err := s.Tenants(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"cluster", "acme"}, vs)
}

// A deployment made before roster held its people brings them along with
// their identifiers (§33.1): adopted once, verified with the password
// issued, and left alone the second time.
func TestAdopt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := Open(ctx, Config{}, t.TempDir(), nil)
	require.NoError(t, err)
	defer s.Close()

	tenant, ops, viewer := pdid.New(1), pdid.New(2), pdid.New(2)
	people := []Adoptee{{Id: ops, Alias: "ops", Name: "Ops", Admin: true}, {Id: viewer, Alias: "viewer"}}
	passwords, err := s.Adopt(ctx, tenant, "legacy", "Legacy Ltd", people)
	require.NoError(t, err)
	require.Len(t, passwords, 2)
	p, err := s.Verify(ctx, "legacy", "ops", passwords["ops"])
	require.NoError(t, err)
	require.Equal(t, ops, p.Id, "the identifier from before")
	require.Equal(t, tenant, p.Tenant)
	id, _, err := s.Tenant(ctx, "legacy")
	require.NoError(t, err)
	require.Equal(t, tenant, id)

	again, err := s.Adopt(ctx, tenant, "legacy", "Legacy Ltd", people)
	require.NoError(t, err)
	require.Empty(t, again, "nothing made twice")
	_, err = s.Verify(ctx, "legacy", "ops", passwords["ops"])
	require.NoError(t, err, "and the password stands")
}

// roster makes a tenant with an administrator of its own, `admin`
// (`server/core/tenant.go`), which is what `shale init` calls the first
// person too: the one adopted under that name is the person from before,
// with their identifier, and roster's is not left beside them holding the
// tenant.
func TestAdoptTheFirstPersonCalledAdmin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := Open(ctx, Config{}, t.TempDir(), nil)
	require.NoError(t, err)
	defer s.Close()

	tenant, admin, bob := pdid.New(1), pdid.New(2), pdid.New(2)
	people := []Adoptee{{Id: admin, Alias: "admin", Name: "Admin", Admin: true}, {Id: bob, Alias: "bob"}}
	passwords, err := s.Adopt(ctx, tenant, "acme", "", people)
	require.NoError(t, err)
	require.Len(t, passwords, 2)
	p, err := s.Verify(ctx, "acme", "admin", passwords["admin"])
	require.NoError(t, err)
	require.Equal(t, admin, p.Id, "the identifier from before")

	again, err := s.Adopt(ctx, tenant, "acme", "", people)
	require.NoError(t, err)
	require.Empty(t, again, "nothing made twice")
}

// A roster.db the build before this one made (shale ab1ea96, roster
// c7bca4b8) is not carried across this roster, which keeps nothing for an
// earlier one: the store refuses to open it, and says what a deployment of
// Shale does about it rather than leaving roster's words about columns.
func TestEmbeddedRosterFromBefore(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Open("testdata/roster-ab1ea96.db.gz")
	require.NoError(t, err)
	defer f.Close()
	z, err := gzip.NewReader(f)
	require.NoError(t, err)
	b, err := io.ReadAll(z)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, RosterDbFile), b, 0o600))

	_, err = Open(context.Background(), Config{}, dir, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRosterFromBefore)
	require.Contains(t, err.Error(), "shale identity migrate")
	require.Contains(t, err.Error(), "§32.2")
}
