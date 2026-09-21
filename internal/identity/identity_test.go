package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
