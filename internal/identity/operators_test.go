package identity

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"
)

// Operators by team at an external roster (§33.1): a member of the team is
// one, somebody else is not, nor anybody in another tenant; the answer is
// kept for the TTL and asked again after it, so leaving the team ends it;
// and a roster that does not answer is no, without being asked once per
// call.
func TestOperatorsByTeam(t *testing.T) {
	ctx := context.Background()
	f := newFakeRoster(t, "hday", "rk_test")
	team := f.addTeam("shale-ops", false)
	f.addTeam("shale-ops", true) // the same name in a site: not this one
	f.addTeam("other", false)
	alice, bob := f.addHolder("alice"), f.addHolder("bob")
	f.join(alice, team)

	s := f.open(t)
	o, err := NewOperators(s, OperatorsConfig{Team: "shale-ops"}, slog.Default())
	require.NoError(t, err)
	require.Equal(t, "hday", o.TenantAlias(), "the one tenant there is a key for")
	now := time.Unix(1_700_000_000, 0)
	o.Now = func() time.Time { return now }
	require.NoError(t, o.Check(ctx))

	tenant := pdid.Id(f.tenant.GetId())
	is, err := o.Is(ctx, tenant, pdid.Id(alice))
	require.NoError(t, err)
	require.True(t, is)
	is, err = o.Is(ctx, tenant, pdid.Id(bob))
	require.NoError(t, err)
	require.False(t, is)
	is, err = o.Is(ctx, pdid.Id(bob), pdid.Id(alice))
	require.NoError(t, err)
	require.False(t, is, "another tenant's person is nobody's operator")

	// Kept for the TTL: roster is not asked again.
	calls := f.calls.Load()
	is, _ = o.Is(ctx, tenant, pdid.Id(alice))
	require.True(t, is)
	require.Equal(t, calls, f.calls.Load())

	// Taken off the team, they stop being one once the TTL is up.
	f.leave(alice)
	now = now.Add(OperatorsTtl + time.Second)
	is, err = o.Is(ctx, tenant, pdid.Id(alice))
	require.NoError(t, err)
	require.False(t, is)

	// Roster down: no, and said, and not asked again for a while.
	f.join(alice, team)
	now = now.Add(OperatorsTtl + time.Second)
	f.down.Store(true)
	is, err = o.Is(ctx, tenant, pdid.Id(alice))
	require.Error(t, err)
	require.False(t, is, "fails closed")
	calls = f.calls.Load()
	_, err = o.Is(ctx, tenant, pdid.Id(alice))
	require.Error(t, err)
	require.Equal(t, calls, f.calls.Load(), "a failure is remembered briefly")
	f.down.Store(false)
	now = now.Add(operatorsRetry + time.Second)
	is, err = o.Is(ctx, tenant, pdid.Id(alice))
	require.NoError(t, err)
	require.True(t, is, "back once roster answers")
}

// The team is named as auth.operators says: by identifier, or by alias,
// where an alias naming two teams in no site is refused rather than one of
// them picked; and a key that does not open the tenant finds nothing.
func TestOperatorsFindTheTeam(t *testing.T) {
	ctx := context.Background()
	f := newFakeRoster(t, "hday", "rk_test")
	team := f.addTeam("shale-ops", false)
	alice := f.addHolder("alice")
	f.join(alice, team)
	s := f.open(t)

	byId, err := NewOperators(s, OperatorsConfig{Tenant: "hday", Team: pdid.Id(team).String()}, nil)
	require.NoError(t, err)
	is, err := byId.Is(ctx, pdid.Id(f.tenant.GetId()), pdid.Id(alice))
	require.NoError(t, err)
	require.True(t, is)

	f.addTeam("shale-ops", false)
	twice, err := NewOperators(s, OperatorsConfig{Team: "shale-ops"}, nil)
	require.NoError(t, err)
	require.ErrorIs(t, twice.Check(ctx), ErrNoTeam)
	is, err = twice.Is(ctx, pdid.Id(f.tenant.GetId()), pdid.Id(alice))
	require.Error(t, err)
	require.False(t, is)

	none, err := NewOperators(s, OperatorsConfig{Team: "nobody"}, nil)
	require.NoError(t, err)
	require.ErrorIs(t, none.Check(ctx), ErrNoTeam)

	_, err = NewOperators(s, OperatorsConfig{Tenant: "elsewhere", Team: "shale-ops"}, nil)
	require.Error(t, err, "a tenant with no key here")
	_, err = NewOperators(s, OperatorsConfig{}, nil)
	require.Error(t, err, "no team")

	wrong, err := Open(ctx, Config{Addr: f.addr, Insecure: true, Keys: map[string]string{"hday": "rk_wrong"}}, t.TempDir(), nil)
	require.NoError(t, err)
	defer wrong.Close()
	o, err := NewOperators(wrong, OperatorsConfig{Team: "shale-ops"}, nil)
	require.NoError(t, err)
	require.Error(t, o.Check(ctx))
}

// A roster that is not there at all -- nothing listening -- is no, too.
func TestOperatorsRosterUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	l.Close()

	s, err := Open(context.Background(), Config{Addr: addr, Insecure: true, Keys: map[string]string{"hday": "rk"}}, t.TempDir(), nil)
	require.NoError(t, err)
	defer s.Close()
	o, err := NewOperators(s, OperatorsConfig{Team: "shale-ops"}, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	is, err := o.Is(ctx, pdid.Nil, pdid.Nil)
	require.Error(t, err)
	require.False(t, is)
}

// The embedded roster gives the `shale` holder what the team lookup needs,
// so `auth.operators` works on a deployment that runs roster in-process.
func TestOperatorsEmbedded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := Open(ctx, Config{}, t.TempDir(), nil)
	require.NoError(t, err)
	defer s.Close()
	go s.Run(ctx)

	admin, _, err := s.Seed(ctx, "acme", "admin")
	require.NoError(t, err)
	carol, err := s.AddPerson(ctx, "acme", "carol", "Carol")
	require.NoError(t, err)
	team, err := s.AddTeam(ctx, "acme", "shale-ops")
	require.NoError(t, err)
	require.NoError(t, s.JoinTeam(ctx, team, admin.Id))

	_, err = NewOperators(s, OperatorsConfig{Team: "shale-ops"}, nil)
	require.Error(t, err, "the embedded roster has no single tenant to default to")
	o, err := NewOperators(s, OperatorsConfig{Tenant: "acme", Team: "shale-ops"}, nil)
	require.NoError(t, err)
	is, err := o.Is(ctx, admin.Tenant, admin.Id)
	require.NoError(t, err)
	require.True(t, is)
	is, err = o.Is(ctx, carol.Tenant, carol.Id)
	require.NoError(t, err)
	require.False(t, is)

	require.NoError(t, s.LeaveTeam(ctx, team, admin.Id))
	o.Forget(admin.Id)
	is, err = o.Is(ctx, admin.Tenant, admin.Id)
	require.NoError(t, err)
	require.False(t, is)
}
