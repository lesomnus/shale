package identity

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/roster/rstr"
	"github.com/lesomnus/z"
)

// Operators by what roster grants (§33.1), at an external roster: one held
// pattern covering the method is a yes on its own, a service is that
// service, and all of Shale administers; roster's `methods` -- wider than
// `everywhere` here -- is not read; a person of another tenant does not
// operate the cluster; the answer is kept for the TTL and asked again
// after it, so a grant taken away ends; and a roster that does not answer
// is no, without being asked once per call.
func TestOperatorsByGrant(t *testing.T) {
	ctx := context.Background()
	f := newFakeRoster(t, "hday")
	alice, bob, carol := f.addHolder("alice"), f.addHolder("bob"), f.addHolder("carol")
	f.grant(alice, Everything)
	f.grant(bob, "/shale.SourceService/*")

	s := f.open(t)
	o, err := NewOperators(s, OperatorsConfig{Tenant: "hday"}, slog.Default())
	require.NoError(t, err)
	require.Equal(t, "hday", o.TenantAlias())
	now := time.Unix(1_700_000_000, 0)
	o.Now = func() time.Time { return now }
	require.NoError(t, o.Check(ctx))

	tenant := pdid.Id(f.tenant.GetId())
	may := func(holder []byte, method string) bool {
		t.Helper()
		ok, err := o.May(ctx, tenant, pdid.Id(holder), method)
		require.NoError(t, err)

		return ok
	}
	require.True(t, may(alice, "/shale.SetService/Add"))
	require.True(t, may(alice, "/shale.NodeService/Adopt"))
	require.True(t, may(bob, "/shale.SourceService/Add"))
	require.False(t, may(bob, "/shale.SetService/Add"), "one service is that service")
	require.False(t, may(carol, "/shale.SetService/Add"), "granted nothing across the tenant, whatever `methods` says")

	is, err := o.Administers(ctx, tenant, pdid.Id(alice))
	require.NoError(t, err)
	require.True(t, is)
	is, err = o.Administers(ctx, tenant, pdid.Id(bob))
	require.NoError(t, err)
	require.False(t, is, "a service is not all of Shale")
	is, err = o.Operates(ctx, tenant, pdid.Id(alice))
	require.NoError(t, err)
	require.True(t, is)
	is, err = o.MayOperate(ctx, pdid.New(1), pdid.Id(alice), "/shale.NodeService/Adopt")
	require.NoError(t, err)
	require.False(t, is, "the cluster is the operators' tenant's, whatever another tenant grants")

	// Kept for the TTL: roster is not asked again.
	calls := f.calls.Load()
	require.True(t, may(alice, "/shale.SetService/Add"))
	require.Equal(t, calls, f.calls.Load())

	// Taken away, it ends once the TTL is up.
	f.grant(alice)
	now = now.Add(OperatorsTtl + time.Second)
	require.False(t, may(alice, "/shale.SetService/Add"))

	// Roster down: no, and said, and not asked again for a while.
	f.grant(alice, Everything)
	now = now.Add(OperatorsTtl + time.Second)
	f.down.Store(true)
	ok, err := o.May(ctx, tenant, pdid.Id(alice), "/shale.SetService/Add")
	require.Error(t, err)
	require.False(t, ok, "fails closed")
	calls = f.calls.Load()
	_, err = o.May(ctx, tenant, pdid.Id(alice), "/shale.SetService/Add")
	require.Error(t, err)
	require.Equal(t, calls, f.calls.Load(), "a failure is remembered briefly")
	f.down.Store(false)
	now = now.Add(operatorsRetry + time.Second)
	require.True(t, may(alice, "/shale.SetService/Add"), "back once roster answers")
}

// `auth.operators` says whose people operate the cluster and nothing about
// a team: a team or a site is refused with what to do instead, rather than
// ignored into a deployment where nobody is an operator; and the tenant
// has to be said, and be one there is a key for.
func TestOperatorsAreNotATeam(t *testing.T) {
	f := newFakeRoster(t, "hday")
	s := f.open(t)

	_, err := NewOperators(s, OperatorsConfig{Tenant: "hday", Team: "shale-ops"}, nil)
	require.ErrorIs(t, err, ErrTeam)
	_, err = NewOperators(s, OperatorsConfig{Site: "seoul"}, nil)
	require.ErrorIs(t, err, ErrTeam)
	require.True(t, OperatorsConfig{Team: "shale-ops"}.On(), "a team left in a configuration is refused, not ignored")

	_, err = NewOperators(s, OperatorsConfig{}, nil)
	require.Error(t, err, "whose people")
	_, err = NewOperators(s, OperatorsConfig{Tenant: "elsewhere"}, nil)
	require.Error(t, err, "a tenant with no key here")

	wrong := f.openWith(t, Config{Keys: map[string]string{"hday": "rt_wrong"}})
	o, err := NewOperators(wrong, OperatorsConfig{Tenant: "hday"}, nil)
	require.NoError(t, err)
	require.Error(t, o.Check(context.Background()))
}

// A roster that is not there at all -- nothing listening -- is no, too.
func TestOperatorsRosterUnreachable(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	l.Close()

	s, err := Open(context.Background(), Config{Addr: addr, Insecure: true, Keys: map[string]string{"hday": "rt_test"}}, t.TempDir(), nil)
	require.NoError(t, err)
	defer s.Close()
	o, err := NewOperators(s, OperatorsConfig{Tenant: "hday"}, nil)
	require.NoError(t, err)
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	is, err := o.Operates(c, pdid.Nil, pdid.Nil)
	require.Error(t, err)
	require.False(t, is)
}

// The embedded roster gives the `shale` holder what asking about grants
// needs, so `auth.operators` works on a deployment that runs roster
// in-process: a person granted all of Shale administers and operates, and
// stops once the binding is gone.
func TestOperatorsEmbedded(t *testing.T) {
	c, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := Open(c, Config{}, t.TempDir(), nil)
	require.NoError(t, err)
	defer s.Close()
	go s.Run(c)

	admin, _, err := s.Seed(c, "acme", "admin")
	require.NoError(t, err)
	carol, err := s.AddPerson(c, "acme", "carol", "Carol")
	require.NoError(t, err)

	o, err := NewOperators(s, OperatorsConfig{Tenant: "acme"}, nil)
	require.NoError(t, err)
	is, err := o.Administers(c, admin.Tenant, admin.Id)
	require.NoError(t, err)
	require.False(t, is, "administering roster is not administering Shale")

	b, err := s.Grant(c, "acme", OperatorRole, []string{Everything}, admin.Id)
	require.NoError(t, err)
	o.Forget(admin.Id)
	is, err = o.Administers(c, admin.Tenant, admin.Id)
	require.NoError(t, err)
	require.True(t, is)
	is, err = o.Operates(c, admin.Tenant, admin.Id)
	require.NoError(t, err)
	require.True(t, is)
	is, err = o.Administers(c, carol.Tenant, carol.Id)
	require.NoError(t, err)
	require.False(t, is)

	require.NoError(t, s.Ungrant(c, b))
	o.Forget(admin.Id)
	is, err = o.Administers(c, admin.Tenant, admin.Id)
	require.NoError(t, err)
	require.False(t, is)
}

// An issuer's `sub` is a Holder.id at roster: found with the key of a tenant
// served here, nobody when no key sees them, and an error -- not nobody --
// when roster does not answer.
func TestById(t *testing.T) {
	c := context.Background()
	f := newFakeRoster(t, "hday")
	alice := f.addHolder("alice")
	s := f.open(t)

	p, err := s.ById(c, pdid.Id(alice).String())
	require.NoError(t, err)
	require.Equal(t, "alice", p.Alias)
	require.Equal(t, "hday", p.TenantAlias)
	require.Equal(t, pdid.Id(f.tenant.GetId()), p.Tenant)

	_, err = s.ById(c, pdid.New(2).String())
	require.ErrorIs(t, err, ErrNoPerson)
	_, err = s.ById(c, "alice")
	require.ErrorIs(t, err, ErrNoPerson)

	f.down.Store(true)
	_, err = s.ById(c, pdid.Id(alice).String())
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrNoPerson)
}

// roster's gate reads roster's roles alone, and Reaches asked with no `at`
// is answered of the asking app's roles (#321): Shale is installed in the
// tenant as an app, as `roster app install --serves '/shale.*/*'` does it,
// and what Grant writes naming Shale's methods is a role **of the app
// Shale**, bound to the person. A role of roster's makes nobody an
// operator; somebody bound nothing reaches nothing, so reads nothing
// where reads are granted; and another app's holder bound a role of
// Shale's naming the delegation's methods reaches them, where one not
// bound reaches nothing.
func TestShalesRolesAreTheApps(t *testing.T) {
	c, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := Open(c, Config{}, t.TempDir(), nil)
	require.NoError(t, err)
	defer s.Close()
	go s.Run(c)

	admin, _, err := s.Seed(c, "acme", "admin")
	require.NoError(t, err)
	nobody, err := s.AddPerson(c, "acme", "nobody", "Nobody")
	require.NoError(t, err)
	portal, err := s.AddPerson(c, "acme", "portal", "Portal")
	require.NoError(t, err)
	kiosk, err := s.AddPerson(c, "acme", "kiosk", "Kiosk")
	require.NoError(t, err)

	o, err := NewOperators(s, OperatorsConfig{Tenant: "acme", Reads: ReadsGranted}, nil)
	require.NoError(t, err)

	// The tenant's administrator at roster holds `/roster.*/*`, which is
	// nothing at Shale.
	is, err := o.Operates(c, admin.Tenant, admin.Id)
	require.NoError(t, err)
	require.False(t, is, "a role of roster's is nothing at Shale")

	// Granted all of Shale: a role of the app Shale, bound to the person.
	_, err = s.Grant(c, "acme", OperatorRole, []string{Everything}, admin.Id)
	require.NoError(t, err)
	o.Forget(admin.Id)
	is, err = o.Operates(c, admin.Tenant, admin.Id)
	require.NoError(t, err)
	require.True(t, is)
	app, err := s.roleApp(c, "acme", OperatorRole)
	require.NoError(t, err)
	shale, err := s.Lookup(c, "acme", Agent)
	require.NoError(t, err)
	require.Equal(t, shale.Id, app, "the role is the app Shale's")

	// Bound nothing, reaching nothing: not a read of the console's.
	may, err := o.May(c, nobody.Tenant, nobody.Id, "/shale.SetService/List")
	require.NoError(t, err)
	require.False(t, may)

	// Another app bound a role of Shale's naming the delegation's methods
	// reaches them, and one not bound does not.
	_, err = s.Grant(c, "acme", "portal-delegation", []string{"/shale.DelegationService/Live", "/shale.DelegationService/Timeline"}, portal.Id)
	require.NoError(t, err)
	held, err := s.Reaches(c, portal.Tenant, portal.Id)
	require.NoError(t, err)
	require.True(t, covers(held, "/shale.DelegationService/Live"))
	require.True(t, covers(held, "/shale.DelegationService/Timeline"))
	held, err = s.Reaches(c, kiosk.Tenant, kiosk.Id)
	require.NoError(t, err)
	require.Empty(t, held)
}

// roleApp is the app a role of a tenant of the embedded roster is of: its
// `app`, the nil identifier for a role of roster's.
func (s *Store) roleApp(ctx context.Context, tenant, role string) (pdid.Id, error) {
	ctx, cancel := unframed(ctx)
	defer cancel()
	r, err := s.em.rs.Ungated.Role().Get(ctx, rstr.RoleGetRequest_builder{
		Ref:    rstr.RoleRef_builder{Slug: rstr.RoleRefBySlug_builder{Alias: z.Ptr(role), Tenant: rstr.TenantRef_builder{Alias: z.Ptr(tenant)}.Build()}.Build()}.Build(),
		Select: rstr.RoleSelect_builder{All: z.Ptr(true)}.Build(),
	}.Build())
	if err != nil {
		return pdid.Nil, err
	}
	if len(r.GetApp().GetId()) == 0 {
		return pdid.Nil, nil
	}

	return pdid.From(r.GetApp().GetId())
}
