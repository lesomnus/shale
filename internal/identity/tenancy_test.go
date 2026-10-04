package identity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"
)

// A deployment key serves the tenants that nominated it (roster's
// `docs/apps.md`): Shale learns them by asking as the key, names every
// other call's tenant with `roster-at`, finds a person in any of them, and
// refuses a tenant that did not nominate it. One that nominates it later
// is found when one of its people arrives, once roster was not asked a
// moment ago; one that ends it is refused by roster at once, and leaves
// the list at the next asking.
func TestADeploymentKeyServesTheTenantsThatNominatedIt(t *testing.T) {
	c := context.Background()
	f := newFakeRoster(t, "hday")
	acme := f.addTenant("acme")
	f.nominate("hday", true)
	alice, erin := f.addHolder("alice"), f.addHolderIn(acme, "erin")

	s := f.openWith(t, Config{Key: f.deployKey})
	now := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return now }

	ts, err := s.Tenants(c)
	require.NoError(t, err)
	require.Equal(t, []string{"hday"}, ts)
	p, err := s.ById(c, pdid.Id(alice).String())
	require.NoError(t, err)
	require.Equal(t, "hday", p.TenantAlias)
	_, err = s.ById(c, pdid.Id(erin).String())
	require.ErrorIs(t, err, ErrNoPerson, "acme did not nominate this key")
	_, _, err = s.Tenant(c, "acme")
	require.ErrorIs(t, err, ErrNoTenant)

	// acme nominates it: asked about again once a moment has passed.
	f.nominate("acme", true)
	lists := f.lists.Load()
	_, _, err = s.Tenant(c, "acme")
	require.ErrorIs(t, err, ErrNoTenant)
	require.Equal(t, lists, f.lists.Load(), "not asked again a moment after")
	now = now.Add(servedRetry + time.Second)
	id, _, err := s.Tenant(c, "acme")
	require.NoError(t, err)
	require.Equal(t, pdid.Id(acme.GetId()), id)
	p, err = s.ById(c, pdid.Id(erin).String())
	require.NoError(t, err)
	require.Equal(t, "acme", p.TenantAlias)
	ts, err = s.Tenants(c)
	require.NoError(t, err)
	require.Equal(t, []string{"acme", "hday"}, ts)

	// acme ends it: roster refuses at once, whatever is known here...
	f.nominate("acme", false)
	_, _, err = s.Tenant(c, "acme")
	require.Error(t, err)
	// ...and acme leaves the list at the next asking.
	now = now.Add(ServedTtl + time.Second)
	ts, err = s.Tenants(c)
	require.NoError(t, err)
	require.Equal(t, []string{"hday"}, ts)

	// What a person is granted is asked in their tenant, through the key.
	f.grant(alice, Everything)
	held, err := s.Reaches(c, pdid.Id(f.tenant.GetId()), pdid.Id(alice))
	require.NoError(t, err)
	require.Equal(t, []string{Everything}, held)
}

// A tenant key is its own tenant: learned from roster once (`MeService`),
// served alone, and asked with no `roster-at`, which roster refuses beside
// an `rt_`.
func TestATenantKeyIsItsOwnTenant(t *testing.T) {
	c := context.Background()
	f := newFakeRoster(t, "hday")
	alice := f.addHolder("alice")
	s := f.openWith(t, Config{Key: f.tenantKey})

	ts, err := s.Tenants(c)
	require.NoError(t, err)
	require.Equal(t, []string{"hday"}, ts)
	p, err := s.ById(c, pdid.Id(alice).String())
	require.NoError(t, err)
	require.Equal(t, "hday", p.TenantAlias)
	_, _, err = s.Tenant(c, "acme")
	require.ErrorIs(t, err, ErrNoTenant)

	calls := f.calls.Load()
	_, err = s.Tenants(c)
	require.NoError(t, err)
	require.Equal(t, calls, f.calls.Load(), "the key's tenant is asked once")
}

// What a key is, is its prefix: one key or keys by tenant, never both; a
// deployment key is `key`, because it serves every tenant that nominated
// it and is answered as nobody without `roster-at`; and a key that is
// neither is refused at start.
func TestTheKeySaysWhatItIs(t *testing.T) {
	open := func(cfg Config) error {
		cfg.Addr, cfg.Insecure = "127.0.0.1:1", true
		s, err := Open(context.Background(), cfg, t.TempDir(), nil)
		if err == nil {
			s.Close()
		}

		return err
	}
	require.NoError(t, open(Config{Key: "rk_one"}))
	require.NoError(t, open(Config{Key: "rt_one"}))
	require.NoError(t, open(Config{Keys: map[string]string{"hday": "rt_one", "acme": "rt_two"}}))

	require.ErrorContains(t, open(Config{Key: "rk_one", Keys: map[string]string{"hday": "rt_one"}}), "not both")
	require.ErrorContains(t, open(Config{Keys: map[string]string{"hday": "rk_one"}}), "auth.roster.key")
	require.ErrorContains(t, open(Config{Key: "xyz"}), "auth.roster.key")
}
