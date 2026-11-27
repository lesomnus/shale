package e2e_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/roster/rstr"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/server/core"
)

// Everybody who signs in holds the basics (§33.1), with the roster inside
// Shale, whose rules are roster's own: a person no role is bound to by
// anybody, in a deployment with operators and reads everybody's, is in
// Shale's group after their first call; what they hold at Shale, and so
// what a token of theirs may be given, is the eighteen; a key for SetService
// List and Live is minted by roster, held to what they hold; it is refused
// a host's read, as they are; and it sees a site said EVERYBODY afterwards,
// as they do.
func TestEverybodyHoldsTheBasics(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: time.Second}
	})
	ctx := context.Background()
	id := c.running.CP.Identity
	conn := plainConn(t, c.running.TenantAddr)
	tenantBase, _ := httpBases(t, c)
	admin := c.dial("@acme/admin")

	carol, err := id.AddPerson(ctx, "acme", "carol", "Carol")
	require.NoError(t, err)
	held, err := id.Reaches(ctx, carol.Tenant, carol.Id)
	require.NoError(t, err)
	require.Empty(t, held, "nobody bound her anything")
	require.NoError(t, id.SetPassword(ctx, "acme", "carol", "carol's password"))
	code, cookie := signIn(t, tenantBase, "acme", "carol", "carol's password")
	require.Equal(t, http.StatusNoContent, code)
	asCarol := metadata.AppendToOutgoingContext(ctx, "cookie", "shale_tenant="+cookie)

	// In the group by her first call, and holding the eighteen.
	listed, err := api.NewHolderServiceClient(conn).ListKeys(asCarol, api.HolderListKeysRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, core.BasicMethods(), listed.GetHolds())
	require.Equal(t, core.KeyScopeMethods(), listed.GetNarrowed(), "what a list of cameras confines of them")
	require.Empty(t, listed.GetNoneHeld())
	held, err = id.Reaches(ctx, carol.Tenant, carol.Id)
	require.NoError(t, err)
	require.ElementsMatch(t, core.BasicMethods(), held, "roster says so")

	// A key, minted by roster against what she holds there.
	methods := []string{api.SetService_List_FullMethodName, api.SetService_Live_FullMethodName}
	made, err := api.NewHolderServiceClient(conn).BeginKey(asCarol, api.HolderBeginKeyRequest_builder{Name: "script", Methods: methods}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, made.GetToken())
	_, err = api.NewHolderServiceClient(conn).BeginKey(asCarol, api.HolderBeginKeyRequest_builder{Name: "hosts", Methods: []string{api.ProducerService_List_FullMethodName}}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a host's read is not hers: %v", err)
	_, err = api.NewProducerServiceClient(conn).List(asCarol, api.ProducerListRequest_builder{}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "nor at the gate: %v", err)
	_, err = api.NewReaderServiceClient(conn).List(asCarol, api.ReaderListRequest_builder{}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)

	// The key sees what she sees: nothing yet, and a site said EVERYBODY
	// once it is said so.
	bearer := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+made.GetToken())
	site := siteOf(t, ctx, admin, "hall", api.SiteVisibility_SITE_VISIBILITY_MEMBERS)
	hall := setIn(t, ctx, admin, "hall", site)
	sets, err := api.NewSetServiceClient(conn).List(bearer, api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Empty(t, sets.GetItems())
	_, err = api.NewSiteServiceClient(admin).Patch(ctx, api.SitePatchRequest_builder{
		Ref: site, Visibility: z.Ptr(api.SiteVisibility_SITE_VISIBILITY_EVERYBODY), DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		sets, err := api.NewSetServiceClient(conn).List(bearer, api.SetListRequest_builder{}.Build())
		return err == nil && len(sets.GetItems()) == 1 && string(sets.GetItems()[0].GetId()) == string(hall.GetId())
	}, sitesSeenWithin, 100*time.Millisecond)
	_, err = api.NewSetServiceClient(conn).Live(bearer, api.SetLiveRequest_builder{Ref: hall}.Build())
	require.NotEqual(t, codes.PermissionDenied, status.Code(err), "Live, which the key names: %v", err)
	require.NotEqual(t, codes.NotFound, status.Code(err), "Live, seen: %v", err)
	_, err = api.NewSetServiceClient(conn).Get(bearer, api.SetGetRequest_builder{Ref: hall}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a method the key does not name: %v", err)
}

// With an outside roster: a person no role is bound to is put in Shale's
// group at their first call, on each road -- the plain header, a key -- and
// holds the eighteen, which a key of theirs is minted for; a second call
// within the remembering asks roster nothing; with reads granted, nothing
// is written at roster and nobody is put anywhere.
func TestEverybodyHoldsTheBasicsWithAnOutsideRoster(t *testing.T) {
	r := newOutsideRoster(t, "acme")
	r.person("admin", identity.Everything)
	alice := r.person("alice")
	c := start(t, func(c *cmd.Config) {
		c.Auth.Roster = identity.Config{Addr: r.addr, Insecure: true, Keys: map[string]string{"acme": r.key}}
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: time.Second}
	})
	ctx := context.Background()
	admin := c.dial("@acme/admin")

	require.Eventually(t, func() bool { return r.app.Group(r.tenant, identity.BasicsGroup) != nil }, 10*time.Second, 50*time.Millisecond,
		"kept at start")
	// Remembered on Shale's own row of the tenant, made at start though
	// nobody of it has signed in yet, once all three are there: what
	// Shale made there once and an operator removes is not made again.
	require.Eventually(t, func() bool {
		row, err := c.running.CP.Ungated.Tenant().Get(ctx, api.TenantGetRequest_builder{
			Ref: api.TenantRef_builder{Id: r.tenant.GetId()}.Build(), Select: api.TenantSelect_builder{All: z.Ptr(true)}.Build(),
		}.Build())
		return err == nil && row.GetLabels()[identity.BasicsLabel] != ""
	}, 10*time.Second, 50*time.Millisecond, "Shale's row of the tenant says it made them")
	role := r.app.Role(r.tenant, identity.BasicsRole)
	require.Equal(t, core.BasicMethods(), role.GetMethods())
	group := r.app.Group(r.tenant, identity.BasicsGroup)
	require.Equal(t, 1, r.app.Bindings(role.GetId(), group.GetId()))

	asAlice := c.dial("@acme/alice")
	listed, err := api.NewHolderServiceClient(asAlice).ListKeys(ctx, api.HolderListKeysRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, core.BasicMethods(), listed.GetHolds())
	require.Contains(t, r.app.Members(group.GetId()), alice)
	adds := r.app.Calls(rstr.GroupMembershipService_Add_FullMethodName)
	_, err = api.NewSetServiceClient(asAlice).List(ctx, api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, adds, r.app.Calls(rstr.GroupMembershipService_Add_FullMethodName), "remembered")

	// Minted as roster mints it, held to what she holds: the basics, and
	// not a host's read.
	_, err = r.mint(alice, api.ProducerService_List_FullMethodName)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	key, err := r.mint(alice, api.SetService_List_FullMethodName, api.SetService_Live_FullMethodName)
	require.NoError(t, err)
	bearer := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
	conn := plainConn(t, c.running.TenantAddr)
	hall := setIn(t, ctx, admin, "hall", siteOf(t, ctx, admin, "hall", api.SiteVisibility_SITE_VISIBILITY_EVERYBODY))
	require.Eventually(t, func() bool {
		sets, err := api.NewSetServiceClient(conn).List(bearer, api.SetListRequest_builder{}.Build())
		return err == nil && len(sets.GetItems()) == 1 && string(sets.GetItems()[0].GetId()) == string(hall.GetId())
	}, sitesSeenWithin, 100*time.Millisecond)

	// Somebody whose first arrival is a key of theirs is put in by it.
	bob := r.person("bob")
	byBob := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+r.apiKey(bob, api.SetService_List_FullMethodName))
	_, err = api.NewSetServiceClient(conn).List(byBob, api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Contains(t, r.app.Members(group.GetId()), bob)
}

// With reads granted Shale writes none of it at roster and puts nobody
// anywhere.
func TestNobodyIsPutAnywhereWithReadsGranted(t *testing.T) {
	r := newOutsideRoster(t, "acme")
	r.person("admin", identity.Everything)
	r.person("alice")
	c := start(t, func(c *cmd.Config) {
		c.Auth.Roster = identity.Config{Addr: r.addr, Insecure: true, Keys: map[string]string{"acme": r.key}}
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: time.Second, Reads: identity.ReadsGranted}
	})
	ctx := context.Background()
	require.Nil(t, c.running.CP.Basics)
	listed, err := api.NewHolderServiceClient(c.dial("@acme/alice")).ListKeys(ctx, api.HolderListKeysRequest_builder{}.Build())
	require.NoError(t, err)
	require.Empty(t, listed.GetHolds())
	_, err = api.NewSetServiceClient(c.dial("@acme/admin")).List(ctx, api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	for _, m := range []string{
		rstr.RoleService_Add_FullMethodName, rstr.GroupService_Add_FullMethodName,
		rstr.BindingService_Add_FullMethodName, rstr.GroupMembershipService_Add_FullMethodName,
		rstr.RoleService_Widen_FullMethodName, rstr.RoleService_Narrow_FullMethodName,
	} {
		require.Zero(t, r.app.Calls(m), m)
	}
	require.Nil(t, r.app.Group(r.tenant, identity.BasicsGroup))
}

// Without operators nothing of it runs: every person administers their
// tenant, and a role of Shale's decides nothing.
func TestNoBasicsWithoutOperators(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	require.Nil(t, c.running.CP.Basics)
	bob, err := c.running.CP.Identity.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	_, err = api.NewSetServiceClient(c.dial("@acme/bob")).List(ctx, api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	held, err := c.running.CP.Identity.Reaches(ctx, bob.Tenant, bob.Id)
	require.NoError(t, err)
	require.Empty(t, held)
}
