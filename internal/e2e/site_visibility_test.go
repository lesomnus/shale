package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
)

// sitesSeenWithin is how long a written site or membership takes to be
// seen: Sites keeps its answer 10 s per caller.
const sitesSeenWithin = 15 * time.Second

func siteOf(t *testing.T, ctx context.Context, admin *grpc.ClientConn, alias string, v api.SiteVisibility) *api.SiteRef {
	t.Helper()
	s, err := api.NewSiteServiceClient(admin).Add(ctx, api.SiteAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: alias, Visibility: z.Ptr(v),
	}.Build())
	require.NoError(t, err)

	return api.SiteRef_builder{Id: s.GetId()}.Build()
}

func setIn(t *testing.T, ctx context.Context, admin *grpc.ClientConn, alias string, site *api.SiteRef) *api.SetRef {
	t.Helper()
	s, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: alias, Site: site,
	}.Build())
	require.NoError(t, err)

	return api.SetRef_builder{Id: s.GetId()}.Build()
}

func setSeen(ctx context.Context, conn *grpc.ClientConn, ref *api.SetRef) error {
	_, err := api.NewSetServiceClient(conn).Get(ctx, api.SetGetRequest_builder{Ref: ref}.Build())
	return err
}

// Where roster says what people may change, a person roster does not
// grant all of Shale sees a site by its kind and by its members (§33.1):
// a MEMBERS site they are not in, a COMPANY site (nobody is of the company
// yet) and a set in no site are outside the wall for every read -- a set,
// a source, a lamina, Live, the timeline, an export; an EVERYBODY site is
// seen; a membership adds a site; a kind patched and a membership added
// are seen within the time an answer is kept, and by a key with no list
// made before; whoever is granted all of Shale sees all of it; and a kind
// there is none of is refused.
func TestSiteVisibilityWithOperators(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Export.Enabled = true
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: time.Second}
	})
	ctx := context.Background()
	admin := c.dial("@acme/admin")
	acme := api.TenantRef_builder{Alias: z.Ptr("acme")}.Build()

	// A recording in a site of nothing said, which is MEMBERS.
	src, ex := exportRecording(t, c, admin)
	members := api.SiteRef_builder{Id: src.GetSite().GetId()}.Build()
	site, err := api.NewSiteServiceClient(admin).Get(ctx, api.SiteGetRequest_builder{Ref: members}.Build())
	require.NoError(t, err)
	require.Equal(t, api.SiteVisibility_SITE_VISIBILITY_UNSPECIFIED, site.GetVisibility())
	laminae, err := api.NewLaminaServiceClient(admin).List(ctx, api.LaminaListRequest_builder{}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, laminae.GetItems())
	lamina := laminae.GetItems()[0]
	recorded := api.SetRef_builder{Id: src.GetSet().GetId()}.Build()

	hall := setIn(t, ctx, admin, "hall", siteOf(t, ctx, admin, "hall", api.SiteVisibility_SITE_VISIBILITY_EVERYBODY))
	company := siteOf(t, ctx, admin, "office", api.SiteVisibility_SITE_VISIBILITY_COMPANY)
	office := setIn(t, ctx, admin, "office", company)
	loose := setIn(t, ctx, admin, "loose", nil)

	bob, err := c.running.CP.Identity.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	asBob := c.dial("@acme/bob")

	// The MEMBERS site he is not in is outside the wall, every way in.
	require.Equal(t, codes.NotFound, status.Code(setSeen(ctx, asBob, recorded)), "a set")
	_, err = api.NewSourceServiceClient(asBob).Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: src.GetId()}.Build()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "a source")
	_, err = api.NewLaminaServiceClient(asBob).Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: lamina.GetId()}.Build()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "a lamina")
	theirs, err := api.NewLaminaServiceClient(asBob).List(ctx, api.LaminaListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Empty(t, theirs.GetItems(), "the laminae")
	_, err = api.NewSetServiceClient(asBob).Live(ctx, api.SetLiveRequest_builder{Ref: recorded}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "Live")
	_, err = api.NewSourceServiceClient(asBob).Live(ctx, api.SourceLiveRequest_builder{Ref: api.SourceRef_builder{Id: src.GetId()}.Build()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "a source's Live")
	_, err = api.NewLaminaServiceClient(asBob).Timeline(ctx, api.LaminaTimelineRequest_builder{Source: ex.GetSource(), From: ex.GetFrom(), To: ex.GetTo(), Size: 10}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "the timeline")
	_, err = api.NewLaminaServiceClient(asBob).Export(ctx, ex)
	require.Equal(t, codes.NotFound, status.Code(err), "an export")

	// The EVERYBODY site is his; the COMPANY site is not, since nobody is
	// of the company yet; a set in no site is nobody's but whoever sees
	// every site.
	require.NoError(t, setSeen(ctx, asBob, hall))
	require.Equal(t, codes.NotFound, status.Code(setSeen(ctx, asBob, office)), "COMPANY, and nobody is of the company")
	require.Equal(t, codes.NotFound, status.Code(setSeen(ctx, asBob, loose)), "in no site")
	sites, err := api.NewSiteServiceClient(asBob).List(ctx, api.SiteListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, sites.GetItems(), 1)
	require.Equal(t, "hall", sites.GetItems()[0].GetAlias())

	// Whoever is granted all of Shale sees all of it, in no site too.
	for _, ref := range []*api.SetRef{recorded, hall, office, loose} {
		require.NoError(t, setSeen(ctx, admin, ref))
	}

	// A key of Bob's that carries no list, made now, sees what he sees.
	methods := []string{api.SetService_Get_FullMethodName, api.SetService_List_FullMethodName}
	_, err = c.running.CP.Identity.Grant(ctx, "acme", "sets", methods, bob.Id)
	require.NoError(t, err)
	key, _, err := c.running.CP.Identity.IssueKey(ctx, "acme", "bob", "sets", identity.Agent, methods, time.Time{})
	require.NoError(t, err)
	byKey := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
	conn := plainConn(t, c.running.TenantAddr)
	require.NoError(t, setSeen(byKey, conn, hall))
	require.Equal(t, codes.NotFound, status.Code(setSeen(byKey, conn, office)))

	// A site made EVERYBODY after the key was made, and a kind patched,
	// are seen by it and by him within the time an answer is kept; a
	// membership adds the site it names, whatever its kind.
	later := setIn(t, ctx, admin, "later", siteOf(t, ctx, admin, "later", api.SiteVisibility_SITE_VISIBILITY_EVERYBODY))
	_, err = api.NewSiteServiceClient(admin).Patch(ctx, api.SitePatchRequest_builder{
		Ref: company, Visibility: z.Ptr(api.SiteVisibility_SITE_VISIBILITY_EVERYBODY), DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.NoError(t, err)
	_, err = api.NewSiteMemberServiceClient(admin).Add(ctx, api.SiteMemberAddRequest_builder{
		Tenant: acme, Site: members, Holder: api.HolderRef_builder{Id: bob.Id.Bytes()}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return setSeen(byKey, conn, later) == nil && setSeen(byKey, conn, office) == nil
	}, sitesSeenWithin, 200*time.Millisecond, "the key sees the site made after it and the site patched")
	require.NoError(t, setSeen(ctx, asBob, office), "and so does he: one answer for his session and his keys")
	require.NoError(t, setSeen(ctx, asBob, recorded), "a member")
	_, err = api.NewLaminaServiceClient(asBob).Export(ctx, ex)
	require.NoError(t, err, "a member exports")
	require.Equal(t, codes.NotFound, status.Code(setSeen(ctx, asBob, loose)), "in no site, still")

	// A kind there is none of is refused, added or patched.
	_, err = api.NewSiteServiceClient(admin).Add(ctx, api.SiteAddRequest_builder{Tenant: acme, Alias: "odd", Visibility: z.Ptr(api.SiteVisibility(7))}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = api.NewSiteServiceClient(admin).Patch(ctx, api.SitePatchRequest_builder{Ref: company, Visibility: z.Ptr(api.SiteVisibility(7)), DateUpdatedForce: z.Ptr(true)}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// Without roster saying what people may change, a row's all_sites is
// every site, as before, and the tenant's first person has it; anybody
// else sees the sites everybody sees and their own (§33.1).
func TestSiteVisibilityWithoutOperators(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	admin := c.dial("@acme/admin")

	hall := setIn(t, ctx, admin, "hall", siteOf(t, ctx, admin, "hall", api.SiteVisibility_SITE_VISIBILITY_EVERYBODY))
	vault := setIn(t, ctx, admin, "vault", siteOf(t, ctx, admin, "vault", api.SiteVisibility_SITE_VISIBILITY_MEMBERS))
	loose := setIn(t, ctx, admin, "loose", nil)
	for _, ref := range []*api.SetRef{hall, vault, loose} {
		require.NoError(t, setSeen(ctx, admin, ref), "the tenant's first person sees every site")
	}

	_, err := c.running.CP.Identity.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	asBob := c.dial("@acme/bob")
	require.NoError(t, setSeen(ctx, asBob, hall))
	require.Equal(t, codes.NotFound, status.Code(setSeen(ctx, asBob, vault)))
	require.Equal(t, codes.NotFound, status.Code(setSeen(ctx, asBob, loose)))
}
