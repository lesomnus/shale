package e2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/internal/sso/ssotest"
	"github.com/lesomnus/shale/internal/token"
	"github.com/lesomnus/shale/server/core"
)

const (
	shaleApi     = "urn:test:api:shale"
	portalClient = "portal-client"
)

// An app viewing on a person's behalf (§33.8), end to end: a portal proves
// a person with their access token for Shale's API and itself with an
// exchange token roster issued to Shale, and gets a handle; the handle
// answers live view as the person, its tokens naming the portal beside
// them and living minutes; it is narrowed to what roster grants the
// portal's holder, to what the person may view, and to what the handle
// names; neither proof is anything alone; a person whose reads are granted
// views through the portal and reads nothing here directly; and an ended
// delegation answers nothing.
func TestDelegatedViewing(t *testing.T) {
	idp := ssotest.New(t, "shale", "s3cret", "shale-cli")
	c := start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: time.Second, Reads: identity.ReadsGranted}
		c.Auth.Oidc.Issuer = idp.URL
		c.Auth.Oidc.ClientId = "shale"
		c.Auth.Oidc.ClientSecret = "s3cret"
		c.Auth.Delegation = cmd.DelegationConfig{
			Audience: shaleApi,
			Clients:  map[string]string{portalClient: "portal", "kiosk-client": "kiosk"},
			TokenTtl: 2 * time.Minute,
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	id := c.running.CP.Identity
	admin := c.dial("@acme/admin")
	// In a site everybody sees, so that what Bob sees is the delegation's
	// to narrow and roster's grants to refuse.
	set, _ := liveSourceAt(t, ctx, c, admin, "av.mp4", everybody(t, ctx, admin, "lobby"))

	bob, err := id.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	portal, err := id.AddPerson(ctx, "acme", "portal", "Portal")
	require.NoError(t, err)
	kiosk, err := id.AddPerson(ctx, "acme", "kiosk", "Kiosk")
	require.NoError(t, err)
	// Each app may ask roster for an exchange token, as `roster app
	// install` lets one.
	for _, app := range []pdid.Id{portal.Id, kiosk.Id} {
		_, err = id.Grant(ctx, "acme", "app", []string{"/roster.DelegationService/Exchange"}, app)
		require.NoError(t, err)
	}

	// The person's token, as the issuer mints one for the portal's client.
	personToken := func(sub, client, aud string) string {
		now := time.Now()
		return idp.Sign(t, map[string]any{
			"iss": idp.URL, "sub": sub, "aud": []string{aud}, "client_id": client,
			"iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(),
		})
	}
	exchange := func(app string) string {
		tok, err := id.Exchange(ctx, "acme", app, []string{"/shale.DelegationService/*"})
		require.NoError(t, err)
		return tok
	}
	conn, err := grpc.NewClient(c.running.TenantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	ds := api.NewDelegationServiceClient(conn)
	as := func(rd string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+rd)
	}
	start := func(rd, tok string, live, recordings bool, sets ...*api.SetRef) (*api.DelegationStartResponse, error) {
		return ds.Start(as(rd), api.DelegationStartRequest_builder{AccessToken: tok, Live: live, Recordings: recordings, Sets: sets}.Build())
	}
	byId := api.SetRef_builder{Id: set.GetId()}.Build()
	await := func(want codes.Code, call func() error) {
		t.Helper()
		require.Eventually(t, func() bool { return status.Code(call()) == want }, 10*time.Second, 200*time.Millisecond)
	}

	// Nothing granted the portal: no delegation, however well proved.
	_, err = start(exchange("portal"), personToken(bob.Id.String(), portalClient, shaleApi), true, false)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), "grants the app nothing")

	// Granted live at roster, the portal starts one -- once Bob may view
	// through it, since reads are granted here and Bob is granted nothing.
	portalLive, err := id.Grant(ctx, "acme", "portal-live", []string{"/shale.DelegationService/Live"}, portal.Id)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := start(exchange("portal"), personToken(bob.Id.String(), portalClient, shaleApi), true, false)
		return status.Code(err) == codes.PermissionDenied && strings.Contains(status.Convert(err).Message(), "grants you nothing")
	}, 10*time.Second, 200*time.Millisecond, "the portal may, and Bob may not")
	_, err = id.Grant(ctx, "acme", "view-through-apps", []string{"/shale.DelegationService/Live"}, bob.Id)
	require.NoError(t, err)
	var started *api.DelegationStartResponse
	await(codes.OK, func() error {
		started, err = start(exchange("portal"), personToken(bob.Id.String(), portalClient, shaleApi), true, false)
		return err
	})
	require.NotEmpty(t, started.GetHandle())
	require.Empty(t, started.GetDelegation().GetSecretHash(), "never answered")
	require.Equal(t, portal.Id.Bytes(), started.GetDelegation().GetApp())

	// Bob himself reads nothing here, and cannot call the app's service.
	asBob := c.dial("@acme/bob")
	_, err = api.NewSetServiceClient(asBob).Live(ctx, api.SetLiveRequest_builder{Ref: byId}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = api.NewDelegationServiceClient(asBob).Live(ctx, api.DelegationLiveRequest_builder{Handle: started.GetHandle(), Set: byId}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a credential the interceptor knows is not an app's")

	// Live through the handle: Bob's tokens, naming the portal, for minutes.
	rd := exchange("portal")
	live, err := ds.Live(as(rd), api.DelegationLiveRequest_builder{Handle: started.GetHandle(), Set: byId}.Build())
	require.NoError(t, err)
	require.Len(t, live.GetSources(), 1)
	claims, err := token.Parse(live.GetSources()[0].GetViewToken())
	require.NoError(t, err)
	require.Equal(t, bob.Id.Bytes(), claims.GetActor())
	require.Equal(t, portal.Id.Bytes(), claims.GetDelegator())
	require.WithinDuration(t, time.Now().Add(2*time.Minute), claims.GetExp().AsTime(), 10*time.Second, "token_ttl, not view_token_ttl")
	v := watch(t, live.GetSources()[0])
	v.video(t, 20, 20*time.Second)
	v.leave(t)

	// What the handle is not for.
	_, err = ds.Timeline(as(rd), api.DelegationTimelineRequest_builder{Handle: started.GetHandle(), Timeline: api.LaminaTimelineRequest_builder{
		Set: byId, From: timestamppb.New(time.Now().Add(-time.Hour)), To: timestamppb.Now(),
	}.Build()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "live only")
	_, err = start(rd, personToken(bob.Id.String(), portalClient, shaleApi), false, true)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "the portal is not granted recordings")
	require.Contains(t, status.Convert(err).Message(), "grants the app nothing")

	// Neither proof alone, nor two that do not belong together.
	_, err = ds.Live(ctx, api.DelegationLiveRequest_builder{Handle: started.GetHandle(), Set: byId}.Build())
	require.Equal(t, codes.Unauthenticated, status.Code(err), "a handle without the app's token")
	_, err = ds.Live(as(rd), api.DelegationLiveRequest_builder{Handle: started.GetHandle() + "x", Set: byId}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "the app's token without the handle")
	_, err = ds.Live(as(exchange("kiosk")), api.DelegationLiveRequest_builder{Handle: started.GetHandle(), Set: byId}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "another app's token")
	_, err = start(exchange("kiosk"), personToken(bob.Id.String(), portalClient, shaleApi), true, false)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "the person's token was the portal's client's")
	_, err = start(rd, personToken(bob.Id.String(), "somebody-else", shaleApi), true, false)
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a client that is no app here")
	_, err = start(rd, personToken(bob.Id.String(), portalClient, "urn:test:api:other"), true, false)
	require.Equal(t, codes.Unauthenticated, status.Code(err), "a token for another API")
	_, err = start(rd, personToken(pdid.New(pdid.Domain(2)).String(), portalClient, shaleApi), true, false)
	require.Equal(t, codes.Unauthenticated, status.Code(err), "nobody roster knows")
	_, err = start(rd, "not a token", true, false)
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	// Narrowed to what it names: a delegation for another set answers none
	// of this one's cameras.
	other, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(), Alias: "yard", Site: api.SiteRef_builder{Id: set.GetSite().GetId()}.Build()}.Build())
	require.NoError(t, err)
	narrow, err := start(rd, personToken(bob.Id.String(), portalClient, shaleApi), true, false, api.SetRef_builder{Id: other.GetId()}.Build())
	require.NoError(t, err)
	_, err = ds.Live(as(rd), api.DelegationLiveRequest_builder{Handle: narrow.GetHandle(), Set: byId}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// The generated verbs are nobody's.
	_, err = api.NewDelegationServiceClient(admin).Get(ctx, api.DelegationGetRequest_builder{Ref: api.DelegationRef_builder{Id: started.GetDelegation().GetId()}.Build()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// Roster's word that Bob was signed out everywhere ends what was
	// started before it, and nothing started after (what the sync stream
	// does, at a roster that has one).
	before, err := start(rd, personToken(bob.Id.String(), portalClient, shaleApi), true, false)
	require.NoError(t, err)
	n, err := core.Core{}.WithDeps(c.running.CP.Deps).Unstanding(ctx, bob.Id, time.Now(), "signed out everywhere")
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1)
	_, err = ds.Live(as(rd), api.DelegationLiveRequest_builder{Handle: before.GetHandle(), Set: byId}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// Somebody suspended at roster is viewed for no more: every use asks.
	carol, err := id.AddPerson(ctx, "acme", "carol", "Carol")
	require.NoError(t, err)
	_, err = id.Grant(ctx, "acme", "view-through-apps", nil, carol.Id)
	require.NoError(t, err)
	var forCarol *api.DelegationStartResponse
	require.Eventually(t, func() bool {
		forCarol, err = start(rd, personToken(carol.Id.String(), portalClient, shaleApi), true, false)
		return err == nil
	}, 10*time.Second, 200*time.Millisecond)
	_, err = ds.Live(as(rd), api.DelegationLiveRequest_builder{Handle: forCarol.GetHandle(), Set: byId}.Build())
	require.NoError(t, err)
	require.NoError(t, id.Suspend(ctx, carol.Id))
	_, err = ds.Live(as(rd), api.DelegationLiveRequest_builder{Handle: forCarol.GetHandle(), Set: byId}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "good standing")

	// Ended by the app, it answers nothing more.
	again, err := start(rd, personToken(bob.Id.String(), portalClient, shaleApi), true, false)
	require.NoError(t, err)
	_, err = ds.Revoke(as(rd), api.DelegationRevokeRequest_builder{Handle: started.GetHandle()}.Build())
	require.NoError(t, err)
	_, err = ds.Live(as(rd), api.DelegationLiveRequest_builder{Handle: started.GetHandle(), Set: byId}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// And the portal's grant taken away at roster stops the next renewal of
	// any other, once the answer about it lapses.
	_, err = ds.Live(as(rd), api.DelegationLiveRequest_builder{Handle: again.GetHandle(), Set: byId}.Build())
	require.NoError(t, err)
	require.NoError(t, id.Ungrant(ctx, portalLive))
	await(codes.PermissionDenied, func() error {
		_, err := ds.Live(as(rd), api.DelegationLiveRequest_builder{Handle: again.GetHandle(), Set: byId}.Build())
		return err
	})
}

// `auth.delegation.persons: tenant` (§33.8): on a deployment whose reads
// are granted, a person granted nothing is viewed for through an app all
// the same -- the app decides who sees what -- and still reads nothing
// here themselves; the app is still held to what roster grants it, and
// the person to their standing. By default the same person is refused.
func TestDelegatedViewingEveryPerson(t *testing.T) {
	for _, persons := range []string{"", cmd.DelegationPersonsTenant} {
		t.Run("persons="+persons, func(t *testing.T) {
			every := persons == cmd.DelegationPersonsTenant
			idp := ssotest.New(t, "shale", "s3cret", "shale-cli")
			c := start(t, func(c *cmd.Config) {
				c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: time.Second, Reads: identity.ReadsGranted}
				c.Auth.Oidc.Issuer = idp.URL
				c.Auth.Oidc.ClientId = "shale"
				c.Auth.Oidc.ClientSecret = "s3cret"
				c.Auth.Delegation = cmd.DelegationConfig{
					Audience: shaleApi,
					Clients:  map[string]string{portalClient: "portal"},
					Persons:  persons,
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			id := c.running.CP.Identity
			admin := c.dial("@acme/admin")
			set, _ := liveSourceAt(t, ctx, c, admin, "av.mp4", everybody(t, ctx, admin, "lobby"))
			byId := api.SetRef_builder{Id: set.GetId()}.Build()

			bob, err := id.AddPerson(ctx, "acme", "bob", "Bob")
			require.NoError(t, err)
			carol, err := id.AddPerson(ctx, "acme", "carol", "Carol")
			require.NoError(t, err)
			portal, err := id.AddPerson(ctx, "acme", "portal", "Portal")
			require.NoError(t, err)
			_, err = id.Grant(ctx, "acme", "app", []string{"/roster.DelegationService/Exchange"}, portal.Id)
			require.NoError(t, err)
			// Live, and not recordings, is what the portal may do.
			_, err = id.Grant(ctx, "acme", "portal-live", []string{"/shale.DelegationService/Live"}, portal.Id)
			require.NoError(t, err)

			personToken := func(sub string) string {
				now := time.Now()
				return idp.Sign(t, map[string]any{
					"iss": idp.URL, "sub": sub, "aud": []string{shaleApi}, "client_id": portalClient,
					"iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(),
				})
			}
			exchange := func() string {
				tok, err := id.Exchange(ctx, "acme", "portal", []string{"/shale.DelegationService/*"})
				require.NoError(t, err)
				return tok
			}
			conn, err := grpc.NewClient(c.running.TenantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer conn.Close()
			ds := api.NewDelegationServiceClient(conn)
			as := func(rd string) context.Context {
				return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+rd)
			}
			startFor := func(who pdid.Id, live, recordings bool) (*api.DelegationStartResponse, error) {
				return ds.Start(as(exchange()), api.DelegationStartRequest_builder{AccessToken: personToken(who.String()), Live: live, Recordings: recordings}.Build())
			}
			liveFor := func(handle string) (*api.SetLiveResponse, error) {
				return ds.Live(as(exchange()), api.DelegationLiveRequest_builder{Handle: handle, Set: byId}.Build())
			}

			// Bob, granted nothing, is viewed for only where every person may be.
			var started *api.DelegationStartResponse
			require.Eventually(t, func() bool {
				started, err = startFor(bob.Id, true, false)
				if every {
					return err == nil
				}
				return status.Code(err) == codes.PermissionDenied && strings.Contains(status.Convert(err).Message(), "grants you nothing")
			}, 10*time.Second, 200*time.Millisecond, "%v", err)

			// Bob himself reads nothing here either way: the console's reads
			// are still granted.
			_, err = api.NewSetServiceClient(c.dial("@acme/bob")).Live(ctx, api.SetLiveRequest_builder{Ref: byId}.Build())
			require.Equal(t, codes.PermissionDenied, status.Code(err))

			if !every {
				return
			}

			// Live through the handle, as Bob and naming the portal.
			live, err := liveFor(started.GetHandle())
			require.NoError(t, err)
			require.Len(t, live.GetSources(), 1)
			claims, err := token.Parse(live.GetSources()[0].GetViewToken())
			require.NoError(t, err)
			require.Equal(t, bob.Id.Bytes(), claims.GetActor())
			require.Equal(t, portal.Id.Bytes(), claims.GetDelegator())

			// The portal's ceiling still holds: it is not granted recordings.
			_, err = startFor(bob.Id, false, true)
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			require.Contains(t, status.Convert(err).Message(), "grants the app nothing")

			// And the person's standing: somebody suspended is viewed for no more.
			forCarol, err := startFor(carol.Id, true, false)
			require.NoError(t, err)
			_, err = liveFor(forCarol.GetHandle())
			require.NoError(t, err)
			require.NoError(t, id.Suspend(ctx, carol.Id))
			_, err = liveFor(forCarol.GetHandle())
			require.Equal(t, codes.PermissionDenied, status.Code(err))
			require.Contains(t, status.Convert(err).Message(), "good standing")
			_, err = startFor(carol.Id, true, false)
			require.Error(t, err, "nor started for")

			// Revoked, it answers nothing.
			_, err = ds.Revoke(as(exchange()), api.DelegationRevokeRequest_builder{Handle: started.GetHandle()}.Build())
			require.NoError(t, err)
			_, err = liveFor(started.GetHandle())
			require.Equal(t, codes.PermissionDenied, status.Code(err))
		})
	}
}

// An app asks which sets a person may see (§33.8): DelegationService/
// Catalog answers what the person's own SetService/List and
// SourceService/List answer them, page by page and in the same order --
// their sites and nothing else, a set in no site to nobody but whoever
// sees every site, a site made EVERYBODY within the time an answer is
// kept -- narrowed to the handle, and carrying nothing of a host. The
// single-set form answers a set seen, and nothing alike for a set not
// seen and one there is not. It is held to what Live is held to: the
// app's grant of the method by name, its own handle, alive, and the
// person's standing.
func TestDelegatedCatalog(t *testing.T) {
	idp := ssotest.New(t, "shale", "s3cret", "shale-cli")
	c := start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: time.Second}
		c.Auth.Oidc.Issuer = idp.URL
		c.Auth.Oidc.ClientId = "shale"
		c.Auth.Oidc.ClientSecret = "s3cret"
		c.Auth.Delegation = cmd.DelegationConfig{
			Audience: shaleApi,
			Clients:  map[string]string{portalClient: "portal", "kiosk-client": "kiosk"},
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	id := c.running.CP.Identity
	admin := c.dial("@acme/admin")
	acme := api.TenantRef_builder{Alias: z.Ptr("acme")}.Build()

	bob, err := id.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	carol, err := id.AddPerson(ctx, "acme", "carol", "Carol")
	require.NoError(t, err)
	// Olive sees every site: roster grants her all of Shale.
	olive, err := id.AddPerson(ctx, "acme", "olive", "Olive")
	require.NoError(t, err)
	_, err = id.Grant(ctx, "acme", "all-of-shale", []string{identity.Everything}, olive.Id)
	require.NoError(t, err)
	portal, err := id.AddPerson(ctx, "acme", "portal", "Portal")
	require.NoError(t, err)
	kiosk, err := id.AddPerson(ctx, "acme", "kiosk", "Kiosk")
	require.NoError(t, err)
	for _, app := range []pdid.Id{portal.Id, kiosk.Id} {
		_, err = id.Grant(ctx, "acme", "app", []string{"/roster.DelegationService/Exchange"}, app)
		require.NoError(t, err)
	}
	// Live, and not the catalogue, to begin with.
	_, err = id.Grant(ctx, "acme", "portal-live", []string{"/shale.DelegationService/Live"}, portal.Id)
	require.NoError(t, err)
	_, err = id.Grant(ctx, "acme", "kiosk-all", []string{"/shale.DelegationService/*"}, kiosk.Id)
	require.NoError(t, err)

	// Two sites: Bob and Carol are members of the yard and not of the vault.
	yard := siteOf(t, ctx, admin, "yard", api.SiteVisibility_SITE_VISIBILITY_MEMBERS)
	vault := siteOf(t, ctx, admin, "vault", api.SiteVisibility_SITE_VISIBILITY_MEMBERS)
	for _, who := range []identity.Person{bob, carol} {
		// Their row here first, as a first call makes it.
		_, err = api.NewSiteServiceClient(c.dial("@acme/"+who.Alias)).List(ctx, api.SiteListRequest_builder{}.Build())
		require.NoError(t, err)
		_, err = api.NewSiteMemberServiceClient(admin).Add(ctx, api.SiteMemberAddRequest_builder{
			Tenant: acme, Site: yard, Holder: api.HolderRef_builder{Id: who.Id.Bytes()}.Build(),
		}.Build())
		require.NoError(t, err)
	}
	made, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: acme, Alias: "gate", Name: "The gate", Labels: map[string]string{"zone": "north"}, Site: yard,
	}.Build())
	require.NoError(t, err)
	gate := api.SetRef_builder{Id: made.GetId()}.Build()
	safe := setIn(t, ctx, admin, "safe", vault)
	loose := setIn(t, ctx, admin, "loose", nil)
	sources := api.NewSourceServiceClient(admin)
	addSource := func(set *api.SetRef, alias, contentType string) {
		v, err := sources.Add(ctx, api.SourceAddRequest_builder{
			Tenant: acme, Set: set, Alias: alias, Name: strings.ToUpper(alias),
			Labels: map[string]string{"floor": "1"},
		}.Build())
		require.NoError(t, err)
		// What its bytes are, as a person says it.
		_, err = sources.Patch(ctx, api.SourcePatchRequest_builder{
			Ref: api.SourceRef_builder{Id: v.GetId()}.Build(), ContentType: z.Ptr(contentType), DateUpdatedForce: z.Ptr(true),
		}.Build())
		require.NoError(t, err)
	}
	addSource(gate, "east", "video/mp4")
	addSource(gate, "west", "application/x-mcap")
	addSource(safe, "door", "video/mp4")
	addSource(loose, "roof", "video/mp4")
	// More of the yard than a page holds.
	for i := range 21 {
		setIn(t, ctx, admin, "yard-"+string(rune('a'+i)), yard)
	}

	personToken := func(sub, client string) string {
		now := time.Now()
		return idp.Sign(t, map[string]any{
			"iss": idp.URL, "sub": sub, "aud": []string{shaleApi}, "client_id": client,
			"iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(),
		})
	}
	exchange := func(app string) string {
		tok, err := id.Exchange(ctx, "acme", app, []string{"/shale.DelegationService/*"})
		require.NoError(t, err)
		return tok
	}
	conn, err := grpc.NewClient(c.running.TenantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	ds := api.NewDelegationServiceClient(conn)
	as := func(rd string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+rd)
	}
	startFor := func(app, client string, who pdid.Id, sets ...*api.SetRef) string {
		t.Helper()
		var v *api.DelegationStartResponse
		require.Eventually(t, func() bool {
			v, err = ds.Start(as(exchange(app)), api.DelegationStartRequest_builder{AccessToken: personToken(who.String(), client), Live: true, Sets: sets}.Build())
			return err == nil
		}, 10*time.Second, 200*time.Millisecond, "%v", err)
		return v.GetHandle()
	}
	catalog := func(app, handle string, set *api.SetRef, size int32, after string) (*api.DelegationCatalogResponse, error) {
		return ds.Catalog(as(exchange(app)), api.DelegationCatalogRequest_builder{Handle: handle, Set: set, Size: size, After: after}.Build())
	}
	// every reads the whole catalogue, a page of size at a time.
	every := func(app, handle string, size int32) ([]*api.DelegationCatalogSet, int) {
		t.Helper()
		var out []*api.DelegationCatalogSet
		pages, after := 0, ""
		for {
			v, err := catalog(app, handle, nil, size, after)
			require.NoError(t, err)
			pages++
			out = append(out, v.GetItems()...)
			if v.GetNext() == "" {
				return out, pages
			}
			after = v.GetNext()
		}
	}
	aliases := func(vs []*api.DelegationCatalogSet) []string {
		var out []string
		for _, v := range vs {
			out = append(out, v.GetAlias())
		}
		return out
	}

	bobs := startFor("portal", portalClient, bob.Id)

	// The portal is not granted the catalogue: refused, by the method's name.
	_, err = catalog("portal", bobs, nil, 0, "")
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), "/shale.DelegationService/Catalog")
	require.Contains(t, status.Convert(err).Message(), "grants the app nothing")
	_, err = id.Grant(ctx, "acme", "portal-catalog", []string{"/shale.DelegationService/Catalog"}, portal.Id)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, err := catalog("portal", bobs, nil, 0, "")
		return err == nil
	}, 10*time.Second, 200*time.Millisecond)

	// Bob sees the yard's sets, oldest first, and neither the vault's nor
	// the one in no site: what his own SetService/List answers him, page
	// for page, by the same cursor.
	asBob := c.dial("@acme/bob")
	// His membership, once the answer kept from his first call lapses.
	require.Eventually(t, func() bool { return setSeen(ctx, asBob, gate) == nil }, sitesSeenWithin, 200*time.Millisecond)
	first, err := catalog("portal", bobs, nil, 0, "")
	require.NoError(t, err)
	require.Len(t, first.GetItems(), 20, "SetService/List's page")
	require.NotEmpty(t, first.GetNext())
	own, err := api.NewSetServiceClient(asBob).List(ctx, api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Equal(t, len(own.GetItems()), len(first.GetItems()))
	for i, v := range own.GetItems() {
		require.Equal(t, v.GetId(), first.GetItems()[i].GetId(), "in SetService/List's order")
	}
	require.Equal(t, own.GetNext(), first.GetNext(), "the same cursor")
	all, pages := every("portal", bobs, 5)
	require.Equal(t, 5, pages, "22 sets, 5 a page")
	want := []string{"gate"}
	for i := range 21 {
		want = append(want, "yard-"+string(rune('a'+i)))
	}
	require.Equal(t, want, aliases(all))
	ownAll, err := api.NewSetServiceClient(asBob).List(ctx, api.SetListRequest_builder{Size: 100}.Build())
	require.NoError(t, err)
	require.Len(t, ownAll.GetItems(), len(all))
	for i, v := range ownAll.GetItems() {
		require.Equal(t, v.GetId(), all[i].GetId())
	}

	// A set as a catalogue shows it: its site, with the site's kind, and
	// its cameras in the set's order, as his own SourceService/List has them.
	g := all[0]
	require.Equal(t, "gate", g.GetAlias())
	require.Equal(t, "The gate", g.GetName())
	require.Equal(t, map[string]string{"zone": "north"}, g.GetLabels())
	require.Equal(t, gate.GetId(), g.GetId())
	require.Equal(t, yard.GetId(), g.GetSite().GetId())
	require.Equal(t, "yard", g.GetSite().GetAlias())
	require.Equal(t, api.SiteVisibility_SITE_VISIBILITY_MEMBERS, g.GetSite().GetVisibility())
	require.Len(t, g.GetSources(), 2)
	ownSources, err := api.NewSourceServiceClient(asBob).List(ctx, api.SourceListRequest_builder{
		Filters: []*api.SourceFilter{api.SourceFilter_builder{Set: gate}.Build()},
	}.Build())
	require.NoError(t, err)
	require.Len(t, ownSources.GetItems(), 2)
	for i, src := range g.GetSources() {
		require.Equal(t, int32(i), src.GetOrdinal(), "the set's order")
		require.Contains(t, []string{"east", "west"}, src.GetAlias())
		require.Equal(t, strings.ToUpper(src.GetAlias()), src.GetName())
		require.Equal(t, map[string]string{"floor": "1"}, src.GetLabels())
	}
	require.Equal(t, "east", g.GetSources()[0].GetAlias())
	require.Equal(t, "video/mp4", g.GetSources()[0].GetContentType())
	require.Equal(t, "application/x-mcap", g.GetSources()[1].GetContentType())
	for _, v := range all[1:] {
		require.Empty(t, v.GetSources(), "a set with no cameras comes with none")
	}

	// Olive, who sees every site, is answered the vault's and the one in no
	// site too, the latter with no site.
	olives := startFor("portal", portalClient, olive.Id)
	theirs, _ := every("portal", olives, 100)
	require.Subset(t, aliases(theirs), []string{"gate", "safe", "loose"})
	for _, v := range theirs {
		if v.GetAlias() == "loose" {
			require.Nil(t, v.GetSite(), "in no site")
			require.Len(t, v.GetSources(), 1)
		}
	}

	// The single-set form: a set seen is answered alone; one not seen and
	// one there is not are answered alike, with nothing.
	one, err := catalog("portal", bobs, gate, 0, "")
	require.NoError(t, err)
	require.Len(t, one.GetItems(), 1)
	require.Equal(t, gate.GetId(), one.GetItems()[0].GetId())
	require.Len(t, one.GetItems()[0].GetSources(), 2)
	require.Empty(t, one.GetNext())
	bySlug, err := catalog("portal", bobs, api.SetRef_builder{Slug: api.SetRefBySlug_builder{Alias: z.Ptr("gate"), Tenant: acme}.Build()}.Build(), 0, "")
	require.NoError(t, err)
	require.True(t, proto.Equal(one, bySlug), "by alias as by id")
	notSeen, err := catalog("portal", bobs, safe, 0, "")
	require.NoError(t, err, "not NOT_FOUND")
	absent, err := catalog("portal", bobs, api.SetRef_builder{Id: pdid.New(core.DomSet).Bytes()}.Build(), 0, "")
	require.NoError(t, err, "not NOT_FOUND")
	require.Empty(t, notSeen.GetItems())
	require.True(t, proto.Equal(notSeen, absent), "not seen and absent are one answer")
	noSlug, err := catalog("portal", bobs, api.SetRef_builder{Slug: api.SetRefBySlug_builder{Alias: z.Ptr("safe"), Tenant: acme}.Build()}.Build(), 0, "")
	require.NoError(t, err)
	require.True(t, proto.Equal(notSeen, noSlug))
	again, err := catalog("portal", olives, safe, 0, "")
	require.NoError(t, err)
	require.Len(t, again.GetItems(), 1, "seen by whoever sees every site")

	// Narrowed to the handle: one naming a set answers that set alone.
	narrow := startFor("portal", portalClient, bob.Id, gate)
	named, _ := every("portal", narrow, 0)
	require.Equal(t, []string{"gate"}, aliases(named))

	// The vault made EVERYBODY is Bob's within the time an answer is kept.
	_, err = api.NewSiteServiceClient(admin).Patch(ctx, api.SitePatchRequest_builder{
		Ref: vault, Visibility: z.Ptr(api.SiteVisibility_SITE_VISIBILITY_EVERYBODY), DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		v, err := catalog("portal", bobs, safe, 0, "")
		return err == nil && len(v.GetItems()) == 1
	}, sitesSeenWithin, 200*time.Millisecond)
	later, _ := every("portal", bobs, 100)
	require.Contains(t, aliases(later), "safe")
	require.NotContains(t, aliases(later), "loose", "in no site, still")

	// Refused as Live is: another app's token with this handle, a handle
	// that is not, a handle ended, a person suspended.
	_, err = catalog("kiosk", bobs, nil, 0, "")
	require.Equal(t, codes.PermissionDenied, status.Code(err), "another app's token")
	kiosks := startFor("kiosk", "kiosk-client", bob.Id)
	_, err = catalog("kiosk", kiosks, nil, 0, "")
	require.NoError(t, err)
	_, err = catalog("portal", kiosks, nil, 0, "")
	require.Equal(t, codes.PermissionDenied, status.Code(err), "another app's handle")
	_, err = catalog("portal", bobs+"x", nil, 0, "")
	require.Equal(t, codes.PermissionDenied, status.Code(err), "no such handle")
	_, err = ds.Catalog(ctx, api.DelegationCatalogRequest_builder{Handle: bobs}.Build())
	require.Equal(t, codes.Unauthenticated, status.Code(err), "a handle without the app's token")
	_, err = ds.Revoke(as(exchange("portal")), api.DelegationRevokeRequest_builder{Handle: bobs}.Build())
	require.NoError(t, err)
	_, err = catalog("portal", bobs, nil, 0, "")
	require.Equal(t, codes.PermissionDenied, status.Code(err), "ended")
	carols := startFor("portal", portalClient, carol.Id)
	_, err = catalog("portal", carols, gate, 0, "")
	require.NoError(t, err)
	require.NoError(t, id.Suspend(ctx, carol.Id))
	_, err = catalog("portal", carols, gate, 0, "")
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "good standing")

	// And nobody calls it with a session of their own.
	_, err = api.NewDelegationServiceClient(asBob).Catalog(ctx, api.DelegationCatalogRequest_builder{Handle: kiosks}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
}
