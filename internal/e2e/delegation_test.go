package e2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
	set, _ := liveSource(t, ctx, c, admin, "av.mp4")

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
	other, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(), Alias: "yard"}.Build())
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
