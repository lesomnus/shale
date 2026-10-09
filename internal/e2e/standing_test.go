package e2e_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
)

// The TTL the tests hold roster's answers for, and how long after it a
// refusal may take to show.
const (
	standingTtl   = time.Second
	standingSlack = 3 * time.Second
)

// A session ends when its person stops being in good standing at roster
// (§33.7), within the TTL, whatever the cookie's own clock says: suspended,
// on the tenant listener; signed out everywhere, on the tenant listener
// and on the cluster listener, where a session minted after it still
// works; and `GET /session` says so too.
func TestSessionEndsWithItsPersonsStanding(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: standingTtl}
	})
	ctx := context.Background()
	id := c.running.CP.Identity
	tenantBase, clusterBase := httpBases(t, c)
	tenantConn, clusterConn := plainConn(t, c.running.TenantAddr), plainConn(t, c.running.ClusterAddr)
	as := func(name, value string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "cookie", name+"="+value)
	}
	listSets := func(cookie string) error {
		_, err := api.NewSetServiceClient(tenantConn).List(as("shale_tenant", cookie), api.SetListRequest_builder{}.Build())
		return err
	}
	listNodes := func(cookie string) error {
		_, err := api.NewNodeServiceClient(clusterConn).List(as("shale_cluster", cookie), api.NodeListRequest_builder{}.Build())
		return err
	}
	who := func(base, name, cookie string) int {
		req, err := http.NewRequest(http.MethodGet, base+"/session", nil)
		require.NoError(t, err)
		req.Header.Set("Cookie", name+"="+cookie)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}
	// ends waits for a call to be refused as a credential that is no good,
	// and says it was within the TTL.
	ends := func(call func() error, why string) {
		t.Helper()
		began := time.Now()
		require.Eventually(t, func() bool { return status.Code(call()) == codes.Unauthenticated }, standingTtl+standingSlack, 50*time.Millisecond, why)
		require.Less(t, time.Since(began), standingTtl+standingSlack, why)
	}

	for _, alias := range []string{"bob", "carol"} {
		_, err := id.AddPerson(ctx, "acme", alias, alias)
		require.NoError(t, err)
		require.NoError(t, id.SetPassword(ctx, "acme", alias, alias+"'s password"))
	}
	require.NoError(t, id.SetPassword(ctx, "acme", "admin", "admin's password"))
	bob, err := id.Lookup(ctx, "acme", "bob")
	require.NoError(t, err)
	carol, err := id.Lookup(ctx, "acme", "carol")
	require.NoError(t, err)
	admin, err := id.Lookup(ctx, "acme", "admin")
	require.NoError(t, err)

	code, asBob := signIn(t, tenantBase, "acme", "bob", "bob's password")
	require.Equal(t, http.StatusNoContent, code)
	code, asCarol := signIn(t, tenantBase, "acme", "carol", "carol's password")
	require.Equal(t, http.StatusNoContent, code)
	code, asAdmin := signIn(t, clusterBase, "acme", "admin", "admin's password")
	require.Equal(t, http.StatusNoContent, code)
	require.NoError(t, listSets(asBob))
	require.NoError(t, listSets(asCarol))
	require.NoError(t, listNodes(asAdmin))
	require.Equal(t, http.StatusOK, who(tenantBase, "shale_tenant", asBob))

	// Suspended: Bob's session reads nothing more.
	require.NoError(t, id.Suspend(ctx, bob.Id))
	ends(func() error { return listSets(asBob) }, "a suspended person's session")
	require.Equal(t, http.StatusUnauthorized, who(tenantBase, "shale_tenant", asBob))
	require.NoError(t, listSets(asCarol), "nobody else's")

	// Signed out everywhere: what Carol signed in with before it ends, and
	// a sign-in after it works.
	require.NoError(t, id.SignOutEverywhere(ctx, carol.Id))
	ends(func() error { return listSets(asCarol) }, "a session from before a sign-out everywhere")
	code, again := signIn(t, tenantBase, "acme", "carol", "carol's password")
	require.Equal(t, http.StatusNoContent, code)
	require.NoError(t, listSets(again), "a session from after it")

	// On the cluster listener too.
	require.NoError(t, id.SignOutEverywhere(ctx, admin.Id))
	ends(func() error { return listNodes(asAdmin) }, "an operator's session from before a sign-out everywhere")
	require.Equal(t, http.StatusUnauthorized, who(clusterBase, "shale_cluster", asAdmin))
}

// A session minted through the issuer is held to its person's standing
// like a password's (§33.7).
func TestSsoSessionEndsWithItsPersonsStanding(t *testing.T) {
	c, idp, tenant, _ := ssoCluster(t)
	ctx := context.Background()
	dave, err := c.running.CP.Identity.AddPerson(ctx, "acme", "dave", "Dave")
	require.NoError(t, err)

	idp.As(dave.Id.String())
	asDave := browser(t)
	get(t, asDave, tenant+"/sso/login")
	cookie := cookieOf(asDave, tenant, "shale_tenant")
	require.NotEmpty(t, cookie)
	conn := plainConn(t, c.running.TenantAddr)
	list := func() error {
		_, err := api.NewSetServiceClient(conn).List(metadata.AppendToOutgoingContext(ctx, "cookie", cookie), api.SetListRequest_builder{}.Build())
		return err
	}
	require.NoError(t, list())

	require.NoError(t, c.running.CP.Identity.Suspend(ctx, dave.Id))
	require.Eventually(t, func() bool { return status.Code(list()) == codes.Unauthenticated }, standingTtl+standingSlack, 50*time.Millisecond)
	resp, _ := get(t, asDave, tenant+"/session")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
