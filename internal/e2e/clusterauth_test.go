package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
)

// signIn posts a password to one listener's `/session` and answers the
// status and the session cookie's value, if one was set.
func signIn(t *testing.T, base, tenant, alias, password string) (int, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"tenant": tenant, "alias": alias, "password": password})
	resp, err := http.Post(base+"/session", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	resp.Body.Close()
	v := ""
	for _, ck := range resp.Cookies() {
		if ck.MaxAge >= 0 && ck.Value != "" {
			v = ck.Value
		}
	}

	return resp.StatusCode, v
}

// httpBases waits for both sign-in listeners and answers their bases.
func httpBases(t *testing.T, c *cluster) (string, string) {
	t.Helper()
	var tenant, clusterHttp string
	require.Eventually(t, func() bool {
		tenant, clusterHttp = c.running.CP.HttpAddr(cmd.SurfaceTenant), c.running.CP.HttpAddr(cmd.SurfaceCluster)
		return tenant != "" && clusterHttp != ""
	}, 10*time.Second, 50*time.Millisecond)

	return "http://" + tenant, "http://" + clusterHttp
}

// plainConn is an API with no credential of its own: each call carries
// whatever cookie it is given.
func plainConn(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return conn
}

// The cluster API serves operators and nobody else (§33.1): a person of
// the operators' tenant whom roster grants one tenant service does nothing
// with it there -- not in another tenant, not in their own -- while an
// operator still reads across tenants; what nobody may write stays closed
// to operators; an app's DelegationService is not served there at all;
// the cluster listener's password sign-in takes operators only; and a
// session the tenant listener minted, an operator's included, is nobody on
// the cluster API whatever name it is presented under.
func TestClusterApiIsOperatorsOnly(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Ttl: time.Second}
	})
	ctx := context.Background()
	id := c.running.CP.Identity
	tenantBase, clusterBase := httpBases(t, c)

	// Another tenant, with a set in it.
	boss, _, err := id.Seed(ctx, "globex", "boss")
	require.NoError(t, err)
	_, err = id.Grant(ctx, "globex", identity.OperatorRole, []string{identity.Everything}, boss.Id)
	require.NoError(t, err)
	globex := api.TenantRef_builder{Alias: z.Ptr("globex")}.Build()
	theirs, err := api.NewSetServiceClient(c.dial("@globex/boss")).Add(ctx, api.SetAddRequest_builder{Tenant: globex, Alias: "vault"}.Build())
	require.NoError(t, err)

	// Bob of the operators' tenant is granted sets, and makes them on the
	// tenant API once the answer about him lapses.
	bob, err := id.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	_, err = id.Grant(ctx, "acme", "sets", []string{"/shale.SetService/*", "/shale.DelegationService/*"}, bob.Id)
	require.NoError(t, err)
	acme := api.TenantRef_builder{Alias: z.Ptr("acme")}.Build()
	require.Eventually(t, func() bool {
		_, err := api.NewSetServiceClient(c.dial("@acme/bob")).Add(ctx, api.SetAddRequest_builder{Tenant: acme, Alias: "bobs"}.Build())
		return err == nil
	}, 10*time.Second, 200*time.Millisecond)

	// On the cluster API the grant is nothing, in any tenant.
	asBob := c.dialCluster("@acme/bob")
	_, err = api.NewSetServiceClient(asBob).List(ctx, api.SetListRequest_builder{}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	_, err = api.NewSetServiceClient(asBob).Erase(ctx, api.SetRef_builder{Id: theirs.GetId()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)
	_, err = api.NewSetServiceClient(asBob).Add(ctx, api.SetAddRequest_builder{Tenant: globex, Alias: "planted"}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)

	// An app's service is not served there, to him or to an operator.
	for _, as := range []string{"@acme/bob", "@acme/admin"} {
		ds := api.NewDelegationServiceClient(c.dialCluster(as))
		_, err = ds.Add(ctx, api.DelegationAddRequest_builder{Tenant: globex}.Build())
		require.Equal(t, codes.Unimplemented, status.Code(err), "%s: %v", as, err)
		_, err = ds.Get(ctx, api.DelegationGetRequest_builder{Ref: api.DelegationRef_builder{Id: theirs.GetId()}.Build()}.Build())
		require.Equal(t, codes.Unimplemented, status.Code(err), "%s: %v", as, err)
	}

	// The operator still reads across tenants, and still may not write what
	// the system writes.
	asAdmin := c.dialCluster("@acme/admin")
	sets, err := api.NewSetServiceClient(asAdmin).List(ctx, api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	var seen bool
	for _, v := range sets.GetItems() {
		seen = seen || bytes.Equal(v.GetId(), theirs.GetId())
	}
	require.True(t, seen, "an operator reads every tenant's sets")
	nodes, err := api.NewNodeServiceClient(asAdmin).List(ctx, api.NodeListRequest_builder{}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, nodes.GetItems())
	_, err = api.NewNodeServiceClient(asAdmin).Patch(ctx, api.NodePatchRequest_builder{
		Ref: api.NodeRef_builder{Id: nodes.GetItems()[0].GetId()}.Build(), Name: z.Ptr("renamed"),
	}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "%v", err)

	// The cluster listener's password sign-in takes operators only; the
	// tenant listener's takes Bob.
	require.NoError(t, id.SetPassword(ctx, "acme", "bob", "bob's password"))
	require.NoError(t, id.SetPassword(ctx, "acme", "admin", "admin's password"))
	code, _ := signIn(t, clusterBase, "acme", "bob", "bob's password")
	require.Equal(t, http.StatusUnauthorized, code)
	code, _ = signIn(t, tenantBase, "acme", "bob", "bob's password")
	require.Equal(t, http.StatusNoContent, code)
	code, opsCookie := signIn(t, clusterBase, "acme", "admin", "admin's password")
	require.Equal(t, http.StatusNoContent, code)
	require.NotEmpty(t, opsCookie)

	clusterConn := plainConn(t, c.running.ClusterAddr)
	asCookie := func(name, value string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "cookie", name+"="+value)
	}
	_, err = api.NewNodeServiceClient(clusterConn).List(asCookie("shale_cluster", opsCookie), api.NodeListRequest_builder{}.Build())
	require.NoError(t, err, "the cluster listener's own session")

	// A session the tenant listener minted -- for an operator, even -- opens
	// nothing on the cluster API.
	code, tenantCookie := signIn(t, tenantBase, "acme", "admin", "admin's password")
	require.Equal(t, http.StatusNoContent, code)
	require.NotEmpty(t, tenantCookie)
	_, err = api.NewSetServiceClient(plainConn(t, c.running.TenantAddr)).List(asCookie("shale_tenant", tenantCookie), api.SetListRequest_builder{}.Build())
	require.NoError(t, err, "where it was minted")
	_, err = api.NewNodeServiceClient(clusterConn).List(asCookie("shale_cluster", tenantCookie), api.NodeListRequest_builder{}.Build())
	require.Equal(t, codes.Unauthenticated, status.Code(err), "%v", err)
	// Nor the other way round.
	_, err = api.NewSetServiceClient(plainConn(t, c.running.TenantAddr)).List(asCookie("shale_tenant", opsCookie), api.SetListRequest_builder{}.Build())
	require.Equal(t, codes.Unauthenticated, status.Code(err), "%v", err)
}
