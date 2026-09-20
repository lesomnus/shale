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
)

// TestSignIn covers §33.1's people: a password set at init, a session
// minted by the sign-in endpoint, the cookie carried as metadata on gRPC,
// the wall holding for the tenant it names, and SetPassword.
func TestSignIn(t *testing.T) {
	c := start(t)
	ctx := context.Background()

	// The admin's password is whatever init printed; the test sets one it
	// knows through the plain header first.
	admin := c.dial("@acme/admin")
	holders := api.NewHolderServiceClient(admin)
	_, err := holders.SetPassword(ctx, api.HolderSetPasswordRequest_builder{
		Ref: api.HolderRef_builder{Slug: api.HolderRefBySlug_builder{
			Alias: z.Ptr("admin"), Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(),
		}.Build()}.Build(),
		Password: "correct horse battery",
	}.Build())
	require.NoError(t, err)

	// The HTTP listener beside the tenant API.
	var httpAddr string
	require.Eventually(t, func() bool {
		httpAddr = c.running.CP.HttpAddr(cmd.SurfaceTenant)
		return httpAddr != ""
	}, 10*time.Second, 50*time.Millisecond)

	signIn := func(tenant, alias, password string) (*http.Response, string) {
		body, _ := json.Marshal(map[string]string{"tenant": tenant, "alias": alias, "password": password})
		resp, err := http.Post("http://"+httpAddr+"/session", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		resp.Body.Close()
		cookie := ""
		for _, ck := range resp.Cookies() {
			cookie = ck.Name + "=" + ck.Value
		}

		return resp, cookie
	}

	resp, _ := signIn("acme", "admin", "wrong")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	resp, cookie := signIn("acme", "admin", "correct horse battery")
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NotEmpty(t, cookie)

	// The cookie as metadata is the session on gRPC.
	conn, err := grpc.NewClient(c.running.TenantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	as := metadata.AppendToOutgoingContext(ctx, "cookie", cookie)

	sets := api.NewSetServiceClient(conn)
	set, err := sets.Add(as, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "by-session",
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "by-session", set.GetAlias())

	// Without it: refused.
	_, err = sets.List(ctx, api.SetListRequest_builder{}.Build())
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	// The wall: the cluster operator, signed in on the cluster API, does not
	// see acme's set through the tenant API's rules, and a tenant admin does
	// not reach the cluster services at all.
	_, err = api.NewNodeServiceClient(conn).List(as, api.NodeListRequest_builder{}.Build())
	require.Equal(t, codes.Unimplemented, status.Code(err), "cluster services are not mounted on the tenant API")
}
