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

// People are roster's rows first (§33.1). One made at roster arrives here
// with nothing and gets a row the first time a credential names them; one
// made through `holder add` is made at roster and then here, on roster's
// identifier; the admin issues them a password, they sign in with it, and
// they see no site until somebody gives them one. Nobody who is not an
// admin issues passwords, and a name roster does not know is refused.
func TestPeopleFromRoster(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	admin := c.dial("@acme/admin")
	holders := api.NewHolderServiceClient(admin)

	// init made the admin at roster and here, as the tenant's first
	// person, who sees every site.
	me, err := holders.Get(ctx, api.HolderGetRequest_builder{
		Ref: api.HolderRef_builder{Slug: api.HolderRefBySlug_builder{
			Alias: z.Ptr("admin"), Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(),
		}.Build()}.Build(),
	}.Build())
	require.NoError(t, err)
	require.True(t, me.GetAllSites())

	// Somebody made at roster alone is nobody here until they arrive.
	bob, err := c.running.CP.Identity.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	_, err = holders.Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: bob.Id.Bytes()}.Build()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err))
	asBob := api.NewHolderServiceClient(c.dial("@acme/bob"))
	row, err := asBob.Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: bob.Id.Bytes()}.Build()}.Build())
	require.NoError(t, err, "the first call names them, and the row is made")
	require.Equal(t, bob.Id.Bytes(), row.GetId(), "on roster's identifier")
	require.Equal(t, "Bob", row.GetName())
	require.False(t, row.GetAllSites(), "not the first person of the tenant")

	// A name roster does not know is refused, not made.
	_, err = api.NewSetServiceClient(c.dial("@acme/mallory")).List(ctx, api.SetListRequest_builder{}.Build())
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	// `holder add` makes the person at roster and then here.
	carol, err := holders.Add(ctx, api.HolderAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "carol", Name: "Carol",
	}.Build())
	require.NoError(t, err)
	p, err := c.running.CP.Identity.Lookup(ctx, "acme", "carol")
	require.NoError(t, err)
	require.Equal(t, p.Id.Bytes(), carol.GetId(), "the identifier is roster's")
	_, err = holders.Add(ctx, api.HolderAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "dave", Id: carol.GetId(),
	}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err), "an identifier of one's own is refused")

	// The admin issues Carol a password, and their own; Carol may not
	// issue one.
	own, err := holders.IssuePassword(ctx, api.HolderIssuePasswordRequest_builder{Ref: api.HolderRef_builder{Id: me.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, own.GetPassword(), "the deployment's act, not a person asking for their own")
	issued, err := holders.IssuePassword(ctx, api.HolderIssuePasswordRequest_builder{Ref: api.HolderRef_builder{Id: carol.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, issued.GetPassword())
	_, err = api.NewHolderServiceClient(c.dial("@acme/carol")).IssuePassword(ctx, api.HolderIssuePasswordRequest_builder{Ref: api.HolderRef_builder{Id: bob.Id.Bytes()}.Build()}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// Carol signs in with it, and the session is hers.
	var httpAddr string
	require.Eventually(t, func() bool {
		httpAddr = c.running.CP.HttpAddr(cmd.SurfaceTenant)
		return httpAddr != ""
	}, 10*time.Second, 50*time.Millisecond)
	body, _ := json.Marshal(map[string]string{"tenant": "acme", "alias": "carol", "password": issued.GetPassword()})
	resp, err := http.Post("http://"+httpAddr+"/session", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	cookie := ""
	for _, ck := range resp.Cookies() {
		cookie = ck.Name + "=" + ck.Value
	}
	conn, err := grpc.NewClient(c.running.TenantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	as := metadata.AppendToOutgoingContext(ctx, "cookie", cookie)
	self, err := api.NewHolderServiceClient(conn).Get(as, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: carol.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, "carol", self.GetAlias())
}
