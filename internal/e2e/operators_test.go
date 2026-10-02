package e2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/identity"
)

// Operators are a team at roster, not a tenant (§33.1): init puts the
// first admin on it; everybody else in the tenant reads every site and
// changes nothing; the cluster tenant's people are no longer operators;
// and putting somebody on the team makes them one on both surfaces.
func TestOperatorsAreATeam(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Auth.Operators = identity.OperatorsConfig{Tenant: "acme", Team: "shale-ops", Ttl: time.Second}
	})
	ctx := context.Background()
	acme := api.TenantRef_builder{Alias: z.Ptr("acme")}.Build()

	// The admin init made is an operator: a write on the tenant API, a read
	// of the cluster API.
	admin := c.dial("@acme/admin")
	_, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: acme, Alias: "lobby"}.Build())
	require.NoError(t, err)
	_, err = api.NewNodeServiceClient(c.dialCluster("@acme/admin")).List(ctx, api.NodeListRequest_builder{}.Build())
	require.NoError(t, err)

	// The cluster tenant's people are not, any more.
	_, err = api.NewNodeServiceClient(c.dialCluster("@cluster/ops")).List(ctx, api.NodeListRequest_builder{}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	// Somebody else in the tenant reads, every site, and writes nothing.
	bob, err := c.running.CP.Identity.AddPerson(ctx, "acme", "bob", "Bob")
	require.NoError(t, err)
	asBob := c.dial("@acme/bob")
	sets, err := api.NewSetServiceClient(asBob).List(ctx, api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, sets.GetItems(), 1, "the set in no site, which is every person's to read")
	_, err = api.NewSourceServiceClient(asBob).List(ctx, api.SourceListRequest_builder{}.Build())
	require.NoError(t, err)
	_, err = api.NewSiteServiceClient(asBob).List(ctx, api.SiteListRequest_builder{}.Build())
	require.NoError(t, err)
	_, err = api.NewSetServiceClient(asBob).Add(ctx, api.SetAddRequest_builder{Tenant: acme, Alias: "mine"}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	_, err = api.NewHolderServiceClient(asBob).List(ctx, api.HolderListRequest_builder{}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "people are an operator's to read")
	_, err = api.NewNodeServiceClient(c.dialCluster("@acme/bob")).List(ctx, api.NodeListRequest_builder{}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	row, err := api.NewHolderServiceClient(admin).Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: bob.Id.Bytes()}.Build()}.Build())
	require.NoError(t, err)
	require.True(t, row.GetAllSites(), "read is tenant-wide by default")

	// On the team, Bob is an operator once the answer about him lapses.
	team, err := c.running.CP.Operators.TeamId(ctx)
	require.NoError(t, err)
	require.NoError(t, c.running.CP.Identity.JoinTeam(ctx, team, bob.Id))
	require.Eventually(t, func() bool {
		_, err := api.NewSetServiceClient(asBob).Add(ctx, api.SetAddRequest_builder{Tenant: acme, Alias: "mine"}.Build())
		return err == nil
	}, 10*time.Second, 200*time.Millisecond)
	_, err = api.NewNodeServiceClient(c.dialCluster("@acme/bob")).List(ctx, api.NodeListRequest_builder{}.Build())
	require.NoError(t, err)
}
