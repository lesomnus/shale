package e2e_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
)

// The trail records what people do (§26.5): a set an admin adds is on it
// with the admin as its actor; the deployment's own writes at init, the
// node's join and heartbeats, and the leader's jobs are not.
func TestTrailOfPeople(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	admin := c.dial("@acme/admin")
	holders := api.NewHolderServiceClient(admin)
	me, err := holders.Get(ctx, api.HolderGetRequest_builder{
		Ref: api.HolderRef_builder{Slug: api.HolderRefBySlug_builder{
			Alias: z.Ptr("admin"), Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(),
		}.Build()}.Build(),
	}.Build())
	require.NoError(t, err)

	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "trail",
	}.Build())
	require.NoError(t, err)
	// The node in the process heartbeats every second: writes nobody made.
	time.Sleep(3 * time.Second)

	rows, err := api.NewAuditServiceClient(c.dialCluster("@cluster/ops")).List(ctx, api.AuditListRequest_builder{Size: 500}.Build())
	require.NoError(t, err)
	var ours *api.Audit
	for _, r := range rows.GetItems() {
		switch {
		case strings.HasPrefix(r.GetAction(), "/shale.NodeService/"), strings.HasPrefix(r.GetAction(), "/shale.RelayService/"),
			strings.HasPrefix(r.GetAction(), "/shale.SinkService/"), strings.HasPrefix(r.GetAction(), "/shale.DeviceService/"):
			t.Fatalf("a host's write is on the trail: %s", r.GetAction())
		case r.GetAction() == "" || r.GetAction() == api.TenantService_Add_FullMethodName || r.GetAction() == api.SigningKeyService_Add_FullMethodName:
			t.Fatalf("the deployment's own write is on the trail: %q", r.GetAction())
		case r.GetAction() == api.SetService_Add_FullMethodName && string(r.GetObjectId()) == string(set.GetId()):
			ours = r
		}
	}
	require.NotNil(t, ours, "the admin's write is on the trail: %d rows", len(rows.GetItems()))
	require.Equal(t, me.GetId(), ours.GetActorId())
}
