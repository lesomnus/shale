package e2e_test

import (
	"context"
	"testing"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
)

// TestPatchLabels: a label patch lands, with the version given and with
// the version check forced (what the CLI sends).
func TestPatchLabels(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	sets := api.NewSetServiceClient(c.dial("@acme/admin"))
	set, err := sets.Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "labeled"}.Build())
	require.NoError(t, err)

	v, err := sets.Patch(ctx, api.SetPatchRequest_builder{
		Ref: api.SetRef_builder{Id: set.GetId()}.Build(), Labels: map[string]string{"zone": "a"}, DateUpdated: set.GetDateUpdated(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "a", v.GetLabels()["zone"], "with the version")

	v, err = sets.Patch(ctx, api.SetPatchRequest_builder{
		Ref: api.SetRef_builder{Id: set.GetId()}.Build(), Labels: map[string]string{"zone": "b"}, DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "b", v.GetLabels()["zone"], "with the version check forced")

	got, err := sets.Get(ctx, api.SetGetRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, "b", got.GetLabels()["zone"])
}
