package e2e_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/identity"
	"github.com/lesomnus/shale/server/core"
)

// A roster that names a tenant Shale has by another identifier (#322): a
// sign-in for it is refused with what to do -- make the tenant at roster
// with the identifier Shale has -- and not failed on the tenant's alias,
// which the row from before still has. Nothing is made, and nobody of the
// tenant is moved: the row stays on its identifier.
func TestATenantRosterNamesByAnotherIdentifier(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	cp := c.running.CP

	acme, err := cp.Ungated.Tenant().Get(ctx, api.TenantGetRequest_builder{Ref: api.TenantRef_builder{Alias: ptr("acme")}.Build()}.Build())
	require.NoError(t, err)
	before, err := cp.Ungated.Holder().List(ctx, api.HolderListRequest_builder{}.Build())
	require.NoError(t, err)

	elsewhere := pdid.New(core.DomTenant)
	eve := identity.Person{Id: pdid.New(core.DomHolder), Tenant: elsewhere, Alias: "eve", Name: "Eve", TenantAlias: "acme"}
	for range 2 {
		_, err = cp.Provision(ctx, eve)
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		msg := status.Convert(err).Message()
		require.Contains(t, msg, "@acme")
		require.Contains(t, msg, "shale tenant ls", "what to do: the tenant at roster, with the identifier this deployment has")
		require.Contains(t, err.Error(), pdid.Id(acme.GetId()).String(), "the log says both")
		require.Contains(t, err.Error(), elsewhere.String())
	}

	// Nothing made, and the tenant is where it was.
	_, err = cp.Ungated.Tenant().Get(ctx, api.TenantGetRequest_builder{Ref: api.TenantRef_builder{Id: elsewhere.Bytes()}.Build()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err))
	again, err := cp.Ungated.Tenant().Get(ctx, api.TenantGetRequest_builder{Ref: api.TenantRef_builder{Alias: ptr("acme")}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, acme.GetId(), again.GetId())
	after, err := cp.Ungated.Holder().List(ctx, api.HolderListRequest_builder{}.Build())
	require.NoError(t, err)
	require.Len(t, after.GetItems(), len(before.GetItems()), "and nobody arrived")
}

// A person roster names by another identifier than the row of their alias
// has (#322): refused with what to do -- make them at roster with the
// identifier Shale has, or erase the row here and arrive as somebody new
// -- and not failed on the alias. Nothing is made, and the row from before
// stays theirs to be had again.
func TestAPersonRosterNamesByAnotherIdentifier(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	cp := c.running.CP

	acme, err := cp.Ungated.Tenant().Get(ctx, api.TenantGetRequest_builder{Ref: api.TenantRef_builder{Alias: ptr("acme")}.Build()}.Build())
	require.NoError(t, err)
	admin, err := cp.Ungated.Holder().Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Slug: api.HolderRefBySlug_builder{
		Alias: ptr("admin"), Tenant: api.TenantRef_builder{Alias: ptr("acme")}.Build(),
	}.Build()}.Build()}.Build())
	require.NoError(t, err)

	elsewhere := pdid.New(core.DomHolder)
	p := identity.Person{Id: elsewhere, Tenant: pdid.Id(acme.GetId()), Alias: "admin", TenantAlias: "acme"}
	for range 2 {
		_, err = cp.Provision(ctx, p)
		require.Error(t, err)
		require.Equal(t, codes.FailedPrecondition, status.Code(err), "%v", err)
		msg := status.Convert(err).Message()
		require.Contains(t, msg, "@acme/admin")
		require.Contains(t, msg, "shale holder ls", "what to do: the person at roster, with the identifier this deployment has")
		require.Contains(t, msg, "shale holder erase", "or the row here erased")
		require.NotContains(t, msg, elsewhere.String())
		require.Contains(t, err.Error(), pdid.Id(admin.GetId()).String(), "the log says both")
		require.Contains(t, err.Error(), elsewhere.String())
	}

	_, err = cp.Ungated.Holder().Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: elsewhere.Bytes()}.Build()}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "nobody made")
	again, err := cp.Ungated.Holder().Get(ctx, api.HolderGetRequest_builder{Ref: api.HolderRef_builder{Id: admin.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, "admin", again.GetAlias(), "and the row from before is as it was")
}
