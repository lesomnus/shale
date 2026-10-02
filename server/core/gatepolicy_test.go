package core

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/gate"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// fakeOperators is the team at roster, as a set: who is on it, in which
// tenant, and whether roster answers at all.
type fakeOperators struct {
	tenant pdid.Id
	on     map[pdid.Id]bool
	down   bool
	asked  int
}

func (f *fakeOperators) Is(_ context.Context, tenant, holder pdid.Id) (bool, error) {
	f.asked++
	if f.down {
		return false, errors.New("roster: connection refused")
	}

	return tenant == f.tenant && f.on[holder], nil
}

// The tenant API where operators are a team (§33.1): every person reads
// what the console's pages read and nothing of people, membership, the
// trail or passwords; only an operator writes; a roster that does not
// answer is a refusal, as Unavailable; and hosts are untouched.
func TestTenantPolicyWithOperators(t *testing.T) {
	tenant := pdid.New(DomTenant)
	op, person := pdid.New(DomHolder), pdid.New(DomHolder)
	ops := &fakeOperators{tenant: tenant, on: map[pdid.Id]bool{op: true}}
	p := TenantPolicy{Operators: ops}
	ctx := context.Background()

	reads := []string{
		api.SetService_Get_FullMethodName, api.SetService_List_FullMethodName, api.SetService_Watch_FullMethodName, api.SetService_Live_FullMethodName,
		api.SourceService_Get_FullMethodName, api.SourceService_List_FullMethodName, api.SourceService_Watch_FullMethodName, api.SourceService_Live_FullMethodName,
		api.LaminaService_Get_FullMethodName, api.LaminaService_List_FullMethodName, api.LaminaService_Watch_FullMethodName, api.LaminaService_Timeline_FullMethodName,
		api.AttemptService_Get_FullMethodName, api.AttemptService_List_FullMethodName,
		api.SiteService_Get_FullMethodName, api.SiteService_List_FullMethodName, api.SiteService_Watch_FullMethodName,
		api.ProducerService_Get_FullMethodName, api.ProducerService_List_FullMethodName, api.ProducerService_Watch_FullMethodName,
		api.ReaderService_Get_FullMethodName, api.ReaderService_List_FullMethodName, api.ReaderService_Watch_FullMethodName,
	}
	operatorsOnly := []string{
		// Writes of every kind a person could reach before.
		api.SetService_Add_FullMethodName, api.SetService_Patch_FullMethodName, api.SetService_Erase_FullMethodName,
		api.SourceService_Add_FullMethodName, api.SourceService_Patch_FullMethodName, api.SourceService_Erase_FullMethodName,
		api.SiteService_Add_FullMethodName, api.SiteService_Patch_FullMethodName, api.SiteService_Erase_FullMethodName,
		api.ProducerService_Adopt_FullMethodName, api.ProducerService_Patch_FullMethodName, api.ProducerService_Erase_FullMethodName,
		api.ReaderService_Adopt_FullMethodName, api.ReaderService_Erase_FullMethodName,
		api.LaminaService_Reschedule_FullMethodName,
		// Administration, read or written.
		api.HolderService_Get_FullMethodName, api.HolderService_List_FullMethodName, api.HolderService_Add_FullMethodName,
		api.HolderService_Patch_FullMethodName, api.HolderService_IssuePassword_FullMethodName,
		api.SiteMemberService_Get_FullMethodName, api.SiteMemberService_List_FullMethodName, api.SiteMemberService_Add_FullMethodName,
		api.AuditService_Get_FullMethodName, api.AuditService_List_FullMethodName,
		// Calls a producer makes, which a person does not.
		api.SetService_Negotiate_FullMethodName, api.SetService_Allocate_FullMethodName,
	}

	call := func(actor pdid.Id, m string) gate.Call {
		return gate.Call{Actor: actor, Tenant: tenant, Action: m}
	}
	for _, m := range reads {
		require.NoError(t, p.May(ctx, call(person, m)), "a person reads %s", m)
		require.NoError(t, p.May(ctx, call(op, m)), "an operator reads %s", m)
		require.True(t, PersonMay(m), m)
	}
	for _, m := range operatorsOnly {
		require.Equal(t, codes.PermissionDenied, status.Code(p.May(ctx, call(person, m))), "a person may not call %s", m)
		require.NoError(t, p.May(ctx, call(op, m)), "an operator calls %s", m)
		require.False(t, PersonMay(m), m)
	}

	// What nobody may call stays closed to operators too.
	for _, m := range []string{api.LaminaService_Add_FullMethodName, api.AttemptService_Patch_FullMethodName, api.NodeService_List_FullMethodName} {
		require.Equal(t, codes.PermissionDenied, status.Code(p.May(ctx, call(op, m))), m)
	}

	// Somebody on a team of that name in another tenant is not an operator
	// here.
	other := gate.Call{Actor: op, Tenant: pdid.New(DomTenant), Action: api.SetService_Add_FullMethodName}
	require.Equal(t, codes.PermissionDenied, status.Code(p.May(ctx, other)))

	// Roster down: reads go on, writes fail closed and say why.
	ops.down = true
	require.NoError(t, p.May(ctx, call(person, api.SetService_List_FullMethodName)))
	asked := ops.asked
	require.NoError(t, p.May(ctx, call(op, api.LaminaService_Timeline_FullMethodName)))
	require.Equal(t, asked, ops.asked, "a read does not ask roster")
	err := p.May(ctx, call(op, api.SetService_Add_FullMethodName))
	require.Equal(t, codes.Unavailable, status.Code(err), "fails closed: %v", err)

	// Hosts are as they were.
	prod, rdr := pdid.New(DomProducer), pdid.New(DomReader)
	require.NoError(t, p.May(ctx, call(prod, api.SetService_Negotiate_FullMethodName)))
	require.Equal(t, codes.PermissionDenied, status.Code(p.May(ctx, call(prod, api.SetService_Add_FullMethodName))))
	require.NoError(t, p.May(ctx, call(rdr, api.LaminaService_Timeline_FullMethodName)))
}

// Without operators the tenant API is the rule before it: a person may do
// anything in their tenant, and only one who sees every site issues a
// password.
func TestTenantPolicyWithoutOperators(t *testing.T) {
	tenant, person := pdid.New(DomTenant), pdid.New(DomHolder)
	p := TenantPolicy{}
	call := gate.Call{Actor: person, Tenant: tenant}

	for _, m := range []string{api.SetService_Add_FullMethodName, api.HolderService_List_FullMethodName, api.ProducerService_Adopt_FullMethodName, api.AuditService_List_FullMethodName} {
		call.Action = m
		require.NoError(t, p.May(context.Background(), call), m)
	}

	call.Action = api.HolderService_IssuePassword_FullMethodName
	require.Equal(t, codes.PermissionDenied, status.Code(p.May(context.Background(), call)))
	admin := frame.New(person, tenant, frame.Grant{}).WithRow(api.Holder_builder{AllSites: true}.Build())
	require.NoError(t, p.May(frame.Into(context.Background(), admin), call))
}

// The cluster API: with operators a team, the team's members in its
// tenant, whatever tenant `cluster_tenant` names, and nobody while roster
// does not answer; without, the cluster tenant's people, and nobody while
// it is not known.
func TestClusterPolicyOperators(t *testing.T) {
	ctx := context.Background()
	hday, cluster := pdid.New(DomTenant), pdid.New(DomTenant)
	op, person, clusterPerson := pdid.New(DomHolder), pdid.New(DomHolder), pdid.New(DomHolder)
	ops := &fakeOperators{tenant: hday, on: map[pdid.Id]bool{op: true}}
	m := api.NodeService_Adopt_FullMethodName

	byTeam := ClusterPolicy{ClusterTenant: cluster, Operators: ops}
	require.NoError(t, byTeam.May(ctx, gate.Call{Actor: op, Tenant: hday, Action: m}))
	require.Equal(t, codes.PermissionDenied, status.Code(byTeam.May(ctx, gate.Call{Actor: person, Tenant: hday, Action: m})))
	require.Equal(t, codes.PermissionDenied, status.Code(byTeam.May(ctx, gate.Call{Actor: clusterPerson, Tenant: cluster, Action: m})),
		"operators are not a tenant once they are a team")
	require.Equal(t, codes.PermissionDenied, status.Code(byTeam.May(ctx, gate.Call{Actor: op, Tenant: hday, Action: api.SigningKeyService_Add_FullMethodName})),
		"keys are still made by Rotate")
	ops.down = true
	require.Equal(t, codes.Unavailable, status.Code(byTeam.May(ctx, gate.Call{Actor: op, Tenant: hday, Action: m})))
	ops.down = false

	// Hosts do not ask roster.
	asked := ops.asked
	require.NoError(t, byTeam.May(ctx, gate.Call{Actor: pdid.New(DomNode), Action: api.NodeService_Heartbeat_FullMethodName}))
	require.Equal(t, asked, ops.asked)

	byTenant := ClusterPolicy{ClusterTenant: cluster}
	require.NoError(t, byTenant.May(ctx, gate.Call{Actor: clusterPerson, Tenant: cluster, Action: m}))
	require.Equal(t, codes.PermissionDenied, status.Code(byTenant.May(ctx, gate.Call{Actor: op, Tenant: hday, Action: m})))
	require.Equal(t, codes.PermissionDenied, status.Code(ClusterPolicy{}.May(ctx, gate.Call{Actor: clusterPerson, Tenant: cluster, Action: m})))
}
