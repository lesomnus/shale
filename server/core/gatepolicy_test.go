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

// fakeOperators is what roster grants, as a table: the patterns each
// person holds across their tenant, whose people operate the cluster, and
// whether roster answers at all. It decides the way `identity.Operators`
// does, one held pattern covering the method on its own.
type fakeOperators struct {
	tenant pdid.Id
	held   map[pdid.Id]map[pdid.Id][]string
	down   bool
	asked  int
}

func (f *fakeOperators) grant(tenant, holder pdid.Id, patterns ...string) {
	if f.held == nil {
		f.held = map[pdid.Id]map[pdid.Id][]string{}
	}
	if f.held[tenant] == nil {
		f.held[tenant] = map[pdid.Id][]string{}
	}
	f.held[tenant][holder] = append(f.held[tenant][holder], patterns...)
}

func (f *fakeOperators) May(_ context.Context, tenant, holder pdid.Id, method string) (bool, error) {
	f.asked++
	if f.down {
		return false, errors.New("roster: connection refused")
	}
	for _, h := range f.held[tenant][holder] {
		if frame.Covers(h, method) {
			return true, nil
		}
	}

	return false, nil
}

func (f *fakeOperators) MayOperate(ctx context.Context, tenant, holder pdid.Id, method string) (bool, error) {
	if tenant != f.tenant {
		return false, nil
	}

	return f.May(ctx, tenant, holder, method)
}

func (f *fakeOperators) Administers(ctx context.Context, tenant, holder pdid.Id) (bool, error) {
	return f.May(ctx, tenant, holder, "/shale.*/*")
}

// The tenant API where roster says what people may change (§33.1): every
// person reads what the console's pages read and nothing of people,
// membership, the trail or passwords; the rest is called as far as a
// pattern roster grants covers it -- all of Shale, or one service; a
// roster that does not answer is a refusal, as Unavailable; and hosts are
// untouched.
func TestTenantPolicyByWhatRosterGrants(t *testing.T) {
	tenant := pdid.New(DomTenant)
	op, person, cameras := pdid.New(DomHolder), pdid.New(DomHolder), pdid.New(DomHolder)
	ops := &fakeOperators{tenant: pdid.New(DomTenant)}
	ops.grant(tenant, op, "/shale.*/*")
	ops.grant(tenant, cameras, "/shale.SourceService/*")
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
	granted := []string{
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
	for _, m := range granted {
		require.Equal(t, codes.PermissionDenied, status.Code(p.May(ctx, call(person, m))), "a person granted nothing may not call %s", m)
		require.NoError(t, p.May(ctx, call(op, m)), "a person granted all of Shale calls %s", m)
		require.False(t, PersonMay(m), m)
	}

	// A grant of one service is that service and no other.
	require.NoError(t, p.May(ctx, call(cameras, api.SourceService_Add_FullMethodName)))
	require.NoError(t, p.May(ctx, call(cameras, api.SourceService_Erase_FullMethodName)))
	require.Equal(t, codes.PermissionDenied, status.Code(p.May(ctx, call(cameras, api.SetService_Add_FullMethodName))))
	require.Equal(t, codes.PermissionDenied, status.Code(p.May(ctx, call(cameras, api.HolderService_List_FullMethodName))))

	// What nobody may call stays closed to operators too.
	for _, m := range []string{api.LaminaService_Add_FullMethodName, api.AttemptService_Patch_FullMethodName, api.NodeService_List_FullMethodName} {
		require.Equal(t, codes.PermissionDenied, status.Code(p.May(ctx, call(op, m))), m)
	}

	// A grant in one tenant is nothing in another.
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

// The cluster API: where roster says what people may change, a person of
// the operators' tenant as far as roster grants them there -- whatever
// tenant `cluster_tenant` names, whatever another tenant grants its own
// people, and nobody while roster does not answer; without, the cluster
// tenant's people, and nobody while it is not known.
func TestClusterPolicyOperators(t *testing.T) {
	ctx := context.Background()
	hday, cluster, acme := pdid.New(DomTenant), pdid.New(DomTenant), pdid.New(DomTenant)
	op, person, clusterPerson, nodes, acmeAdmin := pdid.New(DomHolder), pdid.New(DomHolder), pdid.New(DomHolder), pdid.New(DomHolder), pdid.New(DomHolder)
	ops := &fakeOperators{tenant: hday}
	ops.grant(hday, op, "/shale.*/*")
	ops.grant(hday, nodes, "/shale.NodeService/*")
	ops.grant(acme, acmeAdmin, "/shale.*/*")
	m := api.NodeService_Adopt_FullMethodName

	byGrant := ClusterPolicy{ClusterTenant: cluster, Operators: ops}
	require.NoError(t, byGrant.May(ctx, gate.Call{Actor: op, Tenant: hday, Action: m}))
	require.Equal(t, codes.PermissionDenied, status.Code(byGrant.May(ctx, gate.Call{Actor: person, Tenant: hday, Action: m})))
	require.Equal(t, codes.PermissionDenied, status.Code(byGrant.May(ctx, gate.Call{Actor: clusterPerson, Tenant: cluster, Action: m})),
		"the cluster tenant's people are not operators once roster says who is")
	require.Equal(t, codes.PermissionDenied, status.Code(byGrant.May(ctx, gate.Call{Actor: acmeAdmin, Tenant: acme, Action: m})),
		"all of Shale in a customer's tenant is not the cluster")
	require.Equal(t, codes.PermissionDenied, status.Code(byGrant.May(ctx, gate.Call{Actor: op, Tenant: hday, Action: api.SigningKeyService_Add_FullMethodName})),
		"keys are still made by Rotate")

	// A grant of one service is that service.
	require.NoError(t, byGrant.May(ctx, gate.Call{Actor: nodes, Tenant: hday, Action: m}))
	require.Equal(t, codes.PermissionDenied, status.Code(byGrant.May(ctx, gate.Call{Actor: nodes, Tenant: hday, Action: api.SinkService_List_FullMethodName})))

	ops.down = true
	require.Equal(t, codes.Unavailable, status.Code(byGrant.May(ctx, gate.Call{Actor: op, Tenant: hday, Action: m})))
	ops.down = false

	// Hosts do not ask roster.
	asked := ops.asked
	require.NoError(t, byGrant.May(ctx, gate.Call{Actor: pdid.New(DomNode), Action: api.NodeService_Heartbeat_FullMethodName}))
	require.Equal(t, asked, ops.asked)

	byTenant := ClusterPolicy{ClusterTenant: cluster}
	require.NoError(t, byTenant.May(ctx, gate.Call{Actor: clusterPerson, Tenant: cluster, Action: m}))
	require.Equal(t, codes.PermissionDenied, status.Code(byTenant.May(ctx, gate.Call{Actor: op, Tenant: hday, Action: m})))
	require.Equal(t, codes.PermissionDenied, status.Code(ClusterPolicy{}.May(ctx, gate.Call{Actor: clusterPerson, Tenant: cluster, Action: m})))
}
