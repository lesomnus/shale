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
// anything in their tenant, only one who sees every site issues a
// password, and nobody recovers a camera's credential
// (TestCredentialRecoveryIsAnOperators).
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

// Recovering a camera's credential hands a person its plaintext (§33.1):
// without operators nobody has it -- a site's member, a person who sees
// every site, the tenant's first person alike -- while a camera worker and a
// producer still receive theirs, which are decided by their kind; with
// operators it is roster's grant of the method, like any other call.
func TestCredentialRecoveryIsAnOperators(t *testing.T) {
	ctx := context.Background()
	tenant := pdid.New(DomTenant)
	m := api.CameraCredentialService_Resolve_FullMethodName
	const sentence = "recovering a camera's credential is an operator's act, and this deployment names no operators (auth.operators)"

	p := TenantPolicy{}
	member := pdid.New(DomHolder)
	everySite := pdid.New(DomHolder)
	first := pdid.New(DomHolder)
	for _, who := range []struct {
		name string
		ctx  context.Context
		id   pdid.Id
	}{
		{"a site's member", frame.Into(ctx, frame.New(member, tenant, frame.Grant{}).WithRow(api.Holder_builder{AllSites: false}.Build())), member},
		{"a person who sees every site", frame.Into(ctx, frame.New(everySite, tenant, frame.Grant{}).WithRow(api.Holder_builder{AllSites: true}.Build())), everySite},
		{"the tenant's first person", frame.Into(ctx, frame.New(first, tenant, frame.Grant{}).WithRow(api.Holder_builder{AllSites: true}.Build())), first},
	} {
		err := p.May(who.ctx, gate.Call{Actor: who.id, Tenant: tenant, Action: m})
		require.Equal(t, codes.PermissionDenied, status.Code(err), who.name)
		require.Equal(t, m+": "+sentence, status.Convert(err).Message(), who.name)
	}
	for _, d := range []pdid.Domain{DomCameraWorker, DomProducer} {
		require.NoError(t, p.May(ctx, gate.Call{Actor: pdid.New(d), Tenant: tenant, Action: m}), "a machine's delivery is its kind's")
	}
	// Preparing and importing answer a reference and no material.
	for _, other := range []string{api.CameraCredentialService_Prepare_FullMethodName, api.CameraCredentialService_Import_FullMethodName} {
		require.NoError(t, p.May(ctx, gate.Call{Actor: member, Tenant: tenant, Action: other}), other)
	}

	ops := &fakeOperators{}
	granted, other := pdid.New(DomHolder), pdid.New(DomHolder)
	ops.grant(tenant, granted, m)
	ops.grant(tenant, other, api.CameraCredentialService_Prepare_FullMethodName)
	p = TenantPolicy{Operators: ops}
	require.NoError(t, p.May(ctx, gate.Call{Actor: granted, Tenant: tenant, Action: m}))
	err := p.May(ctx, gate.Call{Actor: other, Tenant: tenant, Action: m})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.NotContains(t, err.Error(), sentence)
	for _, d := range []pdid.Domain{DomCameraWorker, DomProducer} {
		require.NoError(t, p.May(ctx, gate.Call{Actor: pdid.New(d), Tenant: tenant, Action: m}))
	}
}

// The cluster API: where roster says what people may change, a person of
// the operators' tenant whom roster grants all of Shale there -- whatever
// tenant `cluster_tenant` names, whatever another tenant grants its own
// people, nothing for a narrower grant, and nobody while roster does not
// answer; without, the cluster tenant's people, and nobody while it is not
// known. What nobody writes, and an app's DelegationService, are closed to
// operators too.
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

	// A grant of one service is nothing here: the cluster API sees every
	// tenant, so a tenant service granted here would be granted in all of
	// them (§33.1).
	require.Equal(t, codes.PermissionDenied, status.Code(byGrant.May(ctx, gate.Call{Actor: nodes, Tenant: hday, Action: m})))
	require.Equal(t, codes.PermissionDenied, status.Code(byGrant.May(ctx, gate.Call{Actor: nodes, Tenant: hday, Action: api.SinkService_List_FullMethodName})))
	sets := pdid.New(DomHolder)
	ops.grant(hday, sets, "/shale.SetService/*")
	for _, v := range []string{api.SetService_List_FullMethodName, api.SetService_Add_FullMethodName, api.SetService_Erase_FullMethodName} {
		require.Equal(t, codes.PermissionDenied, status.Code(byGrant.May(ctx, gate.Call{Actor: sets, Tenant: hday, Action: v})), v)
	}

	// Closed to everybody, operators included.
	for _, v := range []string{
		api.LaminaService_Add_FullMethodName, api.AttemptService_Patch_FullMethodName, api.NodeService_Patch_FullMethodName,
		api.SinkService_Patch_FullMethodName, api.ReaderService_Add_FullMethodName, api.RelayService_Add_FullMethodName,
		api.DelegationService_Add_FullMethodName, api.DelegationService_Get_FullMethodName, api.DelegationService_Patch_FullMethodName,
		api.DelegationService_Start_FullMethodName, api.DelegationService_Live_FullMethodName,
	} {
		require.Equal(t, codes.PermissionDenied, status.Code(byGrant.May(ctx, gate.Call{Actor: op, Tenant: hday, Action: v})), v)
	}
	require.NoError(t, byGrant.May(ctx, gate.Call{Actor: op, Tenant: hday, Action: api.RelayService_Patch_FullMethodName}), "a relay's labels are the operator's")
	require.NoError(t, byGrant.May(ctx, gate.Call{Actor: op, Tenant: hday, Action: api.SetService_List_FullMethodName}), "an operator reads across tenants")

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

func TestCameraWorkerHasOnlyScopedExecutionVerbs(t *testing.T) {
	p := TenantPolicy{}
	tenant, worker, producer := pdid.New(DomTenant), pdid.New(DomCameraWorker), pdid.New(DomProducer)
	call := func(who pdid.Id, m string) error {
		return p.May(context.Background(), gate.Call{Actor: who, Tenant: tenant, Action: m})
	}
	for _, m := range []string{api.CameraWorkerService_Heartbeat_FullMethodName, api.CameraWorkerService_RenewCertificate_FullMethodName, api.CameraWorkerService_Poll_FullMethodName, api.CameraWorkerService_CheckJob_FullMethodName, api.CameraWorkerService_ObserveJob_FullMethodName, api.CameraJobService_Claim_FullMethodName, api.CameraJobService_Report_FullMethodName, api.CameraJobService_Publish_FullMethodName} {
		require.NoError(t, call(worker, m), m)
		require.Equal(t, codes.PermissionDenied, status.Code(call(producer, m)), "Producer permissions must not expand: %s", m)
	}
	for _, m := range []string{api.CameraWorkerService_Get_FullMethodName, api.CameraWorkerService_List_FullMethodName, api.CameraWorkerService_Watch_FullMethodName, api.CameraWorkerService_Add_FullMethodName, api.CameraWorkerService_Patch_FullMethodName, api.CameraWorkerService_Erase_FullMethodName, api.CameraWorkerService_Adopt_FullMethodName, api.CameraWorkerService_Revoke_FullMethodName, api.CameraJobService_Create_FullMethodName, api.CameraNetworkService_Configure_FullMethodName, api.CameraAssignmentService_Assign_FullMethodName, api.CameraAssignmentService_Snapshot_FullMethodName, api.CameraBindingService_Bind_FullMethodName, api.SourceService_List_FullMethodName, api.SourceService_Live_FullMethodName, api.LaminaService_List_FullMethodName} {
		require.Equal(t, codes.PermissionDenied, status.Code(call(worker, m)), m)
	}
}

// A satellite does its own work on the cluster API and nothing more
// (§41): its heartbeat, certificate and routes, and the key set it
// verifies put tokens with; never a tenant's rows, another host's verbs,
// nor anything on the tenant API. Its Join is public, as every host's.
func TestSatelliteHasOnlyItsOwnVerbs(t *testing.T) {
	ctx := context.Background()
	sat := pdid.New(DomSatellite)
	cluster := ClusterPolicy{ClusterTenant: pdid.New(DomTenant)}
	for _, m := range []string{
		api.SatelliteService_Heartbeat_FullMethodName, api.SatelliteService_RenewCertificate_FullMethodName,
		api.SatelliteService_Route_FullMethodName, api.SatelliteService_Get_FullMethodName,
		api.SigningKeyService_List_FullMethodName,
	} {
		require.NoError(t, cluster.May(ctx, gate.Call{Actor: sat, Action: m}), m)
	}
	for _, m := range []string{
		api.SatelliteService_Adopt_FullMethodName, api.SatelliteService_Assign_FullMethodName, api.SatelliteService_Patch_FullMethodName,
		api.SatelliteService_Erase_FullMethodName, api.SatelliteService_List_FullMethodName,
		api.NodeService_Resolve_FullMethodName, api.NodeService_Heartbeat_FullMethodName, api.RelayService_Heartbeat_FullMethodName,
		api.ProducerService_List_FullMethodName, api.LaminaService_Allocate_FullMethodName, api.SigningKeyService_Rotate_FullMethodName,
	} {
		require.Equal(t, codes.PermissionDenied, status.Code(cluster.May(ctx, gate.Call{Actor: sat, Action: m})), m)
	}
	require.Equal(t, codes.PermissionDenied, status.Code(TenantPolicy{}.May(ctx, gate.Call{Actor: sat, Tenant: pdid.New(DomTenant), Action: api.SetService_Get_FullMethodName})))
	require.True(t, Public(api.SatelliteService_Join_FullMethodName))
	require.True(t, IsClusterService(api.SatelliteService_Heartbeat_FullMethodName))

	// Rows the system writes, closed to operators as well; the labels are
	// theirs.
	op := pdid.New(DomHolder)
	byTenant := ClusterPolicy{ClusterTenant: cluster.ClusterTenant}
	for _, m := range []string{api.SatelliteService_Add_FullMethodName} {
		require.Equal(t, codes.PermissionDenied, status.Code(byTenant.May(ctx, gate.Call{Actor: op, Tenant: cluster.ClusterTenant, Action: m})), m)
	}
	require.NoError(t, byTenant.May(ctx, gate.Call{Actor: op, Tenant: cluster.ClusterTenant, Action: api.SatelliteService_Patch_FullMethodName}))
	require.NoError(t, byTenant.May(ctx, gate.Call{Actor: op, Tenant: cluster.ClusterTenant, Action: api.SatelliteService_Adopt_FullMethodName}))
}

// A person's own keys for Shale are every person's to make, list and end
// on the tenant API (§33.1), whatever roster grants them, with reads
// granted or not and with roster down, since nothing about another person
// is in them; no host's; and nobody's on the cluster API, an operator's
// included.
func TestOwnKeysAreEveryPersonsOnTheTenantApi(t *testing.T) {
	ctx := context.Background()
	tenant, person := pdid.New(DomTenant), pdid.New(DomHolder)
	ops := &fakeOperators{tenant: tenant, down: true}
	ops.grant(tenant, person, "/shale.*/*")
	methods := []string{
		api.HolderService_BeginKey_FullMethodName, api.HolderService_PollKey_FullMethodName,
		api.HolderService_ListKeys_FullMethodName, api.HolderService_EndKey_FullMethodName,
	}
	for _, m := range methods {
		for _, p := range []TenantPolicy{{}, {Operators: ops}, {Operators: ops, Reads: true}} {
			require.NoError(t, p.May(ctx, gate.Call{Actor: person, Tenant: tenant, Action: m}), "%s, %+v", m, p)
		}
		require.Equal(t, codes.PermissionDenied, status.Code(TenantPolicy{}.May(ctx, gate.Call{Actor: pdid.New(DomProducer), Tenant: tenant, Action: m})), m)
		require.Equal(t, codes.PermissionDenied, status.Code(TenantPolicy{}.May(ctx, gate.Call{Actor: pdid.New(DomReader), Tenant: tenant, Action: m})), m)
		ops.down = false
		err := ClusterPolicy{ClusterTenant: tenant, Operators: ops}.May(ctx, gate.Call{Actor: person, Tenant: tenant, Action: m})
		require.Equal(t, codes.PermissionDenied, status.Code(err), "%s on the cluster API, an operator: %v", m, err)
		ops.down = true
	}
	require.Zero(t, ops.asked, "roster is not asked about a person's own keys")
}

// A key that carries a list of cameras (§33.1) calls only what a list
// confines, whatever its methods say and whatever its person may: every
// other method is refused at the door, ReachService and the person's own
// keys included; a key that carries none, and a session, are as before.
// A person's lists are every person's own on the tenant API, and the
// cluster API serves none of them.
func TestScopedKeyCallsOnlyWhatAListConfines(t *testing.T) {
	ctx := context.Background()
	tenant, person := pdid.New(DomTenant), pdid.New(DomHolder)
	ops := &fakeOperators{tenant: tenant}
	ops.grant(tenant, person, "/shale.*/*")
	scoped := func(scope string) context.Context {
		return frame.Into(ctx, frame.New(person, tenant, frame.Whole()).WithTokenScope(scope))
	}
	call := func(m string) gate.Call { return gate.Call{Actor: person, Tenant: tenant, Action: m} }

	require.Len(t, KeyScopeMethods(), 15)
	for _, m := range KeyScopeMethods() {
		for _, p := range []TenantPolicy{{}, {Operators: ops}} {
			require.NoError(t, p.May(scoped("x"), call(m)), "%s, %+v", m, p)
		}
	}
	for _, m := range []string{
		api.SourceService_Add_FullMethodName, api.SourceService_Erase_FullMethodName, api.SetService_Patch_FullMethodName,
		api.ProducerService_List_FullMethodName, api.SiteService_List_FullMethodName, api.CameraPolicyService_List_FullMethodName,
		api.HolderService_ListKeys_FullMethodName, api.HolderService_BeginKey_FullMethodName,
		api.ReachService_List_FullMethodName, api.ReachService_Patch_FullMethodName,
		api.CameraDeviceService_List_FullMethodName, api.SetService_Negotiate_FullMethodName,
		api.CameraBindingService_List_FullMethodName, api.CameraBindingService_Get_FullMethodName, api.CameraBindingService_Watch_FullMethodName,
	} {
		for _, p := range []TenantPolicy{{}, {Operators: ops}} {
			err := p.May(scoped("x"), call(m))
			require.Equal(t, codes.PermissionDenied, status.Code(err), "%s, %+v: %v", m, p, err)
			require.Contains(t, status.Convert(err).Message(), "carries a list of cameras")
			require.NoError(t, p.May(scoped(""), call(m)), "a key that carries none: %s", m)
		}
	}

	// A person's own lists.
	ops.asked = 0
	for _, m := range []string{api.ReachService_Add_FullMethodName, api.ReachService_Get_FullMethodName, api.ReachService_Patch_FullMethodName, api.ReachService_Erase_FullMethodName, api.ReachService_List_FullMethodName} {
		require.True(t, IsTenantService(m), m)
		for _, p := range []TenantPolicy{{}, {Operators: ops}, {Operators: ops, Reads: true}} {
			require.NoError(t, p.May(ctx, call(m)), "%s, %+v", m, p)
		}
		require.Equal(t, codes.PermissionDenied, status.Code(TenantPolicy{}.May(ctx, gate.Call{Actor: pdid.New(DomProducer), Tenant: tenant, Action: m})), m)
		require.Equal(t, codes.PermissionDenied, status.Code(ClusterPolicy{ClusterTenant: tenant, Operators: ops}.May(ctx, call(m))), "%s on the cluster API", m)
	}
	require.Zero(t, ops.asked, "roster is not asked about a person's own lists")
}
