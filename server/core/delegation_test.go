package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/gate"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// Whom an app may view for (§33.8): by default the person is held to what
// they may read themselves, which on a deployment whose reads are granted
// is their grant of the read or of the delegated method; with
// `auth.delegation.persons: tenant`, any person of the tenant, for the
// reads alone, and never what is closed to them.
func TestPersonPolicy(t *testing.T) {
	tenant := pdid.New(DomTenant)
	nobody, viewer, reader := pdid.New(DomHolder), pdid.New(DomHolder), pdid.New(DomHolder)
	ops := &fakeOperators{tenant: tenant}
	ops.grant(tenant, viewer, "/shale.DelegationService/*")
	ops.grant(tenant, reader, api.SetService_Live_FullMethodName)
	ctx := context.Background()

	live := func(actor pdid.Id) gate.Call {
		return gate.Call{Actor: actor, Tenant: tenant, Action: api.SetService_Live_FullMethodName}
	}
	timeline := func(actor pdid.Id) gate.Call {
		return gate.Call{Actor: actor, Tenant: tenant, Action: api.LaminaService_Timeline_FullMethodName}
	}
	denied := func(err error) bool { return status.Code(err) == codes.PermissionDenied }

	t.Run("reads everybody's", func(t *testing.T) {
		for _, every := range []bool{false, true} {
			p := newPersonPolicy(ops, false, DelegationConfig{EveryPerson: every}, delegatedLive)
			require.NoError(t, p.May(ctx, live(nobody)))
		}
	})
	t.Run("reads granted, by default", func(t *testing.T) {
		p := newPersonPolicy(ops, true, DelegationConfig{}, delegatedLive)
		require.True(t, denied(p.May(ctx, live(nobody))), "granted nothing")
		require.NoError(t, p.May(ctx, live(viewer)), "granted the delegated method")
		require.NoError(t, p.May(ctx, live(reader)), "granted the read")
		q := newPersonPolicy(ops, true, DelegationConfig{}, delegatedTimeline)
		require.True(t, denied(q.May(ctx, timeline(reader))), "granted live, not recordings")
		require.NoError(t, q.May(ctx, timeline(viewer)))
	})
	t.Run("reads granted, every person of the tenant", func(t *testing.T) {
		cfg := DelegationConfig{EveryPerson: true}
		p := newPersonPolicy(ops, true, cfg, delegatedLive)
		require.NoError(t, p.May(ctx, live(nobody)), "granted nothing, and viewed for")
		require.NoError(t, newPersonPolicy(ops, true, cfg, delegatedTimeline).May(ctx, timeline(nobody)))

		// The reads alone: not a write, not what the console's pages do not
		// read, and not the app's service itself.
		for _, m := range []string{
			api.SetService_Add_FullMethodName, api.SourceService_Erase_FullMethodName,
			api.HolderService_List_FullMethodName, api.AuditService_List_FullMethodName,
			api.DelegationService_Live_FullMethodName,
		} {
			require.True(t, denied(p.May(ctx, gate.Call{Actor: nobody, Tenant: tenant, Action: m})), m)
		}
		// Not a host that is not a person.
		require.True(t, denied(p.May(ctx, gate.Call{Actor: pdid.New(DomNode), Tenant: tenant, Action: api.SetService_Live_FullMethodName})))
	})
}
