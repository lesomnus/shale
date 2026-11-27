package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"

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

// What Catalog answers is what a catalogue shows and nothing of a host
// (§33.8): every field of its answer, at any depth, is one of these, and
// none is a field a producer, a relay or a satellite has beyond the
// fields every entity has, nor a host's row.
func TestCatalogCarriesNothingOfAHost(t *testing.T) {
	allowed := map[protoreflect.FullName][]protoreflect.Name{
		"shale.DelegationCatalogResponse": {"items", "next"},
		"shale.DelegationCatalogSet":      {"id", "alias", "name", "labels", "site", "sources"},
		"shale.DelegationCatalogSite":     {"id", "alias", "name", "visibility"},
		"shale.DelegationCatalogSource":   {"id", "alias", "name", "ordinal", "content_type", "labels"},
	}
	hosts := []protoreflect.MessageDescriptor{
		(&api.Producer{}).ProtoReflect().Descriptor(),
		(&api.Relay{}).ProtoReflect().Descriptor(),
		(&api.Satellite{}).ProtoReflect().Descriptor(),
	}
	// The fields every entity has (CLAUDE.md): key, tenant, the set axis,
	// alias, name, desc, labels, the timestamps.
	common := map[protoreflect.FieldNumber]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 13: true, 14: true, 15: true}
	hostFields := map[protoreflect.Name]protoreflect.FullName{}
	for _, h := range hosts {
		fs := h.Fields()
		for i := range fs.Len() {
			if f := fs.Get(i); !common[f.Number()] {
				hostFields[f.Name()] = h.FullName()
			}
		}
	}

	seen := map[protoreflect.FullName]bool{}
	var walk func(m protoreflect.MessageDescriptor)
	walk = func(m protoreflect.MessageDescriptor) {
		if seen[m.FullName()] || m.IsMapEntry() {
			return
		}
		seen[m.FullName()] = true
		want, ok := allowed[m.FullName()]
		require.True(t, ok, "%s is in the answer and is no message of the catalogue's", m.FullName())
		fs := m.Fields()
		var names []protoreflect.Name
		for i := range fs.Len() {
			f := fs.Get(i)
			names = append(names, f.Name())
			if h, ok := hostFields[f.Name()]; ok {
				t.Errorf("%s.%s is a field of %s", m.FullName(), f.Name(), h)
			}
			if f.Message() != nil {
				for _, h := range hosts {
					require.NotEqual(t, h.FullName(), f.Message().FullName(), "%s.%s", m.FullName(), f.Name())
				}
				walk(f.Message())
			}
		}
		require.ElementsMatch(t, want, names, "%s", m.FullName())
	}
	walk((&api.DelegationCatalogResponse{}).ProtoReflect().Descriptor())
	require.Len(t, seen, len(allowed))
}
