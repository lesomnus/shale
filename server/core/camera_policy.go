package core

import (
	"bytes"
	"context"
	"strings"
	"unicode/utf8"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	presetwire "github.com/lesomnus/shale/internal/camerapreset"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/camerapolicy"
	"github.com/lesomnus/shale/internal/ent/camerapreset"
	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type coreCameraPreset struct {
	Core
	api.CameraPresetServiceServer
}

func (s Core) CameraPreset() api.CameraPresetServiceServer {
	return coreCameraPreset{s, s.Next().CameraPreset()}
}

type coreCameraPolicy struct {
	Core
	api.CameraPolicyServiceServer
}

func (s Core) CameraPolicy() api.CameraPolicyServiceServer {
	return coreCameraPolicy{s, s.Next().CameraPolicy()}
}

// A tenant template can serve any site in that tenant. A site-specific template
// can serve only that exact site. The worker receives a resolved snapshot, never
// the template's unrelated rules.
func cameraPresetScope(p *api.CameraPreset, tenant, site []byte) error {
	if !bytes.Equal(p.GetTenant().GetId(), tenant) || (len(p.GetSite().GetId()) != 0 && !bytes.Equal(p.GetSite().GetId(), site)) {
		return denied("camera preset", "preset belongs to a different tenant or site")
	}
	return nil
}

func (s coreCameraPreset) Create(ctx context.Context, r *api.CameraPresetCreateRequest) (*api.CameraPreset, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	for field, value := range map[string]string{"preset_key": r.GetPresetKey(), "revision": r.GetRevision()} {
		if err := cameraToken(field, value); err != nil {
			return nil, err
		}
	}
	if len(r.GetName()) > 256 {
		return nil, invalid("name", "preset name is too long")
	}
	var out *api.CameraPreset
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		tenant, err := nx.Tenant().Get(ctx, api.TenantGetRequest_builder{Ref: r.GetTenant()}.Build())
		if err != nil {
			return err
		}
		var site *api.SiteRef
		if r.GetSite() != nil {
			v, err := nx.Site().Get(ctx, api.SiteGetRequest_builder{Ref: r.GetSite()}.Build())
			if err != nil {
				return err
			}
			if !bytes.Equal(v.GetTenant().GetId(), tenant.GetId()) {
				return denied("camera preset", "site belongs to another tenant")
			}
			site = api.SiteRef_builder{Id: v.GetId()}.Build()
		}
		c := New(nx, s.d)
		old, err := c.ent(ctx).CameraPreset.Query().Where(camerapreset.TenantIdEQ(mustId(tenant.GetId()).Uuid()), camerapreset.PresetKeyEQ(r.GetPresetKey()), camerapreset.RevisionEQ(r.GetRevision())).Only(ctx)
		if err == nil {
			if old.DateErased != nil {
				return status.Error(codes.AlreadyExists, "an immutable preset revision cannot be replaced")
			}
			out, err = nx.CameraPreset().Get(ctx, api.CameraPresetGetRequest_builder{Ref: api.CameraPresetRef_builder{Id: old.Id[:]}.Build()}.Build())
			if err != nil {
				return err
			}
			same, err := presetwire.SameRules(out.GetRules(), r.GetRules())
			if err != nil {
				return failed("stored preset revision is invalid")
			}
			if out.GetName() != r.GetName() || !same || !bytes.Equal(out.GetSite().GetId(), site.GetId()) {
				return status.Error(codes.AlreadyExists, "preset revision already has different content")
			}
			return nil
		}
		if !ent.IsNotFound(err) {
			return err
		}
		// A stored revision is immutable even if a Producer selector has since
		// moved to another Set. Replays above reauthorize the revision's scope;
		// only a newly created revision validates today's inventory graph.
		if err := c.cameraPresetRules(ctx, tenant.GetId(), site.GetId(), r.GetRules()); err != nil {
			return err
		}
		out, err = nx.CameraPreset().Add(ctx, api.CameraPresetAddRequest_builder{Tenant: api.TenantRef_builder{Id: tenant.GetId()}.Build(), Site: site, Name: r.GetName(), PresetKey: r.GetPresetKey(), Revision: r.GetRevision(), Rules: r.GetRules()}.Build())
		if err != nil {
			return err
		}
		out, err = nx.CameraPreset().Get(ctx, api.CameraPresetGetRequest_builder{Ref: api.CameraPresetRef_builder{Id: out.GetId()}.Build()}.Build())
		return err
	})
	return out, cameraConflict(err)
}

func (s coreCameraPolicy) Create(ctx context.Context, r *api.CameraPolicyCreateRequest) (*api.CameraPolicy, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	for field, value := range map[string]string{"policy_key": r.GetPolicyKey(), "revision": r.GetRevision()} {
		if err := cameraToken(field, value); err != nil {
			return nil, err
		}
	}
	if len(r.GetName()) > 256 {
		return nil, invalid("name", "policy name is too long")
	}
	var out *api.CameraPolicy
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		network, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: r.GetNetwork()}.Build())
		if err != nil {
			return err
		}
		preset, err := nx.CameraPreset().Get(ctx, api.CameraPresetGetRequest_builder{Ref: r.GetPreset()}.Build())
		if err != nil {
			return err
		}
		if err := cameraPresetScope(preset, network.GetTenant().GetId(), network.GetSite().GetId()); err != nil {
			return err
		}
		if err := c.cameraEnrollmentPolicy(ctx, network, r.GetEnrollment()); err != nil {
			return err
		}
		old, err := c.ent(ctx).CameraPolicy.Query().Where(camerapolicy.TenantIdEQ(mustId(network.GetTenant().GetId()).Uuid()), camerapolicy.PolicyKeyEQ(r.GetPolicyKey()), camerapolicy.RevisionEQ(r.GetRevision())).Only(ctx)
		if err == nil {
			if old.DateErased != nil {
				return status.Error(codes.AlreadyExists, "an immutable policy revision cannot be replaced")
			}
			out, err = nx.CameraPolicy().Get(ctx, api.CameraPolicyGetRequest_builder{Ref: api.CameraPolicyRef_builder{Id: old.Id[:]}.Build()}.Build())
			if err != nil {
				return err
			}
			if !bytes.Equal(out.GetNetwork().GetId(), network.GetId()) || !bytes.Equal(out.GetPreset().GetId(), preset.GetId()) || out.GetName() != r.GetName() || !proto.Equal(out.GetEnrollment(), r.GetEnrollment()) {
				return status.Error(codes.AlreadyExists, "policy revision already has different content")
			}
			return nil
		}
		if !ent.IsNotFound(err) {
			return err
		}
		out, err = nx.CameraPolicy().Add(ctx, api.CameraPolicyAddRequest_builder{Tenant: api.TenantRef_builder{Id: network.GetTenant().GetId()}.Build(), Site: cameraSite(network.GetSite()), Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build(), Preset: api.CameraPresetRef_builder{Id: preset.GetId()}.Build(), PolicyKey: r.GetPolicyKey(), Revision: r.GetRevision(), Name: r.GetName(), Enrollment: r.GetEnrollment()}.Build())
		if err != nil {
			return err
		}
		out, err = nx.CameraPolicy().Get(ctx, api.CameraPolicyGetRequest_builder{Ref: api.CameraPolicyRef_builder{Id: out.GetId()}.Build()}.Build())
		return err
	})
	return out, cameraConflict(err)
}

func (s coreCameraPolicy) Activate(ctx context.Context, r *api.CameraPolicyActivateRequest) (*api.CameraNetwork, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	var out *api.CameraNetwork
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		p, err := nx.CameraPolicy().Get(ctx, api.CameraPolicyGetRequest_builder{Ref: r.GetRef()}.Build())
		if err != nil {
			return err
		}
		if err := c.cameraLockNetwork(ctx, p.GetNetwork().GetId()); err != nil {
			return err
		}
		n, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: p.GetNetwork().GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := cameraSameScope(p.GetTenant().GetId(), p.GetSite().GetId(), n.GetTenant().GetId(), n.GetSite().GetId()); err != nil {
			return err
		}
		if err := cameraVersion(r.GetExpectedNetworkVersion(), n.GetDateUpdated()); err != nil {
			return err
		}
		if bytes.Equal(n.GetActivePolicyId(), p.GetId()) {
			out = n
			return nil
		}
		_, err = nx.CameraNetwork().Patch(ctx, api.CameraNetworkPatchRequest_builder{Ref: api.CameraNetworkRef_builder{Id: n.GetId()}.Build(), ActivePolicyId: p.GetId(), Revision: z.Ptr(n.GetRevision() + 1), DateUpdated: n.GetDateUpdated()}.Build())
		if err != nil {
			return err
		}
		out, err = nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: n.GetId()}.Build()}.Build())
		return err
	})
	return out, cameraConflict(err)
}

func cameraPresetID(raw string, domain pdid.Domain) ([]byte, error) {
	id, err := pdid.Parse(raw)
	if err != nil || id.Domain() != domain || id.String() != raw {
		return nil, invalid("preset.scope", "canonical inventory identifiers are required")
	}
	return id.Bytes(), nil
}

func cameraPresetText(v string) bool {
	return utf8.ValidString(v) && len(v) <= 256 && !strings.ContainsAny(v, "\x00\r\n")
}
