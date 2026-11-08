package core

import (
	"bytes"
	"context"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// negotiateManaged makes an exact full-catalog admission decision. Historical
// Source rows are never deleted, charged as active feeds, or scaled to make a
// new copied stream fit. The same scoped transaction fences preview and commit.
func (s coreSet) negotiateManaged(ctx context.Context, r *api.SetNegotiateRequest) (*api.SetNegotiateResponse, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	if kindOf(f.Actor) != DomProducer {
		return nil, denied("managed admission", "only the assigned Producer negotiates capture")
	}
	target := r.GetCatalog()
	if target.GetAssignmentGeneration() < 1 || target.GetCatalogRevision() < 0 || len(target.GetAssignmentId()) != 16 {
		return nil, invalid("catalog", "complete managed catalog identity is required")
	}
	if len(target.GetBindingId()) > 0 || len(target.GetSourceId()) > 0 || target.GetSpecRevision() != 0 {
		return nil, invalid("catalog", "aggregate admission cannot name an individual capture")
	}
	var out *api.SetNegotiateResponse
	var set *api.Set
	err = s.ownTx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		a, err := c.producerCatalogRow(ctx, api.CameraAssignmentRef_builder{Id: target.GetAssignmentId()}.Build(), f.Actor, f.Tenant)
		if err != nil {
			return err
		}
		if a.GetGeneration() != target.GetAssignmentGeneration() || a.GetCatalogRevision() != target.GetCatalogRevision() {
			return status.Error(codes.Aborted, "managed admission catalog changed")
		}
		set, err = nx.Set().Get(ctx, api.SetGetRequest_builder{Ref: r.GetRef()}.Build())
		if err != nil {
			return err
		}
		if !bytes.Equal(set.GetId(), a.GetSet().GetId()) {
			return denied("managed admission", "Set does not belong to assignment")
		}
		rows, err := c.ent(ctx).CameraBinding.Query().Where(camerabinding.AssignmentIdEQ(mustId(a.GetId()).Uuid()), camerabinding.DateErasedIsNil(), camerabinding.CollectEQ(true)).Order(ent.Asc(camerabinding.FieldSourceAlias)).Limit(501).All(ctx)
		if err != nil {
			return err
		}
		if len(rows) > 500 {
			return status.Error(codes.ResourceExhausted, "managed catalog exceeds source limit")
		}
		proposals := map[string]*api.SourceProposal{}
		for _, p := range r.GetSources() {
			id := string(p.GetSource().GetId())
			if id == "" || proposals[id] != nil {
				return invalid("sources", "unique explicit Source identities are required")
			}
			proposals[id] = p
		}
		if len(proposals) != len(rows) {
			return status.Error(codes.Aborted, "managed admission requires exact current catalog membership")
		}
		bounds, _, _, _, err := c.policies(ctx)
		if err != nil {
			return err
		}
		link := clampLink(r.GetLink(), bounds, func(string, string, string, string) {})
		var agreed []*api.SourceAgreed
		var patches []*api.SourcePatchRequest
		var total int64
		changed := !equalLink(set.GetLink(), link)
		for _, binding := range rows {
			if binding.Desired == nil {
				return failed("collecting binding has no published specification")
			}
			sp := proposals[string(binding.SourceId[:])]
			if sp == nil {
				return status.Error(codes.Aborted, "managed admission names foreign or missing Source")
			}
			if sp.GetProfile().GetMaxBitrate() < binding.Desired.GetProfile().GetMaxBitrateBps() {
				return invalid("profile", "copy ceiling cannot be lower than the desired video ceiling")
			}
			profile, err := clampSegment(segmentDefaults(sp.GetProfile(), bounds), bounds, epochOf(set), func(string, string, string, string) {})
			if err != nil {
				return err
			}
			if profile.GetMaxBitrate() != sp.GetProfile().GetMaxBitrate() {
				return status.Error(codes.ResourceExhausted, "managed copy ceiling exceeds upload policy")
			}
			if total > int64(^uint64(0)>>1)-profile.GetMaxBitrate() {
				return status.Error(codes.ResourceExhausted, "managed capture budget overflow")
			}
			total += profile.GetMaxBitrate()
			src, err := nx.Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: binding.SourceId[:]}.Build()}.Build())
			if err != nil {
				return err
			}
			if !bytes.Equal(src.GetSet().GetId(), set.GetId()) || !bytes.Equal(src.GetTenant().GetId(), a.GetTenant().GetId()) || !bytes.Equal(src.GetSite().GetId(), a.GetSite().GetId()) {
				return denied("managed admission", "Source scope changed")
			}
			ct := sp.GetContentType()
			if ct != "" && ct != "video/mp4" {
				return invalid("content_type", "managed camera media must be MP4")
			}
			live := sp.GetLiveCapable()
			patch := api.SourcePatchRequest_builder{Ref: api.SourceRef_builder{Id: src.GetId()}.Build(), Profile: profile, DateUpdated: src.GetDateUpdated()}
			if ct != "" && src.GetContentType() == "" {
				patch.ContentType = &ct
				changed = true
			}
			if live != src.GetLiveCapable() {
				patch.LiveCapable = &live
				changed = true
			}
			if !equalSegment(src.GetProfile(), profile) {
				changed = true
			}
			if !equalSegment(src.GetProfile(), profile) || patch.ContentType != nil || patch.LiveCapable != nil {
				patches = append(patches, patch.Build())
			}
			agreed = append(agreed, api.SourceAgreed_builder{SourceId: src.GetId(), Ordinal: src.GetOrdinal(), Profile: profile, MaxLength: MaxLength(profile)}.Build())
		}
		if set.GetMaxBitrateTotal() > 0 && total > set.GetMaxBitrateTotal() {
			return status.Error(codes.ResourceExhausted, "managed capture exceeds Set bitrate budget; existing feeds were preserved")
		}
		version := set.GetProfileVersion()
		if changed || version == 0 {
			version++
		}
		out = api.SetNegotiateResponse_builder{ProfileVersion: version, Link: link, Sources: agreed}.Build()
		if r.GetPreview() {
			return nil
		}
		if changed || set.GetProfileVersion() != version {
			if _, err := nx.Set().Patch(ctx, api.SetPatchRequest_builder{Ref: api.SetRef_builder{Id: set.GetId()}.Build(), Link: link, ProfileVersion: &version, DateUpdated: set.GetDateUpdated()}.Build()); err != nil {
				return err
			}
		}
		for _, patch := range patches {
			if _, err := nx.Source().Patch(ctx, patch); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ra, err := s.relayAssignment(ctx, f.Actor, set); err == nil {
		out.SetRelay(ra)
	}
	return out, nil
}
