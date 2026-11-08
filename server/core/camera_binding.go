package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/payday/slug"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"github.com/lesomnus/shale/internal/ent/camerajob"
	"github.com/lesomnus/shale/internal/ent/source"
	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const cameraBindingLabel = "shale.provision.binding"
const cameraDeviceLabel = "shale.provision.device"

type coreCameraBinding struct {
	Core
	api.CameraBindingServiceServer
}

func (s Core) CameraBinding() api.CameraBindingServiceServer {
	return coreCameraBinding{s, s.Next().CameraBinding()}
}
func (s coreCameraBinding) Bind(ctx context.Context, r *api.CameraBindingBindRequest) (*api.CameraBinding, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	if err := cameraToken("logical_key", r.GetLogicalKey()); err != nil {
		return nil, err
	}
	alias := strings.ToLower(strings.TrimSpace(r.GetSourceAlias()))
	if alias != "" {
		if _, err := slug.ParseAlias(alias); err != nil {
			return nil, invalid("source_alias", "invalid source alias")
		}
	}
	var out *api.CameraBinding
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		a, err := c.cameraAssignmentRow(ctx, r.GetAssignment())
		if err != nil {
			return err
		}
		d, err := nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: r.GetDevice()}.Build())
		if err != nil {
			return err
		}
		if err := cameraSameScope(a.GetTenant().GetId(), a.GetSite().GetId(), d.GetTenant().GetId(), d.GetSite().GetId()); err != nil {
			return err
		}
		if !bytes.Equal(a.GetNetwork().GetId(), d.GetNetwork().GetId()) {
			return denied("bind", "device belongs to another camera network")
		}
		old, err := c.ent(ctx).CameraBinding.Query().Where(camerabinding.TenantIdEQ(mustId(a.GetTenant().GetId()).Uuid()), camerabinding.LogicalKeyEQ(r.GetLogicalKey()), camerabinding.DateErasedIsNil()).Only(ctx)
		if err == nil {
			if old.AssignmentId != mustId(a.GetId()).Uuid() || old.DeviceId != mustId(d.GetId()).Uuid() || (alias != "" && old.SourceAlias != alias) {
				return status.Error(codes.AlreadyExists, "logical binding already belongs to another assignment or device")
			}
			if r.GetSource() != nil && len(r.GetSource().GetId()) > 0 && !bytes.Equal(r.GetSource().GetId(), old.SourceId[:]) {
				return status.Error(codes.AlreadyExists, "logical binding already has another Source")
			}
			out, err = nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: old.Id[:]}.Build()}.Build())
			return err
		}
		if !ent.IsNotFound(err) {
			return err
		}
		var src *api.Source
		if r.GetSource() != nil {
			src, err = nx.Source().Get(ctx, api.SourceGetRequest_builder{Ref: r.GetSource()}.Build())
			if err != nil {
				return err
			}
			if alias != "" && alias != src.GetAlias() {
				return invalid("source_alias", "alias does not match the explicitly imported Source")
			}
			alias = src.GetAlias()
		} else {
			if alias == "" {
				h := sha256.Sum256([]byte(mustId(a.GetTenant().GetId()).String() + "/" + r.GetLogicalKey()))
				alias = fmt.Sprintf("cam-%x", h[:8])
			}
			existing, err := c.ent(ctx).Source.Query().Where(source.TenantIdEQ(mustId(a.GetTenant().GetId()).Uuid()), source.AliasEQ(alias), source.DateErasedIsNil()).Only(ctx)
			if err == nil {
				if existing.SetId != mustId(a.GetSet().GetId()).Uuid() || existing.Labels[cameraBindingLabel] != r.GetLogicalKey() || existing.Labels[cameraDeviceLabel] != d.GetDeviceKey() {
					return status.Error(codes.AlreadyExists, "source alias is reserved; importing a manual Source requires an explicit reference")
				}
				src, err = nx.Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: existing.Id[:]}.Build()}.Build())
				if err != nil {
					return err
				}
			} else if !ent.IsNotFound(err) {
				return err
			}
		}
		labels := map[string]string{cameraBindingLabel: r.GetLogicalKey(), cameraDeviceLabel: d.GetDeviceKey()}
		if src == nil {
			src, err = c.Source().Add(ctx, api.SourceAddRequest_builder{Id: pdid.New(DomSource).Bytes(), Set: api.SetRef_builder{Id: a.GetSet().GetId()}.Build(), Alias: alias, Name: r.GetName(), Labels: labels}.Build())
			if err != nil {
				return err
			}
		} else {
			if !bytes.Equal(src.GetSet().GetId(), a.GetSet().GetId()) {
				return denied("bind", "Source belongs to another immutable Set")
			}
			if err := cameraSameScope(src.GetTenant().GetId(), src.GetSite().GetId(), a.GetTenant().GetId(), a.GetSite().GetId()); err != nil {
				return err
			}
			merged := map[string]string{}
			for k, v := range src.GetLabels() {
				merged[k] = v
			}
			for k, v := range labels {
				if got := merged[k]; got != "" && got != v {
					return status.Error(codes.AlreadyExists, "Source has a conflicting physical or logical binding")
				}
				merged[k] = v
			}
			src, err = nx.Source().Patch(ctx, api.SourcePatchRequest_builder{Ref: api.SourceRef_builder{Id: src.GetId()}.Build(), Labels: merged, DateUpdated: src.GetDateUpdated()}.Build())
			if err != nil {
				return err
			}
		}
		out, err = nx.CameraBinding().Add(ctx, api.CameraBindingAddRequest_builder{Tenant: api.TenantRef_builder{Id: a.GetTenant().GetId()}.Build(), Site: cameraSite(a.GetSite()), Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), Source: api.SourceRef_builder{Id: src.GetId()}.Build(), LogicalKey: r.GetLogicalKey(), SourceAlias: alias, Name: r.GetName()}.Build())
		if err != nil {
			return err
		}
		_, err = c.cameraCatalogAdvance(ctx, a)
		return err
	})
	return out, cameraConflict(err)
}
func (s coreCameraBinding) SetCollect(ctx context.Context, r *api.CameraBindingSetCollectRequest) (*api.CameraBinding, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	var out *api.CameraBinding
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		b, err := nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: r.GetRef()}.Build())
		if err != nil {
			return err
		}
		c := New(nx, s.d)
		a, err := c.cameraAssignmentRow(ctx, api.CameraAssignmentRef_builder{Id: b.GetAssignment().GetId()}.Build())
		if err != nil {
			return err
		}
		b, err = nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: r.GetRef()}.Build())
		if err != nil {
			return err
		}
		if err := cameraVersion(r.GetExpectedVersion(), b.GetDateUpdated()); err != nil {
			return err
		}
		if b.GetCollect() == r.GetCollect() {
			out = b
			return nil
		}
		if r.GetCollect() && b.GetDesired() == nil {
			return failed("no validated Source specification has been published")
		}
		if !r.GetCollect() {
			jobs, err := c.ent(ctx).CameraJob.Query().Where(camerajob.BindingIdEQ(mustId(b.GetId()).Uuid()), camerajob.DateErasedIsNil()).All(ctx)
			if err != nil {
				return err
			}
			for _, j := range jobs {
				if cameraTerminalStage(api.CameraJobStage(j.Stage)) {
					continue
				}
				if _, err := nx.CameraJob().Patch(ctx, api.CameraJobPatchRequest_builder{Ref: api.CameraJobRef_builder{Id: j.Id[:]}.Build(), Stage: z.Ptr(api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED), LeaseExpires: timestamppb.New(c.d.now()), DateUpdated: timestamppb.New(j.DateUpdated)}.Build()); err != nil {
					return err
				}
			}
		}
		out, err = nx.CameraBinding().Patch(ctx, api.CameraBindingPatchRequest_builder{Ref: r.GetRef(), Collect: z.Ptr(r.GetCollect()), DateUpdated: b.GetDateUpdated()}.Build())
		if err != nil {
			return err
		}
		_, err = c.cameraCatalogAdvance(ctx, a)
		return err
	})
	return out, cameraConflict(err)
}
