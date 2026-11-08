package core

import (
	"bytes"
	"context"
	"math"
	"strings"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/camerajob"
	"github.com/lesomnus/shale/internal/ent/lamina"
	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type coreCameraJob struct {
	Core
	api.CameraJobServiceServer
}

func (s Core) CameraJob() api.CameraJobServiceServer { return coreCameraJob{s, s.Next().CameraJob()} }
func cameraTerminalStage(v api.CameraJobStage) bool {
	return v == api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE || v == api.CameraJobStage_CAMERA_JOB_STAGE_FAILED || v == api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED
}
func cameraInputs(v *api.CameraJobInputs) error {
	if v == nil {
		return invalid("inputs", "pinned enrollment inputs are required")
	}
	if err := cameraProfile(v.GetDesired(), false); err != nil {
		return err
	}
	for _, r := range []*api.CameraRevisionReference{v.GetPolicy(), v.GetPreset()} {
		if r != nil {
			if err := cameraToken("revision.id", r.GetId()); err != nil {
				return err
			}
			if err := cameraToken("revision", r.GetRevision()); err != nil {
				return err
			}
		}
	}
	if err := cameraCredential(v.GetInitialCredential(), "initial_admin"); err != nil {
		return err
	}
	if err := cameraCredential(v.GetManagementCredential(), "management"); err != nil {
		return err
	}
	if err := cameraCredential(v.GetStreamingCredential(), "streaming"); err != nil {
		return err
	}
	if v.GetAllowActivation() && v.GetInitialCredential() == nil {
		return invalid("initial_credential", "activation requires a recoverable credential reference; central preparation is implemented separately")
	}
	return nil
}
func (s coreCameraJob) Create(ctx context.Context, r *api.CameraJobCreateRequest) (*api.CameraJob, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	if err := cameraToken("request_key", r.GetRequestKey()); err != nil {
		return nil, err
	}
	if err := cameraInputs(r.GetInputs()); err != nil {
		return nil, err
	}
	var out *api.CameraJob
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) (result error) {
		// Return the database's persisted timestamp precision (PostgreSQL uses
		// microseconds), not the unsaved in-memory nanosecond mutation value.
		defer func() {
			if result == nil && out != nil {
				out, result = nx.CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: out.GetId()}.Build()}.Build())
			}
		}()

		b, err := nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: r.GetBinding()}.Build())
		if err != nil {
			return err
		}
		c := New(nx, s.d)
		a, err := c.cameraAssignmentRow(ctx, api.CameraAssignmentRef_builder{Id: b.GetAssignment().GetId()}.Build())
		if err != nil {
			return err
		}
		canonical := proto.Clone(r).(*api.CameraJobCreateRequest)
		canonical.SetBinding(api.CameraBindingRef_builder{Id: b.GetId()}.Build())
		hash := cameraHash(canonical)
		old, err := c.ent(ctx).CameraJob.Query().Where(camerajob.TenantIdEQ(mustId(a.GetTenant().GetId()).Uuid()), camerajob.RequestKeyEQ(r.GetRequestKey()), camerajob.DateErasedIsNil()).Only(ctx)
		if err == nil {
			if !bytes.Equal(old.RequestHash, hash) {
				return status.Error(codes.AlreadyExists, "request key is already bound to different enrollment inputs")
			}
			out, err = nx.CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: old.Id[:]}.Build()}.Build())
			return err
		}
		if !ent.IsNotFound(err) {
			return err
		}
		out, err = nx.CameraJob().Add(ctx, api.CameraJobAddRequest_builder{Tenant: api.TenantRef_builder{Id: a.GetTenant().GetId()}.Build(), Site: cameraSite(a.GetSite()), Binding: api.CameraBindingRef_builder{Id: b.GetId()}.Build(), Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), RequestKey: r.GetRequestKey(), RequestHash: hash, Stage: api.CameraJobStage_CAMERA_JOB_STAGE_PENDING, AssignmentGeneration: a.GetGeneration(), Inputs: r.GetInputs()}.Build())
		return err
	})
	if err != nil {
		return nil, cameraConflict(err)
	}
	return out, nil
}

// Lock in assignment -> physical device order. The job's immutable assignment
// identifies the row to lock, and every subsequent read uses the transaction.
func (s Core) cameraJobRows(ctx context.Context, ref *api.CameraJobRef) (*api.CameraJob, *api.CameraAssignment, *api.CameraBinding, *api.CameraDevice, error) {
	nx := s.Next()
	j, err := nx.CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: ref}.Build())
	if err != nil {
		return nil, nil, nil, nil, err
	}
	a, err := s.cameraAssignmentRow(ctx, api.CameraAssignmentRef_builder{Id: j.GetAssignment().GetId()}.Build())
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if err := s.cameraExecutionScope(ctx, a); err != nil {
		return nil, nil, nil, nil, err
	}
	b, err := nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: j.GetBinding().GetId()}.Build()}.Build())
	if err != nil {
		return nil, nil, nil, nil, err
	}
	d, err := nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: b.GetDevice().GetId()}.Build()}.Build())
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if err := s.cameraLockDevice(ctx, d.GetId()); err != nil {
		return nil, nil, nil, nil, err
	}
	j, err = nx.CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: ref}.Build())
	if err != nil {
		return nil, nil, nil, nil, err
	}
	d, err = nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build()}.Build())
	return j, a, b, d, err
}
func cameraReplay(j *api.CameraJob, owner pdid.Id, key string, hash []byte) (bool, error) {
	if j.GetLastOperationKey() != key {
		return false, nil
	}
	if !bytes.Equal(j.GetLeaseOwner(), owner.Bytes()) || !bytes.Equal(j.GetLastOperationHash(), hash) {
		return false, status.Error(codes.AlreadyExists, "operation key is already bound to a different owner or operation")
	}
	return true, nil
}
func (s coreCameraJob) Claim(ctx context.Context, r *api.CameraJobClaimRequest) (*api.CameraJob, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	who, err := s.cameraExecutor(ctx)
	if err != nil {
		return nil, err
	}
	if err := cameraToken("operation_key", r.GetOperationKey()); err != nil {
		return nil, err
	}
	if r.GetLeaseSeconds() < 1 || r.GetLeaseSeconds() > 300 {
		return nil, invalid("lease_seconds", "a lease lasts one to 300 seconds")
	}
	canonical := proto.Clone(r).(*api.CameraJobClaimRequest)
	canonical.ClearExpectedVersion()
	hash := cameraHash(canonical)
	var out *api.CameraJob
	err = s.tx(ctx, func(ctx context.Context, nx api.Server) (result error) {
		// Return the database's persisted timestamp precision (PostgreSQL uses
		// microseconds), not the unsaved in-memory nanosecond mutation value.
		defer func() {
			if result == nil && out != nil {
				out, result = nx.CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: out.GetId()}.Build()}.Build())
			}
		}()

		c := New(nx, s.d)
		j, a, _, d, err := c.cameraJobRows(ctx, r.GetRef())
		if err != nil {
			return err
		}
		if replay, err := c.cameraClaimReplay(j, a, d, who, r.GetOperationKey(), hash); err != nil {
			return err
		} else if replay {
			out = j
			return nil
		}
		if err := cameraVersion(r.GetExpectedVersion(), j.GetDateUpdated()); err != nil {
			return err
		}
		if cameraTerminalStage(j.GetStage()) {
			return failed("terminal enrollment job cannot be claimed")
		}
		now := c.d.now()
		if j.GetLeaseExpires() != nil && j.GetLeaseExpires().AsTime().After(now) && j.GetAssignmentGeneration() == a.GetGeneration() && !bytes.Equal(j.GetLeaseOwner(), who.Bytes()) {
			return failed("enrollment job is leased to another executor")
		}
		if len(d.GetActiveJobId()) > 0 && !bytes.Equal(d.GetActiveJobId(), j.GetId()) {
			active, err := nx.CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: d.GetActiveJobId()}.Build()}.Build())
			if err != nil {
				return err
			}
			if err := cameraOtherJob(active, j.GetId()); err != nil {
				return err
			}
		}
		gen := d.GetClaimGeneration() + 1
		if _, err := nx.CameraDevice().Patch(ctx, api.CameraDevicePatchRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), ActiveJobId: j.GetId(), ClaimGeneration: z.Ptr(gen), DateUpdated: d.GetDateUpdated()}.Build()); err != nil {
			return err
		}
		stage := j.GetStage()
		if stage == api.CameraJobStage_CAMERA_JOB_STAGE_PENDING {
			stage = api.CameraJobStage_CAMERA_JOB_STAGE_CLAIMED
		}
		out, err = nx.CameraJob().Patch(ctx, api.CameraJobPatchRequest_builder{Ref: r.GetRef(), Stage: z.Ptr(stage), LeaseOwner: who.Bytes(), LeaseGeneration: z.Ptr(gen), LeaseExpires: timestamppb.New(now.Add(time.Duration(r.GetLeaseSeconds()) * time.Second)), AssignmentGeneration: z.Ptr(a.GetGeneration()), LastOperationKey: z.Ptr(r.GetOperationKey()), LastOperationHash: hash, DateUpdated: j.GetDateUpdated()}.Build())
		return err
	})
	if err != nil {
		return nil, cameraConflict(err)
	}
	return out, nil
}
func (s Core) cameraLease(j *api.CameraJob, a *api.CameraAssignment, d *api.CameraDevice, who pdid.Id, lease, assignment int64) error {
	if err := s.cameraLeaseFence(j, a, d, who, lease, assignment); err != nil {
		return err
	}
	if cameraTerminalStage(j.GetStage()) {
		return failed("enrollment job is terminal")
	}
	return nil
}

func (s Core) cameraLeaseFence(j *api.CameraJob, a *api.CameraAssignment, d *api.CameraDevice, who pdid.Id, lease, assignment int64) error {
	if lease == 0 || lease != j.GetLeaseGeneration() || lease != d.GetClaimGeneration() || !bytes.Equal(d.GetActiveJobId(), j.GetId()) || !bytes.Equal(j.GetLeaseOwner(), who.Bytes()) || j.GetLeaseExpires() == nil || !j.GetLeaseExpires().AsTime().After(s.d.now()) {
		return failed("camera job lease expired or was replaced")
	}
	if assignment == 0 || assignment != a.GetGeneration() || assignment != j.GetAssignmentGeneration() {
		return status.Error(codes.Aborted, "camera assignment was replaced")
	}
	return nil
}
func cameraStageTransition(from, to api.CameraJobStage) bool {
	if to == api.CameraJobStage_CAMERA_JOB_STAGE_WAITING || to == api.CameraJobStage_CAMERA_JOB_STAGE_FAILED || to == api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED {
		return true
	}
	switch from {
	case api.CameraJobStage_CAMERA_JOB_STAGE_CLAIMED, api.CameraJobStage_CAMERA_JOB_STAGE_WAITING:
		return to == api.CameraJobStage_CAMERA_JOB_STAGE_APPLYING || to == api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING
	case api.CameraJobStage_CAMERA_JOB_STAGE_APPLYING:
		return to == api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING
	case api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING:
		return to == api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING
	case api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY:
		return to == api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE
	}
	return false
}
func (s coreCameraJob) Report(ctx context.Context, r *api.CameraJobReportRequest) (*api.CameraJob, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	who, err := s.cameraExecutor(ctx)
	if err != nil {
		return nil, err
	}
	if err := cameraToken("operation_key", r.GetOperationKey()); err != nil {
		return nil, err
	}
	if err := cameraObservation(r.GetObserved(), false); err != nil {
		return nil, err
	}
	canonical := proto.Clone(r).(*api.CameraJobReportRequest)
	canonical.ClearExpectedVersion()
	hash := cameraHash(canonical)
	var out *api.CameraJob
	err = s.tx(ctx, func(ctx context.Context, nx api.Server) (result error) {
		// Return the database's persisted timestamp precision (PostgreSQL uses
		// microseconds), not the unsaved in-memory nanosecond mutation value.
		defer func() {
			if result == nil && out != nil {
				out, result = nx.CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: out.GetId()}.Build()}.Build())
			}
		}()

		c := New(nx, s.d)
		j, a, b, d, err := c.cameraJobRows(ctx, r.GetRef())
		if err != nil {
			return err
		}
		// A machine's replay must still refer to its current assignment and
		// physical-device fence. Operator historical acknowledgments retain
		// their existing compatibility behavior.
		if kindOf(who) == DomCameraWorker {
			if err := c.cameraLeaseFence(j, a, d, who, r.GetLeaseGeneration(), r.GetAssignmentGeneration()); err != nil {
				return err
			}
		}
		if replay, err := cameraReplay(j, who, r.GetOperationKey(), hash); err != nil {
			return err
		} else if replay {
			out = j
			return nil
		}
		if err := cameraVersion(r.GetExpectedVersion(), j.GetDateUpdated()); err != nil {
			return err
		}
		if err := c.cameraLease(j, a, d, who, r.GetLeaseGeneration(), r.GetAssignmentGeneration()); err != nil {
			return err
		}
		if !cameraStageTransition(j.GetStage(), r.GetStage()) {
			return failed("invalid enrollment stage transition")
		}
		if r.GetStage() == api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE {
			if err := c.cameraActive(ctx, j, a, b, r.GetObserved()); err != nil {
				return err
			}
		}
		out, err = nx.CameraJob().Patch(ctx, api.CameraJobPatchRequest_builder{Ref: r.GetRef(), Stage: z.Ptr(r.GetStage()), Observed: r.GetObserved(), LastOperationKey: z.Ptr(r.GetOperationKey()), LastOperationHash: hash, DateUpdated: j.GetDateUpdated()}.Build())
		if err != nil {
			return err
		}
		if r.GetStage() == api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE {
			_, err = nx.CameraBinding().Patch(ctx, api.CameraBindingPatchRequest_builder{Ref: api.CameraBindingRef_builder{Id: b.GetId()}.Build(), Observed: r.GetObserved(), AppliedRevision: z.Ptr(j.GetPublishedRevision()), DateUpdated: b.GetDateUpdated()}.Build())
		}
		return err
	})
	if err != nil {
		return nil, cameraConflict(err)
	}
	return out, nil
}
func (s Core) cameraActive(ctx context.Context, j *api.CameraJob, a *api.CameraAssignment, b *api.CameraBinding, o *api.CameraJobObservation) error {
	if err := cameraObservation(o, true); err != nil {
		return err
	}
	if j.GetDatePublished() == nil || j.GetPublishedRevision() == 0 || b.GetDesired() == nil || b.GetDesired().GetRevision() != j.GetPublishedRevision() || !b.GetCollect() {
		return failed("capture deployment is not current")
	}
	after := j.GetDatePublished().AsTime()
	if o.GetAppliedRevision() != j.GetPublishedRevision() {
		return failed("the published capture revision has not been acknowledged as applied")
	}
	if o.GetLiveDecodedFrames() < 2 || !cameraFresh(o.GetLiveFirstFrameAt(), after) || !cameraFresh(o.GetLiveLastFrameAt(), after) || !o.GetLiveLastFrameAt().AsTime().After(o.GetLiveFirstFrameAt().AsTime()) {
		return failed("advancing decoded live frames after publication are required")
	}
	p, err := s.Next().Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: a.GetProducer().GetId()}.Build()}.Build())
	if err != nil {
		return err
	}
	if p.GetState() != api.HostState_HOST_STATE_ADOPTED || !bytes.Equal(p.GetSet().GetId(), a.GetSet().GetId()) || !cameraFresh(p.GetStatus().GetDateReported(), after) || s.d.now().Sub(p.GetStatus().GetDateReported().AsTime()) > 2*time.Minute {
		return failed("fresh assigned Producer input evidence is required")
	}
	input := false
	for _, v := range p.GetStatus().GetSources() {
		if bytes.Equal(v.GetSourceId(), b.GetSource().GetId()) && v.GetInputUp() && v.GetFrameRate() > 0 {
			input = true
		}
	}
	if !input {
		return failed("assigned Producer has not reported this source ingesting")
	}
	count, err := s.ent(ctx).Lamina.Query().Where(lamina.SourceIdEQ(mustId(b.GetSource().GetId()).Uuid()), lamina.StateEQ(int32(api.LaminaState_LAMINA_STATE_COMMITTED)), lamina.IncompleteEQ(false), lamina.DateCommittedGTE(after), lamina.DateStartedGTE(after)).Count(ctx)
	if err != nil {
		return err
	}
	if count == 0 {
		return failed("new complete committed recording after publication is required")
	}
	return nil
}
func (s coreCameraJob) Publish(ctx context.Context, r *api.CameraJobPublishRequest) (*api.CameraJobPublishResponse, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	who, err := s.cameraExecutor(ctx)
	if err != nil {
		return nil, err
	}
	if err := cameraToken("operation_key", r.GetOperationKey()); err != nil {
		return nil, err
	}
	if err := cameraObservation(r.GetObserved(), true); err != nil {
		return nil, err
	}
	spec := r.GetSpec()
	if spec == nil {
		return nil, invalid("spec", "a validated capture specification is required")
	}
	if err := cameraProfile(spec.GetProfile(), true); err != nil {
		return nil, err
	}
	if err := cameraCredential(spec.GetCredential(), "streaming"); err != nil {
		return nil, err
	}
	if !proto.Equal(spec.GetProfile(), r.GetObserved().GetEffective()) {
		return nil, invalid("profile", "capture specification does not match effective settings")
	}
	actual := r.GetObserved().GetVerified().GetProfile()
	desired := spec.GetProfile()
	if actual.GetCodec() != desired.GetCodec() || actual.GetWidth() != desired.GetWidth() || actual.GetHeight() != desired.GetHeight() || math.Abs(actual.GetFps()-desired.GetFps()) > math.Max(0.5, desired.GetFps()*0.05) {
		return nil, invalid("verified", "actual media does not match the capture specification")
	}
	if len(spec.GetLabels()) > 32 || len(spec.GetZone()) > 128 || len(spec.GetName()) > 256 {
		return nil, invalid("spec", "capture metadata exceeds its bounds")
	}
	for k, v := range spec.GetLabels() {
		if len(v) > 256 {
			return nil, invalid("labels", "label is too long")
		}
		if err := cameraToken("label", k); err != nil {
			return nil, err
		}
		for _, word := range []string{"password", "credential", "secret", "token", "authorization"} {
			if strings.Contains(strings.ToLower(k), word) {
				return nil, invalid("labels", "credential labels are not allowed")
			}
		}
	}
	canonical := proto.Clone(r).(*api.CameraJobPublishRequest)
	canonical.ClearExpectedVersion()
	hash := cameraHash(canonical)
	var out *api.CameraJobPublishResponse
	err = s.tx(ctx, func(ctx context.Context, nx api.Server) (result error) {
		defer func() {
			if result == nil && out != nil {
				j, err := nx.CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: out.GetJob().GetId()}.Build()}.Build())
				if err != nil {
					result = err
					return
				}
				b, err := nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: out.GetBinding().GetId()}.Build()}.Build())
				if err != nil {
					result = err
					return
				}
				out.SetJob(j)
				out.SetBinding(b)
			}
		}()

		c := New(nx, s.d)
		j, a, b, d, err := c.cameraJobRows(ctx, r.GetRef())
		if err != nil {
			return err
		}
		// A machine's replay must still refer to its current assignment and
		// physical-device fence. Operator historical acknowledgments retain
		// their existing compatibility behavior.
		if kindOf(who) == DomCameraWorker {
			if err := c.cameraLeaseFence(j, a, d, who, r.GetLeaseGeneration(), r.GetAssignmentGeneration()); err != nil {
				return err
			}
		}
		if replay, err := cameraReplay(j, who, r.GetOperationKey(), hash); err != nil {
			return err
		} else if replay {
			out = api.CameraJobPublishResponse_builder{Job: j, Binding: b, CatalogRevision: j.GetPublishedRevision()}.Build()
			return nil
		}
		if err := cameraVersion(r.GetExpectedVersion(), j.GetDateUpdated()); err != nil {
			return err
		}
		if err := c.cameraLease(j, a, d, who, r.GetLeaseGeneration(), r.GetAssignmentGeneration()); err != nil {
			return err
		}
		if j.GetStage() != api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING {
			return failed("camera must be independently verified before deployment")
		}
		if !proto.Equal(spec.GetCredential(), j.GetInputs().GetStreamingCredential()) {
			return invalid("credential", "capture must use the pinned streaming credential version")
		}
		if len(spec.GetBindingId()) > 0 && !bytes.Equal(spec.GetBindingId(), b.GetId()) {
			return denied("publish", "capture specification names a different binding")
		}
		if len(spec.GetSourceId()) > 0 && !bytes.Equal(spec.GetSourceId(), b.GetSource().GetId()) {
			return denied("publish", "capture specification names another Source")
		}
		if spec.GetAlias() != "" && spec.GetAlias() != b.GetSourceAlias() {
			return invalid("alias", "capture must preserve its reserved Source alias")
		}
		n, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: a.GetNetwork().GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := cameraURI(spec.GetInput(), n.GetCidrs(), true); err != nil {
			return err
		}
		ready := proto.Clone(spec).(*api.CameraCaptureSpec)
		ready.SetBindingId(b.GetId())
		ready.SetSourceId(b.GetSource().GetId())
		ready.SetAlias(b.GetSourceAlias())
		ready.SetRevision(a.GetCatalogRevision() + 1)
		labels := map[string]string{}
		for k, v := range ready.GetLabels() {
			labels[k] = v
		}
		for k, v := range map[string]string{cameraBindingLabel: b.GetLogicalKey(), cameraDeviceLabel: d.GetDeviceKey()} {
			if got := labels[k]; got != "" && got != v {
				return invalid("labels", "capture binding labels do not match their authority")
			}
			labels[k] = v
		}
		ready.SetLabels(labels)
		// The normal Producer registration path verifies configuration labels
		// against its Source row. Publish their authority metadata atomically with
		// the desired specification, preserving unrelated existing labels.
		src, err := nx.Source().Get(ctx, api.SourceGetRequest_builder{Ref: api.SourceRef_builder{Id: b.GetSource().GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if !bytes.Equal(src.GetSet().GetId(), a.GetSet().GetId()) {
			return denied("publish", "Source belongs to another immutable Set")
		}
		for key, expected := range map[string]string{cameraBindingLabel: b.GetLogicalKey(), cameraDeviceLabel: d.GetDeviceKey()} {
			if got := src.GetLabels()[key]; got != "" && got != expected {
				return failed("Source binding metadata changed; resolve ownership before publication")
			}
		}
		merged := map[string]string{}
		for key, value := range src.GetLabels() {
			merged[key] = value
		}
		for key, value := range labels {
			merged[key] = value
		}
		patch := api.SourcePatchRequest_builder{Ref: api.SourceRef_builder{Id: src.GetId()}.Build(), Labels: merged, DateUpdated: src.GetDateUpdated()}
		if ready.GetName() != "" {
			patch.Name = z.Ptr(ready.GetName())
		}
		if ready.GetZone() != "" {
			patch.Zone = z.Ptr(ready.GetZone())
		}
		if _, err := nx.Source().Patch(ctx, patch.Build()); err != nil {
			return err
		}
		b, err = nx.CameraBinding().Patch(ctx, api.CameraBindingPatchRequest_builder{Ref: api.CameraBindingRef_builder{Id: b.GetId()}.Build(), Desired: ready, Observed: r.GetObserved(), Collect: z.Ptr(true), DateUpdated: b.GetDateUpdated()}.Build())
		if err != nil {
			return err
		}
		j, err = nx.CameraJob().Patch(ctx, api.CameraJobPatchRequest_builder{Ref: r.GetRef(), Stage: z.Ptr(api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY), Observed: r.GetObserved(), PublishedRevision: z.Ptr(ready.GetRevision()), DatePublished: timestamppb.New(c.d.now()), LastOperationKey: z.Ptr(r.GetOperationKey()), LastOperationHash: hash, DateUpdated: j.GetDateUpdated()}.Build())
		if err != nil {
			return err
		}
		a, err = c.cameraCatalogAdvance(ctx, a)
		if err != nil {
			return err
		}
		out = api.CameraJobPublishResponse_builder{Job: j, Binding: b, CatalogRevision: a.GetCatalogRevision()}.Build()
		return nil
	})
	if err != nil {
		return nil, cameraConflict(err)
	}
	return out, nil
}

// A replayed Claim is a current execution grant, not a historical write ack.
func (s Core) cameraClaimReplay(j *api.CameraJob, a *api.CameraAssignment, d *api.CameraDevice, who pdid.Id, key string, hash []byte) (bool, error) {
	replay, err := cameraReplay(j, who, key, hash)
	if err != nil || !replay {
		return replay, err
	}
	if err := s.cameraLease(j, a, d, who, j.GetLeaseGeneration(), j.GetAssignmentGeneration()); err != nil {
		return false, err
	}
	return true, nil
}
func cameraOtherJob(active *api.CameraJob, want []byte) error {
	if !bytes.Equal(active.GetId(), want) && !cameraTerminalStage(active.GetStage()) {
		return failed("physical camera has an unsettled job; resume or explicitly settle that job before another intent")
	}
	return nil
}
