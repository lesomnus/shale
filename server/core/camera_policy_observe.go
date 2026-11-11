package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"uuid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	presetwire "github.com/lesomnus/shale/internal/camerapreset"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/cameraassignment"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"github.com/lesomnus/shale/internal/ent/camerajob"
	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s Core) cameraObservationAssignment(ctx context.Context, ref *api.CameraAssignmentRef, generation int64) (*api.CameraAssignment, *api.CameraNetwork, error) {
	if _, err := s.cameraWorkerActor(ctx); err != nil {
		return nil, nil, err
	}
	a, err := s.cameraAssignmentRow(ctx, ref)
	if err != nil {
		return nil, nil, err
	}
	if generation != a.GetGeneration() {
		return nil, nil, status.Error(codes.Aborted, "camera assignment generation changed")
	}
	n, err := s.Next().CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: a.GetNetwork().GetId()}.Build()}.Build())
	return a, n, err
}

func cameraObservationIntent(d *api.CameraDevice) string {
	uris := make([]string, len(d.GetEndpoints()))
	for i, ep := range d.GetEndpoints() {
		uris[i] = ep.GetUri()
	}
	slices.Sort(uris)
	digest := sha256.Sum256([]byte(strings.Join(uris, "\x00")))
	return "observe/" + mustId(d.GetId()).String() + "/" + hex.EncodeToString(digest[:])
}

func (s Core) cameraObserveDevice(ctx context.Context, n *api.CameraNetwork, r *api.CameraPolicyObserveRequest) (*api.CameraDevice, error) {
	if len(r.GetEndpoints()) == 0 {
		return nil, invalid("endpoints", "a discovered camera needs an explicit endpoint")
	}
	if err := s.cameraLockNetwork(ctx, n.GetId()); err != nil {
		return nil, err
	}
	var err error
	n, err = s.Next().CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: n.GetId()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if err := cameraEndpoints(r.GetEndpoints(), n.GetCidrs()); err != nil {
		return nil, err
	}
	if err := cameraPresetIdentity(r.GetIdentity()); err != nil {
		return nil, err
	}
	endpoints := make([]*api.CameraObservedEndpoint, len(r.GetEndpoints()))
	for i, ep := range r.GetEndpoints() {
		endpoints[i] = proto.Clone(ep).(*api.CameraObservedEndpoint)
		endpoints[i].SetUri(camera.SanitizeURI(ep.GetUri()))
	}
	identity := &api.CameraIdentityEvidence{}
	if r.GetIdentity() != nil {
		identity = proto.Clone(r.GetIdentity()).(*api.CameraIdentityEvidence)
	}
	// Discovery hints never promote authentication or replace authenticated
	// identity. Device operations journal authenticated inspection separately.
	identity.SetAuthenticated(false)
	observation := api.CameraEnrollmentObservation_builder{Code: "observed", ObservedAt: timestamppb.New(s.d.now()), DiscoveryId: r.GetDiscoveryId()}.Build()
	key, keys, mac, err := cameraObservationKeys(r)
	if err != nil {
		return nil, err
	}
	rows, err := s.cameraObservationInventory(ctx, n, r, keys, mac)
	if err != nil {
		return nil, err
	}
	if len(rows) > 1 {
		return nil, status.Error(codes.Aborted, "camera observations identify conflicting inventory rows")
	}
	var out *api.CameraDevice
	if len(rows) == 0 {
		out, err = s.Next().CameraDevice().Add(ctx, api.CameraDeviceAddRequest_builder{Tenant: api.TenantRef_builder{Id: n.GetTenant().GetId()}.Build(), Site: cameraSite(n.GetSite()), Network: api.CameraNetworkRef_builder{Id: n.GetId()}.Build(), DeviceKey: key, Identity: identity, Endpoints: endpoints, ObservationRevision: 1, Enrollment: observation}.Build())
	} else {
		row := rows[0]
		if err := s.cameraLockDevice(ctx, row.Id[:]); err != nil {
			return nil, err
		}
		current, readErr := s.Next().CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: row.Id[:]}.Build()}.Build())
		if readErr != nil {
			return nil, readErr
		}
		if err := cameraObservationReconcile(current, r); err != nil {
			return nil, err
		}
		observation.SetDiscoveryId(cameraObservationDiscoveryID(current, r.GetDiscoveryId()))
		if current.GetIdentity().GetAuthenticated() {
			identity = current.GetIdentity()
		} else if len(identity.GetMacs()) == 0 {
			identity.SetMacs(append([]string(nil), current.GetIdentity().GetMacs()...))
		}
		out, err = s.Next().CameraDevice().Patch(ctx, api.CameraDevicePatchRequest_builder{Ref: api.CameraDeviceRef_builder{Id: current.GetId()}.Build(), Identity: identity, Endpoints: endpoints, ObservationRevision: z.Ptr(current.GetObservationRevision() + 1), Enrollment: observation, DateUpdated: current.GetDateUpdated()}.Build())
	}
	if err != nil {
		return nil, cameraConflict(err)
	}
	return s.Next().CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: out.GetId()}.Build()}.Build())
}

func cameraPolicyEligible(ctx context.Context, p *api.CameraPolicy, a *api.CameraAssignment, d *api.CameraDevice) (bool, error) {
	if p.GetEnrollment().GetAllowAny() {
		return true, nil
	}
	rules := make([]*api.CameraPresetRule, len(p.GetEnrollment().GetEligible()))
	for i, match := range p.GetEnrollment().GetEligible() {
		rules[i] = api.CameraPresetRule_builder{Id: "eligible-" + strconv.Itoa(i), Scope: api.CameraPresetScope_builder{TenantId: mustId(a.GetTenant().GetId()).String(), NetworkId: mustId(a.GetNetwork().GetId()).String()}.Build(), Match: match}.Build()
	}
	provider, err := presetwire.Provider("enrollment", "1", rules)
	if err != nil {
		return false, failed("stored enrollment selectors are invalid")
	}
	_, err = provider.Resolve(ctx, cameraPresetEnrollmentScope(a), cameraPresetDevice(d, d.GetIdentity()))
	if errors.Is(err, camera.ErrPresetNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (s Core) cameraPolicyDeviceLimit(ctx context.Context, a *api.CameraAssignment, limit int32) error {
	if limit < 1 {
		return status.Error(codes.ResourceExhausted, "automatic enrollment device limit is disabled")
	}
	var ids []uuid.UUID
	err := s.ent(ctx).CameraBinding.Query().Where(camerabinding.DateErasedIsNil(), camerabinding.HasAssignmentWith(cameraassignment.NetworkIdEQ(mustId(a.GetNetwork().GetId()).Uuid()), cameraassignment.DateErasedIsNil())).Unique(true).Limit(1025).Select(camerabinding.FieldDeviceId).Scan(ctx, &ids)
	if err != nil {
		return err
	}
	if len(ids) >= int(limit) {
		return status.Error(codes.ResourceExhausted, "automatic enrollment device limit reached")
	}
	return nil
}

func cameraSameCameraAddress(input string, endpoints []*api.CameraObservedEndpoint) bool {
	u, err := url.Parse(input)
	if err != nil || u.Hostname() == "" {
		return false
	}
	for _, ep := range endpoints {
		v, err := url.Parse(ep.GetUri())
		if err == nil && v.Hostname() == u.Hostname() {
			return true
		}
	}
	return false
}

func (s Core) cameraEnrollObservation(ctx context.Context, a *api.CameraAssignment, n *api.CameraNetwork, d *api.CameraDevice) (*api.CameraBinding, *api.CameraJob, string, error) {
	if len(n.GetActivePolicyId()) == 0 {
		return nil, nil, "policy_missing", nil
	}
	p, err := s.Next().CameraPolicy().Get(ctx, api.CameraPolicyGetRequest_builder{Ref: api.CameraPolicyRef_builder{Id: n.GetActivePolicyId()}.Build()}.Build())
	if err != nil {
		return nil, nil, "", err
	}
	if !bytes.Equal(p.GetNetwork().GetId(), n.GetId()) || cameraSameScope(p.GetTenant().GetId(), p.GetSite().GetId(), a.GetTenant().GetId(), a.GetSite().GetId()) != nil {
		return nil, nil, "", denied("enrollment", "active policy belongs to another camera scope")
	}
	if !p.GetEnrollment().GetAutomatic() {
		return nil, nil, "policy_manual", nil
	}
	eligible, err := cameraPolicyEligible(ctx, p, a, d)
	if err != nil || !eligible {
		return nil, nil, "policy_not_matched", err
	}
	key := "auto/" + mustId(d.GetId()).String()
	bindings, err := s.ent(ctx).CameraBinding.Query().Where(camerabinding.TenantIdEQ(mustId(a.GetTenant().GetId()).Uuid()), camerabinding.DateErasedIsNil(), camerabinding.Or(camerabinding.LogicalKeyEQ(key), camerabinding.DeviceIdEQ(mustId(d.GetId()).Uuid()))).Limit(3).All(ctx)
	if err != nil {
		return nil, nil, "", err
	}
	if len(bindings) > 1 {
		return nil, nil, "", status.Error(codes.Aborted, "camera has conflicting logical bindings; operator reconciliation required")
	}
	var b *api.CameraBinding
	if len(bindings) == 0 {
		if p.GetEnrollment().GetTrustNewFactoryTls() && cameraDeviceTLSIdentity(p, d) == nil {
			if err := s.cameraPlanFactoryTLS(ctx, a, n, d, p); err != nil {
				return nil, nil, "", err
			}
			return nil, nil, "factory_tls_required", nil
		}
		if err := s.cameraPolicyDeviceLimit(ctx, a, p.GetEnrollment().GetMaxDevices()); err != nil {
			return nil, nil, "", err
		}
		b, err = (coreCameraBinding{s, s.Next().CameraBinding()}).bind(ctx, api.CameraBindingBindRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), Device: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), LogicalKey: key}.Build())
	} else {
		row := bindings[0]
		if row.AssignmentId != mustId(a.GetId()).Uuid() || row.DeviceId != mustId(d.GetId()).Uuid() {
			return nil, nil, "", denied("enrollment", "observed device is already assigned to another Producer")
		}
		b, err = s.Next().CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: row.Id[:]}.Build()}.Build())
	}
	if err != nil {
		return nil, nil, "", err
	}
	unfinished, err := s.ent(ctx).CameraJob.Query().Where(camerajob.BindingIdEQ(mustId(b.GetId()).Uuid()), camerajob.DateErasedIsNil(), camerajob.StageNotIn(int32(api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE), int32(api.CameraJobStage_CAMERA_JOB_STAGE_FAILED), int32(api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED))).Order(ent.Asc(camerajob.FieldId)).First(ctx)
	if err == nil {
		job, err := s.Next().CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: unfinished.Id[:]}.Build()}.Build())
		return b, job, "reconciliation_pending", err
	}
	if !ent.IsNotFound(err) {
		return nil, nil, "", err
	}
	latest, err := s.ent(ctx).CameraJob.Query().Where(camerajob.BindingIdEQ(mustId(b.GetId()).Uuid()), camerajob.DateErasedIsNil()).Order(ent.Desc(camerajob.FieldId)).First(ctx)
	var reuse *api.CameraJobInputs
	activation := p.GetEnrollment().GetAllowActivation()
	code := "enrolled"
	if err == nil {
		job, err := s.Next().CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: latest.Id[:]}.Build()}.Build())
		if err != nil {
			return nil, nil, "", err
		}
		if job.GetRequestKey() == cameraObservationIntent(d) || cameraSameCameraAddress(b.GetDesired().GetInput(), d.GetEndpoints()) {
			code := "already_enrolled"
			if job.GetStage() == api.CameraJobStage_CAMERA_JOB_STAGE_FAILED {
				code = "enrollment_failed"
			}
			if job.GetStage() == api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED {
				code = "enrollment_canceled"
			}
			return b, job, code, nil
		}
		if job.GetStage() == api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED || b.GetDesired() == nil {
			return b, job, "manual_reapply_required", nil
		}
		if job.GetInputs().GetPolicy() == nil {
			return b, job, "manual_reapply_required", nil
		}
		id, err := cameraPresetID(job.GetInputs().GetPolicy().GetId(), DomCameraPolicy)
		if err != nil {
			return nil, nil, "", err
		}
		p, err = s.Next().CameraPolicy().Get(ctx, api.CameraPolicyGetRequest_builder{Ref: api.CameraPolicyRef_builder{Id: id}.Build()}.Build())
		if err != nil {
			return nil, nil, "", err
		}
		// Address recovery retains the old pinned policy/credential versions.
		// It never turns an existing binding into first-time activation based on
		// an unauthenticated discovery identity or an externally reset device.
		reuse, activation, code = job.GetInputs(), false, "transport_reconcile"
	} else if !ent.IsNotFound(err) {
		return nil, nil, "", err
	}
	job, err := s.cameraEnrollJob(ctx, a, b, p, cameraObservationIntent(d), activation, reuse)
	return b, job, code, err
}

func (s coreCameraPolicy) Observe(ctx context.Context, r *api.CameraPolicyObserveRequest) (*api.CameraPolicyObserveResponse, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if err := cameraToken("discovery_id", r.GetDiscoveryId()); err != nil {
		return nil, err
	}
	if _, err := s.cameraWorkerActor(ctx); err != nil {
		return nil, err
	}
	var d *api.CameraDevice
	// Observations remain visible if the separate atomic enrollment transaction
	// must wait for credentials. No partial binding/Source/secret/job is committed.
	err := s.cameraExecutionTx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		_, n, err := c.cameraObservationAssignment(ctx, r.GetAssignment(), r.GetAssignmentGeneration())
		if err != nil {
			return err
		}
		d, err = c.cameraObserveDevice(ctx, n, r)
		return err
	})
	if err != nil {
		return nil, err
	}
	out := api.CameraPolicyObserveResponse_builder{Device: d}.Build()
	err = s.cameraExecutionTx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		a, n, err := c.cameraObservationAssignment(ctx, r.GetAssignment(), r.GetAssignmentGeneration())
		if err != nil {
			return err
		}
		current, err := nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if !bytes.Equal(current.GetNetwork().GetId(), n.GetId()) {
			return denied("observation", "device belongs to another camera network")
		}
		binding, job, code, err := c.cameraEnrollObservation(ctx, a, n, current)
		if err == nil {
			out.SetBinding(binding)
			out.SetJob(job)
			out.SetCode(code)
		}
		return err
	})
	switch {
	case errors.Is(err, errCameraEnrollmentUnconfigured):
		out.SetCode("credential_preparer_unavailable")
	case errors.Is(err, errCameraEnrollmentCredentials):
		out.SetCode("credentials_unavailable")
	case status.Code(err) == codes.ResourceExhausted:
		out.SetCode("capacity_exceeded")
	case err != nil:
		return nil, err
	}
	err = s.cameraExecutionTx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		a, _, err := c.cameraObservationAssignment(ctx, r.GetAssignment(), r.GetAssignmentGeneration())
		if err != nil {
			return err
		}
		current, err := nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if !bytes.Equal(current.GetNetwork().GetId(), a.GetNetwork().GetId()) {
			return denied("observation", "device belongs to another camera network")
		}
		_, err = nx.CameraDevice().Patch(ctx, api.CameraDevicePatchRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), Enrollment: api.CameraEnrollmentObservation_builder{Code: out.GetCode(), ObservedAt: timestamppb.New(s.d.now()), DiscoveryId: cameraObservationDiscoveryID(current, r.GetDiscoveryId())}.Build(), DateUpdated: current.GetDateUpdated()}.Build())
		if err != nil {
			return err
		}
		current, err = nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build()}.Build())
		out.SetDevice(current)
		if out.GetCode() == "factory_tls_required" {
			out.SetFactoryTlsPlan(current.GetFactoryTls().GetPlan())
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	if _, err := s.cameraWorkerActor(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
