package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"slices"
	"strconv"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameratransport"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"github.com/lesomnus/shale/internal/ent/cameradevice"
	"github.com/lesomnus/shale/internal/ent/camerajob"
	"github.com/lesomnus/shale/provision"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Transport candidates (docs/camera-worker.md, "Transport candidates"): a
// camera whose endpoints may only change through verification or an operator
// is observed elsewhere. The observation is staged beside the authorized
// endpoints, bounded and expiring, and never changes them by itself.
const (
	cameraTransportLifetime = 2 * time.Minute
	cameraTransportLimit    = 4

	cameraTransportPending  = cameratransport.StatePending
	cameraTransportAccepted = cameratransport.StateAccepted
	cameraTransportOperator = cameratransport.StateOperator

	// The fixed outcome codes. Every code but the first two leaves the move to
	// an operator's CameraDevice.Observe.
	cameraTransportRequired     = "verification_required"
	cameraTransportDone         = "accepted"
	cameraTransportManual       = "policy_manual"
	cameraTransportInsecureHTTP = "insecure_http"
	cameraTransportTrustMissing = "trust_missing"
	cameraTransportTrustSkip    = "tls_verification_skip"
	cameraTransportTrustDiffers = "trust_mismatch"
	cameraTransportNoIdentity   = "identity_missing"
	cameraTransportNoRecovery   = "recovery_unavailable"
	cameraTransportMutation     = "mutation_active"
	cameraTransportEvidence     = "evidence_conflict"
	cameraTransportAmbiguous    = "ambiguous"
	cameraTransportHostOwned    = "host_owned"
	cameraTransportKeyMismatch  = "key_mismatch"
	cameraTransportExpired      = "expired"
	cameraTransportTrustChanged = "trust_changed"
	cameraTransportGeneration   = "assignment_changed"
	cameraTransportJobChanged   = "job_changed"
	cameraTransportMoved        = "endpoints_changed"
)

// cameraTransportGuarded says whether d's endpoints may change only through a
// verified candidate or an operator: an authenticated identity, a job that
// ever held the device, or a published capture input that records now.
func (s Core) cameraTransportGuarded(ctx context.Context, d *api.CameraDevice) (bool, error) {
	if d.GetIdentity().GetAuthenticated() || len(d.GetActiveJobId()) > 0 {
		return true, nil
	}
	return s.ent(ctx).CameraBinding.Query().Where(camerabinding.DeviceIdEQ(mustId(d.GetId()).Uuid()), camerabinding.DateErasedIsNil(), camerabinding.DesiredNotNil()).Exist(ctx)
}

// cameraTransportTrust is what a recovery would reuse, or why none may run
// without an operator: the device's binding, its latest job's policy revision,
// and the key a job under that revision pins for this device now.
type cameraTransportTrust struct {
	binding *api.CameraBinding
	job     *api.CameraJob
	policy  *api.CameraPolicy
	pin     *api.CameraDeviceTLSIdentity
	port    int32
	code    string
}

func (s Core) cameraTransportTrust(ctx context.Context, a *api.CameraAssignment, n *api.CameraNetwork, d *api.CameraDevice) (cameraTransportTrust, error) {
	var out cameraTransportTrust
	nx := s.Next()
	if len(d.GetActiveJobId()) > 0 {
		active, err := nx.CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: d.GetActiveJobId()}.Build()}.Build())
		if err != nil {
			return out, err
		}
		if !cameraTerminalStage(active.GetStage()) {
			out.code = cameraTransportMutation
			return out, nil
		}
	}
	rows, err := s.ent(ctx).CameraBinding.Query().Where(camerabinding.DeviceIdEQ(mustId(d.GetId()).Uuid()), camerabinding.DateErasedIsNil()).Limit(2).All(ctx)
	if err != nil {
		return out, err
	}
	if len(rows) != 1 || rows[0].AssignmentId != mustId(a.GetId()).Uuid() {
		out.code = cameraTransportNoRecovery
		return out, nil
	}
	unfinished, err := s.ent(ctx).CameraJob.Query().Where(camerajob.BindingIdEQ(rows[0].Id), camerajob.DateErasedIsNil(), camerajob.StageNotIn(int32(api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE), int32(api.CameraJobStage_CAMERA_JOB_STAGE_FAILED), int32(api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED))).Exist(ctx)
	if err != nil {
		return out, err
	}
	if unfinished {
		out.code = cameraTransportMutation
		return out, nil
	}
	b, err := nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: rows[0].Id[:]}.Build()}.Build())
	if err != nil {
		return out, err
	}
	latest, err := s.ent(ctx).CameraJob.Query().Where(camerajob.BindingIdEQ(rows[0].Id), camerajob.DateErasedIsNil()).Order(ent.Desc(camerajob.FieldId)).First(ctx)
	if ent.IsNotFound(err) {
		out.code = cameraTransportNoRecovery
		return out, nil
	}
	if err != nil {
		return out, err
	}
	j, err := nx.CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: latest.Id[:]}.Build()}.Build())
	if err != nil {
		return out, err
	}
	// As the operator path's transport reconciliation: an existing published
	// binding whose last job pinned a policy revision, never a canceled one.
	if b.GetDesired() == nil || j.GetStage() == api.CameraJobStage_CAMERA_JOB_STAGE_CANCELED || j.GetInputs().GetPolicy() == nil {
		out.code = cameraTransportNoRecovery
		return out, nil
	}
	id, err := cameraPresetID(j.GetInputs().GetPolicy().GetId(), DomCameraPolicy)
	if err != nil {
		out.code = cameraTransportNoRecovery
		return out, nil
	}
	p, err := nx.CameraPolicy().Get(ctx, api.CameraPolicyGetRequest_builder{Ref: api.CameraPolicyRef_builder{Id: id}.Build()}.Build())
	if err != nil {
		return out, err
	}
	if !bytes.Equal(p.GetNetwork().GetId(), n.GetId()) || p.GetRevision() != j.GetInputs().GetPolicy().GetRevision() {
		out.code = cameraTransportNoRecovery
		return out, nil
	}
	preset, err := nx.CameraPreset().Get(ctx, api.CameraPresetGetRequest_builder{Ref: api.CameraPresetRef_builder{Id: p.GetPreset().GetId()}.Build()}.Build())
	if err != nil {
		return out, err
	}
	// The recovery is a job under the same revision, preset and transport as
	// the last one: one that cannot be reproduced is not recovered by itself.
	want := cameraEnrollmentInputs(p, preset, false, d)
	if !proto.Equal(want.GetPolicy(), j.GetInputs().GetPolicy()) || !proto.Equal(want.GetPreset(), j.GetInputs().GetPreset()) || !proto.Equal(want.GetBootstrap(), j.GetInputs().GetBootstrap()) || want.GetTlsVerification() != j.GetInputs().GetTlsVerification() || j.GetInputs().GetManagementCredential() == nil || j.GetInputs().GetStreamingCredential() == nil {
		out.code = cameraTransportNoRecovery
		return out, nil
	}
	out.binding, out.job, out.policy = b, j, p
	var active *api.CameraPolicy
	if len(n.GetActivePolicyId()) > 0 {
		active, err = nx.CameraPolicy().Get(ctx, api.CameraPolicyGetRequest_builder{Ref: api.CameraPolicyRef_builder{Id: n.GetActivePolicyId()}.Build()}.Build())
		if err != nil {
			return out, err
		}
	}
	if p.GetEnrollment().GetTlsVerification() == camera.TLSVerificationSkip || active.GetEnrollment().GetTlsVerification() == camera.TLSVerificationSkip {
		// No key is checked, so there is none to verify a new host by.
		out.code = cameraTransportTrustSkip
		return out, nil
	}
	out.pin = cameraDeviceTLSIdentity(p, d)
	bootstrap := p.GetEnrollment().GetBootstrap()
	if out.pin == nil {
		// Ordinary PKI names no key of this device: "a certificate from the
		// same CA" is not this camera. Those, and HTTP, stay with an operator.
		out.code = cameraTransportTrustMissing
		if bootstrap.GetWebScheme() != "https" {
			out.code = cameraTransportInsecureHTTP
		}
		return out, nil
	}
	if bootstrap.GetWebScheme() != "https" || bootstrap.GetWebPort() < 1 || bootstrap.GetWebPort() > 65535 || out.pin.GetDeviceId() != mustId(d.GetId()).String() {
		out.code = cameraTransportInsecureHTTP
		return out, nil
	}
	out.port = bootstrap.GetWebPort()
	if active == nil || !active.GetEnrollment().GetAutomatic() {
		// Discovery starts no job on a network the operator enrolls by hand.
		out.code = cameraTransportManual
		return out, nil
	}
	if !proto.Equal(cameraDeviceTLSIdentity(active, d), out.pin) {
		// The operator's current approval and the device's last job disagree.
		out.code = cameraTransportTrustDiffers
		return out, nil
	}
	if i := d.GetIdentity(); !i.GetAuthenticated() || i.GetSerial() == "" || !slices.ContainsFunc(i.GetMacs(), func(v string) bool { return cameraObservationMAC(v) != "" }) {
		// A recovery must reinspect the same serial and MAC before any write.
		out.code = cameraTransportNoIdentity
	}
	return out, nil
}

// cameraTransportOrigin is the one HTTPS web origin a candidate's endpoints
// name, on the pinned bootstrap port; several hosts are ambiguous.
func cameraTransportOrigin(eps []*api.CameraObservedEndpoint, port int32) (string, bool) {
	host := ""
	for _, ep := range eps {
		h := cameraObservedHost(ep.GetUri())
		if h == "" || host != "" && h != host {
			return "", false
		}
		host = h
	}
	if host == "" || port < 1 || port > 65535 {
		return "", false
	}
	return "https://" + net.JoinHostPort(host, strconv.Itoa(int(port))), true
}

// cameraTransportHostOwned says whether another device in the network already
// owns a candidate host, in any spelling, port or path.
func (s Core) cameraTransportHostOwned(ctx context.Context, n *api.CameraNetwork, d *api.CameraDevice, eps []*api.CameraObservedEndpoint) (bool, error) {
	rows, err := s.ent(ctx).CameraDevice.Query().Where(cameradevice.NetworkIdEQ(mustId(n.GetId()).Uuid()), cameradevice.DateErasedIsNil(), cameradevice.IdNEQ(mustId(d.GetId()).Uuid())).Select(cameradevice.FieldId, cameradevice.FieldEndpoints).Limit(cameraObservationLookupLimit + 1).All(ctx)
	if err != nil {
		return false, err
	}
	if len(rows) >= cameraObservationLookupLimit {
		return false, status.Error(codes.ResourceExhausted, "camera inventory lookup requires explicit operator enrollment or a smaller network")
	}
	for _, row := range rows {
		for _, old := range row.Endpoints {
			for _, ep := range eps {
				if h := cameraObservedHost(old.GetUri()); h != "" && h == cameraObservedHost(ep.GetUri()) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func cameraTransportLive(c *api.CameraTransportCandidate, now time.Time) bool {
	switch c.GetState() {
	case cameraTransportPending, cameraTransportOperator:
	default:
		return false
	}
	return c.GetDateExpires() != nil && now.Before(c.GetDateExpires().AsTime())
}

func cameraTransportDecide(c *api.CameraTransportCandidate, code string, now time.Time) {
	c.SetState(cameraTransportOperator)
	c.SetCode(code)
	c.SetDateDecided(timestamppb.New(now))
}

// cameraTransportMaintain settles what time and today's device made stale:
// an expired candidate, or one from another assignment generation or
// authorized endpoint revision, can no longer be verified.
func cameraTransportMaintain(state *api.CameraTransportState, a *api.CameraAssignment, d *api.CameraDevice, now time.Time) {
	for _, c := range state.GetCandidates() {
		if c.GetState() != cameraTransportPending {
			continue
		}
		switch {
		case !cameraTransportLive(c, now):
			cameraTransportDecide(c, cameraTransportExpired, now)
		case c.GetAssignmentGeneration() != a.GetGeneration():
			cameraTransportDecide(c, cameraTransportGeneration, now)
		case c.GetObservationRevision() != d.GetObservationRevision():
			cameraTransportDecide(c, cameraTransportMoved, now)
		}
	}
}

// cameraTransportAmbiguity: two live candidates of the same authorized
// endpoints name different endpoint sets, so neither is this camera alone.
func cameraTransportAmbiguity(state *api.CameraTransportState, now time.Time) {
	sets := map[string]bool{}
	var live []*api.CameraTransportCandidate
	for _, c := range state.GetCandidates() {
		if cameraTransportLive(c, now) {
			live = append(live, c)
			sets[cameraTransportSetKey(c.GetEndpoints())] = true
		}
	}
	if len(sets) < 2 {
		return
	}
	for _, c := range live {
		if c.GetState() == cameraTransportPending {
			cameraTransportDecide(c, cameraTransportAmbiguous, now)
		}
	}
}

func cameraTransportSetKey(eps []*api.CameraObservedEndpoint) string {
	set := cameratransport.EndpointSet(eps)
	out := ""
	for _, v := range set {
		out += v + "\x00"
	}
	return out
}

// cameraTransportBound keeps the newest candidates, dropping settled ones
// before live ones.
func cameraTransportBound(state *api.CameraTransportState, now time.Time) {
	cs := state.GetCandidates()
	for len(cs) > cameraTransportLimit {
		drop := 0
		for i, c := range cs {
			if !cameraTransportLive(c, now) {
				drop = i
				break
			}
		}
		cs = slices.Delete(cs, drop, drop+1)
	}
	state.SetCandidates(cs)
}

// cameraTransportFind is the candidate this observation repeats: the same
// endpoint set, still live, from today's generation and endpoint revision.
func cameraTransportFind(state *api.CameraTransportState, a *api.CameraAssignment, d *api.CameraDevice, eps []*api.CameraObservedEndpoint, now time.Time) *api.CameraTransportCandidate {
	key := cameraTransportSetKey(eps)
	for _, c := range state.GetCandidates() {
		if cameraTransportLive(c, now) && c.GetAssignmentGeneration() == a.GetGeneration() && c.GetObservationRevision() == d.GetObservationRevision() && cameraTransportSetKey(c.GetEndpoints()) == key {
			return c
		}
	}
	return nil
}

// cameraTransportReason is why a candidate cannot be verified automatically,
// in a fixed order; empty when it can, with its grant's origin.
func cameraTransportReason(conflict, owned bool, trust cameraTransportTrust, eps []*api.CameraObservedEndpoint) (string, string) {
	switch {
	case conflict:
		return cameraTransportEvidence, ""
	case owned:
		return cameraTransportHostOwned, ""
	case trust.code != "":
		return trust.code, ""
	}
	origin, ok := cameraTransportOrigin(eps, trust.port)
	if !ok {
		return cameraTransportAmbiguous, ""
	}
	return "", origin
}

// cameraTransportObserve stages one moved-camera observation on d, which the
// caller locked after its network. It writes only d's transport state: the
// endpoints, identity and observation revision stay as they were.
func (s Core) cameraTransportObserve(ctx context.Context, a *api.CameraAssignment, n *api.CameraNetwork, d *api.CameraDevice, r *api.CameraPolicyObserveRequest, eps []*api.CameraObservedEndpoint, conflict, owned bool) (*api.CameraDevice, *api.CameraTransportCandidate, error) {
	now := s.d.now()
	state := &api.CameraTransportState{}
	if d.GetTransport() != nil {
		state = proto.Clone(d.GetTransport()).(*api.CameraTransportState)
	}
	cameraTransportMaintain(state, a, d, now)
	trust, err := s.cameraTransportTrust(ctx, a, n, d)
	if err != nil {
		return nil, nil, err
	}
	reason, origin := cameraTransportReason(conflict, owned, trust, eps)
	c := cameraTransportFind(state, a, d, eps, now)
	if c == nil {
		key := make([]byte, 16)
		if _, err := rand.Read(key); err != nil {
			return nil, nil, status.Error(codes.Unavailable, "camera transport candidate is unavailable")
		}
		c = api.CameraTransportCandidate_builder{
			Id: hex.EncodeToString(key), Endpoints: eps, DiscoveryId: r.GetDiscoveryId(), Macs: slices.Clone(r.GetIdentity().GetMacs()),
			AssignmentGeneration: a.GetGeneration(), ObservationRevision: d.GetObservationRevision(),
			State: cameraTransportPending, Code: cameraTransportRequired,
			DateObserved: timestamppb.New(now), DateExpires: timestamppb.New(now.Add(cameraTransportLifetime)),
		}.Build()
		if reason == "" {
			c.SetGrant(api.CameraTransportGrant_builder{
				CandidateId: c.GetId(), AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(),
				DeviceId: d.GetId(), NetworkId: n.GetId(), NetworkVersion: n.GetDateUpdated(), ObservationRevision: d.GetObservationRevision(),
				JobId: trust.job.GetId(), Policy: proto.Clone(trust.job.GetInputs().GetPolicy()).(*api.CameraRevisionReference),
				Trust: proto.Clone(trust.pin).(*api.CameraDeviceTLSIdentity), Endpoint: origin,
				DateIssued: c.GetDateObserved(), DateExpires: c.GetDateExpires(),
			}.Build())
		} else {
			cameraTransportDecide(c, reason, now)
		}
		state.SetCandidates(append(state.GetCandidates(), c))
	} else if c.GetState() == cameraTransportPending {
		// A repeated observation reissues the same grant, unless what it was
		// bound to has changed since.
		g := c.GetGrant()
		switch {
		case reason != "":
			cameraTransportDecide(c, reason, now)
		case !bytes.Equal(g.GetJobId(), trust.job.GetId()):
			cameraTransportDecide(c, cameraTransportJobChanged, now)
		case !proto.Equal(g.GetTrust(), trust.pin) || !proto.Equal(g.GetPolicy(), trust.job.GetInputs().GetPolicy()) || !proto.Equal(g.GetNetworkVersion(), n.GetDateUpdated()):
			cameraTransportDecide(c, cameraTransportTrustChanged, now)
		case g.GetEndpoint() != origin:
			cameraTransportDecide(c, cameraTransportMoved, now)
		}
	}
	cameraTransportAmbiguity(state, now)
	cameraTransportBound(state, now)
	if _, err := s.Next().CameraDevice().Patch(ctx, api.CameraDevicePatchRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), Transport: state, DateUpdated: d.GetDateUpdated()}.Build()); err != nil {
		return nil, nil, err
	}
	out, err := s.Next().CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build()}.Build())
	if err != nil {
		return nil, nil, err
	}
	return out, cameraTransportCandidate(out, c.GetId()), nil
}

func cameraTransportCandidate(d *api.CameraDevice, id string) *api.CameraTransportCandidate {
	for _, c := range d.GetTransport().GetCandidates() {
		if c.GetId() == id {
			return c
		}
	}
	return nil
}

// cameraTransportCode is the observation code a candidate's outcome shows.
func cameraTransportCode(c *api.CameraTransportCandidate) string {
	return "transport_" + c.GetCode()
}

func cameraTransportGrantValid(g *api.CameraTransportGrant) error {
	if g == nil || !cameraKey.MatchString(g.GetCandidateId()) || len(g.GetCandidateId()) != 32 || g.GetTrust() == nil || cameraTLSIdentity(g.GetTrust()) != nil || g.GetDateIssued() == nil || g.GetDateExpires() == nil || g.GetDateIssued().CheckValid() != nil || g.GetDateExpires().CheckValid() != nil {
		return invalid("grant", "an exact stored transport grant is required")
	}
	for _, raw := range [][]byte{g.GetAssignmentId(), g.GetDeviceId(), g.GetNetworkId(), g.GetJobId()} {
		if _, err := idOf(raw); err != nil {
			return invalid("grant", "an exact stored transport grant is required")
		}
	}
	return nil
}

// VerifyTransport is the assigned Worker's report of a credential-free TLS
// handshake with one transport candidate. It names the exact stored grant and
// the key the candidate presented. In one transaction the CP rechecks the
// grant against today's assignment generation, network version, device
// endpoints and job, the device's approved key, the other live candidates and
// host ownership; any difference settles the candidate with a fixed code for
// an operator. Otherwise the same transaction moves the device's endpoints and
// enrolls its recovery job. A replay of a decided candidate returns its outcome.
func (s coreCameraPolicy) VerifyTransport(ctx context.Context, r *api.CameraPolicyVerifyTransportRequest) (*api.CameraPolicyObserveResponse, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	g := r.GetGrant()
	if err := cameraTransportGrantValid(g); err != nil {
		return nil, err
	}
	if !cameraSPKISHA256.MatchString(r.GetSpkiSha256()) {
		return nil, invalid("spki_sha256", "an explicit canonical SHA256 SPKI digest is required")
	}
	if _, err := s.cameraWorkerActor(ctx); err != nil {
		return nil, err
	}
	var out *api.CameraPolicyObserveResponse
	err := s.cameraExecutionTx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		// Locks the network, then the assignment, then the device.
		a, err := c.cameraAssignmentRow(ctx, api.CameraAssignmentRef_builder{Id: g.GetAssignmentId()}.Build())
		if err != nil {
			return err
		}
		if !bytes.Equal(a.GetNetwork().GetId(), g.GetNetworkId()) {
			return denied("transport", "grant belongs to another camera network")
		}
		n, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: g.GetNetworkId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := c.cameraLockDevice(ctx, g.GetDeviceId()); err != nil {
			return err
		}
		d, err := nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: g.GetDeviceId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if !bytes.Equal(d.GetNetwork().GetId(), n.GetId()) || cameraSameScope(d.GetTenant().GetId(), d.GetSite().GetId(), a.GetTenant().GetId(), a.GetSite().GetId()) != nil {
			return denied("transport", "device belongs to another assigned scope")
		}
		state := proto.Clone(d.GetTransport()).(*api.CameraTransportState)
		candidate := cameraTransportCandidate(api.CameraDevice_builder{Transport: state}.Build(), g.GetCandidateId())
		if candidate == nil || !proto.Equal(candidate.GetGrant(), g) {
			return status.Error(codes.Aborted, "camera transport grant changed")
		}
		if candidate.GetState() != cameraTransportPending {
			// A lost response is answered with the decided outcome. The same
			// grant cannot be decided again with another key.
			if candidate.GetPresentedSpkiSha256() != "" && candidate.GetPresentedSpkiSha256() != r.GetSpkiSha256() {
				return status.Error(codes.Aborted, "camera transport candidate was already decided")
			}
			out = api.CameraPolicyObserveResponse_builder{Device: d, Code: cameraTransportCode(candidate)}.Build()
			if job := candidate.GetJobId(); len(job) > 0 {
				j, err := nx.CameraJob().Get(ctx, api.CameraJobGetRequest_builder{Ref: api.CameraJobRef_builder{Id: job}.Build()}.Build())
				if err != nil {
					return err
				}
				b, err := nx.CameraBinding().Get(ctx, api.CameraBindingGetRequest_builder{Ref: api.CameraBindingRef_builder{Id: j.GetBinding().GetId()}.Build()}.Build())
				if err != nil {
					return err
				}
				out.SetJob(j)
				out.SetBinding(b)
			}
			return nil
		}
		now := s.d.now()
		decide := func(code string) {
			cameraTransportDecide(candidate, code, now)
		}
		candidate.SetPresentedSpkiSha256(r.GetSpkiSha256())
		switch {
		case !now.Before(g.GetDateExpires().AsTime()) || g.GetDateIssued().AsTime().After(now):
			decide(cameraTransportExpired)
		case r.GetSpkiSha256() != g.GetTrust().GetSpkiSha256():
			// Another key at the new address: a copied EPR or MAC is not this
			// camera. Nothing was sent to it but a TLS handshake.
			decide(cameraTransportKeyMismatch)
		case a.GetGeneration() != g.GetAssignmentGeneration():
			decide(cameraTransportGeneration)
		case !proto.Equal(n.GetDateUpdated(), g.GetNetworkVersion()):
			decide(cameraTransportTrustChanged)
		case d.GetObservationRevision() != g.GetObservationRevision():
			decide(cameraTransportMoved)
		}
		var trust cameraTransportTrust
		if candidate.GetState() == cameraTransportPending {
			var code string
			trust, code, err = c.cameraTransportRecheck(ctx, a, n, d, state, candidate, now)
			if err != nil {
				return err
			}
			if code != "" {
				decide(code)
			}
		}
		out = api.CameraPolicyObserveResponse_builder{}.Build()
		patch := api.CameraDevicePatchRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), Transport: state}.Build()
		if candidate.GetState() == cameraTransportPending {
			// Verified and current: the recovery job and the move commit
			// together, or neither does.
			j, err := c.cameraTransportRecover(ctx, a, d, trust, candidate)
			if err != nil {
				return err
			}
			candidate.SetState(cameraTransportAccepted)
			candidate.SetCode(cameraTransportDone)
			candidate.SetDateDecided(timestamppb.New(now))
			candidate.SetJobId(j.GetId())
			patch.SetEndpoints(candidate.GetEndpoints())
			patch.SetObservationRevision(d.GetObservationRevision() + 1)
			out.SetJob(j)
			out.SetBinding(trust.binding)
			// The job may have read the device; take today's version.
			d, err = nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build()}.Build())
			if err != nil {
				return err
			}
		}
		// The device's observation code shows the outcome, as Observe's would.
		patch.SetEnrollment(api.CameraEnrollmentObservation_builder{Code: cameraTransportCode(candidate), ObservedAt: timestamppb.New(now), DiscoveryId: d.GetEnrollment().GetDiscoveryId()}.Build())
		patch.SetDateUpdated(d.GetDateUpdated())
		if _, err := nx.CameraDevice().Patch(ctx, patch); err != nil {
			return err
		}
		d, err = nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		out.SetDevice(d)
		out.SetCode(cameraTransportCode(candidate))
		return nil
	})
	if err != nil {
		return nil, cameraConflict(err)
	}
	if _, err := s.cameraWorkerActor(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// cameraTransportRecheck is today's answer to everything the grant was bound
// to beyond its own fields: the device's job and approved key, the other
// candidates and who owns the host. Empty when the candidate still stands.
func (s Core) cameraTransportRecheck(ctx context.Context, a *api.CameraAssignment, n *api.CameraNetwork, d *api.CameraDevice, state *api.CameraTransportState, c *api.CameraTransportCandidate, now time.Time) (cameraTransportTrust, string, error) {
	g := c.GetGrant()
	trust, err := s.cameraTransportTrust(ctx, a, n, d)
	if err != nil {
		return trust, "", err
	}
	switch {
	case trust.code == cameraTransportMutation:
		return trust, cameraTransportMutation, nil
	case trust.job == nil || !bytes.Equal(trust.job.GetId(), g.GetJobId()):
		return trust, cameraTransportJobChanged, nil
	case trust.code != "" || !proto.Equal(trust.pin, g.GetTrust()) || !proto.Equal(trust.job.GetInputs().GetPolicy(), g.GetPolicy()):
		return trust, cameraTransportTrustChanged, nil
	}
	if origin, ok := cameraTransportOrigin(c.GetEndpoints(), trust.port); !ok || origin != g.GetEndpoint() {
		return trust, cameraTransportMoved, nil
	}
	for _, other := range state.GetCandidates() {
		if other.GetId() != c.GetId() && cameraTransportLive(other, now) && cameraTransportSetKey(other.GetEndpoints()) != cameraTransportSetKey(c.GetEndpoints()) {
			return trust, cameraTransportAmbiguous, nil
		}
	}
	owned, err := s.cameraTransportHostOwned(ctx, n, d, c.GetEndpoints())
	if err != nil {
		return trust, "", err
	}
	if owned {
		return trust, cameraTransportHostOwned, nil
	}
	if err := cameraEndpoints(c.GetEndpoints(), n.GetCidrs()); err != nil {
		return trust, cameraTransportMoved, nil
	}
	return trust, "", nil
}

// cameraTransportRecover enrolls the recovery job of an accepted candidate:
// the same binding, the last job's policy and preset revisions, its exact
// management and streaming credential versions and the verified key, with no
// initial credential and no activation. The job reinspects the recorded serial
// and MAC before any write (cameraProgressTransport).
func (s Core) cameraTransportRecover(ctx context.Context, a *api.CameraAssignment, d *api.CameraDevice, trust cameraTransportTrust, c *api.CameraTransportCandidate) (*api.CameraJob, error) {
	last := trust.job.GetInputs()
	j, err := s.cameraEnrollJob(ctx, a, trust.binding, trust.policy, "transport/"+c.GetId(), false, last)
	if err != nil {
		return nil, err
	}
	in := j.GetInputs()
	if !bytes.Equal(j.GetBinding().GetId(), trust.binding.GetId()) || !proto.Equal(in.GetDeviceTlsIdentity(), c.GetGrant().GetTrust()) || in.GetAllowActivation() || in.GetInitialCredential() != nil ||
		!proto.Equal(in.GetPolicy(), last.GetPolicy()) || !proto.Equal(in.GetPreset(), last.GetPreset()) || !proto.Equal(in.GetBootstrap(), last.GetBootstrap()) ||
		!proto.Equal(in.GetManagementCredential(), last.GetManagementCredential()) || !proto.Equal(in.GetStreamingCredential(), last.GetStreamingCredential()) || in.GetTlsVerification() != last.GetTlsVerification() {
		return nil, failed("a transport recovery job must keep the last job's pins and the verified key")
	}
	return j, nil
}

// cameraProgressTransport: a camera whose endpoints a verified candidate moved
// was recognized by its key only. Before any configuration write, firmware or
// publication, a job there must reinspect, authenticated, the recorded serial
// and one recorded MAC.
func cameraProgressTransport(run provision.Run, d *api.CameraDevice) error {
	if !cameratransport.Recovered(d) {
		return nil
	}
	if run.Inspection == nil {
		if run.Preset != nil || run.Effective != nil || run.Plan != nil || run.InitializationPlan != nil || run.Initialized != nil || run.Verified != nil || run.DeploymentProfile != nil || run.Firmware != nil || run.Binding.DeploymentID != "" {
			return failed("a moved camera must reinspect its recorded serial and MAC before any write")
		}
		return nil
	}
	if !cameratransport.SameRecorded(d.GetIdentity(), run.Inspection.Device) {
		return failed("a moved camera reported another serial or MAC than its recorded identity")
	}
	return nil
}
