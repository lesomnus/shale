package cameraworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameraprogress"
	"github.com/lesomnus/shale/provision"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// JobStore checkpoints the complete portable workflow in the leased CP job.
// Physical writer exclusion is the CP device/assignment fence, never a local
// file lock or a second independently expiring device lease.
type JobStore struct {
	Execution *Execution
	once      sync.Once
	lock      chan struct{}
}

var _ provision.Store = (*JobStore)(nil)

func jobRequest(w *api.CameraWorkerWork) provision.Request {
	a, b, j := w.GetAssignment(), w.GetBinding(), w.GetJob()
	scope := enrollmentScope(a)
	q := provision.Request{ID: cameraID(j.GetId()), BindingID: b.GetLogicalKey(), Scope: scope, Device: deviceHint(w), UsePreset: true, Alias: b.GetSourceAlias(), Provider: "cp", AllowActivation: j.GetInputs().GetAllowActivation(), ProbeDuration: 5 * time.Second}
	ref := func(v *api.CameraCredentialReference) *camera.CredentialRef {
		if v == nil {
			return nil
		}
		q.Provider = v.GetProvider()
		return &camera.CredentialRef{Provider: v.GetProvider(), ID: v.GetId(), Version: v.GetVersion(), Purpose: camera.AuthPurpose(v.GetPurpose()), Scope: scope, Recoverable: true}
	}
	q.InitialRef, q.ManagementRef, q.StreamingRef = ref(j.GetInputs().GetInitialCredential()), ref(j.GetInputs().GetManagementCredential()), ref(j.GetInputs().GetStreamingCredential())
	return q
}

func (s *JobStore) check(ctx context.Context) (*api.CameraWorkerWork, error) {
	if s == nil || s.Execution == nil {
		return nil, camera.ErrInvalid
	}
	return s.Execution.Check(ctx)
}

// Request returns this job's immutable intent with today's explicitly rebound
// delivery scope and endpoints. An existing checkpoint retains its inspected
// identity and exact credential versions.
func (s *JobStore) Request(ctx context.Context) (provision.Request, error) {
	w, err := s.check(ctx)
	if err != nil {
		return provision.Request{}, err
	}
	q := jobRequest(w)
	r, err := s.Load(ctx, q.ID)
	if err == nil {
		return r.Request, nil
	}
	if errors.Is(err, provision.ErrNotFound) {
		return q, nil
	}
	return provision.Request{}, err
}

func (s *JobStore) Lock(ctx context.Context, key string) (func() error, error) {
	w, err := s.check(ctx)
	if err != nil {
		return nil, err
	}
	q := jobRequest(w)
	if key != provision.Key(q.Scope.TenantID, string(q.Device.ID)) {
		return nil, camera.ErrForbidden
	}
	s.once.Do(func() { s.lock = make(chan struct{}, 1); s.lock <- struct{}{} })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.lock:
	}
	var once sync.Once
	return func() error { once.Do(func() { s.lock <- struct{}{} }); return nil }, nil
}

func (s *JobStore) Reserve(ctx context.Context, q provision.Request) error {
	w, err := s.check(ctx)
	if err != nil {
		return err
	}
	want := jobRequest(w)
	// The request's discovery hints and endpoints may be historical. They do
	// not authorize another inventory device or mutable policy/credential input.
	if q.ID != want.ID || q.BindingID != want.BindingID || q.Scope != want.Scope || q.Device.ID != want.Device.ID || q.Alias != want.Alias || q.Provider != want.Provider || q.AllowActivation != want.AllowActivation || !q.UsePreset || q.Desired != (camera.DesiredState{}) || q.AllowProducerRestart {
		return camera.ErrForbidden
	}
	return q.Validate()
}

func (s *JobStore) Identify(ctx context.Context, scope camera.EnrollmentScope, h camera.DeviceHint) error {
	w, err := s.check(ctx)
	if err != nil {
		return err
	}
	if scope != enrollmentScope(w.GetAssignment()) || h.ID != camera.DeviceID(cameraID(w.GetDevice().GetId())) {
		return camera.ErrForbidden
	}
	prior := w.GetDevice().GetIdentity()
	if prior.GetAuthenticated() && (prior.GetSerial() != "" && prior.GetSerial() != h.Serial || prior.GetHardware() != "" && prior.GetHardware() != h.Hardware) {
		return camera.ErrAmbiguousIdentity
	}
	return nil
}

func (s *JobStore) BeginMutation(ctx context.Context, q provision.Request) error {
	return s.Reserve(ctx, q)
}

func (s *JobStore) CompleteMutation(ctx context.Context, q provision.Request) error {
	// ACTIVE is terminal and cannot obtain another execution grant. A detached
	// local checkpoint is enough for this final no-I/O bookkeeping operation.
	w := s.Execution.Snapshot()
	if w.GetJob().GetStage() == api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE && cameraID(w.GetJob().GetId()) == q.ID {
		return nil
	}
	return s.Reserve(ctx, q)
}

func (s *JobStore) Load(ctx context.Context, id string) (provision.Run, error) {
	w, err := s.check(ctx)
	if err != nil {
		return provision.Run{}, err
	}
	if id != cameraID(w.GetJob().GetId()) {
		return provision.Run{}, camera.ErrForbidden
	}
	state := w.GetJob().GetObserved().GetProgress()
	if state == nil {
		return provision.Run{}, provision.ErrNotFound
	}
	r, err := cameraprogress.Decode(state)
	if err != nil {
		return provision.Run{}, err
	}
	// Immutable material versions survive replacement. Only the explicit
	// delivery scope and fresh transport endpoints follow today's assignment.
	q := jobRequest(w)
	r.Request.Scope = q.Scope
	r.Request.Device.Endpoints = q.Device.Endpoints
	for _, ref := range []*camera.CredentialRef{r.Request.InitialRef, r.Request.ManagementRef, r.Request.StreamingRef, r.InitialRef, r.ManagementRef, r.StreamingRef} {
		if ref != nil {
			ref.Scope = q.Scope
		}
	}
	return r, nil
}

func jobStage(r provision.Run, current api.CameraJobStage) api.CameraJobStage {
	if r.Stage == provision.Active {
		return api.CameraJobStage_CAMERA_JOB_STAGE_ACTIVE
	}
	if current == api.CameraJobStage_CAMERA_JOB_STAGE_DEPLOYMENT_READY {
		return current // reinspection does not erase a durable publication
	}
	switch r.Stage {
	case provision.Activating, provision.Initializing, provision.Applying:
		if current != api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING {
			return api.CameraJobStage_CAMERA_JOB_STAGE_APPLYING
		}
	case provision.Verifying, provision.Deploying:
		return api.CameraJobStage_CAMERA_JOB_STAGE_VERIFYING
	case provision.WaitingAuth, provision.WaitingInput, provision.Unsupported, provision.Failed, provision.Canceled:
		return api.CameraJobStage_CAMERA_JOB_STAGE_WAITING
	}
	return current
}

func captureProfile(p camera.MediaProfile) *api.CameraCaptureProfile {
	return api.CameraCaptureProfile_builder{Codec: p.Codec, Width: int32(p.Width), Height: int32(p.Height), Fps: p.FPS, MaxBitrateBps: p.Bitrate}.Build()
}

func runObservation(r provision.Run, prior *api.CameraJobObservation) (*api.CameraJobObservation, error) {
	state, err := cameraprogress.Encode(r)
	if err != nil {
		return nil, err
	}
	o := api.CameraJobObservation_builder{Progress: state}.Build()
	if len(r.Events) > 0 {
		o.SetCode(r.Events[len(r.Events)-1].Code)
	}
	for _, warning := range r.Warnings {
		o.SetWarnings(append(o.GetWarnings(), api.CameraWarning_builder{Code: warning.Code, Field: warning.Field, Requested: warning.Requested, Applied: warning.Applied, Reason: warning.Reason}.Build()))
	}
	if r.Plan != nil {
		p := r.Plan.After
		if r.DeploymentProfile != nil {
			p = *r.DeploymentProfile
		} else if r.Verified != nil {
			v := r.Verified.Profile
			if p.Codec == "" {
				p.Codec = v.Codec
			}
			if p.Width == 0 {
				p.Width = v.Width
			}
			if p.Height == 0 {
				p.Height = v.Height
			}
			if p.FPS == 0 {
				p.FPS = v.FPS
			}
			if p.Bitrate == 0 {
				p.Bitrate = v.Bitrate
			}
		}
		o.SetEffective(captureProfile(p))
		if v := r.Verified; v != nil && v.Profile.GOP > 0 && v.Profile.FPS > 0 {
			o.GetEffective().SetKeyframeIntervalMs(int64(float64(v.Profile.GOP) * 1000 / v.Profile.FPS))
		}
	}
	if v := r.Verified; v != nil {
		o.SetVerified(api.CameraMediaEvidence_builder{Profile: captureProfile(v.Profile), Decoded: v.Decoded, DecodedFrames: v.DecodedFrames, DurationSeconds: v.Duration.Seconds(), ObservedAt: timestamppb.New(v.ObservedAt)}.Build())
	}
	// Readiness facts are collected through the job observer after publication.
	// A workflow checkpoint cannot manufacture Producer/live acknowledgments.
	if prior != nil {
		o.SetAppliedRevision(prior.GetAppliedRevision())
		o.SetLiveDecodedFrames(prior.GetLiveDecodedFrames())
		o.SetLiveFirstFrameAt(prior.GetLiveFirstFrameAt())
		o.SetLiveLastFrameAt(prior.GetLiveLastFrameAt())
	}
	return o, nil
}

func (s *JobStore) Save(ctx context.Context, r provision.Run) error {
	w, err := s.check(ctx)
	if err != nil {
		return err
	}
	if r.Request.ID != cameraID(w.GetJob().GetId()) {
		return camera.ErrForbidden
	}
	o, err := runObservation(r, w.GetJob().GetObserved())
	if err != nil {
		return err
	}
	if j, b := w.GetJob(), w.GetBinding(); j.GetDatePublished() != nil && j.GetPublishedRevision() > 0 && j.GetPublishedRevision() == b.GetDesired().GetRevision() && b.GetDesired().GetProfile() != nil {
		// Preserve rate facts filled from the first probe. A later sample can
		// vary while the same durable capture specification stays authoritative.
		o.SetEffective(proto.Clone(b.GetDesired().GetProfile()).(*api.CameraCaptureProfile))
	}
	// Version + content identifies a retry without depending on timestamps or
	// a mutable server DateUpdated, and fits the CP's bounded operation token.
	key := "checkpoint/" + strconv.FormatUint(r.Version, 10) + "/" + stateHash(o)
	_, err = s.Execution.Checkpoint(ctx, key, jobStage(r, w.GetJob().GetStage()), o)
	return err
}

func stateHash(o *api.CameraJobObservation) string {
	// The typed codec already validated size/unknown fields. Deterministic
	// protobuf preserves nil versus explicit empty wrappers in the hash.
	b, _ := (proto.MarshalOptions{Deterministic: true}).Marshal(o)
	digest := sha256.Sum256(b)
	return hex.EncodeToString(digest[:])
}

func (s *JobStore) List(ctx context.Context) ([]provision.Run, error) {
	w, err := s.check(ctx)
	if err != nil {
		return nil, err
	}
	r, err := s.Load(ctx, cameraID(w.GetJob().GetId()))
	if errors.Is(err, provision.ErrNotFound) {
		return []provision.Run{}, nil
	}
	if err != nil {
		return nil, err
	}
	return []provision.Run{r}, nil
}
