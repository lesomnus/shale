// Package provision runs a recoverable camera enrollment workflow over injected
// device, credential, state and deployment adapters. It does not host a server.
package provision

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
)

var (
	ErrNotFound = errors.New("provisioning run not found")
	ErrConflict = errors.New("provisioning identity or revision conflict")
	ErrNotReady = errors.New("producer media is not ready")
)

type Stage string

const (
	Pending      Stage = "pending"
	Inspecting   Stage = "inspecting"
	Planned      Stage = "planned"
	Activating   Stage = "activating"
	Initializing Stage = "initializing"
	Applying     Stage = "applying"
	Verifying    Stage = "verifying"
	Deploying    Stage = "deploying"
	Binding      Stage = "binding"
	WaitingAuth  Stage = "waiting_for_auth"
	WaitingInput Stage = "waiting_for_input"
	Unsupported  Stage = "unsupported"
	Failed       Stage = "failed"
	Canceled     Stage = "canceled"
	Active       Stage = "active"
)

// Request.ID is a caller's idempotency key. BindingID names the stable logical
// camera role. A changed desired revision uses a new ID and the same BindingID.
// Device.ID must be an explicit inventory claim, never an address-derived key.
type Request struct {
	ID                   string                    `json:"id"`
	BindingID            string                    `json:"binding_id"`
	Scope                camera.EnrollmentScope    `json:"scope"`
	Device               camera.DeviceHint         `json:"device"`
	Desired              camera.DesiredState       `json:"desired"`
	UsePreset            bool                      `json:"use_preset,omitempty"`
	Alias                string                    `json:"alias,omitempty"`
	Name                 string                    `json:"name,omitempty"`
	Provider             string                    `json:"provider"`
	ManagementRef        *camera.CredentialRef     `json:"management_ref,omitempty"`
	InitialRef           *camera.CredentialRef     `json:"initial_ref,omitempty"`
	StreamingRef         *camera.CredentialRef     `json:"streaming_ref,omitempty"`
	AllowActivation      bool                      `json:"allow_activation"`
	AllowProducerRestart bool                      `json:"allow_producer_restart"`
	ProbeDuration        time.Duration             `json:"probe_duration,omitempty"`
	Bootstrap            *camera.BootstrapSettings `json:"bootstrap,omitempty"`
	DeviceTLSIdentity    *camera.DeviceTLSIdentity `json:"device_tls_identity,omitempty"`
}

type Event struct {
	At    time.Time `json:"at"`
	Stage Stage     `json:"stage"`
	Code  string    `json:"code"`
}

type BindingState struct {
	BindingID    string    `json:"binding_id"`
	Alias        string    `json:"alias"`
	SourceID     string    `json:"source_id,omitempty"`
	DeploymentID string    `json:"deployment_id,omitempty"`
	DeployedAt   time.Time `json:"deployed_at,omitempty"`
	Ingest       bool      `json:"ingest"`
	Live         bool      `json:"live"`
	Committed    bool      `json:"committed"`
	CommittedAt  time.Time `json:"committed_at,omitempty"`
}

type Run struct {
	Request    Request               `json:"request"`
	Stage      Stage                 `json:"stage"`
	Version    uint64                `json:"version"`
	CreatedAt  time.Time             `json:"created_at"`
	UpdatedAt  time.Time             `json:"updated_at"`
	Inspection *camera.Inspection    `json:"inspection,omitempty"`
	Plan       *camera.ChangePlan    `json:"plan,omitempty"`
	Verified   *camera.VerifiedState `json:"verified,omitempty"`
	// Pinned before Publish; retries use the same declared capture payload
	// even when a later independent sample fills unknown rate facts differently.
	DeploymentProfile   *camera.MediaProfile       `json:"deployment_profile,omitempty"`
	InitialRef          *camera.CredentialRef      `json:"initial_ref,omitempty"`
	ManagementRef       *camera.CredentialRef      `json:"management_ref,omitempty"`
	StreamingRef        *camera.CredentialRef      `json:"streaming_ref,omitempty"`
	ActivationAttempted bool                       `json:"activation_attempted"`
	ActivationComplete  bool                       `json:"activation_complete"`
	Binding             BindingState               `json:"binding"`
	Events              []Event                    `json:"events"`
	Impacts             []Impact                   `json:"impacts,omitempty"`
	Preset              *camera.DevicePreset       `json:"preset,omitempty"`
	Effective           *camera.PresetSettings     `json:"effective,omitempty"`
	CandidateIndex      int                        `json:"candidate_index,omitempty"`
	InitializationPlan  *camera.InitializationPlan `json:"initialization_plan,omitempty"`
	Initialized         *camera.DeviceSettings     `json:"initialized,omitempty"`
	Warnings            []camera.PresetWarning     `json:"warnings,omitempty"`
}

type Impact struct {
	Kind                 string `json:"kind"`
	ProducerID           string `json:"producer_id"`
	RestartMayBeRequired bool   `json:"restart_may_be_required"`
	AffectsOtherSources  bool   `json:"affects_other_sources"`
}

// ImpactReporter discloses adapter-specific operational effects during a
// read-only plan. Reporting cannot deploy or restart a Producer.
type ImpactReporter interface{ Impacts(Request) []Impact }

// Store serializes a physical device and reserves its logical assignment across
// processes. Lock has no expiring lease: an uncertain writer must finish or be
// terminated before a replacement writer can issue device operations.
type Store interface {
	Lock(context.Context, string) (func() error, error)
	Reserve(context.Context, Request) error
	Identify(context.Context, camera.EnrollmentScope, camera.DeviceHint) error
	BeginMutation(context.Context, Request) error
	CompleteMutation(context.Context, Request) error
	Load(context.Context, string) (Run, error)
	Save(context.Context, Run) error
	List(context.Context) ([]Run, error)
}

type Deployment struct {
	OperationID string               `json:"operation_id"`
	Request     Request              `json:"request"`
	Profile     camera.MediaProfile  `json:"profile"`
	Credential  camera.CredentialRef `json:"credential"`
	Previous    BindingState         `json:"previous"`
}

// Publish deploys capture configuration; Observe separately resolves the normal
// Producer-created Source and proves new ingest, live and committed recording.
type SourcePublisher interface {
	Publish(context.Context, Deployment) (BindingState, error)
	Observe(context.Context, Deployment, BindingState) (BindingState, error)
}

type Engine struct {
	Store        Store
	Auth         camera.AuthProvider
	Inspector    camera.Inspector
	Configurator camera.VideoConfigurator
	Bootstrapper camera.Bootstrapper
	Verifier     camera.MediaVerifier
	Publisher    SourcePublisher
	Presets      camera.DevicePresetProvider
	Initializer  camera.DeviceInitializer
}

func (q Request) Validate() error {
	if q.Bootstrap != nil && q.Bootstrap.Validate() != nil {
		return camera.ErrInvalid
	}
	if q.DeviceTLSIdentity != nil && (q.DeviceTLSIdentity.Validate() != nil || q.DeviceTLSIdentity.DeviceID != string(q.Device.ID) || q.Bootstrap == nil || q.Bootstrap.WebScheme != "https") {
		return camera.ErrInvalid
	}
	if q.ID == "" || q.BindingID == "" || q.Device.ID == "" || (!q.UsePreset && q.Desired.Revision == "") || q.Provider == "" || q.Scope.TenantID == "" || q.Scope.SetID == "" || q.Scope.ProducerID == "" || q.Scope.NetworkID == "" || len(q.Device.Endpoints) == 0 {
		return camera.ErrInvalid
	}
	if q.UsePreset && q.Desired != (camera.DesiredState{}) {
		return camera.ErrInvalid
	}
	if q.Device.MAC != "" {
		mac, err := net.ParseMAC(q.Device.MAC)
		if err != nil || len(mac) != 6 {
			return camera.ErrInvalid
		}
	}
	if q.ProbeDuration < 0 || q.ProbeDuration > 30*time.Second {
		return camera.ErrInvalid
	}
	for _, ep := range q.Device.Endpoints {
		if ep.URL == "" || ep.URL != camera.SanitizeURI(ep.URL) || (ep.NetworkScope != "" && ep.NetworkScope != q.Scope.NetworkID) {
			return camera.ErrInvalid
		}
	}
	for _, rp := range []struct {
		ref     *camera.CredentialRef
		purpose camera.AuthPurpose
	}{{q.ManagementRef, camera.AuthManagement}, {q.InitialRef, camera.AuthInitialAdmin}, {q.StreamingRef, camera.AuthStreaming}} {
		if rp.ref != nil && (!rp.ref.Authorizes(q.Scope, rp.purpose) || rp.ref.Provider != q.Provider) {
			return camera.ErrForbidden
		}
	}
	return nil
}

func Key(parts ...string) string {
	b, _ := json.Marshal(parts)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Execute with apply=false only inspects and plans. Every apply re-inspects the
// device, including after an ambiguous prior write, before planning another one.
func (e *Engine) Execute(ctx context.Context, q Request, apply bool) (out Run, err error) {
	if q.Bootstrap != nil {
		bootstrap := *q.Bootstrap
		q.Bootstrap = &bootstrap
	}
	if q.DeviceTLSIdentity != nil {
		identity := *q.DeviceTLSIdentity
		q.DeviceTLSIdentity = &identity
	}
	if err = q.Validate(); err != nil {
		return out, err
	}
	if e.Store == nil || e.Inspector == nil || e.Configurator == nil {
		return out, camera.ErrInvalid
	}
	unlock, err := e.Store.Lock(ctx, Key(q.Scope.TenantID, string(q.Device.ID)))
	if err != nil {
		return out, err
	}
	defer unlock()
	if err = e.Store.Reserve(ctx, q); err != nil {
		return out, err
	}
	out, err = e.Store.Load(ctx, q.ID)
	if errors.Is(err, ErrNotFound) {
		now := time.Now().UTC()
		out = Run{Request: q, Stage: Pending, CreatedAt: now, InitialRef: q.InitialRef, ManagementRef: q.ManagementRef, StreamingRef: q.StreamingRef, Binding: BindingState{BindingID: q.BindingID, Alias: q.Alias}}
		if out.Binding.Alias == "" {
			out.Binding.Alias = "cam-" + Key(q.Scope.TenantID, q.BindingID)[:16]
		}
		if err = e.save(ctx, &out, Pending, "created"); err != nil {
			return out, err
		}
	} else if err != nil {
		return out, err
	} else if !sameRequest(q, out.Request) {
		return out, ErrConflict
	}
	if out.Stage != Active && out.Stage != Canceled && out.DeploymentProfile == nil && out.Binding.DeploymentID == "" {
		for _, event := range out.Events {
			if event.Stage == Deploying {
				// Older checkpoints did not journal their publication payload.
				// A private deployment journal may already have committed it;
				// require explicit reconciliation instead of issuing a new write.
				return out, ErrConflict
			}
		}
	}
	// A delayed delivery of an old completed job must not reapply its desired
	// state over a newer revision. Explicit reconciliation uses a new job ID.
	if out.Stage == Active {
		return out, e.Store.CompleteMutation(ctx, q)
	}
	if reporter, ok := e.Publisher.(ImpactReporter); ok {
		out.Impacts = reporter.Impacts(q)
	}
	// Preserve the exact references journaled before a write, even when the
	// provider's current default has changed since the previous process.
	defer func() {
		if err != nil {
			stage, code := failure(err)
			// A canceled request must still leave a durable recovery checkpoint.
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			if se := e.save(sctx, &out, stage, code); se != nil {
				err = errors.Join(safeError(err), se)
			} else {
				err = safeError(err)
			}
		}
	}()
	if out.Stage == Canceled {
		return out, context.Canceled
	}
	if err = e.save(ctx, &out, Inspecting, "inspect"); err != nil {
		return out, err
	}
	activation := camera.ActivationUnknown
	if e.Bootstrapper != nil {
		activation, err = e.Bootstrapper.Activation(ctx, q.Device)
		if err != nil {
			return out, err
		}
	}
	if activation == camera.ActivationRequired {
		if !apply {
			err = e.save(ctx, &out, Planned, "activation_required")
			return out, err
		}
		if !q.AllowActivation || e.Bootstrapper == nil {
			return out, camera.ErrManualActivation
		}
		lease, ae := camera.Acquire(ctx, e.Auth, camera.AuthRequest{Scope: q.Scope, Device: q.Device, Purpose: camera.AuthInitialAdmin, Requirement: camera.AuthRequirement{Access: camera.AccessAdministrator, Recoverable: true}, ExistingRef: out.InitialRef, IntentKey: Key(q.Scope.TenantID, q.BindingID, "initial_admin")})
		if ae != nil {
			return out, ae
		}
		defer lease.Close()
		ref := lease.Ref()
		out.InitialRef = &ref
		out.ActivationAttempted = true
		if err = e.save(ctx, &out, Activating, "credential_prepared"); err != nil {
			return out, err
		}
		if err = e.Store.BeginMutation(ctx, q); err != nil {
			return out, err
		}
		if err = e.Bootstrapper.Activate(ctx, q.Device, lease); err != nil {
			return out, err
		}
		out.ActivationComplete = true
		if err = e.save(ctx, &out, Inspecting, "activated"); err != nil {
			return out, err
		}
	}
	// If activation returned just before a crash, use the already prepared
	// credential to authenticate and verify completion instead of allocating one.
	purpose, ref := camera.AuthManagement, out.ManagementRef
	if out.ActivationAttempted && out.InitialRef != nil {
		purpose, ref = camera.AuthInitialAdmin, out.InitialRef
	}
	access := camera.AccessRead
	if apply {
		access = camera.AccessConfigure
	}
	lease, err := camera.Acquire(ctx, e.Auth, camera.AuthRequest{Scope: q.Scope, Device: q.Device, Purpose: purpose, Requirement: camera.AuthRequirement{Access: access}, ExistingRef: ref})
	if err != nil {
		return out, err
	}
	defer lease.Close()
	cref := lease.Ref()
	if purpose == camera.AuthManagement {
		out.ManagementRef = &cref
	}
	inspection, err := e.Inspector.Inspect(ctx, q.Device, lease)
	if err != nil {
		return out, err
	}
	inspection = cleanInspection(inspection)
	if inspection.Device.ID != "" && inspection.Device.ID != q.Device.ID {
		return out, camera.ErrAmbiguousIdentity
	}
	if old := out.Inspection; old != nil && !samePhysicalDevice(old.Device, inspection.Device) {
		return out, camera.ErrAmbiguousIdentity
	}
	if err = e.Store.Identify(ctx, q.Scope, inspection.Device); err != nil {
		return out, err
	}
	if inspection.Activation == camera.ActivationRequired {
		return out, camera.ErrManualActivation
	}
	if out.ActivationAttempted {
		out.ActivationComplete = true
	}
	out.Inspection = &inspection
	readback, desired, configured, err := e.configure(ctx, &out, inspection, lease, apply)
	if err != nil || !configured {
		return out, err
	}
	check := *out.Plan
	profile, ok := profileFor(readback.Current, check.ProfileToken)
	if !ok || profile.StreamURI == "" {
		return out, camera.ErrUnsupported
	}
	if e.Verifier == nil || e.Publisher == nil {
		return out, camera.ErrInvalid
	}
	stream, err := camera.Acquire(ctx, e.Auth, camera.AuthRequest{Scope: q.Scope, Device: readback.Device, Purpose: camera.AuthStreaming, Requirement: camera.AuthRequirement{Access: camera.AccessRead, Recoverable: true}, ExistingRef: out.StreamingRef})
	if err != nil {
		return out, err
	}
	sref := stream.Ref()
	stream.Close()
	out.StreamingRef = &sref
	if err = e.save(ctx, &out, Verifying, "media_probe"); err != nil {
		return out, err
	}
	d := q.ProbeDuration
	if d == 0 {
		d = 5 * time.Second
	}
	verified, err := e.Verifier.Verify(ctx, camera.MediaRequest{Scope: q.Scope, Device: readback.Device, Profile: profile, Desired: desired, CredentialRef: &sref, NetworkScope: q.Scope.NetworkID, Duration: d, RequireDecode: true})
	if err != nil {
		return out, err
	}
	if !verified.Complete || !verified.Decoded || verified.ObservedAt.IsZero() || verified.Duration < d*8/10 || !verifiedMatches(profile, verified.Profile, desired) {
		return out, ErrNotReady
	}
	if verified.Firmware != readback.Current.Firmware || verified.ProfileRevision != desired.Revision {
		return out, camera.ErrConflict
	}
	verified.Profile.StreamURI = camera.SanitizeURI(verified.Profile.StreamURI)
	out.Verified = &verified
	// Some standard profiles omit rate-control facts. Complete the capture
	// specification from independent observations without rewriting Current.
	captureProfile := profile
	if captureProfile.Codec == "" {
		captureProfile.Codec = verified.Profile.Codec
	}
	if captureProfile.Width == 0 {
		captureProfile.Width = verified.Profile.Width
	}
	if captureProfile.Height == 0 {
		captureProfile.Height = verified.Profile.Height
	}
	if captureProfile.FPS == 0 {
		captureProfile.FPS = verified.Profile.FPS
	}
	if captureProfile.Bitrate == 0 {
		captureProfile.Bitrate = verified.Profile.Bitrate
	}
	if captureProfile.GOP == 0 {
		captureProfile.GOP = verified.Profile.GOP
	}
	if out.DeploymentProfile != nil {
		captureProfile = *out.DeploymentProfile
		if !verifiedMatches(captureProfile, verified.Profile, desired) || captureProfile.Bitrate > 0 && float64(verified.Profile.Bitrate) > float64(captureProfile.Bitrate)*1.3 {
			return out, ErrNotReady
		}
	} else {
		pinned := captureProfile
		pinned.Evidence = nil
		out.DeploymentProfile = &pinned
	}
	deploymentProfile := captureProfile
	deploymentProfile.Evidence = nil
	deploymentRevision, _ := json.Marshal(struct {
		Profile    camera.MediaProfile
		Credential camera.CredentialRef
	}{deploymentProfile, sref})
	dep := Deployment{OperationID: Key(q.Scope.TenantID, q.BindingID, desired.Revision, string(deploymentRevision)), Request: q, Profile: captureProfile, Credential: sref, Previous: out.Binding}
	if out.Binding.DeploymentID != "" {
		dep.OperationID = out.Binding.DeploymentID
	} else {
		out.Binding.DeploymentID = dep.OperationID
	}
	if err = e.save(ctx, &out, Deploying, "capture_deploy"); err != nil {
		return out, err
	}
	if err = e.Store.BeginMutation(ctx, q); err != nil {
		return out, err
	}
	binding, err := e.Publisher.Publish(ctx, dep)
	if err != nil {
		return out, err
	}
	if binding.BindingID != q.BindingID || binding.Alias != out.Binding.Alias {
		return out, ErrConflict
	}
	out.Binding = binding
	if err = e.save(ctx, &out, Binding, "observe_registration"); err != nil {
		return out, err
	}
	out.Binding, err = e.Publisher.Observe(ctx, dep, binding)
	if err != nil {
		return out, err
	}
	if out.Binding.BindingID != q.BindingID || out.Binding.Alias != binding.Alias || out.Binding.SourceID == "" || out.Binding.DeployedAt.IsZero() || out.Binding.CommittedAt.IsZero() || !out.Binding.Ingest || !out.Binding.Live || !out.Binding.Committed || out.Binding.CommittedAt.Before(out.Binding.DeployedAt) {
		return out, ErrNotReady
	}
	err = e.save(ctx, &out, Active, "ready")
	if err == nil {
		err = e.Store.CompleteMutation(ctx, q)
	}
	return out, err
}

func (e *Engine) save(ctx context.Context, r *Run, stage Stage, code string) error {
	r.Stage = stage
	r.UpdatedAt = time.Now().UTC()
	r.Version++
	r.Events = append(r.Events, Event{At: r.UpdatedAt, Stage: stage, Code: code})
	if len(r.Events) > 256 {
		r.Events = r.Events[len(r.Events)-256:]
	}
	return e.Store.Save(ctx, *r)
}

func sameRequest(a, b Request) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}
func profileFor(s camera.CurrentState, token string) (camera.MediaProfile, bool) {
	for _, p := range s.Profiles {
		if p.Token == token {
			return p, true
		}
	}
	return camera.MediaProfile{}, false
}
func cleanInspection(i camera.Inspection) camera.Inspection {
	i.Device.Endpoints = slices.Clone(i.Device.Endpoints)
	i.Current.Profiles = slices.Clone(i.Current.Profiles)
	for n := range i.Device.Endpoints {
		i.Device.Endpoints[n].URL = camera.SanitizeURI(i.Device.Endpoints[n].URL)
	}
	for n := range i.Current.Profiles {
		i.Current.Profiles[n].StreamURI = camera.SanitizeURI(i.Current.Profiles[n].StreamURI)
	}
	return i
}
func samePhysicalDevice(a, b camera.DeviceHint) bool {
	if a.ID != "" && b.ID != "" && a.ID != b.ID {
		return false
	}
	// A changed IP/name is fine. A changed authenticated serial/hardware is not
	// silently interpreted as DHCP. Explicit replacement is a separate action.
	if a.MAC != "" {
		x, xe := net.ParseMAC(a.MAC)
		y, ye := net.ParseMAC(b.MAC)
		if xe != nil || ye != nil || x.String() != y.String() {
			return false
		}
	}
	return (a.Serial == "" || a.Serial == b.Serial) && (a.Hardware == "" || a.Hardware == b.Hardware)
}

func verifiedMatches(reported, actual camera.MediaProfile, w camera.DesiredState) bool {
	codec := func(s string) string {
		s = strings.ToLower(s)
		if s == "hevc" {
			return "h265"
		}
		if s == "avc" {
			return "h264"
		}
		return s
	}
	if actual.Codec == "" || actual.Width <= 0 || actual.Height <= 0 || actual.FPS <= 0 || actual.Bitrate <= 0 {
		return false
	}
	if reported.Token != "" && actual.Token != reported.Token {
		return false
	}
	if reported.Codec != "" && codec(actual.Codec) != codec(reported.Codec) || reported.Width > 0 && actual.Width != reported.Width || reported.Height > 0 && actual.Height != reported.Height || reported.FPS > 0 && math.Abs(actual.FPS-reported.FPS) > reported.FPS*.15 {
		return false
	}
	if w.Codec != "" && codec(actual.Codec) != codec(w.Codec) || w.Width > 0 && w.Width != actual.Width || w.Height > 0 && w.Height != actual.Height || w.FPS > 0 && math.Abs(actual.FPS-w.FPS) > w.FPS*.15 || w.Bitrate > 0 && actual.Bitrate > w.Bitrate*13/10 || w.GOP > 0 && actual.GOP > w.GOP+2 {
		return false
	}
	return true
}
func failure(err error) (Stage, string) {
	switch {
	case errors.Is(err, camera.ErrAuthRequired), errors.Is(err, camera.ErrExpired):
		return WaitingAuth, "credentials_required"
	case errors.Is(err, camera.ErrNeedsUserInput), errors.Is(err, camera.ErrManualActivation), errors.Is(err, camera.ErrAmbiguousIdentity):
		return WaitingInput, "input_required"
	case errors.Is(err, camera.ErrForbidden):
		return Failed, "forbidden"
	case errors.Is(err, camera.ErrConflict), errors.Is(err, ErrConflict):
		return Failed, "conflict"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return Pending, "interrupted"
	case errors.Is(err, camera.ErrUnsupported):
		return Unsupported, "unsupported"
	case errors.Is(err, ErrNotReady):
		return Binding, "media_not_ready"
	default:
		return Failed, "operation_failed"
	}
}

type operationError struct {
	kind error
	code string
}

func (e operationError) Error() string { return fmt.Sprintf("camera provisioning: %s", e.code) }
func (e operationError) Unwrap() error { return e.kind }
func safeError(err error) error {
	_, code := failure(err)
	for _, kind := range []error{camera.ErrAuthRequired, camera.ErrExpired, camera.ErrNeedsUserInput, camera.ErrManualActivation, camera.ErrAmbiguousIdentity, camera.ErrUnsupported, camera.ErrForbidden, camera.ErrConflict, ErrConflict, ErrNotReady, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, kind) {
			return operationError{kind, code}
		}
	}
	return operationError{nil, code}
}
