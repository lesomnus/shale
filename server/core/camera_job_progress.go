package core

import (
	"context"
	"math"
	"reflect"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/camerabootstrap"
	presetwire "github.com/lesomnus/shale/internal/camerapreset"
	"github.com/lesomnus/shale/internal/cameraprogress"
	"github.com/lesomnus/shale/provision"
	"google.golang.org/protobuf/proto"
)

// cameraProgress validates a checkpoint, not an execution grant or proof of
// ACTIVE. Its caller already owns the current job lease and transaction, and
// must write a returned first preset pin atomically with this observation.
func (s Core) cameraProgress(ctx context.Context, j *api.CameraJob, a *api.CameraAssignment, b *api.CameraBinding, d *api.CameraDevice, observed *api.CameraJobObservation) (*api.CameraResolvedPreset, error) {
	before, next := j.GetObserved().GetProgress(), observed.GetProgress()
	if next == nil {
		if before != nil {
			return nil, failed("a durable camera checkpoint cannot be discarded")
		}
		return nil, nil // explicit compatibility with jobs without typed state
	}
	if pin := j.GetInputs().GetDeviceTlsIdentity(); pin != nil && pin.GetDeviceId() != mustId(d.GetId()).String() {
		return nil, invalid("progress.device_tls_identity", "checkpoint TLS identity belongs to another camera device")
	}
	if !proto.Equal(next.GetRequest().GetDeviceTlsIdentity(), j.GetInputs().GetDeviceTlsIdentity()) {
		return nil, invalid("progress.device_tls_identity", "checkpoint must retain the exact pinned device TLS identity")
	}
	run, err := cameraprogress.Decode(next)
	if err != nil {
		return nil, invalid("progress", "invalid bounded typed camera checkpoint")
	}
	if !reflect.DeepEqual(run.Request.Bootstrap, camerabootstrap.Decode(j.GetInputs().GetBootstrap())) {
		return nil, invalid("progress.bootstrap", "checkpoint must retain the exact pinned bootstrap profile")
	}
	// Status-only reports may carry the exact historical checkpoint after a
	// Producer replacement. An edit must instead bind today's complete scope.
	if proto.Equal(before, next) {
		var candidate camera.PresetSettings
		var warnings []camera.PresetWarning
		if run.Effective != nil {
			candidate = *run.Effective
			if run.Preset != nil {
				choices, err := run.Preset.Candidates()
				if err != nil {
					return nil, failed("stored camera checkpoint preset is invalid")
				}
				warnings = camera.WarningsFor(choices[0], candidate)
			}
		}
		// Historical scope is permitted for status. It cannot authenticate a
		// newly invented outer verification or hide the existing fallback.
		if err := cameraProgressMedia(run, candidate, observed); err != nil {
			return nil, err
		}
		if err := cameraProgressDeclaration(run, observed); err != nil {
			return nil, err
		}
		return nil, cameraProgressWarnings(run.Warnings, observed.GetWarnings(), warnings)
	}
	var previous *provision.Run
	if before != nil {
		old, err := cameraprogress.Decode(before)
		if err != nil {
			return nil, failed("stored camera checkpoint is invalid")
		}
		previous = &old
		if old.Version == math.MaxUint64 || run.Version != old.Version+1 || !run.CreatedAt.Equal(old.CreatedAt) {
			return nil, failed("camera checkpoint must advance its durable version and preserve creation time")
		}
		if old.ActivationAttempted && !run.ActivationAttempted || old.ActivationComplete && !run.ActivationComplete {
			return nil, failed("a camera checkpoint cannot forget an activation attempt or completion")
		}
		if old.Binding.DeploymentID != "" && old.Binding.DeploymentID != run.Binding.DeploymentID || old.DeploymentProfile != nil && !reflect.DeepEqual(old.DeploymentProfile, run.DeploymentProfile) {
			return nil, failed("a pending camera publication intent cannot change or be cleared")
		}
		if j.GetDatePublished() != nil && old.CandidateIndex != run.CandidateIndex {
			return nil, failed("a published camera job cannot select another candidate")
		}
	} else if run.Version != 1 {
		return nil, invalid("progress.version", "the first camera checkpoint version must be one")
	}
	if run.CreatedAt.IsZero() || run.UpdatedAt.IsZero() {
		return nil, invalid("progress.time", "checkpoint timestamps are required")
	}
	// Worker clocks do not grant freshness, order or authority. The enclosing
	// CP CAS/version and job lease provide those properties.
	scope := cameraPresetEnrollmentScope(a)
	if run.Request.ID != mustId(j.GetId()).String() || run.Request.BindingID != b.GetLogicalKey() || run.Request.Device.ID != camera.DeviceID(mustId(d.GetId()).String()) || run.Request.Scope != scope || !run.Request.UsePreset || run.Request.Desired != (camera.DesiredState{}) || run.Request.Alias != b.GetSourceAlias() || run.Request.AllowProducerRestart || run.Request.AllowActivation && !j.GetInputs().GetAllowActivation() {
		return nil, invalid("progress.request", "checkpoint does not match the pinned job and current assignment")
	}
	if run.Binding.BindingID != b.GetLogicalKey() || run.Binding.Alias != b.GetSourceAlias() || run.Binding.SourceID != "" && run.Binding.SourceID != mustId(b.GetSource().GetId()).String() {
		return nil, invalid("progress.binding", "checkpoint does not match the stable camera binding")
	}
	if err := cameraProgressRefs(run, j.GetInputs(), scope); err != nil {
		return nil, err
	}
	if run.ActivationAttempted && !run.Request.AllowActivation || run.ActivationComplete && !run.ActivationAttempted {
		return nil, invalid("progress.activation", "checkpoint activation exceeds its pinned intent")
	}
	n, err := s.Next().CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: a.GetNetwork().GetId()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if err := cameraSameScope(a.GetTenant().GetId(), a.GetSite().GetId(), n.GetTenant().GetId(), n.GetSite().GetId()); err != nil {
		return nil, err
	}
	if err := cameraProgressURIs(run, n.GetCidrs(), scope.NetworkID); err != nil {
		return nil, err
	}
	if err := cameraProgressIdentity(run, previous, d); err != nil {
		return nil, err
	}
	if previous != nil && (run.CandidateIndex < previous.CandidateIndex || previous.Effective != nil && run.Effective == nil) {
		return nil, failed("a selected camera candidate cannot move backwards or be cleared")
	}
	if run.Preset == nil {
		if j.GetPinnedPreset() != nil || previous != nil && previous.Preset != nil || run.Effective != nil || run.Plan != nil || run.InitializationPlan != nil || run.Initialized != nil || run.Verified != nil || run.DeploymentProfile != nil || run.CandidateIndex != 0 {
			return nil, invalid("progress.preset", "configuration state requires its immutable preset snapshot")
		}
		if err := cameraProgressMedia(run, camera.PresetSettings{}, observed); err != nil {
			return nil, err
		}
		return nil, cameraProgressWarnings(run.Warnings, observed.GetWarnings(), nil)
	}
	pin, err := presetwire.EncodeSnapshot(*run.Preset)
	if err != nil || !cameraPresetReferenceMatches(pin, j) {
		return nil, invalid("progress.preset", "checkpoint does not name the pinned job configuration")
	}
	if previous != nil && previous.Preset != nil {
		oldPin, err := presetwire.EncodeSnapshot(*previous.Preset)
		if err != nil || !proto.Equal(oldPin, pin) {
			return nil, failed("a resolved camera checkpoint preset cannot change")
		}
	}
	var newPin *api.CameraResolvedPreset
	if stored := j.GetPinnedPreset(); stored != nil {
		if !proto.Equal(stored, pin) {
			return nil, failed("a job's resolved camera preset cannot change")
		}
	} else {
		if run.Inspection == nil {
			return nil, invalid("progress.inspection", "resolving a camera preset requires device inspection")
		}
		identity := cameraProgressIdentityValue(run.Inspection.Device)
		if prior := d.GetIdentity(); prior.GetAuthenticated() && !identity.GetAuthenticated() {
			identity = prior
		}
		expected, err := s.cameraResolvePreset(ctx, j, a, d, identity)
		if err != nil {
			return nil, err
		}
		if !proto.Equal(expected, pin) {
			return nil, invalid("progress.preset", "checkpoint differs from the resolved immutable job configuration")
		}
		newPin = proto.Clone(expected).(*api.CameraResolvedPreset)
	}
	candidates, err := run.Preset.Candidates()
	if err != nil || run.CandidateIndex >= len(candidates) {
		return nil, invalid("progress.candidate", "checkpoint selects no allowed camera candidate")
	}
	if run.Effective == nil {
		if run.CandidateIndex != 0 || run.Plan != nil || run.InitializationPlan != nil || run.Initialized != nil || run.Verified != nil || run.DeploymentProfile != nil {
			return nil, invalid("progress.effective", "configuration state requires an explicit selected candidate")
		}
		if err := cameraProgressWarnings(run.Warnings, observed.GetWarnings(), nil); err != nil {
			return nil, err
		}
		if err := cameraProgressMedia(run, camera.PresetSettings{}, observed); err != nil {
			return nil, err
		}
		return newPin, nil
	}
	candidate := candidates[run.CandidateIndex]
	if !reflect.DeepEqual(*run.Effective, candidate) {
		return nil, invalid("progress.effective", "effective settings differ from the selected pinned candidate")
	}
	if err := cameraProgressWarnings(run.Warnings, observed.GetWarnings(), camera.WarningsFor(candidates[0], candidate)); err != nil {
		return nil, err
	}
	if err := cameraProgressPlan(run, candidate, previous); err != nil {
		return nil, err
	}
	if err := cameraProgressMedia(run, candidate, observed); err != nil {
		return nil, err
	}
	if err := cameraProgressDeclaration(run, observed); err != nil {
		return nil, err
	}
	return newPin, nil
}

func cameraProgressDeclaration(run provision.Run, observed *api.CameraJobObservation) error {
	if run.DeploymentProfile == nil {
		return nil
	}
	p := run.DeploymentProfile
	if run.Binding.DeploymentID == "" || run.Verified == nil || run.Plan == nil || observed.GetEffective() == nil || p.StreamURI != run.Verified.Profile.StreamURI || !cameraProgressActualMatches(run.Verified.Profile, captureProfileForProgress(*p)) || float64(run.Verified.Profile.Bitrate) > float64(p.Bitrate)*1.3 {
		return invalid("progress.deployment", "publication intent differs from independently verified capture")
	}
	declared := proto.Clone(observed.GetEffective()).(*api.CameraCaptureProfile)
	declared.SetKeyframeIntervalMs(0)
	if !proto.Equal(declared, captureProfileForProgress(*p)) || p.Token != run.Plan.ProfileToken || p.Token != run.Verified.Profile.Token {
		return invalid("progress.deployment", "publication profile must retain the selected and verified capture declaration")
	}
	return nil
}

func captureProfileForProgress(p camera.MediaProfile) *api.CameraCaptureProfile {
	return api.CameraCaptureProfile_builder{Codec: p.Codec, Width: int32(p.Width), Height: int32(p.Height), Fps: p.FPS, MaxBitrateBps: p.Bitrate}.Build()
}

func cameraProgressRefs(run provision.Run, input *api.CameraJobInputs, scope camera.EnrollmentScope) error {
	provider := ""
	for _, pair := range []struct {
		pinned  *api.CameraCredentialReference
		request *camera.CredentialRef
		state   *camera.CredentialRef
		purpose camera.AuthPurpose
	}{{input.GetInitialCredential(), run.Request.InitialRef, run.InitialRef, camera.AuthInitialAdmin}, {input.GetManagementCredential(), run.Request.ManagementRef, run.ManagementRef, camera.AuthManagement}, {input.GetStreamingCredential(), run.Request.StreamingRef, run.StreamingRef, camera.AuthStreaming}} {
		if pair.pinned == nil {
			if pair.request != nil || pair.state != nil {
				return invalid("progress.credential", "checkpoint added an unpinned credential")
			}
			continue
		}
		p := pair.pinned
		if p.GetPurpose() != string(pair.purpose) || provider != "" && p.GetProvider() != provider {
			return invalid("progress.credential", "pinned credentials do not share the job provider and purposes")
		}
		provider = p.GetProvider()
		want := camera.CredentialRef{Provider: provider, ID: p.GetId(), Version: p.GetVersion(), Purpose: pair.purpose, Recoverable: true, Scope: scope}
		if pair.request == nil || pair.state == nil || *pair.request != want || *pair.state != want {
			return invalid("progress.credential", "checkpoint must retain every exact pinned credential version and delivery scope")
		}
	}
	if provider == "" {
		provider = "cp"
	}
	if run.Request.Provider != provider {
		return invalid("progress.provider", "checkpoint does not use the pinned credential provider")
	}
	return nil
}

func cameraProgressURIs(run provision.Run, cidrs []string, network string) error {
	device := func(v camera.DeviceHint) error {
		for _, ep := range v.Endpoints {
			if ep.NetworkScope != "" && ep.NetworkScope != network {
				return invalid("progress.endpoint", "checkpoint endpoint belongs to another camera network")
			}
			if err := cameraURI(ep.URL, cidrs, false); err != nil {
				return err
			}
		}
		return nil
	}
	profile := func(v camera.MediaProfile) error {
		if v.StreamURI == "" {
			return nil
		}
		return cameraURI(v.StreamURI, cidrs, true)
	}
	if err := device(run.Request.Device); err != nil {
		return err
	}
	if run.Inspection != nil {
		if err := device(run.Inspection.Device); err != nil {
			return err
		}
		for _, p := range run.Inspection.Current.Profiles {
			if err := profile(p); err != nil {
				return err
			}
		}
	}
	if run.Plan != nil {
		for _, p := range []camera.MediaProfile{run.Plan.Before, run.Plan.After} {
			if err := profile(p); err != nil {
				return err
			}
		}
	}
	if run.Verified != nil {
		if err := profile(run.Verified.Profile); err != nil {
			return err
		}
	}
	if run.DeploymentProfile != nil {
		return profile(*run.DeploymentProfile)
	}
	return nil
}

func cameraProgressIdentityValue(v camera.DeviceHint) *api.CameraIdentityEvidence {
	value := api.CameraIdentityEvidence_builder{Manufacturer: v.Manufacturer, Model: v.Model, Hardware: v.Hardware, Firmware: v.Firmware, Serial: v.Serial}.Build()
	if v.MAC != "" {
		value.SetMacs([]string{v.MAC})
	}
	for _, evidence := range v.Evidence {
		if evidence.Authenticated {
			value.SetAuthenticated(true)
		}
	}
	return value
}

func (s Core) cameraProgressIdentityRecord(ctx context.Context, j *api.CameraJob, d *api.CameraDevice, o *api.CameraJobObservation) error {
	if o.GetProgress() == nil || proto.Equal(j.GetObserved().GetProgress(), o.GetProgress()) || o.GetProgress().GetInspection() == nil {
		return nil
	}
	r, err := cameraprogress.Decode(o.GetProgress())
	if err != nil {
		return invalid("progress", "invalid typed checkpoint")
	}
	identity := cameraProgressIdentityValue(r.Inspection.Device)
	if !identity.GetAuthenticated() {
		return nil
	}
	if len(identity.GetMacs()) == 0 {
		identity.SetMacs(d.GetIdentity().GetMacs())
	}
	if proto.Equal(identity, d.GetIdentity()) {
		return nil
	}
	_, err = s.Next().CameraDevice().Patch(ctx, api.CameraDevicePatchRequest_builder{Ref: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), Identity: identity, DateUpdated: d.GetDateUpdated()}.Build())
	return err
}

func cameraProgressIdentity(run provision.Run, previous *provision.Run, d *api.CameraDevice) error {
	if previous != nil && previous.Inspection != nil && run.Inspection == nil {
		return failed("a camera checkpoint cannot discard inspected physical identity")
	}
	if run.Inspection == nil {
		return nil
	}
	hint := run.Inspection.Device
	if hint.ID != camera.DeviceID(mustId(d.GetId()).String()) {
		return invalid("progress.identity", "inspection must name the assigned inventory device")
	}
	identity := cameraProgressIdentityValue(hint)
	if err := cameraPresetIdentity(identity); err != nil {
		return err
	}
	priors := []*api.CameraIdentityEvidence{d.GetIdentity()}
	if previous != nil && previous.Inspection != nil {
		priors = append(priors, cameraProgressIdentityValue(previous.Inspection.Device))
	}
	for _, prior := range priors {
		if prior.GetAuthenticated() && ((prior.GetSerial() != "" && prior.GetSerial() != identity.GetSerial()) || (prior.GetHardware() != "" && prior.GetHardware() != identity.GetHardware())) {
			return failed("authenticated physical identity changed in a camera checkpoint")
		}
	}
	if previous != nil && previous.Inspection != nil && cameraProgressIdentityValue(previous.Inspection.Device).GetAuthenticated() && !identity.GetAuthenticated() {
		// Otherwise a checkpoint could first clear the authentication marker,
		// then change the now apparently unauthenticated identity on its next
		// version when inventory had no independently retained evidence.
		return failed("a checkpoint cannot discard authenticated physical identity evidence")
	}
	return nil
}

func cameraProgressWarnings(run []camera.PresetWarning, observed []*api.CameraWarning, expected []camera.PresetWarning) error {
	for _, w := range run {
		if err := cameraToken("warning.code", w.Code); err != nil {
			return err
		}
		if !cameraPresetText(w.Field) || !cameraPresetText(w.Requested) || !cameraPresetText(w.Applied) || !cameraPresetText(w.Reason) {
			return invalid("progress.warning", "bounded structured camera warnings are required")
		}
	}
	if len(observed) > 32 {
		return invalid("progress.warning", "too many structured camera warnings")
	}
	for _, w := range observed {
		if w == nil || !cameraPresetText(w.GetField()) || !cameraPresetText(w.GetRequested()) || !cameraPresetText(w.GetApplied()) || !cameraPresetText(w.GetReason()) {
			return invalid("progress.warning", "bounded structured camera warnings are required")
		}
	}
	for _, want := range expected {
		foundRun, foundObserved := false, false
		for _, w := range run {
			foundRun = foundRun || w.Code == want.Code && w.Field == want.Field && w.Requested == want.Requested && w.Applied == want.Applied
		}
		for _, w := range observed {
			foundObserved = foundObserved || w.GetCode() == want.Code && w.GetField() == want.Field && w.GetRequested() == want.Requested && w.GetApplied() == want.Applied
		}
		if !foundRun || !foundObserved {
			return invalid("progress.warning", "the selected camera fallback must retain its structured warning")
		}
	}
	return nil
}

func cameraProgressPlan(run provision.Run, candidate camera.PresetSettings, previous *provision.Run) error {
	initialization := candidate.Initialization != (camera.DeviceSettings{})
	if run.InitializationPlan != nil && (!initialization || camera.ValidateInitializationPlan(*run.InitializationPlan, candidate.Initialization) != nil) || run.Initialized != nil && (!initialization || *run.Initialized != candidate.Initialization) {
		return invalid("progress.initialization", "initialization must retain the selected explicit device settings")
	}
	if previous != nil && previous.CandidateIndex == run.CandidateIndex && previous.InitializationPlan != nil && previous.InitializationPlan.VideoMainBefore != nil && !reflect.DeepEqual(previous.InitializationPlan, run.InitializationPlan) {
		return failed("a native initialization precondition cannot change or be cleared")
	}
	if run.Initialized != nil && candidate.Initialization.SmartCodec != "" && run.InitializationPlan == nil {
		return invalid("progress.initialization", "smart codec verification requires its native precondition")
	}
	if run.Plan == nil {
		return nil
	}
	if initialization && run.Initialized == nil {
		return invalid("progress.initialization", "device initialization must be verified before a video plan")
	}
	p, v := run.Plan, candidate.Video
	if p.ProfileToken == "" || p.Before.Token != p.ProfileToken || p.After.Token != p.ProfileToken || v.ProfileToken != "" && v.ProfileToken != p.ProfileToken || p.Revision != v.Revision {
		return invalid("progress.plan", "video plan does not retain the selected profile and revision")
	}
	want := p.Before
	if v.Codec != "" {
		want.Codec = v.Codec
	}
	if v.Width != 0 {
		want.Width = v.Width
	}
	if v.Height != 0 {
		want.Height = v.Height
	}
	if v.FPS != 0 {
		want.FPS = v.FPS
	}
	if v.Bitrate != 0 {
		want.Bitrate = v.Bitrate
	}
	if v.GOP != 0 {
		want.GOP = v.GOP
	}
	got := p.After
	if want.Token != got.Token || want.Codec != got.Codec || want.Width != got.Width || want.Height != got.Height || want.FPS != got.FPS || want.Bitrate != got.Bitrate || want.GOP != got.GOP {
		return invalid("progress.plan", "video plan changes settings outside the selected pinned intent")
	}
	return nil
}

func cameraProgressMedia(run provision.Run, candidate camera.PresetSettings, observed *api.CameraJobObservation) error {
	if run.Plan == nil {
		if run.Verified != nil || observed.GetVerified() != nil {
			return invalid("progress.verified", "media verification requires the selected video plan")
		}
		return nil
	}
	effective := observed.GetEffective()
	p := run.Plan.After
	if effective == nil || p.Codec != "" && p.Codec != effective.GetCodec() || p.Width != 0 && p.Width != int(effective.GetWidth()) || p.Height != 0 && p.Height != int(effective.GetHeight()) || p.FPS != 0 && p.FPS != effective.GetFps() || p.Bitrate != 0 && p.Bitrate != effective.GetMaxBitrateBps() {
		return invalid("progress.effective", "reported capture settings differ from the configured video plan")
	}
	actual := run.Verified
	if actual == nil {
		if observed.GetVerified() != nil || effective.GetKeyframeIntervalMs() > 0 {
			return invalid("progress.verified", "reported media facts require independent typed verification")
		}
		return nil
	}
	if observed.GetVerified() == nil || !actual.Complete || !actual.Decoded || actual.DecodedFrames < 2 || actual.ObservedAt.IsZero() || actual.Duration <= 0 || actual.ProfileRevision != candidate.Video.Revision || run.Inspection == nil || actual.Firmware != run.Inspection.Current.Firmware {
		return invalid("progress.verified", "complete verification must retain the selected profile revision and firmware")
	}
	proof := observed.GetVerified()
	if !proof.GetDecoded() || proof.GetDecodedFrames() != actual.DecodedFrames || proof.GetDurationSeconds() != actual.Duration.Seconds() || proof.GetObservedAt() == nil || !proof.GetObservedAt().AsTime().Equal(actual.ObservedAt) || proof.GetProfile().GetMaxBitrateBps() != actual.Profile.Bitrate {
		return invalid("progress.verified", "reported media facts differ from the durable independent verification")
	}
	if p := proof.GetProfile(); p.GetCodec() != actual.Profile.Codec || int(p.GetWidth()) != actual.Profile.Width || int(p.GetHeight()) != actual.Profile.Height || p.GetFps() != actual.Profile.FPS {
		return invalid("progress.verified", "reported measured profile differs from the durable independent verification")
	}
	v := actual.Profile
	if v.Codec == "" || v.Width <= 0 || v.Height <= 0 || v.FPS <= 0 || v.Bitrate <= 0 || !cameraProgressActualMatches(v, effective) || !cameraProgressActualMatches(v, observed.GetVerified().GetProfile()) {
		return invalid("progress.verified", "independently observed media differs from the reported capture profile")
	}
	// Read-back bitrate is a configured ceiling. Measured media can be below
	// it, and the existing portable verifier allows bounded encoding overhead.
	if effective.GetMaxBitrateBps() > 0 && (float64(v.Bitrate) > float64(effective.GetMaxBitrateBps())*1.3 || float64(observed.GetVerified().GetProfile().GetMaxBitrateBps()) > float64(effective.GetMaxBitrateBps())*1.3) {
		return invalid("progress.verified", "independently observed bitrate exceeds the configured ceiling")
	}
	if candidate.Video.GOP > 0 && (v.GOP <= 0 || v.GOP > candidate.Video.GOP+2) {
		return invalid("progress.verified", "an explicit GOP requires independently observed keyframe cadence")
	}
	if interval := effective.GetKeyframeIntervalMs(); interval > 0 {
		if v.GOP <= 0 {
			return invalid("progress.verified", "keyframe interval cannot be inferred from configuration alone")
		}
		measured := float64(v.GOP) * 1000 / v.FPS
		if math.Abs(float64(interval)-measured) > math.Max(2, measured*0.05) {
			return invalid("progress.verified", "reported keyframe interval differs from independently observed cadence")
		}
	}
	return nil
}

func cameraProgressActualMatches(actual camera.MediaProfile, reported *api.CameraCaptureProfile) bool {
	return reported != nil && actual.Codec == reported.GetCodec() && actual.Width == int(reported.GetWidth()) && actual.Height == int(reported.GetHeight()) && math.Abs(actual.FPS-reported.GetFps()) <= math.Max(0.5, reported.GetFps()*0.05)
}
