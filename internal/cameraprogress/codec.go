// Package cameraprogress translates complete portable provisioning checkpoints
// to typed CP messages. It carries references, never credential material, and
// confers no job, device or delivery authority.
package cameraprogress

import (
	"errors"
	"slices"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/camerapreset"
	"github.com/lesomnus/shale/provision"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrInvalid = errors.New("camera progress: invalid checkpoint")

const MaxStateBytes = 32 << 10
const schemaVersion = 1

// Encode validates before allocating a wire graph and returns detached messages.
// Timestamps preserve their instant/nanoseconds in the CP's UTC wire convention.
func Encode(run provision.Run) (*api.CameraProvisionState, error) {
	if validateRun(run) != nil {
		return nil, ErrInvalid
	}
	e := encoder{}
	out := api.CameraProvisionState_builder{SchemaVersion: schemaVersion, Request: e.request(run.Request), Stage: string(run.Stage), Version: run.Version, CreatedAt: stamp(run.CreatedAt), UpdatedAt: stamp(run.UpdatedAt), Inspection: e.inspection(run.Inspection), Plan: e.plan(run.Plan), Verified: e.verified(run.Verified), InitialRef: ref(run.InitialRef), ManagementRef: ref(run.ManagementRef), StreamingRef: ref(run.StreamingRef), ActivationAttempted: run.ActivationAttempted, ActivationComplete: run.ActivationComplete, Binding: binding(run.Binding), Events: events(run.Events), Impacts: impacts(run.Impacts), Preset: e.preset(run.Preset), Effective: e.settings(run.Effective), CandidateIndex: int32(run.CandidateIndex), InitializationPlan: initialization(run.InitializationPlan), Initialized: deviceSettings(run.Initialized), Warnings: warnings(run.Warnings), DeploymentProfile: profilePointer(run.DeploymentProfile)}.Build()
	if e.err != nil || validateWire(out) != nil {
		return nil, ErrInvalid
	}
	return out, nil
}

// Decode rejects unknown/oversized/invalid wire data before creating portable
// state. Nil pointers and explicitly empty collections remain distinct.
func Decode(value *api.CameraProvisionState) (provision.Run, error) {
	if value == nil || value.GetSchemaVersion() != schemaVersion || validateWire(value) != nil {
		return provision.Run{}, ErrInvalid
	}
	d := decoder{}
	r := provision.Run{Request: d.request(value.GetRequest()), Stage: provision.Stage(value.GetStage()), Version: value.GetVersion(), CreatedAt: date(value.GetCreatedAt()), UpdatedAt: date(value.GetUpdatedAt()), Inspection: d.inspection(value.GetInspection()), Plan: d.plan(value.GetPlan()), Verified: d.verified(value.GetVerified()), InitialRef: d.ref(value.GetInitialRef()), ManagementRef: d.ref(value.GetManagementRef()), StreamingRef: d.ref(value.GetStreamingRef()), ActivationAttempted: value.GetActivationAttempted(), ActivationComplete: value.GetActivationComplete(), Binding: d.binding(value.GetBinding()), Events: d.events(value.GetEvents()), Impacts: d.impacts(value.GetImpacts()), Preset: d.preset(value.GetPreset()), Effective: d.settings(value.GetEffective()), CandidateIndex: int(value.GetCandidateIndex()), InitializationPlan: d.initialization(value.GetInitializationPlan()), Initialized: d.deviceSettings(value.GetInitialized()), Warnings: d.warnings(value.GetWarnings()), DeploymentProfile: d.profilePointer(value.GetDeploymentProfile())}
	if d.err != nil || validateRun(r) != nil {
		return provision.Run{}, ErrInvalid
	}
	return r, nil
}

func stamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
func date(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime()
}
func scope(v camera.EnrollmentScope) *api.CameraPresetScope {
	return api.CameraPresetScope_builder{TenantId: v.TenantID, SiteId: v.SiteID, SetId: v.SetID, ProducerId: v.ProducerID, NetworkId: v.NetworkID}.Build()
}
func readScope(v *api.CameraPresetScope) camera.EnrollmentScope {
	return camera.EnrollmentScope{TenantID: v.GetTenantId(), SiteID: v.GetSiteId(), SetID: v.GetSetId(), ProducerID: v.GetProducerId(), NetworkID: v.GetNetworkId()}
}
func stringsPB(v []string) *api.CameraProvisionStrings {
	if v == nil {
		return nil
	}
	return api.CameraProvisionStrings_builder{Items: slices.Clone(v)}.Build()
}
func readStrings(v *api.CameraProvisionStrings) []string {
	if v == nil {
		return nil
	}
	return append(make([]string, 0, len(v.GetItems())), v.GetItems()...)
}
func ref(v *camera.CredentialRef) *api.CameraProvisionCredentialRef {
	if v == nil {
		return nil
	}
	return api.CameraProvisionCredentialRef_builder{Provider: v.Provider, Id: v.ID, Version: v.Version, Purpose: string(v.Purpose), Recoverable: v.Recoverable, Scope: scope(v.Scope)}.Build()
}
func evidence(v []camera.Evidence) *api.CameraProvisionEvidenceList {
	if v == nil {
		return nil
	}
	out := make([]*api.CameraProvisionEvidence, len(v))
	for i, x := range v {
		out[i] = api.CameraProvisionEvidence_builder{Source: x.Source, At: stamp(x.At), Firmware: x.Firmware, Authenticated: x.Authenticated}.Build()
	}
	return api.CameraProvisionEvidenceList_builder{Items: out}.Build()
}
func device(v camera.DeviceHint) *api.CameraProvisionDevice {
	var eps *api.CameraProvisionEndpoints
	if v.Endpoints != nil {
		items := make([]*api.CameraProvisionEndpoint, len(v.Endpoints))
		for i, x := range v.Endpoints {
			items[i] = api.CameraProvisionEndpoint_builder{Url: x.URL, NetworkScope: x.NetworkScope}.Build()
		}
		eps = api.CameraProvisionEndpoints_builder{Items: items}.Build()
	}
	return api.CameraProvisionDevice_builder{Id: string(v.ID), CandidateId: v.CandidateID, Endpoints: eps, Manufacturer: v.Manufacturer, Model: v.Model, Hardware: v.Hardware, Firmware: v.Firmware, Serial: v.Serial, Mac: v.MAC, DiscoveryId: v.DiscoveryID, Evidence: evidence(v.Evidence)}.Build()
}
func profile(v camera.MediaProfile) *api.CameraProvisionMediaProfile {
	return api.CameraProvisionMediaProfile_builder{Token: v.Token, Name: v.Name, Codec: v.Codec, Width: int32(v.Width), Height: int32(v.Height), Fps: v.FPS, Bitrate: v.Bitrate, Gop: int32(v.GOP), StreamUri: v.StreamURI, Evidence: evidence(v.Evidence)}.Build()
}
func profiles(v []camera.MediaProfile) *api.CameraProvisionMediaProfiles {
	if v == nil {
		return nil
	}
	out := make([]*api.CameraProvisionMediaProfile, len(v))
	for i, x := range v {
		out[i] = profile(x)
	}
	return api.CameraProvisionMediaProfiles_builder{Items: out}.Build()
}
func current(v camera.CurrentState) *api.CameraProvisionCurrentState {
	return api.CameraProvisionCurrentState_builder{Profiles: profiles(v.Profiles), Firmware: v.Firmware, ObservedAt: stamp(v.ObservedAt)}.Build()
}
func capability(v camera.ProfileCapability) *api.CameraProvisionProfileCapability {
	var rs *api.CameraProvisionResolutions
	var fps *api.CameraProvisionDoubles
	var br *api.CameraProvisionIntegers
	if v.Resolutions != nil {
		out := make([]*api.CameraProvisionResolution, len(v.Resolutions))
		for i, x := range v.Resolutions {
			out[i] = api.CameraProvisionResolution_builder{Width: int32(x.Width), Height: int32(x.Height)}.Build()
		}
		rs = api.CameraProvisionResolutions_builder{Items: out}.Build()
	}
	if v.FrameRates != nil {
		fps = api.CameraProvisionDoubles_builder{Items: slices.Clone(v.FrameRates)}.Build()
	}
	if v.Bitrates != nil {
		br = api.CameraProvisionIntegers_builder{Items: slices.Clone(v.Bitrates)}.Build()
	}
	return api.CameraProvisionProfileCapability_builder{Token: v.Token, Codecs: stringsPB(v.Codecs), Resolutions: rs, FrameRates: fps, Bitrates: br, FrameRateMin: v.FrameRateMin, FrameRateMax: v.FrameRateMax, BitrateMin: v.BitrateMin, BitrateMax: v.BitrateMax, GopMin: int32(v.GOPMin), GopMax: int32(v.GOPMax), Evidence: evidence(v.Evidence)}.Build()
}
func capabilities(v camera.Capabilities) *api.CameraProvisionCapabilities {
	var ps *api.CameraProvisionProfileCapabilities
	if v.Profiles != nil {
		out := make([]*api.CameraProvisionProfileCapability, len(v.Profiles))
		for i, x := range v.Profiles {
			out[i] = capability(x)
		}
		ps = api.CameraProvisionProfileCapabilities_builder{Items: out}.Build()
	}
	return api.CameraProvisionCapabilities_builder{Profiles: ps, Evidence: evidence(v.Evidence)}.Build()
}
func binding(v provision.BindingState) *api.CameraProvisionBinding {
	return api.CameraProvisionBinding_builder{BindingId: v.BindingID, Alias: v.Alias, SourceId: v.SourceID, DeploymentId: v.DeploymentID, DeployedAt: stamp(v.DeployedAt), Ingest: v.Ingest, Live: v.Live, Committed: v.Committed, CommittedAt: stamp(v.CommittedAt)}.Build()
}
func events(v []provision.Event) *api.CameraProvisionEvents {
	if v == nil {
		return nil
	}
	out := make([]*api.CameraProvisionEvent, len(v))
	for i, x := range v {
		out[i] = api.CameraProvisionEvent_builder{At: stamp(x.At), Stage: string(x.Stage), Code: x.Code}.Build()
	}
	return api.CameraProvisionEvents_builder{Items: out}.Build()
}
func impacts(v []provision.Impact) *api.CameraProvisionImpacts {
	if v == nil {
		return nil
	}
	out := make([]*api.CameraProvisionImpact, len(v))
	for i, x := range v {
		out[i] = api.CameraProvisionImpact_builder{Kind: x.Kind, ProducerId: x.ProducerID, RestartMayBeRequired: x.RestartMayBeRequired, AffectsOtherSources: x.AffectsOtherSources}.Build()
	}
	return api.CameraProvisionImpacts_builder{Items: out}.Build()
}
func warnings(v []camera.PresetWarning) *api.CameraProvisionWarnings {
	if v == nil {
		return nil
	}
	out := make([]*api.CameraProvisionWarning, len(v))
	for i, x := range v {
		out[i] = api.CameraProvisionWarning_builder{Code: x.Code, Field: x.Field, Requested: x.Requested, Applied: x.Applied, Reason: x.Reason}.Build()
	}
	return api.CameraProvisionWarnings_builder{Items: out}.Build()
}
func deviceSettings(v *camera.DeviceSettings) *api.CameraProvisionDeviceSettings {
	if v == nil {
		return nil
	}
	return api.CameraProvisionDeviceSettings_builder{PowerLineFrequency: int32(v.PowerLineFrequency)}.Build()
}
func initialization(v *camera.InitializationPlan) *api.CameraProvisionInitializationPlan {
	if v == nil {
		return nil
	}
	return api.CameraProvisionInitializationPlan_builder{Before: deviceSettings(&v.Before), After: deviceSettings(&v.After), Changes: stringsPB(v.Changes), Revision: v.Revision}.Build()
}

type encoder struct{ err error }

func (e *encoder) video(v camera.DesiredState) *api.CameraPresetVideo {
	x, err := camerapreset.EncodeSettings(camera.PresetSettings{Video: v})
	if err != nil {
		e.err = ErrInvalid
		return nil
	}
	return x.GetVideo()
}
func (e *encoder) request(v provision.Request) *api.CameraProvisionRequest {
	return api.CameraProvisionRequest_builder{Id: v.ID, BindingId: v.BindingID, Scope: scope(v.Scope), Device: device(v.Device), Desired: e.video(v.Desired), UsePreset: v.UsePreset, Alias: v.Alias, Name: v.Name, Provider: v.Provider, ManagementRef: ref(v.ManagementRef), InitialRef: ref(v.InitialRef), StreamingRef: ref(v.StreamingRef), AllowActivation: v.AllowActivation, AllowProducerRestart: v.AllowProducerRestart, ProbeDurationNs: int64(v.ProbeDuration)}.Build()
}
func (e *encoder) inspection(v *camera.Inspection) *api.CameraProvisionInspection {
	if v == nil {
		return nil
	}
	return api.CameraProvisionInspection_builder{Device: device(v.Device), Activation: string(v.Activation), Capabilities: capabilities(v.Capabilities), Current: current(v.Current)}.Build()
}
func (e *encoder) plan(v *camera.ChangePlan) *api.CameraProvisionChangePlan {
	if v == nil {
		return nil
	}
	return api.CameraProvisionChangePlan_builder{ProfileToken: v.ProfileToken, Before: profile(v.Before), After: profile(v.After), Changes: stringsPB(v.Changes), Revision: v.Revision}.Build()
}
func (e *encoder) verified(v *camera.VerifiedState) *api.CameraProvisionVerifiedState {
	if v == nil {
		return nil
	}
	return api.CameraProvisionVerifiedState_builder{Profile: profile(v.Profile), AudioCodec: v.AudioCodec, ArrivalFps: v.ArrivalFPS, ArrivalBitrate: v.ArrivalBitrate, DurationNs: int64(v.Duration), MaxGapNs: int64(v.MaxGap), Decoded: v.Decoded, DecodedFrames: v.DecodedFrames, Complete: v.Complete, Evidence: evidence(v.Evidence), ObservedAt: stamp(v.ObservedAt), Firmware: v.Firmware, ProfileRevision: v.ProfileRevision}.Build()
}
func (e *encoder) settings(v *camera.PresetSettings) *api.CameraProvisionSettings {
	if v == nil {
		return nil
	}
	x, err := camerapreset.EncodeSettings(*v)
	if err != nil {
		e.err = ErrInvalid
		return nil
	}
	return api.CameraProvisionSettings_builder{Value: x, PreservePresent: v.Preserve != nil}.Build()
}
func (e *encoder) preset(v *camera.DevicePreset) *api.CameraProvisionPreset {
	if v == nil {
		return nil
	}
	x, err := camerapreset.EncodeSnapshot(*v)
	if err != nil {
		e.err = ErrInvalid
		return nil
	}
	flags := make([]bool, len(v.Fallbacks))
	for i, a := range v.Fallbacks {
		flags[i] = a.Preserve != nil
	}
	return api.CameraProvisionPreset_builder{Value: x, MatchedRulesPresent: v.MatchedRules != nil, AlternativesPresent: v.Fallbacks != nil, RequiredPresent: v.Required != nil, RequestedPreservePresent: v.Requested.Preserve != nil, AlternativePreservePresent: flags}.Build()
}

type decoder struct{ err error }

func (d *decoder) require(ok bool) {
	if !ok {
		d.err = ErrInvalid
	}
}
func (d *decoder) ref(v *api.CameraProvisionCredentialRef) *camera.CredentialRef {
	if v == nil {
		return nil
	}
	d.require(v.GetScope() != nil)
	return &camera.CredentialRef{Provider: v.GetProvider(), ID: v.GetId(), Version: v.GetVersion(), Purpose: camera.AuthPurpose(v.GetPurpose()), Recoverable: v.GetRecoverable(), Scope: readScope(v.GetScope())}
}
func (d *decoder) evidence(v *api.CameraProvisionEvidenceList) []camera.Evidence {
	if v == nil {
		return nil
	}
	out := make([]camera.Evidence, len(v.GetItems()))
	for i, x := range v.GetItems() {
		if x == nil {
			d.err = ErrInvalid
			continue
		}
		out[i] = camera.Evidence{Source: x.GetSource(), At: date(x.GetAt()), Firmware: x.GetFirmware(), Authenticated: x.GetAuthenticated()}
	}
	return out
}
func (d *decoder) device(v *api.CameraProvisionDevice) camera.DeviceHint {
	if v == nil {
		d.err = ErrInvalid
		return camera.DeviceHint{}
	}
	var eps []camera.Endpoint
	if x := v.GetEndpoints(); x != nil {
		eps = make([]camera.Endpoint, len(x.GetItems()))
		for i, p := range x.GetItems() {
			if p == nil {
				d.err = ErrInvalid
				continue
			}
			eps[i] = camera.Endpoint{URL: p.GetUrl(), NetworkScope: p.GetNetworkScope()}
		}
	}
	return camera.DeviceHint{ID: camera.DeviceID(v.GetId()), CandidateID: v.GetCandidateId(), Endpoints: eps, Manufacturer: v.GetManufacturer(), Model: v.GetModel(), Hardware: v.GetHardware(), Firmware: v.GetFirmware(), Serial: v.GetSerial(), MAC: v.GetMac(), DiscoveryID: v.GetDiscoveryId(), Evidence: d.evidence(v.GetEvidence())}
}
func (d *decoder) profile(v *api.CameraProvisionMediaProfile) camera.MediaProfile {
	if v == nil {
		d.err = ErrInvalid
		return camera.MediaProfile{}
	}
	return camera.MediaProfile{Token: v.GetToken(), Name: v.GetName(), Codec: v.GetCodec(), Width: int(v.GetWidth()), Height: int(v.GetHeight()), FPS: v.GetFps(), Bitrate: v.GetBitrate(), GOP: int(v.GetGop()), StreamURI: v.GetStreamUri(), Evidence: d.evidence(v.GetEvidence())}
}
func (d *decoder) profiles(v *api.CameraProvisionMediaProfiles) []camera.MediaProfile {
	if v == nil {
		return nil
	}
	out := make([]camera.MediaProfile, len(v.GetItems()))
	for i, x := range v.GetItems() {
		out[i] = d.profile(x)
	}
	return out
}
func (d *decoder) current(v *api.CameraProvisionCurrentState) camera.CurrentState {
	if v == nil {
		d.err = ErrInvalid
		return camera.CurrentState{}
	}
	return camera.CurrentState{Profiles: d.profiles(v.GetProfiles()), Firmware: v.GetFirmware(), ObservedAt: date(v.GetObservedAt())}
}
func (d *decoder) capability(v *api.CameraProvisionProfileCapability) camera.ProfileCapability {
	if v == nil {
		d.err = ErrInvalid
		return camera.ProfileCapability{}
	}
	var rs []camera.Resolution
	var fps []float64
	var br []int64
	if x := v.GetResolutions(); x != nil {
		rs = make([]camera.Resolution, len(x.GetItems()))
		for i, p := range x.GetItems() {
			if p == nil {
				d.err = ErrInvalid
				continue
			}
			rs[i] = camera.Resolution{Width: int(p.GetWidth()), Height: int(p.GetHeight())}
		}
	}
	if x := v.GetFrameRates(); x != nil {
		fps = append(make([]float64, 0, len(x.GetItems())), x.GetItems()...)
	}
	if x := v.GetBitrates(); x != nil {
		br = append(make([]int64, 0, len(x.GetItems())), x.GetItems()...)
	}
	return camera.ProfileCapability{Token: v.GetToken(), Codecs: readStrings(v.GetCodecs()), Resolutions: rs, FrameRates: fps, Bitrates: br, FrameRateMin: v.GetFrameRateMin(), FrameRateMax: v.GetFrameRateMax(), BitrateMin: v.GetBitrateMin(), BitrateMax: v.GetBitrateMax(), GOPMin: int(v.GetGopMin()), GOPMax: int(v.GetGopMax()), Evidence: d.evidence(v.GetEvidence())}
}
func (d *decoder) capabilities(v *api.CameraProvisionCapabilities) camera.Capabilities {
	if v == nil {
		d.err = ErrInvalid
		return camera.Capabilities{}
	}
	var out []camera.ProfileCapability
	if x := v.GetProfiles(); x != nil {
		out = make([]camera.ProfileCapability, len(x.GetItems()))
		for i, p := range x.GetItems() {
			out[i] = d.capability(p)
		}
	}
	return camera.Capabilities{Profiles: out, Evidence: d.evidence(v.GetEvidence())}
}
func (d *decoder) video(v *api.CameraPresetVideo) camera.DesiredState {
	if v == nil {
		d.err = ErrInvalid
		return camera.DesiredState{}
	}
	x, err := camerapreset.DecodeSettings(api.CameraPresetSettings_builder{Video: proto.Clone(v).(*api.CameraPresetVideo)}.Build())
	if err != nil {
		d.err = ErrInvalid
	}
	return x.Video
}
func (d *decoder) request(v *api.CameraProvisionRequest) provision.Request {
	if v == nil {
		d.err = ErrInvalid
		return provision.Request{}
	}
	d.require(v.GetScope() != nil)
	return provision.Request{ID: v.GetId(), BindingID: v.GetBindingId(), Scope: readScope(v.GetScope()), Device: d.device(v.GetDevice()), Desired: d.video(v.GetDesired()), UsePreset: v.GetUsePreset(), Alias: v.GetAlias(), Name: v.GetName(), Provider: v.GetProvider(), ManagementRef: d.ref(v.GetManagementRef()), InitialRef: d.ref(v.GetInitialRef()), StreamingRef: d.ref(v.GetStreamingRef()), AllowActivation: v.GetAllowActivation(), AllowProducerRestart: v.GetAllowProducerRestart(), ProbeDuration: time.Duration(v.GetProbeDurationNs())}
}
func (d *decoder) inspection(v *api.CameraProvisionInspection) *camera.Inspection {
	if v == nil {
		return nil
	}
	return &camera.Inspection{Device: d.device(v.GetDevice()), Activation: camera.ActivationState(v.GetActivation()), Capabilities: d.capabilities(v.GetCapabilities()), Current: d.current(v.GetCurrent())}
}
func (d *decoder) plan(v *api.CameraProvisionChangePlan) *camera.ChangePlan {
	if v == nil {
		return nil
	}
	return &camera.ChangePlan{ProfileToken: v.GetProfileToken(), Before: d.profile(v.GetBefore()), After: d.profile(v.GetAfter()), Changes: readStrings(v.GetChanges()), Revision: v.GetRevision()}
}
func (d *decoder) verified(v *api.CameraProvisionVerifiedState) *camera.VerifiedState {
	if v == nil {
		return nil
	}
	return &camera.VerifiedState{Profile: d.profile(v.GetProfile()), AudioCodec: v.GetAudioCodec(), ArrivalFPS: v.GetArrivalFps(), ArrivalBitrate: v.GetArrivalBitrate(), Duration: time.Duration(v.GetDurationNs()), MaxGap: time.Duration(v.GetMaxGapNs()), Decoded: v.GetDecoded(), DecodedFrames: v.GetDecodedFrames(), Complete: v.GetComplete(), Evidence: d.evidence(v.GetEvidence()), ObservedAt: date(v.GetObservedAt()), Firmware: v.GetFirmware(), ProfileRevision: v.GetProfileRevision()}
}
func (d *decoder) binding(v *api.CameraProvisionBinding) provision.BindingState {
	if v == nil {
		d.err = ErrInvalid
		return provision.BindingState{}
	}
	return provision.BindingState{BindingID: v.GetBindingId(), Alias: v.GetAlias(), SourceID: v.GetSourceId(), DeploymentID: v.GetDeploymentId(), DeployedAt: date(v.GetDeployedAt()), Ingest: v.GetIngest(), Live: v.GetLive(), Committed: v.GetCommitted(), CommittedAt: date(v.GetCommittedAt())}
}
func (d *decoder) events(v *api.CameraProvisionEvents) []provision.Event {
	if v == nil {
		return nil
	}
	out := make([]provision.Event, len(v.GetItems()))
	for i, x := range v.GetItems() {
		if x == nil {
			d.err = ErrInvalid
			continue
		}
		out[i] = provision.Event{At: date(x.GetAt()), Stage: provision.Stage(x.GetStage()), Code: x.GetCode()}
	}
	return out
}
func (d *decoder) impacts(v *api.CameraProvisionImpacts) []provision.Impact {
	if v == nil {
		return nil
	}
	out := make([]provision.Impact, len(v.GetItems()))
	for i, x := range v.GetItems() {
		if x == nil {
			d.err = ErrInvalid
			continue
		}
		out[i] = provision.Impact{Kind: x.GetKind(), ProducerID: x.GetProducerId(), RestartMayBeRequired: x.GetRestartMayBeRequired(), AffectsOtherSources: x.GetAffectsOtherSources()}
	}
	return out
}
func (d *decoder) warnings(v *api.CameraProvisionWarnings) []camera.PresetWarning {
	if v == nil {
		return nil
	}
	out := make([]camera.PresetWarning, len(v.GetItems()))
	for i, x := range v.GetItems() {
		if x == nil {
			d.err = ErrInvalid
			continue
		}
		out[i] = camera.PresetWarning{Code: x.GetCode(), Field: x.GetField(), Requested: x.GetRequested(), Applied: x.GetApplied(), Reason: x.GetReason()}
	}
	return out
}
func (d *decoder) restore(present bool, items []string) []string {
	if !present {
		d.require(len(items) == 0)
		return nil
	}
	return append(make([]string, 0, len(items)), items...)
}
func (d *decoder) settings(v *api.CameraProvisionSettings) *camera.PresetSettings {
	if v == nil {
		return nil
	}
	d.require(v.GetValue() != nil && v.GetValue().GetVideo() != nil)
	out, err := camerapreset.DecodeSettings(v.GetValue())
	if err != nil {
		d.err = ErrInvalid
	}
	out.Preserve = d.restore(v.GetPreservePresent(), out.Preserve)
	return &out
}
func (d *decoder) preset(v *api.CameraProvisionPreset) *camera.DevicePreset {
	if v == nil {
		return nil
	}
	d.require(v.GetValue() != nil && v.GetValue().GetRequested() != nil && v.GetValue().GetRequested().GetVideo() != nil)
	out, err := camerapreset.DecodeSnapshot(v.GetValue())
	if err != nil {
		d.err = ErrInvalid
		return nil
	}
	out.MatchedRules = d.restore(v.GetMatchedRulesPresent(), out.MatchedRules)
	out.Required = d.restore(v.GetRequiredPresent(), out.Required)
	out.Requested.Preserve = d.restore(v.GetRequestedPreservePresent(), out.Requested.Preserve)
	flags := v.GetAlternativePreservePresent()
	d.require(len(flags) == len(out.Fallbacks))
	if !v.GetAlternativesPresent() {
		d.require(len(out.Fallbacks) == 0)
		out.Fallbacks = nil
	} else if out.Fallbacks == nil {
		out.Fallbacks = make([]camera.PresetSettings, 0)
	}
	for i := range out.Fallbacks {
		if i >= len(flags) {
			d.err = ErrInvalid
			break
		}
		d.require(v.GetValue().GetAlternatives()[i].GetVideo() != nil)
		out.Fallbacks[i].Preserve = d.restore(flags[i], out.Fallbacks[i].Preserve)
	}
	return &out
}
func (d *decoder) deviceSettings(v *api.CameraProvisionDeviceSettings) *camera.DeviceSettings {
	if v == nil {
		return nil
	}
	return &camera.DeviceSettings{PowerLineFrequency: int(v.GetPowerLineFrequency())}
}
func (d *decoder) initialization(v *api.CameraProvisionInitializationPlan) *camera.InitializationPlan {
	if v == nil {
		return nil
	}
	before, after := d.deviceSettings(v.GetBefore()), d.deviceSettings(v.GetAfter())
	if before == nil || after == nil {
		d.err = ErrInvalid
		return nil
	}
	return &camera.InitializationPlan{Before: *before, After: *after, Changes: readStrings(v.GetChanges()), Revision: v.GetRevision()}
}

func profilePointer(v *camera.MediaProfile) *api.CameraProvisionMediaProfile {
	if v == nil {
		return nil
	}
	return profile(*v)
}
func (d *decoder) profilePointer(v *api.CameraProvisionMediaProfile) *camera.MediaProfile {
	if v == nil {
		return nil
	}
	out := d.profile(v)
	return &out
}
