package cameraprogress

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func checkpoint() provision.Run {
	now := time.Date(2026, 11, 8, 11, 12, 13, 123456789, time.UTC)
	scope := camera.EnrollmentScope{TenantID: "tenant", SiteID: "site", SetID: "set", ProducerID: "producer", NetworkID: "network"}
	ref := func(purpose camera.AuthPurpose) *camera.CredentialRef {
		return &camera.CredentialRef{Provider: "cp", ID: "credential-" + string(purpose), Version: "immutable-version", Purpose: purpose, Recoverable: true, Scope: scope}
	}
	evidence := func(source string) []camera.Evidence {
		return []camera.Evidence{{Source: source, At: now, Firmware: "1.2.3", Authenticated: true}}
	}
	device := camera.DeviceHint{ID: "inventory-device", CandidateID: "candidate", Endpoints: []camera.Endpoint{{URL: "http://192.0.2.1/onvif/device_service?channel=1", NetworkScope: scope.NetworkID}, {URL: "https://192.0.2.1/onvif/media_service", NetworkScope: scope.NetworkID}}, Manufacturer: "TP-Link", Model: "C420", Hardware: "hw1", Firmware: "1.2.3", Serial: "serial", MAC: "02:00:00:00:00:01", DiscoveryID: "urn:uuid:device", Evidence: evidence("authenticated-discovery")}
	desired := camera.DesiredState{ProfileToken: "main", Codec: "h264", Width: 1280, Height: 720, FPS: 30000.0 / 1001, Bitrate: 2_000_000, GOP: 30, Revision: "requested-revision", AllowFallback: true}
	before := camera.MediaProfile{Token: "main", Name: "Main", Codec: "h264", Width: 1920, Height: 1080, FPS: 60, Bitrate: 8_000_000, GOP: 60, StreamURI: "rtsp://192.0.2.1/stream?channel=1", Evidence: evidence("onvif-before")}
	after := before
	after.Width, after.Height, after.FPS, after.Bitrate, after.GOP = desired.Width, desired.Height, desired.FPS, desired.Bitrate, desired.GOP
	after.Evidence = evidence("onvif-after")
	effective := camera.PresetSettings{Initialization: camera.DeviceSettings{PowerLineFrequency: 60}, Video: desired, Preserve: []string{}}
	preset := camera.DevicePreset{Ref: camera.PresetRef{ID: "policy", Revision: "immutable-preset-revision"}, MatchedRules: []string{"tenant", "model", "serial"}, Requested: effective, Fallbacks: []camera.PresetSettings{{Initialization: camera.DeviceSettings{PowerLineFrequency: 50}, Video: camera.DesiredState{Width: 640, Height: 360, FPS: 15, Bitrate: 1_000_000, GOP: 15, Revision: "fallback-metadata", AllowFallback: true}, Preserve: []string{camera.PresetFieldProfile}}}, Required: []string{camera.PresetFieldCodec}}
	return provision.Run{
		Request: provision.Request{ID: "job", BindingID: "binding", Scope: scope, Device: device, Desired: desired, Alias: "camera", Name: "4F Main", Provider: "cp", InitialRef: ref(camera.AuthInitialAdmin), ManagementRef: ref(camera.AuthManagement), StreamingRef: ref(camera.AuthStreaming), AllowActivation: true, AllowProducerRestart: true, ProbeDuration: 5*time.Second + 17},
		Stage:   provision.Active, Version: math.MaxUint64, CreatedAt: now, UpdatedAt: now.Add(1*time.Second + 23),
		Inspection:        &camera.Inspection{Device: device, Activation: camera.ActivationReady, Capabilities: camera.Capabilities{Profiles: []camera.ProfileCapability{{Token: "main", Codecs: []string{"h264", "h265"}, Resolutions: []camera.Resolution{{Width: 640, Height: 360}, {Width: 1280, Height: 720}}, FrameRates: []float64{15, 30000.0 / 1001, 60}, Bitrates: []int64{1_000_000, 2_000_000, 8_000_000}, FrameRateMin: 1, FrameRateMax: 60, BitrateMin: 128_000, BitrateMax: 8_000_000, GOPMin: 1, GOPMax: 120, Evidence: evidence("onvif-options")}}, Evidence: evidence("onvif-capabilities")}, Current: camera.CurrentState{Profiles: []camera.MediaProfile{before}, Firmware: "1.2.3", ObservedAt: now}},
		Plan:              &camera.ChangePlan{ProfileToken: "main", Before: before, After: after, Changes: []string{"resolution", "fps", "bitrate", "gop"}, Revision: "immutable-plan"},
		DeploymentProfile: &after,
		Verified:          &camera.VerifiedState{Profile: after, AudioCodec: "opus", ArrivalFPS: 30000.0 / 1001, ArrivalBitrate: 1_998_765, Duration: 5*time.Second + 7, MaxGap: 42*time.Millisecond + 11, Decoded: true, DecodedFrames: 151, Complete: true, Evidence: evidence("decoded-rtsp"), ObservedAt: now.Add(time.Second), Firmware: "1.2.3", ProfileRevision: desired.Revision},
		InitialRef:        ref(camera.AuthInitialAdmin), ManagementRef: ref(camera.AuthManagement), StreamingRef: ref(camera.AuthStreaming), ActivationAttempted: true, ActivationComplete: true,
		Binding: provision.BindingState{BindingID: "binding", Alias: "camera", SourceID: "source", DeploymentID: "deployment", DeployedAt: now.Add(time.Second), Ingest: true, Live: true, Committed: true, CommittedAt: now.Add(2 * time.Second)},
		Events:  []provision.Event{{At: now, Stage: provision.Initializing, Code: "settings_planned"}, {At: now.Add(time.Second), Stage: provision.Active, Code: "bound"}},
		Impacts: []provision.Impact{{Kind: "producer_restart", ProducerID: "producer", RestartMayBeRequired: true, AffectsOtherSources: true}},
		Preset:  &preset, Effective: &effective, CandidateIndex: 1,
		InitializationPlan: &camera.InitializationPlan{Before: camera.DeviceSettings{PowerLineFrequency: 50}, After: camera.DeviceSettings{PowerLineFrequency: 60}, Changes: []string{camera.PresetFieldFrequency}, Revision: "immutable-init-plan"}, Initialized: &camera.DeviceSettings{PowerLineFrequency: 60},
		Warnings: []camera.PresetWarning{{Code: "fallback", Field: camera.PresetFieldResolution, Requested: "1280x720", Applied: "640x360", Reason: "unsupported"}},
	}
}

func wireRoundTrip(t *testing.T, run provision.Run) (*api.CameraProvisionState, provision.Run) {
	t.Helper()
	value, err := Encode(run)
	require.NoError(t, err)
	b, err := proto.Marshal(value)
	require.NoError(t, err)
	require.LessOrEqual(t, len(b), MaxStateBytes)
	persisted := &api.CameraProvisionState{}
	require.NoError(t, proto.Unmarshal(b, persisted))
	got, err := Decode(persisted)
	require.NoError(t, err)
	return persisted, got
}

func TestFullRunWireRoundTrip(t *testing.T) {
	want := checkpoint()
	_, got := wireRoundTrip(t, want)
	require.Equal(t, want, got)
	// These fields influence resuming device writes and must not be a summary.
	require.True(t, got.InitialRef.Recoverable)
	require.Equal(t, want.Request.Scope, got.InitialRef.Scope)
	require.Equal(t, want.Request.Desired, got.Request.Desired)
	require.Equal(t, want.Preset, got.Preset)
	require.Equal(t, want.InitializationPlan, got.InitializationPlan)
}

func TestCheckpointOwnsPortableAndWireGraphs(t *testing.T) {
	want := checkpoint()
	value, err := Encode(want)
	require.NoError(t, err)
	stable := proto.Clone(value).(*api.CameraProvisionState)
	want.Request.Device.Endpoints[0].URL = "http://192.0.2.99/changed"
	want.Inspection.Capabilities.Profiles[0].Resolutions[0].Width = 999
	want.Plan.Changes[0] = "changed"
	want.Preset.Fallbacks[0].Preserve[0] = "changed"
	want.InitialRef.Scope.SiteID = "changed"
	want.Events[0].Code = "changed"
	require.True(t, proto.Equal(stable, value), "encoding must not borrow mutable slices or references")

	got, err := Decode(value)
	require.NoError(t, err)
	expected := checkpoint()
	value.GetRequest().GetDevice().GetEndpoints().GetItems()[0].SetUrl("http://192.0.2.99/changed")
	value.GetInspection().GetCapabilities().GetProfiles().GetItems()[0].GetResolutions().GetItems()[0].SetWidth(999)
	value.GetPlan().GetChanges().SetItems([]string{"changed"})
	value.GetPreset().GetValue().GetAlternatives()[0].SetPreserve([]string{"changed"})
	value.GetInitialRef().GetScope().SetSiteId("changed")
	value.GetEvents().GetItems()[0].SetCode("changed")
	require.Equal(t, expected, got, "decoding must not borrow mutable wire slices or references")
}

func TestCheckpointCollectionAndPointerPresence(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "nil", true: "empty"}[empty], func(t *testing.T) {
			r := checkpoint()
			r.Request.UsePreset, r.Request.Desired = true, camera.DesiredState{}
			r.Request.ManagementRef, r.Request.InitialRef, r.Request.StreamingRef = nil, nil, nil
			r.InitialRef, r.ManagementRef, r.StreamingRef = nil, nil, nil
			r.Inspection = &camera.Inspection{Device: camera.DeviceHint{}}
			r.Plan = &camera.ChangePlan{}
			r.Verified = &camera.VerifiedState{}
			r.InitializationPlan = &camera.InitializationPlan{}
			r.Initialized = &camera.DeviceSettings{}
			r.Effective = &camera.PresetSettings{}
			r.Preset = &camera.DevicePreset{Ref: camera.PresetRef{ID: "preset", Revision: "1"}}
			r.Request.Device.Evidence = nil
			r.Events, r.Impacts, r.Warnings = nil, nil, nil
			if empty {
				r.Request.Device.Evidence = []camera.Evidence{}
				r.Inspection.Device.Endpoints = []camera.Endpoint{}
				r.Inspection.Device.Evidence = []camera.Evidence{}
				r.Inspection.Capabilities.Profiles = []camera.ProfileCapability{{Codecs: []string{}, Resolutions: []camera.Resolution{}, FrameRates: []float64{}, Bitrates: []int64{}, Evidence: []camera.Evidence{}}}
				r.Inspection.Capabilities.Evidence = []camera.Evidence{}
				r.Inspection.Current.Profiles = []camera.MediaProfile{}
				r.Plan.Before.Evidence, r.Plan.After.Evidence = []camera.Evidence{}, []camera.Evidence{}
				r.Plan.Changes = []string{}
				r.Verified.Profile.Evidence, r.Verified.Evidence = []camera.Evidence{}, []camera.Evidence{}
				r.Preset.MatchedRules, r.Preset.Required = []string{}, []string{}
				r.Preset.Fallbacks = []camera.PresetSettings{}
				r.Preset.Requested.Preserve = []string{}
				r.Effective.Preserve = []string{}
				r.InitializationPlan.Changes = []string{}
				r.Events, r.Impacts, r.Warnings = []provision.Event{}, []provision.Impact{}, []camera.PresetWarning{}
			}
			_, got := wireRoundTrip(t, r)
			require.Equal(t, r, got)
		})
	}
	r := checkpoint()
	r.Inspection, r.Plan, r.Verified, r.Preset, r.Effective, r.InitializationPlan, r.Initialized = nil, nil, nil, nil, nil, nil, nil
	_, got := wireRoundTrip(t, r)
	require.Equal(t, r, got, "absent optional state must not become a present zero value")
	r = checkpoint()
	r.Preset.Fallbacks = []camera.PresetSettings{{}, {Preserve: []string{}}, {Preserve: []string{camera.PresetFieldFPS}}}
	_, got = wireRoundTrip(t, r)
	require.Equal(t, r, got, "each fallback retains its own preserve-list presence")
}

func TestCheckpointRejectsPortableInvalidState(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*provision.Run)
	}{
		{"request", func(r *provision.Run) { r.Request.ID = "" }},
		{"stage", func(r *provision.Run) { r.Stage = "unknown" }},
		{"event-stage", func(r *provision.Run) { r.Events[0].Stage = "unknown" }},
		{"invalid-utf8", func(r *provision.Run) { r.Request.Device.Model = string([]byte{0xff, 0xfe}) }},
		{"endpoint-credentials", func(r *provision.Run) { r.Request.Device.Endpoints[0].URL = "http://admin:p%40ss@192.0.2.1/onvif" }},
		{"endpoint-secret-query", func(r *provision.Run) { r.Request.Device.Endpoints[0].URL = "http://192.0.2.1/onvif?token=raw-secret" }},
		{"stream-credentials", func(r *provision.Run) { r.Plan.After.StreamURI = "rtsp://admin:p%40ss@192.0.2.1/stream" }},
		{"stream-secret-query", func(r *provision.Run) { r.Plan.After.StreamURI = "rtsp://192.0.2.1/stream?PASSWORD=raw-secret" }},
		{"endpoint-relative", func(r *provision.Run) { r.Request.Device.Endpoints[0].URL = "/onvif" }},
		{"nan", func(r *provision.Run) { r.Verified.ArrivalFPS = math.NaN() }},
		{"infinity", func(r *provision.Run) { r.Inspection.Capabilities.Profiles[0].FrameRates[0] = math.Inf(1) }},
		{"integer-overflow", func(r *provision.Run) { r.Plan.After.Width = math.MaxInt32 + 1 }},
		{"negative-duration", func(r *provision.Run) { r.Verified.MaxGap = -1 }},
		{"duration-bound", func(r *provision.Run) { r.Verified.Duration = 25 * time.Hour }},
		{"timestamp", func(r *provision.Run) { r.UpdatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"evidence-bound", func(r *provision.Run) { r.Verified.Evidence = make([]camera.Evidence, 33) }},
		{"candidate-bound", func(r *provision.Run) { r.CandidateIndex = 17 }},
		{"events-bound", func(r *provision.Run) { r.Events = make([]provision.Event, 257) }},
		{"string-bound", func(r *provision.Run) { r.Request.Name = strings.Repeat("n", 2049) }},
		{"text-budget", func(r *provision.Run) {
			r.Verified.Evidence = make([]camera.Evidence, 32)
			for i := range r.Verified.Evidence {
				r.Verified.Evidence[i].Source = strings.Repeat("s", 2048)
			}
		}},
		{"foreign-ref", func(r *provision.Run) { r.InitialRef.Scope.TenantID = "other" }},
		{"wrong-purpose-ref", func(r *provision.Run) { r.ManagementRef.Purpose = camera.AuthStreaming }},
		{"invalid-preset", func(r *provision.Run) { r.Preset.Ref.Revision = "" }},
		{"invalid-effective", func(r *provision.Run) { r.Effective.Initialization.PowerLineFrequency = 55 }},
		{"invalid-initialization", func(r *provision.Run) { r.InitializationPlan.After.PowerLineFrequency = 55 }},
		{"invalid-initialized", func(r *provision.Run) { r.Initialized.PowerLineFrequency = 55 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := checkpoint()
			test.edit(&r)
			out, err := Encode(r)
			require.ErrorIs(t, err, ErrInvalid)
			require.Nil(t, out)
			require.EqualError(t, err, "camera progress: invalid checkpoint")
		})
	}
}

func TestCheckpointRejectsMalformedWireWithoutPartialState(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*api.CameraProvisionState)
	}{
		{"schema", func(v *api.CameraProvisionState) { v.SetSchemaVersion(2) }},
		{"missing-request", func(v *api.CameraProvisionState) { v.SetRequest(nil) }},
		{"missing-scope", func(v *api.CameraProvisionState) { v.GetRequest().SetScope(nil) }},
		{"missing-device", func(v *api.CameraProvisionState) { v.GetRequest().SetDevice(nil) }},
		{"missing-desired", func(v *api.CameraProvisionState) { v.GetRequest().SetDesired(nil) }},
		{"missing-binding", func(v *api.CameraProvisionState) { v.SetBinding(nil) }},
		{"missing-ref-scope", func(v *api.CameraProvisionState) { v.GetInitialRef().SetScope(nil) }},
		{"missing-inspection-current", func(v *api.CameraProvisionState) { v.GetInspection().SetCurrent(nil) }},
		{"missing-plan-after", func(v *api.CameraProvisionState) { v.GetPlan().SetAfter(nil) }},
		{"missing-verified-profile", func(v *api.CameraProvisionState) { v.GetVerified().SetProfile(nil) }},
		{"missing-init-before", func(v *api.CameraProvisionState) { v.GetInitializationPlan().SetBefore(nil) }},
		{"missing-settings-value", func(v *api.CameraProvisionState) { v.GetEffective().SetValue(nil) }},
		{"missing-settings-video", func(v *api.CameraProvisionState) { v.GetEffective().GetValue().SetVideo(nil) }},
		{"missing-preset-requested", func(v *api.CameraProvisionState) { v.GetPreset().GetValue().SetRequested(nil) }},
		{"missing-fallback-video", func(v *api.CameraProvisionState) { v.GetPreset().GetValue().GetAlternatives()[0].SetVideo(nil) }},
		{"nil-evidence-item", func(v *api.CameraProvisionState) {
			v.GetVerified().GetEvidence().SetItems([]*api.CameraProvisionEvidence{nil})
		}},
		{"nil-capability-item", func(v *api.CameraProvisionState) {
			v.GetInspection().GetCapabilities().GetProfiles().SetItems([]*api.CameraProvisionProfileCapability{nil})
		}},
		{"inconsistent-presence", func(v *api.CameraProvisionState) { v.GetPreset().SetAlternativesPresent(false) }},
		{"missing-presence-flags", func(v *api.CameraProvisionState) { v.GetPreset().SetAlternativePreservePresent(nil) }},
		{"inconsistent-settings-presence", func(v *api.CameraProvisionState) {
			v.GetPreset().GetValue().GetAlternatives()[0].SetPreserve([]string{camera.PresetFieldFPS})
			v.GetPreset().SetAlternativePreservePresent([]bool{false})
		}},
		{"unknown-state-field", func(v *api.CameraProvisionState) { v.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01}) }},
		{"unknown-nested-field", func(v *api.CameraProvisionState) { v.GetPlan().ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01}) }},
		{"timestamp", func(v *api.CameraProvisionState) { v.SetCreatedAt(&timestamppb.Timestamp{Seconds: 253402300800}) }},
		{"nan", func(v *api.CameraProvisionState) { v.GetVerified().SetArrivalFps(math.NaN()) }},
		{"invalid-utf8", func(v *api.CameraProvisionState) { v.GetRequest().SetName(string([]byte{0xff})) }},
		{"negative", func(v *api.CameraProvisionState) { v.GetPlan().GetAfter().SetWidth(-1) }},
		{"repeated-bound", func(v *api.CameraProvisionState) { v.GetPlan().GetChanges().SetItems(make([]string, 257)) }},
		{"wire-size-bound", func(v *api.CameraProvisionState) {
			v.GetPlan().GetChanges().SetItems([]string{strings.Repeat("s", MaxStateBytes)})
		}},
		{"credentials", func(v *api.CameraProvisionState) {
			v.GetPlan().GetAfter().SetStreamUri("rtsp://admin:secret@192.0.2.1/stream")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			v, err := Encode(checkpoint())
			require.NoError(t, err)
			test.edit(v)
			got, err := Decode(v)
			require.ErrorIs(t, err, ErrInvalid)
			require.Equal(t, provision.Run{}, got)
			require.EqualError(t, err, "camera progress: invalid checkpoint")
		})
	}
	got, err := Decode(nil)
	require.ErrorIs(t, err, ErrInvalid)
	require.Equal(t, provision.Run{}, got)
}

func TestCheckpointRejectsUnclassifiedAuthenticationQueries(t *testing.T) {
	for _, key := range []string{"opaque_auth", "authorization", "key", "custom_access"} {
		t.Run(key, func(t *testing.T) {
			raw := "rtsp://192.0.2.1/stream?" + key + "=private-query-material"
			r := checkpoint()
			r.Plan.After.StreamURI = raw
			_, err := Encode(r)
			require.ErrorIs(t, err, ErrInvalid, "unknown vendor selectors cannot carry credentials into CP state")
			value, err := Encode(checkpoint())
			require.NoError(t, err)
			value.GetPlan().GetAfter().SetStreamUri(raw)
			_, err = Decode(value)
			require.ErrorIs(t, err, ErrInvalid)
		})
	}
}
