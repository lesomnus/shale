package core

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/preset"
	presetwire "github.com/lesomnus/shale/internal/camerapreset"
	"github.com/lesomnus/shale/internal/cameraprogress"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type progressServer struct {
	api.Server
	network *api.CameraNetwork
	preset  *api.CameraPreset
}

func (s progressServer) CameraNetwork() api.CameraNetworkServiceServer {
	return progressNetwork{value: s.network}
}
func (s progressServer) CameraPreset() api.CameraPresetServiceServer {
	return progressPreset{value: s.preset}
}

type progressNetwork struct {
	api.UnimplementedCameraNetworkServiceServer
	value *api.CameraNetwork
}

func (s progressNetwork) Get(context.Context, *api.CameraNetworkGetRequest) (*api.CameraNetwork, error) {
	return proto.Clone(s.value).(*api.CameraNetwork), nil
}

type progressPreset struct {
	api.UnimplementedCameraPresetServiceServer
	value *api.CameraPreset
}

func (s progressPreset) Get(context.Context, *api.CameraPresetGetRequest) (*api.CameraPreset, error) {
	return proto.Clone(s.value).(*api.CameraPreset), nil
}

type progressFixture struct {
	c       Core
	j       *api.CameraJob
	a       *api.CameraAssignment
	b       *api.CameraBinding
	d       *api.CameraDevice
	network *api.CameraNetwork
	run     provision.Run
	pin     *api.CameraResolvedPreset
}

func newProgressFixture(t *testing.T, fallback bool) progressFixture {
	t.Helper()
	tenantID, setID, producerID, networkID := pdid.New(DomTenant), pdid.New(DomSet), pdid.New(DomProducer), pdid.New(DomCameraNetwork)
	assignmentID, bindingID, deviceID, jobID := pdid.New(DomCameraAssignment), pdid.New(DomCameraBinding), pdid.New(DomCameraDevice), pdid.New(DomCameraJob)
	tenant := api.Tenant_builder{Id: tenantID.Bytes()}.Build()
	a := api.CameraAssignment_builder{Id: assignmentID.Bytes(), Tenant: tenant, Set: api.Set_builder{Id: setID.Bytes()}.Build(), Producer: api.Producer_builder{Id: producerID.Bytes()}.Build(), Network: api.CameraNetwork_builder{Id: networkID.Bytes()}.Build()}.Build()
	b := api.CameraBinding_builder{Id: bindingID.Bytes(), Tenant: tenant, Assignment: a, Device: api.CameraDevice_builder{Id: deviceID.Bytes()}.Build(), Source: api.Source_builder{Id: pdid.New(DomSource).Bytes()}.Build(), LogicalKey: "main-profile", SourceAlias: "main"}.Build()
	identity := api.CameraIdentityEvidence_builder{Manufacturer: "fixture", Model: "C420", Hardware: "hw1", Serial: "physical-unit-a", Firmware: "1.0", Authenticated: true}.Build()
	d := api.CameraDevice_builder{Id: deviceID.Bytes(), Tenant: tenant, Network: a.GetNetwork(), Identity: identity}.Build()
	n := api.CameraNetwork_builder{Id: networkID.Bytes(), Tenant: tenant, Cidrs: []string{"192.0.2.0/24"}}.Build()
	settings := camera.PresetSettings{Initialization: camera.DeviceSettings{PowerLineFrequency: 60}, Video: camera.DesiredState{ProfileToken: "main", Codec: "h264", Width: 1280, Height: 720, FPS: 30, Bitrate: 2_000_000, GOP: 30}}
	configuration, err := presetwire.EncodeSettings(settings)
	require.NoError(t, err)
	input := api.CameraJobInputs_builder{Configuration: configuration, AllowActivation: true, InitialCredential: api.CameraCredentialReference_builder{Provider: "cp", Id: "initial", Version: "v1", Purpose: "initial_admin"}.Build(), ManagementCredential: api.CameraCredentialReference_builder{Provider: "cp", Id: "management", Version: "v2", Purpose: "management"}.Build(), StreamingCredential: api.CameraCredentialReference_builder{Provider: "cp", Id: "streaming", Version: "v3", Purpose: "streaming"}.Build()}.Build()
	var p *api.CameraPreset
	if fallback {
		id := pdid.New(DomCameraPreset)
		rules, err := presetwire.EncodeRules([]preset.Rule{{ID: "default", Scope: camera.EnrollmentScope{TenantID: tenantID.String()}, Settings: settings, Fallbacks: []camera.PresetSettings{{Video: camera.DesiredState{FPS: 15}}}}})
		require.NoError(t, err)
		p = api.CameraPreset_builder{Id: id.Bytes(), Tenant: tenant, Revision: "rules-v1", Rules: rules}.Build()
		input.SetConfiguration(nil)
		input.SetPreset(api.CameraRevisionReference_builder{Id: id.String(), Revision: "rules-v1"}.Build())
	}
	j := api.CameraJob_builder{Id: jobID.Bytes(), Tenant: tenant, Assignment: a, Binding: b, Inputs: input}.Build()
	c := New(progressServer{network: n, preset: p}, &Deps{})
	resolved, err := c.cameraResolvePreset(t.Context(), j, a, d, identity)
	require.NoError(t, err)
	portable, err := presetwire.DecodeSnapshot(resolved)
	require.NoError(t, err)
	candidates, err := portable.Candidates()
	require.NoError(t, err)
	selected := candidates[0]
	scope := cameraPresetEnrollmentScope(a)
	ref := func(p *api.CameraCredentialReference) *camera.CredentialRef {
		return &camera.CredentialRef{Provider: p.GetProvider(), ID: p.GetId(), Version: p.GetVersion(), Purpose: camera.AuthPurpose(p.GetPurpose()), Scope: scope, Recoverable: true}
	}
	now := time.Date(2026, 11, 8, 12, 0, 0, 123456789, time.UTC)
	hint := camera.DeviceHint{ID: camera.DeviceID(deviceID.String()), Endpoints: []camera.Endpoint{{URL: "http://192.0.2.10/onvif/device_service", NetworkScope: networkID.String()}}, Manufacturer: "fixture", Model: "C420", Hardware: "hw1", Firmware: "1.0", Serial: "physical-unit-a", Evidence: []camera.Evidence{{Source: "onvif", Authenticated: true}}}
	before := camera.MediaProfile{Token: "main", Codec: "h264", Width: 1920, Height: 1080, FPS: 60, Bitrate: 8_000_000, GOP: 60, StreamURI: "rtsp://192.0.2.10/stream?channel=1"}
	after := before
	after.Width, after.Height, after.FPS, after.Bitrate, after.GOP = selected.Video.Width, selected.Video.Height, selected.Video.FPS, selected.Video.Bitrate, selected.Video.GOP
	run := provision.Run{Request: provision.Request{ID: jobID.String(), BindingID: b.GetLogicalKey(), Scope: scope, Device: hint, UsePreset: true, Alias: b.GetSourceAlias(), Provider: "cp", InitialRef: ref(input.GetInitialCredential()), ManagementRef: ref(input.GetManagementCredential()), StreamingRef: ref(input.GetStreamingCredential()), AllowActivation: true, ProbeDuration: 5 * time.Second}, Stage: provision.Planned, Version: 1, CreatedAt: now, UpdatedAt: now,
		InitialRef: ref(input.GetInitialCredential()), ManagementRef: ref(input.GetManagementCredential()), StreamingRef: ref(input.GetStreamingCredential()), Binding: provision.BindingState{BindingID: b.GetLogicalKey(), Alias: b.GetSourceAlias()}, Inspection: &camera.Inspection{Device: hint, Activation: camera.ActivationReady, Current: camera.CurrentState{Profiles: []camera.MediaProfile{before}, Firmware: "1.0"}}, Preset: &portable, Effective: &selected, InitializationPlan: &camera.InitializationPlan{Before: camera.DeviceSettings{PowerLineFrequency: 50}, After: selected.Initialization, Changes: []string{camera.PresetFieldFrequency}, Revision: "frequency-v1"}, Initialized: &camera.DeviceSettings{PowerLineFrequency: 60}, Plan: &camera.ChangePlan{ProfileToken: "main", Before: before, After: after, Changes: []string{"width", "height", "fps", "bitrate", "gop"}, Revision: selected.Video.Revision}}
	return progressFixture{c: c, j: j, a: a, b: b, d: d, network: n, run: run, pin: resolved}
}

func progressObservation(t *testing.T, run provision.Run) *api.CameraJobObservation {
	t.Helper()
	state, err := cameraprogress.Encode(run)
	require.NoError(t, err)
	var warnings []*api.CameraWarning
	for _, w := range run.Warnings {
		warnings = append(warnings, api.CameraWarning_builder{Code: w.Code, Field: w.Field, Requested: w.Requested, Applied: w.Applied, Reason: w.Reason}.Build())
	}
	out := api.CameraJobObservation_builder{Progress: state, Warnings: warnings}.Build()
	if run.Plan != nil {
		p := run.Plan.After
		out.SetEffective(api.CameraCaptureProfile_builder{Codec: p.Codec, Width: int32(p.Width), Height: int32(p.Height), Fps: p.FPS, MaxBitrateBps: p.Bitrate}.Build())
	}
	if run.Verified != nil {
		p := run.Verified.Profile
		out.SetVerified(api.CameraMediaEvidence_builder{Profile: api.CameraCaptureProfile_builder{Codec: p.Codec, Width: int32(p.Width), Height: int32(p.Height), Fps: p.FPS, MaxBitrateBps: p.Bitrate}.Build(), Decoded: true, DecodedFrames: run.Verified.DecodedFrames, DurationSeconds: run.Verified.Duration.Seconds(), ObservedAt: timestamppb.New(run.Verified.ObservedAt)}.Build())
	}
	return out
}

func (f progressFixture) check(t *testing.T) (*api.CameraResolvedPreset, error) {
	t.Helper()
	return f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, progressObservation(t, f.run))
}

func TestCameraProgressPinsResolvedIntentAndRetainsClockSemantics(t *testing.T) {
	f := newProgressFixture(t, false)
	pin, err := f.check(t)
	require.NoError(t, err)
	require.True(t, proto.Equal(f.pin, pin))
	f.j.SetPinnedPreset(proto.Clone(pin).(*api.CameraResolvedPreset))
	f.j.SetObserved(progressObservation(t, f.run))
	f.run.Version++
	f.run.UpdatedAt = f.run.CreatedAt.Add(-time.Hour) // not a clock-derived grant
	pin, err = f.check(t)
	require.NoError(t, err)
	require.Nil(t, pin, "subsequent checkpoints cannot replace the stored snapshot")
}

func TestCameraProgressHistoricalStatusAndDropping(t *testing.T) {
	f := newProgressFixture(t, false)
	f.j.SetObserved(progressObservation(t, f.run))
	f.j.SetPinnedPreset(proto.Clone(f.pin).(*api.CameraResolvedPreset))
	f.a.SetProducer(api.Producer_builder{Id: pdid.New(DomProducer).Bytes()}.Build())
	f.network.SetCidrs([]string{"198.51.100.0/24"})
	pin, err := f.check(t)
	require.NoError(t, err, "an unchanged historical state is status, including its old Producer and network observations")
	require.Nil(t, pin)
	f.run.Version++
	_, err = f.check(t)
	require.Error(t, err, "changed state must use today's complete scope")
	_, err = f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, &api.CameraJobObservation{})
	require.Error(t, err, "legacy compatibility cannot silently discard a durable checkpoint")
	f.j.SetObserved(nil)
	_, err = f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, nil)
	require.NoError(t, err, "jobs with no typed state retain explicit legacy compatibility")
}

func TestCameraProgressRejectsForgedRequestRefsAndIdentity(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*progressFixture)
	}{
		{"job", func(f *progressFixture) { f.run.Request.ID = pdid.New(DomCameraJob).String() }},
		{"binding", func(f *progressFixture) { f.run.Request.BindingID = "other-role" }},
		{"device", func(f *progressFixture) {
			f.run.Request.Device.ID = camera.DeviceID(pdid.New(DomCameraDevice).String())
		}},
		{"scope", func(f *progressFixture) {
			f.run.Request.Scope.ProducerID = pdid.New(DomProducer).String()
			for _, r := range []*camera.CredentialRef{f.run.Request.InitialRef, f.run.Request.ManagementRef, f.run.Request.StreamingRef, f.run.InitialRef, f.run.ManagementRef, f.run.StreamingRef} {
				r.Scope = f.run.Request.Scope
			}
		}},
		{"alias", func(f *progressFixture) { f.run.Request.Alias = "other" }},
		{"whole-producer-restart", func(f *progressFixture) { f.run.Request.AllowProducerRestart = true }},
		{"activation", func(f *progressFixture) { f.j.GetInputs().SetAllowActivation(false) }},
		{"request-credential", func(f *progressFixture) { f.run.Request.ManagementRef.Version = "unpinned" }},
		{"state-credential", func(f *progressFixture) { f.run.StreamingRef.ID = "unpinned" }},
		{"missing-pinned-credential", func(f *progressFixture) { f.run.InitialRef = nil }},
		{"unrecoverable", func(f *progressFixture) { f.run.InitialRef.Recoverable = false }},
		{"added-credential", func(f *progressFixture) { f.j.GetInputs().SetStreamingCredential(nil) }},
		{"mixed-provider", func(f *progressFixture) { f.j.GetInputs().GetStreamingCredential().SetProvider("other") }},
		{"binding-state-role", func(f *progressFixture) { f.run.Binding.BindingID = "other-role" }},
		{"binding-state-source", func(f *progressFixture) { f.run.Binding.SourceID = pdid.New(DomSource).String() }},
		{"inspection-id", func(f *progressFixture) { f.run.Inspection.Device.ID = "" }},
		{"missing-initial-inspection", func(f *progressFixture) { f.run.Inspection = nil }},
		{"authenticated-serial", func(f *progressFixture) { f.run.Inspection.Device.Serial = "different-unit" }},
		{"cleared-authenticated-hardware", func(f *progressFixture) { f.run.Inspection.Device.Hardware = "" }},
		{"endpoint-cidr", func(f *progressFixture) {
			f.run.Request.Device.Endpoints = []camera.Endpoint{{URL: "http://198.51.100.1/onvif"}}
		}},
		{"inspection-network", func(f *progressFixture) {
			f.run.Inspection.Device.Endpoints = []camera.Endpoint{{URL: "http://192.0.2.10/onvif", NetworkScope: "other"}}
		}},
		{"profile-cidr", func(f *progressFixture) { f.run.Plan.After.StreamURI = "rtsp://198.51.100.1/stream" }},
		{"preset-forged-settings", func(f *progressFixture) { f.run.Preset.Requested.Video.Width = 640 }},
		{"preset-forged-matches", func(f *progressFixture) { f.run.Preset.MatchedRules = []string{"forged-rule"} }},
		{"effective", func(f *progressFixture) { f.run.Effective.Video.GOP++ }},
		{"frequency-plan", func(f *progressFixture) { f.run.InitializationPlan.After.PowerLineFrequency = 50 }},
		{"frequency-readback", func(f *progressFixture) { f.run.Initialized.PowerLineFrequency = 50 }},
		{"frequency-prerequisite", func(f *progressFixture) { f.run.Initialized = nil }},
		{"video-plan-intent", func(f *progressFixture) { f.run.Plan.After.Bitrate++ }},
		{"video-plan-revision", func(f *progressFixture) { f.run.Plan.Revision = "other" }},
		{"first-version", func(f *progressFixture) { f.run.Version = 2 }},
		{"created-time", func(f *progressFixture) { f.run.CreatedAt = time.Time{} }},
		{"updated-time", func(f *progressFixture) { f.run.UpdatedAt = time.Time{} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newProgressFixture(t, false)
			test.edit(&f)
			pin, err := f.check(t)
			require.Error(t, err)
			require.Nil(t, pin)
		})
	}
}

func TestCameraProgressVerificationMustMatchConfiguredAndReportedMedia(t *testing.T) {
	verified := func(t *testing.T) progressFixture {
		f := newProgressFixture(t, false)
		actual := f.run.Plan.After
		actual.FPS, actual.Bitrate = 29.97, 1_900_000
		f.run.Verified = &camera.VerifiedState{Profile: actual, ProfileRevision: f.run.Effective.Video.Revision, Firmware: "1.0", Complete: true, Decoded: true, DecodedFrames: 150, ObservedAt: f.run.UpdatedAt, Duration: 5 * time.Second}
		return f
	}
	f := verified(t)
	pin, err := f.check(t)
	require.NoError(t, err, "measured bitrate below the configured 2 Mbps ceiling is valid")
	require.NotNil(t, pin)
	for _, test := range []struct {
		name string
		edit func(*progressFixture, *api.CameraJobObservation)
	}{
		{"configured-summary", func(_ *progressFixture, o *api.CameraJobObservation) { o.GetEffective().SetWidth(640) }},
		{"forged-freshness", func(_ *progressFixture, o *api.CameraJobObservation) {
			o.GetVerified().SetObservedAt(timestamppb.New(o.GetVerified().GetObservedAt().AsTime().Add(time.Minute)))
		}},
		{"forged-frame-count", func(_ *progressFixture, o *api.CameraJobObservation) { o.GetVerified().SetDecodedFrames(151) }},
		{"forged-measured-fps", func(_ *progressFixture, o *api.CameraJobObservation) { o.GetVerified().GetProfile().SetFps(30) }},
		{"forged-duration", func(_ *progressFixture, o *api.CameraJobObservation) { o.GetVerified().SetDurationSeconds(6) }},
		{"forged-bitrate", func(_ *progressFixture, o *api.CameraJobObservation) {
			o.GetVerified().GetProfile().SetMaxBitrateBps(1_800_000)
		}},
		{"measured-summary", func(_ *progressFixture, o *api.CameraJobObservation) { o.GetVerified().GetProfile().SetHeight(360) }},
		{"fps-tolerance", func(f *progressFixture, _ *api.CameraJobObservation) { f.run.Verified.Profile.FPS = 25 }},
		{"bitrate-ceiling", func(f *progressFixture, _ *api.CameraJobObservation) { f.run.Verified.Profile.Bitrate = 2_700_000 }},
		{"unknown-explicit-gop", func(f *progressFixture, _ *api.CameraJobObservation) { f.run.Verified.Profile.GOP = 0 }},
		{"excessive-gop", func(f *progressFixture, _ *api.CameraJobObservation) { f.run.Verified.Profile.GOP = 90 }},
		{"wrong-profile-revision", func(f *progressFixture, _ *api.CameraJobObservation) { f.run.Verified.ProfileRevision = "other" }},
		{"wrong-firmware", func(f *progressFixture, _ *api.CameraJobObservation) { f.run.Verified.Firmware = "other" }},
		{"unverified", func(f *progressFixture, _ *api.CameraJobObservation) { f.run.Verified.Decoded = false }},
		{"missing-typed-proof", func(f *progressFixture, _ *api.CameraJobObservation) { f.run.Verified = nil }},
		{"missing-summary-proof", func(_ *progressFixture, o *api.CameraJobObservation) { o.SetVerified(nil) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := verified(t)
			o := progressObservation(t, f.run)
			test.edit(&f, o)
			state, err := cameraprogress.Encode(f.run)
			require.NoError(t, err)
			o.SetProgress(state)
			pin, err := f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, o)
			require.Error(t, err)
			require.Nil(t, pin)
		})
	}
	f = verified(t)
	f.j.SetObserved(progressObservation(t, f.run))
	o := progressObservation(t, f.run)
	o.GetEffective().SetWidth(640)
	_, err = f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, o)
	require.Error(t, err, "unchanged historical progress cannot authorize a different outer capture profile")

	f = newProgressFixture(t, false)
	f.j.SetObserved(progressObservation(t, f.run))
	o = progressObservation(t, f.run)
	o.SetVerified(api.CameraMediaEvidence_builder{Profile: o.GetEffective(), Decoded: true, DecodedFrames: 10, DurationSeconds: 5}.Build())
	_, err = f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, o)
	require.Error(t, err, "an unchanged pre-verification checkpoint cannot authenticate a newly supplied media proof")
}

func TestCameraProgressUnknownConfiguredFieldsCanUseActualMedia(t *testing.T) {
	f := newProgressFixture(t, false)
	candidate := camera.PresetSettings{Video: camera.DesiredState{Revision: "revision"}}
	f.run.Plan.After = camera.MediaProfile{Token: "main"}
	f.run.Verified = &camera.VerifiedState{Profile: camera.MediaProfile{Token: "main", Codec: "h264", Width: 640, Height: 360, FPS: 15, Bitrate: 1_000_000}, ProfileRevision: "revision", Firmware: "1.0", Complete: true, Decoded: true, DecodedFrames: 150, ObservedAt: f.run.UpdatedAt, Duration: 5 * time.Second}
	o := progressObservation(t, f.run)
	o.SetEffective(proto.Clone(o.GetVerified().GetProfile()).(*api.CameraCaptureProfile))
	require.NoError(t, cameraProgressMedia(f.run, candidate, o))
	o.GetEffective().SetKeyframeIntervalMs(1000)
	require.Error(t, cameraProgressMedia(f.run, candidate, o), "a configured GOP is not a substitute for measured cadence")
}

func TestCameraProgressWaitingBeforeCredentialsAndPreset(t *testing.T) {
	f := newProgressFixture(t, false)
	f.j.GetInputs().SetInitialCredential(nil)
	f.j.GetInputs().SetManagementCredential(nil)
	f.j.GetInputs().SetStreamingCredential(nil)
	f.j.GetInputs().SetAllowActivation(false)
	f.run.Request.InitialRef, f.run.Request.ManagementRef, f.run.Request.StreamingRef = nil, nil, nil
	f.run.InitialRef, f.run.ManagementRef, f.run.StreamingRef = nil, nil, nil
	f.run.Request.AllowActivation = false
	f.run.Inspection, f.run.Preset, f.run.Effective, f.run.Plan, f.run.InitializationPlan, f.run.Initialized = nil, nil, nil, nil, nil, nil
	f.run.Stage = provision.WaitingAuth
	pin, err := f.check(t)
	require.NoError(t, err)
	require.Nil(t, pin)
}

func TestCameraProgressRequiresPreservedFallbackWarningsAndSelection(t *testing.T) {
	f := newProgressFixture(t, true)
	candidates, err := f.run.Preset.Candidates()
	require.NoError(t, err)
	f.run.CandidateIndex, f.run.Effective = 1, &candidates[1]
	f.run.Plan.After.FPS = candidates[1].Video.FPS
	f.run.Warnings = camera.WarningsFor(candidates[0], candidates[1])
	pin, err := f.check(t)
	require.NoError(t, err)
	require.True(t, proto.Equal(f.pin, pin))
	observed := progressObservation(t, f.run)
	observed.SetWarnings(nil)
	_, err = f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, observed)
	require.Error(t, err, "the public observation cannot hide an allowed fallback")
	observed = progressObservation(t, f.run)
	observed.GetProgress().GetWarnings().SetItems(nil)
	_, err = f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, observed)
	require.Error(t, err, "the resumed state cannot hide an allowed fallback")
	f.j.SetPinnedPreset(pin)
	f.j.SetObserved(progressObservation(t, f.run))
	f.run.Version++
	f.run.CandidateIndex, f.run.Effective = 0, &candidates[0]
	f.run.Plan.After.FPS, f.run.Warnings = candidates[0].Video.FPS, nil
	_, err = f.check(t)
	require.Error(t, err, "a resumed job cannot try a previously rejected preference again")
}

func TestCameraProgressCannotChangePresetVersionCreationOrClearSelectedState(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*progressFixture)
	}{
		{"preset", func(f *progressFixture) { f.run.Preset.Requested.Video.Width = 640 }},
		{"drop-preset", func(f *progressFixture) { f.run.Preset = nil }},
		{"drop-effective", func(f *progressFixture) {
			f.run.Effective, f.run.Plan, f.run.InitializationPlan, f.run.Initialized = nil, nil, nil, nil
		}},
		{"drop-inspection", func(f *progressFixture) { f.run.Inspection = nil }},
		{"replayed-version-with-edits", func(f *progressFixture) { f.run.Version--; f.run.UpdatedAt = f.run.UpdatedAt.Add(time.Nanosecond) }},
		{"skipped-version", func(f *progressFixture) { f.run.Version++ }},
		{"creation-time", func(f *progressFixture) { f.run.CreatedAt = f.run.CreatedAt.Add(time.Nanosecond) }},
		{"previous-authenticated-identity", func(f *progressFixture) {
			f.d.GetIdentity().SetAuthenticated(false)
			f.run.Inspection.Device.Serial = "different"
		}},
		{"cleared-authenticated-marker", func(f *progressFixture) {
			f.d.GetIdentity().SetAuthenticated(false)
			f.run.Inspection.Device.Evidence = nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newProgressFixture(t, false)
			f.j.SetPinnedPreset(proto.Clone(f.pin).(*api.CameraResolvedPreset))
			f.j.SetObserved(progressObservation(t, f.run))
			f.run.Version++
			test.edit(&f)
			pin, err := f.check(t)
			require.Error(t, err)
			require.Nil(t, pin)
		})
	}
}

func TestCameraProgressRejectsUntypedSecretWithoutEcho(t *testing.T) {
	f := newProgressFixture(t, false)
	observed := progressObservation(t, f.run)
	const secret = "must-never-echo-fixture-password"
	observed.GetProgress().GetPlan().GetAfter().SetStreamUri("rtsp://admin:" + secret + "@192.0.2.10/")
	pin, err := f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, observed)
	require.Error(t, err)
	require.Nil(t, pin)
	require.NotContains(t, err.Error(), secret)
	f.run.Warnings = []camera.PresetWarning{{Code: "extra", Reason: strings.Repeat("r", 257)}}
	pin, err = f.check(t)
	require.Error(t, err)
	require.Nil(t, pin)
}

func TestCameraProgressCannotForgetActivationAttempt(t *testing.T) {
	for _, complete := range []bool{false, true} {
		f := newProgressFixture(t, false)
		f.run.ActivationAttempted, f.run.ActivationComplete = true, complete
		f.j.SetObserved(progressObservation(t, f.run))
		f.j.SetPinnedPreset(proto.Clone(f.pin).(*api.CameraResolvedPreset))
		f.run.Version++
		if complete {
			f.run.ActivationComplete = false
		} else {
			f.run.ActivationAttempted = false
		}
		_, err := f.check(t)
		require.Error(t, err, "a resumed activation must not repeat an uncertain first-password write")
	}
}

func TestCameraProgressCannotReplacePendingPublicationIntent(t *testing.T) {
	for _, edit := range []func(*progressFixture){
		func(f *progressFixture) { f.run.Binding.DeploymentID = "changed-operation" },
		func(f *progressFixture) { f.run.Binding.DeploymentID = "" },
		func(f *progressFixture) { f.run.DeploymentProfile = nil },
		func(f *progressFixture) { f.run.DeploymentProfile.FPS = 15 },
	} {
		f := newProgressFixture(t, false)
		p := f.run.Plan.After
		f.run.Verified = &camera.VerifiedState{Profile: p, ProfileRevision: f.run.Effective.Video.Revision, Firmware: "1.0", Complete: true, Decoded: true, DecodedFrames: 150, ObservedAt: f.run.UpdatedAt, Duration: 5 * time.Second}
		f.run.DeploymentProfile = &p
		f.run.Binding.DeploymentID = "first-publication"
		pin, err := f.check(t)
		require.NoError(t, err)
		f.j.SetPinnedPreset(pin)
		f.j.SetObserved(progressObservation(t, f.run))
		f.run.Version++
		edit(&f)
		_, err = f.check(t)
		require.Error(t, err)
	}
}

func TestCameraProgressPreconfigurationCannotAuthorizeOuterMediaProof(t *testing.T) {
	for _, resolved := range []bool{false, true} {
		f := newProgressFixture(t, false)
		f.run.Effective, f.run.Plan, f.run.Verified, f.run.InitializationPlan, f.run.Initialized = nil, nil, nil, nil, nil
		if !resolved {
			f.run.Preset = nil
		}
		o := progressObservation(t, f.run)
		profile := api.CameraCaptureProfile_builder{Codec: "h264", Width: 640, Height: 360, Fps: 15, MaxBitrateBps: 1_000_000}.Build()
		o.SetEffective(profile)
		o.SetVerified(api.CameraMediaEvidence_builder{Profile: profile, Decoded: true, DecodedFrames: 75, DurationSeconds: 5, ObservedAt: timestamppb.New(f.run.UpdatedAt)}.Build())
		_, err := f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, o)
		require.Error(t, err, "an unresolved or unselected checkpoint cannot authenticate a publication by supplying only outer media facts")
	}
}

func TestCameraProgressHistoricalStatusCannotChangePublicationDeclaration(t *testing.T) {
	f := newProgressFixture(t, false)
	f.j.GetInputs().GetConfiguration().GetVideo().SetBitrateBps(0)
	f.run.Preset.Requested.Video.Bitrate = 0
	f.run.Effective.Video.Bitrate = 0
	f.run.Plan.Before.Bitrate = 0
	f.run.Plan.After.Bitrate = 0
	p := f.run.Plan.After
	p.Bitrate = 1_900_000
	f.run.Verified = &camera.VerifiedState{Profile: p, ProfileRevision: f.run.Effective.Video.Revision, Firmware: "1.0", Complete: true, Decoded: true, DecodedFrames: 150, ObservedAt: f.run.UpdatedAt, Duration: 5 * time.Second}
	f.run.DeploymentProfile = &p
	f.run.Binding.DeploymentID = "first-publication"
	o := progressObservation(t, f.run)
	o.SetEffective(captureProfileForProgress(p))
	pin, err := f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, o)
	require.NoError(t, err)
	f.j.SetObserved(proto.Clone(o).(*api.CameraJobObservation))
	f.j.SetPinnedPreset(pin)
	o.GetEffective().SetMaxBitrateBps(2_000_000)
	_, err = f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, o)
	require.Error(t, err, "unchanged progress cannot authorize an outer capture declaration different from its durable publication pin")
}
