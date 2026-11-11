package core

import (
	"context"
	"strings"
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/preset"
	presetwire "github.com/lesomnus/shale/internal/camerapreset"
	"github.com/lesomnus/shale/internal/cameratls"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const fixtureImageDigest = "9460f6c63c06573dbb302ec4b4ee482991caaff817718b919109821591ea669a"

type firmwareProgress struct {
	progressFixture
	pin     *api.CameraDeviceTLSIdentity
	started provision.Run
}

// newFirmwareProgress is a pinned preset job on firmware 1.0 whose preset
// requires 1.0 → 2.0. Its first checkpoint announces that upgrade.
func newFirmwareProgress(t *testing.T) firmwareProgress {
	t.Helper()
	f := newProgressFixture(t, true)
	tenant := mustId(f.a.GetTenant().GetId()).String()
	upgrade := &camera.FirmwareUpgrade{From: []string{"1.0"}, To: "2.0", Image: camera.FirmwareImage{URL: "https://static.example/fw.zip", SHA256: fixtureImageDigest}}
	settings := camera.PresetSettings{Video: camera.DesiredState{Codec: "h264"}}
	rules, err := presetwire.EncodeRules([]preset.Rule{{ID: "model", Scope: camera.EnrollmentScope{TenantID: tenant}, Match: preset.Match{Model: "C420"}, Settings: settings, Firmware: upgrade}})
	require.NoError(t, err)
	f.c.Next().(progressServer).preset.SetRules(rules)

	pin := tlsIdentity(f.d.GetId(), "a")
	f.j.GetInputs().SetBootstrap(bootstrapProfile())
	f.j.GetInputs().SetDeviceTlsIdentity(pin)
	f.d.GetIdentity().SetMacs([]string{"a8:29:dc:c6:26:f3"})

	run := f.run
	run.Request.Bootstrap = &camera.BootstrapSettings{CompatibilityProfile: "fixture-first-password-v1", WebScheme: "https", WebPort: 8443}
	run.Request.DeviceTLSIdentity = cameratls.Decode(pin)
	run.Request.Device.MAC = "a8:29:dc:c6:26:f3"
	inspection := *run.Inspection
	inspection.Device.MAC = "A8-29-DC-C6-26-F3"
	run.Inspection = &inspection
	run.Preset, run.Effective, run.InitializationPlan, run.Initialized, run.Plan = nil, nil, nil, nil, nil
	run.Stage = provision.Initializing
	run.Firmware = &provision.FirmwareState{From: "1.0", To: "2.0", ImageSHA256: fixtureImageDigest}
	f.run = run
	return firmwareProgress{progressFixture: f, pin: pin, started: run}
}

// completed is the next checkpoint: the device answers on 2.0 with a new key.
func (f *firmwareProgress) complete(t *testing.T) {
	t.Helper()
	f.j.SetObserved(progressObservation(t, f.run))
	next := f.run
	next.Version++
	inspection := *next.Inspection
	inspection.Device.Firmware, inspection.Current.Firmware = "2.0", "2.0"
	next.Inspection = &inspection
	next.Firmware = &provision.FirmwareState{From: "1.0", To: "2.0", ImageSHA256: fixtureImageDigest, Complete: true, TLSIdentity: cameratls.Decode(tlsIdentity(f.d.GetId(), "b"))}
	f.run = next
}

func TestCameraProgressAuthorizesOnlyThePresetsFirmwareUpgrade(t *testing.T) {
	f := newFirmwareProgress(t)
	pin, err := f.check(t)
	require.NoError(t, err)
	require.Nil(t, pin, "an upgrade happens before the preset is pinned")
	for name, edit := range map[string]func(*provision.Run){
		"another target":  func(r *provision.Run) { r.Firmware.To = "3.0" },
		"another image":   func(r *provision.Run) { r.Firmware.ImageSHA256 = strings.Repeat("0", 64) },
		"another source":  func(r *provision.Run) { r.Firmware.From = "0.9" },
		"already done":    func(r *provision.Run) { r.Firmware.Complete = true },
		"anonymous":       func(r *provision.Run) { r.Inspection.Device.Evidence = nil },
		"after the pin":   func(r *provision.Run) { r.Preset = &camera.DevicePreset{} },
		"not inspected":   func(r *provision.Run) { r.Inspection = nil },
		"inspected build": func(r *provision.Run) { r.Inspection.Device.Firmware = "2.0" },
	} {
		t.Run(name, func(t *testing.T) {
			g := newFirmwareProgress(t)
			inspection := *g.run.Inspection
			g.run.Inspection = &inspection
			state := *g.run.Firmware
			g.run.Firmware = &state
			edit(&g.run)
			if g.run.Preset != nil {
				// A valid snapshot, resolved while the upgrade is still open.
				resolved, err := g.c.cameraResolvePreset(t.Context(), g.j, g.a, g.d, cameraProgressIdentityValue(g.run.Inspection.Device))
				require.NoError(t, err)
				snapshot, err := presetwire.DecodeSnapshot(resolved)
				require.NoError(t, err)
				g.run.Preset = &snapshot
			}
			_, err := g.check(t)
			require.ErrorContains(t, err, "firmware")
		})
	}
	t.Run("preset without a requirement", func(t *testing.T) {
		g := newFirmwareProgress(t)
		rules := g.c.Next().(progressServer).preset.GetRules()
		rules[0].ClearFirmware()
		_, err := g.check(t)
		require.ErrorContains(t, err, "firmware")
	})
}

func TestCameraProgressCompletesAnUpgradeOnTheRecordedDevice(t *testing.T) {
	f := newFirmwareProgress(t)
	_, err := f.check(t)
	require.NoError(t, err)
	f.complete(t)
	_, err = f.check(t)
	require.NoError(t, err)

	record := cameraFirmwareTLSRecord(f.j, f.d, f.run)
	require.NotNil(t, record)
	require.True(t, proto.Equal(f.pin, record.GetReplaced()))
	require.Equal(t, "sha256:"+strings.Repeat("b", 64), record.GetIdentity().GetSpkiSha256())
	require.Equal(t, "2.0", record.GetFirmware())

	for name, edit := range map[string]func(*provision.Run){
		"another MAC":        func(r *provision.Run) { r.Inspection.Device.MAC = "02:00:00:00:00:01" },
		"no MAC":             func(r *provision.Run) { r.Inspection.Device.MAC = "" },
		"not the target":     func(r *provision.Run) { r.Inspection.Device.Firmware = "1.0" },
		"same key":           func(r *provision.Run) { r.Firmware.TLSIdentity.SPKISHA256 = f.pin.GetSpkiSha256() },
		"another device":     func(r *provision.Run) { r.Firmware.TLSIdentity.DeviceID = pdid.New(DomCameraDevice).String() },
		"changed transition": func(r *provision.Run) { r.Firmware.To = "3.0"; r.Inspection.Device.Firmware = "3.0" },
	} {
		t.Run(name, func(t *testing.T) {
			g := newFirmwareProgress(t)
			g.complete(t)
			inspection := *g.run.Inspection
			g.run.Inspection = &inspection
			state := *g.run.Firmware
			key := *state.TLSIdentity
			state.TLSIdentity = &key
			g.run.Firmware = &state
			edit(&g.run)
			_, err := g.check(t)
			require.ErrorContains(t, err, "firmware")
		})
	}
	t.Run("recorded MAC", func(t *testing.T) {
		g := newFirmwareProgress(t)
		g.complete(t)
		g.d.GetIdentity().SetMacs([]string{"02:00:00:00:00:01"})
		_, err := g.check(t)
		require.ErrorContains(t, err, "firmware")
	})
	t.Run("unpinned job", func(t *testing.T) {
		g := newFirmwareProgress(t)
		g.j.GetInputs().ClearDeviceTlsIdentity()
		g.run.Request.DeviceTLSIdentity = nil
		g.complete(t)
		_, err := g.check(t)
		require.ErrorContains(t, err, "firmware")
	})
}

func TestCameraProgressKeepsACompletedUpgrade(t *testing.T) {
	f := newFirmwareProgress(t)
	f.complete(t)
	f.j.SetObserved(progressObservation(t, f.run))
	next := f.run
	next.Version++
	for name, firmware := range map[string]*provision.FirmwareState{
		"forgotten": nil,
		"reopened":  {From: "1.0", To: "2.0", ImageSHA256: fixtureImageDigest},
		"new key":   {From: "1.0", To: "2.0", ImageSHA256: fixtureImageDigest, Complete: true, TLSIdentity: cameratls.Decode(tlsIdentity(f.d.GetId(), "c"))},
	} {
		t.Run(name, func(t *testing.T) {
			r := next
			r.Firmware = firmware
			_, err := f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, progressObservation(t, r))
			require.ErrorContains(t, err, "firmware")
		})
	}
	r := next
	_, err := f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, progressObservation(t, r))
	require.NoError(t, err)
	require.Nil(t, cameraFirmwareTLSRecord(f.j, f.d, r), "only the completing checkpoint records the key")
}

func TestCameraDeviceTLSIdentityFollowsAnAcceptedUpgradeKey(t *testing.T) {
	f := newProgressFixture(t, false)
	approved, rotated, again := tlsIdentity(f.d.GetId(), "a"), tlsIdentity(f.d.GetId(), "b"), tlsIdentity(f.d.GetId(), "c")
	policy := api.CameraPolicy_builder{Enrollment: api.CameraEnrollmentPolicy_builder{Bootstrap: bootstrapProfile(), DeviceTlsIdentities: []*api.CameraDeviceTLSIdentity{approved}}.Build()}.Build()
	require.True(t, proto.Equal(approved, cameraDeviceTLSIdentity(policy, f.d)))

	f.d.SetFirmwareTls(api.CameraFirmwareTLSState_builder{Replaced: approved, Identity: rotated, JobId: f.j.GetId(), Firmware: "2.0", DateAccepted: timestamppb.Now()}.Build())
	require.True(t, proto.Equal(rotated, cameraDeviceTLSIdentity(policy, f.d)))

	// A later upgrade chains from the same approval.
	f.j.GetInputs().SetDeviceTlsIdentity(rotated)
	run := provision.Run{Firmware: &provision.FirmwareState{From: "2.0", To: "3.0", ImageSHA256: fixtureImageDigest, Complete: true, TLSIdentity: cameratls.Decode(again)}}
	record := cameraFirmwareTLSRecord(f.j, f.d, run)
	require.True(t, proto.Equal(approved, record.GetReplaced()))
	f.d.SetFirmwareTls(record)
	require.True(t, proto.Equal(again, cameraDeviceTLSIdentity(policy, f.d)))

	// A new operator approval takes precedence again.
	policy.GetEnrollment().SetDeviceTlsIdentities([]*api.CameraDeviceTLSIdentity{tlsIdentity(f.d.GetId(), "d")})
	require.Equal(t, "sha256:"+strings.Repeat("d", 64), cameraDeviceTLSIdentity(policy, f.d).GetSpkiSha256())
	// No approval, no pin: a recorded key never becomes a first trust.
	policy.GetEnrollment().SetDeviceTlsIdentities(nil)
	require.Nil(t, cameraDeviceTLSIdentity(policy, f.d))
}

type patchDevices struct {
	api.UnimplementedCameraDeviceServiceServer
	patches *[]*api.CameraDevicePatchRequest
}

func (s patchDevices) Patch(_ context.Context, r *api.CameraDevicePatchRequest) (*api.CameraDevice, error) {
	*s.patches = append(*s.patches, proto.Clone(r).(*api.CameraDevicePatchRequest))
	return &api.CameraDevice{}, nil
}

type patchServer struct {
	api.Server
	patches *[]*api.CameraDevicePatchRequest
}

func (s patchServer) CameraDevice() api.CameraDeviceServiceServer {
	return patchDevices{patches: s.patches}
}

func TestCameraReportRecordsTheUpgradedIdentityAndKeyTogether(t *testing.T) {
	f := newFirmwareProgress(t)
	f.complete(t)
	var patches []*api.CameraDevicePatchRequest
	c := New(patchServer{patches: &patches}, &Deps{})
	require.NoError(t, c.cameraProgressIdentityRecord(t.Context(), f.j, f.d, progressObservation(t, f.run)))
	require.Len(t, patches, 1, "one CAS write carries both facts")
	require.Equal(t, "2.0", patches[0].GetIdentity().GetFirmware())
	require.Equal(t, "sha256:"+strings.Repeat("b", 64), patches[0].GetFirmwareTls().GetIdentity().GetSpkiSha256())
	require.True(t, proto.Equal(f.d.GetDateUpdated(), patches[0].GetDateUpdated()))
}
