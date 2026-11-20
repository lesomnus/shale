package core

import (
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/preset"
	presetwire "github.com/lesomnus/shale/internal/camerapreset"
	"github.com/lesomnus/shale/internal/cameraprogress"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
)

// timeSyncProgress is a job whose pinned configuration asks for sync, on a
// network of 192.0.2.0/24.
func timeSyncProgress(t *testing.T, sync camera.TimeSync) progressFixture {
	t.Helper()
	f := newProgressFixture(t, false)
	f.j.GetInputs().GetConfiguration().SetTimeSync(presetwire.EncodeTimeSync(sync))
	pin, err := f.c.cameraResolvePreset(t.Context(), f.j, f.a, f.d, f.d.GetIdentity())
	require.NoError(t, err)
	portable, err := presetwire.DecodeSnapshot(pin)
	require.NoError(t, err)
	candidates, err := portable.Candidates()
	require.NoError(t, err)
	f.run.Preset, f.run.Effective, f.pin = &portable, &candidates[0], pin
	return f
}

func timeSyncPlan(sync camera.TimeSync, server string) *provision.TimeSyncState {
	return &provision.TimeSyncState{Plan: &camera.InitializationPlan{After: camera.DeviceSettings{TimeSync: sync}, Changes: []string{camera.TimeSyncChangeNTP}, Revision: "fixture-time", TimeSync: &camera.InitializationTimeSync{Server: server, BeforeNTP: []string{"pool.ntp.example"}}}}
}

func TestCameraProgressTimeSyncServerStaysInsideTheCameraNetwork(t *testing.T) {
	edge := camera.TimeSync{NTP: camera.TimeSyncEdge, TimeZone: "KST-9"}
	f := timeSyncProgress(t, edge)
	f.run.TimeSync = timeSyncPlan(edge, "192.0.2.1")
	_, err := f.check(t)
	require.NoError(t, err, "edge resolved to the worker's address on 192.0.2.0/24")
	for _, server := range []string{"198.51.100.1", "127.0.0.1", "192.0.3.1"} {
		f.run.TimeSync = timeSyncPlan(edge, server)
		_, err = f.check(t)
		require.Error(t, err, server)
	}

	explicit := camera.TimeSync{NTP: "192.0.2.7", TimeZone: "UTC0"}
	f = timeSyncProgress(t, explicit)
	f.run.TimeSync = timeSyncPlan(explicit, "192.0.2.7")
	_, err = f.check(t)
	require.NoError(t, err)
	f.run.TimeSync = timeSyncPlan(explicit, "192.0.2.1")
	_, err = cameraprogress.Encode(f.run)
	require.Error(t, err, "an explicit server is written as itself; no checkpoint can say otherwise")
	f.run.TimeSync = timeSyncPlan(edge, "192.0.2.1")
	_, err = f.check(t)
	require.Error(t, err, "a plan for another intent")

	f = newProgressFixture(t, false)
	f.run.TimeSync = timeSyncPlan(edge, "192.0.2.1")
	_, err = f.check(t)
	require.Error(t, err, "a job that never asked for a time sync cannot journal one")
	f.run.TimeSync = &provision.TimeSyncState{}
	_, err = f.check(t)
	require.Error(t, err, "an unsettled time sync names its plan")
}

func TestCameraProgressTimeSyncOutcomeIsKept(t *testing.T) {
	edge := camera.TimeSync{NTP: camera.TimeSyncEdge, TimeZone: "KST-9"}
	f := timeSyncProgress(t, edge)
	f.run.TimeSync = timeSyncPlan(edge, "192.0.2.1")
	f.run.TimeSync.Settled = true
	f.run.Warnings = []camera.PresetWarning{{Code: provision.WarningTimeSyncUnconverged, Field: camera.PresetFieldTimeSync, Requested: edge.String(), Applied: "ntp=192.0.2.1 timezone=KST-9 offset=-12700800000ms", Reason: "fixture"}}
	observation := progressObservation(t, f.run)
	pin, err := f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, observation)
	require.NoError(t, err)
	f.j.SetPinnedPreset(pin)
	f.j.SetObserved(observation)
	f.run.Version++
	f.run.TimeSync = nil
	_, err = f.check(t)
	require.Error(t, err, "a recorded outcome cannot be cleared")
	f.run.TimeSync = timeSyncPlan(edge, "192.0.2.2")
	f.run.TimeSync.Settled = true
	_, err = f.check(t)
	require.Error(t, err, "nor replaced")
}

// An old worker decodes a pinned snapshot without the field it does not know,
// and reports that snapshot: the CP refuses it before any device write, so a
// time sync is never reported applied by a worker that cannot apply it.
func TestCameraProgressRefusesASnapshotThatDroppedTimeSync(t *testing.T) {
	edge := camera.TimeSync{NTP: camera.TimeSyncEdge, TimeZone: "KST-9"}
	f := timeSyncProgress(t, edge)
	old := f.run.Preset.Clone()
	old.Requested.Initialization.TimeSync = camera.TimeSync{}
	candidates, err := old.Candidates()
	require.NoError(t, err)
	f.run.Preset, f.run.Effective = &old, &candidates[0]
	_, err = f.check(t)
	require.ErrorContains(t, err, "differs from the resolved immutable job configuration")
	// With the pin already stored, the same.
	f = timeSyncProgress(t, edge)
	f.j.SetPinnedPreset(f.pin)
	f.run.Preset, f.run.Effective = &old, &candidates[0]
	_, err = f.check(t)
	require.ErrorContains(t, err, "progress.preset")
}

func TestCameraPolicyRefusesAnExplicitTimeServerOutsideItsNetwork(t *testing.T) {
	tenant := pdid.New(DomTenant)
	network := api.CameraNetwork_builder{Id: pdid.New(DomCameraNetwork).Bytes(), Cidrs: []string{"192.0.2.0/24"}}.Build()
	other := pdid.New(DomCameraNetwork).String()
	rule := func(id, scope string, sync camera.TimeSync) preset.Rule {
		return preset.Rule{ID: id, Scope: camera.EnrollmentScope{TenantID: tenant.String(), NetworkID: scope}, Settings: camera.PresetSettings{Initialization: camera.DeviceSettings{TimeSync: sync}}}
	}
	presetOf := func(rules ...preset.Rule) *api.CameraPreset {
		wire, err := presetwire.EncodeRules(rules)
		require.NoError(t, err)
		return api.CameraPreset_builder{Rules: wire}.Build()
	}
	require.NoError(t, cameraPolicyTimeServers(network, presetOf(rule("edge", "", camera.TimeSync{NTP: "edge", TimeZone: "KST-9"}))), "edge resolves on the worker")
	require.NoError(t, cameraPolicyTimeServers(network, presetOf(rule("inside", "", camera.TimeSync{NTP: "192.0.2.1", TimeZone: "KST-9"}))))
	require.Error(t, cameraPolicyTimeServers(network, presetOf(rule("outside", "", camera.TimeSync{NTP: "203.0.113.5", TimeZone: "KST-9"}))))
	require.NoError(t, cameraPolicyTimeServers(network, presetOf(rule("elsewhere", other, camera.TimeSync{NTP: "203.0.113.5", TimeZone: "KST-9"}))), "a rule for another network does not apply here")
	r := rule("fallback", "", camera.TimeSync{NTP: "192.0.2.1", TimeZone: "KST-9"})
	r.Fallbacks = []camera.PresetSettings{{Initialization: camera.DeviceSettings{TimeSync: camera.TimeSync{NTP: "203.0.113.5", TimeZone: "UTC0"}}}}
	require.Error(t, cameraPolicyTimeServers(network, presetOf(r)), "nor in an alternative")
}
