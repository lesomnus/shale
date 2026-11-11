package core

import (
	"testing"

	"github.com/lesomnus/shale/camera"
	presetwire "github.com/lesomnus/shale/internal/camerapreset"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
)

func smartCheckpointPlan() camera.InitializationPlan {
	return camera.InitializationPlan{Before: camera.DeviceSettings{SmartCodec: "on"}, After: camera.DeviceSettings{SmartCodec: "off"}, Changes: []string{"smart_codec"}, Revision: "smart-plan", VideoMainBefore: &camera.InitializationVideoMain{EncodeType: "H264", Resolution: "1920*1080", FrameRate: "65566", BitrateType: "vbr", Quality: "1", Bitrate: "4096", SmartCodec: "on", AdaptiveMode: "off"}}
}

func TestSmartCodecCheckpointCorePinsAndRejectsRebaselining(t *testing.T) {
	f := newProgressFixture(t, false)
	configuration := f.j.GetInputs().GetConfiguration()
	configuration.SetPowerLineFrequency(0)
	configuration.SetSmartCodec("off")
	pin, err := f.c.cameraResolvePreset(t.Context(), f.j, f.a, f.d, f.d.GetIdentity())
	require.NoError(t, err)
	portable, err := presetwire.DecodeSnapshot(pin)
	require.NoError(t, err)
	candidates, err := portable.Candidates()
	require.NoError(t, err)
	f.run.Preset, f.run.Effective = &portable, &candidates[0]
	p := smartCheckpointPlan()
	f.run.InitializationPlan, f.run.Initialized = &p, &p.After
	f.run.Plan.Revision = candidates[0].Video.Revision
	observation := progressObservation(t, f.run)
	got, err := f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, observation)
	require.NoError(t, err)
	require.NotNil(t, got)
	f.j.SetPinnedPreset(got)
	f.j.SetObserved(observation)
	// The exact historical checkpoint remains valid for status-only reports.
	_, err = f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, observation)
	require.NoError(t, err)
	f.run.Version++
	f.run.InitializationPlan.VideoMainBefore.Bitrate = "2048"
	_, err = f.c.cameraProgress(t.Context(), f.j, f.a, f.b, f.d, progressObservation(t, f.run))
	require.Error(t, err, "a new checkpoint cannot replace the durable native precondition")
	// A frequency-only job has no authority to attach a valid smart snapshot.
	other := newProgressFixture(t, false)
	other.run.InitializationPlan.VideoMainBefore = p.VideoMainBefore
	_, err = other.c.cameraProgress(t.Context(), other.j, other.a, other.b, other.d, progressObservation(t, other.run))
	require.Error(t, err)
}

func TestSmartCodecCheckpointRequiresExplicitCandidateAndImmutableNativePlan(t *testing.T) {
	candidate := camera.PresetSettings{Initialization: camera.DeviceSettings{SmartCodec: "off"}}
	p := smartCheckpointPlan()
	run := provision.Run{InitializationPlan: &p}
	require.NoError(t, cameraProgressPlan(run, candidate, nil), "smart-only intent does not require a frequency")
	before := p
	native := *p.VideoMainBefore
	before.VideoMainBefore = &native
	previous := run
	previous.InitializationPlan = &before
	p.VideoMainBefore.Bitrate = "2048"
	require.Error(t, cameraProgressPlan(run, candidate, &previous), "validly typed but changed precondition cannot be rebaselined")
	p.VideoMainBefore.Bitrate = "4096"
	p.Changes = nil
	require.Error(t, cameraProgressPlan(run, candidate, &previous), "a saved write plan cannot be replaced by a later no-op")
	p = smartCheckpointPlan()
	run.InitializationPlan = &p
	p.VideoMainBefore = nil
	require.Error(t, cameraProgressPlan(run, candidate, nil))
	p = smartCheckpointPlan()
	run.InitializationPlan = &p
	require.Error(t, cameraProgressPlan(run, camera.PresetSettings{Initialization: camera.DeviceSettings{PowerLineFrequency: 60}}, nil), "frequency intent cannot carry unsolicited smart snapshot")
	p.After = camera.DeviceSettings{PowerLineFrequency: 60}
	p.Before = camera.DeviceSettings{PowerLineFrequency: 50}
	p.VideoMainBefore = nil
	require.NoError(t, cameraProgressPlan(run, camera.PresetSettings{Initialization: p.After}, nil), "legacy frequency-only plan stays compatible")
	run.InitializationPlan = nil
	run.Initialized = &camera.DeviceSettings{SmartCodec: "off"}
	require.Error(t, cameraProgressPlan(run, candidate, nil), "smart readback must retain its journaled native plan")
}
