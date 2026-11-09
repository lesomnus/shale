package camera_test

import (
	"strings"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
)

func nativeMain() camera.InitializationVideoMain {
	return camera.InitializationVideoMain{EncodeType: "H264", Resolution: "1920*1080", FrameRate: "65566", BitrateType: "vbr", Quality: "1", Bitrate: "4096", SmartCodec: "on", AdaptiveMode: "off"}
}

func TestSmartCodecPresetFieldwiseMergePreserveRequiredAndWarnings(t *testing.T) {
	base := camera.PresetSettings{Initialization: camera.DeviceSettings{PowerLineFrequency: 60, SmartCodec: "off"}}
	require.Equal(t, camera.DeviceSettings{PowerLineFrequency: 50, SmartCodec: "off"}, camera.MergePresetSettings(base, camera.PresetSettings{Initialization: camera.DeviceSettings{PowerLineFrequency: 50}}).Initialization)
	require.Equal(t, camera.DeviceSettings{PowerLineFrequency: 60, SmartCodec: "on"}, camera.MergePresetSettings(base, camera.PresetSettings{Initialization: camera.DeviceSettings{SmartCodec: "on"}}).Initialization)
	require.Equal(t, camera.DeviceSettings{PowerLineFrequency: 60}, camera.MergePresetSettings(base, camera.PresetSettings{Preserve: []string{camera.PresetFieldSmartCodec}}).Initialization)
	p := camera.DevicePreset{Ref: camera.PresetRef{ID: "model", Revision: "1"}, Requested: base, Required: []string{camera.PresetFieldSmartCodec}, Fallbacks: []camera.PresetSettings{{Preserve: []string{camera.PresetFieldSmartCodec}}}}
	require.Error(t, p.Validate())
	p.Required = nil
	require.NoError(t, p.Validate())
	c, err := p.Candidates()
	require.NoError(t, err)
	require.Len(t, c, 2)
	warnings := camera.WarningsFor(c[0], c[1])
	require.Len(t, warnings, 1)
	require.Equal(t, camera.PresetFieldSmartCodec, warnings[0].Field)
	require.Equal(t, "off", warnings[0].Requested)
	require.Equal(t, "preserved", warnings[0].Applied)
	for _, value := range []string{"false", "OFF", "auto", "on\n", "http://private"} {
		require.Error(t, camera.ValidatePresetSettings(camera.PresetSettings{Initialization: camera.DeviceSettings{SmartCodec: value}}))
	}
}

func TestNativeInitializationSnapshotIsClosedAndTiedToExplicitSmartIntent(t *testing.T) {
	v := nativeMain()
	require.NoError(t, v.Validate())
	for _, edit := range []func(*camera.InitializationVideoMain){func(v *camera.InitializationVideoMain) { v.EncodeType = "private://uri" }, func(v *camera.InitializationVideoMain) { v.Resolution = "1920x1080" }, func(v *camera.InitializationVideoMain) { v.Resolution = "9000*1080" }, func(v *camera.InitializationVideoMain) { v.FrameRate = "65536" }, func(v *camera.InitializationVideoMain) { v.Bitrate = "0" }, func(v *camera.InitializationVideoMain) { v.BitrateType = "auto" }, func(v *camera.InitializationVideoMain) { v.Quality = "6" }, func(v *camera.InitializationVideoMain) { v.AdaptiveMode = "" }, func(v *camera.InitializationVideoMain) { v.SmartCodec = "auto" }, func(v *camera.InitializationVideoMain) { v.FrameRate = strings.Repeat("1", 32) }} {
		bad := v
		edit(&bad)
		require.Error(t, bad.Validate())
	}
	want := camera.DeviceSettings{SmartCodec: "off"}
	plan := camera.InitializationPlan{Before: camera.DeviceSettings{SmartCodec: "on"}, After: want, VideoMainBefore: &v}
	require.NoError(t, camera.ValidateInitializationPlan(plan, want))
	require.Error(t, camera.ValidateInitializationPlan(plan, camera.DeviceSettings{}))
	plan.VideoMainBefore = nil
	require.Error(t, camera.ValidateInitializationPlan(plan, want))
	plan.VideoMainBefore = &v
	plan.Before.SmartCodec = "off"
	require.Error(t, camera.ValidateInitializationPlan(plan, want))
	require.NoError(t, camera.ValidateInitializationPlan(camera.InitializationPlan{Before: camera.DeviceSettings{PowerLineFrequency: 50}, After: camera.DeviceSettings{PowerLineFrequency: 60}}, camera.DeviceSettings{PowerLineFrequency: 60}), "old frequency-only plans remain compatible")
}
