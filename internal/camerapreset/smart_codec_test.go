package camerapreset

import (
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
)

func TestSmartCodecSettingsWireRoundtrip(t *testing.T) {
	want := camera.PresetSettings{Initialization: camera.DeviceSettings{PowerLineFrequency: 60, SmartCodec: "off"}, Preserve: []string{camera.PresetFieldBitrate}}
	wire, err := EncodeSettings(want)
	require.NoError(t, err)
	require.Equal(t, "off", wire.GetSmartCodec())
	got, err := DecodeSettings(wire)
	require.NoError(t, err)
	require.Equal(t, want, got)
	wire.SetSmartCodec("auto")
	_, err = DecodeSettings(wire)
	require.Error(t, err)
}
