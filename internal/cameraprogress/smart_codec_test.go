package cameraprogress

import (
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
)

func TestSmartCodecNativeSnapshotTypedRoundtripAndRejection(t *testing.T) {
	r := checkpoint()
	r.InitializationPlan.Before.SmartCodec = "on"
	r.InitializationPlan.After.SmartCodec = "off"
	r.Initialized.SmartCodec = "off"
	r.InitializationPlan.VideoMainBefore = &camera.InitializationVideoMain{EncodeType: "H264", Resolution: "1920*1080", FrameRate: "65566", BitrateType: "vbr", Quality: "1", Bitrate: "4096", SmartCodec: "on", AdaptiveMode: "off"}
	r.Effective.Initialization.SmartCodec = "off"
	r.Preset.Requested.Initialization.SmartCodec = "off"
	wire, err := Encode(r)
	require.NoError(t, err)
	back, err := Decode(wire)
	require.NoError(t, err)
	require.Equal(t, r.InitializationPlan, back.InitializationPlan)
	require.NotSame(t, r.InitializationPlan.VideoMainBefore, back.InitializationPlan.VideoMainBefore)
	wire.GetInitializationPlan().GetVideoMainBefore().SetBitrate("https://private")
	_, err = Decode(wire)
	require.Error(t, err)
	r.InitializationPlan.VideoMainBefore.FrameRate = "private"
	_, err = Encode(r)
	require.Error(t, err)
}
