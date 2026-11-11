package cameraprogress

import (
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
)

const firmwareDigest = "9460f6c63c06573dbb302ec4b4ee482991caaff817718b919109821591ea669a"

func TestFirmwareUpgradeCheckpointRoundTrip(t *testing.T) {
	want := checkpoint()
	want.Preset.Firmware = &camera.FirmwareUpgrade{From: []string{"1.2.0 Build 251020 Rel.55235n"}, To: "1.2.3", Image: camera.FirmwareImage{URL: "https://static.example/c420.zip", SHA256: firmwareDigest}}
	want.Firmware = &provision.FirmwareState{From: "1.2.0 Build 251020 Rel.55235n", To: "1.2.3", ImageSHA256: firmwareDigest, Complete: true, TLSIdentity: &camera.DeviceTLSIdentity{DeviceID: "inventory-device", SPKISHA256: "sha256:" + firmwareDigest}}
	wire, got := wireRoundTrip(t, want)
	require.Equal(t, want, got)
	require.Equal(t, "1.2.3", wire.GetFirmware().GetTo())
	require.Equal(t, "1.2.3", wire.GetPreset().GetValue().GetFirmware().GetTo())

	started := checkpoint()
	started.Firmware = &provision.FirmwareState{From: "1.2.0", To: "1.2.3", ImageSHA256: firmwareDigest}
	_, got = wireRoundTrip(t, started)
	require.Equal(t, started.Firmware, got.Firmware)
}

func TestFirmwareUpgradeCheckpointRejectsInvalidState(t *testing.T) {
	for name, state := range map[string]provision.FirmwareState{
		"no source":           {To: "1.2.3", ImageSHA256: firmwareDigest},
		"source is target":    {From: "1.2.3", To: "1.2.3", ImageSHA256: firmwareDigest},
		"bad digest":          {From: "1.2.0", To: "1.2.3", ImageSHA256: "abc"},
		"key before complete": {From: "1.2.0", To: "1.2.3", ImageSHA256: firmwareDigest, TLSIdentity: &camera.DeviceTLSIdentity{DeviceID: "inventory-device", SPKISHA256: "sha256:" + firmwareDigest}},
		"malformed key":       {From: "1.2.0", To: "1.2.3", ImageSHA256: firmwareDigest, Complete: true, TLSIdentity: &camera.DeviceTLSIdentity{DeviceID: "inventory-device", SPKISHA256: firmwareDigest}},
	} {
		r := checkpoint()
		r.Firmware = &state
		_, err := Encode(r)
		require.Error(t, err, name)
	}
}
