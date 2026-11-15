package cameraprogress

import (
	"strings"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
)

func TestChangedTLSIdentityCheckpointRoundTrip(t *testing.T) {
	want := checkpoint()
	device := string(want.Request.Device.ID)
	want.Request.DeviceTLSIdentity = &camera.DeviceTLSIdentity{DeviceID: device, SPKISHA256: "sha256:" + strings.Repeat("a", 64)}
	want.ChangedTLSIdentity = &camera.DeviceTLSIdentity{DeviceID: device, SPKISHA256: "sha256:" + strings.Repeat("b", 64)}
	wire, got := wireRoundTrip(t, want)
	require.Equal(t, want, got)
	require.Equal(t, "sha256:"+strings.Repeat("b", 64), wire.GetChangedTlsIdentity().GetSpkiSha256())

	// After its own upgrade accepted a key, a job's pin is that key.
	rotated := &camera.DeviceTLSIdentity{DeviceID: device, SPKISHA256: "sha256:" + strings.Repeat("c", 64)}
	want.Firmware = &provision.FirmwareState{From: "1.2.0", To: "1.2.3", ImageSHA256: firmwareDigest, Complete: true, TLSIdentity: rotated}
	_, got = wireRoundTrip(t, want)
	require.Equal(t, want.ChangedTLSIdentity, got.ChangedTLSIdentity)
}

func TestChangedTLSIdentityCheckpointNeedsAPinForTheSameDevice(t *testing.T) {
	base := checkpoint()
	device := string(base.Request.Device.ID)
	pin := &camera.DeviceTLSIdentity{DeviceID: device, SPKISHA256: "sha256:" + strings.Repeat("a", 64)}
	for name, test := range map[string]struct {
		pin, changed *camera.DeviceTLSIdentity
	}{
		"no pin":         {nil, &camera.DeviceTLSIdentity{DeviceID: device, SPKISHA256: "sha256:" + strings.Repeat("b", 64)}},
		"same as pin":    {pin, &camera.DeviceTLSIdentity{DeviceID: device, SPKISHA256: pin.SPKISHA256}},
		"another device": {pin, &camera.DeviceTLSIdentity{DeviceID: "another-device", SPKISHA256: "sha256:" + strings.Repeat("b", 64)}},
		"malformed":      {pin, &camera.DeviceTLSIdentity{DeviceID: device, SPKISHA256: "sha256:" + strings.Repeat("B", 64)}},
	} {
		r := checkpoint()
		r.Request.DeviceTLSIdentity = test.pin
		r.ChangedTLSIdentity = test.changed
		_, err := Encode(r)
		require.ErrorIs(t, err, ErrInvalid, name)
	}
}
