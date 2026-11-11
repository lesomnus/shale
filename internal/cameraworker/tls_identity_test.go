package cameraworker

import (
	"strings"
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
)

func TestStepPinsTheKeyAcceptedAfterItsOwnUpgrade(t *testing.T) {
	approved := api.CameraDeviceTLSIdentity_builder{DeviceId: "device-a", SpkiSha256: "sha256:" + strings.Repeat("1", 64)}.Build()
	rotated := api.CameraDeviceTLSIdentity_builder{DeviceId: "device-a", SpkiSha256: "sha256:" + strings.Repeat("2", 64)}.Build()
	work := func(firmware *api.CameraProvisionFirmware, pin *api.CameraDeviceTLSIdentity) *api.CameraWorkerWork {
		inputs := api.CameraJobInputs_builder{DeviceTlsIdentity: pin}.Build()
		observed := api.CameraJobObservation_builder{Progress: api.CameraProvisionState_builder{Firmware: firmware}.Build()}.Build()
		return api.CameraWorkerWork_builder{Job: api.CameraJob_builder{Inputs: inputs, Observed: observed}.Build()}.Build()
	}
	complete := func(key *api.CameraDeviceTLSIdentity) *api.CameraProvisionFirmware {
		return api.CameraProvisionFirmware_builder{From: "1.2.0", To: "1.2.2", Complete: true, TlsIdentity: key}.Build()
	}
	require.Nil(t, DeviceTLSIdentity(work(complete(rotated), nil)), "no pin means ordinary PKI, never a first trust")
	require.Equal(t, approved.GetSpkiSha256(), DeviceTLSIdentity(work(nil, approved)).SPKISHA256)
	require.Equal(t, rotated.GetSpkiSha256(), DeviceTLSIdentity(work(complete(rotated), approved)).SPKISHA256)

	started := api.CameraProvisionFirmware_builder{From: "1.2.0", To: "1.2.2", TlsIdentity: rotated}.Build()
	require.Equal(t, approved.GetSpkiSha256(), DeviceTLSIdentity(work(started, approved)).SPKISHA256)
	other := api.CameraDeviceTLSIdentity_builder{DeviceId: "device-b", SpkiSha256: rotated.GetSpkiSha256()}.Build()
	require.Equal(t, approved.GetSpkiSha256(), DeviceTLSIdentity(work(complete(other), approved)).SPKISHA256)
	malformed := api.CameraDeviceTLSIdentity_builder{DeviceId: "device-a", SpkiSha256: "2222"}.Build()
	require.Equal(t, approved.GetSpkiSha256(), DeviceTLSIdentity(work(complete(malformed), approved)).SPKISHA256)
}
