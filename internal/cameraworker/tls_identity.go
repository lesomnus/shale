package cameraworker

import (
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameratls"
)

// DeviceTLSIdentity is the key a step must pin: the job's approved key, or the
// key this job accepted right after its own firmware upgrade. The CP validated
// that checkpoint against the device's recorded MAC and serial before storing
// it; a key for another device is ignored.
func DeviceTLSIdentity(w *api.CameraWorkerWork) *camera.DeviceTLSIdentity {
	pin := cameratls.Decode(w.GetJob().GetInputs().GetDeviceTlsIdentity())
	if pin == nil {
		return nil
	}
	firmware := w.GetJob().GetObserved().GetProgress().GetFirmware()
	rotated := cameratls.Decode(firmware.GetTlsIdentity())
	if !firmware.GetComplete() || rotated == nil || rotated.DeviceID != pin.DeviceID || rotated.Validate() != nil {
		return pin
	}
	return rotated
}
