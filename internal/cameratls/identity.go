// Package cameratls translates detached, public device trust metadata. It does
// not approve a key, relax transport scope, or authorize credential delivery.
package cameratls

import (
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
)

func Encode(v *camera.DeviceTLSIdentity) *api.CameraDeviceTLSIdentity {
	if v == nil {
		return nil
	}
	return api.CameraDeviceTLSIdentity_builder{DeviceId: v.DeviceID, SpkiSha256: v.SPKISHA256}.Build()
}

func Decode(v *api.CameraDeviceTLSIdentity) *camera.DeviceTLSIdentity {
	if v == nil {
		return nil
	}
	return &camera.DeviceTLSIdentity{DeviceID: v.GetDeviceId(), SPKISHA256: v.GetSpkiSha256()}
}
