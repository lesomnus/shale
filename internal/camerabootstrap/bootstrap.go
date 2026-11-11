// Package camerabootstrap maps explicit bootstrap pins without resolving a
// driver or granting activation. Callers validate syntax and authorization.
package camerabootstrap

import (
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
)

// Decode preserves absence and returns a detached portable value.
func Decode(v *api.CameraBootstrapProfile) *camera.BootstrapSettings {
	if v == nil {
		return nil
	}
	return &camera.BootstrapSettings{CompatibilityProfile: v.GetCompatibilityProfile(), WebScheme: v.GetWebScheme(), WebPort: int(v.GetWebPort())}
}

// Encode preserves absence and returns a detached wire value.
func Encode(v *camera.BootstrapSettings) *api.CameraBootstrapProfile {
	if v == nil {
		return nil
	}
	return api.CameraBootstrapProfile_builder{CompatibilityProfile: v.CompatibilityProfile, WebScheme: v.WebScheme, WebPort: int32(v.WebPort)}.Build()
}
