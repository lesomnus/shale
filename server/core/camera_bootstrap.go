package core

import (
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/camerabootstrap"
)

func cameraBootstrap(v *api.CameraBootstrapProfile) error {
	if settings := camerabootstrap.Decode(v); settings != nil && settings.Validate() != nil {
		return invalid("bootstrap", "explicit compatibility profile, http/https scheme and valid port are required")
	}
	return nil
}
