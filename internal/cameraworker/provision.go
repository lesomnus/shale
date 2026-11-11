package cameraworker

import (
	"context"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
)

// Provisioner adapts the standalone portable engine to a CP-leased execution.
// Auth and publication remain injected so independent and combined roles use
// the same workflow without importing one another's machine authority.
type Provisioner struct {
	Devices   LeasedDevices
	Presets   api.CameraPresetServiceClient
	Auth      func(*Execution) camera.AuthProvider
	Publisher func(*Execution) provision.SourcePublisher
	// Images fetches firmware a preset requires. Without it such a device
	// is unsupported rather than upgraded.
	Images camera.FirmwareSource
}

func (p Provisioner) Execute(ctx context.Context, e *Execution) error {
	if e == nil || p.Auth == nil || p.Publisher == nil {
		return camera.ErrInvalid
	}
	store := &JobStore{Execution: e}
	// Load before choosing the request so historical authenticated identity,
	// pinning and intent survive a replacement process. Load rebinds only the
	// authorized delivery scope and latest endpoint hints.
	q, err := store.Request(ctx)
	if err != nil {
		return err
	}
	d := p.Devices
	d.Execution = e
	engine := provision.Engine{Store: store, Auth: p.Auth(e), Inspector: d, Configurator: d, Bootstrapper: d, Verifier: d, Initializer: d, Presets: JobPresets{Execution: e, Client: p.Presets}, Publisher: p.Publisher(e), Firmware: d, Images: p.Images}
	_, err = engine.Execute(ctx, q, true)
	return err
}
