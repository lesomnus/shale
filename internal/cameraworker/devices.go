package cameraworker

import (
	"context"
	"net/netip"
	"net/url"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
)

// LeasedDevices wraps even read-only LAN operations in a fresh CP grant and
// deadline. Discovery endpoints are transport hints; an inventory ID remains
// mandatory, and today's CP endpoints replace only the transport portion.
type LeasedDevices struct {
	Execution    *Execution
	Inspector    camera.Inspector
	Configurator camera.VideoConfigurator
	Bootstrapper camera.Bootstrapper
	Initializer  camera.DeviceInitializer
	Verifier     camera.MediaVerifier
}

func freshTransport(h camera.DeviceHint, w *api.CameraWorkerWork) (camera.DeviceHint, error) {
	fresh := deviceHint(w)
	if h.ID == "" || h.ID != fresh.ID || len(fresh.Endpoints) == 0 {
		return h, camera.ErrForbidden
	}
	h.Endpoints = fresh.Endpoints
	return h, nil
}

func (d LeasedDevices) step(ctx context.Context, h camera.DeviceHint, f func(context.Context, camera.DeviceHint) error) error {
	if d.Execution == nil {
		return camera.ErrInvalid
	}
	err := d.Execution.Step(ctx, func(ctx context.Context, w *api.CameraWorkerWork) error {
		h, err := freshTransport(h, w)
		if err != nil {
			return err
		}
		return f(ctx, h)
	})
	if err != nil {
		return privateExecutionError{err}
	}
	return nil
}

func (d LeasedDevices) Inspect(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease) (out camera.Inspection, err error) {
	if d.Inspector == nil {
		return out, camera.ErrInvalid
	}
	err = d.step(ctx, h, func(ctx context.Context, h camera.DeviceHint) error {
		var e error
		out, e = d.Inspector.Inspect(ctx, h, lease)
		return e
	})
	return out, err
}
func (d LeasedDevices) Plan(ctx context.Context, inspection camera.Inspection, desired camera.DesiredState) (out camera.ChangePlan, err error) {
	if d.Configurator == nil {
		return out, camera.ErrInvalid
	}
	err = d.step(ctx, inspection.Device, func(ctx context.Context, h camera.DeviceHint) error {
		var e error
		inspection.Device = h
		out, e = d.Configurator.Plan(ctx, inspection, desired)
		return e
	})
	return out, err
}
func (d LeasedDevices) Apply(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease, p camera.ChangePlan) (out camera.CurrentState, err error) {
	if d.Configurator == nil {
		return out, camera.ErrInvalid
	}
	err = d.step(ctx, h, func(ctx context.Context, h camera.DeviceHint) error {
		var e error
		out, e = d.Configurator.Apply(ctx, h, lease, p)
		return e
	})
	return out, err
}
func (d LeasedDevices) Activation(ctx context.Context, h camera.DeviceHint) (out camera.ActivationState, err error) {
	if d.Bootstrapper == nil {
		return camera.ActivationUnknown, nil
	}
	err = d.step(ctx, h, func(ctx context.Context, h camera.DeviceHint) error {
		var e error
		out, e = d.Bootstrapper.Activation(ctx, h)
		return e
	})
	return out, err
}
func (d LeasedDevices) Activate(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease) error {
	if d.Bootstrapper == nil {
		return camera.ErrManualActivation
	}
	return d.step(ctx, h, func(ctx context.Context, h camera.DeviceHint) error { return d.Bootstrapper.Activate(ctx, h, lease) })
}
func (d LeasedDevices) PlanInitialization(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease, settings camera.DeviceSettings) (out camera.InitializationPlan, err error) {
	if d.Initializer == nil {
		return out, camera.ErrUnsupported
	}
	err = d.step(ctx, h, func(ctx context.Context, h camera.DeviceHint) error {
		var e error
		out, e = d.Initializer.PlanInitialization(ctx, h, lease, settings)
		return e
	})
	return out, err
}
func (d LeasedDevices) ApplyInitialization(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease, p camera.InitializationPlan) (out camera.DeviceSettings, err error) {
	if d.Initializer == nil {
		return out, camera.ErrUnsupported
	}
	err = d.step(ctx, h, func(ctx context.Context, h camera.DeviceHint) error {
		var e error
		out, e = d.Initializer.ApplyInitialization(ctx, h, lease, p)
		return e
	})
	return out, err
}
func (d LeasedDevices) Verify(ctx context.Context, r camera.MediaRequest) (out camera.VerifiedState, err error) {
	if d.Verifier == nil || d.Execution == nil {
		return out, camera.ErrInvalid
	}
	err = d.Execution.Step(ctx, func(ctx context.Context, w *api.CameraWorkerWork) error {
		h, e := freshTransport(r.Device, w)
		if e != nil {
			return e
		}
		scope := enrollmentScope(w.GetAssignment())
		if r.Scope != scope || r.NetworkScope != scope.NetworkID || !currentMediaPrefix(r.Profile.StreamURI, w.GetNetwork().GetCidrs()) {
			return camera.ErrForbidden
		}
		r.Device = h
		out, e = d.Verifier.Verify(ctx, r)
		return e
	})
	if err != nil {
		err = privateExecutionError{err}
	}
	return out, err
}

// A pending RTSP URI is not yet the binding's desired input. It therefore
// needs a fresh prefix check even when the CP can retain the management
// endpoint while narrowing the network between checkpoint and probe.
func currentMediaPrefix(raw string, cidrs []string) bool {
	u, err := url.Parse(raw)
	if err != nil || len(raw) > 2048 || u.User != nil || u.Fragment != "" || u.Opaque != "" || u.Scheme != "rtsp" && u.Scheme != "rtsps" {
		return false
	}
	ip, err := netip.ParseAddr(u.Hostname())
	if err != nil || ip.IsUnspecified() || ip.IsMulticast() || len(cidrs) == 0 || len(cidrs) > 32 {
		return false
	}
	allowed := false
	for _, raw := range cidrs {
		p, err := netip.ParsePrefix(raw)
		if err != nil || p.Bits() == 0 || p != p.Masked() {
			return false
		}
		allowed = allowed || p.Contains(ip.Unmap())
	}
	return allowed
}
