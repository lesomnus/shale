package cameraworker

import (
	"context"
	"net/netip"
	"net/url"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
)

// DeviceAdapters owns the drivers selected for one leased operation. Release,
// when set, runs once after that operation, including construction/driver errors
// and cancellation. A Factory returning partial adapters with an error must
// include their cleanup here.
type DeviceAdapters struct {
	Inspector    camera.Inspector
	Configurator camera.VideoConfigurator
	Bootstrapper camera.Bootstrapper
	Initializer  camera.DeviceInitializer
	Verifier     camera.MediaVerifier
	Release      func()
}

// LeasedDevices wraps even read-only LAN operations in a fresh CP grant and
// deadline. Discovery endpoints are transport hints; an inventory ID remains
// mandatory, and today's CP endpoints replace only the transport portion.
type LeasedDevices struct {
	Execution *Execution
	// Factory constructs adapters from each fresh Work, including its network
	// and pinned inputs. It must perform no LAN I/O and grants no authority of
	// its own. Factory and static adapters are mutually exclusive.
	Factory      func(context.Context, *api.CameraWorkerWork) (DeviceAdapters, error)
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

func (d LeasedDevices) mixedAdapters() bool {
	return d.Factory != nil && (d.Inspector != nil || d.Configurator != nil || d.Bootstrapper != nil || d.Initializer != nil || d.Verifier != nil)
}

func (d LeasedDevices) adapters(ctx context.Context, w *api.CameraWorkerWork, f func(DeviceAdapters) error) error {
	if d.Factory == nil {
		return f(DeviceAdapters{Inspector: d.Inspector, Configurator: d.Configurator, Bootstrapper: d.Bootstrapper, Initializer: d.Initializer, Verifier: d.Verifier})
	}
	adapters, err := d.Factory(ctx, w)
	if adapters.Release != nil {
		defer adapters.Release()
	}
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return f(adapters)
}

func (d LeasedDevices) step(ctx context.Context, h camera.DeviceHint, f func(context.Context, camera.DeviceHint, DeviceAdapters) error) error {
	if d.Execution == nil || d.mixedAdapters() {
		return camera.ErrInvalid
	}
	err := d.Execution.Step(ctx, func(ctx context.Context, w *api.CameraWorkerWork) error {
		h, err := freshTransport(h, w)
		if err != nil {
			return err
		}
		return d.adapters(ctx, w, func(adapters DeviceAdapters) error { return f(ctx, h, adapters) })
	})
	if err != nil {
		return privateExecutionError{err}
	}
	return nil
}

func (d LeasedDevices) Inspect(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease) (out camera.Inspection, err error) {
	if d.Factory == nil && d.Inspector == nil {
		return out, camera.ErrInvalid
	}
	err = d.step(ctx, h, func(ctx context.Context, h camera.DeviceHint, adapters DeviceAdapters) error {
		if adapters.Inspector == nil {
			return camera.ErrInvalid
		}
		var e error
		out, e = adapters.Inspector.Inspect(ctx, h, lease)
		return e
	})
	return out, err
}
func (d LeasedDevices) Plan(ctx context.Context, inspection camera.Inspection, desired camera.DesiredState) (out camera.ChangePlan, err error) {
	if d.Factory == nil && d.Configurator == nil {
		return out, camera.ErrInvalid
	}
	err = d.step(ctx, inspection.Device, func(ctx context.Context, h camera.DeviceHint, adapters DeviceAdapters) error {
		if adapters.Configurator == nil {
			return camera.ErrInvalid
		}
		var e error
		inspection.Device = h
		out, e = adapters.Configurator.Plan(ctx, inspection, desired)
		return e
	})
	return out, err
}
func (d LeasedDevices) Apply(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease, p camera.ChangePlan) (out camera.CurrentState, err error) {
	if d.Factory == nil && d.Configurator == nil {
		return out, camera.ErrInvalid
	}
	err = d.step(ctx, h, func(ctx context.Context, h camera.DeviceHint, adapters DeviceAdapters) error {
		if adapters.Configurator == nil {
			return camera.ErrInvalid
		}
		var e error
		out, e = adapters.Configurator.Apply(ctx, h, lease, p)
		return e
	})
	return out, err
}
func (d LeasedDevices) Activation(ctx context.Context, h camera.DeviceHint) (out camera.ActivationState, err error) {
	if d.Factory == nil && d.Bootstrapper == nil {
		return camera.ActivationUnknown, nil
	}
	err = d.step(ctx, h, func(ctx context.Context, h camera.DeviceHint, adapters DeviceAdapters) error {
		if adapters.Bootstrapper == nil {
			out = camera.ActivationUnknown
			return nil
		}
		var e error
		out, e = adapters.Bootstrapper.Activation(ctx, h)
		return e
	})
	return out, err
}
func (d LeasedDevices) Activate(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease) error {
	if d.Factory == nil && d.Bootstrapper == nil {
		return camera.ErrManualActivation
	}
	return d.step(ctx, h, func(ctx context.Context, h camera.DeviceHint, adapters DeviceAdapters) error {
		if adapters.Bootstrapper == nil {
			return camera.ErrManualActivation
		}
		return adapters.Bootstrapper.Activate(ctx, h, lease)
	})
}
func (d LeasedDevices) PlanInitialization(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease, settings camera.DeviceSettings) (out camera.InitializationPlan, err error) {
	if d.Factory == nil && d.Initializer == nil {
		return out, camera.ErrUnsupported
	}
	err = d.step(ctx, h, func(ctx context.Context, h camera.DeviceHint, adapters DeviceAdapters) error {
		if adapters.Initializer == nil {
			return camera.ErrUnsupported
		}
		var e error
		out, e = adapters.Initializer.PlanInitialization(ctx, h, lease, settings)
		return e
	})
	return out, err
}
func (d LeasedDevices) ApplyInitialization(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease, p camera.InitializationPlan) (out camera.DeviceSettings, err error) {
	if d.Factory == nil && d.Initializer == nil {
		return out, camera.ErrUnsupported
	}
	err = d.step(ctx, h, func(ctx context.Context, h camera.DeviceHint, adapters DeviceAdapters) error {
		if adapters.Initializer == nil {
			return camera.ErrUnsupported
		}
		var e error
		out, e = adapters.Initializer.ApplyInitialization(ctx, h, lease, p)
		return e
	})
	return out, err
}
func (d LeasedDevices) Verify(ctx context.Context, r camera.MediaRequest) (out camera.VerifiedState, err error) {
	if d.Factory == nil && d.Verifier == nil || d.Execution == nil || d.mixedAdapters() {
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
		return d.adapters(ctx, w, func(adapters DeviceAdapters) error {
			if adapters.Verifier == nil {
				return camera.ErrInvalid
			}
			out, e = adapters.Verifier.Verify(ctx, r)
			return e
		})
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
