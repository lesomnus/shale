package drivers

import (
	"context"
	"net"
	"net/url"
	"strconv"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/tlsidentity"
	"github.com/lesomnus/shale/camera/vendors/tplink"
)

// UpgradeFirmware qualifies the current authenticated identity, as
// initialization does, before writing an image through the pinned web client.
func (d *Driver) UpgradeFirmware(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease, file camera.FirmwareFile) error {
	if _, err := d.endpoint(ctx, h); err != nil {
		return err
	}
	v, fresh, err := d.qualifiedVendor(ctx, h, lease)
	if err != nil {
		return err
	}
	return v.UpgradeFirmware(ctx, fresh, lease, file)
}

// ConfirmWebIdentity observes the key the camera's web port presents now and
// logs in with lease over a connection pinned to that key. Only a driver that
// already pins this device's key may do this: it is the step that replaces
// that pin after an upgrade, never a first trust.
func (d *Driver) ConfirmWebIdentity(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease) (camera.DeviceTLSIdentity, error) {
	endpoint, err := d.endpoint(ctx, h)
	if err != nil {
		return camera.DeviceTLSIdentity{}, err
	}
	if d.identity == nil || d.bootstrap == nil || d.bootstrap.WebScheme != "https" {
		return camera.DeviceTLSIdentity{}, camera.ErrInvalid
	}
	if lease == nil {
		return camera.DeviceTLSIdentity{}, camera.ErrAuthRequired
	}
	web := &url.URL{Scheme: "https", Host: net.JoinHostPort(endpoint.Hostname(), strconv.Itoa(d.bootstrap.WebPort))}
	pin, err := tlsidentity.Observe(ctx, web.String())
	if err != nil {
		return camera.DeviceTLSIdentity{}, err
	}
	client, transport, err := d.pinnedWebClient(pin, d.timeout)
	if err != nil {
		return camera.DeviceTLSIdentity{}, err
	}
	defer transport.CloseIdleConnections()
	v := tplink.NewDriver(d.baseline, web.String())
	v.CompatibilityProfile = d.bootstrap.CompatibilityProfile
	v.HTTPClient = client
	h.Evidence = nil
	i, err := v.Inspect(ctx, h, lease)
	if err != nil {
		return camera.DeviceTLSIdentity{}, err
	}
	if !knownAuthenticated(i.Device) {
		return camera.DeviceTLSIdentity{}, camera.ErrUnsupported
	}
	if err := v.VerifyLogin(ctx, lease); err != nil {
		return camera.DeviceTLSIdentity{}, err
	}
	return camera.DeviceTLSIdentity{DeviceID: d.identity.DeviceID, SPKISHA256: pin}, nil
}
