// Package drivers composes the installed ONVIF and narrowly qualified vendor
// adapters. It receives transport policy; it does not obtain authority, select
// credentials, register cameras or infer a first-password protocol.
package drivers

import (
	"context"
	"crypto/x509"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/onvif"
	"github.com/lesomnus/shale/camera/vendors/tplink"
)

type Options struct {
	NetworkID string
	CIDRs     []string
	Bootstrap *camera.BootstrapSettings
	// DeviceTLSIdentity is a centrally approved pin for this exact inventory
	// device. DeviceHosts must come from its current authorized endpoints.
	DeviceTLSIdentity *camera.DeviceTLSIdentity
	DeviceID          string
	DeviceHosts       []string
	// Roots is optional camera certificate trust, never a machine identity.
	Roots   *x509.CertPool
	Timeout time.Duration
}

// Driver is immutable and owns its scoped HTTP transport. Construct one from
// the current operation's grant and Close it after that operation returns.
type Driver struct {
	network      string
	prefixes     []netip.Prefix
	bootstrap    *camera.BootstrapSettings
	client       *http.Client
	transport    *http.Transport
	baseline     *onvif.Client
	identity     *camera.DeviceTLSIdentity
	deviceHosts  map[string]bool
	webClient    *http.Client
	webTransport *http.Transport
	timeout      time.Duration
}

var (
	_ camera.Inspector         = (*Driver)(nil)
	_ camera.VideoConfigurator = (*Driver)(nil)
	_ camera.Bootstrapper      = (*Driver)(nil)
	_ camera.DeviceInitializer = (*Driver)(nil)
	_ camera.FirmwareUpgrader  = (*Driver)(nil)
)

func New(o Options) (*Driver, error) {
	if o.Bootstrap != nil {
		if err := o.Bootstrap.Validate(); err != nil {
			return nil, err
		}
		if o.Bootstrap.CompatibilityProfile != tplink.ProfileC420V122 && o.Bootstrap.CompatibilityProfile != tplink.ProfileC420V120 && o.Bootstrap.CompatibilityProfile != tplink.ProfileC420Known {
			return nil, camera.ErrUnsupported
		}
	}
	prefixes, err := networkPrefixes(o.NetworkID, o.CIDRs)
	if err != nil {
		return nil, err
	}
	if o.Timeout == 0 {
		o.Timeout = 15 * time.Second
	}
	if o.Timeout < time.Millisecond || o.Timeout > 30*time.Second {
		return nil, camera.ErrInvalid
	}
	d := &Driver{network: o.NetworkID, prefixes: prefixes, timeout: o.Timeout}
	if o.Bootstrap != nil {
		copy := *o.Bootstrap
		d.bootstrap = &copy
	}
	if o.DeviceTLSIdentity != nil {
		if o.DeviceTLSIdentity.Validate() != nil || o.DeviceTLSIdentity.DeviceID != o.DeviceID || d.bootstrap == nil || d.bootstrap.WebScheme != "https" || len(o.DeviceHosts) == 0 || len(o.DeviceHosts) > 32 {
			return nil, camera.ErrInvalid
		}
		hosts := make(map[string]bool, len(o.DeviceHosts))
		for _, host := range o.DeviceHosts {
			ip, err := netip.ParseAddr(host)
			if err != nil || ip.String() != host || !d.allowedHost(host) || hosts[host] {
				return nil, camera.ErrInvalid
			}
			hosts[host] = true
		}
		identity := *o.DeviceTLSIdentity
		d.identity, d.deviceHosts = &identity, hosts
	}
	d.client, d.transport = d.httpClient(o.Roots, o.Timeout)
	d.baseline = onvif.NewClient(d.client)
	if d.identity != nil {
		// These explicitly approved C420s can boot with a stale RTC on an
		// isolated LAN. Only WS-Security uses their public UTC sample; neither
		// the camera clock nor host-side authorization time is changed.
		d.baseline.UseDeviceTimeForAuthentication = true
		d.webClient, d.webTransport, err = d.pinnedWebClient(d.identity.SPKISHA256, o.Timeout)
		if err != nil {
			d.Close()
			return nil, err
		}
	}
	return d, nil
}

func (d *Driver) Close() {
	d.transport.CloseIdleConnections()
	if d.webTransport != nil {
		d.webTransport.CloseIdleConnections()
	}
}

func (d *Driver) endpoint(ctx context.Context, h camera.DeviceHint) (*url.URL, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.identity != nil && string(h.ID) != d.identity.DeviceID {
		return nil, camera.ErrForbidden
	}
	if len(h.Endpoints) == 0 || len(h.Endpoints) > 32 {
		return nil, camera.ErrInvalid
	}
	var selected *url.URL
	for _, endpoint := range h.Endpoints {
		if endpoint.NetworkScope != d.network {
			return nil, camera.ErrForbidden
		}
		u, err := publicEndpoint(endpoint.URL)
		if err != nil || !d.allowedHost(u.Hostname()) {
			return nil, camera.ErrForbidden
		}
		if selected == nil && (u.Scheme == "http" || u.Scheme == "https") {
			selected = u
		}
	}
	if selected == nil {
		return nil, camera.ErrInvalid
	}
	return selected, nil
}

func (d *Driver) vendor(ctx context.Context, h camera.DeviceHint) (*tplink.Driver, error) {
	endpoint, err := d.endpoint(ctx, h)
	if err != nil {
		return nil, err
	}
	if d.bootstrap == nil {
		return nil, camera.ErrUnsupported
	}
	// Only the current camera host is reused. A policy can select a known web
	// scheme/port, but cannot redirect a password to another host or URL.
	web := &url.URL{Scheme: d.bootstrap.WebScheme, Host: net.JoinHostPort(endpoint.Hostname(), strconv.Itoa(d.bootstrap.WebPort))}
	v := tplink.NewDriver(d.baseline, web.String())
	v.CompatibilityProfile = d.bootstrap.CompatibilityProfile
	if d.webClient != nil {
		v.HTTPClient = d.webClient
	}
	return v, nil
}

func (d *Driver) Inspect(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease) (camera.Inspection, error) {
	if _, err := d.endpoint(ctx, h); err != nil {
		return camera.Inspection{}, err
	}
	if d.bootstrap == nil {
		return d.baseline.Inspect(ctx, h, lease)
	}
	v, err := d.vendor(ctx, h)
	if err != nil {
		return camera.Inspection{}, err
	}
	// Standard ONVIF inspection and media configuration remain independent of
	// the optional vendor bootstrap pin. Vendor initialization qualifies the
	// authenticated current identity separately before using its web protocol.
	return v.Inspect(ctx, h, lease)
}

func (d *Driver) Plan(ctx context.Context, i camera.Inspection, desired camera.DesiredState) (camera.ChangePlan, error) {
	if _, err := d.endpoint(ctx, i.Device); err != nil {
		return camera.ChangePlan{}, err
	}
	return d.baseline.Plan(ctx, i, desired)
}

func (d *Driver) Apply(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease, plan camera.ChangePlan) (camera.CurrentState, error) {
	if _, err := d.endpoint(ctx, h); err != nil {
		return camera.CurrentState{}, err
	}
	return d.baseline.Apply(ctx, h, lease, plan)
}

func (d *Driver) Activation(ctx context.Context, h camera.DeviceHint) (camera.ActivationState, error) {
	if _, err := d.endpoint(ctx, h); err != nil {
		return camera.ActivationUnknown, err
	}
	if d.bootstrap == nil {
		return camera.ActivationUnknown, nil
	}
	v, err := d.vendor(ctx, h)
	if err != nil {
		return camera.ActivationUnknown, err
	}
	return v.Activation(ctx, h)
}

func (d *Driver) Activate(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease) error {
	if _, err := d.endpoint(ctx, h); err != nil {
		return err
	}
	if d.bootstrap == nil {
		return camera.ErrManualActivation
	}
	v, err := d.vendor(ctx, h)
	if err != nil {
		return err
	}
	return v.Activate(ctx, h, lease)
}

func (d *Driver) PlanInitialization(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease, settings camera.DeviceSettings) (camera.InitializationPlan, error) {
	if _, err := d.endpoint(ctx, h); err != nil {
		return camera.InitializationPlan{}, err
	}
	if settings == (camera.DeviceSettings{}) {
		return camera.InitializationPlan{}, nil
	}
	v, fresh, err := d.qualifiedVendor(ctx, h, lease)
	if err != nil {
		return camera.InitializationPlan{}, err
	}
	return v.PlanInitialization(ctx, fresh, lease, settings)
}

func (d *Driver) ApplyInitialization(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease, plan camera.InitializationPlan) (camera.DeviceSettings, error) {
	if _, err := d.endpoint(ctx, h); err != nil {
		return camera.DeviceSettings{}, err
	}
	if plan.Before == (camera.DeviceSettings{}) && plan.After == (camera.DeviceSettings{}) {
		return camera.DeviceSettings{}, nil
	}
	v, fresh, err := d.qualifiedVendor(ctx, h, lease)
	if err != nil {
		return camera.DeviceSettings{}, err
	}
	return v.ApplyInitialization(ctx, fresh, lease, plan)
}

// A resumed checkpoint can contain yesterday's authenticated firmware hint.
// Qualify the current authenticated reply before using a version-specific web
// protocol. Anonymous first-password activation instead requires the explicit
// bootstrap pin and the Bootstrapper's independently checked state exchange.
func (d *Driver) qualifiedVendor(ctx context.Context, h camera.DeviceHint, lease camera.CredentialLease) (*tplink.Driver, camera.DeviceHint, error) {
	v, err := d.vendor(ctx, h)
	if err != nil {
		return nil, camera.DeviceHint{}, err
	}
	if lease == nil {
		return nil, camera.DeviceHint{}, camera.ErrAuthRequired
	}
	// Caller evidence can describe an earlier process or firmware. Only this
	// operation's successful credentialed inspection may qualify the adapter.
	h.Evidence = nil
	i, err := v.Inspect(ctx, h, lease)
	if err != nil {
		return nil, camera.DeviceHint{}, err
	}
	if !knownAuthenticated(i.Device) {
		return nil, camera.DeviceHint{}, camera.ErrUnsupported
	}
	return v, i.Device, nil
}

func knownAuthenticated(h camera.DeviceHint) bool {
	if !tplink.Match(h) {
		return false
	}
	for _, evidence := range h.Evidence {
		if evidence.Authenticated {
			return true
		}
	}
	return false
}
