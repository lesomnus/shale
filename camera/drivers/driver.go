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
	// SkipTLSVerification, only from a policy's tls_verification: skip, checks
	// no key on the device's HTTPS web port. DeviceID and DeviceHosts bind it
	// as they bind a pin. RecordedSerial and RecordedMACs, the device's
	// authenticated identity at the CP, must then match an authenticated
	// inspection before any credentialed web call.
	SkipTLSVerification bool
	RecordedSerial      string
	RecordedMACs        []string
	DeviceID            string
	DeviceHosts         []string
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
	deviceID     string
	deviceHosts  map[string]bool
	skip         bool
	recorded     recordedIdentity
	webClient    *http.Client
	webTransport *http.Transport
	timeout      time.Duration
}

var (
	_ camera.Inspector           = (*Driver)(nil)
	_ camera.VideoConfigurator   = (*Driver)(nil)
	_ camera.Bootstrapper        = (*Driver)(nil)
	_ camera.DeviceInitializer   = (*Driver)(nil)
	_ camera.FirmwareUpgrader    = (*Driver)(nil)
	_ camera.WebIdentityObserver = (*Driver)(nil)
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
	if o.DeviceTLSIdentity != nil && o.SkipTLSVerification {
		return nil, camera.ErrInvalid
	}
	if o.DeviceTLSIdentity != nil || o.SkipTLSVerification {
		if o.DeviceTLSIdentity != nil && (o.DeviceTLSIdentity.Validate() != nil || o.DeviceTLSIdentity.DeviceID != o.DeviceID) {
			return nil, camera.ErrInvalid
		}
		if o.DeviceID == "" || d.bootstrap == nil || d.bootstrap.WebScheme != "https" || len(o.DeviceHosts) == 0 || len(o.DeviceHosts) > 32 {
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
		d.deviceID, d.deviceHosts = o.DeviceID, hosts
		if o.DeviceTLSIdentity != nil {
			identity := *o.DeviceTLSIdentity
			d.identity = &identity
		} else {
			d.skip, d.recorded = true, newRecordedIdentity(o.RecordedSerial, o.RecordedMACs)
		}
	}
	d.client, d.transport = d.httpClient(o.Roots, o.Timeout)
	d.baseline = onvif.NewClient(d.client)
	if d.identity != nil || d.skip {
		// These explicitly approved C420s can boot with a stale RTC on an
		// isolated LAN. Only WS-Security uses their public UTC sample, and
		// host-side authorization time is not changed. A preset's time_sync
		// alone writes the camera clock, and drops the sample when it does.
		d.baseline.UseDeviceTimeForAuthentication = true
	}
	if d.identity != nil {
		d.webClient, d.webTransport, err = d.pinnedWebClient(d.identity.SPKISHA256, o.Timeout)
		if err != nil {
			d.Close()
			return nil, err
		}
	}
	if d.skip {
		d.webClient, d.webTransport = d.unverifiedWebClient(o.Timeout)
	}
	return d, nil
}

// recordedIdentity is what identifies a device whose web key is not
// verified: the serial and MACs of its authenticated identity at the CP.
type recordedIdentity struct {
	serial string
	macs   map[string]bool
}

func newRecordedIdentity(serial string, macs []string) recordedIdentity {
	out := recordedIdentity{serial: serial, macs: map[string]bool{}}
	for _, raw := range macs {
		if mac, err := net.ParseMAC(raw); err == nil && len(mac) == 6 {
			out.macs[mac.String()] = true
		}
	}
	return out
}

// matches requires the same non-empty serial and one of the recorded MACs.
func (r recordedIdentity) matches(h camera.DeviceHint) bool {
	mac, err := net.ParseMAC(h.MAC)
	return r.serial != "" && h.Serial == r.serial && err == nil && r.macs[mac.String()]
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
	if d.deviceID != "" && string(h.ID) != d.deviceID {
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
	v.TimeServer = d.timeServer
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
	// Without a verified key, only the recorded MAC and serial identify the
	// device before the web protocol sends it a credential.
	if d.skip && !d.recorded.matches(i.Device) {
		return nil, camera.DeviceHint{}, camera.ErrAmbiguousIdentity
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
