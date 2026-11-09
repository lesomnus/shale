package drivers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/vendors/tplink"
	"github.com/lesomnus/shale/internal/cameratest"
)

const fixturePassword = "synthetic admin +:/?"

func scopedDriver(t *testing.T, bootstrap *camera.BootstrapSettings) *Driver {
	t.Helper()
	d, err := New(Options{NetworkID: "lan", CIDRs: []string{"127.0.0.1/32"}, Bootstrap: bootstrap, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	return d
}

func bootstrapFor(t *testing.T, raw string) *camera.BootstrapSettings {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatal(err)
	}
	return &camera.BootstrapSettings{CompatibilityProfile: tplink.ProfileC420V122, WebScheme: u.Scheme, WebPort: port}
}

func fixtureLease(t *testing.T, purpose camera.AuthPurpose) camera.CredentialLease {
	t.Helper()
	l := camera.NewCredentialLease(camera.CredentialRef{Provider: "fixture", ID: "synthetic-admin", Version: "1", Purpose: purpose, Recoverable: true, Scope: camera.EnrollmentScope{TenantID: "lab", NetworkID: "lan"}}, camera.CredentialMaterial{Username: "admin", Password: fixturePassword}, time.Now().Add(time.Minute))
	t.Cleanup(func() { _ = l.Close() })
	return l
}

func fixtureCamera(t *testing.T) *cameratest.ONVIFCamera {
	t.Helper()
	c := cameratest.ServeONVIF(t, camera.MediaProfile{Codec: "h265", Width: 1920, Height: 1080, FPS: 25, Bitrate: 4_096_000, GOP: 25, StreamURI: "rtsp://127.0.0.1/main"})
	c.RequireCredentials("admin", fixturePassword)
	return c
}

func configureVideo(t *testing.T, d *Driver, c *cameratest.ONVIFCamera, hint camera.DeviceHint, pinned bool) {
	t.Helper()
	ctx := context.Background()
	lease := fixtureLease(t, camera.AuthManagement)
	inspection, err := d.Inspect(ctx, hint, lease)
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Device.Firmware != "1.2.2 Build260624 Rel.39352n" || !knownAuthenticated(inspection.Device) {
		t.Fatalf("missing authenticated identity: %+v", inspection.Device)
	}
	if len(inspection.Current.Profiles) != 1 {
		t.Fatalf("profiles: %+v", inspection.Current.Profiles)
	}
	profile := inspection.Current.Profiles[0]
	vendorEvidence := slices.ContainsFunc(profile.Evidence, func(e camera.Evidence) bool { return e.Source == "tplink/c420/media2-preferred" })
	if vendorEvidence != pinned {
		t.Fatalf("vendor evidence selected=%v, explicit pin=%v", vendorEvidence, pinned)
	}
	plan, err := d.Plan(ctx, inspection, camera.DesiredState{Codec: "h264", FPS: 30, GOP: 30, Revision: "fixture-preset-1"})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Before.Codec != "h265" || plan.After.Codec != "h264" || plan.After.FPS != 30 || len(plan.Changes) == 0 {
		t.Fatalf("incomplete plan: %+v", plan)
	}
	if writes, _ := c.Writes(); writes != 0 {
		t.Fatal("inspection or planning wrote the device")
	}
	state, err := d.Apply(ctx, inspection.Device, lease, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Profiles) != 1 || state.Profiles[0].Codec != "h264" || state.Profiles[0].FPS != 30 {
		t.Fatalf("configuration readback: %+v", state)
	}
	if writes, _ := c.Writes(); writes != 1 {
		t.Fatalf("configuration writes=%d", writes)
	}
}

func TestWithoutBootstrapUsesONVIFOnly(t *testing.T) {
	c := fixtureCamera(t)
	d := scopedDriver(t, nil)
	hint := c.Hint("generic-onvif") // A matching discovery hint never selects activation.
	state, err := d.Activation(context.Background(), hint)
	if err != nil || state != camera.ActivationUnknown {
		t.Fatalf("activation=%s: %v", state, err)
	}
	if err := d.Activate(context.Background(), hint, fixtureLease(t, camera.AuthInitialAdmin)); !errors.Is(err, camera.ErrManualActivation) {
		t.Fatalf("unselected activation: %v", err)
	}
	if len(c.Calls()) != 0 {
		t.Fatal("unselected activation contacted the camera")
	}
	configureVideo(t, d, c, hint, false)
	for _, operation := range c.Calls() {
		if operation == "get_encrypt_info" || operation == "set_password" {
			t.Fatalf("ONVIF default invoked vendor operation %q", operation)
		}
	}
}

func TestPinnedC420ActivationAndAuthenticatedConfiguration(t *testing.T) {
	for _, profile := range []string{tplink.ProfileC420V122, tplink.ProfileC420V120, tplink.ProfileC420Known} {
		t.Run(profile, func(t *testing.T) {
			c := fixtureCamera(t)
			c.SetUnactivated(true)
			bootstrap := bootstrapFor(t, c.WebURL)
			bootstrap.CompatibilityProfile = profile
			d := scopedDriver(t, bootstrap)
			// A fresh endpoint has no vendor identity until authenticated inspection.
			hint := camera.DeviceHint{ID: "fresh-c420", Endpoints: c.Hint("").Endpoints}
			state, err := d.Activation(context.Background(), hint)
			if err != nil || state != camera.ActivationRequired {
				t.Fatalf("activation=%s: %v", state, err)
			}
			if err := d.Activate(context.Background(), hint, fixtureLease(t, camera.AuthInitialAdmin)); err != nil {
				t.Fatal(err)
			}
			if writes, activations := c.Writes(); writes != 0 || activations != 1 {
				t.Fatalf("writes=%d activations=%d", writes, activations)
			}
			if _, err := d.Inspect(context.Background(), hint, nil); !errors.Is(err, camera.ErrAuthRequired) {
				t.Fatalf("fixture did not require WS-Security: %v", err)
			}
			configureVideo(t, d, c, hint, true)
			// A replay only reads the ready state, even without another initial lease.
			if err := d.Activate(context.Background(), hint, nil); err != nil {
				t.Fatal(err)
			}
			if _, activations := c.Writes(); activations != 1 {
				t.Fatalf("activation replay wrote again: %d", activations)
			}
		})
	}
}

func TestUnselectedOrEmptyInitializationDoesNotContactDevice(t *testing.T) {
	c := fixtureCamera(t)
	h := c.Hint("no-initialization")
	for _, bootstrap := range []*camera.BootstrapSettings{nil, bootstrapFor(t, c.WebURL)} {
		d := scopedDriver(t, bootstrap)
		if plan, err := d.PlanInitialization(context.Background(), h, nil, camera.DeviceSettings{}); err != nil || plan.Revision != "" || len(plan.Changes) != 0 {
			t.Fatalf("empty initialization plan: %+v %v", plan, err)
		}
		if state, err := d.ApplyInitialization(context.Background(), h, nil, camera.InitializationPlan{}); err != nil || state != (camera.DeviceSettings{}) {
			t.Fatalf("empty initialization apply: %+v %v", state, err)
		}
	}
	d := scopedDriver(t, nil)
	if _, err := d.PlanInitialization(context.Background(), h, nil, camera.DeviceSettings{PowerLineFrequency: 60}); !errors.Is(err, camera.ErrUnsupported) {
		t.Fatalf("unselected vendor initialization: %v", err)
	}
	if len(c.Calls()) != 0 {
		t.Fatalf("unselected initialization contacted the camera: %v", c.Calls())
	}
}

func firmwareProxy(t *testing.T, c *cameratest.ONVIFCamera, firmware string) *httptest.Server {
	t.Helper()
	upstream, err := url.Parse(c.WebURL)
	if err != nil {
		t.Fatal(err)
	}
	// Forward the real WS-Security exchange, then model a changed firmware
	// identity. The caller still carries the prior authenticated CP inspection.
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.ModifyResponse = func(response *http.Response) error {
		data, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			return err
		}
		data = bytes.ReplaceAll(data, []byte("1.2.2 Build260624 Rel.39352n"), []byte(firmware))
		response.Body = io.NopCloser(bytes.NewReader(data))
		response.ContentLength = int64(len(data))
		response.Header.Set("Content-Length", strconv.Itoa(len(data)))
		return nil
	}
	onvif := httptest.NewServer(proxy)
	t.Cleanup(onvif.Close)
	return onvif
}

func TestVendorInitializationRechecksCurrentAuthenticatedFirmware(t *testing.T) {
	c := fixtureCamera(t)
	onvif := firmwareProxy(t, c, "9.0 unknown")
	var webCalls atomic.Int32
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webCalls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	t.Cleanup(web.Close)
	d := scopedDriver(t, bootstrapFor(t, web.URL))
	h := c.Hint("stale-cp-identity")
	h.Endpoints[0].URL = onvif.URL + "/onvif/device_service"
	h.Evidence = []camera.Evidence{{Source: "cp-inspection", Authenticated: true}}
	lease := fixtureLease(t, camera.AuthManagement)
	if _, err := d.PlanInitialization(context.Background(), h, lease, camera.DeviceSettings{PowerLineFrequency: 60}); !errors.Is(err, camera.ErrUnsupported) {
		t.Errorf("stale firmware initialization plan: %v", err)
	}
	if _, err := d.ApplyInitialization(context.Background(), h, lease, camera.InitializationPlan{Before: camera.DeviceSettings{PowerLineFrequency: 50}, After: camera.DeviceSettings{PowerLineFrequency: 60}, Revision: "tplink/c420/image-frequency-v1", Changes: []string{"power_line_frequency"}}); !errors.Is(err, camera.ErrUnsupported) {
		t.Errorf("stale firmware initialization apply: %v", err)
	}
	if !slices.Contains(c.Calls(), "GetDeviceInformation") {
		t.Error("current authenticated firmware was never inspected")
	}
	if webCalls.Load() != 0 {
		t.Fatalf("stale identity permitted %d vendor web requests", webCalls.Load())
	}
}

func TestPriorAuthenticationDoesNotAuthorizeAnonymousVendorInitialization(t *testing.T) {
	c := cameratest.ServeONVIF(t, camera.MediaProfile{Codec: "h264", Width: 1920, Height: 1080, FPS: 25, StreamURI: "rtsp://127.0.0.1/main"})
	var webCalls atomic.Int32
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { webCalls.Add(1); w.WriteHeader(http.StatusBadRequest) }))
	t.Cleanup(web.Close)
	d := scopedDriver(t, bootstrapFor(t, web.URL))
	h := c.Hint("prior-authentication")
	h.Evidence = []camera.Evidence{{Source: "cp-inspection", Authenticated: true}}
	anonymous := camera.NewCredentialLease(camera.CredentialRef{Provider: "fixture", ID: "anonymous", Version: "1", Scope: camera.EnrollmentScope{TenantID: "lab"}, Purpose: camera.AuthManagement}, camera.CredentialMaterial{}, time.Now().Add(time.Minute))
	t.Cleanup(func() { _ = anonymous.Close() })
	for _, tc := range []struct {
		name  string
		lease camera.CredentialLease
		want  error
	}{{"missing-lease", nil, camera.ErrAuthRequired}, {"anonymous-current-read", anonymous, camera.ErrUnsupported}} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := d.PlanInitialization(context.Background(), h, tc.lease, camera.DeviceSettings{PowerLineFrequency: 60}); !errors.Is(err, tc.want) {
				t.Errorf("anonymous current initialization plan: %v", err)
			}
			if _, err := d.ApplyInitialization(context.Background(), h, tc.lease, camera.InitializationPlan{Before: camera.DeviceSettings{PowerLineFrequency: 50}, After: camera.DeviceSettings{PowerLineFrequency: 60}, Revision: "tplink/c420/image-frequency-v1"}); !errors.Is(err, tc.want) {
				t.Errorf("anonymous current initialization apply: %v", err)
			}
		})
	}
	if webCalls.Load() != 0 {
		t.Fatalf("anonymous read permitted %d vendor requests", webCalls.Load())
	}
}

func TestVendorBootstrapPinDoesNotRestrictStandardONVIFMedia(t *testing.T) {
	c := fixtureCamera(t)
	onvif := firmwareProxy(t, c, "9.0 unknown")
	d := scopedDriver(t, bootstrapFor(t, c.WebURL))
	h := c.Hint("standard-onvif")
	h.Endpoints[0].URL = onvif.URL + "/onvif/device_service"
	lease := fixtureLease(t, camera.AuthManagement)
	inspection, err := d.Inspect(context.Background(), h, lease)
	if err != nil || inspection.Device.Firmware != "9.0 unknown" {
		t.Fatalf("unknown firmware inspection: %+v %v", inspection.Device, err)
	}
	plan, err := d.Plan(context.Background(), inspection, camera.DesiredState{Codec: "h264", FPS: 30, Revision: "standard-onvif"})
	if err != nil {
		t.Fatal(err)
	}
	state, err := d.Apply(context.Background(), inspection.Device, lease, plan)
	if err != nil || len(state.Profiles) != 1 || state.Profiles[0].Codec != "h264" || state.Profiles[0].FPS != 30 {
		t.Fatalf("standard ONVIF configuration: %+v %v", state, err)
	}
	if writes, activations := c.Writes(); writes != 1 || activations != 0 {
		t.Fatalf("writes=%d activations=%d", writes, activations)
	}
}

func TestOptionsCloneAndPinnedWebPort(t *testing.T) {
	var webCalls, onvifCalls atomic.Int32
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webCalls.Add(1)
		if r.URL.Path != "/" {
			t.Errorf("unexpected vendor path %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error_code":-40401,"data":{"code":-40405}}`))
	}))
	t.Cleanup(web.Close)
	onvif := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { onvifCalls.Add(1) }))
	t.Cleanup(onvif.Close)
	bootstrap := bootstrapFor(t, web.URL)
	cidrs := []string{"127.0.0.1/32"}
	d, err := New(Options{NetworkID: "lan", CIDRs: cidrs, Bootstrap: bootstrap, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	cidrs[0] = "192.0.2.0/24"
	*bootstrap = camera.BootstrapSettings{CompatibilityProfile: "unknown", WebScheme: "https", WebPort: 1}
	hint := camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: onvif.URL + "/fresh/device", NetworkScope: "lan"}}}
	state, err := d.Activation(context.Background(), hint)
	if err != nil || state != camera.ActivationRequired || webCalls.Load() != 1 || onvifCalls.Load() != 0 {
		t.Fatalf("cloned explicit web pin was not used: state=%s err=%v web=%d onvif=%d", state, err, webCalls.Load(), onvifCalls.Load())
	}
	// The same explicit port cannot turn a new out-of-scope address into authority.
	u, _ := url.Parse(hint.Endpoints[0].URL)
	u.Host = net.JoinHostPort("127.0.0.2", u.Port())
	hint.Endpoints[0].URL = u.String()
	if _, err := d.Activation(context.Background(), hint); !errors.Is(err, camera.ErrForbidden) || webCalls.Load() != 1 {
		t.Fatalf("web pin overrode fresh host scope: %v", err)
	}
}
