package drivers

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/vendors/tplink"
)

func TestInvalidOptionsAndUnknownProfile(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Options)
		want error
	}{
		{"missing-network", func(o *Options) { o.NetworkID = "" }, camera.ErrInvalid},
		{"control-network", func(o *Options) { o.NetworkID = "lan\n" }, camera.ErrInvalid},
		{"missing-cidrs", func(o *Options) { o.CIDRs = nil }, camera.ErrInvalid},
		{"unmasked", func(o *Options) { o.CIDRs = []string{"127.0.0.1/24"} }, camera.ErrInvalid},
		{"default-route", func(o *Options) { o.CIDRs = []string{"0.0.0.0/0"} }, camera.ErrInvalid},
		{"dns-prefix", func(o *Options) { o.CIDRs = []string{"localhost/32"} }, camera.ErrInvalid},
		{"multicast", func(o *Options) { o.CIDRs = []string{"224.0.0.0/4"} }, camera.ErrInvalid},
		{"mapped-prefix", func(o *Options) { o.CIDRs = []string{"::ffff:127.0.0.1/128"} }, camera.ErrInvalid},
		{"negative-timeout", func(o *Options) { o.Timeout = -time.Second }, camera.ErrInvalid},
		{"long-timeout", func(o *Options) { o.Timeout = 31 * time.Second }, camera.ErrInvalid},
		{"unknown-profile", func(o *Options) {
			o.Bootstrap = &camera.BootstrapSettings{CompatibilityProfile: "not-installed", WebScheme: "http", WebPort: 80}
		}, camera.ErrUnsupported},
		{"missing-web-port", func(o *Options) {
			o.Bootstrap = &camera.BootstrapSettings{CompatibilityProfile: tplink.ProfileC420V122, WebScheme: "http"}
		}, camera.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := Options{NetworkID: "lan", CIDRs: []string{"127.0.0.1/32"}}
			tc.edit(&o)
			d, err := New(o)
			if !errors.Is(err, tc.want) || d != nil {
				t.Fatalf("driver=%v err=%v, want %v", d, err, tc.want)
			}
		})
	}
}

func TestScopeAndCanceledRequestsDoNotContactDevice(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	t.Cleanup(s.Close)
	d := scopedDriver(t, bootstrapFor(t, s.URL))
	for _, tc := range []struct {
		name string
		hint camera.DeviceHint
		want error
	}{
		{"foreign-network", camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: s.URL, NetworkScope: "foreign"}}}, camera.ErrForbidden},
		{"foreign-secondary-network", camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: s.URL, NetworkScope: "lan"}, {URL: s.URL, NetworkScope: "foreign"}}}, camera.ErrForbidden},
		{"foreign-ip", camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: "http://127.0.0.2/onvif/device_service", NetworkScope: "lan"}}}, camera.ErrForbidden},
		{"foreign-secondary-ip", camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: s.URL, NetworkScope: "lan"}, {URL: "rtsp://127.0.0.2/main", NetworkScope: "lan"}}}, camera.ErrForbidden},
		{"dns-name", camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: strings.Replace(s.URL, "127.0.0.1", "localhost", 1), NetworkScope: "lan"}}}, camera.ErrForbidden},
		{"userinfo", camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: strings.Replace(s.URL, "://", "://admin:secret@", 1), NetworkScope: "lan"}}}, camera.ErrForbidden},
		{"credential-query", camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: s.URL + "/?password=private", NetworkScope: "lan"}}}, camera.ErrForbidden},
		{"unknown-query", camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: s.URL + "/?stok=private", NetworkScope: "lan"}}}, camera.ErrForbidden},
		{"session-path", camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: s.URL + "/stok=private/ds", NetworkScope: "lan"}}}, camera.ErrForbidden},
		{"mixed-case-session-path", camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: s.URL + "/SToK=private/ds", NetworkScope: "lan"}}}, camera.ErrForbidden},
		{"encoded-session-path", camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: s.URL + "/%73tok%3Dprivate/ds", NetworkScope: "lan"}}}, camera.ErrForbidden},
		{"no-endpoints", camera.DeviceHint{}, camera.ErrInvalid},
		{"only-rtsp", camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: "rtsp://127.0.0.1/main", NetworkScope: "lan"}}}, camera.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) { assertOperationErrors(t, d, context.Background(), tc.hint, tc.want) })
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assertOperationErrors(t, d, ctx, camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: s.URL, NetworkScope: "lan"}}}, context.Canceled)
	if calls.Load() != 0 {
		t.Fatalf("rejected operation made %d requests", calls.Load())
	}
	if _, err := d.endpoint(context.Background(), camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: s.URL + "/onvif/device_service?channel=1&profile=main", NetworkScope: "lan"}}}); err != nil {
		t.Fatalf("safe profile query rejected: %v", err)
	}
}

func assertOperationErrors(t *testing.T, d *Driver, ctx context.Context, h camera.DeviceHint, want error) {
	t.Helper()
	_, inspect := d.Inspect(ctx, h, nil)
	_, plan := d.Plan(ctx, camera.Inspection{Device: h}, camera.DesiredState{})
	_, apply := d.Apply(ctx, h, nil, camera.ChangePlan{})
	_, activation := d.Activation(ctx, h)
	activate := d.Activate(ctx, h, nil)
	_, initializePlan := d.PlanInitialization(ctx, h, nil, camera.DeviceSettings{})
	_, initializeApply := d.ApplyInitialization(ctx, h, nil, camera.InitializationPlan{})
	for _, result := range []struct {
		name string
		err  error
	}{{"inspect", inspect}, {"plan", plan}, {"apply", apply}, {"activation", activation}, {"activate", activate}, {"initialization-plan", initializePlan}, {"initialization-apply", initializeApply}} {
		if !errors.Is(result.err, want) {
			t.Errorf("%s: %v, want %v", result.name, result.err, want)
		}
	}
}

func TestTransportDisablesProxyAndRedirectsButAllowsVendorSessionPaths(t *testing.T) {
	var targetCalls, proxyCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
		if r.URL.Path != "/stok=ephemeral-session/ds" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(target.Close)
	proxy := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { proxyCalls.Add(1) }))
	t.Cleanup(proxy.Close)
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		t.Setenv(key, proxy.URL)
	}
	t.Setenv("NO_PROXY", "")
	d := scopedDriver(t, nil)
	if d.transport.Proxy != nil {
		t.Fatal("camera transport has a proxy selector")
	}
	response, err := d.client.Get(target.URL + "/stok=ephemeral-session/ds")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || targetCalls.Load() != 1 || proxyCalls.Load() != 0 {
		t.Fatalf("session request: status=%d target=%d proxy=%d", response.StatusCode, targetCalls.Load(), proxyCalls.Load())
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/stok=ephemeral-session/ds", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirect.Close)
	response, err = d.client.Get(redirect.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusTemporaryRedirect || targetCalls.Load() != 1 {
		t.Fatalf("followed redirect: status=%d target=%d", response.StatusCode, targetCalls.Load())
	}
	for _, raw := range []string{strings.Replace(target.URL, "127.0.0.1", "localhost", 1), strings.Replace(target.URL, "127.0.0.1", "127.0.0.2", 1), strings.Replace(target.URL, "://", "://admin:secret@", 1), target.URL + "/#fragment"} {
		if response, err := d.client.Get(raw); !errors.Is(err, camera.ErrForbidden) {
			if response != nil {
				_ = response.Body.Close()
			}
			t.Errorf("route %q: %v", raw, err)
		}
	}
	if targetCalls.Load() != 1 || proxyCalls.Load() != 0 {
		t.Fatal("forbidden route reached an HTTP server")
	}
}

func TestPinnedHTTPSUsesConfiguredTrust(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error_code":-40401,"data":{"code":-40405}}`)
	}))
	t.Cleanup(s.Close)
	roots := x509.NewCertPool()
	roots.AddCert(s.Certificate())
	d, err := New(Options{NetworkID: "lan", CIDRs: []string{"127.0.0.1/32"}, Bootstrap: bootstrapFor(t, s.URL), Roots: roots, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	hint := camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: "http://127.0.0.1:1/onvif/device_service", NetworkScope: "lan"}}}
	state, err := d.Activation(context.Background(), hint)
	if err != nil || state != camera.ActivationRequired || calls.Load() != 1 {
		t.Fatalf("explicit HTTPS web pin: state=%s err=%v calls=%d", state, err, calls.Load())
	}
}

func TestIPScopeIncludesIPv6AndRejectsZones(t *testing.T) {
	d, err := New(Options{NetworkID: "lan", CIDRs: []string{"192.0.2.0/24", "fd00::/64"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	for _, tc := range []struct {
		host string
		want bool
	}{{"192.0.2.1", true}, {"::ffff:192.0.2.1", true}, {"fd00::1", true}, {"192.0.3.1", false}, {"fd01::1", false}, {"fd00::1%eth0", false}, {"localhost", false}, {"0.0.0.0", false}, {"ff02::1", false}} {
		if got := d.allowedHost(tc.host); got != tc.want {
			t.Errorf("host %q: allowed=%v, want %v", tc.host, got, tc.want)
		}
	}
	u, _ := url.Parse("http://[fd00::1%25eth0]/onvif/device_service")
	if _, err := d.endpoint(context.Background(), camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: u.String(), NetworkScope: "lan"}}}); !errors.Is(err, camera.ErrForbidden) {
		t.Fatalf("scoped IPv6 zone accepted: %v", err)
	}
}
