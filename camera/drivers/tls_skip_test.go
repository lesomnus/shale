package drivers

import (
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lesomnus/shale/camera"
)

func skipWebOptions(t *testing.T, web string) Options {
	t.Helper()
	return Options{NetworkID: "lan", CIDRs: []string{"127.0.0.0/24"}, Bootstrap: bootstrapFor(t, web), SkipTLSVerification: true, RecordedSerial: "synthetic-camera", RecordedMACs: []string{"02-00-00-00-00-01"}, DeviceID: "inventory-a", DeviceHosts: []string{"127.0.0.1"}}
}

// Without a pin or a policy that skips verification, a camera's self-signed
// web key is refused by ordinary PKI. Skipping is never the default.
func TestSkipTLSVerificationIsOffUnlessAskedFor(t *testing.T) {
	var requests atomic.Int32
	s, _ := factoryWeb(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error_code":-40401,"data":{"code":-40401}}`))
	}))
	h := camera.DeviceHint{ID: "inventory-a", Endpoints: []camera.Endpoint{{URL: "http://127.0.0.1:2020/onvif/device_service", NetworkScope: "lan"}}}

	verifying := scopedDriver(t, bootstrapFor(t, s.URL))
	if _, err := verifying.Activation(t.Context(), h); err == nil || errors.Is(err, camera.ErrTLSIdentityChanged) {
		t.Fatalf("default verification accepted an unverifiable key: %v", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("default verification sent %d requests", requests.Load())
	}

	skipping := pinnedDriver(t, skipWebOptions(t, s.URL))
	if state, err := skipping.Activation(t.Context(), h); err != nil || state != camera.ActivationReady || requests.Load() != 1 {
		t.Fatalf("skipping verification: state=%v err=%v requests=%d", state, err, requests.Load())
	}
	// It still has no key to report or to replace.
	if _, err := skipping.ObserveWebIdentity(t.Context(), h); !errors.Is(err, camera.ErrInvalid) {
		t.Fatalf("observation without a pin: %v", err)
	}
	if _, err := skipping.ConfirmWebIdentity(t.Context(), h, fixtureLease(t, camera.AuthManagement)); !errors.Is(err, camera.ErrInvalid) {
		t.Fatalf("confirmation without a pin: %v", err)
	}

	// The device's own hosts, web port and no redirects still bound it.
	before := requests.Load()
	other := h
	other.ID = "inventory-b"
	if _, err := skipping.Activation(t.Context(), other); !errors.Is(err, camera.ErrForbidden) {
		t.Fatalf("another device: %v", err)
	}
	other = h
	other.Endpoints = []camera.Endpoint{{URL: "http://127.0.0.2:2020/onvif/device_service", NetworkScope: "lan"}}
	if _, err := skipping.Activation(t.Context(), other); !errors.Is(err, camera.ErrForbidden) {
		t.Fatalf("another host: %v", err)
	}
	for _, raw := range []string{strings.Replace(s.URL, "https://", "http://", 1), "https://127.0.0.2:" + strings.Split(s.URL, ":")[2], "https://127.0.0.1:1/"} {
		if _, err := skipping.webClient.Post(raw, "application/json", strings.NewReader("{}")); !errors.Is(err, camera.ErrForbidden) {
			t.Fatalf("web scope %q: %v", raw, err)
		}
	}
	if requests.Load() != before {
		t.Fatalf("refused scope delivered %d requests", requests.Load()-before)
	}
}

func TestSkipTLSVerificationNeedsAnExplicitDeviceAndHTTPSBootstrap(t *testing.T) {
	s, pin := factoryWeb(t, http.NotFoundHandler())
	for name, edit := range map[string]func(*Options){
		"and a pin": func(o *Options) {
			o.DeviceTLSIdentity = &camera.DeviceTLSIdentity{DeviceID: o.DeviceID, SPKISHA256: pin}
		},
		"no device":        func(o *Options) { o.DeviceID = "" },
		"no hosts":         func(o *Options) { o.DeviceHosts = nil },
		"host outside":     func(o *Options) { o.DeviceHosts = []string{"192.0.2.1"} },
		"no bootstrap":     func(o *Options) { o.Bootstrap = nil },
		"cleartext web":    func(o *Options) { o.Bootstrap.WebScheme = "http" },
		"hostname as host": func(o *Options) { o.DeviceHosts = []string{"localhost"} },
	} {
		o := skipWebOptions(t, s.URL)
		edit(&o)
		if d, err := New(o); !errors.Is(err, camera.ErrInvalid) || d != nil {
			t.Fatalf("%s: driver=%v err=%v", name, d, err)
		}
	}
}

// With no key checked, the recorded MAC and serial identify the device: a
// credentialed web call follows only an authenticated inspection matching
// both, and a device with no recorded identity gets none.
func TestSkipTLSVerificationIdentifiesTheDeviceByRecordedMACAndSerial(t *testing.T) {
	c := fixtureCamera(t)
	h := camera.DeviceHint{ID: "inventory-a", Endpoints: c.Hint("").Endpoints}
	for name, test := range map[string]struct {
		serial string
		macs   []string
		want   error
	}{
		"another serial":   {"another-camera", []string{"02:00:00:00:00:01"}, camera.ErrAmbiguousIdentity},
		"another MAC":      {"synthetic-camera", []string{"02:00:00:00:00:02"}, camera.ErrAmbiguousIdentity},
		"nothing recorded": {"", nil, camera.ErrAmbiguousIdentity},
		"recorded":         {"synthetic-camera", []string{"02-00-00-00-00-01"}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			web, _ := factoryWeb(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusBadRequest)
			}))
			o := skipWebOptions(t, web.URL)
			o.RecordedSerial, o.RecordedMACs = test.serial, test.macs
			d := pinnedDriver(t, o)
			_, err := d.PlanInitialization(t.Context(), h, fixtureLease(t, camera.AuthManagement), camera.DeviceSettings{PowerLineFrequency: 60})
			if test.want != nil {
				if !errors.Is(err, test.want) || requests.Load() != 0 {
					t.Fatalf("err=%v requests=%d", err, requests.Load())
				}
				return
			}
			if errors.Is(err, camera.ErrAmbiguousIdentity) || requests.Load() == 0 {
				t.Fatalf("the recorded device was not reached: err=%v requests=%d", err, requests.Load())
			}
		})
	}
}
