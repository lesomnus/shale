package drivers

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
)

func factoryWeb(t *testing.T, handler http.Handler) (*httptest.Server, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "FACTORY-DEVICE"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewUnstartedServer(handler)
	s.Config.ErrorLog = log.New(io.Discard, "", 0)
	s.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	s.StartTLS()
	t.Cleanup(s.Close)
	sum := sha256.Sum256(leaf.RawSubjectPublicKeyInfo)
	return s, "sha256:" + hex.EncodeToString(sum[:])
}

func pinnedWebOptions(t *testing.T, s *httptest.Server, pin string) Options {
	t.Helper()
	return Options{NetworkID: "lan", CIDRs: []string{"127.0.0.0/24"}, Bootstrap: bootstrapFor(t, s.URL), DeviceTLSIdentity: &camera.DeviceTLSIdentity{DeviceID: "inventory-a", SPKISHA256: pin}, DeviceID: "inventory-a", DeviceHosts: []string{"127.0.0.1"}}
}

func TestPinnedFactoryWebIsBoundToTheCurrentDeviceHostAndHTTPSPort(t *testing.T) {
	var calls atomic.Int32
	s, pin := factoryWeb(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error_code":-40401,"data":{"code":-40405}}`)
	}))
	o := pinnedWebOptions(t, s, pin)
	d, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	// The normal ONVIF endpoint stays HTTP. Only the explicit factory web port
	// uses the independent TLS key approval.
	h := camera.DeviceHint{ID: "inventory-a", Endpoints: []camera.Endpoint{{URL: "http://127.0.0.1:2020/onvif/device_service", NetworkScope: "lan"}}}
	state, err := d.Activation(t.Context(), h)
	if err != nil || state != camera.ActivationRequired || calls.Load() != 1 {
		t.Fatalf("factory activation state=%v err=%v requests=%d", state, err, calls.Load())
	}
	// Approval belongs to the detached job, not later changes to caller memory.
	o.DeviceTLSIdentity.SPKISHA256 = "sha256:" + strings.Repeat("0", 64)
	o.DeviceHosts[0] = "127.0.0.2"
	if _, err := d.Activation(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	before := calls.Load()
	h.ID = "inventory-b"
	assertOperationErrors(t, d, context.Background(), h, camera.ErrForbidden)
	h.ID = "inventory-a"
	h.Endpoints[0].URL = "http://127.0.0.2:2020/onvif/device_service"
	assertOperationErrors(t, d, context.Background(), h, camera.ErrForbidden)
	u, _ := url.Parse(s.URL)
	for _, raw := range []string{"http://" + u.Host + "/", "https://127.0.0.2:" + u.Port() + "/", "https://127.0.0.1:1/"} {
		if _, err := d.webClient.Post(raw, "application/json", strings.NewReader("credential-fixture")); !errors.Is(err, camera.ErrForbidden) {
			t.Fatalf("web scope %q: %v", raw, err)
		}
	}
	if _, err := d.client.Get(s.URL); err == nil {
		t.Fatal("factory pin must not weaken the standard ONVIF TLS transport")
	}
	if calls.Load() != before {
		t.Fatalf("rejected scope delivered %d HTTP requests", calls.Load()-before)
	}
}

func TestFactoryWebWrongKeyDoesNotDeliverCredentialPayload(t *testing.T) {
	var requests atomic.Int32
	s, _ := factoryWeb(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	d, err := New(pinnedWebOptions(t, s, "sha256:"+strings.Repeat("0", 64)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	if _, err := d.webClient.Post(s.URL, "application/json", strings.NewReader("credential-fixture")); err == nil {
		t.Fatal("different peer key accepted")
	}
	if requests.Load() != 0 {
		t.Fatalf("wrong key received %d requests", requests.Load())
	}
}

func TestDeviceTLSPinRequiresAnExplicitCurrentDeviceAndHTTPSBootstrap(t *testing.T) {
	s, pin := factoryWeb(t, http.NotFoundHandler())
	for _, edit := range []func(*Options){
		func(o *Options) { o.DeviceID = "another-inventory" },
		func(o *Options) { o.DeviceTLSIdentity.DeviceID = "" },
		func(o *Options) { o.DeviceTLSIdentity.SPKISHA256 = "sha256:" + strings.Repeat("A", 64) },
		func(o *Options) { o.DeviceHosts = nil },
		func(o *Options) { o.DeviceHosts = []string{"127.0.0.1", "127.0.0.1"} },
		func(o *Options) { o.DeviceHosts = []string{"localhost"} },
		func(o *Options) { o.DeviceHosts = []string{"192.0.2.1"} },
		func(o *Options) { o.DeviceHosts = []string{"127.0.0.1", "192.0.2.1"} },
		func(o *Options) { o.Bootstrap = nil },
		func(o *Options) { o.Bootstrap.WebScheme = "http" },
	} {
		o := pinnedWebOptions(t, s, pin)
		edit(&o)
		if d, err := New(o); !errors.Is(err, camera.ErrInvalid) || d != nil {
			t.Fatalf("invalid device TLS approval accepted: driver=%v err=%v", d, err)
		}
	}
}

func TestPinnedWebDoesNotFollowCredentialBearingRedirects(t *testing.T) {
	for _, mode := range []string{"cleartext", "another-port", "another-host"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			s, pin := factoryWeb(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				location := "https://127.0.0.1:1/"
				if mode == "cleartext" {
					location = "http://" + r.Host + "/"
				} else if mode == "another-host" {
					location = "https://" + strings.Replace(r.Host, "127.0.0.1", "127.0.0.2", 1) + "/"
				}
				w.Header().Set("Location", location)
				w.WriteHeader(http.StatusTemporaryRedirect)
			}))
			d, err := New(pinnedWebOptions(t, s, pin))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(d.Close)
			response, err := d.webClient.Post(s.URL, "application/json", strings.NewReader("credential-fixture"))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusTemporaryRedirect || calls.Load() != 1 {
				t.Fatalf("redirect followed: status=%d requests=%d", response.StatusCode, calls.Load())
			}
		})
	}
}
