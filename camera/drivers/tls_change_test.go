package drivers

import (
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lesomnus/shale/camera"
)

// A camera whose key is not the pin is refused in the handshake: the job sees
// a changed key, not a transport error, and no request reaches the camera.
// Only a credential-free handshake then reads the key it presents.
func TestChangedWebKeyIsRefusedThenObservedWithoutARequest(t *testing.T) {
	var requests atomic.Int32
	s, presented := factoryWeb(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	pinned := "sha256:" + strings.Repeat("0", 64)
	d, err := New(pinnedWebOptions(t, s, pinned))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	h := camera.DeviceHint{ID: "inventory-a", Endpoints: []camera.Endpoint{{URL: "http://127.0.0.1:2020/onvif/device_service", NetworkScope: "lan"}}}
	if _, err := d.Activation(t.Context(), h); !errors.Is(err, camera.ErrTLSIdentityChanged) {
		t.Fatalf("activation over a changed key: %v", err)
	}
	if err := d.Activate(t.Context(), h, fixtureLease(t, camera.AuthInitialAdmin)); !errors.Is(err, camera.ErrTLSIdentityChanged) {
		t.Fatalf("activation write over a changed key: %v", err)
	}
	observed, err := d.ObserveWebIdentity(t.Context(), h)
	if err != nil {
		t.Fatal(err)
	}
	if observed != (camera.DeviceTLSIdentity{DeviceID: "inventory-a", SPKISHA256: presented}) {
		t.Fatalf("observed %+v, want %s", observed, presented)
	}
	if requests.Load() != 0 {
		t.Fatalf("a changed key received %d requests", requests.Load())
	}
	// Observation stays inside the pinned device's own scope.
	other := h
	other.ID = "inventory-b"
	if _, err := d.ObserveWebIdentity(t.Context(), other); !errors.Is(err, camera.ErrForbidden) {
		t.Fatalf("another device: %v", err)
	}
	other = h
	other.Endpoints = []camera.Endpoint{{URL: "http://127.0.0.2:2020/onvif/device_service", NetworkScope: "lan"}}
	if _, err := d.ObserveWebIdentity(t.Context(), other); !errors.Is(err, camera.ErrForbidden) {
		t.Fatalf("another host: %v", err)
	}
	// Only a pinned driver explains a refused pin.
	o := pinnedWebOptions(t, s, pinned)
	o.DeviceTLSIdentity, o.DeviceHosts = nil, nil
	unpinned, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unpinned.Close)
	if _, err := unpinned.ObserveWebIdentity(t.Context(), h); !errors.Is(err, camera.ErrInvalid) {
		t.Fatalf("unpinned observation: %v", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("observation sent %d requests", requests.Load())
	}
}
