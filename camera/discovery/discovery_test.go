package discovery_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/discovery"
)

func TestSeedsNeedNoNetworkAndRejectCredentials(t *testing.T) {
	d := discovery.Discoverer{ListenPacket: func(context.Context, string, string) (net.PacketConn, error) {
		t.Fatal("seed opened multicast socket")
		return nil, nil
	}}
	scope := camera.DiscoveryScope{ID: "lan", Seeds: []camera.Endpoint{{URL: "http://camera/onvif/device_service"}, {URL: "http://camera/onvif/device_service"}}}
	results, err := d.Discover(context.Background(), scope)
	if err != nil || len(results) != 1 {
		t.Fatalf("results=%v err=%v", results, err)
	}
	if results[0].Device.ID != "" || results[0].Device.Evidence[0].Authenticated {
		t.Fatal("seed inferred physical identity")
	}
	scope.Seeds = []camera.Endpoint{{URL: "http://admin:secret@camera/onvif"}}
	if _, err = d.Discover(context.Background(), scope); !errors.Is(err, camera.ErrInvalid) {
		t.Fatalf("credentials allowed in discovery: %v", err)
	}
}

func TestWSDiscoveryDeduplicatesAndCarriesScope(t *testing.T) {
	server, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 8192)
		n, addr, err := server.ReadFrom(buf)
		if err != nil {
			return
		}
		if strings.Contains(string(buf[:n]), "Password") {
			t.Error("discovery sent credentials")
		}
		for j := 0; j < 2; j++ {
			server.WriteTo([]byte(reply("urn:uuid:one", "http://camera/onvif http://camera/onvif")), addr)
		}
	}()
	d := discovery.Discoverer{Target: server.LocalAddr()}
	results, err := d.Discover(context.Background(), camera.DiscoveryScope{ID: "lan-a", LocalAddress: "127.0.0.1", Timeout: 80 * time.Millisecond, Multicast: true})
	<-done
	if err != nil || len(results) != 1 {
		t.Fatalf("results=%v err=%v", results, err)
	}
	r := results[0]
	if len(r.Device.Endpoints) != 1 || r.Scope != "lan-a" || r.Device.Endpoints[0].NetworkScope != "lan-a" || r.Device.DiscoveryID != "urn:uuid:one" {
		t.Fatalf("bad discovery: %+v", r)
	}
	if r.Device.ID != "" {
		t.Fatal("discovery allocated physical identity")
	}
}

func TestDiscoveryCancellationClosesSocketPromptly(t *testing.T) {
	server, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { buf := make([]byte, 8192); server.ReadFrom(buf); cancel() }()
	start := time.Now()
	_, err = (discovery.Discoverer{Target: server.LocalAddr()}).Discover(ctx, camera.DiscoveryScope{ID: "lan", LocalAddress: "127.0.0.1", Multicast: true, Timeout: 5 * time.Second})
	if !errors.Is(err, context.Canceled) || time.Since(start) > time.Second {
		t.Fatalf("cancel err=%v elapsed=%s", err, time.Since(start))
	}
}

func TestDiscoveryParentDeadlineAndInvalidInterface(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	server, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	_, err = (discovery.Discoverer{Target: server.LocalAddr()}).Discover(ctx, camera.DiscoveryScope{ID: "lan", LocalAddress: "127.0.0.1", Multicast: true, Timeout: time.Second})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("parent deadline swallowed: %v", err)
	}
	_, err = (discovery.Discoverer{}).Discover(context.Background(), camera.DiscoveryScope{ID: "lan", Interface: "nonexistent-camera-interface", Multicast: true})
	if !errors.Is(err, camera.ErrInvalid) {
		t.Fatalf("invalid interface ignored: %v", err)
	}
}

// Model the timer publication race deterministically: the socket observes the
// elapsed deadline while the parent has not yet published Err/closed Done.
type laggingDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c laggingDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }

func TestDiscoveryElapsedParentDeadlineBeforeContextErr(t *testing.T) {
	ctx := laggingDeadlineContext{Context: context.Background(), deadline: time.Now().Add(-time.Millisecond)}
	if ctx.Err() != nil {
		t.Fatal("fixture must model unpublished cancellation")
	}
	server, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	_, err = (discovery.Discoverer{Target: server.LocalAddr()}).Discover(ctx, camera.DiscoveryScope{ID: "lan", LocalAddress: "127.0.0.1", Multicast: true, Timeout: time.Second})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("elapsed parent deadline swallowed before Err: %v", err)
	}
}

func TestIdentityRequiresClaimAndPreservesAcrossDHCP(t *testing.T) {
	var inventory discovery.Inventory
	hint := camera.DeviceHint{Manufacturer: "vendor", Model: "model", Serial: "duplicated", DiscoveryID: "uuid-one", Endpoints: []camera.Endpoint{{URL: "http://192.0.2.1/onvif"}}, Evidence: []camera.Evidence{{Source: "authenticated_device", Authenticated: true}}}
	first, err := inventory.Claim("camera-one", hint)
	if err != nil {
		t.Fatal(err)
	}
	other := hint
	other.DiscoveryID = "uuid-two"
	other.Endpoints = []camera.Endpoint{{URL: "http://192.0.2.2/onvif"}}
	if _, err = inventory.Reconcile(other); !errors.Is(err, camera.ErrAmbiguousIdentity) {
		t.Fatal("same serial auto-merged")
	}
	second, err := inventory.Claim("camera-two", other)
	if err != nil || first.ID == second.ID {
		t.Fatal("duplicate serials lost identity")
	}
	first.Endpoints = []camera.Endpoint{{URL: "http://192.0.2.3/onvif"}}
	first.Firmware = "2"
	updated, err := inventory.Reconcile(first)
	if err != nil || updated.ID != "camera-one" || len(inventory.Snapshot()) != 2 {
		t.Fatalf("DHCP allocated identity: %+v %v", updated, err)
	}
	replacement := first
	replacement.Serial = "replacement"
	replacement.DiscoveryID = "uuid-new"
	if _, err = inventory.Reconcile(replacement); !errors.Is(err, camera.ErrConflict) {
		t.Fatal("camera replacement silently merged")
	}
	var restored discovery.Inventory
	if err = restored.Restore(inventory.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if _, err = restored.Reconcile(updated); err != nil {
		t.Fatal(err)
	}
}

func reply(id, addresses string) string {
	return `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><d:ProbeMatches xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery"><d:ProbeMatch><a:EndpointReference xmlns:a="http://schemas.xmlsoap.org/ws/2004/08/addressing"><a:Address>` + id + `</a:Address></a:EndpointReference><d:XAddrs>` + addresses + `</d:XAddrs></d:ProbeMatch></d:ProbeMatches></s:Body></s:Envelope>`
}
