package discovery_test

import (
	"errors"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/discovery"
)

func macOnlyHint() camera.DeviceHint {
	return camera.DeviceHint{
		Manufacturer: "TP-Link", Model: "camera", Hardware: "1.0", MAC: "A8-29-DC-C6-23-15",
		Endpoints: []camera.Endpoint{{URL: "http://192.0.2.1/onvif", NetworkScope: "office-lan"}},
		Evidence:  []camera.Evidence{{Source: "authenticated_network_interfaces", Authenticated: true}},
	}
}

func TestExplicitMACIdentitySurvivesDHCPAndInventoryRestore(t *testing.T) {
	var inventory discovery.Inventory
	claimed, err := inventory.Claim("camera-one", macOnlyHint())
	if err != nil {
		t.Fatal(err)
	}
	var restored discovery.Inventory
	if err := restored.Restore(inventory.Snapshot()); err != nil {
		t.Fatal(err)
	}
	claimed.MAC = "a829.dcc6.2315"
	claimed.Endpoints[0].URL = "http://192.0.2.42/onvif"
	claimed.Firmware = "new-firmware"
	updated, err := restored.Reconcile(claimed)
	if err != nil || updated.ID != "camera-one" || updated.Endpoints[0].URL != "http://192.0.2.42/onvif" || len(restored.Snapshot()) != 1 {
		t.Fatalf("authenticated scoped MAC identity was not preserved: %+v, %v", updated, err)
	}
}

func TestMACAloneNeverSelectsAnInventoryRecordOrAuthenticates(t *testing.T) {
	var inventory discovery.Inventory
	if _, err := inventory.Claim("camera-one", macOnlyHint()); err != nil {
		t.Fatal(err)
	}
	unselected := macOnlyHint()
	if _, err := inventory.Reconcile(unselected); !errors.Is(err, camera.ErrAmbiguousIdentity) {
		t.Fatalf("MAC automatically selected an inventory record: %v", err)
	}
	unselected.ID = "camera-one"
	unselected.Evidence[0].Authenticated = false
	if _, err := inventory.Reconcile(unselected); !errors.Is(err, camera.ErrAmbiguousIdentity) {
		t.Fatalf("MAC was treated as authentication: %v", err)
	}
	if _, err := inventory.Claim("unauthenticated", unselected); !errors.Is(err, camera.ErrInvalid) {
		t.Fatalf("unauthenticated MAC claim accepted: %v", err)
	}
	// An explicit second claim remains a distinct logical record even when a
	// network reuses a MAC. There is no address- or MAC-based automatic merge.
	second := macOnlyHint()
	second.Endpoints[0].URL = "http://192.0.2.2/onvif"
	if _, err := inventory.Claim("camera-two", second); err != nil {
		t.Fatal(err)
	}
	if len(inventory.Snapshot()) != 2 {
		t.Fatal("explicit claims were automatically merged")
	}
}

func TestMACOnlyReconcileRequiresOneUnchangedNetworkScope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*camera.DeviceHint)
	}{
		{"different_network", func(h *camera.DeviceHint) { h.Endpoints[0].NetworkScope = "another-lan" }},
		{"missing_network", func(h *camera.DeviceHint) { h.Endpoints[0].NetworkScope = "" }},
		{"mixed_networks", func(h *camera.DeviceHint) {
			h.Endpoints = append(h.Endpoints, camera.Endpoint{URL: "http://192.0.2.2/onvif", NetworkScope: "another-lan"})
		}},
		{"mixed_known_unknown", func(h *camera.DeviceHint) {
			h.Endpoints = append(h.Endpoints, camera.Endpoint{URL: "http://192.0.2.2/onvif"})
		}},
		{"no_endpoints", func(h *camera.DeviceHint) { h.Endpoints = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var inventory discovery.Inventory
			h, err := inventory.Claim("camera-one", macOnlyHint())
			if err != nil {
				t.Fatal(err)
			}
			tc.modify(&h)
			if _, err := inventory.Reconcile(h); !errors.Is(err, camera.ErrConflict) {
				t.Fatalf("unscoped MAC established stable identity: %v", err)
			}
		})
	}
	var unscoped discovery.Inventory
	hint := macOnlyHint()
	hint.Endpoints[0].NetworkScope = ""
	h, err := unscoped.Claim("camera-one", hint)
	if err != nil {
		t.Fatal(err)
	}
	h.Endpoints[0].NetworkScope = "office-lan"
	if _, err := unscoped.Reconcile(h); !errors.Is(err, camera.ErrConflict) {
		t.Fatalf("unknown prior network established MAC identity: %v", err)
	}
}

func TestKnownMACMustMatchEvenWithTheSameSerial(t *testing.T) {
	for _, mac := range []string{"", "a8:29:dc:c6:23:16", "not-a-mac", "a8:29:dc:c6:23:15:00:00"} {
		t.Run(mac, func(t *testing.T) {
			var inventory discovery.Inventory
			hint := macOnlyHint()
			hint.Serial = "same-serial"
			h, err := inventory.Claim("camera-one", hint)
			if err != nil {
				t.Fatal(err)
			}
			h.MAC = mac
			if _, err := inventory.Reconcile(h); !errors.Is(err, camera.ErrConflict) {
				t.Fatalf("changed or missing known MAC was ignored: %v", err)
			}
			if _, err := inventory.Claim("camera-one", h); !errors.Is(err, camera.ErrConflict) {
				t.Fatalf("repeated claim replaced a known MAC: %v", err)
			}
		})
	}
	// Existing authenticated serial identity may cross networks as before;
	// requiring a scope is specific to using a MAC as the sole stable identifier.
	var inventory discovery.Inventory
	hint := macOnlyHint()
	hint.Serial = "same-serial"
	h, err := inventory.Claim("camera-one", hint)
	if err != nil {
		t.Fatal(err)
	}
	h.MAC = "a8:29:dc:c6:23:15"
	h.Endpoints[0].NetworkScope = "another-lan"
	if _, err := inventory.Reconcile(h); err != nil {
		t.Fatalf("serial identity behavior changed: %v", err)
	}
}
