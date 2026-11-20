package drivers

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
)

func TestClocksReadsTheCamerasOwnClockWithoutCredentialsOrWrites(t *testing.T) {
	c := fixtureCamera(t)
	c.SetClock(-147*24*time.Hour, "NTP", "GMT0GMT")
	scope := camera.DiscoveryScope{ID: "lan", CIDRs: []string{"127.0.0.1/32"}}
	clock, err := Clocks{Timeout: time.Second}.ReadClock(t.Context(), scope, camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: c.URL, NetworkScope: "lan"}}})
	if err != nil {
		t.Fatal(err)
	}
	offset := time.Duration(clock.OffsetMilliseconds()) * time.Millisecond
	if d := offset + 147*24*time.Hour; d < -time.Second-clock.RTT || d > time.Second+clock.RTT {
		t.Fatalf("offset %v, want about -147 days", offset)
	}
	if clock.Mode != camera.ClockModeNTP || clock.TZ != "GMT0GMT" {
		t.Fatalf("mode %q, TZ %q", clock.Mode, clock.TZ)
	}
	if calls := c.Calls(); !slices.Equal(calls, []string{"GetSystemDateAndTime"}) {
		t.Fatalf("a clock reading made other calls: %v", calls)
	}
	if len(c.AuthAttempts()) != 0 {
		t.Fatal("a clock reading presented a credential")
	}
	for _, call := range c.Calls() {
		if strings.HasPrefix(call, "Set") {
			t.Fatal("a clock reading wrote to the camera")
		}
	}
}

func TestClocksStaysInsideTheScope(t *testing.T) {
	c := fixtureCamera(t)
	for name, tc := range map[string]struct {
		scope camera.DiscoveryScope
		hint  camera.DeviceHint
	}{
		"outside the prefixes": {camera.DiscoveryScope{ID: "lan", CIDRs: []string{"192.0.2.0/24"}}, camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: c.URL, NetworkScope: "lan"}}}},
		"another network":      {camera.DiscoveryScope{ID: "lan", CIDRs: []string{"127.0.0.1/32"}}, camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: c.URL, NetworkScope: "other"}}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Clocks{Timeout: time.Second}.ReadClock(t.Context(), tc.scope, tc.hint)
			if !errors.Is(err, camera.ErrForbidden) {
				t.Fatalf("want forbidden, got %v", err)
			}
		})
	}
	_, err := Clocks{}.ReadClock(t.Context(), camera.DiscoveryScope{ID: "lan"}, camera.DeviceHint{Endpoints: []camera.Endpoint{{URL: c.URL, NetworkScope: "lan"}}})
	if !errors.Is(err, camera.ErrInvalid) {
		t.Fatalf("a scope without prefixes: want invalid, got %v", err)
	}
	if len(c.Calls()) != 0 {
		t.Fatal("a refused reading reached the camera")
	}
}
