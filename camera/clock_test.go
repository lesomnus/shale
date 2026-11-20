package camera

import (
	"strings"
	"testing"
	"time"
)

func TestClockModeAndZoneAreBounded(t *testing.T) {
	for mode, want := range map[string]bool{"": true, "NTP": true, "Manual": true, "ntp": false, "Auto": false, "NTP ": false} {
		if ValidClockMode(mode) != want {
			t.Errorf("mode %q: want %v", mode, want)
		}
	}
	for tz, want := range map[string]bool{
		"": true, "GMT0GMT": true, "CST-8": true, "EST5EDT,M3.2.0/2,M11.1.0": true, "<+09>-9": true, strings.Repeat("A", 64): true,
		strings.Repeat("A", 65): false, "GMT 0": false, "GMT\n0": false, "GMT\x000": false, "GMT\x7f": false, "시간대": false,
	} {
		if ValidClockTZ(tz) != want {
			t.Errorf("tz %q: want %v", tz, want)
		}
	}
}

func TestClockOffsetTakesTheMiddleOfTheDevicesSecond(t *testing.T) {
	at := time.Date(2026, 11, 20, 12, 0, 0, 250_000_000, time.UTC)
	for _, tc := range []struct {
		utc  time.Time
		want int64
	}{
		{time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC), 250},
		{time.Date(2026, 11, 20, 12, 0, 13, 0, time.UTC), 13_250},
		{time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC), -int64(147*24*time.Hour/time.Millisecond) + 250},
	} {
		if got := (DeviceClock{UTC: tc.utc, ObservedAt: at}).OffsetMilliseconds(); got != tc.want {
			t.Errorf("%v: offset %d, want %d", tc.utc, got, tc.want)
		}
	}
}
