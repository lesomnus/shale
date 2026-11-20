package camera

import (
	"context"
	"time"
)

// A device's DateTimeType, as ONVIF names it.
const (
	ClockModeManual = "Manual"
	ClockModeNTP    = "NTP"
)

// MaxClockRTT bounds the round trip of a clock reading that is recorded: an
// answer slower than this says little about when the device read its clock.
const MaxClockRTT = 30 * time.Second

// maxClockTZ bounds a reported POSIX TZ string.
const maxClockTZ = 64

// DeviceClock is one credential-free reading of a device's own clock. It is an
// observation: reading a clock never sets it. Only a preset's TimeSync, applied
// by a DeviceInitializer, writes one.
type DeviceClock struct {
	// UTC is what the device said, in its whole seconds.
	UTC time.Time
	// ObservedAt is the reader's clock at the round trip's midpoint.
	ObservedAt time.Time
	// RTT is the round trip, connection included: the reading's uncertainty.
	RTT time.Duration
	// Mode is ClockModeManual or ClockModeNTP; empty when the device said
	// neither.
	Mode string
	// TZ is the POSIX TZ string the device reported; empty when it reported
	// none that ValidClockTZ admits.
	TZ string
}

// OffsetMilliseconds is the device's clock minus the reader's at ObservedAt.
// The device answers in whole seconds, so the middle of its second is taken.
// It is computed on Unix milliseconds, so a clock centuries off does not
// overflow a time.Duration.
func (c DeviceClock) OffsetMilliseconds() int64 {
	return c.UTC.UnixMilli() + 500 - c.ObservedAt.UnixMilli()
}

// ValidClockMode says whether mode is a DateTimeType a reading may carry,
// empty being unknown.
func ValidClockMode(mode string) bool {
	return mode == "" || mode == ClockModeManual || mode == ClockModeNTP
}

// ValidClockTZ says whether tz may be recorded as a device's time zone: at
// most 64 bytes of printable ASCII and no space, which a POSIX TZ string is.
// Empty is unknown.
func ValidClockTZ(tz string) bool {
	if len(tz) > maxClockTZ {
		return false
	}
	for _, b := range []byte(tz) {
		if b <= ' ' || b > '~' {
			return false
		}
	}
	return true
}

// ClockReader reads a device's own clock at its endpoint within an authorized
// discovery scope, without a credential. It never sets the clock.
type ClockReader interface {
	ReadClock(context.Context, DiscoveryScope, DeviceHint) (DeviceClock, error)
}
