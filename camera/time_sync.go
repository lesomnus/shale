package camera

import (
	"context"
	"errors"
	"net/netip"
	"slices"
	"strings"
	"time"
)

// TimeSyncEdge is TimeSync.NTP's name for the worker's own address on the
// camera network: the source address it reaches that camera from.
const TimeSyncEdge = "edge"

// TimeSyncTolerance is how far a synchronized device's clock may be from the
// worker's: the device answers in whole seconds, and the reading has a round
// trip.
const TimeSyncTolerance = 2 * time.Second

// ErrTimeServerRefused means a time_sync's NTP server, as the worker resolved
// it, is not inside the camera network. Nothing is written to the device.
var ErrTimeServerRefused = errors.New("camera time server is outside the camera network")

// TimeSync points a device at an NTP server and sets its time zone
// (docs/camera-policy.md, "Camera time sync"). Zero leaves the device's clock
// alone; otherwise both fields are required.
//
// NTP is TimeSyncEdge or an IPv4 literal, and nothing else: a hostname cannot
// name a server, and either form must resolve inside the camera network's
// prefixes, which only the worker applying it knows. TimeZone is a POSIX TZ
// string, as ONVIF carries it.
type TimeSync struct {
	NTP      string `json:"ntp,omitempty" yaml:"ntp,omitempty"`
	TimeZone string `json:"timezone,omitempty" yaml:"timezone,omitempty"`
}

func (t TimeSync) IsZero() bool { return t == TimeSync{} }

func (t TimeSync) Validate() error {
	if t.IsZero() {
		return nil
	}
	if !validNTPSetting(t.NTP) || !ValidPOSIXTimeZone(t.TimeZone) {
		return ErrInvalid
	}
	return nil
}

// String is the setting as a warning or a summary shows it; empty when zero.
func (t TimeSync) String() string {
	if t.IsZero() {
		return ""
	}
	return "ntp=" + t.NTP + " timezone=" + t.TimeZone
}

// Server is an explicit IPv4 NTP setting's address; ok is false for
// TimeSyncEdge, which only a worker can resolve.
func (t TimeSync) Server() (netip.Addr, bool) {
	if t.NTP == TimeSyncEdge {
		return netip.Addr{}, false
	}
	a, ok := timeServerAddr(t.NTP)
	return a, ok
}

func validNTPSetting(v string) bool {
	if v == TimeSyncEdge {
		return true
	}
	_, ok := timeServerAddr(v)
	return ok
}

// timeServerAddr admits a canonical IPv4 unicast literal: no hostname, no
// IPv6, no zone, no leading zeros, no broadcast, multicast or unspecified.
func timeServerAddr(v string) (netip.Addr, bool) {
	a, err := netip.ParseAddr(v)
	if err != nil || !a.Is4() || a.String() != v || a.IsUnspecified() || a.IsMulticast() || a == netip.AddrFrom4([4]byte{255, 255, 255, 255}) {
		return netip.Addr{}, false
	}
	return a, true
}

// TimeServerAllowed says whether server may be written to a camera on the
// network of prefixes: a valid IPv4 server inside one of them.
func TimeServerAllowed(server netip.Addr, prefixes []netip.Prefix) bool {
	if _, ok := timeServerAddr(server.String()); !ok || server.Zone() != "" {
		return false
	}
	return slices.ContainsFunc(prefixes, func(p netip.Prefix) bool { return p.Addr().Is4() && p.Contains(server) })
}

// ValidPOSIXTimeZone admits a POSIX TZ string as ONVIF's TimeZone.TZ carries
// it, at most 64 bytes:
//
//	std offset [dst [offset] [,start[/time],end[/time]]]
//
// A name is three or more letters, or <...> of letters, digits, + and -. An
// offset is [+-]hh[:mm[:ss]] up to 24 hours; a rule's time may reach 167
// hours. A date is Jn (1-365), n (0-365) or Mm.w.d. The ":"-prefixed,
// implementation-defined form is refused.
func ValidPOSIXTimeZone(tz string) bool {
	if len(tz) == 0 || len(tz) > maxClockTZ || !ValidClockTZ(tz) {
		return false
	}
	p := tzParser{s: tz}
	if !p.name() || !p.offset(24) {
		return false
	}
	if p.done() {
		return true
	}
	if !p.name() {
		return false
	}
	if p.done() {
		return true
	}
	if p.peek() != ',' && !p.offset(24) {
		return false
	}
	if p.done() {
		return true
	}
	for range 2 {
		if !p.take(',') || !p.date() {
			return false
		}
		if p.peek() == '/' {
			p.i++
			if !p.offset(167) {
				return false
			}
		}
	}
	return p.done()
}

type tzParser struct {
	s string
	i int
}

func (p *tzParser) done() bool { return p.i == len(p.s) }
func (p *tzParser) peek() byte {
	if p.done() {
		return 0
	}
	return p.s[p.i]
}
func (p *tzParser) take(c byte) bool {
	if p.peek() == c {
		p.i++
		return true
	}
	return false
}

func letter(c byte) bool { return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' }
func digit(c byte) bool  { return c >= '0' && c <= '9' }

func (p *tzParser) name() bool {
	start := p.i
	if p.take('<') {
		for !p.done() && (letter(p.peek()) || digit(p.peek()) || p.peek() == '+' || p.peek() == '-') {
			p.i++
		}
		return p.i-start-1 >= 3 && p.take('>')
	}
	for !p.done() && letter(p.peek()) {
		p.i++
	}
	return p.i-start >= 3
}

// number reads least to most digits, at most max.
func (p *tzParser) number(least, most, max int) (int, bool) {
	n, read := 0, 0
	for read < most && digit(p.peek()) {
		n = n*10 + int(p.peek()-'0')
		p.i++
		read++
	}
	return n, read >= least && n <= max
}

func (p *tzParser) offset(hours int) bool {
	if !p.take('+') {
		p.take('-')
	}
	most := 2
	if hours > 99 {
		most = 3
	}
	if _, ok := p.number(1, most, hours); !ok {
		return false
	}
	for range 2 {
		if !p.take(':') {
			return true
		}
		if _, ok := p.number(2, 2, 59); !ok {
			return false
		}
	}
	return true
}

func (p *tzParser) date() bool {
	switch {
	case p.take('J'):
		n, ok := p.number(1, 3, 365)
		return ok && n >= 1
	case p.take('M'):
		m, ok := p.number(1, 2, 12)
		if !ok || m < 1 || !p.take('.') {
			return false
		}
		w, ok := p.number(1, 1, 5)
		if !ok || w < 1 || !p.take('.') {
			return false
		}
		_, ok = p.number(1, 1, 6)
		return ok
	default:
		_, ok := p.number(1, 3, 365)
		return ok
	}
}

// InitializationTimeSync is a time_sync plan's explicit state: the server the
// worker resolved its NTP setting to, which the plan writes, and what the
// device reported before any write. It holds addresses and names the device
// reported, never a credential or a raw response.
type InitializationTimeSync struct {
	Server                string   `json:"server"`
	BeforeNTPFromDHCP     bool     `json:"before_ntp_from_dhcp,omitempty"`
	BeforeNTP             []string `json:"before_ntp,omitempty"`
	BeforeMode            string   `json:"before_mode,omitempty"`
	BeforeTZ              string   `json:"before_tz,omitempty"`
	BeforeDaylightSavings bool     `json:"before_daylight_savings,omitempty"`
}

// MaxTimeSyncNTPServers bounds the NTP servers a device may report.
const MaxTimeSyncNTPServers = 8

func (t InitializationTimeSync) Validate() error {
	if _, ok := timeServerAddr(t.Server); !ok || len(t.BeforeNTP) > MaxTimeSyncNTPServers || !ValidClockMode(t.BeforeMode) || !ValidClockTZ(t.BeforeTZ) {
		return ErrInvalid
	}
	for _, server := range t.BeforeNTP {
		if server == "" || len(server) > 255 || strings.ContainsFunc(server, func(r rune) bool { return r <= ' ' || r > '~' }) {
			return ErrInvalid
		}
	}
	return nil
}

// Time-sync plan changes: what Apply writes.
const (
	TimeSyncChangeNTP      = "time_sync.ntp"
	TimeSyncChangeTimeZone = "time_sync.timezone"
)

// ValidateTimeSyncPlan binds a time-sync plan to its explicit intent. The plan
// carries nothing but time sync; Before is the intent when the device already
// has it and zero otherwise, and Changes says which of the two writes remain.
// An explicit IPv4 setting resolves to itself.
func ValidateTimeSyncPlan(p InitializationPlan, desired TimeSync) error {
	if desired.IsZero() || desired.Validate() != nil || p.After != (DeviceSettings{TimeSync: desired}) || p.VideoMainBefore != nil || p.TimeSync == nil || p.TimeSync.Validate() != nil {
		return ErrInvalid
	}
	if server, ok := desired.Server(); ok && server.String() != p.TimeSync.Server {
		return ErrInvalid
	}
	if p.Before != (DeviceSettings{}) && p.Before != p.After || len(p.Changes) > 2 || (p.Before == p.After) != (len(p.Changes) == 0) {
		return ErrInvalid
	}
	for i, change := range p.Changes {
		if change != TimeSyncChangeNTP && change != TimeSyncChangeTimeZone || slices.Contains(p.Changes[:i], change) {
			return ErrInvalid
		}
	}
	return nil
}

// ClockObserver reads an enrolled device's own clock at its authorized
// endpoint, without a credential, as a ClockReader does for discovery. It
// never sets the clock.
type ClockObserver interface {
	ObserveClock(context.Context, DeviceHint) (DeviceClock, error)
}
