package onvif

import (
	"context"
	"errors"
	"net/netip"
	"strings"

	"github.com/lesomnus/shale/camera"
)

// TimeConfiguration is a device's time configuration: GetNTP, read with the
// credential, and the public GetSystemDateAndTime.
type TimeConfiguration struct {
	NTPFromDHCP bool
	// NTPManual are the manual servers as the device reports them: an IPv4 or
	// IPv6 address or a DNS name, at most camera.MaxTimeSyncNTPServers.
	NTPManual []string
	// Mode is camera.ClockModeManual or camera.ClockModeNTP; empty when the
	// device said neither.
	Mode            string
	DaylightSavings bool
	TZ              string
	// Clock is the reading the date and time came with.
	Clock camera.DeviceClock
}

// ReadTimeConfiguration reads the device's NTP and date-time configuration at
// its first HTTP(S) endpoint. It writes nothing. A response outside ONVIF's
// shape for these operations is camera.ErrUnsupported: the device's time
// service is not one this package can drive.
func (c *Client) ReadTimeConfiguration(ctx context.Context, device camera.DeviceHint, lease camera.CredentialLease) (TimeConfiguration, error) {
	var out TimeConfiguration
	if lease == nil {
		return out, camera.ErrAuthRequired
	}
	base, err := deviceEndpoint(device)
	if err != nil {
		return out, err
	}
	err = lease.Use(ctx, func(material camera.CredentialMaterial) error {
		r, err := c.request(ctx, base.String(), deviceNS, "GetNTP", "", material)
		if err != nil {
			return timeError(err)
		}
		info := r.all("NTPInformation")
		if len(info) != 1 {
			return camera.ErrUnsupported
		}
		switch from, _ := leafText(info[0], "FromDHCP"); from {
		case "true", "1":
			out.NTPFromDHCP = true
		case "false", "0":
		default:
			return camera.ErrUnsupported
		}
		manual := info[0].all("NTPManual")
		if len(manual) > camera.MaxTimeSyncNTPServers {
			return camera.ErrUnsupported
		}
		for _, host := range manual {
			server, ok := networkHost(host)
			if !ok {
				return camera.ErrUnsupported
			}
			out.NTPManual = append(out.NTPManual, server)
		}
		return nil
	})
	if err != nil {
		return TimeConfiguration{}, err
	}
	clock, r, err := c.readClock(ctx, base.String())
	if err != nil {
		if errors.Is(err, ErrDeviceTime) {
			return TimeConfiguration{}, camera.ErrUnsupported
		}
		return TimeConfiguration{}, err
	}
	out.Clock, out.Mode, out.TZ = clock, clock.Mode, clock.TZ
	at := r.all("SystemDateAndTime")[0]
	if daylight, ok := leafText(at, "DaylightSavings"); ok && (daylight == "true" || daylight == "1") {
		out.DaylightSavings = true
	}
	return out, nil
}

// networkHost is one tt:NetworkHost as text: its address or DNS name for its
// Type, printable and bounded.
func networkHost(n *node) (string, bool) {
	kind, ok := leafText(n, "Type")
	if !ok {
		return "", false
	}
	field := map[string]string{"IPv4": "IPv4Address", "IPv6": "IPv6Address", "DNS": "DNSname"}[kind]
	if field == "" {
		return "", false
	}
	v, ok := leafText(n, field)
	if !ok || v == "" || len(v) > 255 || strings.ContainsFunc(v, func(r rune) bool { return r <= ' ' || r > '~' }) {
		return "", false
	}
	return v, true
}

// SetNTP points the device at one manual IPv4 server, not one from DHCP:
// SetNTP(FromDHCP=false, NTPManual=[IPv4 server]). A device fault, such as
// ActionNotSupported, is camera.ErrUnsupported.
func (c *Client) SetNTP(ctx context.Context, device camera.DeviceHint, lease camera.CredentialLease, server netip.Addr) error {
	if !server.Is4() || server.IsUnspecified() || server.IsMulticast() {
		return camera.ErrInvalid
	}
	body := "<t:FromDHCP>false</t:FromDHCP><t:NTPManual><tt:Type>IPv4</tt:Type><tt:IPv4Address>" + escape(server.String()) + "</tt:IPv4Address></t:NTPManual>"
	return c.writeTime(ctx, device, lease, "SetNTP", body)
}

// SetNTPDateTime makes the device keep its clock by NTP in time zone tz:
// SetSystemDateAndTime(DateTimeType=NTP, DaylightSavings=false, TimeZone{TZ}).
// It sends no UTCDateTime. A device fault is camera.ErrUnsupported.
func (c *Client) SetNTPDateTime(ctx context.Context, device camera.DeviceHint, lease camera.CredentialLease, tz string) error {
	if !camera.ValidPOSIXTimeZone(tz) {
		return camera.ErrInvalid
	}
	body := "<t:DateTimeType>NTP</t:DateTimeType><t:DaylightSavings>false</t:DaylightSavings><t:TimeZone><tt:TZ>" + escape(tz) + "</tt:TZ></t:TimeZone>"
	return c.writeTime(ctx, device, lease, "SetSystemDateAndTime", body)
}

func (c *Client) writeTime(ctx context.Context, device camera.DeviceHint, lease camera.CredentialLease, operation, body string) error {
	if lease == nil {
		return camera.ErrAuthRequired
	}
	base, err := deviceEndpoint(device)
	if err != nil {
		return err
	}
	// The write can move the device's clock, answered or not: a WS-Security
	// Created value after it must come from a fresh reading.
	defer c.forgetDeviceTime(base.String())
	return lease.Use(ctx, func(material camera.CredentialMaterial) error {
		_, err := c.request(ctx, base.String(), deviceNS, operation, body, material)
		return timeError(err)
	})
}

// timeError classifies the device declining a time operation, which a fault
// says, as unsupported. Authorization, transport and context errors are kept.
func timeError(err error) error {
	if errors.Is(err, errDeviceFault) {
		return camera.ErrUnsupported
	}
	return err
}

// forgetDeviceTime drops the authority's cached clock sample, so the next
// authenticated request reads the device's clock again.
func (c *Client) forgetDeviceTime(endpoint string) {
	u, err := validatedEndpoint(endpoint)
	if err != nil {
		return
	}
	c.clockMu.Lock()
	defer c.clockMu.Unlock()
	delete(c.clocks, u.Scheme+"://"+u.Host)
}
