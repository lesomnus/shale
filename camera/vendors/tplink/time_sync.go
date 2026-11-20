package tplink

import (
	"context"
	"net/netip"
	"slices"
	"strings"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/onvif"
)

const timeSyncPlanRevision = "tplink/c420/onvif-time-sync-v1"

// TimeServerFunc resolves a time_sync NTP setting for the camera at hint to
// the IPv4 server written to it: camera.TimeSyncEdge to the address the
// worker reaches that camera from, an IPv4 literal to itself. A server outside
// the camera network is camera.ErrTimeServerRefused. The managed driver
// supplies it, from the camera network's prefixes; without one, time sync is
// unsupported.
type TimeServerFunc func(ctx context.Context, hint camera.DeviceHint, ntp string) (netip.Addr, error)

// timeSyncTarget is the one build time sync is qualified for: HW1.0 firmware
// 1.2.2 Build260624, as smart codec. The qualification is ONVIF's: SetNTP and
// SetSystemDateAndTime are standard device-service operations, and this build
// answers GetSystemDateAndTime in that shape. Their acceptance on the C420
// itself is to be confirmed on hardware (PROTOCOL.md, "Time sync"); until then
// a fault from the device is a warning, not a failed enrollment.
func timeSyncTarget(h camera.DeviceHint) bool {
	if deviceProfile(h) != ProfileC420V122 {
		return false
	}
	fw := strings.ReplaceAll(strings.Join(strings.Fields(h.Firmware), " "), "Build ", "Build")
	return fw == "1.2.2 Build260624" || fw == "1.2.2 Build260624 Rel.39352n"
}

// planTimeSync resolves the server and reads the device's NTP and date-time
// configuration with ONVIF. It never opens a web session: the web login
// helper and its boot_set_date stay unused.
func (d *Driver) planTimeSync(ctx context.Context, hint camera.DeviceHint, lease camera.CredentialLease, desired camera.TimeSync) (camera.InitializationPlan, error) {
	if desired.IsZero() || desired.Validate() != nil {
		return camera.InitializationPlan{}, camera.ErrInvalid
	}
	server, err := d.timeSyncServer(ctx, hint, desired)
	if err != nil {
		return camera.InitializationPlan{}, err
	}
	current, err := d.baseline().ReadTimeConfiguration(ctx, hint, lease)
	if err != nil {
		return camera.InitializationPlan{}, err
	}
	plan := camera.InitializationPlan{After: camera.DeviceSettings{TimeSync: desired}, Revision: timeSyncPlanRevision, TimeSync: &camera.InitializationTimeSync{
		Server:                server.String(),
		BeforeNTPFromDHCP:     current.NTPFromDHCP,
		BeforeNTP:             slices.Clone(current.NTPManual),
		BeforeMode:            current.Mode,
		BeforeTZ:              current.TZ,
		BeforeDaylightSavings: current.DaylightSavings,
	}}
	if !ntpSet(current, server) {
		plan.Changes = append(plan.Changes, camera.TimeSyncChangeNTP)
	}
	if !zoneSet(current, desired.TimeZone) {
		plan.Changes = append(plan.Changes, camera.TimeSyncChangeTimeZone)
	}
	if len(plan.Changes) == 0 {
		plan.Before = plan.After
	}
	return plan, nil
}

// applyTimeSync writes what differs, NTP first, then reads both back. A
// write that the device declines is camera.ErrUnsupported; one it takes but
// does not keep is too, since the device then does not do what was asked.
// The server is resolved again and must still be the plan's.
func (d *Driver) applyTimeSync(ctx context.Context, hint camera.DeviceHint, lease camera.CredentialLease, plan camera.InitializationPlan) (camera.DeviceSettings, error) {
	desired := plan.After.TimeSync
	if camera.ValidateTimeSyncPlan(plan, desired) != nil {
		return camera.DeviceSettings{}, camera.ErrInvalid
	}
	if plan.Revision != timeSyncPlanRevision {
		return camera.DeviceSettings{}, camera.ErrConflict
	}
	server, err := d.timeSyncServer(ctx, hint, desired)
	if err != nil {
		return camera.DeviceSettings{}, err
	}
	if server.String() != plan.TimeSync.Server {
		return camera.DeviceSettings{}, camera.ErrConflict
	}
	o := d.baseline()
	current, err := o.ReadTimeConfiguration(ctx, hint, lease)
	if err != nil {
		return camera.DeviceSettings{}, err
	}
	if !ntpSet(current, server) {
		if err := o.SetNTP(ctx, hint, lease, server); err != nil {
			return camera.DeviceSettings{}, err
		}
	}
	if !zoneSet(current, desired.TimeZone) {
		if err := o.SetNTPDateTime(ctx, hint, lease, desired.TimeZone); err != nil {
			return camera.DeviceSettings{}, err
		}
	}
	readback, err := o.ReadTimeConfiguration(ctx, hint, lease)
	if err != nil {
		return camera.DeviceSettings{}, err
	}
	if !ntpSet(readback, server) || !zoneSet(readback, desired.TimeZone) {
		return camera.DeviceSettings{}, camera.ErrUnsupported
	}
	return camera.DeviceSettings{TimeSync: desired}, nil
}

func (d *Driver) timeSyncServer(ctx context.Context, hint camera.DeviceHint, desired camera.TimeSync) (netip.Addr, error) {
	if err := d.initializationTarget(hint); err != nil {
		return netip.Addr{}, err
	}
	if !timeSyncTarget(hint) || d.TimeServer == nil {
		return netip.Addr{}, camera.ErrUnsupported
	}
	server, err := d.TimeServer(ctx, hint, desired.NTP)
	if err != nil {
		return netip.Addr{}, err
	}
	// An explicit server resolves to itself and nothing else.
	if explicit, ok := desired.Server(); ok && explicit != server || !server.Is4() {
		return netip.Addr{}, camera.ErrTimeServerRefused
	}
	return server, nil
}

// ntpSet is a device that takes its time from server alone, not from DHCP.
func ntpSet(c onvif.TimeConfiguration, server netip.Addr) bool {
	return !c.NTPFromDHCP && len(c.NTPManual) == 1 && c.NTPManual[0] == server.String()
}

// zoneSet is a device that keeps its clock by NTP in time zone tz, with no
// separate daylight-saving flag: tz says it, if anything does.
func zoneSet(c onvif.TimeConfiguration, tz string) bool {
	return c.Mode == camera.ClockModeNTP && !c.DaylightSavings && c.TZ == tz
}
