package tplink

import (
	"context"
	"errors"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/onvif"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/stretchr/testify/require"
)

var kst = camera.TimeSync{NTP: camera.TimeSyncEdge, TimeZone: "KST-9"}

// timeSyncCamera is a qualified C420 on 1.2.2, about 147 days behind, keeping
// time by NTP from a server it cannot reach: what an edge with six C420s found.
func timeSyncCamera(t *testing.T) (*cameratest.ONVIFCamera, *Driver, camera.DeviceHint, camera.CredentialLease) {
	t.Helper()
	c := cameratest.ServeONVIF(t, camera.MediaProfile{Codec: "h264", Width: 1920, Height: 1080, FPS: 25, StreamURI: "rtsp://127.0.0.1/main"})
	c.RequireCredentials("admin", "time sync fixture")
	c.SetClock(-147*24*time.Hour, camera.ClockModeNTP, "GMT0GMT")
	c.SetNTPConfig(false, "pool.ntp.example")
	d := NewDriver(onvif.NewClient(nil), c.WebURL)
	d.TimeServer = func(_ context.Context, _ camera.DeviceHint, ntp string) (netip.Addr, error) {
		if ntp == camera.TimeSyncEdge {
			return netip.MustParseAddr("192.0.2.1"), nil
		}
		return netip.ParseAddr(ntp)
	}
	lease := camera.NewCredentialLease(camera.CredentialRef{Provider: "fixture", ID: "c420-admin", Version: "1", Purpose: camera.AuthManagement, Scope: camera.EnrollmentScope{TenantID: "lab"}, Recoverable: true}, camera.CredentialMaterial{Username: "admin", Password: "time sync fixture"}, time.Now().Add(time.Minute))
	t.Cleanup(func() { _ = lease.Close() })
	hint := c.Hint("c420")
	hint.Evidence = []camera.Evidence{{Source: "onvif/device", Authenticated: true}}
	return c, d, hint, lease
}

func timeWrites(c *cameratest.ONVIFCamera) []string {
	var out []string
	for _, call := range c.Calls() {
		if call == "SetNTP" || call == "SetSystemDateAndTime" {
			out = append(out, call)
		}
	}
	return out
}

func TestTimeSyncPlansReadOnlyThenWritesNTPBeforeTheZone(t *testing.T) {
	c, d, hint, lease := timeSyncCamera(t)
	plan, err := d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: kst})
	require.NoError(t, err)
	require.NoError(t, camera.ValidateTimeSyncPlan(plan, kst))
	require.Equal(t, []string{camera.TimeSyncChangeNTP, camera.TimeSyncChangeTimeZone}, plan.Changes)
	require.Equal(t, camera.DeviceSettings{}, plan.Before)
	require.Equal(t, &camera.InitializationTimeSync{Server: "192.0.2.1", BeforeNTP: []string{"pool.ntp.example"}, BeforeMode: camera.ClockModeNTP, BeforeTZ: "GMT0GMT"}, plan.TimeSync, "the plan names the resolved server and what it replaces")
	require.Empty(t, timeWrites(c), "planning writes nothing")

	applied, err := d.ApplyInitialization(t.Context(), hint, lease, plan)
	require.NoError(t, err)
	require.Equal(t, camera.DeviceSettings{TimeSync: kst}, applied)
	require.Equal(t, []string{"SetNTP", "SetSystemDateAndTime"}, timeWrites(c), "NTP first, then the zone")
	fromDHCP, servers, mode, tz := c.TimeConfig()
	require.False(t, fromDHCP)
	require.Equal(t, []string{"192.0.2.1"}, servers)
	require.Equal(t, camera.ClockModeNTP, mode)
	require.Equal(t, "KST-9", tz)
	for _, call := range c.Calls() {
		require.NotContains(t, []string{"get_encrypt_info", "set_password", "unexpected_vendor_write"}, call, "time sync never opens the web UI")
	}

	again, err := d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: kst})
	require.NoError(t, err)
	require.Empty(t, again.Changes, "an already-right camera needs no write")
	require.Equal(t, again.After, again.Before)
	_, err = d.ApplyInitialization(t.Context(), hint, lease, again)
	require.NoError(t, err)
	require.Len(t, timeWrites(c), 2, "a replay writes nothing more")
}

func TestTimeSyncWritesOnlyWhatDiffers(t *testing.T) {
	c, d, hint, lease := timeSyncCamera(t)
	c.SetNTPConfig(false, "192.0.2.1")
	plan, err := d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: kst})
	require.NoError(t, err)
	require.Equal(t, []string{camera.TimeSyncChangeTimeZone}, plan.Changes)
	_, err = d.ApplyInitialization(t.Context(), hint, lease, plan)
	require.NoError(t, err)
	require.Equal(t, []string{"SetSystemDateAndTime"}, timeWrites(c))

	c, d, hint, lease = timeSyncCamera(t)
	c.SetClock(0, camera.ClockModeNTP, "KST-9")
	c.SetNTPConfig(true)
	plan, err = d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: kst})
	require.NoError(t, err)
	require.Equal(t, []string{camera.TimeSyncChangeNTP}, plan.Changes, "a server from DHCP is not the edge")
	_, err = d.ApplyInitialization(t.Context(), hint, lease, plan)
	require.NoError(t, err)
	require.Equal(t, []string{"SetNTP"}, timeWrites(c))
}

func TestTimeSyncIsQualifiedForOneBuildOnly(t *testing.T) {
	for _, firmware := range []string{"1.2.0 Build251020 Rel.55235n", "1.2.2 Build260625", "1.2.3 Build260624"} {
		c, d, hint, lease := timeSyncCamera(t)
		hint.Firmware = firmware
		_, err := d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: kst})
		require.ErrorIs(t, err, camera.ErrUnsupported, firmware)
		require.Empty(t, c.Calls(), "an unqualified build is not asked anything")
	}
	c, d, hint, lease := timeSyncCamera(t)
	hint.Model = "EasyCam C425"
	_, err := d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: kst})
	require.ErrorIs(t, err, camera.ErrUnsupported)
	hint = c.Hint("c420")
	d.TimeServer = nil
	_, err = d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: kst})
	require.ErrorIs(t, err, camera.ErrUnsupported, "without the camera network's resolver there is no time sync")
	require.Empty(t, c.Calls())
	_, err = d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{PowerLineFrequency: 60, TimeSync: kst})
	require.ErrorIs(t, err, camera.ErrInvalid, "a time sync is planned on its own")
}

func TestTimeSyncServerIsTheResolversAndNeverAnother(t *testing.T) {
	c, d, hint, lease := timeSyncCamera(t)
	d.TimeServer = func(context.Context, camera.DeviceHint, string) (netip.Addr, error) {
		return netip.Addr{}, camera.ErrTimeServerRefused
	}
	_, err := d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: kst})
	require.ErrorIs(t, err, camera.ErrTimeServerRefused)
	d.TimeServer = func(context.Context, camera.DeviceHint, string) (netip.Addr, error) {
		return netip.MustParseAddr("192.0.2.200"), nil
	}
	_, err = d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: camera.TimeSync{NTP: "192.0.2.7", TimeZone: "KST-9"}})
	require.ErrorIs(t, err, camera.ErrTimeServerRefused, "an explicit server resolves to itself only")
	require.Empty(t, c.Calls())

	d.TimeServer = func(context.Context, camera.DeviceHint, string) (netip.Addr, error) {
		return netip.MustParseAddr("192.0.2.1"), nil
	}
	plan, err := d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: kst})
	require.NoError(t, err)
	// The worker's address moved between planning and applying: plan again.
	d.TimeServer = func(context.Context, camera.DeviceHint, string) (netip.Addr, error) {
		return netip.MustParseAddr("192.0.2.2"), nil
	}
	_, err = d.ApplyInitialization(t.Context(), hint, lease, plan)
	require.ErrorIs(t, err, camera.ErrConflict)
	require.Empty(t, timeWrites(c))
	tampered := plan
	tampered.TimeSync = &camera.InitializationTimeSync{Server: "ntp.example.com"}
	_, err = d.ApplyInitialization(t.Context(), hint, lease, tampered)
	require.ErrorIs(t, err, camera.ErrInvalid)
	require.Empty(t, timeWrites(c))
}

func TestTimeSyncThatTheDeviceDeclinesIsUnsupported(t *testing.T) {
	c, d, hint, lease := timeSyncCamera(t)
	c.SetTimeSync(cameratest.TimeSyncBehavior{Unsupported: true})
	_, err := d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: kst})
	require.ErrorIs(t, err, camera.ErrUnsupported)
	require.Empty(t, timeWrites(c))

	c, d, hint, lease = timeSyncCamera(t)
	c.SetTimeSync(cameratest.TimeSyncBehavior{RejectDateTime: true})
	plan, err := d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: kst})
	require.NoError(t, err)
	_, err = d.ApplyInitialization(t.Context(), hint, lease, plan)
	require.True(t, errors.Is(err, camera.ErrUnsupported), "%v", err)
	// SetNTP was taken before the zone was declined; a fresh plan says so.
	observed, err := d.PlanInitialization(t.Context(), hint, lease, camera.DeviceSettings{TimeSync: kst})
	require.NoError(t, err)
	require.Equal(t, []string{camera.TimeSyncChangeTimeZone}, observed.Changes)
	require.Equal(t, []string{"192.0.2.1"}, observed.TimeSync.BeforeNTP)
	require.True(t, slices.Equal([]string{"SetNTP", "SetSystemDateAndTime"}, timeWrites(c)))
	require.False(t, strings.Contains(strings.Join(c.Calls(), ","), "get_encrypt_info"))
}
