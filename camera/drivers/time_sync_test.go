package drivers

import (
	"context"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/stretchr/testify/require"
)

func timeSyncDriver(t *testing.T, c *cameratest.ONVIFCamera, cidrs ...string) *Driver {
	t.Helper()
	d, err := New(Options{NetworkID: "lan", CIDRs: cidrs, Bootstrap: bootstrapFor(t, c.WebURL), Timeout: time.Second})
	require.NoError(t, err)
	t.Cleanup(d.Close)
	return d
}

func timeSyncCalls(c *cameratest.ONVIFCamera) (reads, writes []string) {
	for _, call := range c.Calls() {
		switch call {
		case "GetNTP":
			reads = append(reads, call)
		case "SetNTP", "SetSystemDateAndTime":
			writes = append(writes, call)
		}
	}
	return reads, writes
}

// "edge" is the address this host reaches the camera from, here loopback,
// and a C420 on 1.2.2 is pointed at it through the managed driver.
func TestEdgeTimeSyncResolvesToTheWorkersAddressOnTheCameraNetwork(t *testing.T) {
	c := fixtureCamera(t)
	c.SetClock(-147*24*time.Hour, camera.ClockModeNTP, "GMT0GMT")
	d := timeSyncDriver(t, c, "127.0.0.0/8")
	hint := c.Hint("c420")
	lease := fixtureLease(t, camera.AuthManagement)
	want := camera.TimeSync{NTP: camera.TimeSyncEdge, TimeZone: "KST-9"}
	plan, err := d.PlanInitialization(context.Background(), hint, lease, camera.DeviceSettings{TimeSync: want})
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", plan.TimeSync.Server, "the source address toward a loopback camera")
	_, writes := timeSyncCalls(c)
	require.Empty(t, writes)
	_, err = d.ApplyInitialization(context.Background(), hint, lease, plan)
	require.NoError(t, err)
	_, servers, mode, tz := c.TimeConfig()
	require.Equal(t, []string{"127.0.0.1"}, servers)
	require.Equal(t, camera.ClockModeNTP, mode)
	require.Equal(t, "KST-9", tz)
	clock, err := d.ObserveClock(context.Background(), hint)
	require.NoError(t, err)
	require.InDelta(t, 0, clock.OffsetMilliseconds(), float64(camera.TimeSyncTolerance.Milliseconds()), "the fake's clock came right")
}

func TestTimeSyncServerOutsideTheCameraNetworkIsRefused(t *testing.T) {
	// The network names the camera at 127.0.0.2 alone. This host reaches it
	// from 127.0.0.1, outside, so "edge" is refused as an outside literal is.
	c := cameratest.ServeONVIFWithOptions(t, camera.MediaProfile{Codec: "h264", Width: 1920, Height: 1080, FPS: 25, StreamURI: "rtsp://127.0.0.2/main"}, cameratest.ONVIFOptions{ListenAddress: "127.0.0.2:0"})
	c.RequireCredentials("admin", fixturePassword)
	d := timeSyncDriver(t, c, "127.0.0.2/32")
	hint := c.Hint("c420")
	lease := fixtureLease(t, camera.AuthManagement)
	ntps := []string{"192.0.2.1", "127.0.0.1"}
	if source, err := localAddressToward(context.Background(), "127.0.0.2"); err == nil && source.String() == "127.0.0.1" {
		ntps = append(ntps, camera.TimeSyncEdge)
	}
	for _, ntp := range ntps {
		_, err := d.PlanInitialization(context.Background(), hint, lease, camera.DeviceSettings{TimeSync: camera.TimeSync{NTP: ntp, TimeZone: "UTC0"}})
		require.ErrorIs(t, err, camera.ErrTimeServerRefused, ntp)
	}
	server, err := d.timeServer(context.Background(), hint, "127.0.0.2")
	require.NoError(t, err)
	require.Equal(t, "127.0.0.2", server.String(), "an explicit server inside the network is itself")
	_, err = d.timeServer(context.Background(), hint, "ntp.example.com")
	require.ErrorIs(t, err, camera.ErrInvalid, "no name is resolved")
	reads, writes := timeSyncCalls(c)
	require.Empty(t, reads, "a refused server reads nothing from the camera")
	require.Empty(t, writes)
}

func TestTimeSyncWithoutABootstrapProfileIsUnsupported(t *testing.T) {
	c := fixtureCamera(t)
	d := scopedDriver(t, nil)
	_, err := d.PlanInitialization(context.Background(), c.Hint("generic"), fixtureLease(t, camera.AuthManagement), camera.DeviceSettings{TimeSync: camera.TimeSync{NTP: camera.TimeSyncEdge, TimeZone: "UTC0"}})
	require.ErrorIs(t, err, camera.ErrUnsupported, "the generic ONVIF path has no initializer")
	require.Empty(t, c.Calls())
}
