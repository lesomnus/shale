package cameratest_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/discovery"
	"github.com/lesomnus/shale/camera/onvif"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/stretchr/testify/require"
)

func TestCameraONVIFWriteChangesActualRTSPMedia(t *testing.T) {
	requireMediaTools(t)
	before, after := profiles()
	fixture := cameratest.ServeCamera(t, cameratest.CameraOptions{IP: "127.0.0.2", RTSPPath: "/main", Profile: before, Modes: []camera.MediaProfile{before, after}, Identity: cameratest.CameraIdentity{Serial: "camera-A", MAC: "02:00:00:00:00:0a", DiscoveryID: "urn:uuid:00000000-0000-4000-8000-00000000000a"}})
	fixture.ONVIF.RequireCredentials("fixture", "camera-fixture")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	lease := camera.NewCredentialLease(camera.CredentialRef{Provider: "test", ID: "camera-A", Version: "1", Scope: camera.EnrollmentScope{TenantID: "test"}, Purpose: camera.AuthManagement}, camera.CredentialMaterial{Username: "fixture", Password: "camera-fixture"}, time.Now().Add(time.Minute))
	defer lease.Close()
	client := &onvif.Client{}
	state, err := client.Inspect(ctx, fixture.ONVIF.Hint("camera-A"), lease)
	require.NoError(t, err)
	old, err := probeRTSP(ctx, fixture.Endpoints().RTSP, 1)
	require.NoError(t, err)
	plan, err := client.Plan(ctx, state, camera.DesiredState{ProfileToken: "main", Codec: after.Codec, Width: after.Width, Height: after.Height, FPS: after.FPS, Bitrate: after.Bitrate, GOP: after.GOP})
	require.NoError(t, err)
	readback, err := client.Apply(ctx, fixture.ONVIF.Hint("camera-A"), lease, plan)
	require.NoError(t, err)
	require.Equal(t, after.Width, readback.Profiles[0].Width)
	actual, err := probeRTSP(ctx, fixture.Endpoints().RTSP, 1)
	require.NoError(t, err)
	require.Equal(t, before.Width, old.Width)
	require.Equal(t, after.Width, actual.Width)
	require.Equal(t, after.Height, actual.Height)
	require.InDelta(t, after.FPS, actual.FPS, 0.1)
	require.Greater(t, actual.Bitrate, 2*old.Bitrate)
	writes, activations := fixture.ONVIF.Writes()
	require.Equal(t, 1, writes)
	require.Zero(t, activations)
	t.Logf("ONVIF Set produced actual %dx%d %.2ffps %.0fbps (before %.0fbps)", actual.Width, actual.Height, actual.FPS, actual.Bitrate, old.Bitrate)
}

func TestCameraIdentityDiscoveryRebindAndReset(t *testing.T) {
	requireMediaTools(t)
	before, after := profiles()
	identity := cameratest.CameraIdentity{Serial: "camera-A", MAC: "02:00:00:00:00:0a", DiscoveryID: "urn:uuid:00000000-0000-4000-8000-00000000000a"}
	fixture := cameratest.ServeCamera(t, cameratest.CameraOptions{IP: "127.0.0.2", Profile: after, Identity: identity})
	other := cameratest.ServeCamera(t, cameratest.CameraOptions{IP: "127.0.0.3", Profile: before, Identity: cameratest.CameraIdentity{Serial: "camera-B", MAC: "02:00:00:00:00:0b", DiscoveryID: "urn:uuid:00000000-0000-4000-8000-00000000000b"}})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	client := &onvif.Client{}
	first, err := client.Inspect(ctx, fixture.ONVIF.Hint("a"), nil)
	require.NoError(t, err)
	second, err := client.Inspect(ctx, other.ONVIF.Hint("b"), nil)
	require.NoError(t, err)
	require.NotEqual(t, first.Device.Serial, second.Device.Serial)
	require.NotEqual(t, first.Device.MAC, second.Device.MAC)
	require.NotEqual(t, first.Device.DiscoveryID, second.Device.DiscoveryID)
	probe := fixture.ONVIF.ServeProbe(t, "127.0.0.1:0")
	discover := discovery.Discoverer{Target: probe}
	found, err := discover.Discover(ctx, camera.DiscoveryScope{ID: "fixture-net", LocalAddress: "127.0.0.1", Multicast: true, Timeout: 500 * time.Millisecond})
	require.NoError(t, err)
	require.Len(t, found, 1)
	require.Equal(t, identity.DiscoveryID, found[0].Device.DiscoveryID)
	require.Equal(t, fixture.Endpoints().ONVIF, found[0].Device.Endpoints[0].URL)
	old := fixture.Endpoints()
	conn := openFixtureSession(t, fixture)
	next, err := fixture.Rebind("127.0.0.4")
	require.NoError(t, err)
	require.NotEqual(t, old, next)
	assertSessionClosed(t, conn)
	oldURL, err := url.Parse(old.ONVIF)
	require.NoError(t, err)
	stale, err := net.DialTimeout("tcp", oldURL.Host, 100*time.Millisecond)
	if stale != nil {
		stale.Close()
	}
	require.Error(t, err, "the previous management listener must be closed")
	found, err = discover.Discover(ctx, camera.DiscoveryScope{ID: "fixture-net", LocalAddress: "127.0.0.1", Multicast: true, Timeout: 500 * time.Millisecond})
	require.NoError(t, err)
	require.Len(t, found, 1)
	require.Equal(t, next.ONVIF, found[0].Device.Endpoints[0].URL)
	require.Equal(t, identity.DiscoveryID, found[0].Device.DiscoveryID, "a listener move must not invent another camera")
	conn = openFixtureSession(t, fixture)
	require.NoError(t, fixture.Reset(ctx, before))
	assertSessionClosed(t, conn)
	reset, err := client.Inspect(ctx, fixture.ONVIF.Hint("a"), nil)
	require.NoError(t, err)
	require.Equal(t, identity.Serial, reset.Device.Serial)
	require.Equal(t, identity.MAC, reset.Device.MAC)
	require.Equal(t, before.Width, reset.Current.Profiles[0].Width)
	actual, err := probeRTSP(ctx, fixture.Endpoints().RTSP, 1)
	require.NoError(t, err)
	require.Equal(t, before.Width, actual.Width)
	require.InDelta(t, before.FPS, actual.FPS, 0.1)
}

func TestCameraDHCPAddressChangeKeepsServicePorts(t *testing.T) {
	requireMediaTools(t)
	profile, _ := profiles()
	fixture := cameratest.ServeCamera(t, cameratest.CameraOptions{IP: "127.0.0.5", Profile: profile, Identity: cameratest.CameraIdentity{Serial: "stable-dhcp-camera"}})
	before := fixture.Endpoints()
	oldManagement, err := url.Parse(before.ONVIF)
	require.NoError(t, err)
	oldMedia, err := url.Parse(before.RTSP)
	require.NoError(t, err)
	session := openFixtureSession(t, fixture)
	after, err := fixture.RebindAddress("127.0.0.6")
	require.NoError(t, err)
	management, err := url.Parse(after.ONVIF)
	require.NoError(t, err)
	stream, err := url.Parse(after.RTSP)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.6", management.Hostname())
	require.Equal(t, oldManagement.Port(), management.Port())
	require.Equal(t, oldMedia.Port(), stream.Port(), "DHCP must not invent a different camera service port")
	assertSessionClosed(t, session)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	inspection, err := (&onvif.Client{}).Inspect(ctx, fixture.ONVIF.Hint("stable-dhcp-camera"), nil)
	require.NoError(t, err)
	require.Equal(t, "stable-dhcp-camera", inspection.Device.Serial)
	actual, err := probeRTSP(ctx, after.RTSP, 1)
	require.NoError(t, err)
	require.Equal(t, profile.Width, actual.Width)
	replayed, err := fixture.RebindAddress("127.0.0.6")
	require.NoError(t, err)
	require.Equal(t, after, replayed)
}

func openFixtureSession(t *testing.T, c *cameratest.Camera) net.Conn {
	t.Helper()
	endpoint, err := url.Parse(c.Endpoints().RTSP)
	require.NoError(t, err)
	conn, err := net.DialTimeout("tcp", endpoint.Host, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(2*time.Second)))
	reader := bufio.NewReader(conn)
	for i, method := range []string{"OPTIONS", "DESCRIBE", "SETUP", "PLAY"} {
		uri, headers := endpoint.String(), ""
		if method == "SETUP" {
			uri += "/trackID=0"
			headers = "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n"
		}
		if method == "PLAY" {
			headers = "Session: fixture\r\n"
		}
		_, err = fmt.Fprintf(conn, "%s %s RTSP/1.0\r\nCSeq: %d\r\n%s\r\n", method, uri, i+1, headers)
		require.NoError(t, err)
		status, err := reader.ReadString('\n')
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(status, "RTSP/1.0 200"))
		header, err := textproto.NewReader(reader).ReadMIMEHeader()
		require.NoError(t, err)
		length, err := strconv.Atoi(header.Get("Content-Length"))
		require.NoError(t, err)
		_, err = io.CopyN(io.Discard, reader, int64(length))
		require.NoError(t, err)
	}
	// Observe actual interleaved video before moving/resetting the camera.
	var prefix [4]byte
	_, err = io.ReadFull(reader, prefix[:])
	require.NoError(t, err)
	require.Equal(t, byte('$'), prefix[0])
	_, err = io.CopyN(io.Discard, reader, int64(binary.BigEndian.Uint16(prefix[2:])))
	require.NoError(t, err)
	require.NoError(t, conn.SetDeadline(time.Time{}))
	return &fixtureSession{Conn: conn, reader: reader}
}

type fixtureSession struct {
	net.Conn
	reader *bufio.Reader
}

func (c *fixtureSession) Read(b []byte) (int, error) { return c.reader.Read(b) }
func assertSessionClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	// Buffered packets may drain after close; a live stream would instead hit
	// this deadline. EOF is required before any new-profile session is opened.
	_, err := io.Copy(io.Discard, conn)
	require.NoError(t, err)
}
