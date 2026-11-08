package cameratest

import (
	"context"
	"encoding/xml"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
)

type CameraIdentity struct{ Manufacturer, Model, Hardware, Firmware, Serial, MAC, DiscoveryID string }

func defaultIdentity(i CameraIdentity) CameraIdentity {
	defaults := CameraIdentity{Manufacturer: "TP-Link", Model: "EasyCam C420", Hardware: "1.0", Firmware: "1.2.2 Build260624 Rel.39352n", Serial: "synthetic-camera", MAC: "02:00:00:00:00:01", DiscoveryID: "urn:uuid:00000000-0000-4000-8000-000000000001"}
	for _, p := range [][2]*string{{&i.Manufacturer, &defaults.Manufacturer}, {&i.Model, &defaults.Model}, {&i.Hardware, &defaults.Hardware}, {&i.Firmware, &defaults.Firmware}, {&i.Serial, &defaults.Serial}, {&i.MAC, &defaults.MAC}, {&i.DiscoveryID, &defaults.DiscoveryID}} {
		if *p[0] == "" {
			*p[0] = *p[1]
		}
	}
	return i
}

type CameraOptions struct {
	IP           string
	RTSPPath     string
	Profile      camera.MediaProfile
	Modes        []camera.MediaProfile
	Identity     CameraIdentity
	SetupTimeout time.Duration
}
type CameraEndpoints struct{ ONVIF, RTSP string }
type Camera struct {
	ONVIF *ONVIFCamera
	Media *RTSPCamera
}

// ServeCamera links real bounded synthetic H264 to ONVIF configuration writes.
// The legacy ServeONVIF/SetVideo remain metadata-only mismatch fixtures.
func ServeCamera(t testing.TB, o CameraOptions) *Camera {
	t.Helper()
	if o.IP == "" {
		o.IP = "127.0.0.1"
	}
	media := ServeRTSPCamera(t, RTSPOptions{ListenAddress: net.JoinHostPort(o.IP, "0"), Path: o.RTSPPath, Profile: o.Profile, SetupTimeout: o.SetupTimeout})
	onvif := ServeONVIFWithOptions(t, media.Profile(), ONVIFOptions{ListenAddress: net.JoinHostPort(o.IP, "0"), Identity: o.Identity, Media: media, Modes: o.Modes})
	return &Camera{ONVIF: onvif, Media: media}
}
func (c *Camera) Endpoints() CameraEndpoints {
	return CameraEndpoints{ONVIF: c.ONVIF.CurrentURL(), RTSP: c.Media.URL()}
}

// Reset restores the supplied media profile and closes every RTSP session. It
// preserves identity/test credentials and does not emulate vendor account reset.
func (c *Camera) Reset(ctx context.Context, p camera.MediaProfile) error {
	if err := c.Media.SetProfile(ctx, p); err != nil {
		return err
	}
	c.ONVIF.SetVideo(c.Media.Profile())
	c.Media.Disconnect()
	return nil
}

// Rebind moves test listeners and terminates old HTTP/RTSP sessions. It does
// not implement DHCP or modify the host's interfaces, addresses or routes.
func (c *Camera) Rebind(ip string) (CameraEndpoints, error) {
	if _, err := c.Media.Rebind(net.JoinHostPort(ip, "0")); err != nil {
		return CameraEndpoints{}, err
	}
	if err := c.ONVIF.rebind(net.JoinHostPort(ip, "0")); err != nil {
		return CameraEndpoints{}, err
	}
	c.ONVIF.SetVideo(c.Media.Profile())
	return c.Endpoints(), nil
}

func (c *ONVIFCamera) configure(ctx context.Context, p camera.MediaProfile) error {
	// Caller holds c.mu. Media has its own immutable snapshot and never calls
	// back into this management lock.
	if c.media != nil {
		if err := c.media.SetProfile(ctx, p); err != nil {
			return err
		}
		p = c.media.Profile()
	}
	c.profile = p
	c.writes++
	return nil
}
func (c *ONVIFCamera) CurrentURL() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.server.URL + "/onvif/device_service"
}
func (c *ONVIFCamera) close() { c.mu.Lock(); server := c.server; c.mu.Unlock(); server.Close() }
func (c *ONVIFCamera) rebind(address string) error {
	lis, err := net.Listen("tcp4", address)
	if err != nil {
		return err
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(c.serve))
	server.Listener = lis
	server.Start()
	c.mu.Lock()
	old := c.server
	c.server = server
	c.mu.Unlock()
	old.CloseClientConnections()
	old.Close()
	return nil
}

// ProbeResponse is a pure WS-Discovery seam. Callers can inject it into a
// scoped unicast responder without sending multicast to a real network.
func (c *ONVIFCamera) ProbeResponse(relatesTo string) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return []byte(`<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:a="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:d="http://schemas.xmlsoap.org/ws/2005/04/discovery"><s:Header><a:RelatesTo>` + xmlText(relatesTo) + `</a:RelatesTo></s:Header><s:Body><d:ProbeMatches><d:ProbeMatch><a:EndpointReference><a:Address>` + xmlText(c.identity.DiscoveryID) + `</a:Address></a:EndpointReference><d:Types>NetworkVideoTransmitter</d:Types><d:XAddrs>` + xmlText(c.server.URL+"/onvif/device_service") + `</d:XAddrs></d:ProbeMatch></d:ProbeMatches></s:Body></s:Envelope>`)
}

// ServeProbe replies only on an explicitly bound unicast UDP test socket.
func (c *ONVIFCamera) ServeProbe(t testing.TB, address string) net.Addr {
	t.Helper()
	conn, err := net.ListenPacket("udp4", address)
	if err != nil {
		t.Fatalf("bind synthetic discovery: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	t.Cleanup(func() { conn.Close(); wg.Wait() })
	go func() {
		defer wg.Done()
		buffer := make([]byte, 64<<10)
		for {
			n, from, err := conn.ReadFrom(buffer)
			if err != nil {
				return
			}
			var probe struct {
				Header struct {
					MessageID string `xml:"MessageID"`
				} `xml:"Header"`
			}
			if xml.Unmarshal(buffer[:n], &probe) != nil || !strings.Contains(string(buffer[:n]), "Probe") {
				continue
			}
			_, _ = conn.WriteTo(c.ProbeResponse(probe.Header.MessageID), from)
		}
	}()
	return conn.LocalAddr()
}
