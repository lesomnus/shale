package cameratest

import (
	"bytes"
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/tlsidentity"
	"github.com/stretchr/testify/require"
)

func TestWebKeyMovesWithTheCameraAndAnImpostorPresentsAnother(t *testing.T) {
	key := NewWebKey(t)
	c := ServeONVIFWithOptions(t, camera.MediaProfile{Codec: "h264", Width: 160, Height: 90, FPS: 10}, ONVIFOptions{})
	web := c.ServeWebTLS(t, 0, key)
	u, err := url.Parse(web)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", u.Hostname())
	pin, err := tlsidentity.Observe(t.Context(), web)
	require.NoError(t, err)
	require.Equal(t, key.SPKI, pin)
	// The camera counts a handshake when its own side is done, and under
	// TLS 1.3 the observer's side is done first.
	require.Eventually(t, func() bool { return c.WebHandshakes() == 1 }, 5*time.Second, time.Millisecond)
	require.Empty(t, c.AuthAttempts(), "a handshake presents no credential")

	require.NoError(t, c.MoveTo("127.0.0.2"))
	moved, err := url.Parse(c.WebTLSURL())
	require.NoError(t, err)
	require.Equal(t, "127.0.0.2", moved.Hostname())
	require.Equal(t, u.Port(), moved.Port(), "a DHCP move keeps the configured web port")
	require.True(t, strings.HasPrefix(c.CurrentURL(), "http://127.0.0.2:"))
	pin, err = tlsidentity.Observe(t.Context(), c.WebTLSURL())
	require.NoError(t, err)
	require.Equal(t, key.SPKI, pin, "the same camera presents the same key")
	_, err = tlsidentity.Observe(t.Context(), web)
	require.Error(t, err, "nothing answers at the old address")

	// An impostor with the same CA's certificate is still another key.
	other := key.Issue(t)
	require.NotEqual(t, key.SPKI, other.SPKI)
	require.NoError(t, other.Certificate.Leaf.CheckSignatureFrom(key.Certificate.Leaf))
	c.SetWebKey(other)
	pin, err = tlsidentity.Observe(t.Context(), c.WebTLSURL())
	require.NoError(t, err)
	require.Equal(t, other.SPKI, pin)
}

func TestCameraRecordsEveryAuthenticationAttempt(t *testing.T) {
	c := ServeONVIFWithOptions(t, camera.MediaProfile{Codec: "h264", Width: 160, Height: 90, FPS: 10}, ONVIFOptions{})
	c.ServeWebTLS(t, 0, NewWebKey(t))
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	post := func(target, kind, body string) {
		t.Helper()
		r, err := http.NewRequestWithContext(context.Background(), http.MethodPost, target, bytes.NewBufferString(body))
		require.NoError(t, err)
		r.Header.Set("Content-Type", kind)
		v, err := client.Do(r)
		require.NoError(t, err)
		v.Body.Close()
	}
	// The anonymous activation challenge and an unauthenticated SOAP call are
	// not attempts; a token or any other vendor call is.
	post(c.WebTLSURL()+"/", "application/json", `{"method":"do","user_management":{"get_encrypt_info":null}}`)
	post(c.URL, "application/soap+xml", `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><GetDeviceInformation/></s:Body></s:Envelope>`)
	require.Empty(t, c.AuthAttempts())
	post(c.URL, "application/soap+xml", `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Header><Security><UsernameToken><Username>admin</Username><Password>digest</Password></UsernameToken></Security></s:Header><s:Body><GetDeviceInformation/></s:Body></s:Envelope>`)
	post(c.WebTLSURL()+"/", "application/json", `{"method":"do","login":{"username":"admin","password":"secret"}}`)
	attempts := c.AuthAttempts()
	require.Len(t, attempts, 2)
	require.Equal(t, []string{"onvif", "web"}, []string{attempts[0].Kind, attempts[1].Kind})
	for _, a := range attempts {
		require.Equal(t, "127.0.0.1", a.Host)
		require.False(t, a.At.IsZero())
	}
}

func TestRTSPConnectionsCountEveryAddress(t *testing.T) {
	c := ServeCamera(t, CameraOptions{Profile: camera.MediaProfile{Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 128_000, GOP: 10}})
	require.Zero(t, c.Media.Connections())
	endpoints, err := c.RebindAddress("127.0.0.2")
	require.NoError(t, err)
	require.Contains(t, endpoints.RTSP, "127.0.0.2")
	require.Zero(t, c.Media.Connections(), "moving opens no connection")
}
