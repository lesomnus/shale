package provisionhost

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
)

func TestCPPrivateSessionFile(t *testing.T) {
	for _, value := range []string{"shale_tenant=sealed-session", "__Host-shale_tenant=sealed-session\n"} {
		path := filepath.Join(t.TempDir(), "session")
		require.NoError(t, os.WriteFile(path, []byte(value), 0600))
		got, err := readSession(path)
		require.NoError(t, err)
		require.Equal(t, strings.TrimSuffix(value, "\n"), got)
	}
	for _, value := range []string{
		"", "shale_tenant=", "sealed-bearer-token", "Bearer sealed-session",
		"shale_cluster=sealed-session", "__Host-shale_cluster=sealed-session",
		"session=sealed-session", "shale_tenant=sealed-session; other=credential",
		"shale_tenant=sealed-session; Path=/", "shale_tenant=\"sealed-session\"",
		"shale_tenant=sealed session", "shale_tenant=sealed\tsession",
		"shale_tenant=sealed-session\r\n", "shale_tenant=sealed-session\n\n",
		"shale_tenant=sealed-session\nx-header: injected", "shale_tenant=\x00",
		"shale_tenant=\u00e9", "shale_tenant=" + strings.Repeat("x", 4096),
	} {
		path := filepath.Join(t.TempDir(), "session")
		require.NoError(t, os.WriteFile(path, []byte(value), 0600))
		_, err := readSession(path)
		require.Error(t, err)
		// Validation errors never contain cookie material.
		require.NotContains(t, err.Error(), "sealed-session")
	}
	path := filepath.Join(t.TempDir(), "session")
	_, err := readSession(path)
	require.Error(t, err)
	require.NoError(t, os.WriteFile(path, []byte("shale_tenant=sealed-session"), 0644))
	_, err = readSession(path)
	require.Error(t, err)
	require.NoError(t, os.Chmod(path, 0600))
	link := filepath.Join(t.TempDir(), "session")
	require.NoError(t, os.Symlink(path, link))
	_, err = readSession(link)
	require.Error(t, err)
	_, err = readSession(t.TempDir())
	require.Error(t, err)
}

func TestCPSessionMetadataReloadsWithoutBearerFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session")
	require.NoError(t, os.WriteFile(path, []byte("shale_tenant=first-session\n"), 0600))
	credential := sessionCredential{Path: path, Secure: true}
	require.True(t, credential.RequireTransportSecurity())
	md, err := credential.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]string{"cookie": "shale_tenant=first-session"}, md)
	require.NoError(t, os.WriteFile(path, []byte("shale_tenant=renewed-session\n"), 0600))
	md, err = credential.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]string{"cookie": "shale_tenant=renewed-session"}, md)
	require.NoError(t, os.Chmod(path, 0644))
	md, err = credential.GetRequestMetadata(context.Background())
	require.Error(t, err)
	require.Nil(t, md)
	require.NoError(t, os.Remove(path))
	md, err = credential.GetRequestMetadata(context.Background())
	require.Error(t, err)
	require.Nil(t, md)
}

func TestCPAuthenticationIsExplicitAndUnambiguous(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session")
	require.NoError(t, os.WriteFile(path, []byte("shale_tenant=sealed-session\n"), 0600))
	base := ConnectionConfig{URL: "http://127.0.0.1:7500", AllowLoopbackHTTP: true}
	_, err := connectionGRPC(base)
	require.ErrorIs(t, err, camera.ErrAuthRequired)
	for _, auth := range []ConnectionConfig{
		{SessionFile: path, TokenFile: "token"},
		{SessionFile: path, CertFile: "cert", KeyFile: "key"},
		{TokenFile: "token", CertFile: "cert", KeyFile: "key"},
		{SessionFile: path, TokenFile: "token", CertFile: "cert", KeyFile: "key"},
	} {
		auth.URL, auth.AllowLoopbackHTTP = base.URL, base.AllowLoopbackHTTP
		_, err = connectionGRPC(auth)
		require.ErrorIs(t, err, camera.ErrInvalid)
	}
	base.SessionFile = path
	conn, err := connectionGRPC(base)
	require.NoError(t, err)
	conn.Close()
	base.AllowLoopbackHTTP = false
	_, err = connectionGRPC(base)
	require.ErrorIs(t, err, camera.ErrForbidden)
	base.URL, base.AllowLoopbackHTTP = "http://192.0.2.1:7500", true
	_, err = connectionGRPC(base)
	require.ErrorIs(t, err, camera.ErrForbidden)
	base.URL = "https://shale.example:7500"
	conn, err = connectionGRPC(base)
	require.NoError(t, err)
	conn.Close()
	base.SessionFile = filepath.Join(t.TempDir(), "missing")
	_, err = connectionGRPC(base)
	require.Error(t, err)
	// The worker API speaks bearer tokens. A CP session must never be silently
	// ignored there even when a valid worker token is also configured.
	base.SessionFile, base.TokenFile = path, "token"
	_, _, err = connectionHTTP(base)
	require.ErrorIs(t, err, camera.ErrInvalid)
	base.TokenFile = ""
	_, _, err = connectionHTTP(base)
	require.ErrorIs(t, err, camera.ErrInvalid)
}
