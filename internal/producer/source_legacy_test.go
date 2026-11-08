package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/stretchr/testify/require"
)

func TestLegacyCatalogPrivateTransportAndSidecar(t *testing.T) {
	ctx := context.Background()
	const password = "fixture +:/? pass"
	const token = "fixture +:/? token"
	input := "rtsp://" + url.UserPassword("operator", password).String() + "@192.0.2.10/main?profile=main&opaque_auth=" + url.QueryEscape(token)
	cfg := SourceConfig{Alias: "door", Input: input, Command: "local capture command", Controls: map[string]string{"brightness": "5"}, ExtraOutputArgs: []string{"-custom", "local"}, Onvif: &OnvifConfig{Endpoint: "http://192.0.2.10/onvif"}}
	session := SourceSession{TenantID: "t", ProducerID: "p", SetID: "s"}
	provider, local, auth, err := legacyCatalog([]SourceConfig{cfg}, session, nil)
	require.NoError(t, err)
	v, err := provider.Snapshot(ctx)
	require.NoError(t, err)
	require.Empty(t, v.Sources[0].SourceID, "normal registration still owns legacy identity")
	require.Equal(t, "rtsp://192.0.2.10/main?profile=main", v.Sources[0].Input)
	b, err := json.Marshal(v)
	require.NoError(t, err)
	for _, secret := range []string{password, token, url.QueryEscape(token), "local capture command", "brightness"} {
		require.NotContains(t, string(b), secret)
	}
	got := local["door"]
	require.Equal(t, cfg.Command, got.Command)
	require.Equal(t, cfg.ExtraOutputArgs, got.ExtraOutputArgs)
	require.Equal(t, "local", got.Scope.NetworkID)
	require.False(t, got.InputCredential.Recoverable, "legacy URI credentials cannot authorize activation")
	for i := 0; i < 2; i++ {
		err = credential.WithInput(ctx, auth, got.Scope, camera.DeviceHint{}, got.InputCredential, got.Input, func(actual string, sanitize credential.Sanitizer) error {
			require.True(t, actual == input, "private transport must preserve the exact legacy URI")
			return fmt.Errorf("capture: %s %s %s %s", actual, password, token, url.QueryEscape(token))
		})
		require.Error(t, err)
		for _, secret := range []string{password, token, url.QueryEscape(token), url.PathEscape(password)} {
			require.NotContains(t, err.Error(), secret)
		}
	}
	wrong := got.Scope
	wrong.NetworkID = "other"
	require.ErrorIs(t, credential.WithInput(ctx, auth, wrong, camera.DeviceHint{}, got.InputCredential, got.Input, func(string, credential.Sanitizer) error { t.Fatal("wrong-scope callback"); return nil }), camera.ErrForbidden)
	require.ErrorIs(t, credential.WithInput(ctx, auth, got.Scope, camera.DeviceHint{}, got.InputCredential, "rtsp://192.0.2.11/main", func(string, credential.Sanitizer) error { t.Fatal("changed endpoint callback"); return nil }), camera.ErrForbidden)
	request := camera.AuthRequest{Scope: got.Scope, Purpose: camera.AuthStreaming, ExistingRef: got.InputCredential, Requirement: camera.AuthRequirement{Access: camera.AccessRead, Recoverable: true}}
	_, err = camera.Acquire(ctx, auth, request)
	require.ErrorIs(t, err, camera.ErrNeedsUserInput)
	request.Requirement.Recoverable = false
	lease, err := camera.Acquire(ctx, auth, request)
	require.NoError(t, err)
	require.NoError(t, lease.Close())
	err = lease.(credential.InputLease).WithInput(ctx, got.Input, func(string, credential.Sanitizer) error { t.Fatal("closed lease callback"); return nil })
	require.ErrorIs(t, err, camera.ErrExpired)
	request.Requirement.Access = camera.AccessConfigure
	_, err = camera.Acquire(ctx, auth, request)
	require.ErrorIs(t, err, camera.ErrForbidden)
	for _, object := range []any{auth, lease} {
		require.NotContains(t, fmt.Sprintf("%v %#v", object, object), password)
	}
	cfg.Controls["brightness"] = "9"
	cfg.ExtraOutputArgs[0] = "changed"
	cfg.Onvif.Endpoint = "changed"
	require.Equal(t, "5", got.Controls["brightness"])
	require.Equal(t, "-custom", got.ExtraOutputArgs[0])
	require.Equal(t, "http://192.0.2.10/onvif", got.Onvif.Endpoint)
}

func TestLegacyCatalogKeepsExactExistingReference(t *testing.T) {
	ctx := context.Background()
	scope := camera.EnrollmentScope{TenantID: "t", SetID: "s", ProducerID: "p", NetworkID: "lan"}
	fallback := credential.FileProvider{Dir: filepath.Join(t.TempDir(), "credentials"), Name: "local"}
	ref, err := fallback.Prepare(ctx, scope, camera.AuthStreaming, "existing", camera.CredentialMaterial{Username: "reader", Password: "fixture-secret"})
	require.NoError(t, err)
	configs := []SourceConfig{
		{Alias: "existing", Input: "rtsp://192.0.2.10/main", Scope: scope, InputCredential: &ref},
		{Alias: "private", Input: "rtsp://user:legacy-secret@192.0.2.11/main"},
		{Alias: "usb", Input: "v4l2:/dev/video0", Command: "local command", Size: "unknown"},
	}
	provider, local, auth, err := legacyCatalog(configs, SourceSession{TenantID: "t", ProducerID: "p", SetID: "s"}, fallback)
	require.NoError(t, err)
	got := local["existing"]
	require.Equal(t, ref, *got.InputCredential)
	require.Equal(t, scope, got.Scope)
	require.NoError(t, credential.WithInput(ctx, auth, got.Scope, camera.DeviceHint{}, got.InputCredential, got.Input, func(input string, _ credential.Sanitizer) error {
		u, e := url.Parse(input)
		require.NoError(t, e)
		require.Equal(t, "reader", u.User.Username())
		return nil
	}))
	v, err := provider.Snapshot(ctx)
	require.NoError(t, err)
	require.Zero(t, v.Sources[2].Profile.Width, "unknown legacy size is not fabricated")
	require.Empty(t, v.Sources[2].SourceID)
	configs[0].Input = "rtsp://user:fixture@192.0.2.10/main"
	_, _, _, err = legacyCatalog(configs, SourceSession{}, fallback)
	require.ErrorIs(t, err, camera.ErrInvalid, "combining two credential transports is ambiguous")
}

func TestLegacyCatalogMalformedCredentialInputErrorsStayPrivate(t *testing.T) {
	for _, input := range []string{"rtsp://camera/main?opaque_auth=%private-fixture", "rtsp://user:%private-fixture@camera/main"} {
		_, _, _, err := legacyCatalog([]SourceConfig{{Alias: "door", Input: input}}, SourceSession{}, nil)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private-fixture")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, auth, err := legacyCatalog([]SourceConfig{{Alias: "door", Input: "rtsp://user:secret@camera/main"}}, SourceSession{TenantID: "t"}, nil)
	require.NoError(t, err)
	_, err = auth.Acquire(ctx, camera.AuthRequest{})
	require.True(t, errors.Is(err, context.Canceled))
}
