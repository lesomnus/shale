package producer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/camera/onvif"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/stretchr/testify/require"
)

func TestLegacyCaptureQueryTokenDiagnostics(t *testing.T) {
	ctx := context.Background()
	const raw = "%61bc%2fd%65f%2Btail"
	const decoded = "abc/def+tail"
	input := "rtsp://abc:fixture-password@camera.invalid/live?opaque_auth=" + raw
	_, configs, auth, err := legacyCatalog([]SourceConfig{{Alias: "door", Input: input, Encoder: "copy"}}, SourceSession{TenantID: "t", SetID: "s", ProducerID: "p"}, nil)
	require.NoError(t, err)
	src := configs["door"]
	require.NoError(t, credential.WithInput(ctx, auth, src.Scope, camera.DeviceHint{}, src.InputCredential, src.Input, func(actual string, _ credential.Sanitizer) error {
		require.True(t, actual == input, "private capture input must not be rewritten")
		return nil
	}))
	binary := filepath.Join(t.TempDir(), "capture-tool")
	require.NoError(t, os.WriteFile(binary, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\" >&2\nprintf '%s\\n' 'failure "+raw+"' 'failure "+decoded+"' >&2\nexit 1\n"), 0700))
	var logs bytes.Buffer
	c := &Capture{Source: src, AuthProvider: auth, Ffmpeg: binary, Log: slog.New(slog.NewTextHandler(&logs, nil)), Profile: func() (int64, time.Duration) { return 2_000_000, time.Second }}
	var observed []string
	c.OnLine = func(line string) bool { observed = append(observed, line); return false }
	err = c.once(ctx, func(r io.Reader) { _, _ = io.Copy(io.Discard, r) })
	require.Error(t, err)
	require.NotEmpty(t, logs.String())
	require.NotEmpty(t, observed)
	diagnostics := logs.String() + c.LastError() + err.Error() + fmt.Sprint(observed)
	for _, secret := range []string{raw, decoded, "abc%2fdef%2Btail", "/def+tail", "fixture-password"} {
		require.NotContains(t, diagnostics, secret)
	}
}

func TestLegacyCredentialOctetsKeepPrivateTransportWithoutDiagnosticPanic(t *testing.T) {
	ctx := context.Background()
	input := "rtsp://camera.invalid/live?auth=%ff"
	_, configs, auth, err := legacyCatalog([]SourceConfig{{Alias: "door", Input: input}}, SourceSession{TenantID: "t", SetID: "s", ProducerID: "p"}, nil)
	require.NoError(t, err)
	src := configs["door"]
	sentinel := errors.New("fixture failure")
	err = credential.WithInput(ctx, auth, src.Scope, camera.DeviceHint{}, src.InputCredential, src.Input, func(actual string, _ credential.Sanitizer) error {
		require.True(t, actual == input, "valid URL octets must remain unchanged in the private callback")
		return fmt.Errorf("failed %%ff %s: %w", string([]byte{0xff}), sentinel)
	})
	require.ErrorIs(t, err, sentinel)
	require.Equal(t, "[redacted]", err.Error())
}

type legacyInspect func(context.Context, camera.DeviceHint, camera.CredentialLease) (camera.Inspection, error)

func (f legacyInspect) Inspect(ctx context.Context, d camera.DeviceHint, l camera.CredentialLease) (camera.Inspection, error) {
	return f(ctx, d, l)
}

func TestLegacyOnvifReadAuthenticationSurvivesNormalization(t *testing.T) {
	ctx := context.Background()
	const username = "reader"
	const password = "fixture +:/? password"
	profile := camera.MediaProfile{Token: "main", Name: "Main", Codec: "h264", Width: 1280, Height: 720, FPS: 29.97, Bitrate: 2_000_000, StreamURI: "rtsp://127.0.0.1/main"}
	fixture := cameratest.ServeONVIF(t, profile)
	fixture.RequireCredentials(username, password)
	input := "rtsp://" + url.UserPassword(username, password).String() + "@127.0.0.1/main"
	p, _ := catalogRuntime(t, Config{Sources: []SourceConfig{{Alias: "door", Input: input, MaxBitrateOnvif: true, Onvif: &OnvifConfig{Endpoint: fixture.URL, ProfileToken: "main"}}}})
	require.NoError(t, p.loadSourceCatalog(ctx))
	src := p.order[0]
	require.NotContains(t, src.cfg.Input, "@")
	b, err := json.Marshal(p.initialCatalog)
	require.NoError(t, err)
	require.NotContains(t, string(b), password)
	require.NotContains(t, string(b), fixture.URL, "management settings remain in the private sidecar")
	var leases []camera.CredentialLease
	p.cfg.OnvifInspector = legacyInspect(func(ctx context.Context, device camera.DeviceHint, lease camera.CredentialLease) (camera.Inspection, error) {
		require.NotNil(t, lease, "stripping the public URI must not remove explicit local ONVIF authentication")
		require.True(t, lease.Ref().Authorizes(src.cfg.Scope, camera.AuthManagement))
		require.False(t, lease.Ref().Recoverable)
		require.NoError(t, lease.Use(ctx, func(material camera.CredentialMaterial) error {
			require.True(t, material.Username == username && material.Password == password, "the exact original credential must reach the authenticated inspector")
			return nil
		}))
		leases = append(leases, lease)
		return (&onvif.Client{}).Inspect(ctx, device, lease)
	})
	for _, bitrate := range []int64{2_000_000, 4_000_000} {
		profile.Bitrate = bitrate
		fixture.SetVideo(profile)
		require.NoError(t, p.refreshCeiling(ctx, src))
		require.Equal(t, bitrate+src.cfg.audioShare(), p.startingCeiling(src), "each negotiation reads the camera again with a fresh private lease")
	}
	require.Len(t, leases, 2)
	configurationWrites, activationWrites := fixture.Writes()
	require.Zero(t, configurationWrites)
	require.Zero(t, activationWrites, "legacy management compatibility only inspects the device")
	for _, lease := range leases {
		require.ErrorIs(t, lease.Use(ctx, func(camera.CredentialMaterial) error { t.Fatal("released management lease reused"); return nil }), camera.ErrExpired)
	}
	ref := *src.cfg.InputCredential
	ref.Purpose = camera.AuthManagement
	_, err = camera.Acquire(ctx, p.cfg.AuthProvider, camera.AuthRequest{Scope: src.cfg.Scope, Purpose: camera.AuthManagement, ExistingRef: &ref, Requirement: camera.AuthRequirement{Access: camera.AccessRead}})
	require.Error(t, err, "the private compatibility path must not grant generic management-reference access")
	private := p.cfg.AuthProvider.(*legacyInputProvider)
	wrong := src.cfg.Scope
	wrong.NetworkID = "other"
	_, _, err = private.legacyManagement(ctx, wrong, *src.cfg.InputCredential, src.cfg.Input, fixture.URL)
	require.ErrorIs(t, err, camera.ErrForbidden)
	_, _, err = private.legacyManagement(ctx, src.cfg.Scope, *src.cfg.InputCredential, src.cfg.Input, fixture.URL+"/other")
	require.ErrorIs(t, err, camera.ErrForbidden)
	p.cfg.OnvifInspector = legacyInspect(func(context.Context, camera.DeviceHint, camera.CredentialLease) (camera.Inspection, error) {
		return camera.Inspection{}, fmt.Errorf("failed credential %s", password)
	})
	_, err = p.onvifProfile(ctx, src.cfg)
	require.Error(t, err)
	require.NotContains(t, err.Error(), password)
}

func TestLegacyOnvifCapabilityRequiresOriginalLocalIntent(t *testing.T) {
	ctx := context.Background()
	_, configs, auth, err := legacyCatalog([]SourceConfig{
		{Alias: "inferred", Input: "rtsp://reader:fixture@192.0.2.10/main", MaxBitrateOnvif: true},
		{Alias: "stream-only", Input: "rtsp://reader:fixture@192.0.2.11/main"},
	}, SourceSession{TenantID: "t", SetID: "s", ProducerID: "p"}, nil)
	require.NoError(t, err)
	calls := 0
	p := &Producer{cfg: Config{AuthProvider: auth, OnvifInspector: legacyInspect(func(ctx context.Context, d camera.DeviceHint, l camera.CredentialLease) (camera.Inspection, error) {
		calls++
		require.Equal(t, "http://192.0.2.10:80/onvif/device_service", d.Endpoints[0].URL)
		require.NotNil(t, l)
		require.Equal(t, camera.AuthManagement, l.Ref().Purpose)
		return camera.Inspection{Current: camera.CurrentState{Profiles: []camera.MediaProfile{{StreamURI: "rtsp://192.0.2.10/main", Bitrate: 2_000_000}}}}, nil
	})}}
	_, err = p.onvifProfile(ctx, configs["inferred"])
	require.NoError(t, err, "max_bitrate:onvif preserves the original default endpoint inference")
	_, err = p.onvifProfile(ctx, configs["stream-only"])
	require.ErrorIs(t, err, camera.ErrForbidden, "a stream-only legacy credential cannot acquire new management intent")
	require.Equal(t, 1, calls)
	other := configs["inferred"]
	ref := *other.InputCredential
	ref.ID = "unrelated-explicit-reference"
	other.InputCredential = &ref
	p.cfg.OnvifInspector = legacyInspect(func(_ context.Context, _ camera.DeviceHint, lease camera.CredentialLease) (camera.Inspection, error) {
		require.Nil(t, lease, "an unrelated reference must reach an unauthenticated inspector without legacy material")
		return camera.Inspection{}, camera.ErrAuthRequired
	})
	_, err = p.onvifProfile(ctx, other)
	require.ErrorIs(t, err, camera.ErrAuthRequired, "an unrelated reference cannot borrow the private legacy capability")
}

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
