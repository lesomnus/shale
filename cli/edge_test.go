package cli

import (
	"context"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func edgeTestConfig(t *testing.T) *cmd.Config {
	t.Helper()
	return &cmd.Config{Cp: "https://cp.invalid", State: t.TempDir(), Tenant: "acme"}
}

func TestEdgeDefaultsToSeparateManagedRolesWithoutLocalFallback(t *testing.T) {
	c := edgeTestConfig(t)
	c.Producer.Catalog.Assignment = pdid.New(28).String()
	c.Producer.Catalog.PollInterval = 2 * time.Second
	cfg, err := EdgeConfig(c)
	require.NoError(t, err)
	require.NotNil(t, cfg.Producer)
	require.NotNil(t, cfg.Worker)
	require.NotEqual(t, cfg.Producer.StateDir, cfg.Worker.StateDir)
	require.Empty(t, cfg.Producer.Sources)
	require.Nil(t, cfg.Producer.AuthProvider)
	require.Nil(t, cfg.Producer.AuthProviderFactory, "edge root attaches its CP provider/cache after the host key is ready")
	require.NotNil(t, cfg.Producer.SourceProviderFactory)
	require.Nil(t, cfg.Cache)
	require.Empty(t, c.Producer.Catalog.Provider, "mapping must not rewrite a shared config")
	require.Empty(t, c.Producer.Credentials.Provider)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = cfg.Producer.SourceProviderFactory(ctx, producer.SourceSession{})
	require.ErrorIs(t, err, context.Canceled, "the CP selector remains deferred until machine authentication")
}

func TestEdgeRoleSelectionIsStrictAndEmptyIsNotDefault(t *testing.T) {
	for _, roles := range [][]string{{}, {"worker", "worker"}, {"producer", "producer"}, {"Worker"}, {"worker "}, {"fixture-secret-unknown"}} {
		c := edgeTestConfig(t)
		c.Edge.Roles = roles
		_, err := EdgeConfig(c)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "fixture-secret")
	}
	for _, text := range []struct {
		value string
		valid bool
	}{{"edge: {}", true}, {"edge: {roles: []}", false}} {
		c := edgeTestConfig(t)
		require.NoError(t, yaml.Unmarshal([]byte(text.value), c))
		_, err := EdgeConfig(c)
		require.Equal(t, text.valid, err == nil)
	}
	c := edgeTestConfig(t)
	c.Edge.Roles = []string{"worker"}
	c.Producer.Sources = []cmd.SourceConfig{{Input: "rtsp://user:fixture-secret@camera.invalid/live"}}
	c.Producer.Credentials.Directory = "fixture-secret"
	cfg, err := EdgeConfig(c)
	require.NoError(t, err, "an inactive Producer section can remain in a shared role config")
	require.Nil(t, cfg.Producer)
	require.NotNil(t, cfg.Worker)
	c.Edge.StreamingCache = &cmd.EdgeStreamingCacheConfig{MaxAge: time.Second}
	_, err = EdgeConfig(c)
	require.Error(t, err, "streaming cache requires the Producer role")
	c = edgeTestConfig(t)
	c.Edge.Roles = []string{"producer"}
	c.CameraWorker.CameraCAFile = "unread-inactive-worker"
	cfg, err = EdgeConfig(c)
	require.NoError(t, err)
	require.Nil(t, cfg.Worker)
}

func TestManagedEdgeProducerRejectsAmbiguousCompatibilitySettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*cmd.Config)
	}{
		{"local source", func(c *cmd.Config) {
			c.Producer.Sources = []cmd.SourceConfig{{Input: "rtsp://user:fixture-secret@camera.invalid/live"}}
		}},
		{"demo", func(c *cmd.Config) { c.Producer.Demo = 1 }},
		{"negative demo", func(c *cmd.Config) { c.Producer.Demo = -1 }},
		{"local set", func(c *cmd.Config) { c.Producer.Set = "fixture-secret" }},
		{"push", func(c *cmd.Config) { c.Producer.Push = "fixture-secret" }},
		{"push timing", func(c *cmd.Config) { c.Producer.PushIdle = time.Second }},
		{"static catalog", func(c *cmd.Config) { c.Producer.Catalog.Provider = "static" }},
		{"file catalog", func(c *cmd.Config) { c.Producer.Catalog.Provider = "file" }},
		{"custom catalog", func(c *cmd.Config) { c.Producer.Catalog.Provider = "fixture-secret" }},
		{"credential directory", func(c *cmd.Config) { c.Producer.Credentials.Directory = "fixture-secret" }},
		{"credential provider", func(c *cmd.Config) { c.Producer.Credentials.Provider = "fixture-secret" }},
		{"invalid buffer", func(c *cmd.Config) { c.Producer.Buffer = "fixture-secret" }},
		{"negative heartbeat", func(c *cmd.Config) { c.Producer.HeartbeatInterval = -time.Second }},
		{"invalid selector", func(c *cmd.Config) { c.Producer.Catalog.Assignment = "fixture-secret" }},
		{"wrong selector domain", func(c *cmd.Config) { c.Producer.Catalog.Assignment = pdid.New(8).String() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := edgeTestConfig(t)
			tc.set(c)
			_, err := EdgeConfig(c)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "fixture-secret")
		})
	}
}

func TestEdgeMapsOnlyCameraTransportAndTimeKnobs(t *testing.T) {
	c := edgeTestConfig(t)
	c.CameraWorker = cmd.CameraWorkerConfig{HeartbeatInterval: time.Second, RetryInterval: 2 * time.Second, LeaseSeconds: 30, DiscoveryInterval: time.Minute, DiscoveryTimeout: 3 * time.Second, Ffmpeg: "fixture-ffmpeg", Ffprobe: "fixture-ffprobe"}
	c.Edge.RetryInterval = 4 * time.Second
	c.Edge.StreamingCache = &cmd.EdgeStreamingCacheConfig{MaxAge: 300 * time.Second, MaxEntries: 64}
	cfg, err := EdgeConfig(c)
	require.NoError(t, err)
	require.Equal(t, c.CameraWorker.DiscoveryInterval, cfg.Worker.DiscoveryInterval)
	require.Equal(t, c.CameraWorker.DiscoveryTimeout, cfg.Worker.DiscoveryTimeout)
	require.Equal(t, "fixture-ffmpeg", cfg.Camera.FFmpeg)
	require.Equal(t, "fixture-ffprobe", cfg.Camera.FFprobe)
	require.Equal(t, 4*time.Second, cfg.RetryInterval)
	require.Equal(t, 300*time.Second, cfg.Cache.MaxAge)
	require.Equal(t, 64, cfg.Cache.MaxEntries)
	c.Dev, c.Cp = c.State, ""
	c.Client.Addr = "http://127.0.0.1:7400"
	cfg, err = EdgeConfig(c)
	require.NoError(t, err)
	require.Equal(t, c.Client.Addr, cfg.Worker.Cp)
	require.Equal(t, c.Client.Addr, cfg.Producer.Cp)
}

func TestEdgeCameraTrustDoesNotReplaceCPTrust(t *testing.T) {
	c := edgeTestConfig(t)
	ca, err := pki.NewCA("fixture camera CA", time.Now())
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "camera-ca.pem")
	require.NoError(t, os.WriteFile(path, ca.Bundle(), 0600))
	c.CameraWorker.CameraCAFile = path
	c.CaHash = "fixture-cp-pin"
	cfg, err := EdgeConfig(c)
	require.NoError(t, err)
	require.NotNil(t, cfg.Camera.Roots)
	_, err = ca.Cert.Verify(x509.VerifyOptions{Roots: cfg.Camera.Roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
	require.NoError(t, err, "the additional camera CA is actually trusted")
	require.Equal(t, "fixture-cp-pin", cfg.Worker.CaHash)
	require.Equal(t, "fixture-cp-pin", cfg.Producer.CaHash)
	c.CameraWorker.CameraCAFile = filepath.Join(t.TempDir(), "fixture-secret-missing")
	_, err = EdgeConfig(c)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "fixture-secret")
	require.NoError(t, os.WriteFile(path, []byte("not a certificate"), 0600))
	c.CameraWorker.CameraCAFile = path
	_, err = EdgeConfig(c)
	require.Error(t, err)
}

func TestServeEdgeAndCameraWorkerRejectBeforeNetwork(t *testing.T) {
	isolate(t)
	_, err := run(t, edgeTestConfig(t), "edge:\n  roles: []\n", "serve", "edge")
	require.Error(t, err)
	_, err = run(t, edgeTestConfig(t), "edge:\n  streaming_cache:\n    max_age: 1s\n", "serve", "camera-worker")
	require.Error(t, err, "the explicit Worker command cannot silently enable a Producer cache")
	for _, command := range []string{"edge", "camera-worker", "producer", "all"} {
		out, err := run(t, &cmd.Config{}, "", "serve", command, "--help")
		require.NoError(t, err)
		require.Contains(t, out, "--cp")
	}
}

// A managed edge's cameras come from the CP catalog, so producer.audio is
// how its host records no sound from them.
func TestEdgeProducerCarriesItsAudio(t *testing.T) {
	c := edgeTestConfig(t)
	c.Producer.Audio = &cmd.AudioConfig{Codec: "none"}
	cfg, err := EdgeConfig(c)
	require.NoError(t, err)
	require.Equal(t, &producer.AudioConfig{Codec: "none"}, cfg.Producer.Audio)
	_, err = producer.New(*cfg.Producer)
	require.NoError(t, err)
}
