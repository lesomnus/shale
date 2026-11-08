package cli

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/producer"
)

// producer.fragment_duration reaches every source, the demo's included.
func TestProducerConfigFragment(t *testing.T) {
	var c cmd.Config
	c.Producer.FragmentDuration = 100 * time.Millisecond
	c.Producer.Demo = 1
	c.Producer.Sources = []cmd.SourceConfig{{Alias: "door", Input: "v4l2:/dev/video0", MaxBitrate: "2Mbps"}}
	cfg, err := ProducerConfig(&c)
	require.NoError(t, err)
	require.Len(t, cfg.Sources, 2)
	for _, s := range cfg.Sources {
		require.Equal(t, 100*time.Millisecond, s.Fragment, s.Alias)
	}
}

func TestProducerConfigCatalogSelection(t *testing.T) {
	c := cmd.Config{}
	c.Producer.Catalog = cmd.SourceCatalogConfig{Provider: "cp", PollInterval: 2 * time.Second}
	cfg, err := ProducerConfig(&c)
	require.NoError(t, err)
	require.NotNil(t, cfg.SourceProviderFactory)
	require.Nil(t, cfg.SourceProvider)
	require.Empty(t, cfg.Sources, "CP sources are not synthesized before machine authentication")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = cfg.SourceProviderFactory(ctx, producer.SourceSession{})
	require.ErrorIs(t, err, context.Canceled)

	for _, tc := range []struct {
		name    string
		catalog cmd.SourceCatalogConfig
		sources []cmd.SourceConfig
		demo    int
	}{
		{name: "unknown", catalog: cmd.SourceCatalogConfig{Provider: "worker"}},
		{name: "negative interval", catalog: cmd.SourceCatalogConfig{Provider: "cp", PollInterval: -time.Second}},
		{name: "local assignment", catalog: cmd.SourceCatalogConfig{Provider: "file", Assignment: "assignment"}},
		{name: "local poll", catalog: cmd.SourceCatalogConfig{PollInterval: time.Second}},
		{name: "mixed sources", catalog: cmd.SourceCatalogConfig{Provider: "cp"}, sources: []cmd.SourceConfig{{Alias: "door", Input: "rtsp://camera/main"}}},
		{name: "mixed demo", catalog: cmd.SourceCatalogConfig{Provider: "cp"}, demo: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c.Producer.Catalog, c.Producer.Sources, c.Producer.Demo = tc.catalog, tc.sources, tc.demo
			_, err := ProducerConfig(&c)
			require.Error(t, err)
		})
	}
	for _, mode := range []string{"", "file", "static"} {
		c.Producer.Catalog, c.Producer.Sources, c.Producer.Demo = cmd.SourceCatalogConfig{Provider: mode}, []cmd.SourceConfig{{Alias: "usb", Input: "v4l2:/dev/video0"}}, 0
		cfg, err := ProducerConfig(&c)
		require.NoError(t, err)
		require.Nil(t, cfg.SourceProviderFactory)
		require.Len(t, cfg.Sources, 1)
	}
}

func TestProducerConfigCameraReferenceAndOnvif(t *testing.T) {
	scope := camera.EnrollmentScope{TenantID: "t", SetID: "s", ProducerID: "p", NetworkID: "lan"}
	stream := camera.CredentialRef{Provider: "local", ID: "stream", Version: "v1", Scope: scope, Purpose: camera.AuthStreaming, Recoverable: true}
	management := stream
	management.ID = "admin"
	management.Purpose = camera.AuthManagement
	c := cmd.Config{}
	c.Producer.Credentials = cmd.CredentialStoreConfig{Directory: t.TempDir(), Provider: "local"}
	c.Producer.Sources = []cmd.SourceConfig{{Alias: "camera", Input: "rtsp://camera/main", InputCredential: &stream, Scope: scope, Labels: map[string]string{"shale.provision.binding": "binding"}, MaxBitrate: "onvif", Onvif: &cmd.OnvifConfig{Endpoint: "http://camera/onvif/device_service", ProfileToken: "main", CredentialRef: &management}}}
	cfg, e := ProducerConfig(&c)
	require.NoError(t, e)
	require.Len(t, cfg.Sources, 1)
	got := cfg.Sources[0]
	require.True(t, got.MaxBitrateOnvif)
	require.Equal(t, stream, *got.InputCredential)
	require.Equal(t, scope, got.Scope)
	require.Equal(t, "binding", got.Labels["shale.provision.binding"])
	require.Equal(t, management, *got.Onvif.CredentialRef)
	provider, ok := cfg.AuthProvider.(credential.FileProvider)
	require.True(t, ok)
	require.Equal(t, "local", provider.Name)
}
