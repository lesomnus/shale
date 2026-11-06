package cli

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
	"github.com/lesomnus/shale/cmd"
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
