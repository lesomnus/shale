package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lesomnus/shale/camera"
	cameradiscovery "github.com/lesomnus/shale/camera/discovery"
	"github.com/lesomnus/shale/cmd"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestManagedCameraDHCPHintsAreOptInAndInterfaceBound(t *testing.T) {
	c := edgeTestConfig(t)
	require.Nil(t, CameraWorkerConfig(c).InitialObservations)
	path := filepath.Join(t.TempDir(), "leases")
	require.NoError(t, os.WriteFile(path, []byte("0 02:00:00:00:00:01 192.0.2.42 * *\n"), 0600))
	require.NoError(t, yaml.Unmarshal([]byte("camera_worker:\n  dhcp_leases:\n    - interface: camera0\n      file: "+path+"\n"), c))
	cfg, err := EdgeConfig(c)
	require.NoError(t, err)
	require.NotNil(t, cfg.Worker.InitialObservations)
	scope := camera.DiscoveryScope{ID: "camera-network", Interface: "camera0", CIDRs: []string{"192.0.2.0/24"}, Seeds: []camera.Endpoint{{URL: "http://192.0.2.10:2020/onvif/device_service", NetworkScope: "camera-network"}}}
	rows, err := cfg.Worker.InitialObservations.ObserveInitial(t.Context(), scope)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "http://192.0.2.42:2020/onvif/device_service", rows[0].Device.Endpoints[0].URL)
	require.False(t, rows[0].Device.Evidence[0].Authenticated)
	scope.Interface = "another0"
	rows, err = cfg.Worker.InitialObservations.ObserveInitial(t.Context(), scope)
	require.NoError(t, err)
	require.Empty(t, rows)
	c.CameraWorker.InitialNeighbors = true
	initial, ok := CameraWorkerConfig(c).InitialObservations.(cameradiscovery.InitialProviders)
	require.True(t, ok)
	require.Len(t, initial, 2, "neighbor and DHCP are independent optional providers")
	// Mutating shared YAML afterward cannot retarget a previously built provider.
	c.CameraWorker.DHCPLeases[0] = cmd.CameraDHCPLeaseConfig{Interface: "another0", File: "not-read"}
	rows, err = cfg.Worker.InitialObservations.ObserveInitial(t.Context(), camera.DiscoveryScope{ID: "camera-network", Interface: "camera0", CIDRs: scope.CIDRs, Seeds: scope.Seeds})
	require.NoError(t, err)
	require.Len(t, rows, 1)
}
