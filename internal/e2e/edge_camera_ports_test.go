package e2e_test

import (
	"encoding/json"
	"testing"

	camapi "github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
)

func TestEdgeCameraProtocolAcceptsStableServicePortsAndAddressMoves(t *testing.T) {
	active := false
	start := edgeCameraRequest{Version: 1, ID: 1, Op: "start", IP: "192.168.250.20", Interface: "eth0", HTTPPort: 8080, RTSPPort: 8554, Username: "admin", Password: "synthetic-camera-account", Active: &active, Profile: camapi.MediaProfile{Token: "main", Codec: "h264", Width: 160, Height: 90, FPS: 10, Bitrate: 128000, GOP: 10}}
	encoded, err := json.Marshal(start)
	require.NoError(t, err)
	decoded, valid := edgeCameraRequestFrom(encoded)
	require.True(t, valid, "fixed ports must pass both protocol layers")
	require.True(t, edgeCameraStartValid(decoded))
	for _, port := range []int{-1, 80, 65536} {
		invalid := decoded
		invalid.HTTPPort = port
		require.False(t, edgeCameraStartValid(invalid))
	}
	move := map[string]any{"version": 1, "id": 2, "op": "rebind", "ip": "192.168.250.30", "interface": "eth0", "preserve_ports": true}
	encoded, err = json.Marshal(move)
	require.NoError(t, err)
	decoded, valid = edgeCameraRequestFrom(encoded)
	require.True(t, valid)
	require.True(t, decoded.PreservePorts)
	for _, raw := range []string{`{"version":1,"id":3,"op":"status","http_port":8080}`, `{"version":1,"id":3,"op":"status","preserve_ports":true}`, `{"version":1,"id":3,"op":"rebind","http_port":8080}`} {
		_, valid = edgeCameraRequestFrom([]byte(raw))
		require.False(t, valid, "port options must stay scoped to their operation")
	}
}
