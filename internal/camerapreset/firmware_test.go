package camerapreset

import (
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/preset"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestFirmwareRequirementSurvivesTheWire(t *testing.T) {
	config := presetContract()
	upgrade := &camera.FirmwareUpgrade{From: []string{"1.2.0 Build 251020 Rel.55235n"}, To: "1.2.2 Build 260624 Rel.39352n", Image: camera.FirmwareImage{URL: "https://static.example/c420.zip", SHA256: "9460f6c63c06573dbb302ec4b4ee482991caaff817718b919109821591ea669a"}}
	config.Rules[1].Firmware = upgrade
	portable, err := preset.New(config)
	require.NoError(t, err)
	rules, err := EncodeRules(config.Rules)
	require.NoError(t, err)
	encoded, err := proto.Marshal(api.CameraPreset_builder{Rules: rules}.Build())
	require.NoError(t, err)
	row := &api.CameraPreset{}
	require.NoError(t, proto.Unmarshal(encoded, row))
	central, err := Provider(config.ID, config.Revision, row.GetRules())
	require.NoError(t, err)
	require.Equal(t, portable.Reference(), central.Reference())

	scope := camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}
	resolved, err := central.Resolve(t.Context(), scope, camera.DeviceHint{Model: "C420"})
	require.NoError(t, err)
	require.Equal(t, upgrade, resolved.Firmware)
	snapshot, err := EncodeSnapshot(resolved)
	require.NoError(t, err)
	decoded, err := DecodeSnapshot(snapshot)
	require.NoError(t, err)
	require.Equal(t, resolved, decoded)

	// A rule that does not match leaves the snapshot without a requirement.
	other, err := central.Resolve(t.Context(), scope, camera.DeviceHint{})
	require.NoError(t, err)
	require.Nil(t, other.Firmware)
	snapshot, err = EncodeSnapshot(other)
	require.NoError(t, err)
	require.Nil(t, snapshot.GetFirmware())

	bad := proto.Clone(row.GetRules()[1]).(*api.CameraPresetRule)
	bad.GetFirmware().SetImageUrl("http://static.example/c420.zip")
	_, err = Provider(config.ID, config.Revision, []*api.CameraPresetRule{bad})
	require.ErrorIs(t, err, camera.ErrInvalid)
}
