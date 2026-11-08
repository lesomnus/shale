package camerapreset

import (
	"context"
	"math"
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/preset"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func presetContract() preset.Config {
	scope := camera.EnrollmentScope{TenantID: "tenant"}
	return preset.Config{ID: "immutable-rules", Revision: "7", Rules: []preset.Rule{
		{ID: "defaults", Scope: scope,
			Settings:  camera.PresetSettings{Initialization: camera.DeviceSettings{PowerLineFrequency: 60}, Video: camera.DesiredState{Codec: "h264", Width: 1920, Height: 1080, FPS: 60000.0 / 1001, Bitrate: 8_000_000, GOP: 60, Revision: "metadata", AllowFallback: true}},
			Fallbacks: []camera.PresetSettings{{Video: camera.DesiredState{Width: 1280, Height: 720, FPS: 30}}}, Required: []string{camera.PresetFieldCodec, camera.PresetFieldFrequency}},
		{ID: "model", Scope: scope, Match: preset.Match{Model: "C420"}, Settings: camera.PresetSettings{Video: camera.DesiredState{FPS: 30}}},
		{ID: "serial", Scope: scope, Match: preset.Match{Model: "C420", Serial: "unit-a"}, Fallbacks: []camera.PresetSettings{}},
		{ID: "inventory", Scope: scope, Match: preset.Match{DeviceID: "claimed-device"}, Settings: camera.PresetSettings{Video: camera.DesiredState{FPS: 30000.0 / 1001, GOP: 30}, Preserve: []string{camera.PresetFieldBitrate}}},
	}}
}

// Test behavior after an actual protobuf round trip, including presence-bearing
// empty fallback overrides. Comparing only getters would miss inherited policy.
func TestWireRulesKeepPortableResolutionContract(t *testing.T) {
	config := presetContract()
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
	scope := camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}
	for _, test := range []struct {
		name         string
		hint         camera.DeviceHint
		alternatives int
	}{
		{"default", camera.DeviceHint{}, 1},
		{"model-inherits", camera.DeviceHint{Model: "C420"}, 1},
		{"serial-clears", camera.DeviceHint{Model: "C420", Serial: "unit-a"}, 0},
		{"inventory-most-specific", camera.DeviceHint{ID: "claimed-device", Model: "C420", Serial: "unit-a"}, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			want, err := portable.Resolve(context.Background(), scope, test.hint)
			require.NoError(t, err)
			got, err := central.Resolve(context.Background(), scope, test.hint)
			require.NoError(t, err)
			require.Equal(t, want, got, "wire conversion must retain the canonical resolved revision")
			require.Len(t, got.Fallbacks, test.alternatives)
			wire, err := EncodeSnapshot(got)
			require.NoError(t, err)
			resumed, err := DecodeSnapshot(wire)
			require.NoError(t, err)
			wantCandidates, err := got.Candidates()
			require.NoError(t, err)
			resumedCandidates, err := resumed.Candidates()
			require.NoError(t, err)
			require.Equal(t, wantCandidates, resumedCandidates)
			if test.name == "inventory-most-specific" {
				require.Equal(t, 30000.0/1001, got.Requested.Video.FPS)
				require.Zero(t, got.Requested.Video.Bitrate)
			}
		})
	}
}

func TestWireRulesKeepConflictAndRequiredFieldRejection(t *testing.T) {
	config := presetContract()
	// Both rules have the same specificity; ID/file order cannot select one.
	duplicate := config.Rules[1]
	duplicate.ID = "same-rank"
	duplicate.Settings.Video.FPS = 15
	config.Rules = append(config.Rules, duplicate)
	rules, err := EncodeRules(config.Rules)
	require.NoError(t, err)
	provider, err := Provider(config.ID, config.Revision, rules)
	require.NoError(t, err)
	_, err = provider.Resolve(context.Background(), camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}, camera.DeviceHint{Model: "C420"})
	require.ErrorIs(t, err, camera.ErrConflict)
	config = presetContract()
	config.Rules[0].Fallbacks[0].Initialization.PowerLineFrequency = 50
	rules, err = EncodeRules(config.Rules)
	require.NoError(t, err, "required fields are checked on the composed resolved snapshot")
	provider, err = Provider(config.ID, config.Revision, rules)
	require.NoError(t, err)
	_, err = provider.Resolve(context.Background(), camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}, camera.DeviceHint{})
	require.ErrorIs(t, err, camera.ErrInvalid)
}

func TestWireSnapshotsOwnTheirSlices(t *testing.T) {
	config := presetContract()
	provider, err := preset.New(config)
	require.NoError(t, err)
	want, err := provider.Resolve(context.Background(), camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}, camera.DeviceHint{})
	require.NoError(t, err)
	wire, err := EncodeSnapshot(want)
	require.NoError(t, err)
	got, err := DecodeSnapshot(wire)
	require.NoError(t, err)
	wire.GetMatchedRules()[0] = "mutated"
	wire.GetRequired()[0] = camera.PresetFieldGOP
	wire.GetAlternatives()[0].SetPreserve([]string{camera.PresetFieldFrequency})
	require.Equal(t, want, got)
	got.Required[0] = "caller-change"
	require.NotEqual(t, want.Required, got.Required)
}

func TestWireRejectsInvalidOrUnrepresentableSettings(t *testing.T) {
	_, err := DecodeSnapshot(nil)
	require.ErrorIs(t, err, camera.ErrInvalid)
	_, err = Provider("id", "rev", []*api.CameraPresetRule{nil})
	require.ErrorIs(t, err, camera.ErrInvalid)
	_, err = Provider("id", "rev", []*api.CameraPresetRule{api.CameraPresetRule_builder{Alternatives: api.CameraPresetAlternatives_builder{Candidates: []*api.CameraPresetSettings{nil}}.Build()}.Build()})
	require.ErrorIs(t, err, camera.ErrInvalid)
	config := presetContract()
	config.Rules[0].Settings.Video.Width = math.MaxInt32 + 1
	_, err = EncodeRules(config.Rules)
	require.ErrorIs(t, err, camera.ErrInvalid, "never silently narrow large portable dimensions")
	config = presetContract()
	config.Rules[0].Settings.Video.FPS = math.NaN()
	_, err = EncodeRules(config.Rules)
	require.ErrorIs(t, err, camera.ErrInvalid)
}
