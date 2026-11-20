package camerapreset

import (
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/preset"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTimeSyncTravelsInRulesSnapshotsAndDirectSettings(t *testing.T) {
	sync := camera.TimeSync{NTP: camera.TimeSyncEdge, TimeZone: "KST-9"}
	rules, err := EncodeRules([]preset.Rule{{ID: "c420", Scope: camera.EnrollmentScope{TenantID: "tenant"}, Settings: camera.PresetSettings{Initialization: camera.DeviceSettings{TimeSync: sync}}, Fallbacks: []camera.PresetSettings{{Preserve: []string{camera.PresetFieldTimeSync}}}}})
	require.NoError(t, err)
	require.Equal(t, "edge", rules[0].GetSettings().GetTimeSync().GetNtp())
	require.Equal(t, "KST-9", rules[0].GetSettings().GetTimeSync().GetTimezone())
	require.False(t, rules[0].GetAlternatives().GetCandidates()[0].HasTimeSync(), "no time sync is absent")
	p, err := Provider("id", "1", rules)
	require.NoError(t, err)
	resolved, err := p.Resolve(t.Context(), camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}, camera.DeviceHint{})
	require.NoError(t, err)
	require.Equal(t, sync, resolved.Requested.Initialization.TimeSync)
	snapshot, err := EncodeSnapshot(resolved)
	require.NoError(t, err)
	back, err := DecodeSnapshot(snapshot)
	require.NoError(t, err)
	again, err := EncodeSnapshot(back)
	require.NoError(t, err)
	require.True(t, proto.Equal(snapshot, again))

	direct, err := EncodeSettings(camera.PresetSettings{Initialization: camera.DeviceSettings{TimeSync: camera.TimeSync{NTP: "192.0.2.1", TimeZone: "UTC0"}}})
	require.NoError(t, err)
	settings, err := DecodeSettings(direct)
	require.NoError(t, err)
	require.Equal(t, "192.0.2.1", settings.Initialization.TimeSync.NTP)

	// An empty message says nothing, as an absent one does.
	empty := proto.Clone(rules[0]).(*api.CameraPresetRule)
	empty.GetSettings().SetTimeSync(&api.CameraPresetTimeSync{})
	absent := proto.Clone(rules[0]).(*api.CameraPresetRule)
	absent.GetSettings().ClearTimeSync()
	same, err := SameRules([]*api.CameraPresetRule{empty}, []*api.CameraPresetRule{absent})
	require.NoError(t, err)
	require.True(t, same)
	withEmpty, err := Provider("id", "1", []*api.CameraPresetRule{empty})
	require.NoError(t, err)
	resolved, err = withEmpty.Resolve(t.Context(), camera.EnrollmentScope{TenantID: "tenant", SetID: "set", ProducerID: "producer", NetworkID: "lan"}, camera.DeviceHint{})
	require.NoError(t, err)
	require.True(t, resolved.Requested.Initialization.TimeSync.IsZero())
}

func TestTimeSyncWireRefusesHostnamesAndPartialSettings(t *testing.T) {
	for _, sync := range []*api.CameraPresetTimeSync{
		api.CameraPresetTimeSync_builder{Ntp: "pool.ntp.example", Timezone: "KST-9"}.Build(),
		api.CameraPresetTimeSync_builder{Ntp: "2001:db8::1", Timezone: "KST-9"}.Build(),
		api.CameraPresetTimeSync_builder{Ntp: "edge"}.Build(),
		api.CameraPresetTimeSync_builder{Ntp: "edge", Timezone: "Asia/Seoul"}.Build(),
	} {
		rule := api.CameraPresetRule_builder{Id: "c420", Scope: api.CameraPresetScope_builder{TenantId: "tenant"}.Build(), Settings: api.CameraPresetSettings_builder{TimeSync: sync, Video: api.CameraPresetVideo_builder{}.Build()}.Build()}.Build()
		_, err := Provider("id", "1", []*api.CameraPresetRule{rule})
		require.Error(t, err, "%v", sync)
		_, err = DecodeSettings(rule.GetSettings())
		require.Error(t, err, "%v", sync)
	}
}
