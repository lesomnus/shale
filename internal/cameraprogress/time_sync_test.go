package cameraprogress

import (
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
)

func timeSyncCheckpoint() provision.Run {
	r := checkpoint()
	sync := camera.TimeSync{NTP: camera.TimeSyncEdge, TimeZone: "KST-9"}
	r.Effective.Initialization.TimeSync = sync
	r.Preset.Requested.Initialization.TimeSync = sync
	r.TimeSync = &provision.TimeSyncState{Settled: true, Plan: &camera.InitializationPlan{After: camera.DeviceSettings{TimeSync: sync}, Changes: []string{camera.TimeSyncChangeNTP, camera.TimeSyncChangeTimeZone}, Revision: "tplink/c420/onvif-time-sync-v1", TimeSync: &camera.InitializationTimeSync{Server: "192.0.2.254", BeforeNTP: []string{"pool.ntp.example"}, BeforeMode: "NTP", BeforeTZ: "GMT0GMT"}}}
	r.Warnings = append(r.Warnings, camera.PresetWarning{Code: "time_sync_unconverged", Field: camera.PresetFieldTimeSync, Requested: sync.String(), Applied: "ntp=192.0.2.254 timezone=KST-9 offset=-12700800000ms", Reason: "fixture"})
	return r
}

func TestTimeSyncCheckpointTypedRoundtrip(t *testing.T) {
	r := timeSyncCheckpoint()
	wire, err := Encode(r)
	require.NoError(t, err)
	back, err := Decode(wire)
	require.NoError(t, err)
	require.Equal(t, r.TimeSync, back.TimeSync)
	require.Equal(t, r.Effective, back.Effective)
	require.Equal(t, r.Preset, back.Preset)
	require.Equal(t, "edge", wire.GetEffective().GetValue().GetTimeSync().GetNtp())
	require.Equal(t, "192.0.2.254", wire.GetTimeSync().GetPlan().GetTimeSync().GetServer())

	without := checkpoint()
	wire, err = Encode(without)
	require.NoError(t, err)
	require.False(t, wire.GetEffective().GetValue().HasTimeSync(), "no time sync is absent on the wire, as before the field")
	require.False(t, wire.GetInitializationPlan().GetAfter().HasTimeSync())
	require.False(t, wire.HasTimeSync())
}

func TestTimeSyncCheckpointRejectsHostnamesAndLooseState(t *testing.T) {
	for name, edit := range map[string]func(*api.CameraProvisionState){
		"a hostname server": func(s *api.CameraProvisionState) {
			s.GetTimeSync().GetPlan().GetTimeSync().SetServer("ntp.example.com")
		},
		"a hostname setting": func(s *api.CameraProvisionState) { s.GetEffective().GetValue().GetTimeSync().SetNtp("ntp.example.com") },
		"a bad zone":         func(s *api.CameraProvisionState) { s.GetEffective().GetValue().GetTimeSync().SetTimezone("Asia/Seoul") },
		"an empty time sync": func(s *api.CameraProvisionState) {
			s.GetTimeSync().GetPlan().GetBefore().SetTimeSync(&api.CameraPresetTimeSync{})
		},
		"a plan with no host": func(s *api.CameraProvisionState) { s.GetTimeSync().GetPlan().ClearTimeSync() },
		"a done plan undone":  func(s *api.CameraProvisionState) { s.GetTimeSync().GetPlan().ClearChanges() },
	} {
		wire, err := Encode(timeSyncCheckpoint())
		require.NoError(t, err)
		edit(wire)
		_, err = Decode(wire)
		require.Error(t, err, name)
	}
	r := timeSyncCheckpoint()
	r.TimeSync.Plan.TimeSync.Server = "2001:db8::1"
	_, err := Encode(r)
	require.Error(t, err)
}
