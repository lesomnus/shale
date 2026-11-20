package provision_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
)

// timeInitializer is a C420 adapter's time-sync contract over a camera 147
// days behind: it plans what differs and writes NTP, then the zone.
type timeInitializer struct {
	server, mode, tz    string
	ntp                 []string
	offset              time.Duration
	reject, rejectZone  bool
	refuse, stuck       bool
	writes              []string
	plans, prerequisite int
}

func (d *timeInitializer) PlanInitialization(_ context.Context, _ camera.DeviceHint, _ camera.CredentialLease, w camera.DeviceSettings) (camera.InitializationPlan, error) {
	if w.TimeSync.IsZero() {
		d.prerequisite++
		return camera.InitializationPlan{Before: w, After: w}, nil
	}
	d.plans++
	switch {
	case d.refuse:
		return camera.InitializationPlan{}, camera.ErrTimeServerRefused
	case d.reject:
		return camera.InitializationPlan{}, camera.ErrUnsupported
	}
	p := camera.InitializationPlan{After: w, Revision: "fixture-time", TimeSync: &camera.InitializationTimeSync{Server: d.server, BeforeNTP: slices.Clone(d.ntp), BeforeMode: d.mode, BeforeTZ: d.tz}}
	if !slices.Equal(d.ntp, []string{d.server}) {
		p.Changes = append(p.Changes, camera.TimeSyncChangeNTP)
	}
	if d.mode != camera.ClockModeNTP || d.tz != w.TimeSync.TimeZone {
		p.Changes = append(p.Changes, camera.TimeSyncChangeTimeZone)
	}
	if len(p.Changes) == 0 {
		p.Before = p.After
	}
	return p, nil
}

func (d *timeInitializer) ApplyInitialization(_ context.Context, _ camera.DeviceHint, _ camera.CredentialLease, p camera.InitializationPlan) (camera.DeviceSettings, error) {
	if !slices.Equal(d.ntp, []string{p.TimeSync.Server}) {
		d.writes = append(d.writes, "SetNTP")
		d.ntp = []string{p.TimeSync.Server}
	}
	if d.mode != camera.ClockModeNTP || d.tz != p.After.TimeSync.TimeZone {
		d.writes = append(d.writes, "SetSystemDateAndTime")
		if d.rejectZone {
			return camera.DeviceSettings{}, camera.ErrUnsupported
		}
		d.mode, d.tz = camera.ClockModeNTP, p.After.TimeSync.TimeZone
	}
	return p.After, nil
}

func (d *timeInitializer) ObserveClock(context.Context, camera.DeviceHint) (camera.DeviceClock, error) {
	now := time.Now()
	if !d.stuck && slices.Equal(d.ntp, []string{d.server}) && d.mode == camera.ClockModeNTP {
		d.offset = 0
	}
	return camera.DeviceClock{UTC: now.Add(d.offset).Truncate(time.Second), ObservedAt: now, RTT: time.Millisecond}, nil
}

func timeSyncEngine(t *testing.T, sync camera.TimeSync) (provision.Request, *timeInitializer, *publisher, *provision.Engine) {
	t.Helper()
	q, d, p, e, _ := fixture(t)
	d.ready = true
	video := q.Desired
	q.Desired, q.UsePreset = camera.DesiredState{}, true
	ti := &timeInitializer{server: "192.0.2.1", ntp: []string{"pool.ntp.example"}, mode: camera.ClockModeNTP, tz: "GMT0GMT", offset: -147 * 24 * time.Hour}
	e.Initializer, e.Clock = ti, ti
	e.TimeSyncWait, e.TimeSyncPoll = 50*time.Millisecond, 5*time.Millisecond
	e.Presets = presetProvider(func(context.Context, camera.EnrollmentScope, camera.DeviceHint) (camera.DevicePreset, error) {
		return camera.DevicePreset{Ref: camera.PresetRef{ID: "time", Revision: "1"}, Requested: camera.PresetSettings{Initialization: camera.DeviceSettings{TimeSync: sync}, Video: video}}, nil
	})
	return q, ti, p, e
}

var edgeKST = camera.TimeSync{NTP: camera.TimeSyncEdge, TimeZone: "KST-9"}

func eventIndex(r provision.Run, code string) int {
	return slices.IndexFunc(r.Events, func(e provision.Event) bool { return e.Code == code })
}

func TestTimeSyncIsJournaledThenAppliedAfterVideoAndConverges(t *testing.T) {
	q, ti, p, e := timeSyncEngine(t, edgeKST)
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, []string{"SetNTP", "SetSystemDateAndTime"}, ti.writes)
	require.Zero(t, ti.prerequisite, "a time sync alone has no media prerequisite")
	require.Empty(t, r.Warnings)
	require.NotNil(t, r.TimeSync)
	require.True(t, r.TimeSync.Settled)
	require.Equal(t, "192.0.2.1", r.TimeSync.Plan.TimeSync.Server, "the journal names the server written")
	require.Equal(t, []string{"pool.ntp.example"}, r.TimeSync.Plan.TimeSync.BeforeNTP)
	require.Nil(t, r.InitializationPlan)
	require.Nil(t, r.Initialized)
	planned, verified := eventIndex(r, "time_sync_planned"), eventIndex(r, "time_sync_verified")
	require.Positive(t, planned, "the plan is durable before the first write")
	require.Less(t, eventIndex(r, "plan_ready"), planned, "video settings first")
	require.Less(t, planned, verified)
	require.Less(t, verified, eventIndex(r, "media_probe"))
	require.Equal(t, 1, p.deploys)
}

func TestTimeSyncAlreadyRightWritesNothing(t *testing.T) {
	q, ti, _, e := timeSyncEngine(t, edgeKST)
	ti.ntp, ti.tz, ti.offset = []string{"192.0.2.1"}, "KST-9", 0
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Empty(t, ti.writes)
	require.Equal(t, -1, eventIndex(r, "time_sync_planned"))
	require.True(t, r.TimeSync.Settled)
	require.Empty(t, r.Warnings)
}

func TestTimeSyncThatDoesNotConvergeIsAWarningNotAFailure(t *testing.T) {
	q, ti, p, e := timeSyncEngine(t, edgeKST)
	ti.stuck = true
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Len(t, r.Warnings, 1)
	w := r.Warnings[0]
	require.Equal(t, provision.WarningTimeSyncUnconverged, w.Code)
	require.Equal(t, camera.PresetFieldTimeSync, w.Field)
	require.Equal(t, "ntp=edge timezone=KST-9", w.Requested)
	require.Regexp(t, `^ntp=192\.0\.2\.1 timezone=KST-9 offset=-12700[78]\d{5}ms$`, w.Applied, "147 days behind")
	require.GreaterOrEqual(t, eventIndex(r, "time_sync_unconverged"), 0)
	require.Equal(t, 1, p.deploys)

	// Without a clock to read, settings that read back are unconverged too.
	q, _, _, e = timeSyncEngine(t, edgeKST)
	e.Clock = nil
	r, err = e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, provision.WarningTimeSyncUnconverged, r.Warnings[0].Code)
	require.Contains(t, r.Warnings[0].Applied, "offset=unobserved")
}

func TestTimeSyncTheDeviceCannotTakeIsAnUnsupportedWarning(t *testing.T) {
	q, ti, p, e := timeSyncEngine(t, edgeKST)
	ti.reject = true
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Empty(t, ti.writes)
	require.Equal(t, []camera.PresetWarning{{Code: provision.WarningUnsupported, Field: camera.PresetFieldTimeSync, Requested: "ntp=edge timezone=KST-9", Applied: "unchanged", Reason: "the device or its firmware is not qualified for time sync"}}, r.Warnings)
	require.True(t, r.TimeSync.Settled)
	require.Nil(t, r.TimeSync.Plan)
	require.Equal(t, 1, p.deploys)
}

func TestTimeSyncDeclinedHalfwaySaysWhatTheDeviceHasNow(t *testing.T) {
	q, ti, _, e := timeSyncEngine(t, edgeKST)
	ti.rejectZone = true
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, []string{"SetNTP", "SetSystemDateAndTime"}, ti.writes)
	require.Len(t, r.Warnings, 1)
	require.Equal(t, provision.WarningUnsupported, r.Warnings[0].Code)
	require.Equal(t, "ntp=192.0.2.1 mode=NTP tz=GMT0GMT", r.Warnings[0].Applied, "the server written and the zone kept are both visible")
}

func TestTimeSyncServerOutsideTheNetworkIsRefusedWithoutWrites(t *testing.T) {
	q, ti, _, e := timeSyncEngine(t, camera.TimeSync{NTP: "203.0.113.5", TimeZone: "UTC0"})
	ti.refuse = true
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Empty(t, ti.writes)
	require.Len(t, r.Warnings, 1)
	require.Equal(t, provision.WarningTimeSyncRefused, r.Warnings[0].Code)
	require.Equal(t, "unchanged", r.Warnings[0].Applied)
}

func TestTimeSyncIsSettledOnceAcrossARetry(t *testing.T) {
	q, ti, p, e := timeSyncEngine(t, edgeKST)
	p.failPublish = true
	_, err := e.Execute(t.Context(), q, true)
	require.Error(t, err)
	plans := ti.plans
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, plans, ti.plans, "a recorded outcome is not planned again")
	require.Equal(t, []string{"SetNTP", "SetSystemDateAndTime"}, ti.writes)
}

func TestPresetWithoutTimeSyncNeverAsksForIt(t *testing.T) {
	q, ti, _, e := timeSyncEngine(t, camera.TimeSync{})
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Zero(t, ti.plans)
	require.Empty(t, ti.writes)
	require.Nil(t, r.TimeSync)
	require.Empty(t, r.Warnings)
}
