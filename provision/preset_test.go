package provision_test

import (
	"context"
	"errors"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/shale/provision/filestore"
	"github.com/stretchr/testify/require"
)

type presetProvider func(context.Context, camera.EnrollmentScope, camera.DeviceHint) (camera.DevicePreset, error)

func (f presetProvider) Resolve(c context.Context, s camera.EnrollmentScope, h camera.DeviceHint) (camera.DevicePreset, error) {
	return f(c, s, h)
}

type presetDevice struct {
	*device
	frequency, initializationWrites int
	maxFPS                          float64
	planError, initializationError  error
	rejectMedia, changeOnRejection  bool
	videoPlans                      int
	frequencyAfterMedia             int
}

func (d *presetDevice) Plan(_ context.Context, i camera.Inspection, w camera.DesiredState) (camera.ChangePlan, error) {
	d.videoPlans++
	if d.planError != nil {
		return camera.ChangePlan{}, d.planError
	}
	max := d.maxFPS
	if max == 0 {
		max = 25
		if d.frequency == 60 {
			max = 30
		}
	}
	if w.FPS > max {
		return camera.ChangePlan{}, camera.ErrUnsupported
	}
	p := i.Current.Profiles[0]
	after := p
	if w.Codec != "" {
		after.Codec = w.Codec
	}
	if w.FPS != 0 {
		after.FPS = w.FPS
	}
	plan := camera.ChangePlan{Before: p, After: after, ProfileToken: p.Token, Revision: w.Revision}
	if p.Codec != after.Codec || p.FPS != after.FPS {
		plan.Changes = []string{"media"}
	}
	return plan, nil
}

func (d *presetDevice) Apply(ctx context.Context, h camera.DeviceHint, l camera.CredentialLease, p camera.ChangePlan) (camera.CurrentState, error) {
	if d.rejectMedia && p.After.FPS == 30 {
		if d.changeOnRejection {
			d.profile = p.After
			d.writes++
		}
		return camera.CurrentState{}, camera.ErrUnsupported
	}
	state, err := d.device.Apply(ctx, h, l, p)
	if d.frequencyAfterMedia != 0 {
		d.frequency = d.frequencyAfterMedia
	}
	return state, err
}

func (d *presetDevice) PlanInitialization(_ context.Context, _ camera.DeviceHint, _ camera.CredentialLease, desired camera.DeviceSettings) (camera.InitializationPlan, error) {
	p := camera.InitializationPlan{Before: camera.DeviceSettings{PowerLineFrequency: d.frequency}, After: desired}
	if d.frequency != desired.PowerLineFrequency {
		p.Changes = []string{"power_line_frequency"}
	}
	return p, nil
}

func (d *presetDevice) ApplyInitialization(_ context.Context, _ camera.DeviceHint, _ camera.CredentialLease, p camera.InitializationPlan) (camera.DeviceSettings, error) {
	d.initializationWrites++
	d.frequency = p.After.PowerLineFrequency
	if d.initializationError != nil {
		err := d.initializationError
		d.initializationError = nil
		return camera.DeviceSettings{}, err
	}
	return p.After, nil
}

func presetFixture(t *testing.T, frequency bool) (provision.Request, *presetDevice, *publisher, *provision.Engine, string) {
	t.Helper()
	q, d, p, e, dir := fixture(t)
	d.ready = true
	d.profile.FPS = 25
	q.UsePreset, q.Desired = true, camera.DesiredState{}
	pd := &presetDevice{device: d, frequency: 50}
	e.Inspector, e.Configurator, e.Initializer = pd, pd, pd
	settings := camera.PresetSettings{Video: camera.DesiredState{ProfileToken: "main", Codec: "h264", FPS: 30}}
	if frequency {
		settings.Initialization.PowerLineFrequency = 60
	}
	e.Presets = presetProvider(func(_ context.Context, scope camera.EnrollmentScope, h camera.DeviceHint) (camera.DevicePreset, error) {
		require.Equal(t, q.Scope, scope)
		require.Equal(t, "authenticated-serial", h.Serial)
		return camera.DevicePreset{Ref: camera.PresetRef{ID: "office", Revision: "r1"}, Requested: settings, Fallbacks: []camera.PresetSettings{{Video: camera.DesiredState{FPS: 25}}}, Required: []string{camera.PresetFieldCodec}}, nil
	})
	return q, pd, p, e, dir
}

func TestPresetPlansPrerequisiteWithoutWritingThenAppliesBeforeMedia(t *testing.T) {
	q, d, p, e, _ := presetFixture(t, true)
	r, err := e.Execute(context.Background(), q, false)
	require.NoError(t, err)
	require.Equal(t, provision.Planned, r.Stage)
	require.NotNil(t, r.InitializationPlan)
	require.Nil(t, r.Plan, "30fps capabilities are not queried until frequency is applied")
	require.Zero(t, d.initializationWrites)
	require.Zero(t, d.videoPlans)
	require.Zero(t, d.writes)
	r, err = e.Execute(context.Background(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, 60, r.Initialized.PowerLineFrequency)
	require.Equal(t, float64(30), r.Verified.Profile.FPS)
	require.Empty(t, r.Warnings)
	require.Equal(t, 1, d.initializationWrites)
	require.Equal(t, 1, d.writes)
	require.Equal(t, float64(30), p.last.Profile.FPS)
	version := r.Version
	r, err = e.Execute(context.Background(), q, true)
	require.NoError(t, err)
	require.Equal(t, version, r.Version)
	require.Equal(t, 1, d.initializationWrites)
}

func TestPresetFallbackPersistsEffectiveSettingsAndWarningsAcrossRestart(t *testing.T) {
	q, d, p, e, dir := presetFixture(t, false)
	p.notReady = true
	r, err := e.Execute(context.Background(), q, true)
	require.ErrorIs(t, err, provision.ErrNotReady)
	require.Equal(t, float64(30), r.Preset.Requested.Video.FPS)
	require.Equal(t, float64(25), r.Effective.Video.FPS)
	require.Equal(t, float64(25), r.Verified.Profile.FPS)
	require.Equal(t, 1, r.CandidateIndex)
	require.Len(t, r.Warnings, 1)
	require.Equal(t, "30", r.Warnings[0].Requested)
	require.Equal(t, "25", r.Warnings[0].Applied)
	require.Equal(t, camera.DesiredState{}, r.Request.Desired)
	e.Store, err = filestore.Open(dir)
	require.NoError(t, err)
	e.Presets = presetProvider(func(context.Context, camera.EnrollmentScope, camera.DeviceHint) (camera.DevicePreset, error) {
		t.Fatal("resumed enrollment must use the pinned snapshot")
		return camera.DevicePreset{}, nil
	})
	p.notReady = false
	r, err = e.Execute(context.Background(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Len(t, r.Warnings, 1)
	require.Equal(t, "one-source", r.Binding.SourceID)
	require.Equal(t, 1, d.writes)
	require.Equal(t, 1, p.deploys)
}

func TestPresetLostInitializationResponseResumesWithoutDowngradeOrRepeatedWrite(t *testing.T) {
	q, d, _, e, _ := presetFixture(t, true)
	d.initializationError = errors.New("lost response containing private vendor material")
	r, err := e.Execute(context.Background(), q, true)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "private vendor")
	require.Zero(t, r.CandidateIndex)
	require.Equal(t, 60, d.frequency)
	require.Zero(t, d.writes)
	r, err = e.Execute(context.Background(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, 1, d.initializationWrites)
	require.Equal(t, float64(30), r.Verified.Profile.FPS)
}

func TestPresetOnlyExplicitUnchangedWriteRejectionPermitsFallback(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "changed"}[changed], func(t *testing.T) {
			q, d, _, e, _ := presetFixture(t, false)
			d.maxFPS, d.rejectMedia, d.changeOnRejection = 30, true, changed
			r, err := e.Execute(context.Background(), q, true)
			if changed {
				require.ErrorIs(t, err, camera.ErrUnsupported)
				require.Zero(t, r.CandidateIndex)
				require.Empty(t, r.Warnings)
				return
			}
			require.NoError(t, err)
			require.Equal(t, provision.Active, r.Stage)
			require.Equal(t, 1, r.CandidateIndex)
			require.Equal(t, float64(25), r.Verified.Profile.FPS)
		})
	}
}

func TestPresetDoesNotTreatAuthOrTransportFailureAsUnsupportedSettings(t *testing.T) {
	for _, failure := range []error{camera.ErrForbidden, camera.ErrAuthRequired, context.DeadlineExceeded, errors.New("network failed")} {
		t.Run(failure.Error(), func(t *testing.T) {
			q, d, p, e, _ := presetFixture(t, false)
			d.planError = failure
			r, err := e.Execute(context.Background(), q, true)
			require.Error(t, err)
			require.Zero(t, r.CandidateIndex)
			require.Empty(t, r.Warnings)
			require.Zero(t, d.writes)
			require.Zero(t, p.deploys)
		})
	}
}

func TestPresetInvalidOrMissingPolicyDoesNotWriteConfiguration(t *testing.T) {
	q, d, _, e, _ := presetFixture(t, true)
	e.Presets = nil
	r, err := e.Execute(context.Background(), q, true)
	require.ErrorIs(t, err, camera.ErrNeedsUserInput)
	require.Equal(t, provision.WaitingInput, r.Stage)
	require.Zero(t, d.initializationWrites)
	require.Zero(t, d.writes)
	q.Desired.Revision = "ambiguous-direct-settings"
	_, err = e.Execute(context.Background(), q, true)
	require.ErrorIs(t, err, camera.ErrInvalid)
}

type videoOnly struct{ camera.VideoConfigurator }

func TestPresetCanPreserveUnsupportedOptionalInitializationAndStillRegister(t *testing.T) {
	q, d, _, e, _ := presetFixture(t, true)
	e.Initializer = nil
	e.Configurator = videoOnly{d}
	e.Presets = presetProvider(func(context.Context, camera.EnrollmentScope, camera.DeviceHint) (camera.DevicePreset, error) {
		return camera.DevicePreset{Ref: camera.PresetRef{ID: "best-effort", Revision: "1"}, Requested: camera.PresetSettings{Initialization: camera.DeviceSettings{PowerLineFrequency: 60}, Video: camera.DesiredState{Codec: "h264", ProfileToken: "main", FPS: 30}}, Fallbacks: []camera.PresetSettings{{Preserve: []string{camera.PresetFieldFrequency}, Video: camera.DesiredState{FPS: 25}}}, Required: []string{camera.PresetFieldCodec}}, nil
	})
	r, err := e.Execute(context.Background(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, 50, d.frequency)
	require.Zero(t, d.initializationWrites)
	require.Equal(t, float64(25), r.Verified.Profile.FPS)
	require.Len(t, r.Warnings, 2)
	require.Equal(t, "preserved", r.Warnings[0].Applied)
}

func TestPresetRechecksInitializationAfterVideoSettings(t *testing.T) {
	q, d, p, e, _ := presetFixture(t, true)
	d.frequencyAfterMedia = 50
	r, err := e.Execute(context.Background(), q, true)
	require.ErrorIs(t, err, camera.ErrConflict)
	require.NotEqual(t, provision.Active, r.Stage)
	require.Zero(t, p.deploys)
}
