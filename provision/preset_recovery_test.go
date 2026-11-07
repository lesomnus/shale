package provision_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/lesomnus/shale/provision/filestore"
	"github.com/stretchr/testify/require"
)

// A driver may add context or combine errors. An unsupported marker must not
// turn a failed authorization or an uncertain transport result into a downgrade.
type primaryFailureConfigurator struct {
	*presetDevice
	failure error
}

func (d primaryFailureConfigurator) Plan(ctx context.Context, inspection camera.Inspection, desired camera.DesiredState) (camera.ChangePlan, error) {
	if desired.FPS == 30 {
		return camera.ChangePlan{}, d.failure
	}
	return d.presetDevice.Plan(ctx, inspection, desired)
}

func TestPresetJoinedUnsupportedErrorsDoNotRelaxPolicy(t *testing.T) {
	for _, cause := range []error{camera.ErrAuthRequired, camera.ErrForbidden, camera.ErrExpired, camera.ErrAmbiguousIdentity, context.DeadlineExceeded, errors.New("transport lost")} {
		t.Run(cause.Error(), func(t *testing.T) {
			q, d, publisher, engine, _ := presetFixture(t, false)
			engine.Configurator = primaryFailureConfigurator{presetDevice: d, failure: fmt.Errorf("driver context: %w", errors.Join(camera.ErrUnsupported, cause))}
			run, err := engine.Execute(context.Background(), q, true)
			require.Error(t, err)
			require.NotEqual(t, provision.Active, run.Stage)
			require.Zero(t, run.CandidateIndex)
			require.Empty(t, run.Warnings)
			require.Zero(t, d.writes)
			require.Zero(t, publisher.deploys)
		})
	}
}

func TestPresetWrappedUnsupportedStillPermitsConfiguredAlternative(t *testing.T) {
	q, d, _, engine, _ := presetFixture(t, false)
	engine.Configurator = primaryFailureConfigurator{presetDevice: d, failure: fmt.Errorf("unsupported encoder option: %w", camera.ErrUnsupported)}
	run, err := engine.Execute(context.Background(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, run.Stage)
	require.Equal(t, 1, run.CandidateIndex)
	require.Equal(t, float64(25), run.Verified.Profile.FPS)
	require.Len(t, run.Warnings, 1)
}

type postWriteReadbackInitializer struct {
	*presetDevice
	plans   int
	failure error
}

func (d *postWriteReadbackInitializer) PlanInitialization(ctx context.Context, hint camera.DeviceHint, lease camera.CredentialLease, settings camera.DeviceSettings) (camera.InitializationPlan, error) {
	d.plans++
	if d.plans == 2 {
		return camera.InitializationPlan{}, d.failure
	}
	return d.presetDevice.PlanInitialization(ctx, hint, lease, settings)
}

func addPreserveFrequencyAlternative(engine *provision.Engine) {
	provider := engine.Presets
	engine.Presets = presetProvider(func(ctx context.Context, scope camera.EnrollmentScope, hint camera.DeviceHint) (camera.DevicePreset, error) {
		preset, err := provider.Resolve(ctx, scope, hint)
		if err == nil {
			preset.Fallbacks = []camera.PresetSettings{{Preserve: []string{camera.PresetFieldFrequency}, Video: camera.DesiredState{FPS: 25}}}
		}
		return preset, err
	})
}

func TestPresetPostWriteReadbackErrorsNeverSelectAnotherCandidate(t *testing.T) {
	for _, failure := range []error{camera.ErrUnsupported, camera.ErrAuthRequired, context.DeadlineExceeded} {
		t.Run(failure.Error(), func(t *testing.T) {
			q, d, publisher, engine, dir := presetFixture(t, true)
			addPreserveFrequencyAlternative(engine)
			initializer := &postWriteReadbackInitializer{presetDevice: d, failure: failure}
			engine.Initializer = initializer
			run, err := engine.Execute(context.Background(), q, true)
			require.Error(t, err)
			require.Zero(t, run.CandidateIndex)
			require.Empty(t, run.Warnings)
			require.Equal(t, 60, d.frequency, "write succeeded but readback is unavailable")
			require.Equal(t, 1, d.initializationWrites)
			require.Zero(t, d.writes)
			require.Zero(t, publisher.deploys)
			pinned := run.Preset.Clone()

			// A restarted worker sees the successful write and resumes the
			// same pinned request; it neither reselects policy nor repeats it.
			engine.Store, err = filestore.Open(dir)
			require.NoError(t, err)
			engine.Presets = presetProvider(func(context.Context, camera.EnrollmentScope, camera.DeviceHint) (camera.DevicePreset, error) {
				t.Fatal("a post-write retry must use its pinned preset snapshot")
				return camera.DevicePreset{}, nil
			})
			run, err = engine.Execute(context.Background(), q, true)
			require.NoError(t, err)
			require.Equal(t, provision.Active, run.Stage)
			require.Equal(t, pinned, *run.Preset)
			require.Zero(t, run.CandidateIndex)
			require.Empty(t, run.Warnings)
			require.Equal(t, 1, d.initializationWrites)
			require.Equal(t, float64(30), run.Verified.Profile.FPS)
		})
	}
}

type rejectingInitializer struct {
	*presetDevice
	changesBeforeRejecting bool
}

func (d rejectingInitializer) ApplyInitialization(_ context.Context, _ camera.DeviceHint, _ camera.CredentialLease, plan camera.InitializationPlan) (camera.DeviceSettings, error) {
	d.initializationWrites++
	if d.changesBeforeRejecting {
		d.frequency = plan.After.PowerLineFrequency
	}
	return camera.DeviceSettings{}, camera.ErrUnsupported
}

func TestPresetInitializationRejectionNeedsProofOfNoChange(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("changed_%t", changed), func(t *testing.T) {
			q, d, publisher, engine, _ := presetFixture(t, true)
			addPreserveFrequencyAlternative(engine)
			engine.Initializer = rejectingInitializer{presetDevice: d, changesBeforeRejecting: changed}
			run, err := engine.Execute(context.Background(), q, true)
			if changed {
				require.Error(t, err)
				require.Zero(t, run.CandidateIndex)
				require.Empty(t, run.Warnings)
				require.Zero(t, publisher.deploys)
				return
			}
			require.NoError(t, err)
			require.Equal(t, provision.Active, run.Stage)
			require.Equal(t, 1, run.CandidateIndex)
			require.Equal(t, float64(25), run.Verified.Profile.FPS)
			require.Equal(t, 50, d.frequency)
			require.Len(t, run.Warnings, 2)
			require.Equal(t, "preserved", run.Warnings[0].Applied)
		})
	}
}
