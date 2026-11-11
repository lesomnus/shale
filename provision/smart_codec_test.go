package provision_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
)

type smartInitializer struct {
	mode          string
	frameRate     string
	writes, calls int
	lost, reject  bool
	received      *camera.InitializationVideoMain
}

func (s *smartInitializer) snapshot() *camera.InitializationVideoMain {
	return &camera.InitializationVideoMain{EncodeType: "H264", Resolution: "1920*1080", FrameRate: s.frameRate, BitrateType: "vbr", Quality: "1", Bitrate: "4096", SmartCodec: s.mode, AdaptiveMode: "off"}
}
func (s *smartInitializer) PlanInitialization(_ context.Context, _ camera.DeviceHint, _ camera.CredentialLease, w camera.DeviceSettings) (camera.InitializationPlan, error) {
	if s.reject {
		return camera.InitializationPlan{}, camera.ErrUnsupported
	}
	p := camera.InitializationPlan{Before: camera.DeviceSettings{SmartCodec: s.mode}, After: w, Revision: "fixture-smart", VideoMainBefore: s.snapshot()}
	if s.mode != w.SmartCodec {
		p.Changes = []string{"smart_codec"}
	}
	return p, nil
}
func (s *smartInitializer) ApplyInitialization(_ context.Context, _ camera.DeviceHint, _ camera.CredentialLease, p camera.InitializationPlan) (camera.DeviceSettings, error) {
	s.calls++
	snapshot := *p.VideoMainBefore
	s.received = &snapshot
	if s.mode != p.After.SmartCodec {
		if !reflect.DeepEqual(s.snapshot(), p.VideoMainBefore) {
			return camera.DeviceSettings{}, camera.ErrConflict
		}
		s.mode = p.After.SmartCodec
		s.writes++
	}
	if s.lost {
		s.lost = false
		return camera.DeviceSettings{}, errors.New("fixture lost response")
	}
	return p.After, nil
}
func smartEngine(t *testing.T) (provision.Request, *smartInitializer, *publisher, *provision.Engine) {
	t.Helper()
	q, d, p, e, _ := fixture(t)
	d.ready = true
	video := q.Desired
	q.Desired = camera.DesiredState{}
	q.UsePreset = true
	s := &smartInitializer{mode: "on", frameRate: "65566"}
	e.Initializer = s
	e.Presets = presetProvider(func(context.Context, camera.EnrollmentScope, camera.DeviceHint) (camera.DevicePreset, error) {
		return camera.DevicePreset{Ref: camera.PresetRef{ID: "smart-only", Revision: "1"}, Requested: camera.PresetSettings{Initialization: camera.DeviceSettings{SmartCodec: "off"}, Video: video}, Required: []string{camera.PresetFieldSmartCodec}}, nil
	})
	return q, s, p, e
}
func TestSmartCodecOnlyInitializationAndLostReplyRetainNativePrecondition(t *testing.T) {
	q, s, p, e := smartEngine(t)
	s.lost = true
	first, err := e.Execute(t.Context(), q, true)
	require.Error(t, err)
	require.Equal(t, 1, s.writes)
	require.Zero(t, p.deploys)
	require.Equal(t, 0, first.InitializationPlan.After.PowerLineFrequency)
	s.frameRate = "30" // Another video field changes after the already-applied edit.
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, 1, s.writes)
	require.Equal(t, "65566", s.received.FrameRate, "resume must reuse original snapshot instead of rebaselining")
	require.Equal(t, "65566", r.InitializationPlan.VideoMainBefore.FrameRate)
	require.Equal(t, "off", r.Initialized.SmartCodec)
	require.Equal(t, 1, p.deploys)
}

type resetSmartConfigurator struct {
	camera.VideoConfigurator
	initializer *smartInitializer
}

func (c resetSmartConfigurator) Apply(ctx context.Context, h camera.DeviceHint, l camera.CredentialLease, p camera.ChangePlan) (camera.CurrentState, error) {
	v, e := c.VideoConfigurator.Apply(ctx, h, l, p)
	c.initializer.mode = "on"
	return v, e
}
func TestSmartCodecOnlyIsRecheckedAfterVideoApply(t *testing.T) {
	q, s, p, e := smartEngine(t)
	e.Configurator = resetSmartConfigurator{VideoConfigurator: e.Configurator, initializer: s}
	_, err := e.Execute(t.Context(), q, true)
	require.ErrorIs(t, err, camera.ErrConflict)
	require.Zero(t, p.deploys)
}

func TestSmartCodecUnsupportedUsesOnlyExplicitPreserveFallback(t *testing.T) {
	q, s, p, e := smartEngine(t)
	s.reject = true
	preferred := e.Presets
	e.Presets = presetProvider(func(ctx context.Context, scope camera.EnrollmentScope, hint camera.DeviceHint) (camera.DevicePreset, error) {
		preset, err := preferred.Resolve(ctx, scope, hint)
		preset.Required = nil
		preset.Fallbacks = []camera.PresetSettings{{Preserve: []string{camera.PresetFieldSmartCodec}}}
		return preset, err
	})
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, 1, r.CandidateIndex)
	require.Equal(t, "off", r.Preset.Requested.Initialization.SmartCodec)
	require.Empty(t, r.Effective.Initialization.SmartCodec)
	require.Len(t, r.Warnings, 1)
	require.Equal(t, camera.PresetFieldSmartCodec, r.Warnings[0].Field)
	require.Zero(t, s.writes)
	require.Equal(t, 1, p.deploys)
}
