package provision

import (
	"context"
	"errors"
	"reflect"
	"slices"

	"github.com/lesomnus/shale/camera"
)

var errAlternativeAvailable = errors.New("device setting can use an allowed alternative")

func unsupportedAlternative() error {
	return errors.Join(camera.ErrUnsupported, errAlternativeAvailable)
}

// Only an unambiguous setting rejection authorizes another candidate. In
// particular, joining a rejection with an authentication or transport failure
// must not erase the failure and turn it into a successful downgrade.
func unsupportedSetting(err error) bool {
	if !errors.Is(err, camera.ErrUnsupported) {
		return false
	}
	var supportedLeaves func(error) bool
	supportedLeaves = func(err error) bool {
		if err == camera.ErrUnsupported || err == errAlternativeAvailable {
			return true
		}
		if multi, ok := err.(interface{ Unwrap() []error }); ok {
			children := multi.Unwrap()
			if len(children) == 0 {
				return false
			}
			for _, child := range children {
				if child != nil && !supportedLeaves(child) {
					return false
				}
			}
			return true
		}
		if single, ok := err.(interface{ Unwrap() error }); ok {
			return supportedLeaves(single.Unwrap())
		}
		return false
	}
	return supportedLeaves(err)
}

// configure shares the normal read-back path between direct requests and
// presets. A selected alternative is durable before its first device write;
// retries resume that candidate rather than trying the preferred values again.
func (e *Engine) configure(ctx context.Context, r *Run, inspection camera.Inspection, lease camera.CredentialLease, apply bool) (camera.Inspection, camera.DesiredState, bool, error) {
	q := r.Request
	candidates := []camera.PresetSettings{{Video: q.Desired}}
	if q.UsePreset {
		if r.Preset == nil {
			if e.Presets == nil {
				return inspection, camera.DesiredState{}, false, camera.ErrNeedsUserInput
			}
			preset, err := e.Presets.Resolve(ctx, q.Scope, inspection.Device)
			if errors.Is(err, camera.ErrPresetNotFound) {
				return inspection, camera.DesiredState{}, false, camera.ErrNeedsUserInput
			}
			if err != nil {
				return inspection, camera.DesiredState{}, false, err
			}
			preset = preset.Clone()
			if err := preset.Validate(); err != nil {
				return inspection, camera.DesiredState{}, false, err
			}
			r.Preset = &preset
			if err := e.save(ctx, r, Inspecting, "preset_resolved"); err != nil {
				return inspection, camera.DesiredState{}, false, err
			}
		}
		var err error
		candidates, err = r.Preset.Candidates()
		if err != nil {
			return inspection, camera.DesiredState{}, false, err
		}
	}
	if r.CandidateIndex < 0 || r.CandidateIndex >= len(candidates) {
		return inspection, camera.DesiredState{}, false, camera.ErrConflict
	}
	if r.Effective != nil && !reflect.DeepEqual(*r.Effective, candidates[r.CandidateIndex]) {
		return inspection, camera.DesiredState{}, false, camera.ErrConflict
	}
	for index := r.CandidateIndex; index < len(candidates); index++ {
		candidate := candidates[index]
		if q.UsePreset && (r.Effective == nil || r.CandidateIndex != index) {
			r.CandidateIndex, r.Effective = index, &candidate
			r.Warnings = camera.WarningsFor(candidates[0], candidate)
			r.InitializationPlan, r.Initialized, r.Plan, r.Verified = nil, nil, nil, nil
			if err := e.save(ctx, r, Inspecting, "preset_candidate_selected"); err != nil {
				return inspection, candidate.Video, false, err
			}
		}
		initialized, complete, err := e.initialize(ctx, r, inspection, lease, candidate, apply)
		if err != nil {
			if q.UsePreset && errors.Is(err, errAlternativeAvailable) && unsupportedSetting(err) && index+1 < len(candidates) {
				continue
			}
			return inspection, candidate.Video, false, err
		}
		inspection = initialized
		if !complete {
			return inspection, candidate.Video, false, nil
		}
		plan, err := e.Configurator.Plan(ctx, inspection, candidate.Video)
		if err != nil {
			if q.UsePreset && unsupportedSetting(err) && index+1 < len(candidates) {
				continue
			}
			return inspection, candidate.Video, false, err
		}
		plan.Before.StreamURI = camera.SanitizeURI(plan.Before.StreamURI)
		plan.After.StreamURI = camera.SanitizeURI(plan.After.StreamURI)
		r.Plan = &plan
		if err = e.save(ctx, r, Planned, "plan_ready"); err != nil || !apply {
			return inspection, candidate.Video, false, err
		}
		if len(plan.Changes) != 0 {
			if err = e.save(ctx, r, Applying, "apply_started"); err != nil {
				return inspection, candidate.Video, false, err
			}
			if err = e.Store.BeginMutation(ctx, q); err != nil {
				return inspection, candidate.Video, false, err
			}
			if _, err = e.Configurator.Apply(ctx, inspection.Device, lease, plan); err != nil {
				// An explicit rejection permits an alternative only after an
				// independent read proves that the write made no change.
				if q.UsePreset && unsupportedSetting(err) && index+1 < len(candidates) {
					observed, readErr := e.reinspect(ctx, q, inspection, lease)
					if readErr != nil {
						return inspection, candidate.Video, false, readErr
					}
					before, ok := profileFor(observed.Current, plan.ProfileToken)
					if ok && equalVideo(before, plan.Before) {
						inspection = observed
						continue
					}
				}
				return inspection, candidate.Video, false, err
			}
		}
		readback, err := e.reinspect(ctx, q, inspection, lease)
		if err != nil {
			return inspection, candidate.Video, false, err
		}
		if candidate.Initialization != (camera.DeviceSettings{}) {
			initializer := e.initializer()
			checked, err := initializer.PlanInitialization(ctx, readback.Device, lease, candidate.Initialization)
			if err != nil {
				return readback, candidate.Video, false, err
			}
			if len(checked.Changes) != 0 || checked.Before != candidate.Initialization || checked.After != candidate.Initialization {
				return readback, candidate.Video, false, camera.ErrConflict
			}
			r.Initialized = &checked.Before
		}
		check, err := e.Configurator.Plan(ctx, readback, candidate.Video)
		if err != nil {
			return readback, candidate.Video, false, err
		}
		if len(check.Changes) != 0 {
			return readback, candidate.Video, false, camera.ErrConflict
		}
		r.Inspection = &readback
		return readback, candidate.Video, true, nil
	}
	return inspection, camera.DesiredState{}, false, camera.ErrUnsupported
}

// initialize is deliberately separate from password activation. A frequency
// change can unlock media options, so it must finish before video planning.
func (e *Engine) initialize(ctx context.Context, r *Run, inspection camera.Inspection, lease camera.CredentialLease, candidate camera.PresetSettings, apply bool) (camera.Inspection, bool, error) {
	if candidate.Initialization == (camera.DeviceSettings{}) {
		return inspection, true, nil
	}
	initializer := e.initializer()
	if initializer == nil {
		return inspection, false, unsupportedAlternative()
	}
	var plan camera.InitializationPlan
	var err error
	if saved := r.InitializationPlan; saved != nil && saved.VideoMainBefore != nil {
		plan = *saved
		snapshot := *saved.VideoMainBefore
		plan.VideoMainBefore, plan.Changes = &snapshot, slices.Clone(saved.Changes)
	} else {
		plan, err = initializer.PlanInitialization(ctx, inspection.Device, lease, candidate.Initialization)
	}
	if err != nil {
		if unsupportedSetting(err) {
			return inspection, false, unsupportedAlternative()
		}
		return inspection, false, err
	}
	if camera.ValidateInitializationPlan(plan, candidate.Initialization) != nil {
		return inspection, false, camera.ErrConflict
	}
	r.InitializationPlan = &plan
	if len(plan.Changes) != 0 {
		if err = e.save(ctx, r, Planned, "initialization_planned"); err != nil || !apply {
			return inspection, false, err
		}
		if err = e.save(ctx, r, Initializing, "initialization_started"); err != nil {
			return inspection, false, err
		}
		if err = e.Store.BeginMutation(ctx, r.Request); err != nil {
			return inspection, false, err
		}
		if _, err = initializer.ApplyInitialization(ctx, inspection.Device, lease, plan); err != nil {
			if unsupportedSetting(err) {
				observed, readErr := initializer.PlanInitialization(ctx, inspection.Device, lease, candidate.Initialization)
				if readErr != nil {
					// No independent proof of an unchanged device: do not advance.
					return inspection, false, readErr
				}
				if observed.Before != plan.Before || plan.VideoMainBefore != nil && !reflect.DeepEqual(observed.VideoMainBefore, plan.VideoMainBefore) {
					return inspection, false, camera.ErrConflict
				}
				return inspection, false, unsupportedAlternative()
			}
			return inspection, false, err
		}
	}
	// Verify using another read; an adapter's Apply result is not sufficient.
	readback, err := initializer.PlanInitialization(ctx, inspection.Device, lease, candidate.Initialization)
	if err != nil {
		return inspection, false, err
	}
	if len(readback.Changes) != 0 || readback.Before != candidate.Initialization || readback.After != candidate.Initialization {
		return inspection, false, camera.ErrConflict
	}
	r.Initialized = &readback.Before
	if len(plan.Changes) != 0 {
		inspection, err = e.reinspect(ctx, r.Request, inspection, lease)
		if err != nil {
			return inspection, false, err
		}
		r.Inspection = &inspection
		if err = e.save(ctx, r, Inspecting, "initialization_verified"); err != nil {
			return inspection, false, err
		}
	}
	return inspection, true, nil
}

func (e *Engine) initializer() camera.DeviceInitializer {
	if e.Initializer != nil {
		return e.Initializer
	}
	initializer, _ := e.Configurator.(camera.DeviceInitializer)
	return initializer
}

func (e *Engine) reinspect(ctx context.Context, q Request, previous camera.Inspection, lease camera.CredentialLease) (camera.Inspection, error) {
	i, err := e.Inspector.Inspect(ctx, q.Device, lease)
	if err != nil {
		return i, err
	}
	i = cleanInspection(i)
	if !samePhysicalDevice(previous.Device, i.Device) {
		return i, camera.ErrAmbiguousIdentity
	}
	if err = e.Store.Identify(ctx, q.Scope, i.Device); err != nil {
		return i, err
	}
	return i, nil
}

func equalVideo(a, b camera.MediaProfile) bool {
	return a.Token == b.Token && a.Codec == b.Codec && a.Width == b.Width && a.Height == b.Height && a.FPS == b.FPS && a.Bitrate == b.Bitrate && a.GOP == b.GOP
}
