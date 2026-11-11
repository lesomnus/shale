package camera

import (
	"errors"
	"math"
	"testing"
)

func testDevicePreset() DevicePreset {
	return DevicePreset{
		Ref: PresetRef{ID: "front-door", Revision: "v3"},
		Requested: PresetSettings{
			Initialization: DeviceSettings{PowerLineFrequency: 60},
			Video:          DesiredState{Codec: "h264", Width: 1920, Height: 1080, FPS: 30, Bitrate: 4_096_000},
		},
		Fallbacks: []PresetSettings{{Video: DesiredState{FPS: 25}}, {Video: DesiredState{Width: 1280, Height: 720, FPS: 30}}},
		Required:  []string{PresetFieldCodec, PresetFieldFrequency},
	}
}

func TestPresetCandidatesMaterializeInOrderAndPinRevision(t *testing.T) {
	p := testDevicePreset()
	p.Requested.Video.AllowFallback = true
	p.Fallbacks[0].Video.Revision = "obsolete"
	p.Fallbacks = append(p.Fallbacks, p.Fallbacks[0])
	candidates, err := p.Candidates()
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 3 {
		t.Fatalf("got %d candidates", len(candidates))
	}
	if candidates[0].Video.FPS != 30 || candidates[1].Video.FPS != 25 || candidates[2].Video.Width != 1280 || candidates[2].Video.FPS != 30 {
		t.Fatalf("wrong preference order: %+v", candidates)
	}
	for _, c := range candidates {
		if c.Initialization.PowerLineFrequency != 60 || c.Video.Codec != "h264" || c.Video.Bitrate != 4_096_000 || c.Video.Revision != PresetRevision(p.Ref) || c.Video.AllowFallback {
			t.Fatalf("candidate lost requested setting or immutable revision: %+v", c)
		}
	}
	candidates[1].Video.FPS = 5
	if p.Fallbacks[0].Video.FPS != 25 {
		t.Fatal("candidate mutated the preset snapshot")
	}
	if got := WarningsFor(p.Requested, candidates[2]); len(got) != 1 || got[0].Field != PresetFieldResolution || got[0].Requested != "1920x1080" || got[0].Applied != "1280x720" {
		t.Fatalf("wrong structured warning: %+v", got)
	}
}

func TestPresetRevisionDoesNotCollideOnReferenceDelimiters(t *testing.T) {
	if PresetRevision(PresetRef{ID: "a@b", Revision: "c"}) == PresetRevision(PresetRef{ID: "a", Revision: "b@c"}) {
		t.Fatal("different preset references shared a media revision")
	}
}

func TestPresetRejectsRelaxingRequiredField(t *testing.T) {
	p := testDevicePreset()
	p.Required = append(p.Required, PresetFieldFPS)
	if _, err := p.Candidates(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("required FPS changed: %v", err)
	}
	p = testDevicePreset()
	p.Fallbacks = append(p.Fallbacks, PresetSettings{Initialization: DeviceSettings{PowerLineFrequency: 50}})
	if err := p.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("required frequency changed: %v", err)
	}
}

func TestFallbackCanPreserveOptionalDeviceSettings(t *testing.T) {
	p := testDevicePreset()
	p.Required = []string{PresetFieldCodec}
	p.Fallbacks = []PresetSettings{{Preserve: []string{PresetFieldFrequency}, Video: DesiredState{FPS: 25}}}
	candidates, err := p.Candidates()
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 || candidates[0].Initialization.PowerLineFrequency != 60 || candidates[1].Initialization.PowerLineFrequency != 0 || candidates[1].Video.FPS != 25 || len(candidates[1].Preserve) != 0 {
		t.Fatalf("optional preservation did not materialize: %+v", candidates)
	}
	warnings := WarningsFor(candidates[0], candidates[1])
	if len(warnings) != 2 || warnings[0].Field != PresetFieldFrequency || warnings[0].Requested != "60" || warnings[0].Applied != "preserved" {
		t.Fatalf("wrong preservation warning: %+v", warnings)
	}
	p.Required = append(p.Required, PresetFieldFrequency)
	if err := p.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("required frequency was preserved instead of applied: %v", err)
	}
}

func TestPreservePatchCanBeOverriddenByExplicitDeviceValue(t *testing.T) {
	base := testDevicePreset().Requested
	cleared := MergePresetSettings(base, PresetSettings{Preserve: []string{PresetFieldFrequency, PresetFieldResolution}})
	if cleared.Initialization.PowerLineFrequency != 0 || cleared.Video.Width != 0 || cleared.Video.Height != 0 || len(cleared.Preserve) != 0 {
		t.Fatalf("preserve patch retained desired values: %+v", cleared)
	}
	device := MergePresetSettings(cleared, PresetSettings{Initialization: DeviceSettings{PowerLineFrequency: 50}, Video: DesiredState{Width: 640, Height: 360}})
	if device.Initialization.PowerLineFrequency != 50 || device.Video.Width != 640 || device.Video.Height != 360 {
		t.Fatalf("higher-priority values did not override preservation: %+v", device)
	}
	for _, patch := range []PresetSettings{
		{Preserve: []string{PresetFieldFrequency}, Initialization: DeviceSettings{PowerLineFrequency: 60}},
		{Preserve: []string{PresetFieldFPS, PresetFieldFPS}},
		{Preserve: []string{"arbitrary.setting"}},
	} {
		if err := ValidatePresetSettings(patch); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid preserve patch accepted: %v", err)
		}
	}
	p := testDevicePreset()
	p.Requested.Preserve = []string{PresetFieldGOP}
	if err := p.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unmaterialized requested preset accepted: %v", err)
	}
}

func TestPresetRejectsInvalidSettingsAndRequirements(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*DevicePreset)
	}{
		{"frequency", func(p *DevicePreset) { p.Requested.Initialization.PowerLineFrequency = 55 }},
		{"partial_resolution", func(p *DevicePreset) { p.Fallbacks[0].Video.Width = 640 }},
		{"nonfinite_fps", func(p *DevicePreset) { p.Requested.Video.FPS = math.NaN() }},
		{"negative_bitrate", func(p *DevicePreset) { p.Fallbacks[0].Video.Bitrate = -1 }},
		{"unknown_codec", func(p *DevicePreset) { p.Requested.Video.Codec = "arbitrary-vendor-codec" }},
		{"unknown_required", func(p *DevicePreset) { p.Required = append(p.Required, "vendor.command") }},
		{"unset_required", func(p *DevicePreset) { p.Required = append(p.Required, PresetFieldGOP) }},
		{"repeated_required", func(p *DevicePreset) { p.Required = append(p.Required, PresetFieldCodec) }},
		{"unbounded_fallbacks", func(p *DevicePreset) { p.Fallbacks = make([]PresetSettings, 17) }},
		{"missing_revision", func(p *DevicePreset) { p.Ref.Revision = "" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := testDevicePreset()
			c.modify(&p)
			if err := p.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("accepted invalid preset: %v", err)
			}
		})
	}
}

func TestPresetCloneDoesNotAliasEnrollmentState(t *testing.T) {
	p := testDevicePreset()
	p.MatchedRules = []string{"model-default"}
	p.Fallbacks[0].Preserve = []string{PresetFieldGOP}
	clone := p.Clone()
	clone.MatchedRules[0] = "changed"
	clone.Required[0] = PresetFieldFPS
	clone.Fallbacks[0].Video.FPS = 1
	clone.Fallbacks[0].Preserve[0] = PresetFieldFPS
	if p.MatchedRules[0] != "model-default" || p.Required[0] != PresetFieldCodec || p.Fallbacks[0].Video.FPS != 25 || p.Fallbacks[0].Preserve[0] != PresetFieldGOP {
		t.Fatal("preset snapshot shares mutable slices")
	}
}
