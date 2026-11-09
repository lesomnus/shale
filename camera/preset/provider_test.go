package preset

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/lesomnus/shale/camera"
)

var testScope = camera.EnrollmentScope{TenantID: "tenant", SiteID: "office", SetID: "cameras", ProducerID: "producer", NetworkID: "lan"}

func testRules() Config {
	return Config{ID: "office-presets", Revision: "v7", Rules: []Rule{
		{ID: "tenant", Scope: camera.EnrollmentScope{TenantID: "tenant"}, Settings: camera.PresetSettings{Video: camera.DesiredState{Codec: "h264", Width: 1280, Height: 720}}, Required: []string{camera.PresetFieldCodec}},
		{ID: "office", Scope: camera.EnrollmentScope{TenantID: "tenant", SiteID: "office"}, Settings: camera.PresetSettings{Initialization: camera.DeviceSettings{PowerLineFrequency: 60}}},
		{ID: "model", Scope: camera.EnrollmentScope{TenantID: "tenant"}, Match: Match{Manufacturer: "TP-Link", Model: "EasyCam C420"}, Settings: camera.PresetSettings{Video: camera.DesiredState{Width: 1920, Height: 1080, FPS: 30}}, Fallbacks: []camera.PresetSettings{{Video: camera.DesiredState{FPS: 25}}}},
		{ID: "serial", Scope: camera.EnrollmentScope{TenantID: "tenant"}, Match: Match{Manufacturer: "TP-Link", Serial: "serial-1"}, Settings: camera.PresetSettings{Video: camera.DesiredState{Bitrate: 2_048_000}}},
	}}
}

func testDevice() camera.DeviceHint {
	return camera.DeviceHint{ID: "camera-1", Manufacturer: "TP-Link", Model: "EasyCam C420", Hardware: "1.0", Firmware: "build-1", Serial: "serial-1", MAC: "A8-29-DC-C6-23-15", Evidence: []camera.Evidence{{Source: "onvif/device", Authenticated: true}}}
}

func TestConfigExportIsDetachedAndRetainsSourceRevisionAndFallbackPresence(t *testing.T) {
	config := testRules()
	config.Rules[0].Fallbacks = []camera.PresetSettings{}
	config.Rules[0].Settings.Preserve = []string{camera.PresetFieldGOP}
	config.Rules[2].Fallbacks[0].Preserve = []string{camera.PresetFieldBitrate}
	p, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ref := p.Reference()
	got := p.Config()
	if got.Revision != "v7" || got.ID != "office-presets" {
		t.Fatal("export must retain the source revision, not its computed fingerprint")
	}
	byID := func(c Config, id string) *Rule {
		for i := range c.Rules {
			if c.Rules[i].ID == id {
				return &c.Rules[i]
			}
		}
		return nil
	}
	if byID(got, "tenant").Fallbacks == nil || len(byID(got, "tenant").Fallbacks) != 0 || byID(got, "office").Fallbacks != nil {
		t.Fatal("export lost absent versus explicitly empty fallback overrides")
	}
	byID(got, "tenant").Settings.Preserve[0] = camera.PresetFieldFPS
	byID(got, "tenant").Required[0] = camera.PresetFieldFPS
	byID(got, "model").Fallbacks[0].Video.FPS = 1
	byID(got, "model").Fallbacks[0].Preserve[0] = camera.PresetFieldFPS
	got.Rules[0].ID = "changed"
	next := p.Config()
	if p.Reference() != ref || byID(next, "tenant").Settings.Preserve[0] != camera.PresetFieldGOP || byID(next, "tenant").Required[0] != camera.PresetFieldCodec || byID(next, "model").Fallbacks[0].Video.FPS != 25 || byID(next, "model").Fallbacks[0].Preserve[0] != camera.PresetFieldBitrate {
		t.Fatal("export aliases live provider configuration")
	}
	if (new(Provider)).Config().Rules != nil || (*Provider)(nil).Config().Rules != nil {
		t.Fatal("empty and nil providers must not invent source rules")
	}
}

func TestResolveHierarchyAndSnapshotIsolation(t *testing.T) {
	config := testRules()
	p, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	// Mutating the source configuration must not change this provider version.
	config.Rules[2].Settings.Video.FPS = 1
	config.Rules[2].Fallbacks[0].Video.FPS = 2
	config.Rules[0].Required[0] = camera.PresetFieldFPS
	device := testDevice()
	out, err := p.Resolve(context.Background(), testScope, device)
	if err != nil {
		t.Fatal(err)
	}
	if out.Ref.ID != "office-presets" || !strings.HasPrefix(out.Ref.Revision, "v7~") || len(out.Ref.Revision) != len("v7~")+64 || !slices.Equal(out.MatchedRules, []string{"tenant", "office", "model", "serial"}) {
		t.Fatalf("wrong snapshot identity: %+v", out)
	}
	if out.Requested.Initialization.PowerLineFrequency != 60 || out.Requested.Video.Codec != "h264" || out.Requested.Video.Width != 1920 || out.Requested.Video.FPS != 30 || out.Requested.Video.Bitrate != 2_048_000 {
		t.Fatalf("wrong resolved settings: %+v", out.Requested)
	}
	out.Fallbacks[0].Video.FPS = 3
	out.Required[0] = camera.PresetFieldFPS
	out.MatchedRules[0] = "changed"
	next, err := p.Resolve(context.Background(), testScope, device)
	if err != nil {
		t.Fatal(err)
	}
	if next.Fallbacks[0].Video.FPS != 25 || next.Required[0] != camera.PresetFieldCodec || next.MatchedRules[0] != "tenant" {
		t.Fatal("returned snapshot aliased provider state")
	}
	if len(device.Evidence) != 1 || !device.Evidence[0].Authenticated || device.ID != "camera-1" {
		t.Fatal("preset resolution changed device evidence or identity")
	}
}

func TestResolveDeviceOverridesAndExactFirmware(t *testing.T) {
	c := testRules()
	c.Rules = append(c.Rules,
		Rule{ID: "firmware", Scope: camera.EnrollmentScope{TenantID: "tenant"}, Match: Match{Manufacturer: "TP-Link", Model: "EasyCam C420", Hardware: "1.0", Firmware: "build-1"}, Settings: camera.PresetSettings{Video: camera.DesiredState{FPS: 25}}},
		Rule{ID: "inventory", Scope: camera.EnrollmentScope{TenantID: "tenant"}, Match: Match{DeviceID: "camera-1"}, Settings: camera.PresetSettings{Video: camera.DesiredState{Width: 1280, Height: 720}}, Fallbacks: []camera.PresetSettings{}},
	)
	p, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	out, err := p.Resolve(context.Background(), testScope, testDevice())
	if err != nil {
		t.Fatal(err)
	}
	if out.Requested.Video.FPS != 25 || out.Requested.Video.Width != 1280 || len(out.Fallbacks) != 0 || out.MatchedRules[len(out.MatchedRules)-1] != "inventory" {
		t.Fatalf("override precedence failed: %+v", out)
	}
	d := testDevice()
	d.Firmware = "build-2"
	out, err = p.Resolve(context.Background(), testScope, d)
	if err != nil {
		t.Fatal(err)
	}
	if out.Requested.Video.FPS != 30 || slices.Contains(out.MatchedRules, "firmware") {
		t.Fatal("firmware predicate matched another build")
	}
}

func TestScopedPreserveOverridesDefaultsWithoutAliasing(t *testing.T) {
	c := testRules()
	c.Rules = append(c.Rules, Rule{ID: "device", Scope: camera.EnrollmentScope{TenantID: "tenant"}, Match: Match{DeviceID: "camera-1"}, Settings: camera.PresetSettings{Preserve: []string{camera.PresetFieldFrequency}}, Fallbacks: []camera.PresetSettings{{Preserve: []string{camera.PresetFieldResolution}, Video: camera.DesiredState{FPS: 25}}}})
	p, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Rules[len(c.Rules)-1].Settings.Preserve[0] = camera.PresetFieldFPS
	c.Rules[len(c.Rules)-1].Fallbacks[0].Preserve[0] = camera.PresetFieldFPS
	out, err := p.Resolve(context.Background(), testScope, testDevice())
	if err != nil {
		t.Fatal(err)
	}
	if out.Requested.Initialization.PowerLineFrequency != 0 || out.Requested.Video.FPS != 30 || len(out.Requested.Preserve) != 0 {
		t.Fatalf("wrong materialized override: %+v", out.Requested)
	}
	candidates, err := out.Candidates()
	if err != nil {
		t.Fatal(err)
	}
	if candidates[1].Video.Width != 0 || candidates[1].Video.Height != 0 || candidates[1].Video.FPS != 25 {
		t.Fatalf("nested preserve patch not isolated: %+v", candidates[1])
	}
	out.Fallbacks[0].Preserve[0] = camera.PresetFieldFPS
	again, err := p.Resolve(context.Background(), testScope, testDevice())
	if err != nil {
		t.Fatal(err)
	}
	if again.Fallbacks[0].Preserve[0] != camera.PresetFieldResolution {
		t.Fatal("returned nested preserve slice mutated provider")
	}
}

func TestEqualSpecificityConflictIsIndependentOfFileOrder(t *testing.T) {
	c := testRules()
	c.Rules = append(c.Rules, Rule{ID: "conflicting-model", Scope: camera.EnrollmentScope{TenantID: "tenant"}, Match: Match{Manufacturer: "TP-Link", Model: "EasyCam C420"}, Settings: camera.PresetSettings{Video: camera.DesiredState{FPS: 15}}})
	for range 2 {
		p, err := New(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Resolve(context.Background(), testScope, testDevice()); !errors.Is(err, camera.ErrConflict) {
			t.Fatalf("ambiguous preset rules: %v", err)
		}
		slices.Reverse(c.Rules)
	}
}

func TestSnapshotFingerprintTracksPolicyAndResolvedMatch(t *testing.T) {
	resolve := func(c Config, d camera.DeviceHint) camera.DevicePreset {
		t.Helper()
		p, err := New(c)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := p.Resolve(context.Background(), testScope, d)
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	first := resolve(testRules(), testDevice())
	reordered := testRules()
	slices.Reverse(reordered.Rules)
	if other := resolve(reordered, testDevice()); other.Ref != first.Ref {
		t.Fatal("file order changed a semantic snapshot revision")
	}
	edited := testRules()
	edited.Rules[2].Settings.Video.FPS = 20
	if other := resolve(edited, testDevice()); other.Ref == first.Ref {
		t.Fatal("edited content reused a revision without a manual version bump")
	}
	d := testDevice()
	d.Serial = "another-camera"
	if other := resolve(testRules(), d); other.Ref == first.Ref {
		t.Fatal("different resolved preset reused a snapshot reference")
	}

	// An explicitly empty fallback list clears inherited candidates; absence
	// retains them. Omitempty must not erase this distinction in the digest.
	withOverride := testRules()
	withOverride.Rules = append(withOverride.Rules, Rule{ID: "inventory", Scope: camera.EnrollmentScope{TenantID: "tenant"}, Match: Match{DeviceID: "camera-1"}})
	inherited := resolve(withOverride, testDevice())
	withOverride.Rules[len(withOverride.Rules)-1].Fallbacks = []camera.PresetSettings{}
	cleared := resolve(withOverride, testDevice())
	if inherited.Ref == cleared.Ref || len(inherited.Fallbacks) != 1 || len(cleared.Fallbacks) != 0 {
		t.Fatal("absent and cleared fallbacks have the same reference or policy")
	}

	ordered := testRules()
	ordered.Rules[2].Fallbacks = append(ordered.Rules[2].Fallbacks, camera.PresetSettings{Video: camera.DesiredState{FPS: 15}})
	forward := resolve(ordered, testDevice())
	slices.Reverse(ordered.Rules[2].Fallbacks)
	if reverse := resolve(ordered, testDevice()); reverse.Ref == forward.Ref {
		t.Fatal("candidate preference order was omitted from revision")
	}
}

func TestMACNormalizationAndNetworkScope(t *testing.T) {
	c := Config{ID: "mac-presets", Revision: "v1", Rules: []Rule{{ID: "camera", Scope: camera.EnrollmentScope{TenantID: "tenant", NetworkID: "lan"}, Match: Match{MAC: "a829.dcc6.2315"}, Settings: camera.PresetSettings{Video: camera.DesiredState{FPS: 25}}}}}
	p, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"A8:29:DC:C6:23:15", "a8-29-dc-c6-23-15", "a829.dcc6.2315"} {
		d := testDevice()
		d.MAC = raw
		if _, err := p.Resolve(context.Background(), testScope, d); err != nil {
			t.Fatalf("normalization %q: %v", raw, err)
		}
	}
	otherNetwork := testScope
	otherNetwork.NetworkID = "other"
	if _, err := p.Resolve(context.Background(), otherNetwork, testDevice()); !errors.Is(err, camera.ErrPresetNotFound) {
		t.Fatalf("MAC escaped network scope: %v", err)
	}
	c.Rules[0].Scope.NetworkID = ""
	if _, err := New(c); !errors.Is(err, camera.ErrInvalid) {
		t.Fatalf("unscoped MAC rule: %v", err)
	}
}

func TestScopeIsolationAndContext(t *testing.T) {
	p, err := New(testRules())
	if err != nil {
		t.Fatal(err)
	}
	foreign := testScope
	foreign.TenantID = "other"
	if _, err := p.Resolve(context.Background(), foreign, testDevice()); !errors.Is(err, camera.ErrPresetNotFound) {
		t.Fatalf("foreign tenant matched preset: %v", err)
	}
	empty := testScope
	empty.NetworkID = ""
	if _, err := p.Resolve(context.Background(), empty, testDevice()); !errors.Is(err, camera.ErrInvalid) {
		t.Fatalf("incomplete scope accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Resolve(ctx, testScope, testDevice()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled query: %v", err)
	}
}

func TestDecodeYAMLAndJSONStrictly(t *testing.T) {
	yamlDoc := `id: office
revision: v1
rules:
  - id: c420
    scope:
      tenant_id: tenant
    match:
      model: EasyCam C420
    settings:
      initialization:
        power_line_frequency: 60
      video:
        profile_token: main
        codec: h264
        width: 1920
        height: 1080
        fps: 30
    fallbacks:
      - video:
          fps: 25
    required: [video.codec]
`
	jsonDoc := `{"id":"office","revision":"v1","rules":[{"id":"c420","scope":{"tenant_id":"tenant"},"match":{"model":"EasyCam C420"},"settings":{"initialization":{"power_line_frequency":60},"video":{"profile_token":"main","codec":"h264","width":1920,"height":1080,"fps":30}},"fallbacks":[{"video":{"fps":25}}],"required":["video.codec"]}]}`
	for _, doc := range []string{yamlDoc, jsonDoc} {
		p, err := Decode(strings.NewReader(doc))
		if err != nil {
			t.Fatal(err)
		}
		out, err := p.Resolve(context.Background(), testScope, testDevice())
		if err != nil {
			t.Fatal(err)
		}
		candidates, err := out.Candidates()
		if err != nil || len(candidates) != 2 || candidates[0].Video.ProfileToken != "main" || candidates[1].Video.FPS != 25 {
			t.Fatalf("decoded wrong settings: %+v, %v", candidates, err)
		}
	}
	for _, doc := range []string{yamlDoc + "unknown_field: true\n", yamlDoc + "---\nid: second\n", strings.Replace(yamlDoc, "fps: 30", "fsp: 30", 1), strings.Repeat(" ", maxFileBytes+1)} {
		if _, err := Decode(strings.NewReader(doc)); !errors.Is(err, camera.ErrInvalid) {
			t.Fatalf("accepted invalid document: %v", err)
		}
	}
	path := filepath.Join(t.TempDir(), "presets.yaml")
	if err := os.WriteFile(path, []byte(yamlDoc), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
}

func TestRuleValidationAndRequiredFallbackEnforcement(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Rules[0].Scope.TenantID = "" },
		func(c *Config) { c.Rules[1].ID = c.Rules[0].ID },
		func(c *Config) { c.Rules[0].Required = []string{"unrecognized"} },
		func(c *Config) { c.Rules[0].Match.MAC = "a8:29:dc:c6:23:15:22:33"; c.Rules[0].Scope.NetworkID = "lan" },
	} {
		c := testRules()
		mutate(&c)
		if _, err := New(c); !errors.Is(err, camera.ErrInvalid) {
			t.Fatalf("accepted invalid rule: %v", err)
		}
	}
	c := testRules()
	c.Rules[2].Required = []string{camera.PresetFieldFPS}
	p, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Resolve(context.Background(), testScope, testDevice()); !errors.Is(err, camera.ErrInvalid) {
		t.Fatalf("fallback relaxed required field: %v", err)
	}
}
