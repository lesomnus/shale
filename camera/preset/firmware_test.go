package preset

import (
	"errors"
	"strings"
	"testing"

	"github.com/lesomnus/shale/camera"
)

const testDigest = "9460f6c63c06573dbb302ec4b4ee482991caaff817718b919109821591ea669a"

func testUpgrade() *camera.FirmwareUpgrade {
	return &camera.FirmwareUpgrade{From: []string{"1.2.0 Build 251020 Rel.55235n"}, To: "1.2.2 Build 260624 Rel.39352n", Image: camera.FirmwareImage{URL: "https://static.example/c420.zip", SHA256: testDigest}}
}

func TestFirmwareRequirementIsResolvedFromTheMostSpecificRule(t *testing.T) {
	config := testRules()
	config.Rules[2].Firmware = testUpgrade()
	p, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Resolve(t.Context(), testScope, testDevice())
	if err != nil {
		t.Fatal(err)
	}
	if got.Firmware == nil || got.Firmware.To != "1.2.2 Build 260624 Rel.39352n" || got.Firmware.Image.SHA256 != testDigest {
		t.Fatalf("firmware: %+v", got.Firmware)
	}
	got.Firmware.From[0] = "changed"
	again, _ := p.Resolve(t.Context(), testScope, testDevice())
	if again.Firmware.From[0] != "1.2.0 Build 251020 Rel.55235n" {
		t.Fatal("resolved firmware shares provider state")
	}

	// A more specific rule replaces the requirement as a whole.
	override := testUpgrade()
	override.To = "1.2.3"
	config.Rules[3].Firmware = override
	p, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = p.Resolve(t.Context(), testScope, testDevice())
	if got.Firmware.To != "1.2.3" {
		t.Fatalf("override: %+v", got.Firmware)
	}
}

func TestPresetWithoutFirmwareKeepsItsReference(t *testing.T) {
	// Presets published before the field existed must keep their fingerprints.
	before, err := New(testRules())
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := before.Resolve(t.Context(), testScope, testDevice())
	if err != nil || resolved.Firmware != nil {
		t.Fatal(resolved.Firmware, err)
	}
	if !strings.HasPrefix(before.Reference().Revision, "v7~") {
		t.Fatal(before.Reference())
	}
	config := testRules()
	config.Rules[2].Firmware = testUpgrade()
	with, _ := New(config)
	if with.Reference() == before.Reference() {
		t.Fatal("a firmware requirement must change the ruleset reference")
	}
}

func TestEqualRulesWithDifferentFirmwareConflict(t *testing.T) {
	config := testRules()
	twin := config.Rules[2]
	twin.ID = "model-twin"
	twin.Firmware = testUpgrade()
	config.Rules = append(config.Rules, twin)
	p, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Resolve(t.Context(), testScope, testDevice()); !errors.Is(err, camera.ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
}

func TestInvalidFirmwareRequirements(t *testing.T) {
	for name, edit := range map[string]func(*camera.FirmwareUpgrade){
		"no source":         func(u *camera.FirmwareUpgrade) { u.From = nil },
		"source is target":  func(u *camera.FirmwareUpgrade) { u.From = append(u.From, u.To) },
		"duplicate source":  func(u *camera.FirmwareUpgrade) { u.From = append(u.From, u.From[0]) },
		"no target":         func(u *camera.FirmwareUpgrade) { u.To = "" },
		"padded target":     func(u *camera.FirmwareUpgrade) { u.To = " 1.2.2" },
		"cleartext image":   func(u *camera.FirmwareUpgrade) { u.Image.URL = "http://static.example/c420.zip" },
		"credentials":       func(u *camera.FirmwareUpgrade) { u.Image.URL = "https://user:pw@static.example/c420.zip" },
		"uppercase digest":  func(u *camera.FirmwareUpgrade) { u.Image.SHA256 = strings.ToUpper(testDigest) },
		"short digest":      func(u *camera.FirmwareUpgrade) { u.Image.SHA256 = testDigest[:10] },
		"control character": func(u *camera.FirmwareUpgrade) { u.From[0] = "1.2.0\n" },
	} {
		upgrade := testUpgrade()
		edit(upgrade)
		config := testRules()
		config.Rules[2].Firmware = upgrade
		if _, err := New(config); !errors.Is(err, camera.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestDecodeFirmwareYAML(t *testing.T) {
	p, err := Decode(strings.NewReader(`
id: c420
revision: v1
rules:
  - id: model
    scope: {tenant_id: tenant}
    match: {manufacturer: TP-Link, model: EasyCam C420, hardware: "1.0"}
    settings: {video: {codec: h264}}
    firmware:
      from: ["1.2.0 Build 251020 Rel.55235n"]
      to: "1.2.2 Build 260624 Rel.39352n"
      image:
        url: "https://static.example/c420.zip"
        sha256: ` + testDigest + `
`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Resolve(t.Context(), testScope, testDevice())
	if err != nil || !got.Firmware.Applies("1.2.0 Build 251020 Rel.55235n") || got.Firmware.Applies("1.2.2 Build 260624 Rel.39352n") {
		t.Fatal(got.Firmware, err)
	}
}
