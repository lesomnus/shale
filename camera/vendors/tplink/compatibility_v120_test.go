package tplink

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/lesomnus/shale/camera"
)

func v120Hint() camera.DeviceHint {
	h := testHint()
	h.Firmware = "1.2.0 Build251020 Rel.55235n"
	return h
}

func TestC420V120FirmwareBoundary(t *testing.T) {
	for _, firmware := range []string{"1.2.0 Build251020 Rel.55235n", "1.2.0 Build 251020 Rel.55235n", "1.2.0 Build251020", "1.2.0 Build 251020"} {
		h := v120Hint()
		h.Firmware = firmware
		if !Match(h) {
			t.Errorf("qualified firmware %q was not matched", firmware)
		}
	}
	for _, firmware := range []string{"1.2.0 Build251021 Rel.55235n", "1.2.0 Build251020 Rel.unknown", "1.2.0 Build251020-extra", "1.2.0 Build2510200", "1.2.1 Build251020 Rel.55235n"} {
		h := v120Hint()
		h.Firmware = firmware
		if Match(h) {
			t.Errorf("unqualified firmware %q was matched", firmware)
		}
	}
	for _, field := range []string{"manufacturer", "model", "hardware"} {
		h := v120Hint()
		switch field {
		case "manufacturer":
			h.Manufacturer = "other"
		case "model":
			h.Model = "C420 other"
		case "hardware":
			h.Hardware = "2.0"
		}
		if Match(h) {
			t.Errorf("unqualified %s was matched", field)
		}
	}
}

func TestC420V120NormalChallengeAndThirtyFPS(t *testing.T) {
	f := newInitializationFixture(t)
	f.challengeCode, f.firmware = -40407, v120Hint().Firmware
	d, lease := f.driver(), f.lease()
	defer lease.Close()
	h := v120Hint()
	h.Endpoints = []camera.Endpoint{{URL: f.server.URL + "/onvif/device_service"}}
	i, err := d.Inspect(t.Context(), h, lease)
	if err != nil {
		t.Fatal(err)
	}
	desired := camera.DesiredState{ProfileToken: "main", Codec: "h264", Width: 1920, Height: 1080, FPS: 30}
	if _, err := d.Plan(t.Context(), i, desired); !errors.Is(err, camera.ErrUnsupported) {
		t.Fatalf("factory frequency admitted 30fps: %v", err)
	}
	p, err := d.PlanInitialization(t.Context(), h, lease, camera.DeviceSettings{PowerLineFrequency: 60})
	if err != nil {
		t.Fatal(err)
	}
	if p.Before.PowerLineFrequency != 50 || f.writes != 0 {
		t.Fatal("read-only plan changed the device")
	}
	if _, err = d.ApplyInitialization(t.Context(), h, lease, p); err != nil {
		t.Fatal(err)
	}
	i, err = d.Inspect(t.Context(), h, lease)
	if err != nil {
		t.Fatal(err)
	}
	videoPlan, err := d.Plan(t.Context(), i, desired)
	if err != nil {
		t.Fatalf("fresh 60Hz options did not permit 30fps: %v", err)
	}
	actual, err := d.Apply(t.Context(), h, lease, videoPlan)
	if err != nil || len(actual.Profiles) != 1 || actual.Profiles[0].FPS != 30 || actual.Profiles[0].Width != 1920 || actual.Profiles[0].Height != 1080 || actual.Profiles[0].Codec != "h264" || actual.Profiles[0].Bitrate != 4_096_000 || actual.Profiles[0].GOP != 25 {
		t.Fatalf("encoder configuration was not read back at 1080p30: %v", err)
	}
	if f.writes != 1 || f.videoWrites != 1 || f.logins != 2 || f.logouts != 2 || f.unexpected != 0 {
		t.Fatal("unexpected mutation or missing cleanup")
	}
}

func TestExplicitAndSharedProfilesKeepFirmwareBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, profile, firmware string
		allowed                 bool
	}{
		{"v120-exact", ProfileC420V120, v120Hint().Firmware, true},
		{"v122-exact", ProfileC420V122, testHint().Firmware, true},
		{"v120-not-v122", ProfileC420V120, testHint().Firmware, false},
		{"v122-not-v120", ProfileC420V122, v120Hint().Firmware, false},
		{"shared-v120", ProfileC420Known, v120Hint().Firmware, true},
		{"shared-v122", ProfileC420Known, testHint().Firmware, true},
		{"shared-unknown", ProfileC420Known, "1.3.0 Build270101", false},
		{"shared-unknown-release", ProfileC420Known, "1.2.0 Build251020 Rel.unknown", false},
		{"unknown-profile", "tplink-any", v120Hint().Firmware, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newInitializationFixture(t)
			d, lease := f.driver(), f.lease()
			defer lease.Close()
			d.CompatibilityProfile = tc.profile
			h := testHint()
			h.Firmware = tc.firmware
			p, err := d.PlanInitialization(t.Context(), h, lease, camera.DeviceSettings{PowerLineFrequency: 60})
			if tc.allowed {
				if err != nil || p.Before.PowerLineFrequency != 50 {
					t.Fatalf("qualified profile was denied: %v", err)
				}
				return
			}
			if !errors.Is(err, camera.ErrUnsupported) || f.webCalls != 0 || f.logins != 0 || f.writes != 0 {
				t.Fatalf("firmware/profile boundary was bypassed: %v", err)
			}
			_, err = d.ApplyInitialization(t.Context(), h, lease, camera.InitializationPlan{Before: camera.DeviceSettings{PowerLineFrequency: 50}, After: camera.DeviceSettings{PowerLineFrequency: 60}, Revision: frequencyPlanRevision})
			if !errors.Is(err, camera.ErrUnsupported) || f.webCalls != 0 || f.logins != 0 || f.writes != 0 {
				t.Fatalf("apply bypassed firmware/profile boundary: %v", err)
			}
		})
	}
}

func TestAnonymousActivationUsesExplicitKnownProtocol(t *testing.T) {
	for _, profile := range []string{ProfileC420V120, ProfileC420V122, ProfileC420Known} {
		t.Run(profile, func(t *testing.T) {
			f := newActivationFixture(t)
			d, lease := testDriver(f), initialLease(true)
			defer lease.Close()
			d.CompatibilityProfile = profile
			if err := d.Activate(t.Context(), camera.DeviceHint{}, lease); err != nil || f.writes != 1 {
				t.Fatalf("explicit first-password protocol failed: %v", err)
			}
			if err := d.Activate(t.Context(), camera.DeviceHint{}, lease); err != nil || f.writes != 1 {
				t.Fatalf("initialized account was rewritten: %v", err)
			}
		})
	}
}

func TestNormalClientChallengeUsesOnlyQualifiedLoginStates(t *testing.T) {
	for _, code := range []int{-40401, -40407} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			f := newInitializationFixture(t)
			f.challengeCode = code
			d, lease := f.driver(), f.lease()
			defer lease.Close()
			if _, err := d.PlanInitialization(t.Context(), testHint(), lease, camera.DeviceSettings{PowerLineFrequency: 60}); err != nil || f.logins != 1 || f.logouts != 1 || f.writes != 0 {
				t.Fatalf("qualified login challenge failed: %v", err)
			}
		})
	}
}

func TestNormalChallengeIsNotAnyActivationReadyState(t *testing.T) {
	for _, code := range []int{-40405, -40404, -40408, -40414, 0, -99999} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			f := newInitializationFixture(t)
			f.challengeCode = code
			d, lease := f.driver(), f.lease()
			defer lease.Close()
			_, err := d.PlanInitialization(t.Context(), testHint(), lease, camera.DeviceSettings{PowerLineFrequency: 60})
			if err == nil || f.logins != 0 || f.writes != 0 {
				t.Fatalf("non-login state %d reached account or settings", code)
			}
			if strings.Contains(err.Error(), f.password) {
				t.Fatal("credential leaked")
			}
		})
	}
}

func TestObservedNormalChallengeRequiresCompleteProtocol(t *testing.T) {
	for _, change := range []string{"http-status", "outer-code", "type", "password-type", "nonce", "key"} {
		t.Run(change, func(t *testing.T) {
			f := newInitializationFixture(t)
			f.challengeCode = -40407
			switch change {
			case "http-status":
				f.challengeStatus = 200
			case "outer-code":
				f.challengeOuter = 0
			case "type":
				f.currentKind = "9"
			case "password-type":
				f.passwordType = "plain"
			case "nonce":
				f.emptyNonce = true
			case "key":
				f.badKey = true
			}
			d, lease := f.driver(), f.lease()
			defer lease.Close()
			_, err := d.PlanInitialization(t.Context(), testHint(), lease, camera.DeviceSettings{PowerLineFrequency: 60})
			if err == nil || f.logins != 0 || f.writes != 0 || errors.Is(err, camera.ErrUnsupported) {
				t.Fatalf("incomplete challenge did not fail closed: %v", err)
			}
		})
	}
}

func TestVendorChallengeDoesNotReuseRetiredConnection(t *testing.T) {
	// Deterministically model a challenge connection which cannot service a
	// later request. No socket-close timing or physical-firmware claim is used.
	f := newInitializationFixture(t)
	f.retireChallengeConnection = true
	d, lease := f.driver(), f.lease()
	defer lease.Close()
	if _, err := d.PlanInitialization(context.Background(), testHint(), lease, camera.DeviceSettings{PowerLineFrequency: 60}); err != nil {
		t.Fatalf("challenge connection was reused: %v", err)
	}
	if f.reusedRetired != 0 || f.logins != 1 || f.logouts != 1 || f.writes != 0 {
		t.Fatal("retired connection or unexpected camera operation")
	}
}
