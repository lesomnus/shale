package tplink

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/lesomnus/shale/camera"
)

func firmwareHint(f *initializationFixture, firmware string) camera.DeviceHint {
	hint := testHint()
	hint.Firmware = firmware
	hint.Endpoints = []camera.Endpoint{{URL: f.server.URL + "/onvif/device_service"}}
	return hint
}

func TestUpgradeFirmwareRepeatsLocalUpdate(t *testing.T) {
	f := newInitializationFixture(t)
	d, lease := f.driver(), f.lease()
	defer lease.Close()
	image := bytes.Repeat([]byte{0x5a}, 4096)
	err := d.UpgradeFirmware(context.Background(), firmwareHint(f, "1.2.0 Build 251020 Rel.55235n"), lease, camera.FirmwareFile{Name: "dir/Sx25_up_boot_signed.bin", SHA256: "unused", Data: image})
	if err != nil {
		t.Fatal(err)
	}
	if f.upgradeAnnounced != 1 || len(f.uploads) != 1 || !bytes.Equal(f.uploads[0], image) || f.uploadNames[0] != "Sx25_up_boot_signed.bin" {
		t.Fatalf("announced=%d uploads=%d names=%q", f.upgradeAnnounced, len(f.uploads), f.uploadNames)
	}
	// The session is closed afterwards, as the UI does; nothing else is sent.
	if f.logins != 1 || f.logouts != 1 || f.unexpected != 0 || f.writes != 0 {
		t.Fatalf("logins=%d logouts=%d unexpected=%d writes=%d", f.logins, f.logouts, f.unexpected, f.writes)
	}
}

func TestUpgradeFirmwareRefusals(t *testing.T) {
	image := camera.FirmwareFile{Name: "fw.bin", Data: []byte("image")}
	t.Run("upload refused", func(t *testing.T) {
		f := newInitializationFixture(t)
		f.uploadReply = `{"error_code":-1}`
		d, lease := f.driver(), f.lease()
		defer lease.Close()
		err := d.UpgradeFirmware(context.Background(), firmwareHint(f, "1.2.0 Build 251020 Rel.55235n"), lease, image)
		if !errors.Is(err, errProtocol) || len(f.uploads) != 1 {
			t.Fatalf("err=%v uploads=%d", err, len(f.uploads))
		}
		// Unlike the camera UI, Shale does not reboot after a refused upload.
		if f.unexpected != 0 {
			t.Fatalf("unexpected=%d", f.unexpected)
		}
	})
	t.Run("unexpected upload path", func(t *testing.T) {
		f := newInitializationFixture(t)
		f.upgradeURL = "%2fadmin%2fsystem%2fsomething_else"
		d, lease := f.driver(), f.lease()
		defer lease.Close()
		err := d.UpgradeFirmware(context.Background(), firmwareHint(f, "1.2.0 Build 251020 Rel.55235n"), lease, image)
		if !errors.Is(err, errProtocol) || len(f.uploads) != 0 {
			t.Fatalf("err=%v uploads=%d", err, len(f.uploads))
		}
	})
	t.Run("unknown firmware", func(t *testing.T) {
		f := newInitializationFixture(t)
		d, lease := f.driver(), f.lease()
		defer lease.Close()
		err := d.UpgradeFirmware(context.Background(), firmwareHint(f, "1.1.9 Build 240101"), lease, image)
		if !errors.Is(err, camera.ErrUnsupported) || f.webCalls != 0 {
			t.Fatalf("err=%v web calls=%d", err, f.webCalls)
		}
	})
	t.Run("empty image", func(t *testing.T) {
		f := newInitializationFixture(t)
		d, lease := f.driver(), f.lease()
		defer lease.Close()
		err := d.UpgradeFirmware(context.Background(), firmwareHint(f, "1.2.0 Build 251020 Rel.55235n"), lease, camera.FirmwareFile{})
		if !errors.Is(err, camera.ErrInvalid) || f.webCalls != 0 {
			t.Fatalf("err=%v web calls=%d", err, f.webCalls)
		}
	})
	t.Run("wrong password", func(t *testing.T) {
		f := newInitializationFixture(t)
		f.loginRefused = true
		d, lease := f.driver(), f.lease()
		defer lease.Close()
		err := d.UpgradeFirmware(context.Background(), firmwareHint(f, "1.2.0 Build 251020 Rel.55235n"), lease, image)
		if !errors.Is(err, camera.ErrAuthRequired) || f.upgradeAnnounced != 0 {
			t.Fatalf("err=%v announced=%d", err, f.upgradeAnnounced)
		}
	})
}

func TestVerifyLoginWritesNothing(t *testing.T) {
	f := newInitializationFixture(t)
	d, lease := f.driver(), f.lease()
	defer lease.Close()
	if err := d.VerifyLogin(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	if f.logins != 1 || f.logouts != 1 || f.writes != 0 || f.unexpected != 0 || f.upgradeAnnounced != 0 {
		t.Fatalf("logins=%d logouts=%d writes=%d unexpected=%d", f.logins, f.logouts, f.writes, f.unexpected)
	}
	f.loginRefused = true
	if err := d.VerifyLogin(context.Background(), lease); !errors.Is(err, camera.ErrAuthRequired) {
		t.Fatalf("refused login: %v", err)
	}
}

func TestFirmwareName(t *testing.T) {
	for in, want := range map[string]string{"": "firmware.bin", "a/b/c.bin": "c.bin", `a\b.bin`: "b.bin", "x\"y.bin": "firmware.bin", "/": "firmware.bin"} {
		if got := firmwareName(in); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
}
