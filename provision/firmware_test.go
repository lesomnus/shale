package provision_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/provision"
	"github.com/stretchr/testify/require"
)

const (
	fwOld    = "1.2.0 Build 251020 Rel.55235n"
	fwNew    = "1.2.2 Build 260624 Rel.39352n"
	fwDigest = "9460f6c63c06573dbb302ec4b4ee482991caaff817718b919109821591ea669a"
	oldKey   = "sha256:" + "1111111111111111111111111111111111111111111111111111111111111111"
	newKey   = "sha256:" + "2222222222222222222222222222222222222222222222222222222222222222"
)

var errRebooting = errors.New("fixture camera is rebooting")

// firmwareCamera reboots into its pending build after an upload. Until then it
// fails a number of inspections, then answers with the new build and key.
type firmwareCamera struct {
	*device
	firmware, mac, serial string
	key                   string
	pending, pendingKey   string
	pendingSerial         string
	rebootPolls           int
	uploads, confirms     int
	activationChecks      int
	interruptUpload       bool
	ignoreUpload          bool
	files                 []camera.FirmwareFile
}

func (c *firmwareCamera) Activation(ctx context.Context, h camera.DeviceHint) (camera.ActivationState, error) {
	c.activationChecks++
	return c.device.Activation(ctx, h)
}

func (c *firmwareCamera) Inspect(ctx context.Context, h camera.DeviceHint, l camera.CredentialLease) (camera.Inspection, error) {
	if c.pending != "" {
		if c.rebootPolls > 0 {
			c.rebootPolls--
			return camera.Inspection{}, errRebooting
		}
		c.firmware, c.key, c.pending = c.pending, c.pendingKey, ""
		if c.pendingSerial != "" {
			c.serial = c.pendingSerial
		}
	}
	i, err := c.device.Inspect(ctx, h, l)
	if err != nil {
		return i, err
	}
	i.Device.Manufacturer, i.Device.Model = "TP-Link", "EasyCam C420"
	i.Device.Firmware, i.Device.MAC, i.Device.Serial = c.firmware, c.mac, c.serial
	i.Device.Evidence = []camera.Evidence{{Source: "fixture", At: time.Now().UTC(), Firmware: c.firmware, Authenticated: true}}
	return i, nil
}

func (c *firmwareCamera) UpgradeFirmware(_ context.Context, h camera.DeviceHint, l camera.CredentialLease, f camera.FirmwareFile) error {
	if err := l.Use(context.Background(), func(camera.CredentialMaterial) error { return nil }); err != nil {
		return err
	}
	if h.Firmware != c.firmware {
		return camera.ErrConflict
	}
	c.uploads++
	c.files = append(c.files, f)
	if !c.ignoreUpload {
		c.pending, c.rebootPolls = fwNew, 3
	}
	if c.interruptUpload {
		return context.Canceled
	}
	return nil
}

func (c *firmwareCamera) ConfirmWebIdentity(_ context.Context, h camera.DeviceHint, l camera.CredentialLease) (camera.DeviceTLSIdentity, error) {
	if err := l.Use(context.Background(), func(camera.CredentialMaterial) error { return nil }); err != nil {
		return camera.DeviceTLSIdentity{}, err
	}
	c.confirms++
	return camera.DeviceTLSIdentity{DeviceID: string(h.ID), SPKISHA256: c.key}, nil
}

type images struct{ fetches int }

func (i *images) Fetch(_ context.Context, image camera.FirmwareImage) (camera.FirmwareFile, error) {
	i.fetches++
	return camera.FirmwareFile{Name: "fw.bin", SHA256: image.SHA256, Data: []byte("image")}, nil
}

type resolved struct{ firmware []string }

func firmwareEngine(t *testing.T) (provision.Request, *firmwareCamera, *images, *resolved, *publisher, *provision.Engine) {
	t.Helper()
	q, d, p, e, _ := fixture(t)
	d.ready = true
	video := q.Desired
	q.Desired = camera.DesiredState{}
	q.UsePreset = true
	q.Bootstrap = &camera.BootstrapSettings{CompatibilityProfile: "fixture-v1", WebScheme: "https", WebPort: 443}
	q.DeviceTLSIdentity = &camera.DeviceTLSIdentity{DeviceID: string(q.Device.ID), SPKISHA256: oldKey}
	c := &firmwareCamera{device: d, firmware: fwOld, mac: "a8:29:dc:c6:26:f3", serial: "A3669F75EA80B92A2", key: oldKey, pendingKey: newKey}
	e.Inspector, e.Configurator, e.Bootstrapper = c, c, c
	e.Configurator = struct {
		camera.VideoConfigurator
	}{d}
	e.Firmware = c
	source := &images{}
	e.Images = source
	e.FirmwareReboot, e.FirmwarePoll = 2*time.Second, time.Millisecond
	seen := &resolved{}
	e.Presets = presetProvider(func(_ context.Context, _ camera.EnrollmentScope, h camera.DeviceHint) (camera.DevicePreset, error) {
		seen.firmware = append(seen.firmware, h.Firmware)
		preset := camera.DevicePreset{Ref: camera.PresetRef{ID: "c420", Revision: "1"}, MatchedRules: []string{"model"}, Requested: camera.PresetSettings{Video: video}, Firmware: &camera.FirmwareUpgrade{From: []string{fwOld}, To: fwNew, Image: camera.FirmwareImage{URL: "https://images.example/c420.zip", SHA256: fwDigest}}}
		if h.Firmware == fwNew {
			preset.MatchedRules = append(preset.MatchedRules, "model-v122")
		}
		return preset, nil
	})
	return q, c, source, seen, p, e
}

func codes(r provision.Run) []string {
	out := []string{}
	for _, e := range r.Events {
		out = append(out, e.Code)
	}
	return out
}

func TestFirmwareUpgradeRunsBeforeThePresetIsResolved(t *testing.T) {
	q, c, source, seen, p, e := firmwareEngine(t)
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, 1, c.uploads)
	require.Equal(t, 1, source.fetches)
	require.Equal(t, fwDigest, c.files[0].SHA256)
	require.Equal(t, 1, c.confirms)
	require.Equal(t, 1, p.deploys)

	require.Equal(t, &provision.FirmwareState{From: fwOld, To: fwNew, ImageSHA256: fwDigest, Complete: true, TLSIdentity: &camera.DeviceTLSIdentity{DeviceID: string(q.Device.ID), SPKISHA256: newKey}}, r.Firmware)
	// The pinned preset is the one resolved for the upgraded build.
	require.Equal(t, []string{"model", "model-v122"}, r.Preset.MatchedRules)
	require.Equal(t, []string{fwOld, fwNew}, seen.firmware, "resolved once per build")
	require.Equal(t, fwNew, r.Inspection.Device.Firmware)
	// The job's approved pin itself is never rewritten.
	require.Equal(t, oldKey, r.Request.DeviceTLSIdentity.SPKISHA256)

	events := codes(r)
	started, upgraded, pinned := slices.Index(events, "firmware_upgrade_started"), slices.Index(events, "firmware_upgraded"), slices.Index(events, "preset_resolved")
	require.True(t, 0 <= started && started < upgraded && upgraded < pinned, events)
}

func TestFirmwareUpgradeLeavesOtherBuildsAlone(t *testing.T) {
	for _, build := range []string{fwNew, "1.1.0 Build 240101 Rel.1n", ""} {
		t.Run(build, func(t *testing.T) {
			q, c, source, seen, _, e := firmwareEngine(t)
			c.firmware = build
			r, err := e.Execute(t.Context(), q, true)
			require.NoError(t, err)
			require.Equal(t, provision.Active, r.Stage)
			require.Equal(t, []string{build}, seen.firmware, "the firmware check's resolution is the one pinned")
			require.Zero(t, c.uploads)
			require.Zero(t, source.fetches)
			require.Nil(t, r.Firmware)
		})
	}
}

func TestFirmwareUpgradeResumesWithoutWritingAgain(t *testing.T) {
	q, c, source, _, _, e := firmwareEngine(t)
	c.interruptUpload = true
	first, err := e.Execute(t.Context(), q, true)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, provision.Pending, first.Stage)
	require.NotNil(t, first.Firmware)
	require.False(t, first.Firmware.Complete)
	require.Equal(t, fwOld, first.Inspection.Device.Firmware)

	checks := c.activationChecks
	c.interruptUpload = false
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.Equal(t, provision.Active, r.Stage)
	require.Equal(t, 1, c.uploads)
	require.Equal(t, 1, source.fetches)
	// The rotated web key would refuse an activation check made with the old pin.
	require.Equal(t, checks, c.activationChecks)
	require.True(t, r.Firmware.Complete)
	require.Equal(t, newKey, r.Firmware.TLSIdentity.SPKISHA256)
}

func TestFirmwareUpgradeKeepsAnUnchangedKey(t *testing.T) {
	q, c, _, _, _, e := firmwareEngine(t)
	c.pendingKey = oldKey
	r, err := e.Execute(t.Context(), q, true)
	require.NoError(t, err)
	require.True(t, r.Firmware.Complete)
	require.Nil(t, r.Firmware.TLSIdentity)
}

func TestFirmwareUpgradeRequiresTheRecordedDevice(t *testing.T) {
	q, c, _, _, p, e := firmwareEngine(t)
	c.pendingSerial = "SOMEONE-ELSE"
	r, err := e.Execute(t.Context(), q, true)
	require.ErrorIs(t, err, camera.ErrAmbiguousIdentity)
	require.Equal(t, provision.WaitingInput, r.Stage)
	require.Zero(t, c.confirms)
	require.False(t, r.Firmware.Complete)
	require.Nil(t, r.Preset)
	require.Zero(t, p.deploys)
}

func TestFirmwareUpgradeNotApplied(t *testing.T) {
	q, c, _, _, _, e := firmwareEngine(t)
	c.ignoreUpload = true
	e.FirmwareReboot = 20 * time.Millisecond
	r, err := e.Execute(t.Context(), q, true)
	require.ErrorIs(t, err, provision.ErrFirmwareNotApplied)
	require.Equal(t, provision.Failed, r.Stage)
	require.Equal(t, "firmware_not_applied", r.Events[len(r.Events)-1].Code)
	require.Zero(t, c.confirms)
}

func TestFirmwareUpgradePlanOnly(t *testing.T) {
	q, c, source, _, _, e := firmwareEngine(t)
	r, err := e.Execute(t.Context(), q, false)
	require.NoError(t, err)
	require.Equal(t, provision.Planned, r.Stage)
	require.Equal(t, "firmware_upgrade_planned", r.Events[len(r.Events)-1].Code)
	require.Zero(t, c.uploads)
	require.Zero(t, source.fetches)
	require.Nil(t, r.Firmware)
}

func TestFirmwareUpgradeUnsupportedWithoutUpgrader(t *testing.T) {
	q, c, _, _, _, e := firmwareEngine(t)
	e.Firmware = nil
	r, err := e.Execute(t.Context(), q, true)
	require.ErrorIs(t, err, camera.ErrUnsupported)
	require.Equal(t, provision.Unsupported, r.Stage)
	require.Zero(t, c.uploads)
}

func TestFirmwareCheckWithoutAPresetNeedsInput(t *testing.T) {
	q, c, _, _, _, e := firmwareEngine(t)
	e.Presets = presetProvider(func(context.Context, camera.EnrollmentScope, camera.DeviceHint) (camera.DevicePreset, error) {
		return camera.DevicePreset{}, camera.ErrPresetNotFound
	})
	r, err := e.Execute(t.Context(), q, true)
	require.ErrorIs(t, err, camera.ErrNeedsUserInput)
	require.Equal(t, provision.WaitingInput, r.Stage)
	require.Zero(t, c.uploads)
}
