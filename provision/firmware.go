package provision

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/lesomnus/shale/camera"
)

// ErrFirmwareNotApplied means the device answered again after an upgrade but
// did not report the target build within the wait.
var ErrFirmwareNotApplied = errors.New("device did not report the target firmware")

const (
	defaultFirmwareReboot = 6 * time.Minute
	defaultFirmwarePoll   = 10 * time.Second
	// A lease this close to expiry is replaced before a device operation.
	leaseMargin = 10 * time.Second
)

// FirmwareState is checkpointed before the image is written. Complete is set
// once the device reports To again with the same MAC and serial. TLSIdentity is
// the new key it presented then, when that differs from the job's pin.
type FirmwareState struct {
	From        string                    `json:"from"`
	To          string                    `json:"to"`
	ImageSHA256 string                    `json:"image_sha256"`
	Complete    bool                      `json:"complete,omitempty"`
	TLSIdentity *camera.DeviceTLSIdentity `json:"tls_identity,omitempty"`
}

type leaseSource func(context.Context) (camera.CredentialLease, error)

// renew replaces a lease that is closer than margin to expiry. Waiting for a
// download or a reboot outlasts the short lease taken at the start of a run.
func renew(ctx context.Context, lease *camera.CredentialLease, acquire leaseSource, margin time.Duration) error {
	if expiry := (*lease).ExpiresAt(); expiry.IsZero() || time.Until(expiry) > margin {
		return nil
	}
	next, err := acquire(ctx)
	if err != nil {
		return err
	}
	(*lease).Close()
	*lease = next
	return nil
}

// firmware runs before the preset is resolved and pinned: the upgraded build
// can match different preset rules. It returns stop when a read-only run only
// discovered that an upgrade is required. When no upgrade was needed it
// returns the preset it resolved for this inspection, for configure to pin.
func (e *Engine) firmware(ctx context.Context, r *Run, before camera.DeviceHint, inspection camera.Inspection, lease *camera.CredentialLease, acquire leaseSource, apply bool) (camera.Inspection, *camera.DevicePreset, bool, error) {
	if r.Preset != nil || !r.Request.UsePreset || r.Firmware != nil && r.Firmware.Complete {
		return inspection, nil, false, nil
	}
	if r.Firmware == nil {
		if e.Presets == nil {
			return inspection, nil, false, nil
		}
		preset, err := e.Presets.Resolve(ctx, r.Request.Scope, inspection.Device)
		if errors.Is(err, camera.ErrPresetNotFound) {
			return inspection, nil, false, camera.ErrNeedsUserInput
		}
		if err != nil {
			return inspection, nil, false, err
		}
		upgrade := preset.Firmware
		if upgrade == nil || !upgrade.Applies(inspection.Device.Firmware) {
			return inspection, &preset, false, nil
		}
		if err := upgrade.Validate(); err != nil {
			return inspection, nil, false, err
		}
		if e.Firmware == nil || e.Images == nil {
			return inspection, nil, false, camera.ErrUnsupported
		}
		if !apply {
			return inspection, nil, true, e.save(ctx, r, Planned, "firmware_upgrade_planned")
		}
		file, err := e.Images.Fetch(ctx, upgrade.Image)
		if err != nil {
			return inspection, nil, false, err
		}
		if file.SHA256 != upgrade.Image.SHA256 || len(file.Data) == 0 {
			return inspection, nil, false, camera.ErrConflict
		}
		if err := renew(ctx, lease, acquire, 2*leaseMargin); err != nil {
			return inspection, nil, false, err
		}
		r.Firmware = &FirmwareState{From: inspection.Device.Firmware, To: upgrade.To, ImageSHA256: upgrade.Image.SHA256}
		if err := e.save(ctx, r, Initializing, "firmware_upgrade_started"); err != nil {
			return inspection, nil, false, err
		}
		if err := e.Store.BeginMutation(ctx, r.Request); err != nil {
			return inspection, nil, false, err
		}
		if err := e.Firmware.UpgradeFirmware(ctx, inspection.Device, *lease, file); err != nil {
			return inspection, nil, false, err
		}
		before = inspection.Device
		if inspection, err = e.awaitFirmware(ctx, r.Request, r.Firmware.To, lease, acquire); err != nil {
			return inspection, nil, false, err
		}
	}
	if inspection.Device.Firmware != r.Firmware.To {
		return inspection, nil, false, ErrFirmwareNotApplied
	}
	// The new key is accepted only for the device this run upgraded: the same
	// non-empty MAC and serial, and the job's own credential logs in.
	if !sameRecordedDevice(before, inspection.Device) {
		return inspection, nil, false, camera.ErrAmbiguousIdentity
	}
	if err := e.Store.Identify(ctx, r.Request.Scope, inspection.Device); err != nil {
		return inspection, nil, false, err
	}
	if pin := r.Request.DeviceTLSIdentity; pin != nil {
		if e.Firmware == nil {
			return inspection, nil, false, camera.ErrUnsupported
		}
		if err := renew(ctx, lease, acquire, leaseMargin); err != nil {
			return inspection, nil, false, err
		}
		identity, err := e.Firmware.ConfirmWebIdentity(ctx, inspection.Device, *lease)
		if err != nil {
			return inspection, nil, false, err
		}
		if identity.DeviceID != pin.DeviceID || identity.Validate() != nil {
			return inspection, nil, false, camera.ErrConflict
		}
		if identity.SPKISHA256 != pin.SPKISHA256 {
			r.Firmware.TLSIdentity = &identity
		}
	}
	r.Firmware.Complete = true
	r.Inspection = &inspection
	return inspection, nil, false, e.save(ctx, r, Inspecting, "firmware_upgraded")
}

// awaitFirmware polls until the device answers with the target build. Errors
// while it reboots are expected; the last one is returned after the deadline.
func (e *Engine) awaitFirmware(ctx context.Context, q Request, to string, lease *camera.CredentialLease, acquire leaseSource) (camera.Inspection, error) {
	wait, poll := e.FirmwareReboot, e.FirmwarePoll
	if wait <= 0 {
		wait = defaultFirmwareReboot
	}
	if poll <= 0 {
		poll = defaultFirmwarePoll
	}
	deadline := time.Now().Add(wait)
	timer := time.NewTimer(poll)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return camera.Inspection{}, ctx.Err()
		case <-timer.C:
		}
		var i camera.Inspection
		err := renew(ctx, lease, acquire, leaseMargin)
		if err == nil {
			i, err = e.Inspector.Inspect(ctx, q.Device, *lease)
		}
		if err == nil && i.Device.Firmware == to {
			return cleanInspection(i), nil
		}
		if ctx.Err() != nil {
			return camera.Inspection{}, ctx.Err()
		}
		if errors.Is(err, camera.ErrForbidden) {
			return camera.Inspection{}, err
		}
		if !time.Now().Before(deadline) {
			if err != nil {
				return camera.Inspection{}, err
			}
			return cleanInspection(i), ErrFirmwareNotApplied
		}
		timer.Reset(poll)
	}
}

// sameRecordedDevice is stricter than samePhysicalDevice: both MAC and serial
// must be present and equal. It gates accepting a new TLS key.
func sameRecordedDevice(a, b camera.DeviceHint) bool {
	if a.Serial == "" || a.Serial != b.Serial || a.MAC == "" || a.ID != b.ID && a.ID != "" && b.ID != "" || a.Hardware != b.Hardware {
		return false
	}
	x, xe := net.ParseMAC(a.MAC)
	y, ye := net.ParseMAC(b.MAC)
	return xe == nil && ye == nil && x.String() == y.String()
}
