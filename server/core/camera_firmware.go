package core

import (
	"context"
	"reflect"
	"slices"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/preset"
	"github.com/lesomnus/shale/internal/cameratls"
	"github.com/lesomnus/shale/provision"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// cameraProgressFirmware authorizes a firmware upgrade and its completion.
//
// The checkpoint that starts an upgrade arrives before the image is written,
// so it is the authorization: the job's own preset, resolved for the inspected
// device, must require exactly this transition. Completion needs the target
// build from an authenticated inspection of the same recorded device. A new
// TLS key is accepted only there, in place of the job's pinned key.
func (s Core) cameraProgressFirmware(ctx context.Context, run provision.Run, previous *provision.Run, j *api.CameraJob, a *api.CameraAssignment, d *api.CameraDevice) error {
	var old *provision.FirmwareState
	if previous != nil {
		old = previous.Firmware
	}
	next := run.Firmware
	if next == nil {
		if old != nil {
			return failed("a camera checkpoint cannot forget a firmware upgrade")
		}
		return nil
	}
	if run.Inspection == nil {
		return invalid("progress.firmware", "a firmware upgrade requires device inspection")
	}
	if old != nil && (old.From != next.From || old.To != next.To || old.ImageSHA256 != next.ImageSHA256) {
		return failed("a started firmware upgrade cannot change")
	}
	if old != nil && old.Complete {
		if !reflect.DeepEqual(old, next) {
			return failed("a completed firmware upgrade cannot change")
		}
		return nil
	}
	if run.Preset != nil && !next.Complete {
		return invalid("progress.firmware", "a preset is resolved only after its firmware upgrade completes")
	}
	device := run.Inspection.Device
	if !cameraAuthenticated(device) {
		return invalid("progress.firmware", "firmware upgrade state requires an authenticated inspection")
	}
	if old == nil {
		if next.Complete || run.Preset != nil || j.GetPinnedPreset() != nil || device.Firmware != next.From {
			return invalid("progress.firmware", "an upgrade starts from the inspected build, before the preset is pinned")
		}
		resolved, err := s.cameraResolvePreset(ctx, j, a, d, cameraProgressIdentityValue(device))
		if err != nil {
			return err
		}
		upgrade := resolved.GetFirmware()
		if upgrade == nil || !slices.Contains(upgrade.GetFrom(), next.From) || upgrade.GetTo() != next.To || upgrade.GetImageSha256() != next.ImageSHA256 {
			return invalid("progress.firmware", "the job's preset does not require this firmware upgrade")
		}
		return nil
	}
	if !next.Complete {
		return nil
	}
	if device.Firmware != next.To {
		return invalid("progress.firmware", "a completed upgrade requires the target build")
	}
	if previous.Inspection == nil || !cameraSameRecordedDevice(previous.Inspection.Device, device, d.GetIdentity()) {
		return failed("a firmware upgrade completes only on the recorded MAC and serial")
	}
	if v := next.TLSIdentity; v != nil {
		pin := j.GetInputs().GetDeviceTlsIdentity()
		if pin == nil || v.DeviceID != pin.GetDeviceId() || v.SPKISHA256 == pin.GetSpkiSha256() || cameraTLSIdentity(cameratls.Encode(v)) != nil {
			return invalid("progress.firmware.tls_identity", "a new key may only replace this job's pinned device key")
		}
	}
	return nil
}

func cameraAuthenticated(v camera.DeviceHint) bool {
	for _, e := range v.Evidence {
		if e.Authenticated {
			return true
		}
	}
	return false
}

// cameraSameRecordedDevice requires the same non-empty serial and MAC before
// and after, and in the device's authenticated inventory record.
func cameraSameRecordedDevice(before, after camera.DeviceHint, recorded *api.CameraIdentityEvidence) bool {
	if before.Serial == "" || before.Serial != after.Serial || !recorded.GetAuthenticated() || recorded.GetSerial() != after.Serial {
		return false
	}
	x, xe := preset.NormalizeMAC(before.MAC)
	y, ye := preset.NormalizeMAC(after.MAC)
	if xe != nil || ye != nil || x != y {
		return false
	}
	return slices.ContainsFunc(recorded.GetMacs(), func(mac string) bool {
		z, err := preset.NormalizeMAC(mac)
		return err == nil && z == y
	})
}

// cameraFirmwareTLSRecord returns the device's new accepted key when this
// checkpoint completes an upgrade that rotated it. It replaces the approved key
// the job's pin came from, so a chain of upgrades still names that approval.
func cameraFirmwareTLSRecord(j *api.CameraJob, d *api.CameraDevice, run provision.Run) *api.CameraFirmwareTLSState {
	v := run.Firmware
	if v == nil || !v.Complete || v.TLSIdentity == nil || j.GetObserved().GetProgress().GetFirmware().GetComplete() {
		return nil
	}
	pin := j.GetInputs().GetDeviceTlsIdentity()
	if pin == nil {
		return nil
	}
	replaced := pin
	if prior := d.GetFirmwareTls(); prior != nil && proto.Equal(prior.GetIdentity(), pin) {
		replaced = prior.GetReplaced()
	}
	return api.CameraFirmwareTLSState_builder{
		Replaced:     proto.Clone(replaced).(*api.CameraDeviceTLSIdentity),
		Identity:     cameratls.Encode(v.TLSIdentity),
		JobId:        j.GetId(),
		Firmware:     v.To,
		DateAccepted: timestamppb.Now(),
	}.Build()
}
