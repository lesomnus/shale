package core

import (
	"bytes"
	"context"
	"net/url"
	"slices"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// cameraClockTolerance is how far a clock reading's time may be from the
// CP's: the clock tolerance hosts are held to (§10). A Worker further off
// than that measures every camera against a clock that is itself wrong.
const cameraClockTolerance = 5 * time.Minute

var (
	// A camera's clock is read to the second between 1970 and 9999, as
	// ONVIF's date can say, plus the half second its whole seconds hide.
	cameraClockEarliest = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	cameraClockLatest   = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
)

// cameraClock checks a Worker's clock reading before it is recorded: a valid
// time near the CP's, a camera clock between 1970 and 9999 at that time, a
// bounded round trip, and a mode and TZ string a reading may carry.
func cameraClock(c *api.CameraClockObservation, now time.Time) error {
	if c == nil {
		return invalid("clock", "a clock reading is required")
	}
	at := c.GetObservedAt()
	if at == nil || at.CheckValid() != nil {
		return invalid("clock.observed_at", "a valid reading time is required")
	}
	if d := at.AsTime().Sub(now); d > cameraClockTolerance || d < -cameraClockTolerance {
		return invalid("clock.observed_at", "the reading's time is more than five minutes from the control plane's")
	}
	if c.GetRttMs() < 0 || c.GetRttMs() > camera.MaxClockRTT.Milliseconds() {
		return invalid("clock.rtt_ms", "a round trip of zero to thirty seconds is required")
	}
	// Bound the offset before adding it to anything: far beyond any clock a
	// camera can report, and far inside an int64.
	if span := cameraClockLatest - cameraClockEarliest; c.GetOffsetMs() < -span || c.GetOffsetMs() > span {
		return invalid("clock.offset_ms", "the camera's clock must be between 1970 and 9999")
	}
	if device := at.AsTime().UnixMilli() + c.GetOffsetMs(); device < cameraClockEarliest || device >= cameraClockLatest {
		return invalid("clock.offset_ms", "the camera's clock must be between 1970 and 9999")
	}
	if !camera.ValidClockMode(c.GetMode()) {
		return invalid("clock.mode", `the mode is "Manual", "NTP" or unknown`)
	}
	if !camera.ValidClockTZ(c.GetTz()) {
		return invalid("clock.tz", "a POSIX TZ string of at most 64 printable characters is required")
	}
	return nil
}

// ObserveClock records the assigned Worker's credential-free reading of a
// device's own clock (docs/camera-worker.md, "Camera clocks"). The reading
// must have been taken at one of the device's authorized HTTP(S) endpoints,
// under today's assignment generation; it replaces the last reading and
// nothing else. No camera's clock is set, here or anywhere.
func (s coreCameraPolicy) ObserveClock(ctx context.Context, r *api.CameraPolicyObserveClockRequest) (*api.CameraDevice, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if err := cameraClock(r.GetClock(), s.d.now()); err != nil {
		return nil, err
	}
	if id, err := idOf(r.GetDevice().GetId()); err != nil || id.Domain() != DomCameraDevice {
		return nil, invalid("device", "a device ID is required")
	}
	if u, err := url.Parse(r.GetEndpoint()); err != nil || u.Scheme != "http" && u.Scheme != "https" {
		return nil, invalid("endpoint", "the HTTP(S) endpoint the clock was read at is required")
	}
	if _, err := s.cameraWorkerActor(ctx); err != nil {
		return nil, err
	}
	var out *api.CameraDevice
	err := s.cameraExecutionTx(ctx, func(ctx context.Context, nx api.Server) error {
		c := New(nx, s.d)
		// Locks the network, then the assignment, then the device.
		a, n, err := c.cameraObservationAssignment(ctx, r.GetAssignment(), r.GetAssignmentGeneration())
		if err != nil {
			return err
		}
		if err := c.cameraLockDevice(ctx, r.GetDevice().GetId()); err != nil {
			return err
		}
		ref := api.CameraDeviceRef_builder{Id: r.GetDevice().GetId()}.Build()
		d, err := nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: ref}.Build())
		if err != nil {
			return err
		}
		if d.GetDateErased() != nil {
			return status.Error(codes.NotFound, "camera device was erased")
		}
		if !bytes.Equal(d.GetNetwork().GetId(), n.GetId()) || cameraSameScope(d.GetTenant().GetId(), d.GetSite().GetId(), a.GetTenant().GetId(), a.GetSite().GetId()) != nil {
			return denied("clock", "device belongs to another assigned scope")
		}
		// A camera seen somewhere else is a transport candidate, not this
		// device: only a reading at its own endpoints is its clock.
		if !slices.ContainsFunc(d.GetEndpoints(), func(ep *api.CameraObservedEndpoint) bool { return ep.GetUri() == r.GetEndpoint() }) {
			return status.Error(codes.Aborted, "camera device endpoints changed")
		}
		if _, err := nx.CameraDevice().Patch(ctx, api.CameraDevicePatchRequest_builder{Ref: ref, Clock: r.GetClock(), DateUpdated: d.GetDateUpdated()}.Build()); err != nil {
			return err
		}
		out, err = nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: ref}.Build())
		return err
	})
	if err != nil {
		return nil, err
	}
	if _, err := s.cameraWorkerActor(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
