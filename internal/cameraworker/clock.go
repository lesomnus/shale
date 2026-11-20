package cameraworker

import (
	"context"
	"net/url"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// clockReadTimeout bounds one camera's clock reading.
	clockReadTimeout = 5 * time.Second
	// maxClockDevices bounds the cameras whose last reading is remembered.
	maxClockDevices = 1024
)

// clockEndpoint is where a camera's clock is read: the first HTTP(S) endpoint
// this scan saw for it that is also one of the device's authorized endpoints.
// A camera seen only somewhere else, a transport candidate, has none.
func clockEndpoint(request *api.CameraPolicyObserveRequest, d *api.CameraDevice, cidrs []string) (string, bool) {
	authorized := make(map[string]bool, len(d.GetEndpoints()))
	for _, ep := range d.GetEndpoints() {
		authorized[ep.GetUri()] = true
	}
	for _, ep := range request.GetEndpoints() {
		raw := ep.GetUri()
		u, err := url.Parse(raw)
		if err != nil || u.Scheme != "http" && u.Scheme != "https" || !authorized[raw] {
			continue
		}
		if _, err := discoveryEndpoint(raw, cidrs); err != nil {
			continue
		}
		return raw, true
	}
	return "", false
}

// clockDue says whether a camera's clock may be read now, and if so records
// the attempt. A failed reading waits as long as one that was reported, so a
// camera that does not answer is not asked again at every scan.
func (w *Worker) clockDue(device string, now time.Time) bool {
	w.clockMu.Lock()
	defer w.clockMu.Unlock()
	if w.clockReads == nil {
		w.clockReads = make(map[string]time.Time)
	}
	last, seen := w.clockReads[device]
	if seen && now.Sub(last) < w.cfg.ClockInterval {
		return false
	}
	if !seen && len(w.clockReads) >= maxClockDevices {
		for id, at := range w.clockReads {
			if now.Sub(at) >= w.cfg.ClockInterval {
				delete(w.clockReads, id)
			}
		}
		if len(w.clockReads) >= maxClockDevices {
			return false
		}
	}
	w.clockReads[device] = now
	return true
}

// clockObservation is a reading in the bounds the CP records: a known mode
// and a TZ string or neither, a bounded round trip, and a device clock
// between 1970 and 9999.
func clockObservation(c camera.DeviceClock) (*api.CameraClockObservation, error) {
	if c.ObservedAt.IsZero() || c.RTT < 0 || c.RTT > camera.MaxClockRTT || c.UTC.Year() < 1970 || c.UTC.Year() > 9999 {
		return nil, camera.ErrInvalid
	}
	at := timestamppb.New(c.ObservedAt)
	if at.CheckValid() != nil {
		return nil, camera.ErrInvalid
	}
	mode, tz := c.Mode, c.TZ
	if !camera.ValidClockMode(mode) {
		mode = ""
	}
	if !camera.ValidClockTZ(tz) {
		tz = ""
	}
	// Rounded up: the uncertainty is never understated.
	rtt := (c.RTT + time.Millisecond - 1) / time.Millisecond
	return api.CameraClockObservation_builder{OffsetMs: c.OffsetMilliseconds(), RttMs: int64(rtt), Mode: mode, Tz: tz, ObservedAt: at}.Build(), nil
}

// observeClock reads the clock of a camera this scan placed at its
// authorized endpoints and reports it to CameraPolicy.ObserveClock, which
// rechecks the assignment and that the endpoint is still the device's
// (docs/camera-worker.md, "Camera clocks"). It sends the camera no credential
// and nothing that sets its clock. An older CP, which records no clock, is not
// an error.
func (w *Worker) observeClock(ctx context.Context, policies api.CameraPolicyServiceClient, scope camera.DiscoveryScope, request *api.CameraPolicyObserveRequest, observed *api.CameraPolicyObserveResponse) error {
	if w.cfg.Clocks == nil {
		return nil
	}
	d := observed.GetDevice()
	id := cameraID(d.GetId())
	if id == "" || d.GetDateErased() != nil {
		return nil
	}
	endpoint, ok := clockEndpoint(request, d, scope.CIDRs)
	if !ok || !w.clockDue(id, time.Now()) {
		return nil
	}
	call, cancel := context.WithTimeout(ctx, clockReadTimeout)
	clock, err := w.cfg.Clocks.ReadClock(call, scope, camera.DeviceHint{ID: camera.DeviceID(id), Endpoints: []camera.Endpoint{{URL: endpoint, NetworkScope: scope.ID}}})
	cancel()
	if err != nil {
		return err
	}
	reading, err := clockObservation(clock)
	if err != nil {
		return err
	}
	call, cancel = context.WithTimeout(ctx, discoveryRPCTimeout)
	defer cancel()
	_, err = policies.ObserveClock(call, api.CameraPolicyObserveClockRequest_builder{Assignment: request.GetAssignment(), AssignmentGeneration: request.GetAssignmentGeneration(), Device: api.CameraDeviceRef_builder{Id: d.GetId()}.Build(), Endpoint: endpoint, Clock: reading}.Build())
	if status.Code(err) == codes.Unimplemented {
		return nil
	}
	return err
}
