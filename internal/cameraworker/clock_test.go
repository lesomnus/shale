package cameraworker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type clockReaderFunc func(context.Context, camera.DiscoveryScope, camera.DeviceHint) (camera.DeviceClock, error)

func (f clockReaderFunc) ReadClock(ctx context.Context, scope camera.DiscoveryScope, h camera.DeviceHint) (camera.DeviceClock, error) {
	return f(ctx, scope, h)
}

type clockPolicyClient struct {
	api.CameraPolicyServiceClient
	mu       sync.Mutex
	requests []*api.CameraPolicyObserveClockRequest
	err      error
}

func (c *clockPolicyClient) ObserveClock(_ context.Context, r *api.CameraPolicyObserveClockRequest, _ ...grpc.CallOption) (*api.CameraDevice, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, r)
	return nil, c.err
}

const clockEndpointURI = "http://192.0.2.10:2020/onvif/device_service"

func clockObservationFixture(t *testing.T, offset time.Duration) (*Worker, camera.DiscoveryScope, *api.CameraPolicyObserveRequest, *api.CameraPolicyObserveResponse, *[]camera.DeviceHint) {
	t.Helper()
	w, assigned, _ := discoveryFixture(t)
	w.cfg.ClockInterval = time.Minute
	scope, err := discoveryScope(assigned, w.ID().Bytes(), time.Second)
	require.NoError(t, err)
	var reads []camera.DeviceHint
	w.cfg.Clocks = clockReaderFunc(func(_ context.Context, s camera.DiscoveryScope, h camera.DeviceHint) (camera.DeviceClock, error) {
		require.Equal(t, scope.ID, s.ID)
		require.Equal(t, scope.CIDRs, s.CIDRs)
		reads = append(reads, h)
		now := time.Now()
		return camera.DeviceClock{UTC: now.Add(offset).Truncate(time.Second), ObservedAt: now, RTT: 3*time.Millisecond + 400*time.Microsecond, Mode: camera.ClockModeNTP, TZ: "GMT0GMT"}, nil
	})
	a := assigned.GetAssignment()
	eps := []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "rtsp://192.0.2.10:554/stream1"}.Build(), api.CameraObservedEndpoint_builder{Uri: clockEndpointURI}.Build()}
	request := api.CameraPolicyObserveRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build(), AssignmentGeneration: a.GetGeneration(), DiscoveryId: "urn:uuid:9c654a77-3a86-4466-b189-7bbdc38ad248", Endpoints: eps}.Build()
	d := api.CameraDevice_builder{Id: pdid.New(27).Bytes(), Network: assigned.GetNetwork(), Endpoints: eps}.Build()
	return w, scope, request, api.CameraPolicyObserveResponse_builder{Code: "already_enrolled", Device: d}.Build(), &reads
}

func TestObserveClockReportsTheCamerasOffsetAtItsAuthorizedEndpoint(t *testing.T) {
	skew := -147 * 24 * time.Hour
	w, scope, request, observed, reads := clockObservationFixture(t, skew)
	policies := &clockPolicyClient{}
	require.NoError(t, w.observeClock(t.Context(), policies, scope, request, observed))
	require.Len(t, *reads, 1)
	h := (*reads)[0]
	require.Equal(t, camera.DeviceID(cameraID(observed.GetDevice().GetId())), h.ID)
	require.Equal(t, []camera.Endpoint{{URL: clockEndpointURI, NetworkScope: scope.ID}}, h.Endpoints, "only the HTTP endpoint the scan and the device share")
	require.Len(t, policies.requests, 1)
	r := policies.requests[0]
	require.Equal(t, request.GetAssignment().GetId(), r.GetAssignment().GetId())
	require.Equal(t, request.GetAssignmentGeneration(), r.GetAssignmentGeneration())
	require.Equal(t, observed.GetDevice().GetId(), r.GetDevice().GetId())
	require.Equal(t, clockEndpointURI, r.GetEndpoint())
	c := r.GetClock()
	require.InDelta(t, skew.Milliseconds(), c.GetOffsetMs(), 1000)
	require.Equal(t, int64(4), c.GetRttMs(), "the round trip is rounded up")
	require.Equal(t, "NTP", c.GetMode())
	require.Equal(t, "GMT0GMT", c.GetTz())
	require.WithinDuration(t, time.Now(), c.GetObservedAt().AsTime(), time.Minute)

	// The same camera is not read again within the interval, even by a
	// later scan; another camera is.
	require.NoError(t, w.observeClock(t.Context(), policies, scope, request, observed))
	require.Len(t, *reads, 1)
	other := api.CameraPolicyObserveResponse_builder{Device: api.CameraDevice_builder{Id: pdid.New(27).Bytes(), Endpoints: observed.GetDevice().GetEndpoints()}.Build()}.Build()
	require.NoError(t, w.observeClock(t.Context(), policies, scope, request, other))
	require.Len(t, *reads, 2)
	w.clockReads[cameraID(observed.GetDevice().GetId())] = time.Now().Add(-time.Minute)
	require.NoError(t, w.observeClock(t.Context(), policies, scope, request, observed))
	require.Len(t, *reads, 3, "due again after the interval")
}

func TestObserveClockReadsNothingWhereTheCameraIsNotItsDevice(t *testing.T) {
	for name, edit := range map[string]func(*Worker, *api.CameraPolicyObserveRequest, *api.CameraPolicyObserveResponse){
		"no reader": func(w *Worker, _ *api.CameraPolicyObserveRequest, _ *api.CameraPolicyObserveResponse) {
			w.cfg.Clocks = nil
		},
		// A transport candidate: the scan saw the camera elsewhere.
		"moved": func(_ *Worker, r *api.CameraPolicyObserveRequest, _ *api.CameraPolicyObserveResponse) {
			r.SetEndpoints([]*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.20:2020/onvif/device_service"}.Build()})
		},
		"media only": func(_ *Worker, r *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			eps := []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "rtsp://192.0.2.10:554/stream1"}.Build()}
			r.SetEndpoints(eps)
			o.GetDevice().SetEndpoints(eps)
		},
		"outside the prefixes": func(_ *Worker, r *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			eps := []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://198.51.100.10:2020/onvif/device_service"}.Build()}
			r.SetEndpoints(eps)
			o.GetDevice().SetEndpoints(eps)
		},
		"erased": func(_ *Worker, _ *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			o.GetDevice().SetDateErased(timestamppb.Now())
		},
		"no device": func(_ *Worker, _ *api.CameraPolicyObserveRequest, o *api.CameraPolicyObserveResponse) {
			o.SetDevice(nil)
		},
	} {
		t.Run(name, func(t *testing.T) {
			w, scope, request, observed, reads := clockObservationFixture(t, 0)
			edit(w, request, observed)
			policies := &clockPolicyClient{}
			require.NoError(t, w.observeClock(t.Context(), policies, scope, request, observed))
			require.Empty(t, *reads)
			require.Empty(t, policies.requests)
		})
	}
}

func TestObserveClockToleratesAnOlderControlPlane(t *testing.T) {
	w, scope, request, observed, _ := clockObservationFixture(t, 13*time.Second)
	policies := &clockPolicyClient{err: status.Error(codes.Unimplemented, "unknown method ObserveClock")}
	require.NoError(t, w.observeClock(t.Context(), policies, scope, request, observed), "a CP that records no clock is not a failure")
	w, scope, request, observed, _ = clockObservationFixture(t, 13*time.Second)
	policies = &clockPolicyClient{err: status.Error(codes.Aborted, "camera device endpoints changed")}
	require.Equal(t, codes.Aborted, status.Code(w.observeClock(t.Context(), policies, scope, request, observed)))
}

func TestObserveClockWaitsAfterAFailedReading(t *testing.T) {
	w, scope, request, observed, _ := clockObservationFixture(t, 0)
	calls := 0
	w.cfg.Clocks = clockReaderFunc(func(context.Context, camera.DiscoveryScope, camera.DeviceHint) (camera.DeviceClock, error) {
		calls++
		return camera.DeviceClock{}, errors.New("no answer")
	})
	policies := &clockPolicyClient{}
	require.Error(t, w.observeClock(t.Context(), policies, scope, request, observed))
	require.NoError(t, w.observeClock(t.Context(), policies, scope, request, observed))
	require.Equal(t, 1, calls, "a camera that does not answer is not asked at every scan")
	require.Empty(t, policies.requests)
}

func TestClockDueIsBounded(t *testing.T) {
	w := &Worker{cfg: Config{ClockInterval: time.Minute}}
	now := time.Now()
	for i := range maxClockDevices {
		require.True(t, w.clockDue(strings.Repeat("x", i+1), now))
	}
	require.False(t, w.clockDue("one more", now), "the table is full of cameras read just now")
	require.True(t, w.clockDue("one more", now.Add(time.Minute)), "readings older than the interval make room")
	require.LessOrEqual(t, len(w.clockReads), maxClockDevices)
}

func TestClockObservationIsInTheBoundsTheCPRecords(t *testing.T) {
	now := time.Now()
	good := camera.DeviceClock{UTC: now.Truncate(time.Second), ObservedAt: now, RTT: time.Millisecond, Mode: "NTP", TZ: "GMT0GMT"}
	c, err := clockObservation(good)
	require.NoError(t, err)
	require.Equal(t, "NTP", c.GetMode())
	unknown := good
	unknown.Mode, unknown.TZ = "Auto", "GMT 0"
	c, err = clockObservation(unknown)
	require.NoError(t, err)
	require.Empty(t, c.GetMode(), "a mode it may not carry is unknown")
	require.Empty(t, c.GetTz(), "a TZ it may not carry is unknown")
	for name, edit := range map[string]func(*camera.DeviceClock){
		"no time":      func(c *camera.DeviceClock) { c.ObservedAt = time.Time{} },
		"negative rtt": func(c *camera.DeviceClock) { c.RTT = -time.Millisecond },
		"slow":         func(c *camera.DeviceClock) { c.RTT = camera.MaxClockRTT + time.Millisecond },
		"before 1970":  func(c *camera.DeviceClock) { c.UTC = time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC) },
		"after 9999":   func(c *camera.DeviceClock) { c.UTC = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := good
			edit(&bad)
			_, err := clockObservation(bad)
			require.ErrorIs(t, err, camera.ErrInvalid)
		})
	}
}

func TestClockConfigurationIsBounded(t *testing.T) {
	w, err := New(Config{StateDir: t.TempDir(), Cp: "cp.example"})
	require.NoError(t, err)
	require.Nil(t, w.cfg.Clocks, "the library reads no clock unless its composition root says how")
	require.Equal(t, 5*time.Minute, w.cfg.ClockInterval)
	for _, interval := range []time.Duration{time.Millisecond, time.Hour + 1, -time.Second} {
		_, err := New(Config{StateDir: t.TempDir(), Cp: "cp.example", ClockInterval: interval})
		require.Error(t, err)
	}
}
