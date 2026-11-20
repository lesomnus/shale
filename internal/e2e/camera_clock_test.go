package e2e_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	cameramodel "github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/drivers"
	"github.com/lesomnus/shale/internal/cameratest"
	"github.com/lesomnus/shale/internal/cameraworker"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const day = 24 * time.Hour

// clockDiscoverer reports the cameras the test puts on the LAN at their
// current endpoints, with their EPRs.
type clockDiscoverer struct {
	mu      sync.Mutex
	cameras []*cameratest.ONVIFCamera
}

func (d *clockDiscoverer) Discover(_ context.Context, scope cameramodel.DiscoveryScope) ([]cameramodel.Observation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []cameramodel.Observation
	for _, c := range d.cameras {
		out = append(out, cameramodel.Observation{Scope: scope.ID, ObservedAt: time.Now(), Device: cameramodel.DeviceHint{CandidateID: c.CurrentURL(), DiscoveryID: c.Hint("").DiscoveryID, Endpoints: []cameramodel.Endpoint{{URL: c.CurrentURL(), NetworkScope: scope.ID}}}})
	}
	return out, nil
}

// requireOffset says the recorded offset is the camera's skew, to within the
// half second its whole seconds hide, half the round trip, and the moment
// between the skewed answer and the host's clock.
func requireOffset(t *testing.T, want time.Duration, c *api.CameraClockObservation) {
	t.Helper()
	got := time.Duration(c.GetOffsetMs()) * time.Millisecond
	bound := time.Second + time.Duration(c.GetRttMs())*time.Millisecond
	require.InDelta(t, float64(want), float64(got), float64(bound), "offset %v, want %v", got, want)
}

// An edge with six TP-Link C420s found four about 147 days behind and two
// some seconds off, all in NTP mode with no time server to reach. The real
// Worker reads each camera's own clock credential-free and the offset, mode
// and TZ reach its CameraDevice; a clock set right later is measured as
// such. Nothing is written to a camera.
func TestCameraWorkerReportsCameraClock(t *testing.T) {
	server, cp, admin, ctx := cameraPolicyControl(t)
	worker, _, agent := cameraWorkerMachine(t, ctx, server, cp, admin, "clock-worker", nil)
	producer, _, _ := cameraWorkerProducer(t, ctx, server, cp, admin, "clock-producer", nil)
	network, err := api.NewCameraNetworkServiceClient(admin).Configure(ctx, api.CameraNetworkConfigureRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "clock-lan", Cidrs: []string{"127.0.0.0/8"}, InterfaceHint: "lo"}.Build())
	require.NoError(t, err)
	_, err = api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "clock", WorkerId: worker.GetId()}.Build())
	require.NoError(t, err)

	profile := cameramodel.MediaProfile{Codec: "h264", Width: 160, Height: 90, FPS: 10}
	behind := cameratest.ServeONVIFWithOptions(t, profile, cameratest.ONVIFOptions{ListenAddress: "127.0.0.2:0", Identity: cameratest.CameraIdentity{Serial: "clock-behind", MAC: "02:00:00:00:33:01", DiscoveryID: "urn:uuid:5b0c3f0e-7f3a-4c55-9d43-6a1f3c2b3301"}})
	behind.SetClock(-147*day, "NTP", "GMT0GMT")
	ahead := cameratest.ServeONVIFWithOptions(t, profile, cameratest.ONVIFOptions{ListenAddress: "127.0.0.3:0", Identity: cameratest.CameraIdentity{Serial: "clock-ahead", MAC: "02:00:00:00:33:02", DiscoveryID: "urn:uuid:5b0c3f0e-7f3a-4c55-9d43-6a1f3c2b3302"}})
	ahead.SetClock(13*time.Second, "", "")
	discoverer := &clockDiscoverer{cameras: []*cameratest.ONVIFCamera{behind, ahead}}

	runtime, err := cameraworker.New(cameraworker.Config{StateDir: agent.Store.Dir, Cp: cp, Tenant: "acme", HardwareID: "clock-worker", CaHash: pki.Fingerprint(server.CA.Cert), HeartbeatInterval: 100 * time.Millisecond, RetryInterval: 100 * time.Millisecond, LeaseSeconds: 30, DiscoveryInterval: time.Second, DiscoveryTimeout: 500 * time.Millisecond, Discoverer: discoverer, Clocks: drivers.Clocks{Timeout: 2 * time.Second}, ClockInterval: time.Second, Log: agent.Log, Executor: cameraworker.ExecutorFunc(func(context.Context, *cameraworker.Execution) error {
		t.Error("no policy is active: discovery enrolls nothing")
		return nil
	})})
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Worker did not stop")
		}
	})

	devices := api.NewCameraDeviceServiceClient(admin)
	read := func(c *cameratest.ONVIFCamera) *api.CameraDevice {
		list, err := devices.List(ctx, api.CameraDeviceListRequest_builder{Filters: []*api.CameraDeviceFilter{api.CameraDeviceFilter_builder{Network: api.CameraNetworkRef_builder{Id: network.GetId()}.Build()}.Build()}}.Build())
		require.NoError(t, err)
		for _, d := range list.GetItems() {
			if slices.ContainsFunc(d.GetEndpoints(), func(ep *api.CameraObservedEndpoint) bool { return ep.GetUri() == c.CurrentURL() }) {
				return d
			}
		}
		return nil
	}
	clockOf := func(c *cameratest.ONVIFCamera) *api.CameraClockObservation {
		t.Helper()
		var out *api.CameraClockObservation
		require.Eventually(t, func() bool {
			out = read(c).GetClock()
			return out != nil
		}, 20*time.Second, 50*time.Millisecond, "the Worker reports the camera's clock")
		return out
	}

	b := clockOf(behind)
	requireOffset(t, -147*day, b)
	require.Equal(t, "NTP", b.GetMode())
	require.Equal(t, "GMT0GMT", b.GetTz())
	require.GreaterOrEqual(t, b.GetRttMs(), int64(0))
	require.Less(t, b.GetRttMs(), int64(2000))
	require.WithinDuration(t, time.Now(), b.GetObservedAt().AsTime(), time.Minute, "on the Worker's clock, which is this host's")
	a := clockOf(ahead)
	requireOffset(t, 13*time.Second, a)
	require.Empty(t, a.GetMode(), "a camera that does not say its mode is unknown")
	require.Empty(t, a.GetTz())

	// A camera whose clock is set right is measured as right at a later scan.
	behind.SetClock(0, "NTP", "GMT0GMT")
	require.Eventually(t, func() bool {
		c := read(behind).GetClock()
		offset := time.Duration(c.GetOffsetMs()) * time.Millisecond
		return c != nil && offset > -time.Second-time.Duration(c.GetRttMs())*time.Millisecond && offset < time.Second+time.Duration(c.GetRttMs())*time.Millisecond
	}, 20*time.Second, 50*time.Millisecond, "the fixed clock is measured")

	// Read, never written: the public clock and nothing else, no credential.
	for _, c := range []*cameratest.ONVIFCamera{behind, ahead} {
		calls := c.Calls()
		require.Contains(t, calls, "GetSystemDateAndTime")
		for _, call := range calls {
			require.Equal(t, "GetSystemDateAndTime", call, "a clock reading is the camera's only call")
		}
		require.Empty(t, c.AuthAttempts(), "no credential reaches a camera for its clock")
	}
}

// ObserveClock is the assigned Worker's, for a device of its assignment at
// the device's own endpoint, and records only a reading within bounds. A
// device a Worker without clocks observed has none.
func TestCameraWorkerClockReadingIsChecked(t *testing.T) {
	server, cp, admin, ctx := cameraPolicyControl(t)
	worker, conn, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "clock-checked", nil)
	_, other, _ := cameraWorkerMachine(t, ctx, server, cp, admin, "clock-other", nil)
	producer, _, producerConn := cameraWorkerProducer(t, ctx, server, cp, admin, "clock-checked-producer", nil)
	n := cameraPolicyNetwork(t, ctx, admin, "clock-checked-network", nil)
	assignment, err := api.NewCameraAssignmentServiceClient(admin).Assign(ctx, api.CameraAssignmentAssignRequest_builder{Network: api.CameraNetworkRef_builder{Id: n.GetId()}.Build(), Producer: api.ProducerRef_builder{Id: producer.GetId()}.Build(), WorkerSlot: "edge", WorkerId: worker.GetId()}.Build())
	require.NoError(t, err)
	endpoint := "http://192.0.2.10:2020/onvif/device_service"
	machine := api.NewCameraPolicyServiceClient(conn)
	observed, err := machine.Observe(ctx, api.CameraPolicyObserveRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build(), AssignmentGeneration: assignment.GetGeneration(), DiscoveryId: "urn:uuid:5b0c3f0e-7f3a-4c55-9d43-6a1f3c2b3310", Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: endpoint}.Build(), api.CameraObservedEndpoint_builder{Uri: "rtsp://192.0.2.10:554/stream1"}.Build()}}.Build())
	require.NoError(t, err)
	device := observed.GetDevice()
	require.Nil(t, device.GetClock(), "an observation carries no clock: a Worker that reads none leaves it absent")

	reading := api.CameraClockObservation_builder{OffsetMs: -147*day.Milliseconds() + 250, RttMs: 3, Mode: "NTP", Tz: "GMT0GMT", ObservedAt: timestamppb.Now()}.Build()
	request := api.CameraPolicyObserveClockRequest_builder{Assignment: api.CameraAssignmentRef_builder{Id: assignment.GetId()}.Build(), AssignmentGeneration: assignment.GetGeneration(), Device: api.CameraDeviceRef_builder{Id: device.GetId()}.Build(), Endpoint: endpoint, Clock: reading}.Build()
	got, err := machine.ObserveClock(ctx, request)
	require.NoError(t, err)
	require.True(t, proto.Equal(reading, got.GetClock()))
	stored, err := api.NewCameraDeviceServiceClient(admin).Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.True(t, proto.Equal(reading, stored.GetClock()), "the operator reads what the Worker reported")
	require.True(t, proto.Equal(device.GetEndpoints()[0], stored.GetEndpoints()[0]), "a reading changes nothing but the clock")
	require.Equal(t, device.GetObservationRevision(), stored.GetObservationRevision())
	require.True(t, proto.Equal(device.GetIdentity(), stored.GetIdentity()))
	require.True(t, proto.Equal(device.GetEnrollment(), stored.GetEnrollment()))

	refused := func(code codes.Code, client api.CameraPolicyServiceClient, edit func(*api.CameraPolicyObserveClockRequest)) {
		t.Helper()
		r := proto.Clone(request).(*api.CameraPolicyObserveClockRequest)
		r.GetClock().SetObservedAt(timestamppb.Now())
		r.GetClock().SetOffsetMs(1)
		edit(r)
		_, err := client.ObserveClock(ctx, r)
		require.Equal(t, code, status.Code(err), "%v", err)
	}
	same := func(*api.CameraPolicyObserveClockRequest) {}
	refused(codes.PermissionDenied, api.NewCameraPolicyServiceClient(other), same)
	refused(codes.PermissionDenied, api.NewCameraPolicyServiceClient(producerConn), same)
	refused(codes.PermissionDenied, api.NewCameraPolicyServiceClient(admin), same)
	refused(codes.Aborted, machine, func(r *api.CameraPolicyObserveClockRequest) { r.SetAssignmentGeneration(r.GetAssignmentGeneration() + 1) })
	refused(codes.Aborted, machine, func(r *api.CameraPolicyObserveClockRequest) { r.SetEndpoint("http://192.0.2.20:2020/onvif/device_service") })
	refused(codes.InvalidArgument, machine, func(r *api.CameraPolicyObserveClockRequest) { r.SetEndpoint("rtsp://192.0.2.10:554/stream1") })
	refused(codes.InvalidArgument, machine, func(r *api.CameraPolicyObserveClockRequest) { r.SetDevice(nil) })
	refused(codes.InvalidArgument, machine, func(r *api.CameraPolicyObserveClockRequest) { r.SetClock(nil) })
	refused(codes.InvalidArgument, machine, func(r *api.CameraPolicyObserveClockRequest) { r.GetClock().SetMode("Auto") })
	refused(codes.InvalidArgument, machine, func(r *api.CameraPolicyObserveClockRequest) { r.GetClock().SetTz("GMT 0") })
	refused(codes.InvalidArgument, machine, func(r *api.CameraPolicyObserveClockRequest) { r.GetClock().SetRttMs(-1) })
	refused(codes.InvalidArgument, machine, func(r *api.CameraPolicyObserveClockRequest) { r.GetClock().SetOffsetMs(1 << 62) })
	refused(codes.InvalidArgument, machine, func(r *api.CameraPolicyObserveClockRequest) {
		r.GetClock().SetObservedAt(timestamppb.New(time.Now().Add(-time.Hour)))
	})
	stored, err = api.NewCameraDeviceServiceClient(admin).Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: device.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.True(t, proto.Equal(reading, stored.GetClock()), "a refused reading records nothing")
}
