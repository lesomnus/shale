package e2e_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
)

// A node's word reaches only what it holds (§33.7): a report about another
// live node's sink or device, however alarming, changes nothing about
// them, and a sink's capacity is stored clamped.
func TestNodeTelemetryIsItsOwn(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")
	a, stopA := c.startNode("node-a", filepath.Join(t.TempDir(), "a"))
	defer stopA()
	b, stopB := c.startNode("node-b", filepath.Join(t.TempDir(), "b"))
	defer stopB()

	sinks := api.NewSinkServiceClient(ops)
	devices := api.NewDeviceServiceClient(ops)
	var aSink *api.Sink
	var aDevice *api.Device
	require.Eventually(t, func() bool {
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{Size: 50}.Build())
		if err != nil {
			return false
		}
		for _, s := range vs.GetItems() {
			if string(s.GetNode().GetId()) == string(a.Id().Bytes()) && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED {
				aSink = s
			}
		}
		if aSink == nil {
			return false
		}
		d, err := devices.Get(ctx, api.DeviceGetRequest_builder{Ref: api.DeviceRef_builder{Id: aSink.GetDevice().GetId()}.Build()}.Build())
		if err != nil {
			return false
		}
		aDevice = d

		return true
	}, 30*time.Second, 200*time.Millisecond, "node A's sink is attached")
	require.NotEqual(t, api.Pressure_PRESSURE_CRITICAL, aSink.GetPressure())

	// Node B heartbeats with A's sink and device in its report: A's sink is
	// critical, its capabilities gone, its capacity absurd, and A's device
	// failing.
	asB := c.dialCluster(b.Id().String())
	_, err := api.NewNodeServiceClient(asB).Heartbeat(ctx, api.NodeHeartbeatRequest_builder{
		Devices: []*api.DeviceReport{api.DeviceReport_builder{
			HardwareId: aDevice.GetHardwareId(), Model: "evil", Capacity: 1, IoErrors: 1000, FailedWrites: 1000,
		}.Build()},
		Sinks: []*api.SinkReport{api.SinkReport_builder{
			SinkId: aSink.GetId(), DeviceHardwareId: aDevice.GetHardwareId(), Path: "/evil",
			Capacity: 1 << 60, Free: 0, Pressure: api.Pressure_PRESSURE_CRITICAL,
			Capabilities: api.SinkCapabilities_builder{Xattr: false}.Build(), AcceptWrites: false,
		}.Build()},
	}.Build())
	require.NoError(t, err, "the heartbeat itself is taken; the foreign rows in it are not")

	after, err := sinks.Get(ctx, api.SinkGetRequest_builder{Ref: api.SinkRef_builder{Id: aSink.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, aSink.GetPressure(), after.GetPressure(), "another node's pressure is not A's sink's")
	require.Equal(t, aSink.GetCapacity(), after.GetCapacity())
	require.Equal(t, aSink.GetPath(), after.GetPath())
	require.Equal(t, string(a.Id().Bytes()), string(after.GetNode().GetId()), "the sink stays A's")
	require.True(t, after.GetCapabilities().GetXattr(), "capabilities as A reported them")
	dev, err := devices.Get(ctx, api.DeviceGetRequest_builder{Ref: api.DeviceRef_builder{Id: aDevice.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, aDevice.GetModel(), dev.GetModel())
	require.Equal(t, api.DeviceHealth_DEVICE_HEALTH_HEALTHY, dev.GetHealth(), "another node's SMART does not quarantine A's disk")
	require.Equal(t, string(a.Id().Bytes()), string(dev.GetNode().GetId()))

	// B's own sink is B's to describe, within max_sink_capacity.
	var bSink *api.Sink
	vs, err := sinks.List(ctx, api.SinkListRequest_builder{Size: 50}.Build())
	require.NoError(t, err)
	for _, s := range vs.GetItems() {
		if string(s.GetNode().GetId()) == string(b.Id().Bytes()) {
			bSink = s
		}
	}
	require.NotNil(t, bSink)
	require.LessOrEqual(t, bSink.GetCapacity(), int64(64)<<40, "capacity is clamped to max_sink_capacity")
}
