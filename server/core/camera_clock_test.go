package core

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCameraClockAcceptsOnlyBoundedReadings(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	good := api.CameraClockObservation_builder{OffsetMs: -147 * 24 * 3600 * 1000, RttMs: 4, Mode: "NTP", Tz: "GMT0GMT", ObservedAt: timestamppb.New(now.Add(-2 * time.Second))}.Build()
	require.NoError(t, cameraClock(good, now))
	for name, edit := range map[string]func(*api.CameraClockObservation){
		"unknown mode and zone": func(c *api.CameraClockObservation) { c.SetMode(""); c.SetTz("") },
		"manual":                func(c *api.CameraClockObservation) { c.SetMode("Manual") },
		"a camera at 1970":      func(c *api.CameraClockObservation) { c.SetOffsetMs(-now.Add(-2*time.Second).UnixMilli() + 500) },
		"a camera in 9999":      func(c *api.CameraClockObservation) { c.SetOffsetMs(time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).UnixMilli() + 500 - now.Add(-2*time.Second).UnixMilli()) },
		"a slow answer":         func(c *api.CameraClockObservation) { c.SetRttMs(30_000) },
		"a Worker a little off": func(c *api.CameraClockObservation) { c.SetObservedAt(timestamppb.New(now.Add(5 * time.Minute))) },
	} {
		t.Run(name, func(t *testing.T) {
			c := proto.Clone(good).(*api.CameraClockObservation)
			edit(c)
			require.NoError(t, cameraClock(c, now))
		})
	}
	for name, edit := range map[string]func(*api.CameraClockObservation){
		"no time":               func(c *api.CameraClockObservation) { c.SetObservedAt(nil) },
		"an invalid time":       func(c *api.CameraClockObservation) { c.SetObservedAt(&timestamppb.Timestamp{Seconds: 1, Nanos: -1}) },
		"a Worker far ahead":    func(c *api.CameraClockObservation) { c.SetObservedAt(timestamppb.New(now.Add(6 * time.Minute))) },
		"a Worker far behind":   func(c *api.CameraClockObservation) { c.SetObservedAt(timestamppb.New(now.Add(-6 * time.Minute))) },
		"a negative round trip": func(c *api.CameraClockObservation) { c.SetRttMs(-1) },
		"a slower answer":       func(c *api.CameraClockObservation) { c.SetRttMs(30_001) },
		"a camera before 1970":  func(c *api.CameraClockObservation) { c.SetOffsetMs(-now.Add(-2*time.Second).UnixMilli() - 1) },
		"a camera after 9999":   func(c *api.CameraClockObservation) { c.SetOffsetMs(time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() - now.Add(-2*time.Second).UnixMilli()) },
		"the largest offset":    func(c *api.CameraClockObservation) { c.SetOffsetMs(math.MaxInt64) },
		"the smallest offset":   func(c *api.CameraClockObservation) { c.SetOffsetMs(math.MinInt64) },
		"another mode":          func(c *api.CameraClockObservation) { c.SetMode("Auto") },
		"a mode in lower case":  func(c *api.CameraClockObservation) { c.SetMode("ntp") },
		"a zone with a space":   func(c *api.CameraClockObservation) { c.SetTz("GMT 0") },
		"a zone with a newline": func(c *api.CameraClockObservation) { c.SetTz("GMT0\nGMT") },
		"a long zone":           func(c *api.CameraClockObservation) { c.SetTz(strings.Repeat("A", 65)) },
		"a zone not ASCII":      func(c *api.CameraClockObservation) { c.SetTz("시간대") },
	} {
		t.Run(name, func(t *testing.T) {
			c := proto.Clone(good).(*api.CameraClockObservation)
			edit(c)
			require.Equal(t, codes.InvalidArgument, status.Code(cameraClock(c, now)))
		})
	}
	require.Equal(t, codes.InvalidArgument, status.Code(cameraClock(nil, now)))
}
