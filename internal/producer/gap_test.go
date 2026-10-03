package producer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/lesomnus/otx"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// A recording at 30 fps has no frame longer than a frame.
func TestMP4ReaderLongest(t *testing.T) {
	b, err := os.ReadFile("testdata/av.mp4")
	require.NoError(t, err)
	r := NewMP4Reader(bytes.NewReader(b))
	var longest time.Duration
	for {
		var f Frame
		if err := r.Next(&f); errors.Is(err, io.EOF) {
			break
		} else {
			require.NoError(t, err)
		}
		longest = max(longest, f.Longest)
	}
	require.InDelta(t, float64(time.Second/30), float64(longest), float64(time.Millisecond))
}

// A frame lasting more than one and three quarters frames is a gap, and the frames
// that would have filled it are counted missed (§38.6).
func TestAccountGap(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	ctx := otx.Into(context.Background(), otx.New(otx.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))))
	p := &Producer{m: newMetrics(ctx)}
	s := &source{cfg: SourceConfig{Alias: "cam", Fps: 30}}
	for _, d := range []time.Duration{0, 33 * time.Millisecond, 50 * time.Millisecond, 68 * time.Millisecond, 2 * time.Second} {
		p.accountGap(s, &Frame{Longest: d})
	}

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if d, ok := m.Data.(metricdata.Sum[int64]); ok {
				got[m.Name] = d.DataPoints[0].Value
			}
		}
	}
	require.Equal(t, int64(2), got["shale.producer.frame_gaps"], "68 ms and 2 s; 50 ms is jitter")
	require.Equal(t, int64(1+59), got["shale.producer.frames_missed"])
}
