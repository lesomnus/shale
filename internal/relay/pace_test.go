package relay

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/lesomnus/otx"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/lesomnus/payday/pdid"
)

// The playout clock: samples go at their timestamps from the first one on;
// a late one goes now and the clock keeps its pace from there; a jump ahead
// starts the clock over.
func TestPlayout(t *testing.T) {
	var c playout
	t0 := time.Date(2026, 10, 3, 21, 0, 0, 0, time.UTC)
	ms := time.Millisecond
	when := func(media time.Duration, now time.Time, moved string) time.Time {
		t.Helper()
		at, why := c.when(media, now)
		require.Equal(t, moved, why)
		return at
	}

	require.Equal(t, t0, when(10*time.Second, t0, ""), "the first sample goes at once")
	require.Equal(t, t0.Add(33*ms), when(10*time.Second+33*ms, t0, ""), "the next at its timestamp")
	require.Equal(t, t0.Add(500*ms), when(10*time.Second+500*ms, t0.Add(10*ms), ""))

	// A fragment 300 ms late: sent now, and the ones after it at its pace.
	now := t0.Add(1300 * ms)
	require.Equal(t, now, when(11*time.Second, now, "late"))
	require.Equal(t, now.Add(33*ms), when(11*time.Second+33*ms, now, ""))

	// 50 ms late is within the tolerance: kept at its time.
	require.Equal(t, now.Add(100*ms), when(11*time.Second+100*ms, now.Add(150*ms), ""))

	// One that comes on time by the camera's clock waits the 300 ms the
	// late one moved the clock by: the clock does not come back.
	require.Equal(t, now.Add(500*ms), when(11*time.Second+500*ms, t0.Add(1500*ms), ""))

	// A jump of a minute ahead: the clock starts over at it.
	later := now.Add(200 * ms)
	require.Equal(t, later, when(71*time.Second, later, "ahead"))
	require.Equal(t, later.Add(33*ms), when(71*time.Second+33*ms, later, ""))
}

// What the pacer adds to the delay is measured on the earliest sample of
// each fragment, and each move of its clock is counted by why.
func TestPaceDelay(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	ctx := otx.Into(context.Background(), otx.New(otx.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))))
	s := newSource(&Relay{m: newMetrics(ctx)}, pdid.New(pdid.Domain(8)))
	s.addViewer("a.1", &tape{})
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.restartPacing()
	}()
	pace := func(media time.Duration) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.pace([]pacedSample{{media: media, data: []byte("p"), d: 33 * time.Millisecond}})
	}

	// A clock half a second behind: the fragment is late, goes at once,
	// and the clock moves to it.
	s.mu.Lock()
	s.clock = playout{set: true, wall: time.Now().Add(-500 * time.Millisecond)}
	s.mu.Unlock()
	pace(0)
	// One that came 400 ms early by that clock waits its 400 ms.
	pace(400 * time.Millisecond)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	var delays []float64
	resets := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Histogram[float64]:
				if m.Name == "shale.relay.pace_delay_ms" {
					require.Len(t, d.DataPoints, 1)
					require.Equal(t, uint64(2), d.DataPoints[0].Count)
					low, _ := d.DataPoints[0].Min.Value()
					high, _ := d.DataPoints[0].Max.Value()
					delays = append(delays, low, high)
				}
			case metricdata.Sum[int64]:
				if m.Name == "shale.relay.pace_resets" {
					for _, p := range d.DataPoints {
						why, _ := p.Attributes.Value("why")
						resets[why.AsString()] += p.Value
					}
				}
			}
		}
	}
	require.Equal(t, map[string]int64{"late": 1}, resets)
	require.Len(t, delays, 2)
	require.Zero(t, delays[0], "the late fragment waits nothing")
	require.InDelta(t, 400, delays[1], 50, "the early one waits for its time")
}

// A decode time of days in 90 kHz ticks does not overflow.
func TestTicks64(t *testing.T) {
	require.Equal(t, 10*24*time.Hour, ticks64(90000*86400*10, 90000))
	require.Equal(t, 1500*time.Millisecond, ticks64(135000, 90000))
	require.Zero(t, ticks64(5, 0))
}

// tape is a viewer that notes the video it is sent.
type tape struct{ got []string }

func (t *tape) write(sm sample)                      { t.got = append(t.got, string(sm.data)) }
func (t *tape) writeAudio(_ []byte, _ time.Duration) {}

// A joining viewer gets nothing until its connection is up, then the
// keyframe at once, the rest of the group of pictures faster than real
// time with what the pacer releases meanwhile behind it, and once caught
// up what follows, in order: never a frame twice, which a frame sent both
// live and in the group would be, nor a live frame in the middle of the
// group.
func TestCatchUp(t *testing.T) {
	s := newSource(&Relay{}, pdid.New(pdid.Domain(8)))
	release := func(out ...pacedSample) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.release(out)
	}
	got := func() []string {
		s.mu.Lock()
		defer s.mu.Unlock()
		return slices.Clone(v(s).got)
	}
	frame := 300 * time.Millisecond
	release(pacedSample{key: true, data: []byte("k"), d: frame}, pacedSample{data: []byte("p1"), d: frame})
	s.addViewer("a.1", &tape{})
	release(pacedSample{data: []byte("p2"), d: frame})
	require.Empty(t, got(), "nothing before the connection is up")

	began := time.Now()
	s.catchUp("a.1")
	require.Equal(t, []string{"k"}, got(), "the keyframe at once")
	release(pacedSample{data: []byte("p3"), d: frame})
	s.catchUp("a.1")
	require.Equal(t, []string{"k"}, got(), "a frame released while catching up waits its turn")

	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.catching) == 0
	}, 5*time.Second, 5*time.Millisecond)
	took := time.Since(began)
	require.Equal(t, []string{"k", "p1", "p2", "p3"}, got())
	require.GreaterOrEqual(t, took, 4*frame/catchUpSpeed, "faster than real time, not at once")
	require.Less(t, took, 4*frame, "faster than real time")

	release(pacedSample{data: []byte("p4"), d: frame})
	require.Equal(t, []string{"k", "p1", "p2", "p3", "p4"}, got(), "caught up: what comes goes at once")
}

// A viewer that leaves while catching up is sent nothing more.
func TestCatchUpLeaves(t *testing.T) {
	s := newSource(&Relay{}, pdid.New(pdid.Domain(8)))
	s.mu.Lock()
	s.release([]pacedSample{{key: true, data: []byte("k"), d: time.Second}, {data: []byte("p1"), d: time.Second}})
	s.mu.Unlock()
	tp := &tape{}
	s.addViewer("a.1", tp)
	s.catchUp("a.1")
	s.removeViewer("a.1")
	time.Sleep(500 * time.Millisecond)
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Equal(t, []string{"k"}, tp.got)
	require.Empty(t, s.catching)
}

// v is a source's only viewer, a tape; with the lock held.
func v(s *source) *tape {
	for _, x := range s.viewers {
		return x.(*tape)
	}

	return &tape{}
}
