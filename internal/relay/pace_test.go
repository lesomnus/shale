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

	// One that comes on time by the camera's clock would wait the 300 ms
	// the late one moved the clock by: when alone does not bring it back,
	// converge does (TestPlayoutConverges).
	require.Equal(t, now.Add(500*ms), when(11*time.Second+500*ms, t0.Add(1500*ms), ""))

	// A jump of a minute ahead: the clock starts over at it.
	later := now.Add(200 * ms)
	require.Equal(t, later, when(71*time.Second, later, "ahead"))
	require.Equal(t, later.Add(33*ms), when(71*time.Second+33*ms, later, ""))
}

// A clock behind the arrivals comes back by a fifth of each fragment's span
// at most, until a fragment's first sample no longer waits; one that is not
// behind is left alone (§39.4).
func TestPlayoutConverges(t *testing.T) {
	ms := time.Millisecond
	t0 := time.Date(2026, 11, 10, 17, 0, 0, 0, time.UTC)
	c := playout{set: true, wall: t0.Add(300 * ms), media: 0}
	// Fragments of 200 ms arrive on time by the camera's clock; the clock
	// has them wait 300 ms.
	var backs []time.Duration
	for k := range 10 {
		arrived := t0.Add(time.Duration(k) * 200 * ms)
		backs = append(backs, c.converge(time.Duration(k)*200*ms, 200*ms, arrived))
	}
	require.Equal(t, []time.Duration{40 * ms, 40 * ms, 40 * ms, 40 * ms, 40 * ms, 40 * ms, 40 * ms, 20 * ms, 0, 0}, backs)
	at, _ := c.when(2*time.Second, t0.Add(2*time.Second))
	require.Equal(t, t0.Add(2*time.Second), at, "back at the arrivals")

	// Not behind: an early arrival is not a reason to move.
	require.Zero(t, c.converge(2200*ms, 200*ms, t0.Add(2300*ms)))
	var unset playout
	require.Zero(t, unset.converge(0, 200*ms, t0))
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

// The pacer brings a clock that fell behind back over the fragments that
// follow, without sending any of them before the one before it: a lag of
// 300 ms is gone in a few fragments, and counted (§39.4).
func TestPaceRecovers(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	ctx := otx.Into(context.Background(), otx.New(otx.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))))
	s := newSource(&Relay{m: newMetrics(ctx)}, pdid.New(pdid.Domain(8)))
	s.addViewer("a.1", &tape{})
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.restartPacing()
	}()
	frame := 40 * time.Millisecond
	fragment := func(k int) []pacedSample {
		var out []pacedSample
		for i := range 5 {
			out = append(out, pacedSample{media: time.Duration(5*k+i) * frame, data: []byte("p"), d: frame})
		}
		return out
	}
	// Each fragment comes on time by the camera's clock. Rather than wait
	// out its 200 ms, the clock is moved back by them: the same to pace,
	// and no sleep for a busy test runner to stretch.
	var waits []time.Duration
	for k := range 8 {
		s.mu.Lock()
		now := time.Now()
		if k == 0 {
			// The clock 300 ms behind, as a late fragment leaves it.
			s.clock = playout{set: true, wall: now.Add(300 * time.Millisecond)}
		}
		s.pace(fragment(k))
		waits = append(waits, s.paced[len(s.paced)-5].at.Sub(now).Round(10*time.Millisecond))
		s.clock.wall = s.clock.wall.Add(-5 * frame)
		s.mu.Unlock()
	}
	require.Greater(t, waits[0], 200*time.Millisecond, "behind at first")
	for k := 1; k < len(waits); k++ {
		require.LessOrEqual(t, waits[k], waits[k-1]+10*time.Millisecond, "never further behind")
	}
	require.LessOrEqual(t, waits[len(waits)-1], 20*time.Millisecond, "back at the arrivals")

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	var recovered float64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if d, ok := m.Data.(metricdata.Sum[float64]); ok && m.Name == "shale.relay.pace_recovered_ms" {
				recovered = d.DataPoints[0].Value
			}
		}
	}
	require.InDelta(t, 300, recovered, 30)
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

// A catching-up viewer gets a quiet group of pictures, small frames, at
// catchUpSpeed; a busy one at catchUpRate on its link; and never slower
// than catchUpFloor, however big the frames (§39.4).
func TestCatchUpGap(t *testing.T) {
	frame := 40 * time.Millisecond
	quiet := sample{data: make([]byte, 2_000), d: frame}
	require.Equal(t, frame/catchUpSpeed, catchUpGap(quiet), "a quiet frame at twelve times real time")
	busy := sample{data: make([]byte, 20_000), d: frame}
	require.Equal(t, 10*time.Millisecond, catchUpGap(busy), "20 kB at 16 Mbit/s")
	key := sample{data: make([]byte, 400_000), d: frame}
	require.Equal(t, frame/catchUpFloor, catchUpGap(key), "never slower than three times real time")

	// Twelve seconds of a quiet 25 fps group catch up in about a second, as
	// the C420s' did in six.
	var took time.Duration
	for range 12 * 25 {
		took += catchUpGap(quiet)
	}
	require.InDelta(t, time.Second.Seconds(), took.Seconds(), 0.05)
}

// v is a source's only viewer, a tape; with the lock held.
func v(s *source) *tape {
	for _, x := range s.viewers {
		return x.(*tape)
	}

	return &tape{}
}
