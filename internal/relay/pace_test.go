package relay

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"
)

// The playout clock: samples go at their timestamps from the first one on;
// a late one goes now and the clock keeps its pace from there; a jump ahead
// starts the clock over.
func TestPlayout(t *testing.T) {
	var c playout
	t0 := time.Date(2026, 10, 3, 21, 0, 0, 0, time.UTC)
	ms := time.Millisecond

	require.Equal(t, t0, c.when(10*time.Second, t0), "the first sample goes at once")
	require.Equal(t, t0.Add(33*ms), c.when(10*time.Second+33*ms, t0), "the next at its timestamp")
	require.Equal(t, t0.Add(500*ms), c.when(10*time.Second+500*ms, t0.Add(10*ms)))

	// A fragment 300 ms late: sent now, and the ones after it at its pace.
	now := t0.Add(1300 * ms)
	require.Equal(t, now, c.when(11*time.Second, now))
	require.Equal(t, now.Add(33*ms), c.when(11*time.Second+33*ms, now))

	// 50 ms late is within the tolerance: kept at its time.
	require.Equal(t, now.Add(100*ms), c.when(11*time.Second+100*ms, now.Add(150*ms)))

	// A jump of a minute ahead: the clock starts over at it.
	later := now.Add(200 * ms)
	require.Equal(t, later, c.when(71*time.Second, later))
	require.Equal(t, later.Add(33*ms), c.when(71*time.Second+33*ms, later))
}

// A decode time of days in 90 kHz ticks does not overflow.
func TestTicks64(t *testing.T) {
	require.Equal(t, 10*24*time.Hour, ticks64(90000*86400*10, 90000))
	require.Equal(t, 1500*time.Millisecond, ticks64(135000, 90000))
	require.Zero(t, ticks64(5, 0))
}

// tape is a viewer that notes the video it is sent.
type tape struct{ got []string }

func (t *tape) write(data []byte, _ time.Duration)   { t.got = append(t.got, string(data)) }
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
