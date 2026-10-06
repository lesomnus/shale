package producer

import (
	"context"
	"strings"
	"sync"
	"time"
)

// A capture that stalls stops delivering fragments, and what comes after
// may say nothing of it: a remux stage rebases the timestamps across the
// hole, so the fMP4 the producer reads is continuous, no frame is long, and
// accountGap counts nothing (§38.6). The wall clock does not rebase. A
// stall is told from two things measured against it: the time between
// fragments, which is the stall whatever the timestamps say, and the wall
// time that passed beyond the media time, which is the part of it the
// timestamps hid.

// stallAfter is how long a source may deliver nothing before it is
// stalled: past a few fragments of any cadence the producer writes, and
// past two keyframe intervals for a stream that brings one per fragment.
func stallAfter(key time.Duration) time.Duration {
	return max(5*time.Second, 2*key)
}

// stallTracker is one stream's watch for wall time without media.
type stallTracker struct {
	mu sync.Mutex
	// last is when the last fragment came; zero before the first.
	last time.Time
	// lead is how far the wall clock was ahead of the media at the last
	// fragment; held says it was measured.
	lead time.Duration
	held bool
	// warned says the quiet was reported while it lasted.
	warned bool
}

// reset forgets the media clock: a new init segment starts its own.
func (t *stallTracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.held = false
}

// seen takes a fragment with video, arriving at now, whose media begins at
// media. It answers the wall time since the fragment before, how much of
// that the media does not account for, and whether the quiet was already
// reported.
func (t *stallTracker) seen(now time.Time, media time.Duration) (quiet, hidden time.Duration, warned bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.last.IsZero() {
		quiet = now.Sub(t.last)
	}
	lead := time.Duration(now.UnixNano()) - media
	if t.held {
		hidden = lead - t.lead
	}
	warned = t.warned
	t.last, t.lead, t.held, t.warned = now, lead, true, false

	return quiet, hidden, warned
}

// quiet answers how long the stream has delivered nothing at now, once:
// true the first time it is past after, and not again until a fragment
// comes.
func (t *stallTracker) quiet(now time.Time, after time.Duration) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.last.IsZero() || t.warned {
		return 0, false
	}
	d := now.Sub(t.last)
	if d <= after {
		return 0, false
	}
	t.warned = true

	return d, true
}

// keyInterval is the source's measured keyframe interval, zero before one.
func (s *source) keyInterval() time.Duration {
	if s.cutter == nil {
		return 0
	}

	return s.cutter.Stats().KeyInterval
}

// watchStall reports a stream that has gone quiet while it is quiet, so the
// upload errors that follow read as what they are. It returns when ctx ends.
func (p *Producer) watchStall(ctx context.Context, s *source, t *stallTracker) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if d, ok := t.quiet(p.now(), stallAfter(s.keyInterval())); ok {
			p.log.Warn("capture stalled: the source delivers nothing", "source", s.cfg.Alias, "for", d.Round(time.Second).String())
		}
	}
}

// accountStall counts what seen measured (§38.6): a quiet past stallAfter
// is a stall, and wall time past the media is frames that never came, which
// accountGap did not see because the timestamps hid them.
func (p *Producer) accountStall(s *source, quiet, hidden time.Duration, warned bool) {
	after := stallAfter(s.keyInterval())
	attr := sourceAttr(s.cfg.Alias)
	if quiet > after {
		p.m.stalls.Add(context.Background(), 1, attr)
		p.m.stalled.Add(context.Background(), quiet.Seconds(), attr)
	}
	if hidden > after {
		fps := s.cfg.Fps
		if fps <= 0 {
			fps = 30
		}
		frame := time.Second / time.Duration(fps)
		p.m.frameGaps.Add(context.Background(), 1, attr)
		p.m.framesMissed.Add(context.Background(), int64((hidden+frame/2)/frame), attr)
	}
	if quiet > after || warned {
		p.log.Warn("capture resumed after delivering nothing", "source", s.cfg.Alias, "for", quiet.Round(100*time.Millisecond).String(), "timestamps_hid", hidden > after)
	}
}

// discontinuity is a capture's stderr line saying its timestamps jumped
// and were rebased, which is how a stall reads to ffmpeg's MPEG-TS demuxer:
// counted and logged as a warning, where it would be one more line at info.
func (p *Producer) discontinuity(s *source, line string) bool {
	if !strings.Contains(line, "timestamp discontinuity") {
		return false
	}
	p.m.discontinuities.Add(context.Background(), 1, sourceAttr(s.cfg.Alias))
	p.log.Warn("capture: the timestamps jumped and ffmpeg rebased them; frames missed there do not show in the recording", "source", s.cfg.Alias, "said", line)

	return true
}
