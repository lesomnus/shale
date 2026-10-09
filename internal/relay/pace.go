package relay

import (
	"sort"
	"time"
)

// Pacing (§39.4). A producer sends a fragment at a time, half a second of
// frames each, and the relay used to hand a fragment's frames to its viewers
// the moment it came: every half second a burst, then nothing. A browser's
// jitter buffer starts small, so playback stalled at the fragments' pace
// until it had grown to a fragment; a viewer saw the picture stop and go,
// worst right after joining. The relay now sends each sample when its
// timestamp says, so what a viewer gets is as even as what the camera took.

// pacedSample is one sample waiting for its time: when it plays on the
// stream's clock, and what it is.
type pacedSample struct {
	media time.Duration
	at    time.Time
	audio bool
	key   bool
	data  []byte
	d     time.Duration
}

// playout maps the stream's clock onto the wall clock: a sample at media
// time m is sent at wall + (m - media).
type playout struct {
	set   bool
	wall  time.Time
	media time.Duration
}

const (
	// paceLate is how far behind its time a sample may be before the clock
	// moves to now: a fragment that came late is sent at once, and those
	// after it keep its new pace rather than catch up in a burst.
	paceLate = 100 * time.Millisecond
	// paceAhead is how far ahead a sample's time may be before the clock
	// starts over at it: a jump in the stream's timestamps, not a wait.
	paceAhead = 3 * time.Second
)

// when is a sample's wall time, moving the clock as paceLate and paceAhead
// say.
func (c *playout) when(media time.Duration, now time.Time) time.Time {
	if !c.set {
		c.set, c.wall, c.media = true, now, media
	}
	at := c.wall.Add(media - c.media)
	switch {
	case at.Before(now.Add(-paceLate)), at.After(now.Add(paceAhead)):
		c.wall, c.media = now, media
		at = now
	}

	return at
}

// pace takes a fragment's samples; with the lock held. With nobody watching
// they are not paced, only kept for the group of pictures a joining viewer
// gets; otherwise they wait for their time in order and a pacer sends
// them.
func (s *source) pace(out []pacedSample) {
	if len(s.viewers) == 0 && len(s.paced) == 0 {
		s.clock = playout{}
		s.release(out)
		return
	}
	// Audio and video in the order they play, so neither waits behind the
	// other's later samples at the head of the queue.
	sort.SliceStable(out, func(i, j int) bool { return out[i].media < out[j].media })
	now := time.Now()
	for i := range out {
		out[i].at = s.clock.when(out[i].media, now)
	}
	s.paced = append(s.paced, out...)
	if !s.pacing {
		s.pacing = true
		go s.runPacer()
	}
}

// runPacer sends the waiting samples as their times come, and ends when
// none wait.
func (s *source) runPacer() {
	for {
		s.mu.Lock()
		if len(s.paced) == 0 {
			s.pacing = false
			s.mu.Unlock()
			return
		}
		wait := time.Until(s.paced[0].at)
		if wait > 0 {
			s.mu.Unlock()
			time.Sleep(wait)
			continue
		}
		now := time.Now()
		k := 0
		for k < len(s.paced) && !s.paced[k].at.After(now) {
			k++
		}
		due := s.paced[:k]
		s.release(due)
		s.paced = append(s.paced[:0:0], s.paced[k:]...)
		s.mu.Unlock()
	}
}

// release sends samples to the viewers and keeps the video's group of
// pictures for those who join; with the lock held.
func (s *source) release(out []pacedSample) {
	for _, sm := range out {
		if sm.audio {
			// A viewer catching up gets the sound from the live edge on,
			// where its picture is then.
			for k, v := range s.viewers {
				if !s.joining[k] && s.catching[k] == nil {
					v.writeAudio(sm.data, sm.d)
				}
			}
			continue
		}
		if sm.key {
			s.gop = s.gop[:0]
		}
		if sm.key || len(s.gop) > 0 {
			s.gop = append(s.gop, sample{data: sm.data, d: sm.d})
		}
		for k, v := range s.viewers {
			switch o := s.catching[k]; {
			case s.joining[k]:
			case o != nil:
				o.samples = append(o.samples, sample{data: sm.data, d: sm.d})
			default:
				v.write(sm.data, sm.d)
			}
		}
	}
}

// restartPacing drops what waits and starts the clock over: a new stream,
// or none; with the lock held. A running pacer finds nothing and ends.
func (s *source) restartPacing() {
	s.paced = nil
	s.clock = playout{}
}
