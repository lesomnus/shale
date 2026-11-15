package relay

import (
	"context"
	"errors"
	"time"

	"github.com/lesomnus/shale/api"
)

var errStaleGrant = errors.New("relay: stale publication grant")

// checkGrant runs with s.mu held. Managed identity and the largest CP Begin
// sequence survive detach/reauth; an older still-valid capability cannot roll
// the source back or turn it into the legacy empty-epoch path.
func (s *source) checkGrant(g *api.CaptureEpochGrant) error {
	if g == nil {
		if s.managed {
			return errStaleGrant
		}
		return nil
	}
	if g.GetSequence() < 0 || g.GetCaptureEpoch() != "" && g.GetSequence() == 0 {
		return errStaleGrant
	}
	if s.managed && (g.GetSequence() < s.epochSequence || g.GetSequence() == s.epochSequence && (s.epochDenied && g.GetCaptureEpoch() != "" || s.epoch != "" && g.GetCaptureEpoch() != "" && s.epoch != g.GetCaptureEpoch())) {
		return errStaleGrant
	}
	return nil
}

// invalidateLocked drops every old incarnation cache and wakes paced/catch-up
// writers. Peer connections are closed after unlocking, only for this source.
func (s *source) invalidateLocked() []string {
	keys := make([]string, 0, len(s.viewers))
	for key := range s.viewers {
		keys = append(keys, key)
	}
	s.viewers = map[string]sink{}
	s.joining = map[string]bool{}
	s.catching = map[string]*owed{}
	s.init, s.params, s.gop = nil, nil, nil
	s.initEpoch = ""
	s.ring.reset()
	s.seq = 0
	s.restartPacing()
	if s.idle != nil {
		s.idle.Stop()
		s.idle = nil
	}
	return keys
}

func (s *source) closeEpochPeers(keys []string) {
	if s.r.whep == nil {
		return
	}
	for _, key := range keys {
		s.r.whep.end(key)
	}
}

// authorizeLocked commits a grant after the entire attachment was validated
// with every affected source locked, and registers the stream as one of the
// source's publishers: a producer's own stream as the newest of its kind, a
// satellite's as it was (§39.3). The source's feeder is then the best of
// them. The returned action performs slow peer shutdown and stream control
// only after all source locks have been released.
func (s *source) authorizeLocked(f feeder, satellite bool, grant *api.CaptureEpochGrant, always bool, window time.Duration) func() {
	wasStarted := s.started
	var close []string
	if grant != nil {
		changed := !s.managed || grant.GetSequence() != s.epochSequence || grant.GetCaptureEpoch() != s.epoch || grant.GetCaptureEpoch() == "" && !s.epochDenied
		if changed {
			close = s.invalidateLocked()
		}
		s.managed, s.epoch, s.epochSequence = true, grant.GetCaptureEpoch(), grant.GetSequence()
		s.epochDenied = grant.GetCaptureEpoch() == ""
		s.stopped = s.epochDenied
	}
	p := s.publishers[f]
	if p == nil {
		p = &publisher{satellite: satellite}
		s.publishers[f] = p
	}
	p.grant, p.always, p.window = grant, always, window
	if !satellite {
		// A producer's stream feeds every source of its token, and the
		// one authorized last is the newest of its streams.
		p.feeds, p.order = true, s.r.sources.next()
	}
	old := s.feeder
	next := s.best()
	s.feeder = next
	if next != nil {
		np := s.publishers[next]
		s.always, s.window = np.always, np.window
	}
	// A managed viewer capability requires an init from this incarnation.
	// Prime on-demand sources too, otherwise the first viewer cannot open.
	prime := s.managed && !s.stopped && s.init == nil
	s.started = next != nil && !s.stopped && (s.always || len(s.viewers) > 0 || prime)
	start := s.started && (!wasStarted || old != next || len(close) > 0)
	stop := wasStarted && old != nil && (old != next || !s.started)
	if next == nil && old != nil {
		s.gop = nil
		s.ring.reset()
		s.seq = 0
		s.restartPacing()
	}
	if old != nil && next != nil && old != next && s.r.m != nil {
		s.r.m.handoffs.Add(context.Background(), 1)
	}
	return func() {
		s.closeEpochPeers(close)
		if stop {
			old.stop(s.id)
		}
		if start {
			next.start(s.id)
		}
	}
}

// resetEpoch takes a Reset from the source's feeder, and from the producer's
// own stream while a satellite feeds the source: the capture's incarnation
// is the producer's to say, whoever carries its bytes (§41.8).
func (s *source) resetEpoch(f feeder, grant *api.CaptureEpochGrant, epoch string, stopped bool) bool {
	s.mu.Lock()
	if !s.controlledBy(f) || s.checkGrant(grant) != nil || s.managed && (grant == nil || epoch != grant.GetCaptureEpoch() || !stopped && epoch == "") || !s.managed && epoch != "" {
		s.mu.Unlock()
		return false
	}
	if s.managed && !stopped && (s.epochDenied || s.stopped) {
		s.mu.Unlock()
		return false
	}
	if epoch == s.epoch && stopped == s.stopped {
		// A refreshed signed grant can reach the relay before Begin's reply
		// installs the Producer's local epoch. Its first Start is then ignored.
		// This retained current Reset acknowledges local readiness: repeat the
		// prime until init arrives, without disturbing established sources.
		prime := s.managed && !stopped && s.started && s.init == nil
		feeder := s.feeder
		s.mu.Unlock()
		if prime && feeder != nil {
			feeder.start(s.id)
		}
		return true
	}
	keys := s.invalidateLocked()
	s.epoch, s.stopped = epoch, stopped
	if stopped {
		s.started = false
	}
	// Stop is an irreversible denial at this Begin sequence. Resuming a
	// managed capture needs a fresh, larger signed Begin sequence.
	if stopped && s.managed {
		s.epochDenied = true
	}
	s.mu.Unlock()
	s.closeEpochPeers(keys)
	return true
}

// controlledBy says whether a stream's Reset is taken: the feeder's, and the
// producer's own stream's while a satellite feeds the source; with the lock
// held.
func (s *source) controlledBy(f feeder) bool {
	if s.feeder == f {
		return true
	}
	p, cur := s.publishers[f], s.publishers[s.feeder]

	return p != nil && !p.satellite && cur != nil && cur.satellite
}

// removePublisherLocked participates in the same multi-source commit as
// authorizeLocked, including the old membership's removed sources. A
// satellite's stream whose token no longer names a source hands it to the
// next feeder; the producer's own stream that no longer names it takes the
// source off the relay.
func (s *source) removePublisherLocked(f feeder) func() {
	p := s.publishers[f]
	delete(s.publishers, f)
	if s.feeder != f {
		return nil
	}
	if p != nil && p.satellite {
		return s.electLocked()
	}
	keys := s.invalidateLocked()
	s.feeder, s.started = nil, false
	if s.managed {
		s.epochDenied, s.stopped = true, true
	}
	return func() { s.closeEpochPeers(keys) }
}

// viewEpochLocked also requires an init received under the exact managed
// epoch. Legacy sources preserve their historical ability to open before init.
func (s *source) viewEpochLocked(expected string) bool {
	if s.stopped || s.epochDenied {
		return false
	}
	if s.managed {
		return expected != "" && expected == s.epoch && s.init != nil && s.initEpoch == expected
	}
	return expected == "" && s.epoch == ""
}

func (s *source) viewEpoch(expected string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.viewEpochLocked(expected)
}

func (s *source) viewSnapshot(expected string) (int64, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.viewEpochLocked(expected) {
		return 0, false, false
	}
	h265 := false
	if s.init != nil {
		if v := s.init.Video(); v != nil {
			h265 = v.H265()
		}
	}
	return s.epochSequence, h265, true
}
