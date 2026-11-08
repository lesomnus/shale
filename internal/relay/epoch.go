package relay

import (
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

func (s *source) authorize(f feeder, grant *api.CaptureEpochGrant, always bool, window time.Duration) error {
	s.mu.Lock()
	if err := s.checkGrant(grant); err != nil {
		s.mu.Unlock()
		return err
	}
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
	old := s.feeder
	s.feeder, s.always, s.window = f, always, window
	// A managed viewer capability requires an init from this incarnation.
	// Prime on-demand sources too, otherwise the first viewer cannot open.
	prime := s.managed && !s.stopped && s.init == nil
	start := !s.stopped && (always || len(s.viewers) > 0 || prime) && (!s.started || old != f || len(close) > 0)
	s.started = !s.stopped && (always || len(s.viewers) > 0 || prime)
	stop := wasStarted && !s.started
	s.mu.Unlock()
	s.closeEpochPeers(close)
	if start {
		f.start(s.id)
	}
	if stop {
		f.stop(s.id)
	}
	return nil
}

func (s *source) resetEpoch(f feeder, grant *api.CaptureEpochGrant, epoch string, stopped bool) bool {
	s.mu.Lock()
	if s.feeder != f || s.checkGrant(grant) != nil || s.managed && (grant == nil || !stopped && (epoch == "" || epoch != grant.GetCaptureEpoch())) || !s.managed && epoch != "" {
		s.mu.Unlock()
		return false
	}
	if s.managed && !stopped && (s.epochDenied || s.stopped) {
		s.mu.Unlock()
		return false
	}
	if epoch == s.epoch && stopped == s.stopped {
		s.mu.Unlock()
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

func (s *source) removePublisher(f feeder) {
	s.mu.Lock()
	if s.feeder != f {
		s.mu.Unlock()
		return
	}
	keys := s.invalidateLocked()
	s.feeder, s.started = nil, false
	if s.managed {
		s.epochDenied, s.stopped = true, true
	}
	s.mu.Unlock()
	s.closeEpochPeers(keys)
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
