package relay

import (
	"sync"
	"time"

	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/mpegts"
)

// A source is one camera as the relay sees it (§39.3, §39.4): the producer
// stream that feeds it, the group of pictures since the last keyframe, and
// the viewers watching. Bytes flow only while someone watches.

// feeder is what a source asks of the producer attached to it.
type feeder interface {
	start(source pdid.Id)
	stop(source pdid.Id)
}

// sink is where a source's access units go: one per viewer.
type sink interface {
	write(au mpegts.AccessUnit, d time.Duration)
}

type source struct {
	id pdid.Id
	r  *Relay

	mu      sync.Mutex
	feeder  feeder
	started bool
	demux   *mpegts.Demuxer
	gop     []mpegts.AccessUnit
	lastPTS int64
	viewers map[string]sink
	idle    *time.Timer
	// Bytes and units, for the heartbeat.
	bytes int64
}

func newSource(r *Relay, id pdid.Id) *source {
	return &source{id: id, r: r, demux: mpegts.New(), lastPTS: -1, viewers: map[string]sink{}}
}

// fallbackDuration is a frame's duration when the stamps do not say.
const fallbackDuration = 40 * time.Millisecond

// attach makes a producer stream the feeder of this source; a stream
// already feeding it is superseded (the producer re-attached).
func (s *source) attach(f feeder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.feeder = f
	// A viewer was waiting: ask for bytes right away.
	if len(s.viewers) > 0 {
		s.started = true
		go f.start(s.id)
	} else {
		s.started = false
	}
}

// detach forgets a feeder that went away; viewers stay and get bytes again
// when the producer is back.
func (s *source) detach(f feeder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.feeder == f {
		s.feeder = nil
		s.started = false
		s.demux = mpegts.New()
		s.gop = nil
	}
}

// feed takes TS bytes from the producer.
func (s *source) feed(b []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bytes += int64(len(b))
	units, err := s.demux.Write(b)
	if err != nil {
		s.r.log.Warn("ingest", "source", s.id.String(), "err", err.Error())
		s.demux = mpegts.New()
		return
	}
	for _, au := range units {
		d := mpegts.Duration(s.lastPTS, au.PTS, fallbackDuration)
		s.lastPTS = au.PTS
		if au.Keyframe {
			s.gop = s.gop[:0]
		}
		if au.Keyframe || len(s.gop) > 0 {
			s.gop = append(s.gop, au)
		}
		for _, v := range s.viewers {
			v.write(au, d)
		}
	}
}

// addViewer starts the feed when this is the first viewer, and hands the
// current group of pictures to the viewer so it starts at once (§39.4).
func (s *source) addViewer(key string, v sink) {
	s.mu.Lock()
	s.viewers[key] = v
	if s.idle != nil {
		s.idle.Stop()
		s.idle = nil
	}
	f := s.feeder
	first := !s.started && f != nil
	if first {
		s.started = true
	}
	gop := append([]mpegts.AccessUnit(nil), s.gop...)
	s.mu.Unlock()

	if first {
		f.start(s.id)
	}
	var prev int64 = -1
	for _, au := range gop {
		v.write(au, mpegts.Duration(prev, au.PTS, fallbackDuration))
		prev = au.PTS
	}
}

// removeViewer stops the feed relay_idle_stop after the last viewer left,
// which absorbs a page reload (§39.3).
func (s *source) removeViewer(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.viewers, key)
	if len(s.viewers) > 0 || !s.started {
		return
	}
	if s.idle != nil {
		s.idle.Stop()
	}
	s.idle = time.AfterFunc(s.r.cfg.IdleStop, func() {
		s.mu.Lock()
		if len(s.viewers) > 0 || !s.started {
			s.mu.Unlock()
			return
		}
		s.started = false
		f := s.feeder
		s.gop = nil
		s.mu.Unlock()
		if f != nil {
			f.stop(s.id)
		}
	})
}

// catchUp hands a viewer the group of pictures again, for a viewer whose
// connection came up after it was added.
func (s *source) catchUp(v sink) {
	s.mu.Lock()
	gop := append([]mpegts.AccessUnit(nil), s.gop...)
	s.mu.Unlock()
	var prev int64 = -1
	for _, au := range gop {
		v.write(au, mpegts.Duration(prev, au.PTS, fallbackDuration))
		prev = au.PTS
	}
}

// ---- the registry ---------------------------------------------------------

type sources struct {
	r    *Relay
	mu   sync.Mutex
	byId map[pdid.Id]*source
	// producers counts attached producer streams.
	producers int
}

func newSources(r *Relay) *sources { return &sources{r: r, byId: map[pdid.Id]*source{}} }

func (ss *sources) get(id pdid.Id) *source {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	s := ss.byId[id]
	if s == nil {
		s = newSource(ss.r, id)
		ss.byId[id] = s
	}

	return s
}

func (ss *sources) attached(delta int) {
	ss.mu.Lock()
	ss.producers += delta
	ss.mu.Unlock()
}

// status is what the heartbeat reports (§39.2).
func (ss *sources) status() *api.RelayStatus {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	st := api.RelayStatus_builder{AttachedProducers: int32(ss.producers)}
	var active, viewers int32
	for _, s := range ss.byId {
		s.mu.Lock()
		if s.started {
			active++
		}
		viewers += int32(len(s.viewers))
		s.mu.Unlock()
	}
	st.ActiveSources = active
	st.Viewers = viewers

	return st.Build()
}

// viewerCount is how many sessions are open, and how many for one actor.
func (ss *sources) viewerCount(actor string) (total, mine int) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	for _, s := range ss.byId {
		s.mu.Lock()
		for key := range s.viewers {
			total++
			if actorOf(key) == actor {
				mine++
			}
		}
		s.mu.Unlock()
	}

	return total, mine
}

// sampleOf is an access unit as a track sample.
func sampleOf(au mpegts.AccessUnit, d time.Duration) media.Sample {
	return media.Sample{Data: au.Data, Duration: d}
}
