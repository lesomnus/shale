package relay

import (
	"context"
	"sync"
	"time"

	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/mpegts"
)

// A source is one camera as the relay sees it (§39.3, §39.4): the producer
// stream that feeds it, the group of pictures since the last keyframe, and
// the viewers watching. Bytes flow at all times or only while someone
// watches, as the producer's live policy says.

// feeder is what a source asks of the producer attached to it.
type feeder interface {
	start(source pdid.Id)
	stop(source pdid.Id)
}

// sink is where a source's access units go: one per viewer.
type sink interface {
	write(au mpegts.AccessUnit, d time.Duration)
	writeAudio(u mpegts.AudioUnit, d time.Duration)
}

type source struct {
	id pdid.Id
	r  *Relay

	mu     sync.Mutex
	feeder feeder
	// always says the feeder's policy is `always` (§39.3): started at
	// attach, and never stopped for want of viewers.
	always  bool
	started bool
	demux   *mpegts.Demuxer
	gop     []mpegts.AccessUnit
	// The recent window (§39.4): as long as the token said, none when it
	// said nothing.
	window time.Duration
	ring   *ring
	lastPTS  int64
	lastAPTS int64
	viewers  map[string]sink
	idle     *time.Timer
	// Bytes and units, for the heartbeat.
	bytes int64
}

func newSource(r *Relay, id pdid.Id) *source {
	return &source{id: id, r: r, demux: mpegts.New(), lastPTS: -1, lastAPTS: -1, viewers: map[string]sink{}, ring: newRing()}
}

// fallbackDuration is a frame's duration when the stamps do not say.
const fallbackDuration = 40 * time.Millisecond

// attach makes a producer stream the feeder of this source; a stream
// already feeding it is superseded (the producer re-attached). `always`
// is the producer's live policy for this attachment (§39.3) and `window`
// how much of the stream is kept for the recent window (§39.4).
func (s *source) attach(f feeder, always bool, window time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.feeder != nil && s.feeder != f {
		// A second stream from the producer replaces the first; the
		// first's end then changes nothing (detach compares).
		s.r.log.Info("producer re-attached", "source", s.id.String())
	}
	s.feeder = f
	s.always = always
	s.window = window
	// A new stream begins at offset 0: what the ring holds is another's.
	s.ring.reset()
	if s.idle != nil {
		s.idle.Stop()
		s.idle = nil
	}
	// `always`, or a viewer was waiting: ask for bytes right away.
	if always || len(s.viewers) > 0 {
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
		s.demux = s.freshDemux()
		s.gop = nil
		s.ring.reset()
	}
}

// freshDemux is a demuxer for the stream to come, which knows the
// parameter sets the last one saw: a producer that attaches again sends
// the same camera, and one whose encoder wrote them once will not write
// them again (#84).
func (s *source) freshDemux() *mpegts.Demuxer {
	d := mpegts.New()
	if s.demux != nil {
		d.SetParams(s.demux.Params())
	}

	return d
}

// feed takes TS bytes from the producer.
func (s *source) feed(b []byte) {
	s.r.m.ingress.Add(context.Background(), int64(len(b)))
	now := time.Now()
	s.mu.Lock()
	s.bytes += int64(len(b))
	off := s.demux.Pos()
	units, audio, err := s.demux.WriteAll(b)
	if err != nil {
		s.r.log.Warn("ingest", "source", s.id.String(), "err", err.Error())
		s.demux = s.freshDemux()
		s.ring.reset()
		s.mu.Unlock()
		return
	}
	if s.window > 0 {
		// The recent window (§39.4): the bytes as they came, the
		// keyframes to start from, and the tears to start after.
		s.ring.add(b, off, now)
		for _, au := range units {
			if au.Torn {
				s.ring.tear(au.Offset)
			}
			if au.Keyframe {
				s.ring.key(au.Offset, au.PTS, au.CC, now, au.HasParams)
			}
			s.ring.unit(au.PTS)
		}
		s.ring.trim(now.Add(-s.window))
	}
	s.fanOut(units, audio)
	s.mu.Unlock()
	if s.window > 0 {
		s.r.sources.enforce()
	}
}

// recent is the recent window from the oldest keyframe that came within
// `since` of now (the whole window when zero), as one TS, with when that
// keyframe came and how long the window from it is (§39.4).
func (s *source) recent(since time.Duration) ([]byte, time.Time, float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var from time.Time
	if since > 0 {
		from = time.Now().Add(-since)
	}
	k, ok := s.ring.start(from)
	if !ok {
		return nil, time.Time{}, 0, false
	}
	tables := s.demux.Tables()
	if tables == nil {
		return nil, time.Time{}, 0, false
	}

	end := s.demux.OpenAt()
	if end >= 0 && end <= k.off {
		// The keyframe itself is still being assembled: nothing whole yet.
		return nil, time.Time{}, 0, false
	}

	return s.ring.snapshot(k, end, tables, s.demux.Params(), s.demux.VideoPID()), k.at, s.ring.seconds(k), true
}

// fanOut hands the units to the viewers and keeps the group of pictures;
// with the lock held.
func (s *source) fanOut(units []mpegts.AccessUnit, audio []mpegts.AudioUnit) {
	for _, u := range audio {
		d := mpegts.Duration(s.lastAPTS, u.PTS, 20*time.Millisecond)
		s.lastAPTS = u.PTS
		for _, v := range s.viewers {
			v.writeAudio(u, d)
		}
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
// which absorbs a page reload (§39.3); a source fed under `always` is
// never stopped for want of viewers.
func (s *source) removeViewer(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.viewers, key)
	if len(s.viewers) > 0 || !s.started || s.always {
		return
	}
	if s.idle != nil {
		s.idle.Stop()
	}
	s.idle = time.AfterFunc(s.r.cfg.IdleStop, func() {
		s.mu.Lock()
		if len(s.viewers) > 0 || !s.started || s.always {
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
	var ingress int64
	for _, s := range ss.byId {
		s.mu.Lock()
		if s.started {
			active++
		}
		viewers += int32(len(s.viewers))
		ingress += s.bytes
		s.mu.Unlock()
	}
	st.ActiveSources = active
	st.Viewers = viewers
	st.IngressBytes = ingress
	st.RewindBytes = ss.held()
	st.RewindBudget = ss.r.cfg.RewindBudget

	return st.Build()
}

// held is the bytes the recent windows hold together; with the registry
// locked.
func (ss *sources) held() int64 {
	var n int64
	for _, s := range ss.byId {
		s.mu.Lock()
		n += s.ring.bytes
		s.mu.Unlock()
	}

	return n
}

// enforce keeps the recent windows together within the budget (§39.5):
// over it, the largest window loses its oldest bytes, until they fit.
func (ss *sources) enforce() {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	budget := ss.r.cfg.RewindBudget
	if budget <= 0 {
		return
	}
	for range 64 {
		over := ss.held() - budget
		if over <= 0 {
			return
		}
		var largest *source
		var most int64
		for _, s := range ss.byId {
			s.mu.Lock()
			if s.ring.bytes > most {
				largest, most = s, s.ring.bytes
			}
			s.mu.Unlock()
		}
		if largest == nil {
			return
		}
		largest.mu.Lock()
		largest.ring.evict(min(over, most))
		largest.mu.Unlock()
	}
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
