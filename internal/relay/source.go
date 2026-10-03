package relay

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/fmp4"
)

// A source is one camera as the relay sees it (§39.3, §39.4): the producer
// stream that feeds it, the group of pictures since the last keyframe, the
// recent window, and the viewers watching. Bytes flow at all times or only
// while someone watches, as the producer's live policy says.

// feeder is what a source asks of the producer attached to it.
type feeder interface {
	start(source pdid.Id)
	stop(source pdid.Id)
}

// sink is where a source's samples go: one per viewer. A video sample is
// Annex B, a keyframe with its parameter sets in front; an audio sample
// is one Opus packet.
type sink interface {
	write(data []byte, d time.Duration)
	writeAudio(data []byte, d time.Duration)
}

// sample is one video access unit kept for a joining viewer.
type sample struct {
	data []byte
	d    time.Duration
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
	// init is the stream's init segment, once it came; params the video
	// track's parameter sets in Annex B, put in front of every keyframe a
	// viewer gets.
	init   *fmp4.Init
	params []byte
	gop    []sample
	// The recent window (§39.4): as long as the token said, none when it
	// said nothing.
	window time.Duration
	ring   *ring
	// seq is the last fragment's number, 0 before the first.
	seq uint32
	// The samples fanOut found, sent to the viewers when their timestamps
	// say (pace.go); pacing says a pacer runs.
	paced   []pacedSample
	clock   playout
	pacing  bool
	viewers map[string]sink
	idle    *time.Timer
	// Bytes and units, for the heartbeat.
	bytes int64
}

func newSource(r *Relay, id pdid.Id) *source {
	return &source{id: id, r: r, viewers: map[string]sink{}, ring: newRing()}
}

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
	// A new stream begins with its own init segment: what the ring holds
	// is another's.
	s.ring.reset()
	s.seq = 0
	s.restartPacing()
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
// when the producer is back. The init segment stays: a producer that
// attaches again sends the same camera.
func (s *source) detach(f feeder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.feeder == f {
		s.feeder = nil
		s.started = false
		s.gop = nil
		s.ring.reset()
		s.seq = 0
		s.restartPacing()
	}
}

var errNoInit = errors.New("a fragment before any init segment")

// feed takes one unit from the producer (§39.3): an init segment, or one
// fragment as it came.
func (s *source) feed(b []byte) {
	s.r.m.ingress.Add(context.Background(), int64(len(b)))
	now := time.Now()
	s.mu.Lock()
	s.bytes += int64(len(b))
	if _, ok := fmp4.Find(b, "moov"); ok {
		init, err := fmp4.ParseInit(b)
		if err != nil {
			s.r.log.Warn("ingest", "source", s.id.String(), "err", err.Error())
			s.mu.Unlock()
			return
		}
		s.init = init
		s.params = nil
		if v := init.Video(); v != nil {
			s.params = v.ParamSets()
		}
		// A new stream: what came before it does not join what follows.
		s.ring.reset()
		s.seq = 0
		s.restartPacing()
		s.mu.Unlock()
		return
	}
	if s.init == nil {
		s.r.log.Warn("ingest", "source", s.id.String(), "err", errNoInit.Error())
		s.mu.Unlock()
		return
	}
	frag, err := fmp4.ParseFragment(b, s.init)
	if err != nil {
		s.r.log.Warn("ingest", "source", s.id.String(), "err", err.Error())
		s.mu.Unlock()
		return
	}
	torn := s.seq != 0 && frag.Seq != s.seq+1
	s.seq = frag.Seq
	if s.window > 0 {
		// The recent window (§39.4): the fragments as they came, the
		// keys to start from, and the tears to start after.
		off := s.ring.add(b, now)
		if torn {
			s.ring.tear(off)
		}
		if v := frag.Video(s.init); v != nil {
			if frag.Key(s.init) {
				s.ring.key(off, v.Time, now)
			}
			s.ring.unit(v.Time + v.Duration())
		}
		s.ring.trim(now.Add(-s.window))
	}
	s.fanOut(frag)
	s.mu.Unlock()
	if s.window > 0 {
		s.r.sources.enforce()
	}
}

// recent is the recent window from the oldest keyframe that came within
// `since` of now (the whole window when zero), as one fragmented MP4 with
// the init segment first, with when that keyframe came and how long the
// window from it is (§39.4).
func (s *source) recent(since time.Duration) ([]byte, time.Time, float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.init == nil {
		return nil, time.Time{}, 0, false
	}
	var from time.Time
	if since > 0 {
		from = time.Now().Add(-since)
	}
	k, ok := s.ring.start(from)
	if !ok {
		return nil, time.Time{}, 0, false
	}
	var scale int64
	if v := s.init.Video(); v != nil {
		scale = int64(v.Timescale)
	}

	return s.ring.snapshot(k, s.init.Bytes), k.at, s.ring.seconds(k, scale), true
}

// fanOut takes a fragment's samples, each with when it plays, to be sent
// to the viewers when that comes (pace.go); with the lock held.
func (s *source) fanOut(frag *fmp4.Fragment) {
	var out []pacedSample
	if opus := s.init.Opus(); opus != nil {
		if tr := frag.Traf(opus.ID); tr != nil {
			t := ticks64(tr.Time, opus.Timescale)
			for _, sm := range tr.Samples {
				d := ticks(sm.Duration, opus.Timescale, 20*time.Millisecond)
				out = append(out, pacedSample{media: t, audio: true, data: frag.Bytes[sm.Off : sm.Off+sm.Size], d: d})
				t += d
			}
		}
	}
	if video := s.init.Video(); video != nil {
		if tr := frag.Traf(video.ID); tr != nil {
			n := video.LengthSize()
			t := ticks64(tr.Time, video.Timescale)
			for _, sm := range tr.Samples {
				raw := frag.Bytes[sm.Off : sm.Off+sm.Size]
				key := video.SampleKey(raw, sm)
				data := fmp4.AnnexB(raw, n)
				if key && len(s.params) > 0 {
					data = append(append([]byte(nil), s.params...), data...)
				}
				d := ticks(sm.Duration, video.Timescale, fallbackDuration)
				out = append(out, pacedSample{media: t, key: key, data: data, d: d})
				t += d
			}
		}
	}
	s.pace(out)
}

// fallbackDuration is a frame's duration when the stream does not say.
const fallbackDuration = 40 * time.Millisecond

// ticks64 is a decode time in a track's timescale as time.
func ticks64(n int64, timescale uint32) time.Duration {
	if timescale == 0 {
		return 0
	}

	ts := int64(timescale)
	// Whole seconds first: a stream's decode time in 90 kHz ticks times a
	// nanosecond's second overflows in about a day.
	return time.Duration(n/ts)*time.Second + time.Duration(n%ts)*time.Second/time.Duration(ts)
}

// ticks is a duration in a track's timescale as time, or the fallback
// when the stream does not say.
func ticks(n uint32, timescale uint32, fallback time.Duration) time.Duration {
	if n == 0 || timescale == 0 {
		return fallback
	}

	return time.Duration(n) * time.Second / time.Duration(timescale)
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
	gop := append([]sample(nil), s.gop...)
	s.mu.Unlock()

	if first {
		f.start(s.id)
	}
	for _, sm := range gop {
		v.write(sm.data, sm.d)
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
	gop := append([]sample(nil), s.gop...)
	s.mu.Unlock()
	for _, sm := range gop {
		v.write(sm.data, sm.d)
	}
}

// h265 says the source's video is H.265, once its init segment came.
func (s *source) h265() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.init == nil {
		return false
	}
	v := s.init.Video()

	return v != nil && v.H265()
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
