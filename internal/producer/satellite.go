package producer

import (
	"bytes"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/satellite"
	"github.com/lesomnus/shale/internal/upload"
)

// Uploading through a satellite (§41.5): the same requests, paths and
// tokens a node is sent, sent to the satellite the CP assigned instead,
// which forwards each to the node its token names. Nothing else of the
// upload changes: a node's answer comes back through the satellite as the
// node gave it and is handled as it always was. What is new is a failure
// of the satellite itself, which is the satellite's and not the node's.

// Via is the path uploads take: `producer.upload_via`.
type Via string

const (
	// ViaDirect is to the node an allocation names, as before satellites:
	// the default.
	ViaDirect Via = "direct"
	// ViaSatellite is through the assigned satellite, and directly while
	// none is assigned. A satellite that cannot be reached is waited for,
	// and the CP assigns another, or none, once it has not been heard for
	// satellite_down_after.
	ViaSatellite Via = "satellite"
	// ViaAuto is through the assigned satellite, and directly for a while
	// once the satellite cannot be reached.
	ViaAuto Via = "auto"
)

// LiveFeed is who feeds the relay a source whose live uploads go through
// the satellite (§41.8): `producer.live_feed`.
type LiveFeed string

const (
	// LiveFeedAuto hands the satellite the live of every source whose live
	// uploads go through it and that needs no live helper, so the uplink
	// carries each byte once: the default. It changes nothing while uploads
	// go directly.
	LiveFeedAuto LiveFeed = "auto"
	// LiveFeedProducer keeps the producer's own tee for every source, as
	// before satellites.
	LiveFeedProducer LiveFeed = "producer"
)

// ParseLiveFeed reads `auto` or `producer`; empty is LiveFeedAuto.
func ParseLiveFeed(s string) (LiveFeed, error) {
	switch LiveFeed(strings.ToLower(strings.TrimSpace(s))) {
	case "", LiveFeedAuto:
		return LiveFeedAuto, nil
	case LiveFeedProducer:
		return LiveFeedProducer, nil
	}

	return "", fmt.Errorf("%q: auto or producer", s)
}

// liveBehind is how old the bytes at the start of a live request may be and
// still go to the relay through the satellite (§41.8): a lamina's first
// request waits for the one before to commit, which takes less; a request
// that resumes after an outage begins with a backlog that does not, and its
// live begins past it.
const liveBehind = 2 * time.Second

// ParseVia reads `direct`, `satellite` or `auto`; empty is ViaDirect.
func ParseVia(s string) (Via, error) {
	switch Via(strings.ToLower(strings.TrimSpace(s))) {
	case "", ViaDirect:
		return ViaDirect, nil
	case ViaSatellite:
		return ViaSatellite, nil
	case ViaAuto:
		return ViaAuto, nil
	}

	return "", fmt.Errorf("%q: direct, satellite or auto", s)
}

const (
	// FirstSatellitePause is how long a producer on `auto` uploads
	// directly after its satellite could not be reached, before it tries
	// the satellite again; it doubles with every failure in a row, up to
	// MaxSatellitePause, as an HTTP/3 handshake that fails does (§35.6).
	FirstSatellitePause = time.Minute
	MaxSatellitePause   = 15 * time.Minute
	// satelliteWakeEvery bounds how often a failing satellite has the
	// heartbeat ask the CP for the assignment again.
	satelliteWakeEvery = 5 * time.Second
)

// SatelliteLink is the producer's side of its satellite: the assignment
// the CP last answered, where each request goes, and how the satellite
// has done.
type SatelliteLink struct {
	via Via
	// dev names the producer in a header, development mode having no
	// client certificate (§34.7).
	dev      bool
	producer pdid.Id
	log      *slog.Logger
	now      func() time.Time
	// wake has the heartbeat ask the CP again, for an assignment that
	// replaces a satellite that fails.
	wake func()

	mu         sync.Mutex
	assignment *api.SatelliteAssignment
	base       string
	paused     time.Time
	failures   int
	woke       time.Time
	// said is what the log last said about the path, so it says it once.
	said string
	// liveFeed is `producer.live_feed`; live the sources whose live it
	// hands the satellite now: their last live request went through it
	// marked so (§41.8).
	liveFeed LiveFeed
	live     map[string]bool

	through   atomic.Int64
	direct    atomic.Int64
	fallbacks atomic.Int64
	// retain is the producer's `retain`, and the counts of `accepted`
	// since the last report (§41.9): segments handed off and their bytes,
	// and requests the satellite passed through.
	retain         string
	handedOff      atomic.Int64
	handedOffBytes atomic.Int64
	passed         atomic.Int64
	// passedTotal is passed since the producer started, for its Stats.
	passedTotal atomic.Int64
}

// NewSatelliteLink is a link for uploads that go the way via says; dev is
// development mode. An Uploader made apart from a producer, a test's, is
// given one and Set the assignment it is to use.
func NewSatelliteLink(via Via, dev bool, log *slog.Logger) *SatelliteLink {
	if via == "" {
		via = ViaDirect
	}
	if log == nil {
		log = slog.Default()
	}

	return &SatelliteLink{via: via, dev: dev, log: log, now: time.Now, liveFeed: LiveFeedAuto, live: map[string]bool{}}
}

// SetLiveFeed is `producer.live_feed` (§41.8); empty is LiveFeedAuto.
func (l *SatelliteLink) SetLiveFeed(v LiveFeed) {
	if v == "" {
		v = LiveFeedAuto
	}
	l.mu.Lock()
	l.liveFeed = v
	l.mu.Unlock()
}

// liveRequest marks a live request through the satellite whose source's
// live it hands over (§41.8) with where its live begins, and notes whether
// the source's live is handed over now: its live requests go through the
// satellite, marked. A change has the heartbeat tell the CP at once, which
// gives the satellite a feed token for exactly those sources.
func (l *SatelliteLink) liveRequest(req *http.Request, source []byte, viaSatellite bool, seg *Segment, offset int64) {
	if l == nil || l.via == ViaDirect || len(source) == 0 {
		return
	}
	l.mu.Lock()
	hand := viaSatellite && seg.Live && l.liveFeed != LiveFeedProducer
	changed := l.live[string(source)] != hand
	if hand {
		l.live[string(source)] = true
	} else {
		delete(l.live, string(source))
	}
	l.mu.Unlock()
	if hand {
		req.Header.Set(satellite.HdrLive, strconv.FormatInt(seg.LiveFrom(offset, time.Now(), liveBehind), 10))
	}
	if changed && l.wake != nil {
		l.wake()
	}
}

// keepLive forgets the sources that are not among those given: a source
// the producer no longer has hands nothing over.
func (l *SatelliteLink) keepLive(sources map[string]bool) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for id := range l.live {
		if !sources[id] {
			delete(l.live, id)
		}
	}
}

// Handed are the sources whose live the producer hands the satellite now
// (§41.8).
func (l *SatelliteLink) Handed() [][]byte {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([][]byte, 0, len(l.live))
	for id := range l.live {
		out = append(out, []byte(id))
	}
	slices.SortFunc(out, bytes.Compare)

	return out
}

// Set takes the assignment the CP answered; nil is none.
func (l *SatelliteLink) Set(a *api.SatelliteAssignment) {
	if l == nil || l.via == ViaDirect {
		return
	}
	base := ""
	if a != nil {
		base = endpointBase(a.GetEndpoints(), l.dev)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if a == nil || base == "" {
		l.assignment, l.base = nil, ""
		return
	}
	if l.assignment != nil && string(l.assignment.GetSatelliteId()) != string(a.GetSatelliteId()) {
		// Another satellite: the one that failed is not this one.
		l.paused, l.failures = time.Time{}, 0
	}
	l.assignment, l.base = a, base
}

// endpointBase is scheme://host:port of the first endpoint a producer may
// use: https, or http in development mode.
func endpointBase(eps []*api.Endpoint, dev bool) string {
	for _, ep := range eps {
		if ep.GetScheme() != "https" && (ep.GetScheme() != "http" || !dev) {
			continue
		}
		if ep.GetHost() == "" || ep.GetPort() <= 0 {
			continue
		}

		return ep.GetScheme() + "://" + net.JoinHostPort(ep.GetHost(), strconv.Itoa(int(ep.GetPort())))
	}

	return ""
}

// route is where a request for a candidate whose endpoint is ep goes: the
// satellite's base, and true, or the node's own.
func (l *SatelliteLink) route(ep *api.Endpoint) (string, bool) {
	node := ep.GetScheme() + "://" + net.JoinHostPort(ep.GetHost(), strconv.Itoa(int(ep.GetPort())))
	if l == nil || l.via == ViaDirect {
		return node, false
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	a := l.assignment
	switch {
	case a == nil || l.base == "" || (a.GetDateExpires() != nil && now.After(a.GetDateExpires().AsTime())):
		l.say("no satellite is assigned; uploading directly to the nodes", "")
		return node, false
	case l.via == ViaAuto && now.Before(l.paused):
		l.say("the satellite could not be reached; uploading directly for a while", mustId(a.GetSatelliteId()).String())
		return node, false
	}
	l.say("uploading through the satellite", mustId(a.GetSatelliteId()).String())

	return l.base, true
}

// say logs a change of path, once.
func (l *SatelliteLink) say(msg, satellite string) {
	if l.said == msg+satellite {
		return
	}
	l.said = msg + satellite
	if satellite == "" {
		l.log.Info(msg, "via", string(l.via))
		return
	}
	l.log.Info(msg, "via", string(l.via), "satellite", satellite)
}

// accepts says whether the satellite requests go to now takes `retain:
// accepted`, as the CP said (§41.9): one of an earlier release would not
// know the handoff, and would pass it on to a node.
func (l *SatelliteLink) accepts() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.assignment != nil && l.assignment.GetAccept()
}

// holder is where the rest of a segment whose start the satellite holds
// goes (§41.9): the satellite assigned, whether or not `auto` went
// directly for a while, since only it has those bytes. False without one.
func (l *SatelliteLink) holder() (string, bool) {
	if l == nil || l.via == ViaDirect {
		return "", false
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	a := l.assignment
	if a == nil || l.base == "" || (a.GetDateExpires() != nil && now.After(a.GetDateExpires().AsTime())) {
		return "", false
	}

	return l.base, true
}

// prepare marks a request that goes through the satellite: in development
// mode, which producer it is.
func (l *SatelliteLink) prepare(req *http.Request, viaSatellite bool) {
	if viaSatellite && l.dev && !l.producer.IsZero() {
		req.Header.Set(upload.HdrProducer, l.producer.String())
	}
}

// sent counts a request on its path.
func (l *SatelliteLink) sent(viaSatellite bool) {
	if l == nil {
		return
	}
	if viaSatellite {
		l.through.Add(1)
	} else {
		l.direct.Add(1)
	}
}

// failed is the satellite not reached, or refusing this producer: on
// `auto` the producer goes directly for a while, longer each time in a
// row; on `satellite` it asks the CP again, which assigns another once it
// holds this one down. It answers whether the producer falls back.
func (l *SatelliteLink) failed(err error) bool {
	if l == nil {
		return false
	}
	now := l.now()
	l.mu.Lock()
	id := mustId(l.assignment.GetSatelliteId()).String()
	wake := now.Sub(l.woke) >= satelliteWakeEvery
	if wake {
		l.woke = now
	}
	var pause time.Duration
	if l.via == ViaAuto && !now.Before(l.paused) {
		pause = FirstSatellitePause << min(l.failures, 10)
		if pause > MaxSatellitePause {
			pause = MaxSatellitePause
		}
		l.failures++
		l.paused = now.Add(pause)
		l.fallbacks.Add(1)
	}
	l.mu.Unlock()
	switch {
	case pause > 0:
		l.log.Warn("satellite failed; uploading directly for a while", "satellite", id, "pause", pause.String(), "err", err.Error())
	case wake:
		// Every source's requests fail alike while it lasts: said once
		// in a while.
		l.log.Warn("satellite failed", "satellite", id, "via", string(l.via), "err", err.Error())
	}
	if wake && l.wake != nil {
		l.wake()
	}

	return pause > 0
}

// succeeded is an answer through the satellite: it is reachable.
func (l *SatelliteLink) succeeded() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.failures = 0
	l.mu.Unlock()
}

// directFailed is a request sent directly that did not reach the node
// either, on `auto` while the satellite is paused: the producer's own link
// may be down rather than the satellite, which is tried again at once, as
// HTTP/3 is after TCP fails as well (§35.6).
func (l *SatelliteLink) directFailed() {
	if l == nil || l.via != ViaAuto {
		return
	}
	l.mu.Lock()
	if l.now().Before(l.paused) {
		l.paused = time.Time{}
	}
	l.mu.Unlock()
}

// wanted says whether the producer asks the CP for a satellite.
func (l *SatelliteLink) wanted() bool { return l != nil && l.via != ViaDirect }

// report is the heartbeat's: the path asked for, the satellite held, and
// the requests on each path since the last report (§41.5). A producer that
// uploads directly reports nothing, as before satellites.
func (l *SatelliteLink) report() *api.UploadReport {
	if !l.wanted() {
		return nil
	}
	b := api.UploadReport_builder{
		Via:              string(l.via),
		ThroughSatellite: l.through.Swap(0),
		Direct:           l.direct.Swap(0),
		Fallbacks:        l.fallbacks.Swap(0),
		Retain:           l.retain,
		HandedOff:        l.handedOff.Swap(0),
		HandedOffBytes:   l.handedOffBytes.Swap(0),
		Passed:           l.passed.Swap(0),
		LiveSources:      l.Handed(),
	}
	l.mu.Lock()
	if l.assignment != nil {
		b.SatelliteId = l.assignment.GetSatelliteId()
	}
	b.LiveFeed = string(l.liveFeed)
	l.mu.Unlock()

	return b.Build()
}

// satelliteFailure says whether an answer, or its absence, is the
// satellite's failure rather than the node's (§41.5).
func satelliteFailure(viaSatellite bool, resp *http.Response, err error) bool {
	return upload.SatelliteFailure(viaSatellite, resp, err)
}
