package satellite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/fmp4"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/internal/token"
)

// Live fan-out (§41.8). A producer whose live uploads go through the
// satellite hands it the sources whose live it may feed the relay with, and
// the satellite reads their fragments out of the upload bodies as they pass:
// the producer's uplink carries each byte once. For each producer it keeps
// one stream to the producer's relay, the stream the producer's own tee
// keeps (§39.3), with a feed token the CP gave it for those sources, and
// tells the relay which of them it feeds now: those whose live upload is
// passing through. The relay takes it as their feeder over the producer's
// stream, and gives them back to the producer's stream when it stops.
//
// The upload is what matters: a fragment is read out of bytes the proxy
// copies anyway, queued without waiting, and dropped when the relay is
// behind; nothing here holds a request.

// HdrLive is on an upload request whose source's live the producer hands the
// satellite (§41.8): the offset of the lamina from which on its bytes are
// live, which is where the request begins unless the bytes there are old. The
// satellite takes it off before the request goes on to the node.
const HdrLive = "Shale-Live"

const (
	// liveQueue bounds what waits for one relay stream, in fragments, as the
	// producer's tee's does: live bytes beyond it are dropped, not held.
	liveQueue = 32
	// liveIdle is how long a source stays fed from here after its live
	// upload ended with its lamina, for the next lamina's to begin: a node's
	// commit and the producer's next request take less.
	liveIdle = 3 * time.Second
	// feedRetry is how often a stream to a relay that is down is tried
	// again, doubling to feedRetryMax.
	feedRetry    = time.Second
	feedRetryMax = 30 * time.Second
	// refreshEvery bounds how often the feeds ask the CP for fresh tokens.
	feedRefreshEvery = time.Second
)

// liveFeeds is every producer's stream to its relay.
type liveFeeds struct {
	log *slog.Logger
	// dial opens a connection to a relay's ingest; refresh asks the CP for
	// the producers' assignments again soon.
	dial    func(ctx context.Context, addr string, plain bool) (*grpc.ClientConn, error)
	refresh func()

	mu        sync.Mutex
	ctx       context.Context
	links     map[pdid.Id]*feedLink
	refreshed time.Time

	bytes   atomic.Int64
	dropped atomic.Int64
}

func newLiveFeeds(log *slog.Logger) *liveFeeds {
	return &liveFeeds{log: log, links: map[pdid.Id]*feedLink{}}
}

// start is where the streams run: until ctx is done.
func (lf *liveFeeds) start(ctx context.Context) {
	lf.mu.Lock()
	lf.ctx = ctx
	lf.mu.Unlock()
}

// set takes the producers the CP answered: a stream for each that has a
// relay assignment, and none for the others, whose sources are fed from
// here no more. A producer the list no longer names is forgotten.
func (lf *liveFeeds) set(vs []*api.SatelliteProducer) {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	seen := map[pdid.Id]bool{}
	for _, v := range vs {
		id, err := pdid.From(v.GetProducerId())
		if err != nil || id.Domain() != DomProducer {
			continue
		}
		seen[id] = true
		var claims *api.TokenClaims
		ra := v.GetRelay()
		if ra != nil && len(ra.GetRelayId()) > 0 && len(ra.GetEndpoints()) > 0 {
			if c, err := token.Parse(ra.GetPublishToken()); err == nil && c.GetOp() == api.TokenOp_TOKEN_OP_FEED {
				claims = c
			}
		}
		l := lf.links[id]
		if claims == nil {
			if l != nil {
				l.set(nil, nil)
			}
			continue
		}
		if l == nil {
			if l = lf.newLinkLocked(id); l == nil {
				continue
			}
		}
		l.set(ra, claims)
	}
	for id, l := range lf.links {
		if !seen[id] {
			l.close()
			delete(lf.links, id)
		}
	}
}

// newLinkLocked makes a producer's stream, which runs while the satellite
// does; nil before it runs.
func (lf *liveFeeds) newLinkLocked(producer pdid.Id) *feedLink {
	if lf.ctx == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(lf.ctx)
	l := newFeedLink(lf, producer, cancel)
	lf.links[producer] = l
	go l.run(ctx)

	return l
}

// link is the producer's stream, made now if it has none: a live upload
// may begin before the CP's feed for it comes, and is read from its start
// so the feed can begin as soon as it does.
func (lf *liveFeeds) link(producer pdid.Id) *feedLink {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	if l := lf.links[producer]; l != nil {
		return l
	}

	return lf.newLinkLocked(producer)
}

// askSoon has the CP asked for fresh assignments, at most every
// feedRefreshEvery: a token that names an older capture, or a relay that
// refused one.
func (lf *liveFeeds) askSoon() {
	lf.mu.Lock()
	if time.Since(lf.refreshed) < feedRefreshEvery || lf.refresh == nil {
		lf.mu.Unlock()
		return
	}
	lf.refreshed = time.Now()
	lf.mu.Unlock()
	lf.refresh()
}

// feeding is how many sources the satellite feeds relays now.
func (lf *liveFeeds) feeding() int {
	lf.mu.Lock()
	links := make([]*feedLink, 0, len(lf.links))
	for _, l := range lf.links {
		links = append(links, l)
	}
	lf.mu.Unlock()
	n := 0
	for _, l := range links {
		l.mu.Lock()
		for _, fs := range l.sources {
			if fs.claimed {
				n++
			}
		}
		l.mu.Unlock()
	}

	return n
}

// drop counts a fragment the relay was too far behind to be queued.
func (lf *liveFeeds) drop() {
	n := lf.dropped.Add(1)
	if n == 1 || n%1000 == 0 {
		lf.log.Warn("live dropped: a relay is not keeping up", "dropped", n)
	}
}

// tap is what reads a PUT's body on its way for live, or nil when the request
// is not one to feed from: no HdrLive, not a live upload, or a producer
// whose relay the CP gave no feed for.
func (lf *liveFeeds) tap(producer pdid.Id, c *api.TokenClaims, key string, r *http.Request) *tap {
	v := r.Header.Get(HdrLive)
	if v == "" || c.GetMode() != api.UploadMode_UPLOAD_MODE_LIVE {
		return nil
	}
	from, err := strconv.ParseInt(v, 10, 64)
	if err != nil || from < 0 {
		return nil
	}
	var offset int64
	if s := r.Header.Get(storage.HdrUploadOffset); s != "" {
		if offset, err = strconv.ParseInt(s, 10, 64); err != nil || offset < 0 {
			return nil
		}
	}
	src, err := pdid.From(c.GetRecord().GetSourceId())
	if err != nil {
		return nil
	}
	l := lf.link(producer)
	if l == nil {
		return nil
	}

	return l.tap(src, c.GetRecord().GetCaptureEpoch(), key, offset, max(from, offset))
}

// feedLink is one producer's stream to its relay.
type feedLink struct {
	lf       *liveFeeds
	producer pdid.Id
	cancel   context.CancelFunc

	mu sync.Mutex
	// assignment is the CP's last, and claims its token's; sent is the
	// claims the open stream was last authorized with, nil while none is
	// open.
	assignment *api.RelayAssignment
	claims     *api.TokenClaims
	sent       *api.TokenClaims
	sources    map[pdid.Id]*feedSource
	// reauthorize says the open stream is to be sent the token again;
	// feeds that it is to be told what the satellite feeds now.
	reauthorize bool
	feeds       bool
	changed     chan struct{}
	controls    chan struct{}
	send        chan queued
	closed      bool
}

// queued is a Data message waiting for the relay, with when it began to.
type queued struct {
	msg *api.AttachRequest
	at  time.Time
}

// feedSource is one source's live as it passes: the upload being read, and
// what goes to the relay.
type feedSource struct {
	id pdid.Id
	// The lamina being read: its key, the offset of the next byte, and the
	// splitter that reads it; nil when the bytes cannot be followed. gen is
	// the request reading it now; a request that came after takes over.
	key   string
	pos   int64
	split *fmp4.Splitter
	gen   uint64
	from  int64
	epoch string
	// claimed says the satellite feeds it now, as the relay is told; tap is
	// the request whose live holds the claim, 0 when none does, and idle the
	// wait for the next one's.
	claimed bool
	tap     uint64
	idle    *time.Timer
	// The relay started it; keyed says the next key fragment went, with
	// init in front, the init segment last read and sent the last one sent
	// on this stream; seq is the last fragment read.
	started bool
	keyed   bool
	init    *fmp4.Init
	sent    []byte
	seq     uint32
}

func newFeedLink(lf *liveFeeds, producer pdid.Id, cancel context.CancelFunc) *feedLink {
	return &feedLink{lf: lf, producer: producer, cancel: cancel, sources: map[pdid.Id]*feedSource{}, changed: make(chan struct{}, 1), controls: make(chan struct{}, 1), send: make(chan queued, liveQueue)}
}

func (l *feedLink) source(id pdid.Id) *feedSource {
	fs := l.sources[id]
	if fs == nil {
		fs = &feedSource{id: id}
		l.sources[id] = fs
	}

	return fs
}

// set takes the CP's assignment: another relay, or the same at other
// endpoints, is a new stream; a token that says something new, or one the
// open stream's is half through, is sent on it. None ends the stream.
func (l *feedLink) set(ra *api.RelayAssignment, claims *api.TokenClaims) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if ra == nil {
		if l.assignment != nil {
			l.assignment, l.claims = nil, nil
			signal(l.changed)
		}
		for _, fs := range l.sources {
			l.unclaimLocked(fs)
		}
		return
	}
	same := l.assignment != nil && bytes.Equal(l.assignment.GetRelayId(), ra.GetRelayId()) && sameEndpoints(l.assignment.GetEndpoints(), ra.GetEndpoints())
	l.assignment, l.claims = ra, claims
	if !same {
		signal(l.changed)
	} else if l.sent != nil && renew(l.sent, claims, time.Now()) {
		l.reauthorize = true
		signal(l.controls)
	}
	// What the token no longer names is not fed from here.
	for id, fs := range l.sources {
		if fs.claimed && !names(claims, id) {
			l.unclaimLocked(fs)
		}
	}
}

// close ends the stream for good: the CP gave no feed for the producer.
func (l *feedLink) close() {
	l.mu.Lock()
	l.closed = true
	for _, fs := range l.sources {
		if fs.idle != nil {
			fs.idle.Stop()
		}
		fs.claimed, fs.tap, fs.started, fs.keyed = false, 0, false, false
	}
	l.mu.Unlock()
	l.cancel()
}

// renew says whether a stream authorized with `sent` is to be sent `next`:
// it names other sources or captures, or another policy, or `sent` is
// half through its life and the relay would end the stream at its expiry.
func renew(sent, next *api.TokenClaims, now time.Time) bool {
	if !sameSources(sent.GetSources(), next.GetSources()) || sent.GetLive() != next.GetLive() || sent.GetRewindSeconds() != next.GetRewindSeconds() || len(sent.GetCaptureEpochs()) != len(next.GetCaptureEpochs()) {
		return true
	}
	for i, g := range sent.GetCaptureEpochs() {
		h := next.GetCaptureEpochs()[i]
		if !bytes.Equal(g.GetSourceId(), h.GetSourceId()) || g.GetCaptureEpoch() != h.GetCaptureEpoch() || g.GetSequence() != h.GetSequence() {
			return true
		}
	}
	iat, exp := sent.GetIat().AsTime(), sent.GetExp().AsTime()

	return now.After(iat.Add(exp.Sub(iat) / 2))
}

func sameSources(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}

	return true
}

// sameEndpoints says whether two assignments name the same listeners.
func sameEndpoints(a, b []*api.Endpoint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].GetScheme() != b[i].GetScheme() || a[i].GetHost() != b[i].GetHost() || a[i].GetPort() != b[i].GetPort() {
			return false
		}
	}

	return true
}

// names says whether a token names a source.
func names(c *api.TokenClaims, id pdid.Id) bool {
	for _, raw := range c.GetSources() {
		if bytes.Equal(raw, id.Bytes()) {
			return true
		}
	}

	return false
}

// grantFor is the capture the token grants a source, and whether it grants
// one: a managed source's (§39.3).
func grantFor(c *api.TokenClaims, id pdid.Id) (string, bool) {
	for _, g := range c.GetCaptureEpochs() {
		if bytes.Equal(g.GetSourceId(), id.Bytes()) {
			return g.GetCaptureEpoch(), true
		}
	}

	return "", false
}

func signal(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

func (l *feedLink) current() (*api.RelayAssignment, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.assignment, l.closed
}

// run keeps the stream up while there is an assignment.
func (l *feedLink) run(ctx context.Context) {
	wait := feedRetry
	for {
		select {
		case <-l.changed:
		default:
		}
		ra, closed := l.current()
		if closed || ctx.Err() != nil {
			return
		}
		if ra == nil {
			select {
			case <-ctx.Done():
				return
			case <-l.changed:
			}
			continue
		}
		up, err := l.attach(ctx, ra)
		if ctx.Err() != nil {
			return
		}
		if up {
			wait = feedRetry
		}
		if err != nil {
			l.lf.log.Warn("live feed", "producer", l.producer.String(), "relay", mustId(ra.GetRelayId()).String(), "code", status.Code(err).String(), "err", err.Error(), "retry_in", wait.String())
			// A token the relay refused may name an older capture, or have
			// expired: ask the CP for the assignment again.
			l.lf.askSoon()
		}
		select {
		case <-ctx.Done():
			return
		case <-l.changed:
			wait = feedRetry
		case <-time.After(wait):
			wait = min(wait*2, feedRetryMax)
		}
	}
}

func mustId(b []byte) pdid.Id {
	id, _ := pdid.From(b)

	return id
}

// attach is one stream: Hello, the sources fed from here, Data for those
// the relay started, until either side ends it. up says the relay
// welcomed it.
func (l *feedLink) attach(ctx context.Context, ra *api.RelayAssignment) (up bool, err error) {
	ep := ra.GetEndpoints()[0]
	conn, err := l.lf.dial(ctx, fmt.Sprintf("%s:%d", ep.GetHost(), ep.GetPort()), ep.GetScheme() == "http")
	if err != nil {
		return false, err
	}
	defer conn.Close()
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := api.NewRelayIngestClient(conn).Attach(sctx)
	if err != nil {
		return false, err
	}
	l.mu.Lock()
	claims := l.claims
	tok := l.assignment.GetPublishToken()
	l.mu.Unlock()
	if claims == nil {
		// Let go of since it was asked for.
		return false, nil
	}
	if err := stream.Send(api.AttachRequest_builder{Hello: api.AttachRequest_Hello_builder{PublishToken: tok}.Build()}.Build()); err != nil {
		return false, err
	}
	l.mu.Lock()
	l.sent, l.reauthorize, l.feeds = claims, false, true
	for len(l.send) > 0 {
		<-l.send
	}
	for _, fs := range l.sources {
		fs.started, fs.keyed, fs.sent = false, false, nil
	}
	l.mu.Unlock()
	signal(l.controls)
	defer func() {
		l.mu.Lock()
		l.sent = nil
		for _, fs := range l.sources {
			fs.started, fs.keyed, fs.sent = false, false, nil
		}
		l.mu.Unlock()
	}()

	errs := make(chan error, 2)
	var welcomed atomic.Bool
	var running sync.WaitGroup
	running.Add(2)
	defer func() { cancel(); conn.Close(); running.Wait() }()
	go func() {
		defer running.Done()
		for {
			if msg := l.takeControl(); msg != nil {
				if err := stream.Send(msg); err != nil {
					errs <- err
					return
				}
				continue
			}
			select {
			case <-sctx.Done():
				return
			case <-l.controls:
			case q := <-l.send:
				// t2 (§39.3), as the producer's tee stamps it.
				q.msg.GetData().SetSentAt(timestamppb.Now())
				if err := stream.Send(q.msg); err != nil {
					errs <- err
					return
				}
				l.lf.bytes.Add(int64(len(q.msg.GetData().GetPayload())))
			}
		}
	}()
	go func() {
		defer running.Done()
		for {
			msg, err := stream.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					err = errors.New("the relay closed the stream")
				}
				errs <- err
				return
			}
			switch {
			case msg.GetWelcome() != nil:
				welcomed.Store(true)
				l.lf.log.Info("feeding the relay", "producer", l.producer.String(), "relay", mustId(ra.GetRelayId()).String(), "sources", len(msg.GetWelcome().GetSources()))
			case msg.GetStart() != nil:
				if id, err := pdid.From(msg.GetStart().GetSourceId()); err == nil {
					l.start(id, true)
				}
			case msg.GetStop() != nil:
				if id, err := pdid.From(msg.GetStop().GetSourceId()); err == nil {
					l.start(id, false)
				}
			}
		}
	}()

	// The stream ends with its context, not CloseSend: the sender may be in
	// Send, and gRPC does not allow the two at once.
	select {
	case <-ctx.Done():
		return welcomed.Load(), nil
	case <-l.changed:
		signal(l.changed)
		return welcomed.Load(), nil
	case err := <-errs:
		return welcomed.Load(), err
	}
}

// takeControl is the next control message for the open stream: the token
// again, then what the satellite feeds now; nil when there is none.
func (l *feedLink) takeControl() *api.AttachRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.reauthorize && l.assignment != nil {
		l.reauthorize = false
		l.sent = l.claims

		return api.AttachRequest_builder{Hello: api.AttachRequest_Hello_builder{PublishToken: l.assignment.GetPublishToken()}.Build()}.Build()
	}
	if l.feeds {
		l.feeds = false
		var ids [][]byte
		for id, fs := range l.sources {
			if fs.claimed {
				ids = append(ids, id.Bytes())
			}
		}

		return api.AttachRequest_builder{Feed: api.AttachRequest_Feed_builder{Sources: ids}.Build()}.Build()
	}

	return nil
}

// start is the relay's Start (on) or Stop for a source: from the next key
// fragment on, with the init segment in front, as the producer's tee does
// (§39.3).
func (l *feedLink) start(id pdid.Id, on bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fs := l.source(id)
	if fs.started != on {
		l.lf.log.Debug("live", "source", id.String(), "on", on)
	}
	fs.started, fs.keyed = on, false
}

// claimLocked says the satellite feeds a source now, for the request `gen`
// whose live reached its `from`: unless the token does not name the
// source, or grants another capture than the upload's, which a fresher
// token may.
func (l *feedLink) claimLocked(fs *feedSource, gen uint64) {
	if fs.tap == gen && fs.claimed {
		return
	}
	if l.closed || l.claims == nil || !names(l.claims, fs.id) {
		return
	}
	if epoch, managed := grantFor(l.claims, fs.id); managed && epoch != fs.epoch || !managed && fs.epoch != "" {
		go l.lf.askSoon()
		return
	}
	fs.tap = gen
	if fs.idle != nil {
		fs.idle.Stop()
		fs.idle = nil
	}
	if !fs.claimed {
		fs.claimed, fs.keyed = true, false
		l.feeds = true
		signal(l.controls)
	}
}

// endLocked is the end of request `gen`'s live: a lamina that is complete
// keeps the source fed from here for liveIdle, for the next lamina's
// request; one that broke hands it back to the producer's stream at once.
func (l *feedLink) endLocked(fs *feedSource, gen uint64, complete bool) {
	if fs.tap != gen || gen == 0 {
		return
	}
	fs.tap = 0
	if !complete {
		l.unclaimLocked(fs)
		return
	}
	if fs.idle != nil {
		fs.idle.Stop()
	}
	var t *time.Timer
	t = time.AfterFunc(liveIdle, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if fs.idle == t && fs.tap == 0 && fs.claimed {
			l.unclaimLocked(fs)
		}
	})
	fs.idle = t
}

// unclaimLocked hands a source back: the relay is told, and nothing more
// of it is sent from here.
func (l *feedLink) unclaimLocked(fs *feedSource) {
	if fs.idle != nil {
		fs.idle.Stop()
		fs.idle = nil
	}
	fs.tap = 0
	if !fs.claimed {
		return
	}
	fs.claimed, fs.started, fs.keyed = false, false, false
	l.feeds = true
	signal(l.controls)
}

// tap is one request's reading of a lamina for live: from `offset` of the
// lamina `key` of a source, its live from `from` on.
func (l *feedLink) tap(id pdid.Id, epoch, key string, offset, from int64) *tap {
	l.mu.Lock()
	defer l.mu.Unlock()
	fs := l.source(id)
	fs.gen++
	t := &tap{l: l, fs: fs, gen: fs.gen}
	switch {
	case fs.split != nil && fs.key == key && offset <= fs.pos:
		// The rest of a lamina already being read: what is read already is
		// passed over.
		t.skip = fs.pos - offset
	case offset == 0:
		fs.key, fs.pos = key, 0
		fs.split = &fmp4.Splitter{Max: maxFragment, Want: func(at int64) bool { return l.wantLocked(fs, at) }, Unit: func(u *fmp4.Unit) { l.unitLocked(fs, u) }}
	default:
		// A lamina whose beginning went elsewhere: its boxes cannot be
		// found; the next lamina's are.
		fs.key, fs.split = key, nil
	}
	if epoch != fs.epoch {
		fs.epoch, fs.keyed = epoch, false
	}
	fs.from = from

	return t
}

// maxFragment bounds a fragment read for live: one ingest message, with
// what else it says, within what a relay takes (relay.MaxFragment, 32 MiB).
const maxFragment = 32<<20 - 64<<10

// wantLocked is whether the fragment at offset `at` of the lamina is read:
// it is live, and the relay started the source. The first live one claims
// the source for the request reading it.
func (l *feedLink) wantLocked(fs *feedSource, at int64) bool {
	if at < fs.from {
		return false
	}
	l.claimLocked(fs, fs.gen)

	return fs.claimed && fs.started
}

// unitLocked takes what the splitter read: an init segment is kept for the
// next key fragment, sent only when it is not the one sent last, so a
// lamina that begins with the same one continues the feed; a fragment goes
// to the relay from the first key fragment on.
func (l *feedLink) unitLocked(fs *feedSource, u *fmp4.Unit) {
	if u.Init != nil {
		fs.init = u.Init
		if !bytes.Equal(u.Init.Bytes, fs.sent) {
			fs.keyed = false
		}
		return
	}
	f := u.Frag
	if fs.seq != 0 && f.Seq != fs.seq+1 {
		// Fragments went by unread, or a capture began again: from the
		// next key fragment, with the init segment in front.
		fs.keyed = false
	}
	fs.seq = f.Seq
	if !fs.claimed || !fs.started || fs.init == nil {
		return
	}
	if !fs.keyed {
		if !f.Key(fs.init) {
			return
		}
		fs.keyed = true
		fs.sent = fs.init.Bytes
		l.queueLocked(fs, fs.init.Bytes)
	}
	l.queueLocked(fs, f.Bytes)
}

// queueLocked queues one Data message, or drops it when the relay is
// behind: never waits.
func (l *feedLink) queueLocked(fs *feedSource, b []byte) {
	now := time.Now()
	msg := api.AttachRequest_builder{Data: api.AttachRequest_Data_builder{SourceId: fs.id.Bytes(), CaptureEpoch: fs.epoch, Payload: b, ReadAt: timestamppb.New(now)}.Build()}.Build()
	select {
	case l.send <- queued{msg: msg, at: now}:
	default:
		l.lf.drop()
	}
}

// tap reads one request's body for live as it passes.
type tap struct {
	l     *feedLink
	fs    *feedSource
	gen   uint64
	skip  int64
	ended bool
}

// write is the next bytes of the body, as the proxy reads them.
func (t *tap) write(p []byte) {
	l, fs := t.l, t.fs
	l.mu.Lock()
	defer l.mu.Unlock()
	if t.ended || fs.gen != t.gen || fs.split == nil {
		return
	}
	if t.skip > 0 {
		k := min(int64(len(p)), t.skip)
		p, t.skip = p[k:], t.skip-k
	}
	if len(p) == 0 {
		return
	}
	fs.split.Write(p)
	fs.pos += int64(len(p))
	if err := fs.split.Err(); err != nil {
		l.lf.log.Info("live: the upload's fragments cannot be read", "source", fs.id.String(), "err", err.Error())
		fs.split = nil
		l.endLocked(fs, t.gen, false)
	}
}

// end is the end of the request: complete when its body ended as it should.
func (t *tap) end(complete bool) {
	l, fs := t.l, t.fs
	l.mu.Lock()
	defer l.mu.Unlock()
	t.ended = true
	l.endLocked(fs, t.gen, complete)
}
