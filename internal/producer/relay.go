package producer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/fmp4"
)

// The live tee (§39.3, §38.7): the producer keeps one stream to the relay
// the CP assigned it, sends nothing until the relay says Start for a
// source, then forwards that source's fragments from the next one a lamina
// may start at on, the init segment first, one message per fragment, until
// Stop. The bytes are the ones it stores, an Opus track among them (§38.7);
// a stream the producer did not encode whose audio is not Opus goes through
// the live helper (live.go) first.

// relayLink is the producer's side of the relay stream.
type relayLink struct {
	p   *Producer
	ctx context.Context

	mu         sync.Mutex
	assignment *api.RelayAssignment
	changed    chan struct{}
	// active is the sources the relay started; key says whether the next
	// keyframe was seen since.
	active      map[pdid.Id]*tap
	send        chan queued
	controls    chan struct{}
	refresh     bool
	reauthorize bool
	resets      map[pdid.Id]relayEpoch
	epochs      map[pdid.Id]relayEpoch
	// relayId is the relay attached to, for the heartbeat's sake.
	relayId  pdid.Id
	dropped  int64
	noFfmpeg bool
}

// tap is one started source: sent from the first key fragment on.
type tap struct {
	keyed bool
	epoch string
	// h is the live helper while the source's audio is not Opus (§38.7);
	// nil when the bytes go as they are.
	h *helper
	// raw says the helper failed for good and the bytes go as they are;
	// fails counts its exits.
	raw   bool
	fails int
}

// sendQueue bounds what waits for the relay, in fragments; live bytes are
// dropped beyond it rather than held.
const sendQueue = 32

// queued is a Data message waiting for the relay, with when it began to.
type queued struct {
	msg *api.AttachRequest
	at  time.Time
}

type relayEpoch struct {
	epoch   string
	stopped bool
}

func newRelayLink(p *Producer) *relayLink {
	return &relayLink{p: p, changed: make(chan struct{}, 1), active: map[pdid.Id]*tap{}, send: make(chan queued, sendQueue), controls: make(chan struct{}, 1), resets: map[pdid.Id]relayEpoch{}, epochs: map[pdid.Id]relayEpoch{}}
}

// set takes the assignment the CP handed over; a different relay makes
// the loop reconnect.
func (l *relayLink) set(ra *api.RelayAssignment) {
	if ra == nil || len(ra.GetRelayId()) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	// The same relay with a fresher token is the same assignment: the
	// token is sent in an authenticated Hello, and the stream stays. The same relay at
	// other endpoints is a relay that restarted (§39.6): the stream to the
	// old ones is ended and the new ones dialed. A changed live policy is
	// applied in place by the relay at Hello (§39.3).
	same := l.assignment != nil && string(l.assignment.GetRelayId()) == string(ra.GetRelayId()) &&
		sameEndpoints(l.assignment.GetEndpoints(), ra.GetEndpoints())
	fresh := l.assignment == nil || l.assignment.GetPublishToken() != ra.GetPublishToken()
	l.assignment = ra
	if same && fresh {
		l.reauthorize = true
		l.signalControl()
	}
	if !same {
		select {
		case l.changed <- struct{}{}:
		default:
		}
	}
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

func (l *relayLink) current() *api.RelayAssignment {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.assignment
}

// run keeps the stream up while there is an assignment.
func (l *relayLink) run(ctx context.Context) error {
	l.mu.Lock()
	l.ctx = ctx
	l.mu.Unlock()
	wait := relayRetry
	// lost is when the link last went from up to down: for relayRetryFor
	// from then the producer asks every relayRetry, since a relay that
	// went is replaced only when the CP stops hearing it, and the producer
	// learns of that by asking (§39.2). A slower one would add its gap to
	// every viewer's wait.
	var lost time.Time
	for {
		// A change signaled before this point is in what current() answers
		// now; a signal left over would end the stream about to be opened
		// the moment it is up. The assignment negotiation hands over before
		// this loop starts is such a one.
		select {
		case <-l.changed:
		default:
		}
		ra := l.current()
		if ra == nil {
			select {
			case <-ctx.Done():
				return nil
			case <-l.changed:
			}
			continue
		}
		up, err := l.attach(ctx, ra)
		if ctx.Err() != nil {
			return nil
		}
		if up || lost.IsZero() {
			lost = time.Now()
			wait = relayRetry
		}
		if err != nil {
			l.p.log.Warn("relay", "code", status.Code(err).String(), "retry_in", wait.String())
			// A token the relay refused may have expired: ask for a fresh one.
			if fresh, err := api.NewProducerServiceClient(l.p.cp()).Relay(ctx, &api.ProducerRelayRequest{}); err == nil {
				l.set(fresh.GetRelay())
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-l.changed:
			wait = relayRetry
		case <-time.After(wait):
			if time.Since(lost) >= relayRetryFor && wait < 30*time.Second {
				wait *= 2
			}
		}
	}
}

// relayRetry is how often a producer whose relay link is down tries it
// again, asking the CP for its assignment each time, for relayRetryFor;
// it backs off to 30 s from then on.
const (
	relayRetry    = time.Second
	relayRetryFor = time.Minute
)

// attach is one stream: Hello, then Start and Stop from the relay and Data
// from the taps, until either side ends it. up says the relay welcomed it.
func (l *relayLink) attach(ctx context.Context, ra *api.RelayAssignment) (up bool, err error) {
	if len(ra.GetEndpoints()) == 0 {
		return false, errors.New("the assignment carries no endpoint")
	}
	ep := ra.GetEndpoints()[0]
	addr := fmt.Sprintf("%s:%d", ep.GetHost(), ep.GetPort())
	conn, err := l.p.agent.DialAddr(ctx, addr, ep.GetScheme() == "http")
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
	if err := stream.Send(api.AttachRequest_builder{Hello: api.AttachRequest_Hello_builder{PublishToken: ra.GetPublishToken()}.Build()}.Build()); err != nil {
		return false, err
	}
	relayId, _ := pdid.From(ra.GetRelayId())
	l.mu.Lock()
	l.relayId = relayId
	for id, state := range l.epochs {
		l.resets[id] = state
	}
	// Drain what an earlier stream left queued.
	for len(l.send) > 0 {
		<-l.send
	}
	l.mu.Unlock()
	defer l.clear()

	errs := make(chan error, 2)
	var welcomed atomic.Bool
	var running sync.WaitGroup
	running.Add(2)
	defer func() { cancel(); conn.Close(); running.Wait() }()
	go func() {
		defer running.Done()
		for {
			if l.takeRefresh() {
				l.refreshAssignment(sctx)
			}
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
				continue
			case q := <-l.send:
				// Control may have arrived together with this lossy item.
				for {
					control := l.takeControl()
					if control == nil {
						break
					}
					if err := stream.Send(control); err != nil {
						errs <- err
						return
					}
				}
				if !l.currentData(q.msg) {
					continue
				}
				// t2 (§39.3): handed to the stream, so the relay tells this
				// queue from the uplink.
				q.msg.GetData().SetSentAt(timestamppb.Now())
				if err := stream.Send(q.msg); err != nil {
					errs <- err
					return
				}
				// Send returns once flow control lets the message go to
				// the transport: the wait is the queue's and the uplink's
				// backpressure, not what the socket holds after.
				if m := l.p.m; m != nil {
					m.liveWait.Record(sctx, float64(time.Since(q.at).Microseconds())/1000)
				}
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
				l.p.log.Info("attached to the relay", "relay", relayId.String(), "sources", len(msg.GetWelcome().GetSources()))
			case msg.GetStart() != nil:
				if id, err := pdid.From(msg.GetStart().GetSourceId()); err == nil {
					l.start(id)
				}
			case msg.GetStop() != nil:
				if id, err := pdid.From(msg.GetStop().GetSourceId()); err == nil {
					l.stop(id)
				}
			}
		}
	}()

	// The stream ends with its context, canceled on the way out, and not
	// with CloseSend: the sender above may be inside Send, and gRPC does
	// not allow the two at once. The relay takes a canceled stream as it
	// takes a closed one.
	select {
	case <-ctx.Done():
		return welcomed.Load(), nil
	case <-l.changed:
		// A new assignment: reconnect to it.
		l.mu.Lock()
		select {
		case l.changed <- struct{}{}:
		default:
		}
		l.mu.Unlock()
		return welcomed.Load(), nil
	case err := <-errs:
		return welcomed.Load(), err
	}
}

func (l *relayLink) start(id pdid.Id) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.active[id]; !ok {
		state := l.epochs[id]
		if state.stopped {
			return
		}
		l.active[id] = &tap{epoch: state.epoch}
		l.p.log.Info("live", "source", id.String())
	}
}

func (l *relayLink) stop(id pdid.Id) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if t, ok := l.active[id]; ok {
		delete(l.active, id)
		if t.h != nil {
			t.h.close()
			t.h = nil
		}
		l.p.log.Info("live off", "source", id.String())
	}
}

func (l *relayLink) clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, t := range l.active {
		if t.h != nil {
			t.h.close()
			t.h = nil
		}
	}
	l.active = map[pdid.Id]*tap{}
	l.relayId = pdid.Nil
}

// transcodes is how many live helpers run, for the heartbeat.
func (l *relayLink) transcodes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	var n int64
	for _, t := range l.active {
		if t.h != nil {
			n++
		}
	}

	return n
}

// feed is the tap in the capture loop: a started source's fragments go to
// the relay from the first one a lamina may start at on, the init segment
// first, through the live helper when the audio is not Opus (§38.7).
func (l *relayLink) feed(s *source, f *Frame, init *fmp4.Init) { l.feedEpoch(s, f, init, "") }

func (l *relayLink) feedEpoch(s *source, f *Frame, init *fmp4.Init, epoch string) {
	if s.row == nil {
		return
	}
	id, err := pdid.From(s.row.GetId())
	if err != nil {
		return
	}
	l.mu.Lock()
	state, known := l.epochs[id]
	if known && (state.stopped || state.epoch != epoch) || !known && epoch != "" {
		l.mu.Unlock()
		return
	}
	t, ok := l.active[id]
	if !ok {
		l.mu.Unlock()
		return
	}
	if t.epoch != epoch {
		l.mu.Unlock()
		return
	}
	if f.Kind == FramePrefix {
		// A new init segment: the capture started over, and so does the
		// tee, from the next key fragment with the new segment in front.
		t.keyed = false
		l.mu.Unlock()
		return
	}
	var prefix []byte
	if !t.keyed {
		if !f.Key {
			l.mu.Unlock()
			return
		}
		t.keyed = true
		if t.h == nil && !t.raw && needsOpus(init) {
			h, err := l.startHelperEpoch(id, s, epoch)
			t.h = h
			switch {
			case err == nil:
			case errors.Is(err, errNoFfmpeg):
				// Said once per attachment: the bytes go as they are.
				t.raw = true
				if !l.noFfmpeg {
					l.noFfmpeg = true
					l.p.log.Warn("no ffmpeg for the live helper: cameras whose audio is not Opus play silent live", "source", s.cfg.Alias)
				}
			default:
				t.raw = true
				l.p.log.Warn("live helper could not start; sending the camera's bytes as they are", "source", s.cfg.Alias, "err", err.Error())
			}
		}
		if init != nil {
			prefix = init.Bytes
		}
	}
	h := t.h
	l.mu.Unlock()
	for _, b := range [][]byte{prefix, f.Payload} {
		if len(b) == 0 {
			continue
		}
		if h != nil {
			if !h.write(b) {
				l.drop()
			}

			continue
		}
		l.sendDataEpoch(id, epoch, b)
	}
}

// sendData queues one Data message for the relay, dropping it when the
// relay is behind rather than holding the capture loop.
func (l *relayLink) sendData(id pdid.Id, payload []byte) {
	l.sendDataEpoch(id, "", payload)
}

func (l *relayLink) sendDataEpoch(id pdid.Id, epoch string, payload []byte) {
	// t1 (§39.3): the producer has the payload, from its capture or, for a
	// source whose audio is transcoded, from the live helper.
	now := time.Now()
	msg := api.AttachRequest_builder{Data: api.AttachRequest_Data_builder{SourceId: id.Bytes(), CaptureEpoch: epoch, Payload: append([]byte(nil), payload...), ReadAt: timestamppb.New(now)}.Build()}.Build()
	if !l.currentData(msg) {
		return
	}
	select {
	case l.send <- queued{msg: msg, at: now}:
	default:
		l.drop()
	}
}

// resetSource is reliable, coalesced control. A stopped old helper cannot tag
// its buffered output with the new incarnation: callbacks retain their epoch.
func (l *relayLink) resetSource(id pdid.Id, epoch string, stopped bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	state := relayEpoch{epoch: epoch, stopped: stopped}
	if old, ok := l.epochs[id]; ok && old == state {
		return
	}
	l.epochs[id], l.resets[id] = state, state
	if t := l.active[id]; t != nil {
		if t.h != nil {
			t.h.close()
			t.h.cancel()
			t.h = nil
		}
		t.keyed, t.epoch = false, epoch
		if stopped {
			delete(l.active, id)
		}
	}
	l.signalControl()
}

// sourcesChanged only queues work; admission never performs network I/O here.
func (l *relayLink) sourcesChanged() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refresh = true
	l.signalControl()
}

// signalControl requires l.mu. Notifications coalesce; the maps retain work.
func (l *relayLink) signalControl() {
	select {
	case l.controls <- struct{}{}:
	default:
	}
}

func (l *relayLink) takeRefresh() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	refresh := l.refresh
	l.refresh = false
	return refresh
}

func (l *relayLink) refreshAssignment(ctx context.Context) {
	if l.p.conn.Load() == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if reply, err := api.NewProducerServiceClient(l.p.cp()).Relay(ctx, &api.ProducerRelayRequest{}); err == nil {
		l.set(reply.GetRelay())
	}
}

func (l *relayLink) takeControl() *api.AttachRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.reauthorize && l.assignment != nil && string(l.assignment.GetRelayId()) == string(l.relayId.Bytes()) {
		l.reauthorize = false
		// Repeat current resets after every authenticated membership update.
		// Earlier control for a new source may have preceded its grant.
		for id, state := range l.epochs {
			l.resets[id] = state
		}
		return api.AttachRequest_builder{Hello: api.AttachRequest_Hello_builder{PublishToken: l.assignment.GetPublishToken()}.Build()}.Build()
	}
	for id, state := range l.resets {
		delete(l.resets, id)
		return api.AttachRequest_builder{Reset: api.AttachRequest_Reset_builder{SourceId: id.Bytes(), CaptureEpoch: state.epoch, Stopped: state.stopped}.Build()}.Build()
	}
	return nil
}

func (l *relayLink) currentData(msg *api.AttachRequest) bool {
	d := msg.GetData()
	if d == nil {
		return false
	}
	id, err := pdid.From(d.GetSourceId())
	if err != nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	state, known := l.epochs[id]
	return known && !state.stopped && state.epoch == d.GetCaptureEpoch() || !known && d.GetCaptureEpoch() == ""
}

func (l *relayLink) drop() {
	l.mu.Lock()
	l.dropped++
	n := l.dropped
	l.mu.Unlock()
	if n == 1 || n%1000 == 0 {
		l.p.log.Warn("live bytes dropped: the relay or the live helper is not keeping up", "dropped", n)
	}
}

// Ensure the client stream type is what we use.
var _ grpc.ClientStream = (api.RelayIngest_AttachClient)(nil)
