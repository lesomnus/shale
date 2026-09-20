package producer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// The live tee (§39.3, §38.7): the producer keeps one stream to the relay
// the CP assigned it, sends nothing until the relay says Start for a
// source, then forwards that source's TS bytes from the next keyframe on
// with the tables prepended, until Stop. The video is the one it stores;
// audio that is not Opus goes through the live helper (live.go) first.

// relayLink is the producer's side of the relay stream.
type relayLink struct {
	p   *Producer
	ctx context.Context

	mu         sync.Mutex
	assignment *api.RelayAssignment
	changed    chan struct{}
	// active is the sources the relay started; key says whether the next
	// keyframe was seen since.
	active map[pdid.Id]*tap
	send   chan *api.AttachRequest
	// relayId is the relay attached to, for the heartbeat's sake.
	relayId  pdid.Id
	dropped  int64
	noFfmpeg bool
}

// tap is one started source: bytes accumulate from the keyframe on.
type tap struct {
	keyed bool
	buf   []byte
	// h is the live helper while the source's audio is not Opus (§38.7);
	// nil when the bytes go as they are.
	h *helper
	// raw says the helper failed for good and the bytes go as they are;
	// fails counts its exits.
	raw   bool
	fails int
}

const (
	// tapFlush is how many bytes a Data message carries at most.
	tapFlush = 32 * PacketSize
	// sendQueue bounds what waits for the relay; live bytes are dropped
	// beyond it rather than held.
	sendQueue = 256
)

func newRelayLink(p *Producer) *relayLink {
	return &relayLink{p: p, changed: make(chan struct{}, 1), active: map[pdid.Id]*tap{}, send: make(chan *api.AttachRequest, sendQueue)}
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
	// token is used at the next dial, the stream stays.
	same := l.assignment != nil && string(l.assignment.GetRelayId()) == string(ra.GetRelayId())
	l.assignment = ra
	if !same {
		select {
		case l.changed <- struct{}{}:
		default:
		}
	}
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
	wait := time.Second
	for {
		ra := l.current()
		if ra == nil {
			select {
			case <-ctx.Done():
				return nil
			case <-l.changed:
			}
			continue
		}
		err := l.attach(ctx, ra)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			l.p.log.Warn("relay", "err", err.Error(), "retry_in", wait.String())
			// A token the relay refused may have expired: ask for a fresh one.
			if fresh, err := api.NewProducerServiceClient(l.p.conn).Relay(ctx, &api.ProducerRelayRequest{}); err == nil {
				l.set(fresh.GetRelay())
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-l.changed:
			wait = time.Second
		case <-time.After(wait):
			if wait < 30*time.Second {
				wait *= 2
			}
		}
	}
}

// attach is one stream: Hello, then Start and Stop from the relay and Data
// from the taps, until either side ends it.
func (l *relayLink) attach(ctx context.Context, ra *api.RelayAssignment) error {
	if len(ra.GetEndpoints()) == 0 {
		return errors.New("the assignment carries no endpoint")
	}
	ep := ra.GetEndpoints()[0]
	addr := fmt.Sprintf("%s:%d", ep.GetHost(), ep.GetPort())
	conn, err := l.p.agent.DialAddr(ctx, addr, ep.GetScheme() == "http")
	if err != nil {
		return err
	}
	defer conn.Close()

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := api.NewRelayIngestClient(conn).Attach(sctx)
	if err != nil {
		return err
	}
	if err := stream.Send(api.AttachRequest_builder{Hello: api.AttachRequest_Hello_builder{PublishToken: ra.GetPublishToken()}.Build()}.Build()); err != nil {
		return err
	}
	relayId, _ := pdid.From(ra.GetRelayId())
	l.mu.Lock()
	l.relayId = relayId
	// Drain what an earlier stream left queued.
	for len(l.send) > 0 {
		<-l.send
	}
	l.mu.Unlock()
	defer l.clear()

	errs := make(chan error, 2)
	go func() {
		for {
			select {
			case <-sctx.Done():
				return
			case msg := <-l.send:
				if err := stream.Send(msg); err != nil {
					errs <- err
					return
				}
			}
		}
	}()
	go func() {
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

	select {
	case <-ctx.Done():
		stream.CloseSend()
		return nil
	case <-l.changed:
		// A new assignment: reconnect to it.
		l.mu.Lock()
		select {
		case l.changed <- struct{}{}:
		default:
		}
		l.mu.Unlock()
		stream.CloseSend()
		return nil
	case err := <-errs:
		return err
	}
}

func (l *relayLink) start(id pdid.Id) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.active[id]; !ok {
		l.active[id] = &tap{}
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

// feed is the tap in the capture loop: a started source's packets go to
// the relay from the next keyframe on, the tables first, in batches,
// through the live helper when the audio is not Opus (§38.7).
func (l *relayLink) feed(s *source, pk *Packet, reader *Reader) {
	if s.row == nil {
		return
	}
	id, err := pdid.From(s.row.GetId())
	if err != nil {
		return
	}
	l.mu.Lock()
	t, ok := l.active[id]
	if !ok {
		l.mu.Unlock()
		return
	}
	if !t.keyed {
		if !reader.IsKeyframe(pk) {
			l.mu.Unlock()
			return
		}
		t.keyed = true
		t.buf = append(t.buf[:0], reader.Tables()...)
		if t.h == nil && !t.raw && needsOpus(reader.Streams()) {
			h, err := l.startHelper(id, s)
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
	}
	t.buf = append(t.buf, pk.Data[:]...)
	if len(t.buf) < tapFlush {
		l.mu.Unlock()
		return
	}
	chunk := append([]byte(nil), t.buf...)
	t.buf = t.buf[:0]
	h := t.h
	l.mu.Unlock()
	if h != nil {
		if !h.write(chunk) {
			l.drop()
		}

		return
	}
	l.sendData(id, chunk)
}

// sendData queues one Data message for the relay, dropping it when the
// relay is behind rather than holding the capture loop.
func (l *relayLink) sendData(id pdid.Id, payload []byte) {
	msg := api.AttachRequest_builder{Data: api.AttachRequest_Data_builder{SourceId: id.Bytes(), Payload: append([]byte(nil), payload...)}.Build()}.Build()
	select {
	case l.send <- msg:
	default:
		l.drop()
	}
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
