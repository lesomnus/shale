package relay

import (
	"errors"
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// ingest is the RelayIngest service: one stream per producer (§39.3). The
// producer says Hello with its publish token; the relay says which sources
// it accepted, then Start and Stop as viewers come and go; the producer
// sends Data for the started ones.
type ingest struct {
	api.UnimplementedRelayIngestServer
	r *Relay
}

// attachment is one producer's stream, the feeder of its sources.
type attachment struct {
	r       *Relay
	stream  api.RelayIngest_AttachServer
	sendMu  sync.Mutex
	sources map[pdid.Id]*source
	actor   pdid.Id
}

func (a *attachment) send(msg *api.AttachResponse) {
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	if err := a.stream.Send(msg); err != nil {
		a.r.log.Debug("ingest send", "err", err.Error())
	}
}

func (a *attachment) start(id pdid.Id) {
	a.r.log.Info("start", "source", id.String(), "producer", a.actor.String())
	a.send(api.AttachResponse_builder{Start: api.AttachResponse_Start_builder{SourceId: id.Bytes()}.Build()}.Build())
}

func (a *attachment) stop(id pdid.Id) {
	a.r.log.Info("stop", "source", id.String(), "producer", a.actor.String())
	a.send(api.AttachResponse_builder{Stop: api.AttachResponse_Stop_builder{SourceId: id.Bytes()}.Build()}.Build())
}

func (g *ingest) Attach(stream api.RelayIngest_AttachServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "the first message is Hello")
	}
	claims, err := g.r.verify(hello.GetPublishToken(), api.TokenOp_TOKEN_OP_PUBLISH)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	actor, _ := pdid.From(claims.GetActor())

	a := &attachment{r: g.r, stream: stream, sources: map[pdid.Id]*source{}, actor: actor}
	var accepted [][]byte
	for _, b := range claims.GetSources() {
		id, err := pdid.From(b)
		if err != nil {
			continue
		}
		s := g.r.sources.get(id)
		a.sources[id] = s
		accepted = append(accepted, b)
	}
	if len(accepted) == 0 {
		return status.Error(codes.PermissionDenied, "the token names no source")
	}
	a.send(api.AttachResponse_builder{Welcome: api.AttachResponse_Welcome_builder{Sources: accepted}.Build()}.Build())
	for _, s := range a.sources {
		s.attach(a)
	}
	g.r.sources.attached(1)
	g.r.log.Info("producer attached", "producer", actor.String(), "sources", len(accepted))
	defer func() {
		for _, s := range a.sources {
			s.detach(a)
		}
		g.r.sources.attached(-1)
		g.r.log.Info("producer detached", "producer", actor.String())
	}()

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if stream.Context().Err() != nil {
				return nil
			}

			return err
		}
		d := msg.GetData()
		if d == nil {
			continue
		}
		id, err := pdid.From(d.GetSourceId())
		if err != nil {
			continue
		}
		s := a.sources[id]
		if s == nil {
			// Not among the token's sources: ignored, not fatal.
			continue
		}
		s.feed(d.GetPayload())
	}
}
