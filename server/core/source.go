package core

import (
	"context"
	"net"

	"google.golang.org/grpc/peer"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/source"
)

// Rules on the generated verbs: a source gets a stable ordinal and its set's
// site; a site member names exactly one of a person or a reader.

type coreSource struct {
	Core
	api.SourceServiceServer
}

func (s Core) Source() api.SourceServiceServer {
	return coreSource{s, s.Next().Source()}
}

// Get is the generated read, behind the wall and the scope beside it; a
// key whose list of cameras is gone is told so (§33.1).
func (s coreSource) Get(ctx context.Context, req *api.SourceGetRequest) (*api.Source, error) {
	if err := s.reachGone(ctx); err != nil {
		return nil, err
	}

	return s.SourceServiceServer.Get(ctx, req)
}

// Add assigns the ordinal: one past the highest the set has ever had,
// erased members included, so an ordinal is never reused (§7). The site is
// copied from the set, because payday narrows each row by its own field 3.
// What the caller says of the source is kept: alias, name, desc, labels,
// zone, profile, its content type (§7, §38.9) and date_created. Not taken
// from the request: tenant (the set's), observed and starvation (the CP's),
// and live_capable, which its producer says when it negotiates (§39.3).
func (s coreSource) Add(ctx context.Context, req *api.SourceAddRequest) (*api.Source, error) {
	if req.GetSet() == nil {
		return nil, invalid("set", "a source belongs to a set")
	}

	set, err := s.Next().Set().Get(ctx, api.SetGetRequest_builder{
		Ref: req.GetSet(),
	}.Build())
	if err != nil {
		return nil, err
	}
	// A producer registers sources into its own set and no other (§38.4).
	if f, err := actor(ctx); err == nil && kindOf(f.Actor) == DomProducer {
		if err := s.producerOwns(ctx, f.Actor, set); err != nil {
			return nil, err
		}
	}

	var out *api.Source
	err = s.tx(ctx, func(ctx context.Context, nx api.Server) error {
		// The set's site as it is now, held: a set placed in a site
		// meanwhile has this source placed with it (§7).
		site, err := s.heldSite(ctx, set.GetId())
		if err != nil {
			return err
		}
		last, err := s.ent(ctx).Source.Query().
			Where(source.SetIdEQ(mustId(set.GetId()).Uuid())).
			Order(ent.Desc(source.FieldOrdinal)).
			First(ctx)
		ordinal := int32(0)
		if err == nil {
			ordinal = last.Ordinal + 1
		} else if !ent.IsNotFound(err) {
			return err
		}

		r := api.SourceAddRequest_builder{
			Id:          req.GetId(),
			Tenant:      api.TenantRef_builder{Id: set.GetTenant().GetId()}.Build(),
			Alias:       req.GetAlias(),
			Name:        req.GetName(),
			Desc:        req.GetDesc(),
			Labels:      req.GetLabels(),
			Set:         api.SetRef_builder{Id: set.GetId()}.Build(),
			Ordinal:     ordinal,
			Zone:        req.GetZone(),
			Profile:     req.GetProfile(),
			ContentType: req.GetContentType(),
			DateCreated: req.GetDateCreated(),
			Site:        siteRefOf(site),
		}
		v, err := nx.Source().Add(ctx, r.Build())
		out = v

		return err
	})
	if err != nil {
		return nil, err
	}

	return out, nil
}

type coreSiteMember struct {
	Core
	api.SiteMemberServiceServer
}

func (s Core) SiteMember() api.SiteMemberServiceServer {
	return coreSiteMember{s, s.Next().SiteMember()}
}

func (s coreSiteMember) Add(ctx context.Context, req *api.SiteMemberAddRequest) (*api.SiteMember, error) {
	if (req.GetHolder() == nil) == (req.GetReader() == nil) {
		return nil, invalid("holder", "name exactly one of a holder or a reader")
	}

	return s.SiteMemberServiceServer.Add(ctx, req)
}

func peerAddr(ctx context.Context) (string, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return "", false
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return p.Addr.String(), true
	}

	return host, true
}
