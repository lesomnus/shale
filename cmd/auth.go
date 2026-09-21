package cmd

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/core"
)

// Resolver turns a claim into the frame a request is served in.
//
// Two kinds of principal exist (§33.1): people, who are Holders and arrive
// with a session or, in development, a plain header; and hosts, whose
// certificate names their row. A host is served only while its row is
// adopted and records the serial of the certificate it presented, which is
// how an erased host or a replaced certificate is refused without a
// revocation list (§33.4).
//
// It reads through the server the wall was never installed on: working out
// who is calling happens before there is anybody to be walled by.
func Resolver(s *Server) auth.Resolver {
	own := s.Ungated

	return auth.ResolverFunc(func(ctx context.Context, id auth.Identity) (*frame.Frame, error) {
		if id.Id == "" {
			// A name: a person.
			if id.Tenant == "" || id.Alias == "" {
				return nil, fmt.Errorf("names nobody: %w", auth.ErrNoCredential)
			}
			ref := api.HolderRef_builder{
				Slug: api.HolderRefBySlug_builder{
					Alias:  z.Ptr(id.Alias),
					Tenant: api.TenantRef_builder{Alias: z.Ptr(id.Tenant)}.Build(),
				}.Build(),
			}.Build()
			f, err := holderFrame(ctx, own, ref)
			if err == nil || !errors.Is(err, auth.ErrNoCredential) || s.Identity == nil {
				return f, err
			}
			// Nobody here by that name: somebody roster knows arriving for
			// the first time gets their rows now (§33.1). The credential
			// itself was believed by whoever handed it over, which for a
			// name is the plain header of development mode.
			p, err := s.Identity.Lookup(ctx, id.Tenant, id.Alias)
			if err != nil {
				return nil, fmt.Errorf("%w: %s", auth.ErrNoCredential, err)
			}
			if _, err := s.Provision(ctx, p); err != nil {
				return nil, err
			}

			return holderFrame(ctx, own, ref)
		}

		k, err := pdid.Parse(id.Id)
		if err != nil {
			return nil, fmt.Errorf("%w: %s", auth.ErrNoCredential, err)
		}

		switch k.Domain() {
		case core.DomHolder:
			return holderFrame(ctx, own, api.HolderRef_builder{Id: k.Bytes()}.Build())
		case core.DomProducer, core.DomReader, core.DomNode, core.DomRelay:
			return hostFrame(ctx, own, k, id)
		default:
			return nil, fmt.Errorf("%w: a %s cannot call", auth.ErrNoCredential, k.Domain())
		}
	})
}

func holderFrame(ctx context.Context, own api.Server, ref *api.HolderRef) (*frame.Frame, error) {
	v, err := own.Holder().Get(ctx, api.HolderGetRequest_builder{
		Ref: ref,
		Select: api.HolderSelect_builder{
			All:    z.Ptr(true),
			Tenant: api.TenantSelect_builder{All: z.Ptr(true)}.Build(),
		}.Build(),
	}.Build())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, fmt.Errorf("%w: %s", auth.ErrNoCredential, err)
		}

		return nil, err
	}

	actor, err := pdid.From(v.GetId())
	if err != nil {
		return nil, err
	}
	tenant, err := pdid.From(v.GetTenant().GetId())
	if err != nil {
		return nil, err
	}

	return frame.New(actor, tenant, frame.Grant{}).WithRow(v), nil
}

// hostFrame resolves a host by its row, and refuses it unless adopted and,
// over mTLS, presenting the certificate the row records.
func hostFrame(ctx context.Context, own api.Server, k pdid.Id, id auth.Identity) (*frame.Frame, error) {
	var (
		state  api.HostState
		serial string
		tenant pdid.Id
		row    any
	)

	switch k.Domain() {
	case core.DomProducer:
		v, err := own.Producer().Get(ctx, api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: k.Bytes()}.Build()}.Build())
		if err != nil {
			return nil, refused(err)
		}
		state, serial, row = v.GetState(), v.GetCertSerial(), v
		tenant, _ = pdid.From(v.GetTenant().GetId())
	case core.DomReader:
		v, err := own.Reader().Get(ctx, api.ReaderGetRequest_builder{Ref: api.ReaderRef_builder{Id: k.Bytes()}.Build()}.Build())
		if err != nil {
			return nil, refused(err)
		}
		state, serial, row = v.GetState(), v.GetCertSerial(), v
		tenant, _ = pdid.From(v.GetTenant().GetId())
	case core.DomNode:
		v, err := own.Node().Get(ctx, api.NodeGetRequest_builder{Ref: api.NodeRef_builder{Id: k.Bytes()}.Build()}.Build())
		if err != nil {
			return nil, refused(err)
		}
		state, serial, row = v.GetState(), v.GetCertSerial(), v
	case core.DomRelay:
		v, err := own.Relay().Get(ctx, api.RelayGetRequest_builder{Ref: api.RelayRef_builder{Id: k.Bytes()}.Build()}.Build())
		if err != nil {
			return nil, refused(err)
		}
		state, serial, row = v.GetState(), v.GetCertSerial(), v
	}

	if state != api.HostState_HOST_STATE_ADOPTED {
		return nil, fmt.Errorf("%w: host %s is not adopted", auth.ErrNoCredential, k)
	}

	// The serial of the certificate this connection was made with must be
	// the one the row records; a plain header (development) has none.
	if id.Method == auth.MethodMTls {
		cert, ok := peerCert(ctx)
		if !ok {
			return nil, fmt.Errorf("%w: no client certificate", auth.ErrNoCredential)
		}
		if pki.Serial(cert) != serial {
			return nil, fmt.Errorf("%w: the certificate of host %s was replaced", auth.ErrNoCredential, k)
		}
	}

	f := frame.New(k, tenant, frame.Grant{})
	if m, ok := row.(interface{ ProtoReflect() }); ok {
		_ = m
	}

	return f, nil
}

func refused(err error) error {
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("%w: %s", auth.ErrNoCredential, err)
	}

	return err
}

func peerCert(ctx context.Context) (*x509.Certificate, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, false
	}
	info, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(info.State.VerifiedChains) == 0 || len(info.State.VerifiedChains[0]) == 0 {
		return nil, false
	}

	return info.State.VerifiedChains[0][0], true
}
