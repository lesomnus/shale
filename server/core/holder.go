package core

import (
	"context"
	"errors"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/identity"
)

// People are roster's rows first (§33.1): `holder add` makes the person
// there and then here, anchored on the same identifier, and a password is
// issued there. Nothing here holds a secret.
type coreHolder struct {
	Core
	api.HolderServiceServer
}

func (s Core) Holder() api.HolderServiceServer {
	return coreHolder{s, s.Next().Holder()}
}

// Add makes the person at roster, in the caller's tenant, and then the row
// here. A row with an identifier of its own is refused: the identifier is
// roster's.
func (s coreHolder) Add(ctx context.Context, req *api.HolderAddRequest) (*api.Holder, error) {
	if s.d.Identity == nil {
		return nil, status.Error(codes.FailedPrecondition, "no identity store: this deployment has no roster to make a person at")
	}
	if len(req.GetId()) > 0 {
		return nil, invalid("id", "a person's identifier is roster's to mint")
	}
	if req.GetAlias() == "" {
		return nil, invalid("alias", "a person has an alias")
	}
	f, err := actor(ctx)
	if err != nil {
		return nil, err
	}
	t, err := s.d.Own.Tenant().Get(ctx, api.TenantGetRequest_builder{Ref: api.TenantRef_builder{Id: f.Tenant.Bytes()}.Build()}.Build())
	if err != nil {
		return nil, err
	}
	if ref := req.GetTenant(); ref != nil {
		if id := ref.GetId(); len(id) > 0 && string(id) != string(t.GetId()) {
			return nil, invalid("tenant", "a person is made in the caller's own tenant")
		}
		if a := ref.GetAlias(); a != "" && a != t.GetAlias() {
			return nil, invalid("tenant", "a person is made in the caller's own tenant")
		}
	}
	p, err := s.d.Identity.AddPerson(ctx, t.GetAlias(), req.GetAlias(), req.GetName())
	if err != nil {
		return nil, rosterErr(err)
	}
	r := api.HolderAddRequest_builder{
		Id:       p.Id.Bytes(),
		Tenant:   api.TenantRef_builder{Id: t.GetId()}.Build(),
		Alias:    p.Alias,
		Name:     req.GetName(),
		Desc:     req.GetDesc(),
		Labels:   req.GetLabels(),
		AllSites: req.GetAllSites(),
	}

	return s.HolderServiceServer.Add(ctx, r.Build())
}

// IssuePassword has roster give the person a fresh password and answers
// it once. A person who sees every site may, for anyone in the tenant.
func (s coreHolder) IssuePassword(ctx context.Context, req *api.HolderIssuePasswordRequest) (*api.HolderIssuePasswordResponse, error) {
	if s.d.Identity == nil {
		return nil, status.Error(codes.FailedPrecondition, "no identity store")
	}
	h, err := s.HolderServiceServer.Get(ctx, api.HolderGetRequest_builder{
		Ref:    req.GetRef(),
		Select: api.HolderSelect_builder{All: z.Ptr(true), Tenant: api.TenantSelect_builder{All: z.Ptr(true)}.Build()}.Build(),
	}.Build())
	if err != nil {
		return nil, err
	}
	password, err := s.d.Identity.IssuePassword(ctx, h.GetTenant().GetAlias(), h.GetAlias())
	if err != nil {
		return nil, rosterErr(err)
	}

	return api.HolderIssuePasswordResponse_builder{Password: password}.Build(), nil
}

// rosterErr is what roster's refusal means to a caller here.
func rosterErr(err error) error {
	switch {
	case errors.Is(err, identity.ErrExternal):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, identity.ErrNoTenant), errors.Is(err, identity.ErrNoPerson):
		return status.Error(codes.NotFound, err.Error())
	}
	if st, ok := status.FromError(err); ok && st.Code() != codes.OK {
		return status.Error(st.Code(), "roster: "+st.Message())
	}

	return status.Errorf(codes.Unavailable, "roster: %s", err.Error())
}
