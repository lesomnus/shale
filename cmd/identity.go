package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/identity"
)

// Provision makes Shale's rows for a person roster vouched for (§33.1):
// the tenant and the holder, anchored on roster's identifiers, so that
// `Holder.id` is the `sub` every product knows them by. The first person
// of a tenant sees every site; the rest see what a Shale admin gives
// them. A row that is there already is refreshed where its names drifted.
// Rows are made no other way.
func (s *Server) Provision(ctx context.Context, p identity.Person) (*api.Holder, error) {
	s.provisionMu.Lock()
	defer s.provisionMu.Unlock()
	if err := s.provisionTenant(ctx, p.Tenant, p.TenantAlias, p.TenantName); err != nil {
		return nil, err
	}
	own := s.Ungated
	h, err := own.Holder().Get(ctx, api.HolderGetRequest_builder{
		Ref:    api.HolderRef_builder{Id: p.Id.Bytes()}.Build(),
		Select: api.HolderSelect_builder{All: z.Ptr(true), Tenant: api.TenantSelect_builder{All: z.Ptr(true)}.Build()}.Build(),
	}.Build())
	switch {
	case err == nil:
		if h.GetAlias() == p.Alias && h.GetName() == p.Name {
			return h, nil
		}
		patch := api.HolderPatchRequest_builder{Ref: api.HolderRef_builder{Id: p.Id.Bytes()}.Build(), DateUpdatedForce: z.Ptr(true)}
		if h.GetAlias() != p.Alias {
			patch.Alias = z.Ptr(p.Alias)
		}
		if h.GetName() != p.Name {
			patch.Name = z.Ptr(p.Name)
		}

		return own.Holder().Patch(ctx, patch.Build())
	case status.Code(err) != codes.NotFound:
		return nil, err
	}

	// The first person of a tenant administers it.
	others, err := own.Holder().List(ctx, api.HolderListRequest_builder{
		Filters: []*api.HolderFilter{api.HolderFilter_builder{Tenant: api.TenantRef_builder{Id: p.Tenant.Bytes()}.Build()}.Build()},
		Size:    1,
	}.Build())
	if err != nil {
		return nil, err
	}
	first := len(others.GetItems()) == 0
	h, err = own.Holder().Add(ctx, api.HolderAddRequest_builder{
		Id:       p.Id.Bytes(),
		Tenant:   api.TenantRef_builder{Id: p.Tenant.Bytes()}.Build(),
		Alias:    p.Alias,
		Name:     p.Name,
		AllSites: first,
	}.Build())
	if err != nil {
		return nil, fmt.Errorf("provision @%s/%s: %w", p.TenantAlias, p.Alias, err)
	}
	slog.Default().InfoContext(ctx, "provisioned person", "tenant", p.TenantAlias, "alias", p.Alias, "all_sites", first)

	return h, nil
}

// provisionTenant makes the tenant's row when it is missing. The lock is
// held.
func (s *Server) provisionTenant(ctx context.Context, id pdid.Id, alias, name string) error {
	own := s.Ungated
	_, err := own.Tenant().Get(ctx, api.TenantGetRequest_builder{Ref: api.TenantRef_builder{Id: id.Bytes()}.Build()}.Build())
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return err
	}
	if _, err := own.Tenant().Add(ctx, api.TenantAddRequest_builder{Id: id.Bytes(), Alias: alias, Name: name}.Build()); err != nil {
		return fmt.Errorf("provision tenant @%s: %w", alias, err)
	}
	slog.Default().InfoContext(ctx, "provisioned tenant", "alias", alias)

	return nil
}

// ProvisionTenant makes a tenant's row from what roster says about it,
// with nobody in it yet: the cluster tenant, before its first operator
// signs in.
func (s *Server) ProvisionTenant(ctx context.Context, alias string) (pdid.Id, error) {
	id, name, err := s.Identity.Tenant(ctx, alias)
	if err != nil {
		return pdid.Nil, err
	}
	s.provisionMu.Lock()
	defer s.provisionMu.Unlock()

	return id, s.provisionTenant(ctx, id, alias, name)
}

// Prepare runs after the database is migrated and before anything is
// served: the cluster tenant is found, at roster when it is not here yet.
func (s *Server) Prepare(ctx context.Context) error {
	if !s.ClusterTenant.IsZero() || s.Identity == nil {
		return nil
	}
	id, err := s.ProvisionTenant(ctx, s.clusterAlias())
	if err != nil {
		if errors.Is(err, identity.ErrNoTenant) {
			// Not initialized, or nobody made it yet: the cluster API has
			// no operators until then.
			return nil
		}

		return fmt.Errorf("the cluster tenant: %w", err)
	}
	s.ClusterTenant = id
	s.Deps.ClusterTenant = id

	return nil
}

func (s *Server) clusterAlias() string {
	if v := s.cfg.Control.ClusterTenant; v != "" {
		return v
	}

	return "cluster"
}
