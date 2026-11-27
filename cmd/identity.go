package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

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
// of a tenant sees every site, by their row's all_sites; the rest see the
// sites a site's kind and its members give them. Where roster says what
// people may change (`auth.operators`), nobody's row is made with
// all_sites, the first person's neither: who sees every site there is whom
// roster grants all of Shale, and a row's all_sites is not read (Sites).
// A row that is there already is refreshed where its names drifted, and
// its all_sites left as it is. Rows are made no other way.
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

	// A row of their alias under another identifier is them, at a roster
	// that names them by another one (#322).
	switch here, err := own.Holder().Get(ctx, api.HolderGetRequest_builder{
		Ref: api.HolderRef_builder{Slug: api.HolderRefBySlug_builder{
			Alias: z.Ptr(p.Alias), Tenant: api.TenantRef_builder{Id: p.Tenant.Bytes()}.Build(),
		}.Build()}.Build(),
	}.Build()); {
	case err == nil:
		v := PersonElsewhere{TenantAlias: p.TenantAlias, Alias: p.Alias, Here: pdid.Id(here.GetId()), Roster: p.Id}
		s.elsewhere.say(ctx, s.Deps.Log, v, "identity: roster names a person by another identifier than this deployment has them under; they do not sign in until they are made at roster with this deployment's, or their row here is erased",
			"tenant", v.TenantAlias, "alias", v.Alias, "here", v.Here.String(), "roster", v.Roster.String())

		return nil, v
	case status.Code(err) != codes.NotFound:
		return nil, err
	}

	// The first person of a tenant administers it, where roster does not
	// say who does.
	others, err := own.Holder().List(ctx, api.HolderListRequest_builder{
		Filters: []*api.HolderFilter{api.HolderFilter_builder{Tenant: api.TenantRef_builder{Id: p.Tenant.Bytes()}.Build()}.Build()},
		Size:    1,
	}.Build())
	if err != nil {
		return nil, err
	}
	first := len(others.GetItems()) == 0
	all := first && !s.cfg.Auth.Operators.On()
	h, err = own.Holder().Add(ctx, api.HolderAddRequest_builder{
		Id:       p.Id.Bytes(),
		Tenant:   api.TenantRef_builder{Id: p.Tenant.Bytes()}.Build(),
		Alias:    p.Alias,
		Name:     p.Name,
		AllSites: all,
	}.Build())
	if err != nil {
		return nil, fmt.Errorf("provision @%s/%s: %w", p.TenantAlias, p.Alias, err)
	}
	slog.Default().InfoContext(ctx, "provisioned person", "tenant", p.TenantAlias, "alias", p.Alias, "all_sites", all)

	return h, nil
}

// provisionTenant makes the tenant's row when it is missing. The lock is
// held.
//
// A row of that alias under another identifier is the tenant this
// deployment has, at a roster that names it by another one (#322): a
// roster it was moved to, or one made again. Every row of the tenant hangs
// off the identifier, so it is refused with what to do, and not made a
// second time or moved onto the new one.
func (s *Server) provisionTenant(ctx context.Context, id pdid.Id, alias, name string) error {
	own := s.Ungated
	_, err := own.Tenant().Get(ctx, api.TenantGetRequest_builder{Ref: api.TenantRef_builder{Id: id.Bytes()}.Build()}.Build())
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return err
	}
	switch here, err := own.Tenant().Get(ctx, api.TenantGetRequest_builder{Ref: api.TenantRef_builder{Alias: z.Ptr(alias)}.Build()}.Build()); {
	case err == nil:
		v := TenantElsewhere{Alias: alias, Here: pdid.Id(here.GetId()), Roster: id}
		s.elsewhere.say(ctx, s.Deps.Log, v, "identity: roster names a tenant by another identifier than this deployment has it under; nobody of it signs in until it is made at roster with this deployment's",
			"tenant", v.Alias, "here", v.Here.String(), "roster", v.Roster.String())

		return v
	case status.Code(err) != codes.NotFound:
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

// ErrTenantElsewhere is a tenant this deployment has under another
// identifier than the roster it asks names it by; [TenantElsewhere] says
// which.
var ErrTenantElsewhere = errors.New("roster names this tenant by another identifier than this deployment has it under")

// TenantElsewhere is a sign-in for a tenant whose alias this deployment
// has under another identifier (#322). Shale's rows are anchored on
// roster's identifiers, so a tenant moved to another roster is made there
// with the one it has here, which `roster tenant add` takes; until then
// nobody of it signs in. Nothing moves the rows onto the new identifier.
//
// Its error says both identifiers, for the log; what it answers a caller
// says what to do and neither of them.
type TenantElsewhere struct {
	Alias string
	// Here is the tenant's identifier in this deployment, and Roster the
	// one the roster asked gives it.
	Here, Roster pdid.Id
}

func (e TenantElsewhere) Error() string {
	return fmt.Sprintf("tenant @%s is %s here and %s at roster: %s", e.Alias, e.Here, e.Roster, e.todo())
}

func (e TenantElsewhere) todo() string {
	return "make the tenant at roster with the identifier this deployment has it under, which `shale tenant ls` prints and `roster tenant add` takes, and its people sign in again"
}

// Is makes it [ErrTenantElsewhere].
func (TenantElsewhere) Is(target error) bool { return target == ErrTenantElsewhere }

// GRPCStatus is what a caller is answered: FailedPrecondition, saying what
// to do and no identifier.
func (e TenantElsewhere) GRPCStatus() *status.Status {
	return status.New(codes.FailedPrecondition, fmt.Sprintf("roster names tenant @%s by another identifier than this deployment has it under; %s", e.Alias, e.todo()))
}

// Message is the sentence for a page: what [TenantElsewhere.GRPCStatus]
// says.
func (e TenantElsewhere) Message() string { return e.GRPCStatus().Message() }

// ErrPersonElsewhere is a person this deployment has under another
// identifier than the roster it asks names them by; [PersonElsewhere] says
// which.
var ErrPersonElsewhere = errors.New("roster names this person by another identifier than this deployment has them under")

// PersonElsewhere is a sign-in of somebody whose alias a row of this
// deployment has under another identifier (#322): the person from before,
// at a roster that names them by another one. Made at roster with the
// identifier they have here, they are who they were; or an administrator
// erases the row here, and they arrive as somebody new, without what the
// row had. Nothing moves the row onto the new identifier.
type PersonElsewhere struct {
	TenantAlias, Alias string
	// Here is the person's identifier in this deployment, and Roster the one
	// the roster asked gives them.
	Here, Roster pdid.Id
}

func (e PersonElsewhere) Error() string {
	return fmt.Sprintf("@%s/%s is %s here and %s at roster: %s", e.TenantAlias, e.Alias, e.Here, e.Roster, e.todo())
}

func (e PersonElsewhere) todo() string {
	return "make them at roster with the identifier this deployment has them under, which `shale holder ls` prints and `roster holder add` takes; " +
		"or an administrator erases their row here (`shale holder erase`), and they arrive as somebody new, " +
		"without the site memberships, the all_sites and the delegations of the row from before"
}

// Is makes it [ErrPersonElsewhere].
func (PersonElsewhere) Is(target error) bool { return target == ErrPersonElsewhere }

// GRPCStatus is what a caller is answered: FailedPrecondition, saying what
// to do and no identifier.
func (e PersonElsewhere) GRPCStatus() *status.Status {
	return status.New(codes.FailedPrecondition, fmt.Sprintf("roster names @%s/%s by another identifier than this deployment has them under; %s", e.TenantAlias, e.Alias, e.todo()))
}

// Message is the sentence for a page: what [PersonElsewhere.GRPCStatus]
// says.
func (e PersonElsewhere) Message() string { return e.GRPCStatus().Message() }

// elsewhereError is [TenantElsewhere] or [PersonElsewhere], for a page.
type elsewhereError interface {
	error
	Message() string
}

// elsewhere says each [TenantElsewhere] and [PersonElsewhere] once in a
// process, at ERROR: the first sign-in is the operator's news, and every
// one after it is the same news.
type elsewhere struct {
	mu   sync.Mutex
	said map[any]bool
}

func (w *elsewhere) say(ctx context.Context, log *slog.Logger, key any, msg string, args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.said[key] {
		return
	}
	if w.said == nil {
		w.said = map[any]bool{}
	}
	w.said[key] = true
	if log == nil {
		log = slog.Default()
	}
	log.ErrorContext(ctx, msg, args...)
}
