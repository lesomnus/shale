package identity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"

	"github.com/lesomnus/z"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/config"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/payday/spin"

	"github.com/lesomnus/payday/auth"
	rostercli "github.com/lesomnus/roster/cli"
	rostercmd "github.com/lesomnus/roster/cmd"
	"github.com/lesomnus/roster/rstr"
)

// embedded is roster in this process: its server on an in-process
// listener nothing outside can dial, which is why it may believe what a
// caller says it is (payday's Plain), and the deployment's own door to it,
// the server the wall was never installed on, for the rows a deployment
// writes about itself (§34.7).
type embedded struct {
	rs  *rostercmd.Server
	cfg rostercmd.Config
	g   *grpc.Server
	lis *bufconn.Listener
	log *slog.Logger
}

// RosterDbFile is the embedded roster's database, beside the control
// plane's.
const RosterDbFile = "roster.db"

// unframed is a context for the deployment's own door at the embedded
// roster. Both apps read payday's one frame type, so a request's frame
// here would be a frame there, and roster would take Shale's actor for
// one of its own: the person asking for a password would be refused as
// asking for their own. What the deployment does through that door is
// nobody's request, so the frame stays behind; the deadline comes along.
func unframed(ctx context.Context) (context.Context, context.CancelFunc) {
	if d, ok := ctx.Deadline(); ok {
		return context.WithDeadline(context.Background(), d)
	}

	return context.WithCancel(context.Background())
}

func openEmbedded(ctx context.Context, cfg Config, stateDir string, log *slog.Logger) (*embedded, error) {
	rc := rostercmd.Config{Db: cfg.Db}
	if rc.Db.Driver == "" {
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			return nil, err
		}
		rc.Db.Driver = "sqlite3"
		rc.Db.Dsn = "file:" + filepath.Join(stateDir, RosterDbFile) + "?_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	}
	// Nothing watches roster's rows from here, and a broker would be the
	// wrong one for a second control plane on the same database anyway.
	rc.Watch.Broker = config.BrokerNone
	rs, err := rostercmd.Build(ctx, rc)
	if err != nil {
		return nil, fmt.Errorf("embedded roster: %w", err)
	}
	if err := rostercli.Migrate(ctx, rs); err != nil {
		rs.Close()
		return nil, fmt.Errorf("embedded roster: %w", err)
	}
	g, err := rs.Grpc(ctx, rc)
	if err != nil {
		rs.Close()
		return nil, fmt.Errorf("embedded roster: %w", err)
	}
	log.Info("identity: roster in this process, on its own database; reachable from this process only", "db", rc.Db.Driver)
	e := &embedded{rs: rs, cfg: rc, g: g, lis: bufconn.Listen(1 << 20), log: log}
	// Served from here on, so that whoever opened the store can call it:
	// `shale init` does, before anything runs. What Run adds is roster's
	// own housekeeping.
	go func() {
		if err := g.Serve(e.lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("identity: the embedded roster stopped", "err", err.Error())
		}
	}()

	return e, nil
}

func (e *embedded) run(ctx context.Context) error {
	err := spin.Run(ctx, slices.Values(e.rs.Spin))
	e.g.Stop()

	return err
}

func (e *embedded) Close() error {
	e.g.Stop()

	return e.rs.Close()
}

// agent writes the `shale` holder, the role it holds and the binding into
// a tenant, through the deployment's own door; it refuses a tenant that is
// not there rather than making one, since a tenant is a customer.
func (e *embedded) agent(ctx context.Context, tenant string) error {
	ctx, cancel := unframed(ctx)
	defer cancel()
	own := e.rs.Ungated
	t, err := own.Tenant().Get(ctx, rstr.TenantGetRequest_builder{Ref: rstr.TenantRef_builder{Alias: z.Ptr(tenant)}.Build()}.Build())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return fmt.Errorf("%w: %s", ErrNoTenant, tenant)
		}

		return err
	}
	tref := rstr.TenantRef_builder{Id: t.GetId()}.Build()
	h, err := own.Holder().Get(ctx, rstr.HolderGetRequest_builder{
		Ref: rstr.HolderRef_builder{Slug: rstr.HolderRefBySlug_builder{Alias: z.Ptr(Agent), Tenant: tref}.Build()}.Build(),
	}.Build())
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return err
		}
		h, err = own.Holder().Add(ctx, rstr.HolderAddRequest_builder{Tenant: tref, Alias: Agent, Name: "Shale"}.Build())
		if err != nil {
			return fmt.Errorf("holder %s: %w", Agent, err)
		}
	}
	r, err := own.Role().Get(ctx, rstr.RoleGetRequest_builder{
		Ref: rstr.RoleRef_builder{Slug: rstr.RoleRefBySlug_builder{Alias: z.Ptr(Agent), Tenant: tref}.Build()}.Build(),
	}.Build())
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return err
		}
		r, err = own.Role().Add(ctx, rstr.RoleAddRequest_builder{
			Tenant: tref, Alias: Agent, Name: "what Shale asks roster", Methods: AgentMethods,
		}.Build())
		if err != nil {
			return fmt.Errorf("role %s: %w", Agent, err)
		}
	} else if missing := slices.DeleteFunc(slices.Clone(AgentMethods), func(m string) bool { return slices.Contains(r.GetMethods(), m) }); len(missing) > 0 {
		// A role written by an older Shale: what this one asks is added.
		if _, err := own.Role().Patch(ctx, rstr.RolePatchRequest_builder{
			Ref:              rstr.RoleRef_builder{Id: r.GetId()}.Build(),
			Methods:          append(slices.Clone(r.GetMethods()), missing...),
			DateUpdatedForce: z.Ptr(true),
		}.Build()); err != nil {
			return fmt.Errorf("role %s: %w", Agent, err)
		}
	}
	vs, err := own.Binding().List(ctx, rstr.BindingListRequest_builder{
		Filters: []*rstr.BindingFilter{rstr.BindingFilter_builder{
			Role:   rstr.RoleRef_builder{Id: r.GetId()}.Build(),
			Holder: rstr.HolderRef_builder{Id: h.GetId()}.Build(),
		}.Build()},
	}.Build())
	if err != nil {
		return err
	}
	if len(vs.GetItems()) == 0 {
		if _, err := own.Binding().Add(ctx, rstr.BindingAddRequest_builder{
			Role: rstr.RoleRef_builder{Id: r.GetId()}.Build(), Holder: rstr.HolderRef_builder{Id: h.GetId()}.Build(),
		}.Build()); err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("binding: %w", err)
		}
	}

	return nil
}

func (e *embedded) tenants(ctx context.Context) ([]string, error) {
	ctx, cancel := unframed(ctx)
	defer cancel()
	var vs []string
	after := ""
	for {
		res, err := e.rs.Ungated.Tenant().List(ctx, rstr.TenantListRequest_builder{Size: 100, After: after}.Build())
		if err != nil {
			return nil, err
		}
		for _, t := range res.GetItems() {
			vs = append(vs, t.GetAlias())
		}
		if res.GetNext() == "" || len(res.GetItems()) == 0 {
			return vs, nil
		}
		after = res.GetNext()
	}
}

// Seed makes a tenant and its first person at the embedded roster, bound
// to the role that administers the tenant there, with a password shown
// once: what `shale init` does for the cluster operators and the first
// tenant (§33.1). An external roster refuses: its operator does this.
func (s *Store) Seed(ctx context.Context, tenant, holder string) (Person, string, error) {
	if s.em == nil {
		return Person{}, "", ErrExternal
	}
	ctx, cancel := unframed(ctx)
	defer cancel()
	seeded, err := rostercmd.Seed(ctx, s.em.rs, rostercmd.Seeding{Tenant: tenant, Holder: holder})
	if err != nil {
		return Person{}, "", err
	}
	secret, err := s.issue(ctx, seeded.Holder)
	if err != nil {
		return Person{}, "", err
	}
	p, err := s.lookupOwn(ctx, seeded.Holder)
	if err != nil {
		return Person{}, "", err
	}

	return p, secret, nil
}

// IssuePassword gives a person a fresh password, shown once, replacing
// whatever they had: the deployment's act, so only the embedded roster
// takes it; at an external one the operator does it there.
func (s *Store) IssuePassword(ctx context.Context, tenant, alias string) (string, error) {
	if s.em == nil {
		return "", ErrExternal
	}
	p, err := s.Lookup(ctx, tenant, alias)
	if err != nil {
		return "", err
	}

	return s.issue(ctx, p.Id)
}

func (s *Store) issue(ctx context.Context, holder pdid.Id) (string, error) {
	ctx, cancel := unframed(ctx)
	defer cancel()
	res, err := s.em.rs.Ungated.Credential().Issue(ctx, rstr.CredentialIssueRequest_builder{
		Ref: rstr.HolderRef_builder{Id: holder.Bytes()}.Build(), Kind: "password",
	}.Build())
	if err != nil {
		return "", err
	}

	return res.GetSecret(), nil
}

// SetPassword writes a person's password to the one given, at the
// embedded roster, as the deployment: for tests and a container's
// entrypoint, where the secret comes from somewhere already.
func (s *Store) SetPassword(ctx context.Context, tenant, alias, password string) error {
	if s.em == nil {
		return ErrExternal
	}
	p, err := s.Lookup(ctx, tenant, alias)
	if err != nil {
		return err
	}
	ctx, cancel := unframed(ctx)
	defer cancel()
	_, err = s.em.rs.Ungated.Credential().Set(ctx, rstr.CredentialSetRequest_builder{
		Ref: rstr.HolderRef_builder{Id: p.Id.Bytes()}.Build(), Kind: "password", Secret: []byte(password),
	}.Build())

	return err
}

func (s *Store) lookupOwn(ctx context.Context, holder pdid.Id) (Person, error) {
	ctx, cancel := unframed(ctx)
	defer cancel()
	v, err := s.em.rs.Ungated.Holder().Get(ctx, rstr.HolderGetRequest_builder{
		Ref:    rstr.HolderRef_builder{Id: holder.Bytes()}.Build(),
		Select: rstr.HolderSelect_builder{All: z.Ptr(true), Tenant: rstr.TenantSelect_builder{All: z.Ptr(true)}.Build()}.Build(),
	}.Build())
	if err != nil {
		return Person{}, err
	}

	return personOf(v)
}

// Adopt writes a tenant and its people, made before roster held them, into
// the embedded roster with the identifiers they already have, and issues
// each person a password: the upgrade of a deployment that predates roster
// (§33.1). What roster has already is left alone, so it can be run again.
// A person who sees every site is bound to the role that administers the
// tenant at roster, as init's first person is. Answers the passwords by
// alias, for the people it made.
func (s *Store) Adopt(ctx context.Context, tenant pdid.Id, alias, name string, people []Adoptee) (map[string]string, error) {
	if s.em == nil {
		return nil, ErrExternal
	}
	ctx, cancel := unframed(ctx)
	defer cancel()
	own := s.em.rs.Ungated
	tref := rstr.TenantRef_builder{Id: tenant.Bytes()}.Build()
	if _, err := own.Tenant().Get(ctx, rstr.TenantGetRequest_builder{Ref: tref}.Build()); err != nil {
		if status.Code(err) != codes.NotFound {
			return nil, err
		}
		if _, err := own.Tenant().Add(ctx, rstr.TenantAddRequest_builder{Id: tenant.Bytes(), Alias: alias, Name: name}.Build()); err != nil {
			return nil, fmt.Errorf("tenant @%s: %w", alias, err)
		}
	}
	passwords := map[string]string{}
	for _, p := range people {
		if _, err := own.Holder().Get(ctx, rstr.HolderGetRequest_builder{Ref: rstr.HolderRef_builder{Id: p.Id.Bytes()}.Build()}.Build()); err == nil {
			continue
		} else if status.Code(err) != codes.NotFound {
			return nil, err
		}
		if _, err := own.Holder().Add(ctx, rstr.HolderAddRequest_builder{Id: p.Id.Bytes(), Tenant: tref, Alias: p.Alias, Name: p.Name}.Build()); err != nil {
			return nil, fmt.Errorf("@%s/%s: %w", alias, p.Alias, err)
		}
		if p.Admin {
			if err := s.em.everything(ctx, tref, p.Id); err != nil {
				return nil, fmt.Errorf("@%s/%s: %w", alias, p.Alias, err)
			}
		}
		secret, err := s.issue(ctx, p.Id)
		if err != nil {
			return nil, fmt.Errorf("@%s/%s: %w", alias, p.Alias, err)
		}
		passwords[p.Alias] = secret
	}

	return passwords, nil
}

// OperatorRole is the role `shale init` writes at the embedded roster for
// whoever operates the cluster: [Everything], all of Shale.
const OperatorRole = "shale-operator"

// Grant binds a person of a tenant of the embedded roster to a role naming
// methods, made in the tenant when it has none by that alias, as the
// deployment, and answers the binding: what `shale init` does for the
// first operator where `auth.operators` is set, and what the tests grant
// with. A role already there is bound as the tenant has it. At an external
// roster its operator, or a tenant administrator, binds roles there.
func (s *Store) Grant(ctx context.Context, tenant, role string, methods []string, holder pdid.Id) (pdid.Id, error) {
	if s.em == nil {
		return pdid.Nil, ErrExternal
	}
	ctx, cancel := unframed(ctx)
	defer cancel()
	own := s.em.rs.Ungated
	tref := rstr.TenantRef_builder{Alias: z.Ptr(tenant)}.Build()
	r, err := own.Role().Get(ctx, rstr.RoleGetRequest_builder{
		Ref: rstr.RoleRef_builder{Slug: rstr.RoleRefBySlug_builder{Alias: z.Ptr(role), Tenant: tref}.Build()}.Build(),
	}.Build())
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return pdid.Nil, err
		}
		r, err = own.Role().Add(ctx, rstr.RoleAddRequest_builder{Tenant: tref, Alias: role, Name: role, Methods: methods}.Build())
		if err != nil {
			return pdid.Nil, fmt.Errorf("role @%s/%s: %w", tenant, role, err)
		}
	}
	b, err := own.Binding().Add(ctx, rstr.BindingAddRequest_builder{
		Role: rstr.RoleRef_builder{Id: r.GetId()}.Build(), Holder: rstr.HolderRef_builder{Id: holder.Bytes()}.Build(),
	}.Build())
	if err != nil {
		return pdid.Nil, fmt.Errorf("binding @%s/%s: %w", tenant, role, err)
	}

	return pdid.Id(b.GetId()), nil
}

// Ungrant takes a binding [Grant] made away.
func (s *Store) Ungrant(ctx context.Context, binding pdid.Id) error {
	if s.em == nil {
		return ErrExternal
	}
	ctx, cancel := unframed(ctx)
	defer cancel()
	_, err := s.em.rs.Ungated.Binding().Erase(ctx, rstr.BindingRef_builder{Id: binding.Bytes()}.Build())

	return err
}

// Adoptee is a person Adopt writes, as Shale knows them.
type Adoptee struct {
	Id    pdid.Id
	Alias string
	Name  string
	// Admin binds them to the role that administers the tenant at roster.
	Admin bool
}

// everything binds a person to the role that says everything roster
// serves, made when the tenant has none: what roster's own init writes
// for a tenant's first person.
func (e *embedded) everything(ctx context.Context, tref *rstr.TenantRef, holder pdid.Id) error {
	own := e.rs.Ungated
	r, err := own.Role().Get(ctx, rstr.RoleGetRequest_builder{
		Ref: rstr.RoleRef_builder{Slug: rstr.RoleRefBySlug_builder{Alias: z.Ptr(rostercmd.Everyverb), Tenant: tref}.Build()}.Build(),
	}.Build())
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return err
		}
		r, err = own.Role().Add(ctx, rstr.RoleAddRequest_builder{
			Tenant: tref, Alias: rostercmd.Everyverb, Methods: []string{rostercmd.EveryRosterMethod},
		}.Build())
		if err != nil {
			return err
		}
	}
	if _, err := own.Binding().Add(ctx, rstr.BindingAddRequest_builder{
		Role: rstr.RoleRef_builder{Id: r.GetId()}.Build(), Holder: rstr.HolderRef_builder{Id: holder.Bytes()}.Build(),
	}.Build()); err != nil && status.Code(err) != codes.AlreadyExists {
		return err
	}

	return nil
}

// Exchange is the exchange token an app of a tenant of the embedded roster
// is issued for Shale (§33.8): roster's `DelegationService/Exchange`, asked
// as the app's holder, naming Shale's holder in the tenant as its audience.
// What an app does at an external roster with its own key; here, what the
// tests prove an app with.
func (s *Store) Exchange(ctx context.Context, tenant, app string, methods []string) (string, error) {
	if s.em == nil {
		return "", ErrExternal
	}
	if err := s.ensureAgent(ctx, tenant); err != nil {
		return "", err
	}
	as := auth.PlainProvider("@" + tenant + "/" + app).Provide(ctx)
	v, err := rstr.NewDelegationServiceClient(s.conn).Exchange(as, rstr.DelegationExchangeRequest_builder{
		Audience: rstr.HolderRef_builder{
			Slug: rstr.HolderRefBySlug_builder{Alias: z.Ptr(Agent), Tenant: rstr.TenantRef_builder{Alias: z.Ptr(tenant)}.Build()}.Build(),
		}.Build(),
		Methods: methods,
	}.Build())
	if err != nil {
		return "", fmt.Errorf("roster: exchange: %w", err)
	}

	return v.GetToken(), nil
}

// Suspend suspends a person of the embedded roster, as a tenant's
// administrator does at roster: they may not sign in, and what they held
// stops working. For the tests.
func (s *Store) Suspend(ctx context.Context, holder pdid.Id) error {
	if s.em == nil {
		return ErrExternal
	}
	ctx, cancel := unframed(ctx)
	defer cancel()
	_, err := s.em.rs.Ungated.Holder().Patch(ctx, rstr.HolderPatchRequest_builder{
		Ref:              rstr.HolderRef_builder{Id: holder.Bytes()}.Build(),
		DateDisabled:     timestamppb.Now(),
		DateUpdatedForce: z.Ptr(true),
	}.Build())

	return err
}
