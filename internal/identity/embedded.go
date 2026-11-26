//go:build !js

package identity

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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
	rostercore "github.com/lesomnus/roster/server/core"
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

// ErrRosterFromBefore is an embedded roster's database this roster cannot
// migrate: one an earlier Shale's roster made, which roster carries nothing
// across for (docs/09-operations.md §32.2).
var ErrRosterFromBefore = errors.New("a roster database an earlier Shale made is not one this roster reads; if this is one, " +
	"move it aside and run `shale identity migrate`, which makes the tenants and people again " +
	"under their identifiers with new passwords (docs/09-operations.md §32.2)")

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
	// Whether there was a database to migrate: one that was made before is
	// one an earlier roster may have made.
	made := rc.Db.Driver != ""
	if rc.Db.Driver == "" {
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			return nil, err
		}
		if _, err := os.Stat(filepath.Join(stateDir, RosterDbFile)); err == nil {
			made = true
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
		if made {
			// roster's words are about its columns, and what they say to
			// do does not make an earlier roster's file this one's.
			return nil, fmt.Errorf("embedded roster: %w\n\n%w", err, ErrRosterFromBefore)
		}

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

// dial is a connection to the roster in this process.
func (e *embedded) dial() (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///roster",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return e.lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
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

// agent installs Shale in a tenant as an app, through the deployment's
// own door, as `roster app install --allow … --serves '/shale.*/*' shale`
// does it (roster's `cli.InstallApp`): the `shale` holder, a role of
// roster's naming what Shale asks roster ([AgentMethods]) bound to it, and
// `shale-everything`, a role of the app's naming all of Shale, which is
// what lets Shale's own roles be written there. roster's gate reads
// roster's roles alone, and `HolderService/Reaches` asked as Shale is
// answered of Shale's roles alone (#321). There is no control plane here,
// so no nomination and no key: Shale is answered as the holder by the
// plain header on a listener nothing else reaches. It refuses a tenant
// that is not there rather than making one, since a tenant is a customer.
func (e *embedded) agent(ctx context.Context, tenant string) error {
	ctx, cancel := unframed(ctx)
	defer cancel()
	if _, err := e.rs.Ungated.Tenant().Get(ctx, rstr.TenantGetRequest_builder{Ref: rstr.TenantRef_builder{Alias: z.Ptr(tenant)}.Build()}.Build()); err != nil {
		if status.Code(err) == codes.NotFound {
			return fmt.Errorf("%w: %s", ErrNoTenant, tenant)
		}

		return err
	}
	said, err := rostercli.InstallApp(ctx, e.rs, rostercli.AppInstall{
		Name:   Agent,
		Tenant: tenant,
		Allow:  AgentMethods,
		Serves: []string{Everything},
	})
	if err != nil {
		return fmt.Errorf("installing Shale in %s: %w", tenant, err)
	}
	e.log.DebugContext(ctx, "identity: Shale is an app of the tenant", "tenant", tenant, "said", strings.Join(said, " "))

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
		Holder: rstr.HolderRef_builder{Id: holder.Bytes()}.Build(), Kind: "password",
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
		Holder: rstr.HolderRef_builder{Id: p.Id.Bytes()}.Build(), Kind: "password", Secret: []byte(password),
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
		if err := s.em.unadministered(ctx, tref, people); err != nil {
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

// unadministered takes away the administrator roster made with a tenant
// Adopt just made (`rostercore.Administers`, `admin`, with an identifier of
// roster's minting) where the people adopted take its place: one of them is
// called that, and would be refused the name, or one of them administers the
// tenant and is bound to the same role. Where none does it stays, so the
// tenant is not left with nobody to administer it at roster.
func (e *embedded) unadministered(ctx context.Context, tref *rstr.TenantRef, people []Adoptee) error {
	if !slices.ContainsFunc(people, func(p Adoptee) bool { return p.Alias == rostercore.Administers || p.Admin }) {
		return nil
	}
	own := e.rs.Ungated
	h, err := own.Holder().Get(ctx, rstr.HolderGetRequest_builder{
		Ref: rstr.HolderRef_builder{Slug: rstr.HolderRefBySlug_builder{Alias: z.Ptr(rostercore.Administers), Tenant: tref}.Build()}.Build(),
	}.Build())
	switch {
	case status.Code(err) == codes.NotFound:
		return nil
	case err != nil:
		return err
	}
	if slices.ContainsFunc(people, func(p Adoptee) bool { return bytes.Equal(p.Id.Bytes(), h.GetId()) }) {
		return nil
	}
	if _, err := own.Holder().Erase(ctx, rstr.HolderRef_builder{Id: h.GetId()}.Build()); err != nil {
		return fmt.Errorf("the administrator roster made: %w", err)
	}

	return nil
}

// OperatorRole is the role `shale init` writes at the embedded roster for
// whoever operates the cluster: [Everything], all of Shale, a role of the
// app Shale's.
const OperatorRole = "shale-operator"

// Grant binds a person of a tenant of the embedded roster to a role naming
// methods, made in the tenant when it has none by that alias, as the
// deployment, and answers the binding: what `shale init` does for the
// first operator where `auth.operators` is set, and what the tests grant
// with. A role already there is bound as the tenant has it. At an external
// roster its operator, or a tenant administrator, binds roles there.
//
// A role naming Shale's methods is a role **of the app Shale** (`Role.app`,
// its holder in the tenant), which is what Shale's `Reaches` is answered
// of (#321); one naming roster's is roster's, which is what roster's gate
// reads. One that names both is refused, since a role is one app's. It is
// bound to the person, and never held through a team.
func (s *Store) Grant(ctx context.Context, tenant, role string, methods []string, holder pdid.Id) (pdid.Id, error) {
	if s.em == nil {
		return pdid.Nil, ErrExternal
	}
	shales := 0
	for _, m := range methods {
		if strings.HasPrefix(m, "/shale.") {
			shales++
		}
	}
	if shales > 0 && shales < len(methods) {
		return pdid.Nil, fmt.Errorf("role @%s/%s: Shale's methods and roster's are two roles, one of each app", tenant, role)
	}
	if shales > 0 {
		if err := s.ensureAgent(ctx, tenant); err != nil {
			return pdid.Nil, err
		}
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
		add := rstr.RoleAddRequest_builder{Tenant: tref, Alias: role, Name: role, Methods: methods}
		if shales > 0 {
			add.App = rstr.HolderRef_builder{Slug: rstr.HolderRefBySlug_builder{Alias: z.Ptr(Agent), Tenant: tref}.Build()}.Build()
		}
		r, err = own.Role().Add(ctx, add.Build())
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
// as the app's holder, naming Shale's holder in the tenant as the app it is for
// (`app`, which was `audience`).
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
		App: rstr.HolderRef_builder{
			Slug: rstr.HolderRefBySlug_builder{Alias: z.Ptr(Agent), Tenant: rstr.TenantRef_builder{Alias: z.Ptr(tenant)}.Build()}.Build(),
		}.Build(),
		Methods: methods,
	}.Build())
	if err != nil {
		return "", fmt.Errorf("roster: exchange: %w", err)
	}

	return v.GetToken(), nil
}

// SignOutEverywhere ends everything issued to a person of the embedded
// roster until now, as roster does when they are signed out everywhere.
// For the tests.
func (s *Store) SignOutEverywhere(ctx context.Context, holder pdid.Id) error {
	if s.em == nil {
		return ErrExternal
	}
	ctx, cancel := unframed(ctx)
	defer cancel()
	_, err := s.em.rs.Ungated.Holder().Patch(ctx, rstr.HolderPatchRequest_builder{
		Ref:              rstr.HolderRef_builder{Id: holder.Bytes()}.Build(),
		DateInvalidated:  timestamppb.Now(),
		DateUpdatedForce: z.Ptr(true),
	}.Build())

	return err
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

// IssueKey mints a person of a tenant of the embedded roster a key, as
// they do it themselves at roster's account page: roster's
// `ApiKeyService/Issue` asked as the person, for the app `app` by its
// alias in the tenant -- Shale's is [Agent] -- or for roster itself where
// it is empty, held to what they hold there, ending at `ends` unless that
// is zero. Answers the token, shown this once, and the key's identifier.
// For the tests.
func (s *Store) IssueKey(ctx context.Context, tenant, alias, name, app string, methods []string, ends time.Time) (string, pdid.Id, error) {
	if s.em == nil {
		return "", pdid.Nil, ErrExternal
	}
	if app == Agent {
		if err := s.ensureAgent(ctx, tenant); err != nil {
			return "", pdid.Nil, err
		}
	}
	tref := rstr.TenantRef_builder{Alias: z.Ptr(tenant)}.Build()
	req := rstr.ApiKeyIssueRequest_builder{
		Holder:  rstr.HolderRef_builder{Slug: rstr.HolderRefBySlug_builder{Alias: z.Ptr(alias), Tenant: tref}.Build()}.Build(),
		Alias:   name,
		Methods: methods,
	}
	if app != "" {
		req.App = rstr.HolderRef_builder{Slug: rstr.HolderRefBySlug_builder{Alias: z.Ptr(app), Tenant: tref}.Build()}.Build()
	}
	if !ends.IsZero() {
		req.DateExpired = timestamppb.New(ends)
	}
	as := auth.PlainProvider("@" + tenant + "/" + alias).Provide(ctx)
	v, err := rstr.NewApiKeyServiceClient(s.conn).Issue(as, req.Build())
	if err != nil {
		return "", pdid.Nil, fmt.Errorf("roster: issue: %w", err)
	}

	return v.GetToken(), pdid.Id(v.GetKey().GetId()), nil
}

// EndKey ends a key as its person does at roster's account page:
// `ApiKeyService/Erase`, asked as them. For the tests.
func (s *Store) EndKey(ctx context.Context, tenant, alias string, key pdid.Id) error {
	if s.em == nil {
		return ErrExternal
	}
	as := auth.PlainProvider("@" + tenant + "/" + alias).Provide(ctx)
	_, err := rstr.NewApiKeyServiceClient(s.conn).Erase(as, rstr.ApiKeyRef_builder{Id: key.Bytes()}.Build())

	return err
}

// InstallApp installs another app in a tenant of the embedded roster, as
// `roster app install --serves … --administer …` does, with nothing of
// roster's allowed it: what a key for another app is minted for. For the
// tests.
func (s *Store) InstallApp(ctx context.Context, tenant, app string, serves, administer []string) error {
	if s.em == nil {
		return ErrExternal
	}
	ctx, cancel := unframed(ctx)
	defer cancel()
	_, err := rostercli.InstallApp(ctx, s.em.rs, rostercli.AppInstall{
		Name: app, Tenant: tenant, Allow: []string{"/roster.HolderService/Get"}, Serves: serves, Administer: administer,
	})

	return err
}

// IssueKeyAs mints a person a key for Shale at the roster in this process,
// as they do it themselves at a front door: roster's `ApiKeyService/Issue`
// asked as the person, so the methods are held to what they hold at Shale
// (a role of Shale's), and the key's name, end and scope are refused in
// roster's words. This console is that front door where roster runs here
// (§33.1). Answers the token, shown this once, and the key.
func (s *Store) IssueKeyAs(ctx context.Context, tenant, holder pdid.Id, k TerminalKey) (string, *rstr.ApiKey, error) {
	if s.em == nil {
		return "", nil, ErrExternal
	}
	p, err := s.lookupOwn(ctx, holder)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "", nil, ErrNoPerson
		}

		return "", nil, err
	}
	if p.Tenant != tenant {
		return "", nil, ErrNoPerson
	}
	if err := s.ensureAgent(ctx, p.TenantAlias); err != nil {
		return "", nil, err
	}
	ctx, cancel := unframed(ctx)
	defer cancel()
	as := auth.PlainProvider("@" + p.TenantAlias + "/" + p.Alias).Provide(ctx)
	v, err := rstr.NewApiKeyServiceClient(s.conn).Issue(as, issueRequest(tenant, holder, k))
	if err != nil {
		return "", nil, err
	}
	key := v.GetKey()
	if got, err := s.Keys(ctx, tenant, holder); err == nil {
		// The row as listing answers it, with the app's name beside it.
		for _, w := range got {
			if bytes.Equal(w.GetId(), key.GetId()) {
				key = w
			}
		}
	}

	return v.GetToken(), key, nil
}
