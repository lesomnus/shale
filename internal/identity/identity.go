// Package identity is who somebody is, which roster answers (§33.1).
//
// The tenants and the people in them are roster's rows; Shale's are
// anchored on the same identifiers and made on demand when a person roster
// vouched for first arrives. A password is checked where it is held and
// never here. roster is either a deployment of its own that this one
// reaches over the wire, or one run inside the control plane's process on
// its own database, the way the node and the relay are run in `serve all`
// (§34.7).
//
// Whichever it is, Shale talks to it the same way: as the holder `shale`
// in each tenant it serves, holding the role that lets it check a password
// and read a person. On an external roster that holder's tenant key is
// what the operator hands over; on the embedded one Shale writes the
// holder and its role itself and names it on an in-process listener
// nothing else can reach.
package identity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/config"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/roster/rstr"
)

// Config is `auth.roster` (§36.1).
type Config struct {
	// Addr is an external roster's data plane. Empty runs one in this
	// process, on Db.
	Addr string `yaml:"addr"`
	// Insecure dials an external roster in plaintext, for a lab.
	Insecure bool `yaml:"insecure"`
	// CaFile pins the CA an external roster's certificate chains to; empty
	// is the system pool.
	CaFile string `yaml:"ca_file"`
	// Keys are the tenant keys Shale acts with on an external roster, one
	// per tenant it serves, by the tenant's alias: `env:NAME`, `file:PATH`,
	// or the key itself. A tenant with no key here is one this deployment
	// does not serve, whatever roster holds.
	Keys map[string]string `yaml:"keys"`
	// Db is the embedded roster's database. Empty is SQLite beside the
	// control plane's state; a deployment with more than one control plane
	// names one they share.
	Db config.DbConfig `yaml:"db"`
}

// Embedded says whether roster runs in this process.
func (c Config) Embedded() bool { return c.Addr == "" }

// Agent is the alias of the holder Shale acts as in every tenant.
const Agent = "shale"

// AgentMethods is what that holder may call: checking a password, reading
// a person and the tenant, making a person.
var AgentMethods = []string{
	"/roster.VouchService/Verify",
	"/roster.HolderService/Get",
	"/roster.HolderService/Add",
	"/roster.TenantService/Get",
	"/roster.MeService/Get",
}

// Person is who roster said somebody is: the identifiers Shale's rows are
// anchored on, and the names to make them with.
type Person struct {
	Id     pdid.Id
	Tenant pdid.Id
	Alias  string
	Name   string

	TenantAlias string
	TenantName  string
}

var (
	// ErrRefused is a wrong password, or nobody by that name: one answer
	// for both, as roster gives it.
	ErrRefused = errors.New("refused")
	// ErrNoTenant is a tenant this deployment does not serve: no key for it
	// on an external roster, no such tenant on the embedded one.
	ErrNoTenant = errors.New("no such tenant here")
	// ErrNoPerson is nobody by that name in a tenant this deployment serves.
	ErrNoPerson = errors.New("no such person")
	// ErrExternal is an operation only the embedded roster takes: on an
	// external one it is the operator's, at roster.
	ErrExternal = errors.New("people and tenants are made at roster, which is not this process")
)

// Locked is a refusal that says when the account opens again.
type Locked struct{ Until time.Time }

func (e Locked) Error() string      { return "locked until " + e.Until.UTC().Format(time.RFC3339) }
func (Locked) Is(target error) bool { return target == ErrRefused }

// Store is the connection to roster, whichever kind.
type Store struct {
	cfg  Config
	log  *slog.Logger
	conn *grpc.ClientConn
	// keys are the tenant keys, by tenant alias, on an external roster.
	keys map[string]string

	// The embedded roster, nil for an external one.
	em *embedded

	mu     sync.Mutex
	agents map[string]bool
}

// Open connects to roster, or builds and migrates the embedded one; Run
// serves it. `stateDir` is where the embedded database goes when Db names
// none.
func Open(ctx context.Context, cfg Config, stateDir string, log *slog.Logger) (*Store, error) {
	if log == nil {
		log = slog.Default()
	}
	s := &Store{cfg: cfg, log: log, agents: map[string]bool{}}
	if cfg.Embedded() {
		em, err := openEmbedded(ctx, cfg, stateDir, log)
		if err != nil {
			return nil, err
		}
		s.em = em
		s.conn, err = grpc.NewClient("passthrough:///roster",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return em.lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			em.Close()
			return nil, err
		}

		return s, nil
	}

	keys := map[string]string{}
	for tenant, ref := range cfg.Keys {
		v, err := resolveRef(ref)
		if err != nil {
			return nil, fmt.Errorf("auth.roster.keys.%s: %w", tenant, err)
		}
		keys[tenant] = v
	}
	s.keys = keys
	var creds credentials.TransportCredentials
	switch {
	case cfg.Insecure:
		creds = insecure.NewCredentials()
	case cfg.CaFile != "":
		pem, err := os.ReadFile(cfg.CaFile)
		if err != nil {
			return nil, fmt.Errorf("auth.roster.ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("auth.roster.ca_file: %s holds no certificate", cfg.CaFile)
		}
		creds = credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
	default:
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})
	}
	conn, err := grpc.NewClient(cfg.Addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("auth.roster.addr: %w", err)
	}
	s.conn = conn
	log.Info("identity: roster", "addr", cfg.Addr, "tenants", len(keys))

	return s, nil
}

// resolveRef reads a key reference: `env:NAME`, `file:PATH`, or the
// value itself.
func resolveRef(ref string) (string, error) {
	switch {
	case strings.HasPrefix(ref, "env:"):
		v := os.Getenv(ref[4:])
		if v == "" {
			return "", fmt.Errorf("%s is not set", ref[4:])
		}

		return strings.TrimSpace(v), nil
	case strings.HasPrefix(ref, "file:"):
		b, err := os.ReadFile(ref[5:])
		if err != nil {
			return "", err
		}

		return strings.TrimSpace(string(b)), nil
	}

	return strings.TrimSpace(ref), nil
}

// Embedded says whether roster runs in this process.
func (s *Store) Embedded() bool { return s.em != nil }

// Run serves the embedded roster until ctx ends; on an external one it
// waits for ctx.
func (s *Store) Run(ctx context.Context) error {
	if s.em == nil {
		<-ctx.Done()
		return nil
	}

	return s.em.run(ctx)
}

// Close ends the connection and the embedded roster.
func (s *Store) Close() error {
	var err error
	if s.conn != nil {
		err = s.conn.Close()
	}
	if s.em != nil {
		if e := s.em.Close(); err == nil {
			err = e
		}
	}

	return err
}

// as is a context that calls roster as the `shale` holder of a tenant.
func (s *Store) as(ctx context.Context, tenant string) (context.Context, error) {
	if s.em != nil {
		if err := s.ensureAgent(ctx, tenant); err != nil {
			return nil, err
		}

		return auth.PlainProvider("@" + tenant + "/" + Agent).Provide(ctx), nil
	}
	key, ok := s.keys[tenant]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoTenant, tenant)
	}

	return auth.BearerProvider(key).Provide(ctx), nil
}

// ensureAgent writes the `shale` holder, its role and the binding into a
// tenant of the embedded roster, once.
func (s *Store) ensureAgent(ctx context.Context, tenant string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.agents[tenant] {
		return nil
	}
	if err := s.em.agent(ctx, tenant); err != nil {
		return err
	}
	s.agents[tenant] = true

	return nil
}

// Tenants is what this deployment serves: on an external roster the
// tenants it holds keys for, on the embedded one every tenant.
func (s *Store) Tenants(ctx context.Context) ([]string, error) {
	if s.em != nil {
		return s.em.tenants(ctx)
	}
	vs := make([]string, 0, len(s.keys))
	for t := range s.keys {
		vs = append(vs, t)
	}

	return vs, nil
}

// Verify checks a person's password with roster and answers who they are.
func (s *Store) Verify(ctx context.Context, tenant, alias, password string) (Person, error) {
	as, err := s.as(ctx, tenant)
	if err != nil {
		return Person{}, err
	}
	res, err := rstr.NewVouchServiceClient(s.conn).Verify(as, rstr.VouchVerifyRequest_builder{
		Who:    rstr.VouchWho_builder{Tenant: tenant, Alias: alias}.Build(),
		Kind:   "password",
		Secret: []byte(password),
	}.Build())
	if err != nil {
		return Person{}, fmt.Errorf("roster: %w", err)
	}
	if !res.GetOk() {
		if t := res.GetLockedUntil(); t != nil {
			return Person{}, Locked{Until: t.AsTime()}
		}

		return Person{}, ErrRefused
	}
	id, err := pdid.From(res.GetHolder())
	if err != nil {
		return Person{}, err
	}

	return s.person(as, rstr.HolderRef_builder{Id: id.Bytes()}.Build())
}

// Lookup is a person by name, with nothing checked: for a caller some
// other credential already vouched for.
func (s *Store) Lookup(ctx context.Context, tenant, alias string) (Person, error) {
	as, err := s.as(ctx, tenant)
	if err != nil {
		return Person{}, err
	}

	return s.person(as, rstr.HolderRef_builder{
		Slug: rstr.HolderRefBySlug_builder{Alias: z.Ptr(alias), Tenant: rstr.TenantRef_builder{Alias: z.Ptr(tenant)}.Build()}.Build(),
	}.Build())
}

func (s *Store) person(as context.Context, ref *rstr.HolderRef) (Person, error) {
	v, err := rstr.NewHolderServiceClient(s.conn).Get(as, rstr.HolderGetRequest_builder{
		Ref:    ref,
		Select: rstr.HolderSelect_builder{All: z.Ptr(true), Tenant: rstr.TenantSelect_builder{All: z.Ptr(true)}.Build()}.Build(),
	}.Build())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return Person{}, ErrNoPerson
		}

		return Person{}, fmt.Errorf("roster: %w", err)
	}
	if v.GetDateDisabled() != nil {
		return Person{}, fmt.Errorf("%w: disabled", ErrRefused)
	}

	return personOf(v)
}

func personOf(v *rstr.Holder) (Person, error) {
	id, err := pdid.From(v.GetId())
	if err != nil {
		return Person{}, err
	}
	tenant, err := pdid.From(v.GetTenant().GetId())
	if err != nil {
		return Person{}, err
	}

	return Person{
		Id: id, Tenant: tenant, Alias: v.GetAlias(), Name: v.GetName(),
		TenantAlias: v.GetTenant().GetAlias(), TenantName: v.GetTenant().GetName(),
	}, nil
}

// Tenant is a tenant this deployment serves, by alias.
func (s *Store) Tenant(ctx context.Context, alias string) (id pdid.Id, name string, err error) {
	as, err := s.as(ctx, alias)
	if err != nil {
		return pdid.Nil, "", err
	}
	v, err := rstr.NewTenantServiceClient(s.conn).Get(as, rstr.TenantGetRequest_builder{
		Ref: rstr.TenantRef_builder{Alias: z.Ptr(alias)}.Build(),
	}.Build())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return pdid.Nil, "", fmt.Errorf("%w: %s", ErrNoTenant, alias)
		}

		return pdid.Nil, "", fmt.Errorf("roster: %w", err)
	}
	id, err = pdid.From(v.GetId())

	return id, v.GetName(), err
}

// AddPerson makes a person in a tenant, at roster, with no way to sign in
// yet: IssuePassword gives them one.
func (s *Store) AddPerson(ctx context.Context, tenant, alias, name string) (Person, error) {
	as, err := s.as(ctx, tenant)
	if err != nil {
		return Person{}, err
	}
	v, err := rstr.NewHolderServiceClient(s.conn).Add(as, rstr.HolderAddRequest_builder{
		Tenant: rstr.TenantRef_builder{Alias: z.Ptr(tenant)}.Build(),
		Alias:  alias,
		Name:   name,
	}.Build())
	if err != nil {
		return Person{}, fmt.Errorf("roster: %w", err)
	}
	p, err := personOf(v)
	if err != nil {
		return Person{}, err
	}
	if p.TenantAlias == "" {
		p.TenantAlias = tenant
	}

	return p, nil
}

// Person names by identifiers what Shale's rows are made from; the rest
// of a person's facts stay roster's.
