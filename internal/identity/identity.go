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
// in each tenant it serves, holding the role that lets it check a password,
// read a person, and ask what roster grants them. On an external roster the
// key the operator hands over says how (roster's `docs/apps.md`): a
// deployment key is answered as the holder each tenant nominated for it, a
// tenant key as the holder it hangs on. On the embedded one Shale writes the
// holder and its role itself and names it on an in-process listener nothing
// else can reach.
package identity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
	"uuid"

	"github.com/lesomnus/z"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/config"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/roster/rstr"
	"github.com/lesomnus/roster/server/front"
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
	// Key is the one key Shale acts with on an external roster: `env:NAME`,
	// `file:PATH`, or the key itself. Its prefix says what it is, the way
	// roster reads it (roster's `docs/apps.md`, *One client for all three*):
	//
	//   - `rk_`, a deployment key a roster operator minted on roster's
	//     control plane. The tenants it serves are the ones that
	//     **nominated** it -- `NominationService.List`, asked as the key,
	//     answers its own and nobody else's -- and every call names its
	//     tenant beside the key with `roster-at`, which roster answers as
	//     the holder that tenant nominated. So a tenant is served by
	//     `roster app install` at roster, and not by anything here.
	//   - `rt_`, a tenant key: one tenant, the key's own.
	Key string `yaml:"key"`
	// Keys are tenant keys (`rt_`) by the tenant's alias, one per tenant
	// it serves: a deployment holding a key each of several tenants minted
	// for it, which is roster's shape C held several times. A tenant with
	// no key here is one this deployment does not serve, whatever roster
	// holds. Not together with Key.
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

// AgentMethods is what that holder may call on the embedded roster:
// checking a password, reading a person and the tenant, making a person,
// and asking what roster grants somebody (`auth.operators`, §33.1).
//
// An external roster's holder needs less, since people are made and given
// passwords there: `/roster.HolderService/Get`,
// `/roster.HolderService/Reaches` and `/roster.TenantService/Get` --
// and `/roster.MeService/Get` for an `rt_`, which is how Shale learns the
// key's own tenant.
var AgentMethods = []string{
	"/roster.VouchService/Verify",
	"/roster.HolderService/Get",
	"/roster.HolderService/Add",
	"/roster.HolderService/Reaches",
	"/roster.TenantService/Get",
	"/roster.MeService/Get",
}

// The prefixes a key is read by, the way roster reads them.
const (
	prefixDeploymentKey = "rk_"
	prefixTenantKey     = "rt_"
)

// ServedTtl is how long the tenants an `rk_` serves are believed before
// roster is asked again. A key cannot watch its nominations, so a tenant
// that nominates it is found at the next asking -- or at once when one of
// its people arrives, since a tenant that is not known is asked about
// again, at most once per [servedRetry].
const ServedTtl = time.Minute

// servedRetry is how soon roster is asked again about the tenants an
// `rk_` serves, after it was asked for any reason.
const servedRetry = 5 * time.Second

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
	// key is the one key, on an external roster that was given one.
	key string
	// keys are the tenant keys, by tenant alias, on an external roster
	// that was given several.
	keys map[string]string

	// The embedded roster, nil for an external one.
	em *embedded

	mu     sync.Mutex
	agents map[string]bool

	// now is the clock, for the tests.
	now func() time.Time

	// What roster said about tenants, kept: for an `rk_` the tenants that
	// nominated it and when that was asked, for an `rt_` the key's own,
	// and for any tenant met the alias it goes by.
	tmu       sync.Mutex
	served    map[string]pdid.Id
	servedAt  time.Time
	asked     time.Time
	servedErr error
	own       string
	aliases   map[pdid.Id]string
}

// Open connects to roster, or builds and migrates the embedded one; Run
// serves it. `stateDir` is where the embedded database goes when Db names
// none.
func Open(ctx context.Context, cfg Config, stateDir string, log *slog.Logger) (*Store, error) {
	if log == nil {
		log = slog.Default()
	}
	s := &Store{cfg: cfg, log: log, agents: map[string]bool{}, aliases: map[pdid.Id]string{}, now: time.Now}
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

	if cfg.Key != "" && len(cfg.Keys) > 0 {
		return nil, errors.New("auth.roster: key or keys, not both -- one deployment key, or a tenant key per tenant")
	}
	if cfg.Key != "" {
		v, err := resolveRef(cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("auth.roster.key: %w", err)
		}
		if !strings.HasPrefix(v, prefixDeploymentKey) && !strings.HasPrefix(v, prefixTenantKey) {
			return nil, fmt.Errorf("auth.roster.key: neither %s nor %s, which is how roster says what a key is", prefixDeploymentKey, prefixTenantKey)
		}
		s.key = v
	}
	keys := map[string]string{}
	for tenant, ref := range cfg.Keys {
		v, err := resolveRef(ref)
		if err != nil {
			return nil, fmt.Errorf("auth.roster.keys.%s: %w", tenant, err)
		}
		if strings.HasPrefix(v, prefixDeploymentKey) {
			// Answered as a tenant's nominated holder only beside
			// `roster-at`, and one key serves every tenant that nominated
			// it: one key is what says so.
			return nil, fmt.Errorf("auth.roster.keys.%s: a deployment key (%s) is auth.roster.key, which serves every tenant that nominated it", tenant, prefixDeploymentKey)
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
	switch {
	case strings.HasPrefix(s.key, prefixDeploymentKey):
		log.Info("identity: roster", "addr", cfg.Addr, "key", "deployment key; the tenants that nominated it")
	case s.key != "":
		log.Info("identity: roster", "addr", cfg.Addr, "key", "tenant key; its own tenant")
	default:
		log.Info("identity: roster", "addr", cfg.Addr, "tenants", len(keys))
	}

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

// as is a context that calls roster as Shale's holder in a tenant: the
// `shale` holder the embedded roster has, the holder a tenant nominated
// for an `rk_`, or the holder a tenant key hangs on.
func (s *Store) as(ctx context.Context, tenant string) (context.Context, error) {
	if s.em != nil {
		if err := s.ensureAgent(ctx, tenant); err != nil {
			return nil, err
		}

		return auth.PlainProvider("@" + tenant + "/" + Agent).Provide(ctx), nil
	}
	switch {
	case strings.HasPrefix(s.key, prefixDeploymentKey):
		ok, err := s.serves(ctx, tenant)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("%w: %s did not nominate this deployment's key", ErrNoTenant, tenant)
		}

		return s.at(ctx, front.TenantMark+tenant), nil
	case s.key != "":
		own, err := s.ownTenant(ctx)
		if err != nil {
			return nil, err
		}
		if own != tenant {
			return nil, fmt.Errorf("%w: %s, and this deployment's tenant key is @%s's", ErrNoTenant, tenant, own)
		}

		return auth.BearerProvider(s.key).Provide(ctx), nil
	}
	key, ok := s.keys[tenant]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoTenant, tenant)
	}

	return auth.BearerProvider(key).Provide(ctx), nil
}

// at is a context whose calls carry the deployment key and say which
// tenant they are about: `roster-at` (roster's `server/front.HeaderAt`),
// which roster answers as the holder that tenant nominated for the key.
// Written `@` and the tenant, the way a caller with no name of its own to
// give names one.
func (s *Store) at(ctx context.Context, at string) context.Context {
	return metadata.AppendToOutgoingContext(auth.BearerProvider(s.key).Provide(ctx), front.HeaderAt, at)
}

// serves says whether a tenant nominated this deployment's `rk_`, asking
// roster again when what is known is [ServedTtl] old, or when the tenant
// is not known and roster was not asked a moment ago.
//
// An answer that is out of date widens nothing: roster finds the
// nomination again on every call that names the tenant, and refuses one
// that has ended.
func (s *Store) serves(ctx context.Context, tenant string) (bool, error) {
	s.tmu.Lock()
	defer s.tmu.Unlock()
	now := s.now()
	_, ok := s.served[tenant]
	known := s.served != nil
	switch {
	case known && ok && now.Sub(s.servedAt) < ServedTtl:
		return true, nil
	case now.Sub(s.asked) < servedRetry:
		// Asked a moment ago, whatever it answered: that stands, so a
		// roster that is down is not asked once per call.
		if !known {
			return false, s.servedErr
		}

		return ok, nil
	}
	if err := s.nominations(ctx, now); err != nil {
		s.servedErr = err
		if !known {
			return false, err
		}
		s.log.WarnContext(ctx, "identity: roster did not say which tenants nominated this key; the last answer stands", "err", err.Error())

		return ok, nil
	}
	_, ok = s.served[tenant]

	return ok, nil
}

// nominations asks roster which tenants nominated this deployment's key,
// and what each is called. The tenant lock is held.
//
// `NominationService.List` asked as the key, naming no tenant, answers the
// key's own nominations and nobody else's (roster's
// `server/core/nomination.go`); it is the one method the key holds as
// itself. A nomination names its tenant by identifier, and the alias is
// asked in that tenant, as the holder it nominated.
func (s *Store) nominations(ctx context.Context, now time.Time) error {
	s.asked = now
	as := auth.BearerProvider(s.key).Provide(ctx)
	var ids []pdid.Id
	after := ""
	for {
		res, err := rstr.NewNominationServiceClient(s.conn).List(as, rstr.NominationListRequest_builder{Size: 100, After: after}.Build())
		if err != nil {
			return fmt.Errorf("roster: the tenants that nominated this key: %w", err)
		}
		for _, n := range res.GetItems() {
			id, err := pdid.From(n.GetTenant().GetId())
			if err != nil {
				return fmt.Errorf("roster: a nomination's tenant: %w", err)
			}
			ids = append(ids, id)
		}
		if after = res.GetNext(); after == "" || len(res.GetItems()) == 0 {
			break
		}
	}

	served := make(map[string]pdid.Id, len(ids))
	for _, id := range ids {
		alias, ok := s.aliases[id]
		if !ok {
			v, err := rstr.NewTenantServiceClient(s.conn).Get(s.at(ctx, front.TenantMark+id.String()),
				rstr.TenantGetRequest_builder{Ref: rstr.TenantRef_builder{Id: id.Bytes()}.Build()}.Build())
			if err != nil {
				return fmt.Errorf("roster: tenant %s: %w", id, err)
			}
			alias = v.GetAlias()
			s.aliases[id] = alias
		}
		served[alias] = id
	}
	s.served, s.servedAt = served, now

	return nil
}

// ownTenant is the tenant an `rt_` is a key of, asked once: roster's
// answer to who the key is (`MeService.Get`), and that tenant's alias.
func (s *Store) ownTenant(ctx context.Context) (string, error) {
	s.tmu.Lock()
	defer s.tmu.Unlock()
	if s.own != "" {
		return s.own, nil
	}
	as := auth.BearerProvider(s.key).Provide(ctx)
	me, err := rstr.NewMeServiceClient(s.conn).Get(as, rstr.MeGetRequest_builder{}.Build())
	if err != nil {
		return "", fmt.Errorf("roster: whose this key is: %w", err)
	}
	id, err := pdid.From(me.GetTenant())
	if err != nil {
		return "", fmt.Errorf("roster: whose this key is: %w", err)
	}
	t, err := rstr.NewTenantServiceClient(s.conn).Get(as, rstr.TenantGetRequest_builder{Ref: rstr.TenantRef_builder{Id: id.Bytes()}.Build()}.Build())
	if err != nil {
		return "", fmt.Errorf("roster: tenant %s: %w", id, err)
	}
	s.own = t.GetAlias()
	s.aliases[id] = s.own

	return s.own, nil
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
// tenants that nominated its deployment key, its tenant key's own, or the
// tenants it holds keys for; on the embedded one every tenant.
func (s *Store) Tenants(ctx context.Context) ([]string, error) {
	if s.em != nil {
		return s.em.tenants(ctx)
	}
	switch {
	case strings.HasPrefix(s.key, prefixDeploymentKey):
		s.tmu.Lock()
		defer s.tmu.Unlock()
		if now := s.now(); now.Sub(s.servedAt) >= ServedTtl && now.Sub(s.asked) >= servedRetry {
			if err := s.nominations(ctx, now); err != nil {
				s.servedErr = err
				if s.served == nil {
					return nil, err
				}
				// What was known a minute ago is still the best answer,
				// and a later call asks again.
				s.log.WarnContext(ctx, "identity: roster did not say which tenants nominated this key; the last answer stands", "err", err.Error())
			}
		} else if s.served == nil {
			return nil, s.servedErr
		}

		return slices.Sorted(maps.Keys(s.served)), nil
	case s.key != "":
		own, err := s.ownTenant(ctx)
		if err != nil {
			return nil, err
		}

		return []string{own}, nil
	}
	vs := make([]string, 0, len(s.keys))
	for t := range s.keys {
		vs = append(vs, t)
	}

	return vs, nil
}

// alias is the alias of a tenant this deployment serves, by identifier.
func (s *Store) alias(ctx context.Context, tenant pdid.Id) (string, error) {
	s.tmu.Lock()
	v, ok := s.aliases[tenant]
	s.tmu.Unlock()
	if ok {
		return v, nil
	}
	ts, err := s.Tenants(ctx)
	if err != nil {
		return "", err
	}
	for _, t := range ts {
		id, _, err := s.Tenant(ctx, t)
		if err != nil {
			return "", err
		}
		if id == tenant {
			return t, nil
		}
	}

	return "", fmt.Errorf("%w: %s", ErrNoTenant, tenant)
}

// Reaches is what roster grants a person of a tenant this deployment
// serves, across the whole of it: `HolderService.Reaches`' `everywhere`
// (§33.1). Not its `methods`, which is the gate's union -- roster's own
// wall narrows that for a grant bound at one of roster's sites or held in
// a team, and Shale has no such narrowing to apply, so reading it would
// hand that grant everywhere. A person roster does not have holds nothing.
func (s *Store) Reaches(ctx context.Context, tenant, holder pdid.Id) ([]string, error) {
	alias, err := s.alias(ctx, tenant)
	if err != nil {
		return nil, err
	}
	as, err := s.as(ctx, alias)
	if err != nil {
		return nil, err
	}
	v, err := rstr.NewHolderServiceClient(s.conn).Reaches(as, rstr.HolderReachesRequest_builder{
		Ref: rstr.HolderRef_builder{Id: holder.Bytes()}.Build(),
	}.Build())
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}

		return nil, fmt.Errorf("roster: %w", err)
	}

	return v.GetEverywhere(), nil
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

// ById is the person an issuer's `sub` names (§33.1): roster's `sub` is a
// `Holder.id`, so it is looked up as one, with the key of each tenant this
// deployment serves until one of them has it. Somebody in no tenant served
// here is nobody, whatever the issuer vouched for: ErrNoPerson. A tenant
// key that does not see them answers NotFound through the wall, which is
// the same answer.
func (s *Store) ById(ctx context.Context, sub string) (Person, error) {
	id, err := uuid.Parse(sub)
	if err != nil {
		return Person{}, fmt.Errorf("%w: %q is not roster's identifier", ErrNoPerson, sub)
	}
	tenants, err := s.Tenants(ctx)
	if err != nil {
		return Person{}, err
	}
	slices.Sort(tenants)
	for _, t := range tenants {
		as, err := s.as(ctx, t)
		if err != nil {
			if errors.Is(err, ErrNoTenant) {
				continue
			}

			return Person{}, err
		}
		p, err := s.person(as, rstr.HolderRef_builder{Id: id[:]}.Build())
		if errors.Is(err, ErrNoPerson) {
			continue
		}

		return p, err
	}

	return Person{}, ErrNoPerson
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
	if err == nil {
		s.tmu.Lock()
		s.aliases[id] = alias
		s.tmu.Unlock()
	}

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
