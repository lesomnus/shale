package identity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"
)

// OperatorsConfig is `auth.operators` (§33.1): that what a person may
// change here is what roster grants them, and whose people operate the
// cluster.
//
// Empty is the rule a deployment had before it: the people of
// `control.cluster_tenant` operate the cluster, and every person
// administers their own tenant.
type OperatorsConfig struct {
	// Tenant is the alias of the tenant whose people the cluster API
	// serves, which is a tenant this deployment serves: a person of it may
	// call there what roster grants them in it.
	Tenant string `yaml:"tenant"`
	// Ttl is how long an answer about a person is believed before roster
	// is asked again; 30 s by default. A grant taken away at roster stops
	// working within it.
	Ttl time.Duration `yaml:"ttl"`

	// Team and Site named the team operators were, before a team stopped
	// being how this is said. Set, they are refused with what to write
	// instead, rather than ignored into a deployment where nobody is an
	// operator.
	Team string `yaml:"team"`
	Site string `yaml:"site"`
}

// On says whether what people may change here is roster's to say.
func (c OperatorsConfig) On() bool { return c.Tenant != "" || c.Team != "" || c.Site != "" }

// OperatorsTtl is how long an answer is kept when nothing says.
const OperatorsTtl = 30 * time.Second

// operatorsRetry is how long a failure to ask is remembered: the answer
// in the meantime is no, without asking again, so a roster that is down
// is not asked once per call and the log says so once per person.
const operatorsRetry = 5 * time.Second

// Everything is every method Shale serves, as a pattern. Whoever roster
// grants it -- or a pattern covering it, such as `/*.*/*` -- administers:
// sees every site of their tenant, is shown as an operator, and on the
// cluster API, when they are of the operators' tenant, may sign in.
const Everything = "/shale.*/*"

// ErrTeam is `auth.operators.team`, which is refused.
var ErrTeam = errors.New("auth.operators.team: operators are not a team any more")

// Operators answers what a person may change, by roster's grant (§33.1):
// a role at roster naming Shale's methods, bound to them or to a group
// they are in, asked with `HolderService.Reaches` and read from its
// `everywhere`. Answers are kept for Ttl; when roster cannot be asked the
// answer is no, and the log says why.
//
// # Not a team any more
//
// This used to be the members of a team at roster. A team membership is
// organisation and not permission there -- one with no role grants
// nothing -- so roster lets anybody who may write memberships write one,
// and reading it as "may operate the cluster" was a permission roster did
// not know existed and could not guard. A grant is guarded: nobody binds
// a role naming methods they do not hold themselves.
type Operators struct {
	s   *Store
	cfg OperatorsConfig
	log *slog.Logger
	// Now is the clock.
	Now func() time.Time

	mu     sync.Mutex
	tenant pdid.Id
	cache  map[grantKey]grantEntry
}

type grantKey struct{ tenant, holder pdid.Id }

type grantEntry struct {
	at   time.Time
	held []string
	err  error
}

// NewOperators is the grant lookup over a store. The operators' tenant is
// resolved on first use, so a roster that is not up yet does not stop the
// control plane from starting.
func NewOperators(s *Store, cfg OperatorsConfig, log *slog.Logger) (*Operators, error) {
	if cfg.Team != "" || cfg.Site != "" {
		return nil, fmt.Errorf("%w: a membership is not a permission at roster, so anybody who may add one could make an operator. "+
			"What a person may change is what roster grants them -- bind a role naming %s, or the services they look after, "+
			"to them or to a group they are in -- and drop team and site (§33.1)", ErrTeam, Everything)
	}
	if cfg.Tenant == "" {
		return nil, errors.New("auth.operators.tenant: say whose people operate the cluster")
	}
	if s.em == nil && len(s.keys) > 0 {
		if _, ok := s.keys[cfg.Tenant]; !ok {
			return nil, fmt.Errorf("auth.operators.tenant: %s is not a tenant this deployment holds a key for (auth.roster.keys)", cfg.Tenant)
		}
	}
	if cfg.Ttl <= 0 {
		cfg.Ttl = OperatorsTtl
	}
	if log == nil {
		log = slog.Default()
	}

	return &Operators{s: s, cfg: cfg, log: log, Now: time.Now, cache: map[grantKey]grantEntry{}}, nil
}

// TenantAlias is the tenant the cluster's operators are people of.
func (o *Operators) TenantAlias() string { return o.cfg.Tenant }

// May says whether holder of tenant may call method there: whether one
// pattern roster grants them across the tenant covers it.
func (o *Operators) May(ctx context.Context, tenant, holder pdid.Id, method string) (bool, error) {
	held, err := o.held(ctx, tenant, holder)
	if err != nil {
		return false, err
	}

	return covers(held, method), nil
}

// Administers says whether holder of tenant holds all of Shale there.
func (o *Operators) Administers(ctx context.Context, tenant, holder pdid.Id) (bool, error) {
	return o.May(ctx, tenant, holder, Everything)
}

// MayOperate says whether holder may call method on the cluster API: a
// person of the operators' tenant, granted it there. A person of any other
// tenant may not, whatever their own tenant grants them -- the cluster API
// sees every tenant.
func (o *Operators) MayOperate(ctx context.Context, tenant, holder pdid.Id, method string) (bool, error) {
	t, err := o.resolve(ctx)
	if err != nil {
		o.log.WarnContext(ctx, "operators: cannot find the operators' tenant at roster; nobody is an operator until it is found", "tenant", o.cfg.Tenant, "err", err.Error())
		return false, err
	}
	if tenant != t {
		return false, nil
	}

	return o.May(ctx, tenant, holder, method)
}

// Operates says whether holder is a cluster operator: of the operators'
// tenant, and granted all of Shale there.
func (o *Operators) Operates(ctx context.Context, tenant, holder pdid.Id) (bool, error) {
	return o.MayOperate(ctx, tenant, holder, Everything)
}

// Check finds the operators' tenant at roster, which is what a key that
// does not serve it fails at: for `shale init` to say so early.
func (o *Operators) Check(ctx context.Context) error {
	_, err := o.resolve(ctx)

	return err
}

// Forget drops what is known about a person, or about everybody.
func (o *Operators) Forget(holder pdid.Id) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if holder.IsZero() {
		o.cache = map[grantKey]grantEntry{}
		return
	}
	for k := range o.cache {
		if k.holder == holder {
			delete(o.cache, k)
		}
	}
}

// held is what roster grants a person across their tenant, kept for Ttl.
func (o *Operators) held(ctx context.Context, tenant, holder pdid.Id) ([]string, error) {
	k := grantKey{tenant: tenant, holder: holder}
	now := o.Now()
	o.mu.Lock()
	e, ok := o.cache[k]
	o.mu.Unlock()
	if ok {
		if e.err == nil && now.Sub(e.at) < o.cfg.Ttl {
			return e.held, nil
		}
		if e.err != nil && now.Sub(e.at) < operatorsRetry {
			return nil, e.err
		}
	}

	held, err := o.s.Reaches(ctx, tenant, holder)
	if err != nil {
		o.log.WarnContext(ctx, "operators: roster did not answer; this person changes nothing until it does", "holder", holder.String(), "err", err.Error())
	}
	o.mu.Lock()
	o.cache[k] = grantEntry{at: now, held: held, err: err}
	o.mu.Unlock()

	return held, err
}

// resolve finds the operators' tenant at roster, once.
func (o *Operators) resolve(ctx context.Context) (pdid.Id, error) {
	o.mu.Lock()
	t := o.tenant
	o.mu.Unlock()
	if !t.IsZero() {
		return t, nil
	}

	t, _, err := o.s.Tenant(ctx, o.cfg.Tenant)
	if err != nil {
		return pdid.Nil, err
	}
	o.mu.Lock()
	o.tenant = t
	o.mu.Unlock()
	o.log.InfoContext(ctx, "operators: the people roster grants Shale's methods to", "tenant", o.cfg.Tenant)

	return t, nil
}

// covers says whether one held pattern covers want on its own, which is
// `frame.Covers`' rule: a union of narrower patterns does not add up to a
// wider one, so a grant made before a service existed does not reach it.
func covers(held []string, want string) bool {
	return slices.ContainsFunc(held, func(h string) bool { return frame.Covers(h, want) })
}
