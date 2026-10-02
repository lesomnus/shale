package identity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"uuid"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/roster/rstr"
)

// OperatorsConfig is `auth.operators` (§33.1): who the cluster operators
// are, said as a team at roster rather than as a tenant of their own.
//
// Empty is the rule a deployment had before it: the people of
// `control.cluster_tenant` operate the cluster, and every person
// administers their own tenant.
type OperatorsConfig struct {
	// Tenant is the alias of the tenant the team is in, which is a tenant
	// this deployment serves. Empty is the one tenant `auth.roster.keys`
	// names, when it names one.
	Tenant string `yaml:"tenant"`
	// Team is the team: its alias, or its identifier. A team in no site is
	// found by alias among the tenant's teams; one in a site needs Site.
	Team string `yaml:"team"`
	// Site is the alias of the site the team is in, when it is in one.
	Site string `yaml:"site"`
	// Ttl is how long an answer is believed before roster is asked again;
	// 30 s by default. A person taken off the team stops being an operator
	// within it.
	Ttl time.Duration `yaml:"ttl"`
}

// On says whether operators are a team here.
func (c OperatorsConfig) On() bool { return c.Team != "" }

// OperatorsTtl is how long a membership answer is kept when nothing says.
const OperatorsTtl = 30 * time.Second

// operatorsRetry is how long a failure to ask is remembered: the answer
// in the meantime is no, without asking again, so a roster that is down
// is not asked once per call and the log says so once per person.
const operatorsRetry = 5 * time.Second

// ErrNoTeam is a team roster does not have, or one named ambiguously.
var ErrNoTeam = errors.New("no such team")

// Operators answers whether a person is a cluster operator: a member of
// the team `auth.operators` names, by roster's word, asked with the
// tenant's key (§33.1). Answers are kept for Ttl; when roster cannot be
// asked the answer is no, and the log says why.
type Operators struct {
	s   *Store
	cfg OperatorsConfig
	log *slog.Logger
	// Now is the clock.
	Now func() time.Time

	mu     sync.Mutex
	tenant pdid.Id
	team   []byte
	cache  map[pdid.Id]opEntry
}

type opEntry struct {
	at  time.Time
	is  bool
	err error
}

// NewOperators is the team lookup over a store. The tenant and the team
// are resolved on first use, so a roster that is not up yet does not stop
// the control plane from starting.
func NewOperators(s *Store, cfg OperatorsConfig, log *slog.Logger) (*Operators, error) {
	if !cfg.On() {
		return nil, errors.New("auth.operators.team: say which team the operators are")
	}
	if cfg.Tenant == "" {
		if len(s.keys) != 1 {
			return nil, errors.New("auth.operators.tenant: say which tenant the team is in")
		}
		for t := range s.keys {
			cfg.Tenant = t
		}
	}
	if s.em == nil {
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

	return &Operators{s: s, cfg: cfg, log: log, Now: time.Now, cache: map[pdid.Id]opEntry{}}, nil
}

// TenantAlias is the tenant the operators are people of.
func (o *Operators) TenantAlias() string { return o.cfg.Tenant }

// Team says the team as configured, for the log and for `shale init`.
func (o *Operators) Team() string {
	if o.cfg.Site != "" {
		return "@" + o.cfg.Tenant + "/" + o.cfg.Site + "/" + o.cfg.Team
	}

	return "@" + o.cfg.Tenant + "/" + o.cfg.Team
}

// Is says whether `holder` of `tenant` is on the operators' team. A person
// of any other tenant is not.
func (o *Operators) Is(ctx context.Context, tenant, holder pdid.Id) (bool, error) {
	t, team, err := o.resolve(ctx)
	if err != nil {
		o.log.WarnContext(ctx, "operators: cannot find the team at roster; nobody is an operator until it is found", "team", o.Team(), "err", err.Error())
		return false, err
	}
	if tenant != t {
		return false, nil
	}

	now := o.Now()
	o.mu.Lock()
	e, ok := o.cache[holder]
	o.mu.Unlock()
	if ok {
		if e.err == nil && now.Sub(e.at) < o.cfg.Ttl {
			return e.is, nil
		}
		if e.err != nil && now.Sub(e.at) < operatorsRetry {
			return false, e.err
		}
	}

	is, err := o.member(ctx, holder, team)
	if err != nil {
		o.log.WarnContext(ctx, "operators: roster did not answer; this person is not an operator until it does", "holder", holder.String(), "team", o.Team(), "err", err.Error())
	}
	o.mu.Lock()
	o.cache[holder] = opEntry{at: now, is: is, err: err}
	o.mu.Unlock()

	return is, err
}

// Check finds the tenant and the team at roster, which is what a key that
// cannot read teams fails at: for `shale init` to say so early.
func (o *Operators) Check(ctx context.Context) error {
	_, _, err := o.resolve(ctx)

	return err
}

// TeamId is the team's identifier at roster.
func (o *Operators) TeamId(ctx context.Context) (pdid.Id, error) {
	_, team, err := o.resolve(ctx)
	if err != nil {
		return pdid.Nil, err
	}

	return pdid.Id(team), nil
}

// Forget drops what is known about a person, or about everybody.
func (o *Operators) Forget(holder pdid.Id) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if holder.IsZero() {
		o.cache = map[pdid.Id]opEntry{}
		return
	}
	delete(o.cache, holder)
}

func (o *Operators) member(ctx context.Context, holder pdid.Id, team []byte) (bool, error) {
	as, err := o.s.as(ctx, o.cfg.Tenant)
	if err != nil {
		return false, err
	}
	c := rstr.NewTeamMembershipServiceClient(o.s.conn)
	after := ""
	for {
		// By the person alone, and the team compared here: a person is on a
		// handful of teams, and the question does not then depend on how a
		// filter of two fields is read.
		res, err := c.List(as, rstr.TeamMembershipListRequest_builder{
			Filters: []*rstr.TeamMembershipFilter{rstr.TeamMembershipFilter_builder{
				Holder: rstr.HolderRef_builder{Id: holder.Bytes()}.Build(),
			}.Build()},
			Size:  100,
			After: after,
		}.Build())
		if err != nil {
			return false, fmt.Errorf("roster: %w", err)
		}
		for _, m := range res.GetItems() {
			if string(m.GetTeam().GetId()) == string(team) && m.GetDateErased() == nil {
				return true, nil
			}
		}
		if after = res.GetNext(); after == "" || len(res.GetItems()) == 0 {
			return false, nil
		}
	}
}

// resolve finds the tenant and the team at roster, once.
func (o *Operators) resolve(ctx context.Context) (pdid.Id, []byte, error) {
	o.mu.Lock()
	t, team := o.tenant, o.team
	o.mu.Unlock()
	if !t.IsZero() && team != nil {
		return t, team, nil
	}

	t, _, err := o.s.Tenant(ctx, o.cfg.Tenant)
	if err != nil {
		return pdid.Nil, nil, err
	}
	team, err = o.findTeam(ctx)
	if err != nil {
		return pdid.Nil, nil, err
	}
	o.mu.Lock()
	o.tenant, o.team = t, team
	o.mu.Unlock()
	o.log.InfoContext(ctx, "operators: the members of a team at roster", "team", o.Team(), "id", uuid.UUID(team).String())

	return t, team, nil
}

func (o *Operators) findTeam(ctx context.Context) ([]byte, error) {
	if v, err := uuid.Parse(o.cfg.Team); err == nil {
		return v[:], nil
	}
	as, err := o.s.as(ctx, o.cfg.Tenant)
	if err != nil {
		return nil, err
	}
	teams := rstr.NewTeamServiceClient(o.s.conn)
	if o.cfg.Site != "" {
		v, err := teams.Get(as, rstr.TeamGetRequest_builder{
			Ref: rstr.TeamRef_builder{Slug: rstr.TeamRefBySlug_builder{
				Alias: z.Ptr(o.cfg.Team),
				Site: rstr.SiteRef_builder{Slug: rstr.SiteRefBySlug_builder{
					Alias: z.Ptr(o.cfg.Site), Tenant: rstr.TenantRef_builder{Alias: z.Ptr(o.cfg.Tenant)}.Build(),
				}.Build()}.Build(),
			}.Build()}.Build(),
		}.Build())
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return nil, fmt.Errorf("%w: %s", ErrNoTeam, o.Team())
			}

			return nil, fmt.Errorf("roster: %w", err)
		}

		return v.GetId(), nil
	}

	// A team in no site has no slug at roster, so it is found among the
	// tenant's teams by its alias.
	var siteless, sited [][]byte
	after := ""
	for {
		res, err := teams.List(as, rstr.TeamListRequest_builder{
			Filters: []*rstr.TeamFilter{rstr.TeamFilter_builder{Tenant: rstr.TenantRef_builder{Alias: z.Ptr(o.cfg.Tenant)}.Build()}.Build()},
			Size:    100,
			After:   after,
		}.Build())
		if err != nil {
			return nil, fmt.Errorf("roster: %w", err)
		}
		for _, v := range res.GetItems() {
			if !strings.EqualFold(v.GetAlias(), o.cfg.Team) {
				continue
			}
			if len(v.GetSite().GetId()) == 0 {
				siteless = append(siteless, v.GetId())
			} else {
				sited = append(sited, v.GetId())
			}
		}
		if after = res.GetNext(); after == "" || len(res.GetItems()) == 0 {
			break
		}
	}
	switch {
	case len(siteless) == 1:
		return siteless[0], nil
	case len(siteless) == 0 && len(sited) == 1:
		return sited[0], nil
	case len(siteless)+len(sited) == 0:
		return nil, fmt.Errorf("%w: %s", ErrNoTeam, o.Team())
	default:
		return nil, fmt.Errorf("%w: %s names %d teams; say auth.operators.site, or the team's identifier", ErrNoTeam, o.Team(), len(siteless)+len(sited))
	}
}
