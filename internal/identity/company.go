package identity

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/roster/rstr"
)

// CompanyConfig is `auth.company` (§33.1): how long who is of the company
// is believed.
type CompanyConfig struct {
	// Ttl is how long an answer about a person, and a tenant's mail
	// domains, are kept before roster is asked again; ten minutes by
	// default. Longer than a grant is kept (`auth.operators.ttl`) because
	// each reading of a person's addresses is written to roster's access
	// record, the address being personal there: asked every 30 s, the
	// record would be mostly Shale.
	Ttl time.Duration `yaml:"ttl"`
}

// CompanyTtl is how long an answer is kept when nothing says.
const CompanyTtl = 10 * time.Minute

// companyRetry is how long a failure to ask is remembered: the answer in
// the meantime is no, without asking again, so a roster that is down or
// refuses is not asked once per call and the log says so once per
// remembered failure.
const companyRetry = 5 * time.Second

// CompanyMethods are what Shale asks roster to tell who is of the company:
// what an operator of roster adds to what Shale may call in a tenant
// (`roster app install --allow`).
var CompanyMethods = []string{
	rstr.EmailService_List_FullMethodName,
	rstr.MailDomainService_List_FullMethodName,
}

// Company says who is of the company, whom a site said COMPANY shows its
// cameras (§33.1): somebody with a **proved** address at a mail domain
// their tenant names at roster.
//
// Proved is an address of theirs whose `date_verified` roster stamped --
// from a directory's word or a link they confirmed; a request cannot assert
// it. A mail domain the tenant names is a `MailDomain` row of the tenant's,
// and the address's part after its last `@`, lower-cased, is its name
// exactly: no subdomain, no suffix. A mail domain is the tenant's own
// statement about its own people, which roster does not prove -- and that
// is enough here, since it only ever sorts people **inside** that tenant:
// a tenant that names a domain it does not own makes people of its own the
// company's, and nobody of anybody else's.
//
// Shale asks roster as itself in the tenant, the person's addresses
// (`EmailService.List` filtered to them, the address and `date_verified`
// alone) and the tenant's mail domains (`MailDomainService.List`), and
// keeps the answer and never the address: each is compared and dropped,
// in no log line, no error, nothing kept. An answer is kept for Ttl, the
// tenant's domains as long; a failure is remembered for 5 s, and is no.
//
// Where roster runs inside Shale everybody of the tenant is of the
// company: a deployment with roster inside it holds no addresses and no
// mail domains, and has no outsiders to tell apart, so COMPANY and
// EVERYBODY show the same people. roster is not asked.
type Company struct {
	s   *Store
	ttl time.Duration
	log *slog.Logger
	// Now is the clock.
	Now func() time.Time

	mu      sync.Mutex
	people  map[grantKey]companyEntry
	domains map[pdid.Id]domainsEntry
}

type companyEntry struct {
	at  time.Time
	is  bool
	err error
}

type domainsEntry struct {
	at    time.Time
	names map[string]bool
	err   error
}

// NewCompany is who is of the company, over a store.
func NewCompany(s *Store, cfg CompanyConfig, log *slog.Logger) *Company {
	if cfg.Ttl <= 0 {
		cfg.Ttl = CompanyTtl
	}
	if log == nil {
		log = slog.Default()
	}

	return &Company{s: s, ttl: cfg.Ttl, log: log, Now: time.Now, people: map[grantKey]companyEntry{}, domains: map[pdid.Id]domainsEntry{}}
}

// Of says whether holder of tenant is of the company. An answer roster
// could not give is an error, and false.
func (c *Company) Of(ctx context.Context, tenant, holder pdid.Id) (bool, error) {
	if c.s.Embedded() {
		return true, nil
	}

	k := grantKey{tenant: tenant, holder: holder}
	now := c.Now()
	c.mu.Lock()
	e, ok := c.people[k]
	c.mu.Unlock()
	if ok {
		if e.err == nil && now.Sub(e.at) < c.ttl {
			return e.is, nil
		}
		if e.err != nil && now.Sub(e.at) < companyRetry {
			return false, e.err
		}
	}

	is := false
	names, err := c.domainsOf(ctx, tenant, now)
	if err == nil && len(names) > 0 {
		// A tenant that names no mail domain has nobody of the company,
		// and nobody's addresses are read.
		is, err = c.s.provedAt(ctx, tenant, holder, names)
		if err != nil {
			c.warn(ctx, tenant, err)
		}
	}
	if err == nil || ctx.Err() == nil {
		// A call that ended while roster was asked keeps nothing: its
		// failure is its own and not roster's.
		c.mu.Lock()
		c.people[k] = companyEntry{at: now, is: is, err: err}
		c.mu.Unlock()
	}

	return is && err == nil, err
}

// domainsOf is the names of the mail domains tenant names, kept for Ttl.
func (c *Company) domainsOf(ctx context.Context, tenant pdid.Id, now time.Time) (map[string]bool, error) {
	c.mu.Lock()
	e, ok := c.domains[tenant]
	c.mu.Unlock()
	if ok {
		if e.err == nil && now.Sub(e.at) < c.ttl {
			return e.names, nil
		}
		if e.err != nil && now.Sub(e.at) < companyRetry {
			return nil, e.err
		}
	}

	names, err := c.s.mailDomains(ctx, tenant)
	if err != nil {
		c.warn(ctx, tenant, err)
	}
	if err == nil || ctx.Err() == nil {
		c.mu.Lock()
		c.domains[tenant] = domainsEntry{at: now, names: names, err: err}
		c.mu.Unlock()
	}

	return names, err
}

// warn says why nobody of a tenant is of the company for now, once per
// failure remembered. The error is roster's and Shale's words, and no
// address: none is put into one.
func (c *Company) warn(ctx context.Context, tenant pdid.Id, err error) {
	if ctx.Err() != nil {
		return
	}
	if status.Code(err) == codes.PermissionDenied {
		c.log.WarnContext(ctx, "company: roster refuses Shale what tells who is of the company, so nobody of this tenant is until roster's operator adds "+
			strings.Join(CompanyMethods, " and ")+" to what Shale may call there (roster app install --allow …, §33.1)",
			"tenant", tenant.String(), "err", err.Error())
		return
	}
	c.log.WarnContext(ctx, "company: roster did not answer; nobody of this tenant is of the company until it does", "tenant", tenant.String(), "err", err.Error())
}

// mailDomains is the names of the mail domains a tenant of this
// deployment's names at roster, lower-cased (roster writes them so):
// `MailDomainService.List` of the tenant, asked as Shale there, the name
// alone selected.
func (s *Store) mailDomains(ctx context.Context, tenant pdid.Id) (map[string]bool, error) {
	alias, err := s.alias(ctx, tenant)
	if err != nil {
		return nil, err
	}
	as, err := s.as(ctx, alias)
	if err != nil {
		return nil, err
	}
	names := map[string]bool{}
	after := ""
	for {
		res, err := rstr.NewMailDomainServiceClient(s.conn).List(as, rstr.MailDomainListRequest_builder{
			Filters: []*rstr.MailDomainFilter{rstr.MailDomainFilter_builder{Tenant: rstr.TenantRef_builder{Id: tenant.Bytes()}.Build()}.Build()},
			Size:    100,
			After:   after,
			Select:  rstr.MailDomainSelect_builder{Name: z.Ptr(true)}.Build(),
		}.Build())
		if err != nil {
			return nil, fmt.Errorf("roster: the tenant's mail domains: %w", err)
		}
		for _, v := range res.GetItems() {
			if n := strings.ToLower(v.GetName()); n != "" {
				names[n] = true
			}
		}
		if after = res.GetNext(); after == "" || len(res.GetItems()) == 0 {
			break
		}
	}

	return names, nil
}

// provedAt says whether a person of a tenant this deployment serves has a
// proved address at one of domains: `EmailService.List` filtered to them,
// asked as Shale in the tenant, selecting the address and `date_verified`
// and nothing else. Each address is compared and dropped -- it is not
// logged, not put in an error, not kept -- and what is answered is yes or
// no. A person roster does not have has no address.
func (s *Store) provedAt(ctx context.Context, tenant, holder pdid.Id, domains map[string]bool) (bool, error) {
	alias, err := s.alias(ctx, tenant)
	if err != nil {
		return false, err
	}
	as, err := s.as(ctx, alias)
	if err != nil {
		return false, err
	}
	after := ""
	for {
		res, err := rstr.NewEmailServiceClient(s.conn).List(as, rstr.EmailListRequest_builder{
			Filters: []*rstr.EmailFilter{rstr.EmailFilter_builder{Holder: rstr.HolderRef_builder{Id: holder.Bytes()}.Build()}.Build()},
			Size:    100,
			After:   after,
			Select:  rstr.EmailSelect_builder{Address: z.Ptr(true), DateVerified: z.Ptr(true)}.Build(),
		}.Build())
		if err != nil {
			return false, fmt.Errorf("roster: the person's addresses: %w", err)
		}
		for _, v := range res.GetItems() {
			if v.GetDateVerified() != nil && domains[domainOf(v.GetAddress())] {
				return true, nil
			}
		}
		if after = res.GetNext(); after == "" || len(res.GetItems()) == 0 {
			break
		}
	}

	return false, nil
}

// domainOf is an address's part after its last `@`, lower-cased; empty for
// a string with none, which names no domain.
func domainOf(address string) string {
	i := strings.LastIndexByte(address, '@')
	if i < 0 {
		return ""
	}

	return strings.ToLower(address[i+1:])
}
