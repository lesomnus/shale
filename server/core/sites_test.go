package core

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/server/bare"
	"github.com/lesomnus/shale/server/pd"
)

// fakeCompany is of the company whom it names, and fails for whom it is
// told to; asked counts the questions.
type fakeCompany struct {
	of     map[pdid.Id]bool
	failed map[pdid.Id]bool
	asked  *atomic.Int32
}

func (c fakeCompany) Of(_ context.Context, _, holder pdid.Id) (bool, error) {
	c.asked.Add(1)
	if c.failed[holder] {
		return true, errors.New("roster: connection refused")
	}

	return c.of[holder], nil
}

type sitesFixture struct {
	readerFixture
	sites   *Sites
	walled  pd.Sink
	now     time.Time
	company fakeCompany
	ops     *fakeOperators
	tid     pdid.Id
}

// newSitesFixture is a tenant, Sites over it, and a sink behind the wall
// and the site axis it answers, as cmd/serve.go stacks them. With ops, it
// is a deployment where roster says what people may change.
func newSitesFixture(t *testing.T, operators bool) *sitesFixture {
	t.Helper()
	f := &sitesFixture{readerFixture: newReaderFixture(t), now: time.Now(), company: fakeCompany{of: map[pdid.Id]bool{}, failed: map[pdid.Id]bool{}, asked: &atomic.Int32{}}}
	f.tid = mustId(f.tenant.GetId())
	f.sites = NewSites(&Deps{Ent: f.client, Own: f.sink, Company: f.company, Now: func() time.Time { return f.now }})
	if operators {
		f.ops = &fakeOperators{tenant: f.tid}
		f.sites.Operators = f.ops
	}
	walled, err := pd.NewSink(f.client, bare.WithMinter(pd.Minter()), bare.WithScope(bare.Scopes{pd.Wall(), pd.Grouped(f.sites.Of)}))
	require.NoError(t, err)
	f.walled = walled

	return f
}

// person is a holder row of the tenant's; the row says all_sites as given.
func (f *sitesFixture) person(t *testing.T, alias string, all bool) *api.Holder {
	t.Helper()
	h, err := f.sink.Holder().Add(f.ctx, api.HolderAddRequest_builder{
		Id: pdid.New(DomHolder).Bytes(), Tenant: api.TenantRef_builder{Id: f.tenant.GetId()}.Build(), Alias: alias, AllSites: all,
	}.Build())
	require.NoError(t, err)

	return h
}

// as is a call of the person's, their row in the frame as the resolver
// puts it there.
func (f *sitesFixture) as(h *api.Holder) context.Context {
	return frame.Into(context.Background(), frame.New(mustId(h.GetId()), f.tid, frame.Whole()).WithScope(frame.Only(f.tid)).WithRow(h))
}

// kind adds a site of a kind, with a set in it and a source of that set.
func (f *sitesFixture) kind(t *testing.T, alias string, v api.SiteVisibility) *api.Site {
	t.Helper()
	tref := api.TenantRef_builder{Id: f.tenant.GetId()}.Build()
	s, err := f.core.Site().Add(f.ctx, api.SiteAddRequest_builder{Tenant: tref, Alias: alias, Visibility: z.Ptr(v)}.Build())
	require.NoError(t, err)
	set, err := f.sink.Set().Add(f.ctx, api.SetAddRequest_builder{Tenant: tref, Alias: alias, Site: api.SiteRef_builder{Id: s.GetId()}.Build()}.Build())
	require.NoError(t, err)
	_, err = f.core.Source().Add(f.ctx, api.SourceAddRequest_builder{Tenant: tref, Alias: alias, Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)

	return s
}

// loose is a set in no site, with a source.
func (f *sitesFixture) loose(t *testing.T, alias string) {
	t.Helper()
	tref := api.TenantRef_builder{Id: f.tenant.GetId()}.Build()
	set, err := f.sink.Set().Add(f.ctx, api.SetAddRequest_builder{Tenant: tref, Alias: alias}.Build())
	require.NoError(t, err)
	_, err = f.core.Source().Add(f.ctx, api.SourceAddRequest_builder{Tenant: tref, Alias: alias, Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	require.NoError(t, err)
}

func (f *sitesFixture) member(t *testing.T, s *api.Site, h *api.Holder) {
	t.Helper()
	_, err := f.core.SiteMember().Add(f.ctx, api.SiteMemberAddRequest_builder{
		Tenant: api.TenantRef_builder{Id: f.tenant.GetId()}.Build(), Site: api.SiteRef_builder{Id: s.GetId()}.Build(), Holder: api.HolderRef_builder{Id: h.GetId()}.Build(),
	}.Build())
	require.NoError(t, err)
}

// later is a call past the remembered answers.
func (f *sitesFixture) later() { f.now = f.now.Add(sitesTTL) }

// seen is what a call sees: every site if all, else the aliases of the
// sites, and, through the wall and the site axis, the aliases of the sets,
// the sources and the sites it reads.
type seen struct {
	all                  bool
	ids                  []string
	sets, sources, sites []string
}

func (f *sitesFixture) seen(t *testing.T, ctx context.Context) seen {
	t.Helper()
	vs, all, err := f.sites.Of(ctx)
	require.NoError(t, err)
	out := seen{all: all}
	for _, v := range vs {
		s, err := f.sink.Site().Get(f.ctx, api.SiteGetRequest_builder{Ref: api.SiteRef_builder{Id: uuidBytes(v)}.Build()}.Build())
		require.NoError(t, err)
		out.ids = append(out.ids, s.GetAlias())
	}
	sets, err := f.walled.Set().List(ctx, api.SetListRequest_builder{}.Build())
	require.NoError(t, err)
	for _, v := range sets.GetItems() {
		out.sets = append(out.sets, v.GetAlias())
	}
	sources, err := f.walled.Source().List(ctx, api.SourceListRequest_builder{}.Build())
	require.NoError(t, err)
	for _, v := range sources.GetItems() {
		out.sources = append(out.sources, v.GetAlias())
	}
	sites, err := f.walled.Site().List(ctx, api.SiteListRequest_builder{}.Build())
	require.NoError(t, err)
	for _, v := range sites.GetItems() {
		out.sites = append(out.sites, v.GetAlias())
	}
	for _, vs := range [][]string{out.ids, out.sets, out.sources, out.sites} {
		slices.Sort(vs)
	}

	return out
}

func uuidBytes(v uuid.UUID) []byte { return pdid.Id(v).Bytes() }

// only is what a caller sees who sees these sites and no other: their sets,
// sources and the sites themselves, and no row in no site.
func only(aliases ...string) seen {
	slices.Sort(aliases)

	return seen{ids: aliases, sets: aliases, sources: aliases, sites: aliases}
}

// Where roster says what people may change, a person not granted all of
// Shale sees the sites said EVERYBODY, those said COMPANY when they are of
// the company, and those they are a member of, whatever those say; a site
// said nothing is MEMBERS; a row in no site is nobody's; their row's
// all_sites is not read; whoever is granted all of Shale sees all of it
// (§33.1).
func TestSitesOfAPersonWithOperators(t *testing.T) {
	f := newSitesFixture(t, true)
	f.kind(t, "unsaid", api.SiteVisibility_SITE_VISIBILITY_UNSPECIFIED)
	members := f.kind(t, "members", api.SiteVisibility_SITE_VISIBILITY_MEMBERS)
	company := f.kind(t, "company", api.SiteVisibility_SITE_VISIBILITY_COMPANY)
	f.kind(t, "everybody", api.SiteVisibility_SITE_VISIBILITY_EVERYBODY)
	f.loose(t, "loose")

	// A row made before, with all_sites on, as Provision made every row
	// where roster says what people may change.
	bob := f.person(t, "bob", true)
	require.Equal(t, only("everybody"), f.seen(t, f.as(bob)), "nothing by the row, and the EVERYBODY site")

	carol := f.person(t, "carol", false)
	f.company.of[mustId(carol.GetId())] = true
	require.Equal(t, only("company", "everybody"), f.seen(t, f.as(carol)), "of the company")

	// A membership adds its site, whatever its kind.
	dave := f.person(t, "dave", false)
	f.member(t, members, dave)
	f.member(t, company, dave)
	require.Equal(t, only("company", "everybody", "members"), f.seen(t, f.as(dave)))

	// A company that could not be asked is no.
	erin := f.person(t, "erin", false)
	f.company.failed[mustId(erin.GetId())] = true
	require.Equal(t, only("everybody"), f.seen(t, f.as(erin)))

	// Granted all of Shale: every site and every row, the loose ones too.
	admin := f.person(t, "admin", false)
	f.ops.grant(f.tid, mustId(admin.GetId()), "/shale.*/*")
	got := f.seen(t, f.as(admin))
	require.True(t, got.all)
	require.Equal(t, []string{"company", "everybody", "loose", "members", "unsaid"}, got.sets)
	require.Equal(t, []string{"company", "everybody", "loose", "members", "unsaid"}, got.sources)
	require.Equal(t, []string{"company", "everybody", "members", "unsaid"}, got.sites)

	// A roster that does not answer grants nothing, and the kinds still
	// say what the person sees.
	f.ops.down = true
	f.later()
	require.Equal(t, only("everybody"), f.seen(t, f.as(admin)))
}

// Where roster does not say what people may change, a row's all_sites is
// every site, as before; anybody else sees the EVERYBODY sites and their
// own (§33.1).
func TestSitesOfAPersonWithoutOperators(t *testing.T) {
	f := newSitesFixture(t, false)
	members := f.kind(t, "members", api.SiteVisibility_SITE_VISIBILITY_MEMBERS)
	f.kind(t, "company", api.SiteVisibility_SITE_VISIBILITY_COMPANY)
	f.kind(t, "everybody", api.SiteVisibility_SITE_VISIBILITY_EVERYBODY)
	f.loose(t, "loose")

	first := f.person(t, "first", true)
	got := f.seen(t, f.as(first))
	require.True(t, got.all)
	require.Contains(t, got.sets, "loose")

	// With no row in the frame, the row is read.
	noRow := frame.Into(context.Background(), frame.New(mustId(first.GetId()), f.tid, frame.Whole()).WithScope(frame.Only(f.tid)))
	f.later()
	require.True(t, f.seen(t, noRow).all)

	bob := f.person(t, "bob", false)
	require.Equal(t, only("everybody"), f.seen(t, f.as(bob)))
	f.member(t, members, bob)
	f.later()
	require.Equal(t, only("everybody", "members"), f.seen(t, f.as(bob)))
}

// A reader is a host and nobody: it sees the sites it is a member of, or
// the whole tenant, and gains no site by its kind (§33.1).
func TestSitesOfAReaderAreNotAKinds(t *testing.T) {
	f := newSitesFixture(t, true)
	members := f.kind(t, "members", api.SiteVisibility_SITE_VISIBILITY_MEMBERS)
	f.kind(t, "everybody", api.SiteVisibility_SITE_VISIBILITY_EVERYBODY)
	f.kind(t, "company", api.SiteVisibility_SITE_VISIBILITY_COMPANY)
	f.loose(t, "loose")

	narrow := f.join(t)
	f.adopt(t, narrow.GetId(), api.SiteRef_builder{Id: members.GetId()}.Build())
	f.company.of[mustId(narrow.GetId())] = true
	ctx := frame.Into(context.Background(), frame.New(mustId(narrow.GetId()), f.tid, frame.Whole()).WithScope(frame.Only(f.tid)))
	require.Equal(t, only("members"), f.seen(t, ctx))

	_, err := f.sink.Reader().Erase(f.ctx, api.ReaderRef_builder{Id: narrow.GetId()}.Build())
	require.NoError(t, err)
	whole := f.join(t)
	f.adopt(t, whole.GetId())
	ctx = frame.Into(context.Background(), frame.New(mustId(whole.GetId()), f.tid, frame.Whole()).WithScope(frame.Only(f.tid)))
	got := f.seen(t, ctx)
	require.True(t, got.all)
	require.Contains(t, got.sets, "loose")
}

// A site's kind is written at Add and by Patch, and seen within sitesTTL:
// the answer is remembered and not forgotten when a site is written. A
// kind there is no rule for is refused, at Add and at Patch.
func TestSiteKindIsSeenWithinTheRememberingTime(t *testing.T) {
	f := newSitesFixture(t, true)
	lobby := f.kind(t, "lobby", api.SiteVisibility_SITE_VISIBILITY_UNSPECIFIED)
	require.Equal(t, api.SiteVisibility_SITE_VISIBILITY_UNSPECIFIED, lobby.GetVisibility())
	bob := f.person(t, "bob", false)
	require.Equal(t, only(), f.seen(t, f.as(bob)))

	lobby, err := f.core.Site().Patch(f.ctx, api.SitePatchRequest_builder{
		Ref: api.SiteRef_builder{Id: lobby.GetId()}.Build(), Visibility: z.Ptr(api.SiteVisibility_SITE_VISIBILITY_EVERYBODY), DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, api.SiteVisibility_SITE_VISIBILITY_EVERYBODY, lobby.GetVisibility())
	require.Empty(t, f.seen(t, f.as(bob)).ids, "remembered")
	f.later()
	require.Equal(t, only("lobby"), f.seen(t, f.as(bob)))

	tref := api.TenantRef_builder{Id: f.tenant.GetId()}.Build()
	_, err = f.core.Site().Add(f.ctx, api.SiteAddRequest_builder{Tenant: tref, Alias: "odd", Visibility: z.Ptr(api.SiteVisibility(7))}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	_, err = f.core.Site().Patch(f.ctx, api.SitePatchRequest_builder{
		Ref: api.SiteRef_builder{Id: lobby.GetId()}.Build(), Visibility: z.Ptr(api.SiteVisibility(7)), DateUpdatedForce: z.Ptr(true),
	}.Build())
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	got, err := f.sink.Site().Get(f.ctx, api.SiteGetRequest_builder{Ref: api.SiteRef_builder{Id: lobby.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, api.SiteVisibility_SITE_VISIBILITY_EVERYBODY, got.GetVisibility())

	// An erased site gives nobody anything by its kind.
	_, err = f.sink.Site().Erase(f.ctx, api.SiteRef_builder{Id: lobby.GetId()}.Build())
	require.NoError(t, err)
	f.later()
	vs, all, err := f.sites.Of(f.as(bob))
	require.NoError(t, err)
	require.False(t, all)
	require.Empty(t, vs)
}

// Who is of the company is asked only when it can matter: when the
// person's tenant has a site said COMPANY, not erased, that they would not
// see anyway. A tenant with no such site never asks about anybody (§33.1).
func TestSitesAskTheCompanyOnlyWhenItMatters(t *testing.T) {
	f := newSitesFixture(t, true)
	f.kind(t, "everybody", api.SiteVisibility_SITE_VISIBILITY_EVERYBODY)
	members := f.kind(t, "members", api.SiteVisibility_SITE_VISIBILITY_MEMBERS)
	bob := f.person(t, "bob", false)
	f.company.of[mustId(bob.GetId())] = true

	require.Equal(t, only("everybody"), f.seen(t, f.as(bob)))
	require.Zero(t, f.company.asked.Load(), "no COMPANY site, nobody asked about")

	// A COMPANY site he is a member of: he sees it anyway.
	company := f.kind(t, "company", api.SiteVisibility_SITE_VISIBILITY_COMPANY)
	f.member(t, company, bob)
	f.member(t, members, bob)
	f.later()
	require.Equal(t, only("company", "everybody", "members"), f.seen(t, f.as(bob)))
	require.Zero(t, f.company.asked.Load(), "a COMPANY site he sees anyway")

	// One he would not see otherwise: asked, and seen.
	f.kind(t, "office", api.SiteVisibility_SITE_VISIBILITY_COMPANY)
	f.later()
	require.Equal(t, only("company", "everybody", "members", "office"), f.seen(t, f.as(bob)))
	require.Equal(t, int32(1), f.company.asked.Load())

	// Somebody not of the company is asked about, and does not see it.
	carol := f.person(t, "carol", false)
	require.Equal(t, only("everybody"), f.seen(t, f.as(carol)))
	require.Equal(t, int32(2), f.company.asked.Load())
	require.Equal(t, only("everybody"), f.seen(t, f.as(carol)), "remembered")
	require.Equal(t, int32(2), f.company.asked.Load())
}
