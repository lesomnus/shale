package identity

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/roster/rstr"
)

// testBasics stands for core.BasicMethods, which this package cannot
// import: what matters here is that the role names exactly the list it is
// given.
var testBasics = []string{
	"/shale.SetService/Get", "/shale.SetService/List", "/shale.SetService/Live", "/shale.SiteService/List",
}

// basicsFixture is Basics over an outside roster's double, its log kept,
// on a clock of the test's, remembering what it made in one place for
// every process, as Shale's database is.
type basicsFixture struct {
	f      *fakeRoster
	s      *Store
	b      *Basics
	made   *memMade
	log    *bytes.Buffer
	now    time.Time
	tenant pdid.Id
}

// memMade is BasicsMade in memory: what Shale's row of a tenant says.
type memMade struct {
	mu   sync.Mutex
	m    map[pdid.Id]bool
	sets int
}

func (m *memMade) Made(_ context.Context, tenant pdid.Id) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.m[tenant], nil
}

func (m *memMade) SetMade(_ context.Context, tenant pdid.Id, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.m == nil {
		m.m = map[pdid.Id]bool{}
	}
	m.m[tenant] = true
	m.sets++

	return nil
}

func (m *memMade) of(tenant pdid.Id) bool {
	v, _ := m.Made(context.Background(), tenant)
	return v
}

func newBasicsFixture(t *testing.T) *basicsFixture {
	t.Helper()
	x := &basicsFixture{f: newFakeRoster(t, "acme"), made: &memMade{}, log: &bytes.Buffer{}, now: time.Unix(1_700_000_000, 0)}
	x.s = x.f.open(t)
	x.b = x.basics(x.s)
	x.tenant = pdid.Id(x.f.tenant.GetId())

	return x
}

// basics is another process's Basics on the same roster.
func (x *basicsFixture) basics(s *Store) *Basics {
	b := NewBasics(s, testBasics, x.made, slog.New(slog.NewTextHandler(&lockedBuffer{b: x.log}, nil)))
	b.Now = func() time.Time { return x.now }

	return b
}

type lockedBuffer struct {
	mu sync.Mutex
	b  *bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.b.Write(p)
}

// unquoted is a text log as it was written, before slog quoted it.
func unquoted(log string) string { return strings.ReplaceAll(log, `\"`, `"`) }

func (x *basicsFixture) role() *rstr.Role   { return x.f.app.Role(x.f.tenant, BasicsRole) }
func (x *basicsFixture) group() *rstr.Group { return x.f.app.Group(x.f.tenant, BasicsGroup) }
func (x *basicsFixture) calls() int32       { return x.f.calls.Load() }

// Absent and never made, the role, the group and the binding are made,
// each Shale's and saying so, and Shale remembers it made them; kept
// again, nothing is written; the role is bound to the group with no site,
// and whoever is in the group reaches its methods. Equal to the list, the
// role is no line.
func TestBasicsAreMade(t *testing.T) {
	x := newBasicsFixture(t)
	ctx := context.Background()
	require.False(t, x.made.of(x.tenant))
	require.NoError(t, x.b.Keep(ctx))
	require.True(t, x.made.of(x.tenant), "remembered")

	r, g := x.role(), x.group()
	require.NotNil(t, r)
	require.NotNil(t, g)
	require.ElementsMatch(t, testBasics, r.GetMethods())
	require.Equal(t, Agent, r.GetApp().GetAlias(), "a role of Shale's")
	require.Equal(t, Agent, g.GetApp().GetAlias(), "a group of Shale's")
	require.Contains(t, r.GetDesc(), "Made by Shale")
	require.Contains(t, r.GetDesc(), "does not write its methods again")
	require.Contains(t, g.GetDesc(), "Made by Shale")
	require.Contains(t, g.GetDesc(), "reads to granted")
	require.Equal(t, 1, x.f.app.Bindings(r.GetId(), g.GetId()))
	require.Contains(t, x.log.String(), "is bound to")
	require.NotContains(t, x.log.String(), "differs")

	// Kept again by a second process: read, and nothing written.
	require.NoError(t, x.basics(x.f.open(t)).Keep(ctx))
	require.Equal(t, int32(1), x.f.app.Calls(rstr.RoleService_Add_FullMethodName))
	require.Equal(t, int32(1), x.f.app.Calls(rstr.GroupService_Add_FullMethodName))
	require.Equal(t, int32(1), x.f.app.Calls(rstr.BindingService_Add_FullMethodName))
	require.Zero(t, x.f.app.Calls(rstr.RoleService_Widen_FullMethodName))
	require.Zero(t, x.f.app.Calls(rstr.RoleService_Narrow_FullMethodName))
	require.Equal(t, 1, x.f.app.Bindings(r.GetId(), g.GetId()))
}

// A role there with other methods -- an operator's widening and narrowing,
// or another build's list -- is not written: its methods are as they were,
// and one line at start names the difference both ways, with what roster's
// operator runs to bring it to the list.
func TestBasicsLeaveTheRoleAsItIs(t *testing.T) {
	x := newBasicsFixture(t)
	there := []string{"/shale.SetService/Get", "/shale.ProducerService/List", "/shale.SiteService/*"}
	x.f.app.PutRole(x.f.tenant, BasicsRole, true, there...)
	require.NoError(t, x.b.Keep(context.Background()))
	require.ElementsMatch(t, there, x.role().GetMethods(), "not written")
	require.Zero(t, x.f.app.Calls(rstr.RoleService_Narrow_FullMethodName))
	require.Zero(t, x.f.app.Calls(rstr.RoleService_Widen_FullMethodName))
	require.Zero(t, x.f.app.Calls(rstr.RoleService_Add_FullMethodName))

	out := unquoted(x.log.String())
	require.Equal(t, 1, strings.Count(out, "differs from this build's list"), out)
	require.Contains(t, out, `roster role narrow @acme/shale-basics '{"methods":["/shale.ProducerService/List","/shale.SiteService/*"]}'`)
	require.Contains(t, out, `roster role widen @acme/shale-basics '{"methods":["/shale.SetService/List","/shale.SetService/Live","/shale.SiteService/List"]}'`)
	require.Contains(t, out, "not_in_the_list=/shale.ProducerService/List,/shale.SiteService/*")
	require.Contains(t, out, "not_in_the_role=/shale.SetService/List,/shale.SetService/Live,/shale.SiteService/List")

	// Said once by a process: a person first seen after start asks nothing
	// of the role.
	x.b.Seen(context.Background(), x.tenant, pdid.Id(x.f.addHolder("alice")))
	require.Equal(t, 1, strings.Count(x.log.String(), "differs from this build's list"))
}

// What Shale made once and an operator then removed at roster is not made
// again: the binding (the basics taken away from the group, people still
// put in it), the role, the group -- each by another process, at its
// start, as a restart is; and nobody is put in a group that is gone.
func TestBasicsRemovedAreNotMadeAgain(t *testing.T) {
	ctx := context.Background()
	x := newBasicsFixture(t)
	require.NoError(t, x.b.Keep(ctx))
	r, g := x.role(), x.group()
	adds := func() []int32 {
		return []int32{
			x.f.app.Calls(rstr.RoleService_Add_FullMethodName), x.f.app.Calls(rstr.GroupService_Add_FullMethodName),
			x.f.app.Calls(rstr.BindingService_Add_FullMethodName),
		}
	}
	made := adds()
	require.Equal(t, []int32{1, 1, 1}, made)

	// The binding.
	x.f.app.EraseBinding(r.GetId(), g.GetId())
	x.log.Reset()
	next := x.basics(x.f.open(t))
	require.NoError(t, next.Keep(ctx))
	require.Equal(t, made, adds(), "nothing made again")
	require.Zero(t, x.f.app.Bindings(r.GetId(), g.GetId()))
	require.Contains(t, x.log.String(), "the binding of shale-basics to shale-people was removed at roster")
	require.NotContains(t, x.log.String(), "is bound to")
	alice := x.f.addHolder("alice")
	next.Seen(ctx, x.tenant, pdid.Id(alice))
	require.Equal(t, [][]byte{alice}, x.f.app.Members(g.GetId()), "still put in the group")

	// The role.
	x.f.app.EraseRole(r.GetId())
	x.log.Reset()
	require.NoError(t, x.basics(x.f.open(t)).Keep(ctx))
	require.Equal(t, made, adds())
	require.Nil(t, x.role())
	require.Contains(t, x.log.String(), "the role shale-basics was removed at roster")

	// The group: nobody put in it after, and it is said once.
	x.f.app.EraseGroup(g.GetId())
	x.log.Reset()
	require.NoError(t, x.basics(x.f.open(t)).Keep(ctx))
	require.Equal(t, made, adds())
	require.Nil(t, x.group())
	require.Contains(t, x.log.String(), "the group shale-people was removed at roster")

	// Removed while a process runs: the next person's place is refused
	// NotFound, said once, and nobody is asked for after it.
	y := newBasicsFixture(t)
	require.NoError(t, y.b.Keep(ctx))
	y.f.app.EraseGroup(y.group().GetId())
	y.log.Reset()
	y.b.Seen(ctx, y.tenant, pdid.Id(y.f.addHolder("carol")))
	y.now = y.now.Add(BasicsTtl + time.Second)
	calls := y.f.app.Calls(rstr.GroupMembershipService_Add_FullMethodName)
	y.b.Seen(ctx, y.tenant, pdid.Id(y.f.addHolder("dave")))
	require.Equal(t, calls, y.f.app.Calls(rstr.GroupMembershipService_Add_FullMethodName), "nobody put in a group that is gone")
	require.Zero(t, y.f.app.Calls(rstr.GroupService_Add_FullMethodName)-1, "not made again")
	require.Equal(t, 1, strings.Count(y.log.String(), "was removed at roster"), y.log.String())
	require.NotContains(t, y.log.String(), "could not put")
}

// A tenant where the three are there and Shale's row says nothing -- made
// by a process that ended before it remembered -- is remembered as made
// now, and nothing is written at roster.
func TestBasicsThereAreRemembered(t *testing.T) {
	ctx := context.Background()
	x := newBasicsFixture(t)
	require.NoError(t, x.b.Keep(ctx))
	x.made.m = nil
	require.NoError(t, x.basics(x.f.open(t)).Keep(ctx))
	require.True(t, x.made.of(x.tenant))
	require.Equal(t, int32(1), x.f.app.Calls(rstr.RoleService_Add_FullMethodName))
	require.Equal(t, int32(1), x.f.app.Calls(rstr.BindingService_Add_FullMethodName))
}

// Two processes keeping at once make one role, one group and one binding,
// and neither fails.
func TestBasicsTwiceAtOnce(t *testing.T) {
	for range 10 {
		x := newBasicsFixture(t)
		a, b := x.basics(x.f.open(t)), x.basics(x.f.open(t))
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i, v := range []*Basics{a, b} {
			wg.Go(func() { errs[i] = v.Keep(context.Background()) })
		}
		wg.Wait()
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		require.NotContains(t, x.log.String(), "level=WARN", "a second process is no error")
		r, g := x.role(), x.group()
		require.NotNil(t, r)
		require.NotNil(t, g)
		require.Equal(t, 1, x.f.app.Bindings(r.GetId(), g.GetId()))
	}

	// A binding there already is roster's AlreadyExists, and no error.
	x := newBasicsFixture(t)
	require.NoError(t, x.b.Keep(context.Background()))
	r, g := x.role(), x.group()
	_, err := rstr.NewBindingServiceClient(x.s.conn).Add(x.as(t), rstr.BindingAddRequest_builder{
		Role: rstr.RoleRef_builder{Id: r.GetId()}.Build(), Group: rstr.GroupRef_builder{Id: g.GetId()}.Build(),
	}.Build())
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	require.Equal(t, 1, x.f.app.Bindings(r.GetId(), g.GetId()))
}

func (x *basicsFixture) as(t *testing.T) context.Context {
	t.Helper()
	as, err := x.s.as(context.Background(), x.f.tenant.GetAlias())
	require.NoError(t, err)

	return as
}

// A roster that refuses Shale its own role and group -- installed without
// `--serves` -- is one line that says what is missing and what a token
// offers until then, not said again within the remembering; nobody is put
// anywhere, nothing is asked for a person, and nothing breaks.
func TestBasicsRefused(t *testing.T) {
	x := newBasicsFixture(t)
	x.f.refuse(rstr.RoleService_Get_FullMethodName, true)
	ctx := context.Background()
	require.NoError(t, x.b.Keep(ctx))
	require.Equal(t, 1, strings.Count(x.log.String(), "\n"), x.log.String())
	require.Contains(t, x.log.String(), "--serves")
	require.Contains(t, x.log.String(), "a token offers only what roles an operator binds by hand")
	require.Nil(t, x.role())
	require.Nil(t, x.group())

	alice := pdid.Id(x.f.addHolder("alice"))
	calls := x.calls()
	x.b.Seen(ctx, x.tenant, alice)
	x.b.Seen(ctx, x.tenant, pdid.Id(x.f.addHolder("bob")))
	require.Equal(t, calls, x.calls(), "nothing is asked within the remembering")
	require.Zero(t, x.f.app.Calls(rstr.GroupMembershipService_Add_FullMethodName))
	require.Equal(t, 1, strings.Count(x.log.String(), "\n"))

	// Allowed, and past the remembering: kept, and the person put in.
	x.f.refuse(rstr.RoleService_Get_FullMethodName, false)
	x.now = x.now.Add(BasicsTtl + time.Second)
	x.b.Seen(ctx, x.tenant, alice)
	require.NotNil(t, x.group())
	require.Len(t, x.f.app.Members(x.group().GetId()), 1)
}

// A role or a group of the name that is somebody else's is left as it is,
// said, and nothing is written.
func TestBasicsLeaveWhatIsNotShales(t *testing.T) {
	x := newBasicsFixture(t)
	x.f.app.PutGroup(x.f.tenant, BasicsGroup, false)
	require.NoError(t, x.b.Keep(context.Background()))
	require.Contains(t, x.log.String(), "is not Shale's")
	require.Zero(t, x.f.app.Calls(rstr.BindingService_Add_FullMethodName))
	require.NotEqual(t, Agent, x.group().GetApp().GetAlias())
}

// A person is put in the group when first seen, then holds what the role
// names across the tenant; seen again within the remembering, roster is
// asked nothing; already a member is the ordinary answer and no error; and
// somebody taken out at roster is put back when next seen after it.
func TestBasicsPutPeopleIn(t *testing.T) {
	x := newBasicsFixture(t)
	ctx := context.Background()
	alice := x.f.addHolder("alice")

	// The tenant is kept when it is first served, here by Alice.
	x.b.Seen(ctx, x.tenant, pdid.Id(alice))
	g := x.group()
	require.NotNil(t, g)
	require.Equal(t, [][]byte{alice}, x.f.app.Members(g.GetId()))
	held, err := x.s.Reaches(ctx, x.tenant, pdid.Id(alice))
	require.NoError(t, err)
	require.ElementsMatch(t, testBasics, held, "what a key of hers may be given")

	calls := x.calls()
	x.b.Seen(ctx, x.tenant, pdid.Id(alice))
	require.Equal(t, calls, x.calls(), "within the remembering, roster is asked nothing")

	// Already a member, for a process that did not put them in.
	bob := x.f.addHolder("bob")
	x.f.app.Place(bob, g.GetId())
	other := x.basics(x.f.open(t))
	x.log.Reset()
	other.Seen(ctx, x.tenant, pdid.Id(bob))
	require.NotContains(t, x.log.String(), "could not put")
	require.Len(t, x.f.app.Members(g.GetId()), 2)
	calls = x.calls()
	other.Seen(ctx, x.tenant, pdid.Id(bob))
	require.Equal(t, calls, x.calls(), "remembered as any other")

	// Taken out at roster: put back when next seen after the remembering.
	x.f.app.Take(alice, g.GetId())
	x.b.Seen(ctx, x.tenant, pdid.Id(alice))
	require.Len(t, x.f.app.Members(g.GetId()), 1, "not within it")
	x.now = x.now.Add(BasicsTtl + time.Second)
	x.b.Seen(ctx, x.tenant, pdid.Id(alice))
	require.Len(t, x.f.app.Members(g.GetId()), 2, "put back")

}

// A roster that is down does not hold up the call that brought a person:
// Seen answers within its bound, the failure is said once and remembered,
// and the person is put in when roster answers again after it.
func TestBasicsRosterDown(t *testing.T) {
	x := newBasicsFixture(t)
	ctx := context.Background()
	require.NoError(t, x.b.Keep(ctx))
	alice := x.f.addHolder("alice")
	x.f.down.Store(true)
	x.log.Reset()

	began := time.Now()
	x.b.Seen(ctx, x.tenant, pdid.Id(alice))
	require.Less(t, time.Since(began), BasicsTimeout+time.Second)
	calls := x.calls()
	x.b.Seen(ctx, x.tenant, pdid.Id(alice))
	require.Equal(t, calls, x.calls(), "not asked again within 30 s")
	require.Equal(t, 1, strings.Count(x.log.String(), "\n"), x.log.String())

	x.f.down.Store(false)
	x.now = x.now.Add(basicsRetry + time.Second)
	x.b.Seen(ctx, x.tenant, pdid.Id(alice))
	require.Len(t, x.f.app.Members(x.group().GetId()), 1)

	// A roster that does not answer at all is waited for no longer than
	// the bound.
	slow := x.basics(x.f.open(t))
	slow.Timeout = 50 * time.Millisecond
	x.f.down.Store(true)
	began = time.Now()
	slow.Seen(ctx, x.tenant, pdid.Id(x.f.addHolder("bob")))
	require.Less(t, time.Since(began), time.Second)
}

// Where reads are granted, what an earlier `everybody` left at roster is
// said once and left; where nothing was left, nothing is said.
func TestBasicsLeft(t *testing.T) {
	x := newBasicsFixture(t)
	ctx := context.Background()
	require.NoError(t, x.b.Left(ctx))
	require.Empty(t, x.log.String())

	require.NoError(t, x.b.Keep(ctx))
	x.log.Reset()
	writes := x.f.app.Calls(rstr.BindingService_Add_FullMethodName)
	require.NoError(t, x.basics(x.f.open(t)).Left(ctx))
	require.Equal(t, 1, strings.Count(x.log.String(), "\n"))
	require.Contains(t, x.log.String(), "reads is granted")
	require.Equal(t, writes, x.f.app.Calls(rstr.BindingService_Add_FullMethodName))
	require.NotNil(t, x.role(), "left")
}

// The roster inside Shale is roster's own code: the same writes as Shale's
// holder there make the role, the group and the binding, put a person in,
// and roster then answers that they hold the basics; a second add of the
// same person is roster's AlreadyExists, which is no error; two processes
// at once leave one binding; and a role widened by somebody is brought
// back to the list.
func TestBasicsEmbedded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := Open(ctx, Config{}, t.TempDir(), nil)
	require.NoError(t, err)
	defer s.Close()
	go s.Run(ctx)
	admin, _, err := s.Seed(ctx, "acme", "admin")
	require.NoError(t, err)
	carol, err := s.AddPerson(ctx, "acme", "carol", "Carol")
	require.NoError(t, err)

	log := &bytes.Buffer{}
	made := &memMade{}
	newBasics := func() *Basics {
		return NewBasics(s, testBasics, made, slog.New(slog.NewTextHandler(&lockedBuffer{b: log}, nil)))
	}
	a, b := newBasics(), newBasics()
	var wg sync.WaitGroup
	for _, v := range []*Basics{a, b} {
		wg.Go(func() { require.NoError(t, v.Keep(ctx)) })
	}
	wg.Wait()
	require.NotContains(t, log.String(), "level=WARN", log.String())

	held, err := s.Reaches(ctx, carol.Tenant, carol.Id)
	require.NoError(t, err)
	require.Empty(t, held, "nobody is in the group before they are seen")
	a.Seen(ctx, carol.Tenant, carol.Id)
	held, err = s.Reaches(ctx, carol.Tenant, carol.Id)
	require.NoError(t, err)
	require.ElementsMatch(t, testBasics, held)

	// roster's answer to a second add of the same pair.
	as, err := s.as(ctx, "acme")
	require.NoError(t, err)
	group, err := rstr.NewGroupServiceClient(s.conn).Get(as, rstr.GroupGetRequest_builder{
		Ref: rstr.GroupRef_builder{Slug: rstr.GroupRefBySlug_builder{Alias: z.Ptr(BasicsGroup), Tenant: rstr.TenantRef_builder{Alias: z.Ptr("acme")}.Build()}.Build()}.Build(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, "Made by Shale", group.GetDesc()[:len("Made by Shale")])
	_, err = rstr.NewGroupMembershipServiceClient(s.conn).Add(as, rstr.GroupMembershipAddRequest_builder{
		Holder: rstr.HolderRef_builder{Id: carol.Id.Bytes()}.Build(), Group: rstr.GroupRef_builder{Id: group.GetId()}.Build(),
	}.Build())
	require.Equal(t, codes.AlreadyExists, status.Code(err), "%v", err)
	log.Reset()
	newBasics().Seen(ctx, carol.Tenant, carol.Id)
	require.NotContains(t, log.String(), "could not put", "already a member is no error")
	newBasics().Seen(ctx, admin.Tenant, admin.Id)
	require.NotContains(t, log.String(), "could not put")

	// One binding, though two processes kept at once.
	role, err := rstr.NewRoleServiceClient(s.conn).Get(as, rstr.RoleGetRequest_builder{
		Ref: rstr.RoleRef_builder{Slug: rstr.RoleRefBySlug_builder{Alias: z.Ptr(BasicsRole), Tenant: rstr.TenantRef_builder{Alias: z.Ptr("acme")}.Build()}.Build()}.Build(),
	}.Build())
	require.NoError(t, err)
	bs, err := rstr.NewBindingServiceClient(s.conn).List(as, rstr.BindingListRequest_builder{
		Filters: []*rstr.BindingFilter{rstr.BindingFilter_builder{Role: rstr.RoleRef_builder{Id: role.GetId()}.Build()}.Build()},
	}.Build())
	require.NoError(t, err)
	require.Len(t, bs.GetItems(), 1)

	// Widened by somebody, left so at the next start, and said.
	_, err = rstr.NewRoleServiceClient(s.conn).Widen(as, rstr.RoleWidenRequest_builder{
		Ref: rstr.RoleRef_builder{Id: role.GetId()}.Build(), Methods: []string{"/shale.ProducerService/List"},
	}.Build())
	require.NoError(t, err)
	log.Reset()
	require.NoError(t, newBasics().Keep(ctx))
	role, err = rstr.NewRoleServiceClient(s.conn).Get(as, rstr.RoleGetRequest_builder{Ref: rstr.RoleRef_builder{Id: role.GetId()}.Build()}.Build())
	require.NoError(t, err)
	require.ElementsMatch(t, append(slices.Clone(testBasics), "/shale.ProducerService/List"), role.GetMethods())
	require.Contains(t, unquoted(log.String()), `roster role narrow @acme/shale-basics '{"methods":["/shale.ProducerService/List"]}'`)

	// roster's own answer for what an operator erased is what it answers
	// for what was never made: the role, the group and the binding each
	// erased are absent to Shale and could be added again. Shale's row
	// says it made them, so none is.
	_, err = rstr.NewBindingServiceClient(s.conn).Erase(as, rstr.BindingRef_builder{Id: bs.GetItems()[0].GetId()}.Build())
	require.NoError(t, err)
	_, err = rstr.NewRoleServiceClient(s.conn).Erase(as, rstr.RoleRef_builder{Id: role.GetId()}.Build())
	require.NoError(t, err)
	_, err = rstr.NewGroupServiceClient(s.conn).Erase(as, rstr.GroupRef_builder{Id: group.GetId()}.Build())
	require.NoError(t, err)
	log.Reset()
	require.NoError(t, newBasics().Keep(ctx))
	require.Contains(t, log.String(), "the role shale-basics, the group shale-people was removed at roster", log.String())
	_, err = rstr.NewRoleServiceClient(s.conn).Get(as, rstr.RoleGetRequest_builder{
		Ref: rstr.RoleRef_builder{Slug: rstr.RoleRefBySlug_builder{Alias: z.Ptr(BasicsRole), Tenant: rstr.TenantRef_builder{Alias: z.Ptr("acme")}.Build()}.Build()}.Build(),
	}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "not made again")
	_, err = rstr.NewGroupServiceClient(s.conn).Get(as, rstr.GroupGetRequest_builder{
		Ref: rstr.GroupRef_builder{Slug: rstr.GroupRefBySlug_builder{Alias: z.Ptr(BasicsGroup), Tenant: rstr.TenantRef_builder{Alias: z.Ptr("acme")}.Build()}.Build()}.Build(),
	}.Build())
	require.Equal(t, codes.NotFound, status.Code(err), "not made again")

	// Never made, the same absence is made: roster takes the alias again.
	made.m = nil
	require.NoError(t, newBasics().Keep(ctx))
	held, err = s.Reaches(ctx, carol.Tenant, carol.Id)
	require.NoError(t, err)
	require.Empty(t, held, "nobody is in the new group before they are seen")
	newBasics().Seen(ctx, carol.Tenant, carol.Id)
	held, err = s.Reaches(ctx, carol.Tenant, carol.Id)
	require.NoError(t, err)
	require.ElementsMatch(t, testBasics, held)
}
