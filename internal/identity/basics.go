package identity

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/roster/rstr"
)

// The role and the group Shale keeps at roster in each tenant it serves
// for the basics (§33.1): what everybody who signs in holds where
// `auth.operators.reads` is `everybody`. Both are Shale's own -- of the app
// Shale (`Role.app`, `Group.app`), written as its holder there -- and the
// role is bound to the group with no site, so whoever is in the group holds
// the role's methods at Shale across the tenant, which is what a key of
// theirs can be given.
const (
	// BasicsRole names the basics, and nothing else.
	BasicsRole = "shale-basics"
	// BasicsGroup is everybody Shale has seen signed in.
	BasicsGroup = "shale-people"
)

// What the role and the group say of themselves at roster, so that an
// operator who meets them there knows whose they are.
const (
	basicsRoleDesc = "Made by Shale: the basic API every person signed in to Shale may call (auth.operators.reads: everybody), " +
		"the reads and views of cameras and sites. Shale made it once and does not write its methods again: widen or narrow it here, and Shale says at start where it differs from its own list."
	basicsGroupDesc = "Made by Shale: every person Shale has seen signed in, put here so that they hold " + BasicsRole + ". " +
		"Shale puts back anybody taken out; to take the basics away from everybody, remove this group's binding (Shale does not make it again), or set auth.operators.reads to granted."
)

// BasicsLabel is the label on Shale's own row of a tenant that says Shale
// made the role, the group and the binding there. roster cannot say it: an
// erased row is not read by anybody, its alias is free for an Add again,
// and a binding erased may be made again, so one an operator removed and
// one never made answer alike. While the label is there Shale makes none
// of the three again; taken off, Shale makes what is absent at its next
// start.
const BasicsLabel = "shale.basics"

// BasicsMade is where Shale remembers, per tenant, that it made the
// basics there: the tenant's row in Shale's own database (cmd).
type BasicsMade interface {
	// Made says whether Shale made the basics in the tenant.
	Made(ctx context.Context, tenant pdid.Id) (bool, error)
	// SetMade remembers that it did, making the tenant's row from what
	// roster says of it where there is none yet.
	SetMade(ctx context.Context, tenant pdid.Id, alias string) error
}

// BasicsTtl is how long a person is remembered as put in the group: Shale
// asks roster again about them, and puts them back if they were taken out,
// at their first call after it.
const BasicsTtl = 10 * time.Minute

// basicsRetry is how long a failure is remembered, for a person or for a
// tenant: nothing is asked again of roster for them in the meantime, and
// the log says so once per failure remembered.
const basicsRetry = 30 * time.Second

// BasicsTimeout bounds what one call of a person's waits for roster while
// they are put in the group, keeping the group included: a roster that is
// slow or down delays that call by at most this, once per basicsRetry.
const BasicsTimeout = 2 * time.Second

// basicsKeepTimeout bounds keeping the role and the group of one tenant
// at start.
const basicsKeepTimeout = 30 * time.Second

// Basics writes down at roster what everybody who signs in holds at Shale
// (§33.1), where roster says what people may change (`auth.operators`) and
// reads are everybody's: in each tenant served, a role of Shale's naming
// exactly the methods it is given (core.BasicMethods) and a group of
// Shale's, the role bound to the group with no site; and each person, when
// Shale first sees them, put in the group, so that roster knows they hold
// the basics and a token of theirs can be given them. The gate lets them
// call the same methods with no role; this is what lets a token offer them.
//
// The role, the group and the binding are made once and then left to
// whoever operates the deployment: at start and when a tenant is first
// served each is read, and made if absent, unless Shale's row of the
// tenant says it made them (BasicsLabel), in which case one that is absent
// was removed by an operator and is not made again. The role's methods
// are never written after it is made: where they differ from the build's
// list it is said once, with what an operator runs at roster to bring it
// to the list, and left. A second process doing the same at once is no
// error: an Add that finds the alias taken reads what the other made, and
// roster binds a role to a group once.
//
// A person is put in with `GroupMembershipService/Add` as Shale, and
// remembered for [BasicsTtl]; *already a member* is the ordinary answer.
// None of it is in the way of the call that caused it: bounded by
// [BasicsTimeout], a failure logged once and remembered for 30 s, the call
// going on with whatever the person holds. There is no exclusion: somebody
// taken out of the group at roster is put back when next seen, and
// somebody who should not use Shale is suspended at roster.
//
// A roster that refuses Shale these writes -- an app installed without
// `--serves`, which is what binds it roster's verbs for its own roles and
// groups -- is said in one line per tenant, remembered for BasicsTtl, and
// until then a token offers only the roles an operator binds by hand.
type Basics struct {
	s       *Store
	methods []string
	made    BasicsMade
	log     *slog.Logger
	// Now is the clock.
	Now func() time.Time
	// Timeout is [BasicsTimeout] unless a test says otherwise.
	Timeout time.Duration

	mu      sync.Mutex
	tenants map[pdid.Id]*basicsTenant
	people  map[grantKey]basicsEntry
}

// basicsTenant is a tenant's group, once kept, or why it is not. lock is
// held while it is kept, by one call at a time. kept with no group is a
// group an operator removed: nobody is put anywhere.
type basicsTenant struct {
	lock  chan struct{}
	kept  bool
	group []byte
	at    time.Time
	err   error
}

type basicsEntry struct {
	at  time.Time
	err error
}

// NewBasics writes the basics, methods, at roster over a store, made
// remembering in which tenants it made them. made may be nil only for
// [Basics.Left], which writes nothing.
func NewBasics(s *Store, methods []string, made BasicsMade, log *slog.Logger) *Basics {
	if log == nil {
		log = slog.Default()
	}
	ms := slices.Clone(methods)
	slices.Sort(ms)

	return &Basics{
		s: s, methods: slices.Compact(ms), made: made, log: log, Now: time.Now, Timeout: BasicsTimeout,
		tenants: map[pdid.Id]*basicsTenant{}, people: map[grantKey]basicsEntry{},
	}
}

// ErrBasicsRefused is roster refusing Shale the writes of its own role and
// group: Shale was installed there without `--serves`.
var ErrBasicsRefused = errors.New("roster refuses Shale writing its own roles and groups")

// ErrNotShales is a role or a group of Shale's name at roster that is not
// Shale's: the tenant's own, or another app's. Shale leaves it.
var ErrNotShales = errors.New("not Shale's")

// Keep keeps the role, the group and the binding in every tenant this
// deployment serves, each bounded on its own: what start does. A tenant
// whose keeping fails is said and tried again when somebody of it is next
// seen.
func (b *Basics) Keep(ctx context.Context) error {
	ts, err := b.s.Tenants(ctx)
	if err != nil {
		b.log.WarnContext(ctx, "basics: cannot ask roster which tenants are served, so the basics are kept as each is first served", "err", err.Error())
		return nil
	}
	for _, alias := range ts {
		ctx, cancel := context.WithTimeout(ctx, basicsKeepTimeout)
		id, _, err := b.s.Tenant(ctx, alias)
		if err == nil {
			_, err = b.group(ctx, ctx, id)
		}
		cancel()
		if err != nil && errors.Is(err, ErrNoTenant) {
			continue
		}
	}

	return ctx.Err()
}

// Seen puts a person in the group of the basics, unless they were within
// BasicsTtl or a failure for them was within 30 s; the tenant's group is
// kept first if it is not yet. It answers nothing: whatever roster says,
// the call that brought the person goes on.
func (b *Basics) Seen(ctx context.Context, tenant, holder pdid.Id) {
	k := grantKey{tenant: tenant, holder: holder}
	now := b.Now()
	b.mu.Lock()
	e, ok := b.people[k]
	b.mu.Unlock()
	if ok && ((e.err == nil && now.Sub(e.at) < BasicsTtl) || (e.err != nil && now.Sub(e.at) < basicsRetry)) {
		return
	}

	at, cancel := context.WithTimeout(ctx, b.Timeout)
	defer cancel()
	group, err := b.group(at, ctx, tenant)
	if err != nil || group == nil {
		// Said, and remembered, for the tenant; or no group, since an
		// operator removed it.
		return
	}
	err = b.s.putIn(at, tenant, group, holder)
	if ctx.Err() != nil {
		// The call went away while roster was asked: what it found is its
		// own and not roster's, and nothing is kept.
		return
	}
	if status.Code(err) == codes.NotFound {
		// The group is gone at roster since it was kept: removed by an
		// operator, and not made again.
		b.gone(ctx, tenant)
		return
	}
	b.mu.Lock()
	b.people[k] = basicsEntry{at: now, err: err}
	b.mu.Unlock()
	if err != nil {
		b.log.WarnContext(ctx, "basics: could not put a person in "+BasicsGroup+" at roster; they call what they hold, and are put in when next seen after 30 s",
			"tenant", tenant.String(), "holder", holder.String(), "err", err.Error())
	}
}

// gone is a tenant's group found removed at roster while people were put
// in it: said once, and nobody is put anywhere in it again.
func (b *Basics) gone(ctx context.Context, tenant pdid.Id) {
	b.mu.Lock()
	t, ok := b.tenants[tenant]
	b.mu.Unlock()
	if !ok {
		return
	}
	select {
	case t.lock <- struct{}{}:
	default:
		return
	}
	defer func() { <-t.lock }()
	if t.group == nil {
		return
	}
	t.group = nil
	b.log.WarnContext(ctx, "basics: "+BasicsGroup+" was removed at roster; Shale made it once and does not make it again, and puts nobody in it",
		"tenant", tenant.String())
}

// group is the tenant's group of the basics, kept now if it is not yet and
// no failure to keep it is remembered. ctx bounds the asking; parent is the
// call it is for, which keeps nothing if it went away.
func (b *Basics) group(ctx, parent context.Context, tenant pdid.Id) ([]byte, error) {
	b.mu.Lock()
	t, ok := b.tenants[tenant]
	if !ok {
		t = &basicsTenant{lock: make(chan struct{}, 1)}
		b.tenants[tenant] = t
	}
	b.mu.Unlock()

	select {
	case t.lock <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-t.lock }()
	if t.kept {
		return t.group, nil
	}
	now := b.Now()
	if t.err != nil && now.Sub(t.at) < retryOf(t.err) {
		return nil, t.err
	}

	alias, err := b.s.alias(ctx, tenant)
	var k basicsKept
	if err == nil {
		k, err = b.keep(ctx, tenant, alias)
	}
	if err != nil && parent.Err() != nil {
		// Ended while roster was asked: nothing is kept for the tenant.
		return nil, err
	}
	t.kept, t.group, t.at, t.err = err == nil, k.group, now, err
	if err != nil {
		b.warn(ctx, tenant, alias, err)
		return nil, err
	}
	b.said(ctx, alias, k)

	return k.group, nil
}

// keep makes in a tenant what of the role, the group and the binding is
// absent, unless Shale made them there once, and remembers that it did.
func (b *Basics) keep(ctx context.Context, tenant pdid.Id, alias string) (basicsKept, error) {
	if b.made == nil {
		return basicsKept{}, errors.New("basics: nowhere to remember what Shale made")
	}
	made, err := b.made.Made(ctx, tenant)
	if err != nil {
		return basicsKept{}, fmt.Errorf("whether Shale made the basics: %w", err)
	}
	k, err := b.s.keepBasics(ctx, alias, b.methods, made)
	if err != nil {
		return basicsKept{}, err
	}
	if !made {
		if err := b.made.SetMade(ctx, tenant, alias); err != nil {
			return basicsKept{}, fmt.Errorf("remembering that Shale made the basics: %w", err)
		}
	}

	return k, nil
}

// said is what keeping a tenant found, once: what is there, a role that
// differs from the build's list, and what an operator removed.
func (b *Basics) said(ctx context.Context, alias string, k basicsKept) {
	if k.group != nil && k.bound {
		b.log.InfoContext(ctx, "basics: "+BasicsRole+" is bound to "+BasicsGroup+" at roster, and whoever Shale sees signed in is put in the group",
			"tenant", alias, "methods", len(b.methods))
	}
	if len(k.extra) > 0 || len(k.missing) > 0 {
		b.log.WarnContext(ctx, "basics: "+BasicsRole+" at roster differs from this build's list of the basics, and Shale leaves it as it is"+
			basicsToTheList(alias, k.extra, k.missing),
			"tenant", alias, "not_in_the_list", strings.Join(k.extra, ","), "not_in_the_role", strings.Join(k.missing, ","))
	}
	var gone []string
	if k.roleGone {
		gone = append(gone, "the role "+BasicsRole)
	}
	if k.groupGone {
		gone = append(gone, "the group "+BasicsGroup)
	}
	if k.bindingGone {
		gone = append(gone, "the binding of "+BasicsRole+" to "+BasicsGroup)
	}
	if len(gone) > 0 {
		b.log.InfoContext(ctx, "basics: "+strings.Join(gone, ", ")+" was removed at roster; Shale made it once and does not make it again"+
			" (taking the label "+BasicsLabel+" off the tenant's row here makes Shale make what is absent at its next start)",
			"tenant", alias, "people_put_in", k.group != nil)
	}
}

// basicsToTheList is what roster's operator runs to bring the role to the
// list: what it names that the list does not, narrowed; what the list
// names that it does not, widened.
func basicsToTheList(alias string, extra, missing []string) string {
	ref := "@" + alias + "/" + BasicsRole
	var cmds []string
	if len(extra) > 0 {
		cmds = append(cmds, "roster role narrow "+ref+" "+methodsDoc(extra))
	}
	if len(missing) > 0 {
		cmds = append(cmds, "roster role widen "+ref+" "+methodsDoc(missing))
	}

	return "; to bring it to the list, roster's operator runs: " + strings.Join(cmds, " and ")
}

func methodsDoc(ms []string) string {
	return `'{"methods":["` + strings.Join(ms, `","`) + `"]}'`
}

// retryOf is how long a tenant's failure is remembered: a refusal is the
// state of a deployment until its roster operator acts, and is said again
// every BasicsTtl rather than every 30 s.
func retryOf(err error) time.Duration {
	if errors.Is(err, ErrBasicsRefused) || errors.Is(err, ErrNotShales) {
		return BasicsTtl
	}

	return basicsRetry
}

func (b *Basics) warn(ctx context.Context, tenant pdid.Id, alias string, err error) {
	name := alias
	if name == "" {
		name = tenant.String()
	}
	switch {
	case errors.Is(err, ErrBasicsRefused):
		b.log.WarnContext(ctx, "basics: roster refuses Shale writing its own role and group, so nobody is put in "+BasicsGroup+" and a token offers only what roles an operator binds by hand. "+
			"Roster's operator installs Shale with --serves '"+Everything+"' (roster app install), which lets it write its own roles and groups; Shale writes them when it next starts or serves the tenant",
			"tenant", name, "err", err.Error())
	case errors.Is(err, ErrNotShales):
		b.log.WarnContext(ctx, "basics: a role or a group at roster has the name Shale keeps for the basics and is not Shale's, so Shale leaves it and writes nothing there; "+
			"a token offers only what roles an operator binds by hand until it is renamed",
			"tenant", name, "err", err.Error())
	default:
		b.log.WarnContext(ctx, "basics: roster did not answer while Shale kept the basics; tried again within 30 s", "tenant", name, "err", err.Error())
	}
}

// Left says, once at start, where reads are granted, in which tenant the
// role and the group of the basics are still at roster from when reads
// were everybody's: Shale writes neither any more and puts nobody in the
// group, and leaves them, since an operator may have come to rely on them.
// They are roster's to remove.
func (b *Basics) Left(ctx context.Context) error {
	ts, err := b.s.Tenants(ctx)
	if err != nil {
		return nil
	}
	for _, alias := range ts {
		ctx, cancel := context.WithTimeout(ctx, basicsKeepTimeout)
		role, group, err := b.s.basicsThere(ctx, alias)
		cancel()
		if err != nil || (!role && !group) {
			continue
		}
		b.log.WarnContext(ctx, "basics: auth.operators.reads is granted, and Shale's "+BasicsRole+" and "+BasicsGroup+" from when reads were everybody's are still at roster. "+
			"Shale writes neither and puts nobody in the group; whoever is in it holds the basics until roster's operator removes the group's binding, or the two",
			"tenant", alias, "role", role, "group", group)
	}

	return ctx.Err()
}

// basicsThere says whether the role and the group of the basics are at
// roster in a tenant, as Shale's.
func (s *Store) basicsThere(ctx context.Context, alias string) (role, group bool, err error) {
	as, err := s.as(ctx, alias)
	if err != nil {
		return false, false, err
	}
	tref := rstr.TenantRef_builder{Alias: z.Ptr(alias)}.Build()
	_, err = rstr.NewRoleServiceClient(s.conn).Get(as, rstr.RoleGetRequest_builder{
		Ref: rstr.RoleRef_builder{Slug: rstr.RoleRefBySlug_builder{Alias: z.Ptr(BasicsRole), Tenant: tref}.Build()}.Build(),
	}.Build())
	switch {
	case err == nil:
		role = true
	case status.Code(err) != codes.NotFound:
		return false, false, err
	}
	_, err = rstr.NewGroupServiceClient(s.conn).Get(as, rstr.GroupGetRequest_builder{
		Ref: rstr.GroupRef_builder{Slug: rstr.GroupRefBySlug_builder{Alias: z.Ptr(BasicsGroup), Tenant: tref}.Build()}.Build(),
	}.Build())
	switch {
	case err == nil:
		group = true
	case status.Code(err) != codes.NotFound:
		return false, false, err
	}

	return role, group, nil
}

// basicsKept is what keeping a tenant found: the group people are put in,
// nil where it was removed; whether the role is bound to it; the methods
// the role names that the list does not, and the list's that it does not;
// and which of the three an operator removed after Shale made them.
type basicsKept struct {
	group                            []byte
	bound                            bool
	extra, missing                   []string
	roleGone, groupGone, bindingGone bool
}

// keepBasics keeps the role, the group and the binding in a tenant, as
// Shale there: each read, and made if absent unless made says Shale made
// them once. The role's methods are compared with methods, and not
// written.
func (s *Store) keepBasics(ctx context.Context, alias string, methods []string, made bool) (basicsKept, error) {
	var k basicsKept
	as, err := s.as(ctx, alias)
	if err != nil {
		return k, err
	}
	role, err := s.keepBasicsRole(as, alias, methods, made, &k)
	if err != nil {
		return k, refusedOr(err)
	}
	group, err := s.keepBasicsGroup(as, alias, made)
	if err != nil {
		return k, refusedOr(err)
	}
	k.roleGone, k.groupGone, k.group = role == nil, group == nil, group
	if role == nil || group == nil {
		return k, nil
	}
	k.bound, err = s.keepBasicsBinding(as, role, group, made)
	if err != nil {
		return k, refusedOr(err)
	}
	k.bindingGone = !k.bound

	return k, nil
}

// refusedOr is roster's PermissionDenied as ErrBasicsRefused, in its words.
func refusedOr(err error) error {
	if status.Code(err) == codes.PermissionDenied {
		return fmt.Errorf("%w: %s", ErrBasicsRefused, status.Convert(err).Message())
	}

	return err
}

// shaleIn is Shale's holder in a tenant, by name, as the app a row is of.
func shaleIn(alias string) *rstr.HolderRef {
	return rstr.HolderRef_builder{Slug: rstr.HolderRefBySlug_builder{Alias: z.Ptr(Agent), Tenant: rstr.TenantRef_builder{Alias: z.Ptr(alias)}.Build()}.Build()}.Build()
}

// keepBasicsRole is the role, made with methods if absent and never made,
// and answers its identifier, or nil where it is absent and was made once.
// What it names is compared with methods into k, and not written. Shale
// reads only its own roles at roster, so a role of that name that is not
// Shale's is NotFound to it and its name taken to Add: ErrNotShales.
func (s *Store) keepBasicsRole(as context.Context, alias string, methods []string, made bool, k *basicsKept) ([]byte, error) {
	c := rstr.NewRoleServiceClient(s.conn)
	ref := rstr.RoleRef_builder{Slug: rstr.RoleRefBySlug_builder{Alias: z.Ptr(BasicsRole), Tenant: rstr.TenantRef_builder{Alias: z.Ptr(alias)}.Build()}.Build()}.Build()
	get := func() (*rstr.Role, error) {
		return c.Get(as, rstr.RoleGetRequest_builder{Ref: ref, Select: rstr.RoleSelect_builder{All: z.Ptr(true)}.Build()}.Build())
	}
	r, err := get()
	if status.Code(err) == codes.NotFound && made {
		return nil, nil
	}
	if status.Code(err) == codes.NotFound {
		r, err = c.Add(as, rstr.RoleAddRequest_builder{
			Tenant: rstr.TenantRef_builder{Alias: z.Ptr(alias)}.Build(), Alias: BasicsRole, Name: "Shale: the basics", Desc: basicsRoleDesc,
			Methods: methods, App: shaleIn(alias),
		}.Build())
		if status.Code(err) == codes.AlreadyExists {
			// Made a moment ago by another process, or somebody else's.
			r, err = get()
			if status.Code(err) == codes.NotFound {
				return nil, fmt.Errorf("%w: role %s", ErrNotShales, BasicsRole)
			}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("role %s: %w", BasicsRole, err)
	}

	// Compared, and left: the role is its operator's once made.
	k.extra = slices.DeleteFunc(slices.Clone(r.GetMethods()), func(m string) bool { return slices.Contains(methods, m) })
	k.missing = slices.DeleteFunc(slices.Clone(methods), func(m string) bool { return slices.Contains(r.GetMethods(), m) })
	slices.Sort(k.extra)

	return r.GetId(), nil
}

// keepBasicsGroup is the group, made if absent and never made, and
// answers its identifier, or nil where it is absent and was made once.
func (s *Store) keepBasicsGroup(as context.Context, alias string, made bool) ([]byte, error) {
	c := rstr.NewGroupServiceClient(s.conn)
	ref := rstr.GroupRef_builder{Slug: rstr.GroupRefBySlug_builder{Alias: z.Ptr(BasicsGroup), Tenant: rstr.TenantRef_builder{Alias: z.Ptr(alias)}.Build()}.Build()}.Build()
	get := func() (*rstr.Group, error) { return c.Get(as, rstr.GroupGetRequest_builder{Ref: ref}.Build()) }
	g, err := get()
	if status.Code(err) == codes.NotFound && made {
		return nil, nil
	}
	if status.Code(err) == codes.NotFound {
		g, err = c.Add(as, rstr.GroupAddRequest_builder{
			Tenant: rstr.TenantRef_builder{Alias: z.Ptr(alias)}.Build(), Alias: BasicsGroup, Name: "Shale: everybody signed in", Desc: basicsGroupDesc,
			App: shaleIn(alias),
		}.Build())
		if status.Code(err) == codes.AlreadyExists {
			g, err = get()
			if status.Code(err) == codes.NotFound {
				return nil, fmt.Errorf("%w: group %s", ErrNotShales, BasicsGroup)
			}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("group %s: %w", BasicsGroup, err)
	}

	return g.GetId(), nil
}

// keepBasicsBinding binds the role to the group with no site, unless it
// is or was made once, and says whether it is bound. roster binds a role
// to somebody once, looked for and written in one transaction (`Bound`): a
// second process making the same binding at once is answered
// AlreadyExists, which is no error.
func (s *Store) keepBasicsBinding(as context.Context, role, group []byte, made bool) (bool, error) {
	c := rstr.NewBindingServiceClient(s.conn)
	res, err := c.List(as, rstr.BindingListRequest_builder{
		Filters: []*rstr.BindingFilter{rstr.BindingFilter_builder{
			Role: rstr.RoleRef_builder{Id: role}.Build(), Group: rstr.GroupRef_builder{Id: group}.Build(),
		}.Build()},
		Size:   100,
		Select: rstr.BindingSelect_builder{All: z.Ptr(true), Site: rstr.SiteSelect_builder{}.Build()}.Build(),
	}.Build())
	if err != nil {
		return false, fmt.Errorf("binding %s to %s: %w", BasicsRole, BasicsGroup, err)
	}
	// One held in a site is not this one.
	if slices.ContainsFunc(res.GetItems(), func(b *rstr.Binding) bool { return len(b.GetSite().GetId()) == 0 }) {
		return true, nil
	}
	if made {
		return false, nil
	}
	_, err = c.Add(as, rstr.BindingAddRequest_builder{
		Role: rstr.RoleRef_builder{Id: role}.Build(), Group: rstr.GroupRef_builder{Id: group}.Build(),
	}.Build())
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return false, fmt.Errorf("binding %s to %s: %w", BasicsRole, BasicsGroup, err)
	}

	return true, nil
}

// putIn puts a person in a tenant's group, as Shale there: a place that is
// there already is the ordinary answer, AlreadyExists, and no error.
func (s *Store) putIn(ctx context.Context, tenant pdid.Id, group []byte, holder pdid.Id) error {
	as, err := s.asIn(ctx, tenant)
	if err != nil {
		return err
	}
	_, err = rstr.NewGroupMembershipServiceClient(s.conn).Add(as, rstr.GroupMembershipAddRequest_builder{
		Holder: rstr.HolderRef_builder{Id: holder.Bytes()}.Build(),
		Group:  rstr.GroupRef_builder{Id: group}.Build(),
	}.Build())
	if status.Code(err) == codes.AlreadyExists {
		return nil
	}

	return err
}
