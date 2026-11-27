// Package rosterapp is an app's own roles, groups, the bindings of the one
// to the other and the people in the groups, as roster answers its app
// holder for them -- as much of it as Shale writes there (§33.1, the
// basics) -- for the tests' doubles of an external roster. Nothing that
// runs imports it.
//
// It answers as roster's `main` does, in the parts Shale leans on:
//
//   - a caller is the app's holder of the tenant it is answered in, which
//     reads and writes the app's rows and no other (`app-administrator`
//     held for itself): another's role or group of the same name is
//     NotFound to it, and its own Add of that name AlreadyExists;
//   - an alias is the tenant's once among the rows not erased, a second Add
//     is AlreadyExists;
//   - a role is bound to somebody once: a second Add of the same binding is
//     AlreadyExists (roster's `Bound`, in one transaction);
//   - a place in a group is unique among the places not erased, a second
//     Add of the same pair is AlreadyExists, and an erased one counts for
//     nothing;
//   - Widen adds what the role does not hold or cover; Narrow takes out
//     every entry a named method covers.
package rosterapp

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/roster/rstr"
)

// App is the rows, over every tenant the double answers.
type App struct {
	// Alias is the app's holder's alias in every tenant: Shale's is `shale`.
	Alias string
	// Tenant is the tenant a call is answered in, which the double's
	// interceptor put on the context; nil is nobody's.
	Tenant func(ctx context.Context) *rstr.Tenant

	mu       sync.Mutex
	roles    []*rstr.Role
	groups   []*rstr.Group
	bindings []*rstr.Binding
	places   []*place
	seq      int64

	// Calls counts each method as it is asked.
	calls sync.Map
}

type place struct {
	holder, group []byte
	erased        bool
}

// Register serves the four services on g.
func (a *App) Register(g *grpc.Server) {
	rstr.RegisterRoleServiceServer(g, roles{App: a})
	rstr.RegisterGroupServiceServer(g, groups{App: a})
	rstr.RegisterBindingServiceServer(g, bindings{App: a})
	rstr.RegisterGroupMembershipServiceServer(g, places{App: a})
}

// Calls is how many times method was asked.
func (a *App) Calls(method string) int32 {
	v, ok := a.calls.Load(method)
	if !ok {
		return 0
	}

	return v.(*atomic.Int32).Load()
}

func (a *App) count(method string) {
	v, _ := a.calls.LoadOrStore(method, &atomic.Int32{})
	v.(*atomic.Int32).Add(1)
}

// Everywhere is what a holder holds across their tenant through the app's
// groups: the methods of each role bound, with no site, to a group they
// have a place in.
func (a *App) Everywhere(holder []byte) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, p := range a.places {
		if p.erased || string(p.holder) != string(holder) {
			continue
		}
		for _, b := range a.bindings {
			if string(b.GetGroup().GetId()) != string(p.group) || len(b.GetSite().GetId()) > 0 {
				continue
			}
			for _, r := range a.roles {
				if string(r.GetId()) == string(b.GetRole().GetId()) {
					out = append(out, r.GetMethods()...)
				}
			}
		}
	}
	slices.Sort(out)

	return slices.Compact(out)
}

// Role is the tenant's role of that alias, whoever's, or nil.
func (a *App) Role(tenant *rstr.Tenant, alias string) *rstr.Role {
	a.mu.Lock()
	defer a.mu.Unlock()
	if v := a.roleBy(tenant, alias, ""); v != nil {
		return proto.Clone(v).(*rstr.Role)
	}

	return nil
}

// Group is the tenant's group of that alias, whoever's, or nil.
func (a *App) Group(tenant *rstr.Tenant, alias string) *rstr.Group {
	a.mu.Lock()
	defer a.mu.Unlock()
	if v := a.groupBy(tenant, alias, ""); v != nil {
		return proto.Clone(v).(*rstr.Group)
	}

	return nil
}

// Bindings is how many bindings there are of the role to the group.
func (a *App) Bindings(role, group []byte) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, b := range a.bindings {
		if string(b.GetRole().GetId()) == string(role) && string(b.GetGroup().GetId()) == string(group) {
			n++
		}
	}

	return n
}

// Members is the holders with a place in the group.
func (a *App) Members(group []byte) [][]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out [][]byte
	for _, p := range a.places {
		if !p.erased && string(p.group) == string(group) {
			out = append(out, p.holder)
		}
	}

	return out
}

// PutRole writes a role as somebody at the box would: of the app (`own`)
// or of somebody else's, naming methods.
func (a *App) PutRole(tenant *rstr.Tenant, alias string, own bool, methods ...string) *rstr.Role {
	a.mu.Lock()
	defer a.mu.Unlock()
	app := "someone-else"
	if own {
		app = a.Alias
	}
	r := rstr.Role_builder{
		Id: a.id(), Tenant: rstr.Tenant_builder{Id: tenant.GetId()}.Build(), Alias: alias, Methods: methods,
		App: rstr.Holder_builder{Alias: app}.Build(),
	}.Build()
	a.roles = append(a.roles, r)

	return r
}

// PutGroup writes a group as somebody at the box would.
func (a *App) PutGroup(tenant *rstr.Tenant, alias string, own bool) *rstr.Group {
	a.mu.Lock()
	defer a.mu.Unlock()
	app := "someone-else"
	if own {
		app = a.Alias
	}
	g := rstr.Group_builder{Id: a.id(), Tenant: rstr.Tenant_builder{Id: tenant.GetId()}.Build(), Alias: alias, App: rstr.Holder_builder{Alias: app}.Build()}.Build()
	a.groups = append(a.groups, g)

	return g
}

// Take ends a holder's place in a group, as an administrator at roster does.
func (a *App) Take(holder, group []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, p := range a.places {
		if string(p.holder) == string(holder) && string(p.group) == string(group) {
			p.erased = true
		}
	}
}

// Place gives a holder a place in a group, as an administrator at roster
// does.
func (a *App) Place(holder, group []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.places = append(a.places, &place{holder: holder, group: group})
}

// EraseGroup erases a group, as an administrator at roster does.
func (a *App) EraseGroup(group []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.groups = slices.DeleteFunc(a.groups, func(g *rstr.Group) bool { return string(g.GetId()) == string(group) })
}

// EraseRole erases a role, as an administrator at roster does: read by
// nobody after, and its alias free for an Add again.
func (a *App) EraseRole(role []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.roles = slices.DeleteFunc(a.roles, func(r *rstr.Role) bool { return string(r.GetId()) == string(role) })
}

// EraseBinding erases the bindings of a role to a group, as an
// administrator at roster does: one erased may be made again.
func (a *App) EraseBinding(role, group []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.bindings = slices.DeleteFunc(a.bindings, func(b *rstr.Binding) bool {
		return string(b.GetRole().GetId()) == string(role) && string(b.GetGroup().GetId()) == string(group)
	})
}

// id is a new identifier, in the order rows are made. The lock is held.
func (a *App) id() []byte {
	a.seq++

	return pdid.New(1).Bytes()
}

func (a *App) now() *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(0, a.seq))
}

// roleBy is the tenant's role of an alias, of the app `of` or anybody's
// when empty. The lock is held.
func (a *App) roleBy(tenant *rstr.Tenant, alias, of string) *rstr.Role {
	for _, r := range a.roles {
		if string(r.GetTenant().GetId()) == string(tenant.GetId()) && r.GetAlias() == alias && (of == "" || r.GetApp().GetAlias() == of) {
			return r
		}
	}

	return nil
}

func (a *App) groupBy(tenant *rstr.Tenant, alias, of string) *rstr.Group {
	for _, g := range a.groups {
		if string(g.GetTenant().GetId()) == string(tenant.GetId()) && g.GetAlias() == alias && (of == "" || g.GetApp().GetAlias() == of) {
			return g
		}
	}

	return nil
}

// role is the app's own role a ref names in the tenant. The lock is held.
func (a *App) role(tenant *rstr.Tenant, ref *rstr.RoleRef) *rstr.Role {
	for _, r := range a.roles {
		if string(r.GetTenant().GetId()) != string(tenant.GetId()) || r.GetApp().GetAlias() != a.Alias {
			continue
		}
		if string(r.GetId()) == string(ref.GetId()) || (ref.GetSlug() != nil && r.GetAlias() == ref.GetSlug().GetAlias()) {
			return r
		}
	}

	return nil
}

func (a *App) group(tenant *rstr.Tenant, ref *rstr.GroupRef) *rstr.Group {
	for _, g := range a.groups {
		if string(g.GetTenant().GetId()) != string(tenant.GetId()) || g.GetApp().GetAlias() != a.Alias {
			continue
		}
		if string(g.GetId()) == string(ref.GetId()) || (ref.GetSlug() != nil && g.GetAlias() == ref.GetSlug().GetAlias()) {
			return g
		}
	}

	return nil
}

// mine refuses a row said to be another app's, as roster refuses its app
// holder writing one (`mayWriteOfApp`).
func (a *App) mine(ref *rstr.HolderRef) error {
	if ref.GetSlug().GetAlias() != a.Alias {
		return status.Error(codes.PermissionDenied, "app: you hold this for the app, and this is not the app's")
	}

	return nil
}

type roles struct {
	rstr.UnimplementedRoleServiceServer
	*App
}

func (a roles) Get(ctx context.Context, req *rstr.RoleGetRequest) (*rstr.Role, error) {
	a.count(rstr.RoleService_Get_FullMethodName)
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.role(a.Tenant(ctx), req.GetRef())
	if r == nil {
		return nil, status.Error(codes.NotFound, "Role not found")
	}

	return proto.Clone(r).(*rstr.Role), nil
}

func (a roles) Add(ctx context.Context, req *rstr.RoleAddRequest) (*rstr.Role, error) {
	a.count(rstr.RoleService_Add_FullMethodName)
	if err := a.mine(req.GetApp()); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	t := a.Tenant(ctx)
	if a.roleBy(t, req.GetAlias(), "") != nil {
		return nil, status.Error(codes.AlreadyExists, "Role already exists")
	}
	r := rstr.Role_builder{
		Id: a.id(), Tenant: rstr.Tenant_builder{Id: t.GetId()}.Build(), Alias: req.GetAlias(), Name: req.GetName(), Desc: req.GetDesc(),
		Methods: slices.Clone(req.GetMethods()), App: rstr.Holder_builder{Alias: a.Alias}.Build(),
	}.Build()
	r.SetDateCreated(a.now())
	a.roles = append(a.roles, r)

	return proto.Clone(r).(*rstr.Role), nil
}

func (a roles) Widen(ctx context.Context, req *rstr.RoleWidenRequest) (*rstr.Role, error) {
	a.count(rstr.RoleService_Widen_FullMethodName)
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.role(a.Tenant(ctx), req.GetRef())
	if r == nil {
		return nil, status.Error(codes.NotFound, "Role not found")
	}
	ms := r.GetMethods()
	for _, m := range req.GetMethods() {
		if !slices.ContainsFunc(ms, func(h string) bool { return frame.Covers(h, m) }) {
			ms = append(ms, m)
		}
	}
	r.SetMethods(ms)

	return proto.Clone(r).(*rstr.Role), nil
}

func (a roles) Narrow(ctx context.Context, req *rstr.RoleNarrowRequest) (*rstr.Role, error) {
	a.count(rstr.RoleService_Narrow_FullMethodName)
	if len(req.GetMethods()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "methods: what to take out")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.role(a.Tenant(ctx), req.GetRef())
	if r == nil {
		return nil, status.Error(codes.NotFound, "Role not found")
	}
	r.SetMethods(slices.DeleteFunc(r.GetMethods(), func(h string) bool {
		return slices.ContainsFunc(req.GetMethods(), func(m string) bool { return frame.Covers(m, h) })
	}))

	return proto.Clone(r).(*rstr.Role), nil
}

type groups struct {
	rstr.UnimplementedGroupServiceServer
	*App
}

func (a groups) Get(ctx context.Context, req *rstr.GroupGetRequest) (*rstr.Group, error) {
	a.count(rstr.GroupService_Get_FullMethodName)
	a.mu.Lock()
	defer a.mu.Unlock()
	g := a.group(a.Tenant(ctx), req.GetRef())
	if g == nil {
		return nil, status.Error(codes.NotFound, "Group not found")
	}

	return proto.Clone(g).(*rstr.Group), nil
}

func (a groups) Add(ctx context.Context, req *rstr.GroupAddRequest) (*rstr.Group, error) {
	a.count(rstr.GroupService_Add_FullMethodName)
	if err := a.mine(req.GetApp()); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	t := a.Tenant(ctx)
	if a.groupBy(t, req.GetAlias(), "") != nil {
		return nil, status.Error(codes.AlreadyExists, "Group already exists")
	}
	g := rstr.Group_builder{
		Id: a.id(), Tenant: rstr.Tenant_builder{Id: t.GetId()}.Build(), Alias: req.GetAlias(), Name: req.GetName(), Desc: req.GetDesc(),
		App: rstr.Holder_builder{Alias: a.Alias}.Build(),
	}.Build()
	a.groups = append(a.groups, g)

	return proto.Clone(g).(*rstr.Group), nil
}

type bindings struct {
	rstr.UnimplementedBindingServiceServer
	*App
}

func (a bindings) List(ctx context.Context, req *rstr.BindingListRequest) (*rstr.BindingListResponse, error) {
	a.count(rstr.BindingService_List_FullMethodName)
	a.mu.Lock()
	defer a.mu.Unlock()
	t := a.Tenant(ctx)
	var out []*rstr.Binding
	for _, b := range a.bindings {
		r := a.role(t, rstr.RoleRef_builder{Id: b.GetRole().GetId()}.Build())
		if r == nil {
			// Not the app's, or not the tenant's: walled.
			continue
		}
		if fs := req.GetFilters(); len(fs) > 0 && !slices.ContainsFunc(fs, func(f *rstr.BindingFilter) bool {
			return (f.GetRole() == nil || string(f.GetRole().GetId()) == string(b.GetRole().GetId())) &&
				(f.GetGroup() == nil || string(f.GetGroup().GetId()) == string(b.GetGroup().GetId()))
		}) {
			continue
		}
		out = append(out, proto.Clone(b).(*rstr.Binding))
	}

	return rstr.BindingListResponse_builder{Items: out}.Build(), nil
}

func (a bindings) Add(ctx context.Context, req *rstr.BindingAddRequest) (*rstr.Binding, error) {
	a.count(rstr.BindingService_Add_FullMethodName)
	a.mu.Lock()
	defer a.mu.Unlock()
	t := a.Tenant(ctx)
	r, g := a.role(t, req.GetRole()), a.group(t, req.GetGroup())
	if r == nil || g == nil {
		return nil, status.Error(codes.NotFound, "Binding: referenced entity not found")
	}
	for _, b := range a.bindings {
		if string(b.GetRole().GetId()) == string(r.GetId()) && string(b.GetGroup().GetId()) == string(g.GetId()) && b.GetSite() == nil {
			return nil, status.Error(codes.AlreadyExists, "Binding already exists")
		}
	}
	b := rstr.Binding_builder{Id: a.id(), Role: rstr.Role_builder{Id: r.GetId()}.Build(), Group: rstr.Group_builder{Id: g.GetId()}.Build()}.Build()
	b.SetDateCreated(a.now())
	a.bindings = append(a.bindings, b)

	return proto.Clone(b).(*rstr.Binding), nil
}

func (a bindings) Erase(ctx context.Context, req *rstr.BindingRef) (*rstr.BindingEraseResponse, error) {
	a.count(rstr.BindingService_Erase_FullMethodName)
	a.mu.Lock()
	defer a.mu.Unlock()
	for i, b := range a.bindings {
		if string(b.GetId()) == string(req.GetId()) && a.role(a.Tenant(ctx), rstr.RoleRef_builder{Id: b.GetRole().GetId()}.Build()) != nil {
			a.bindings = slices.Delete(a.bindings, i, i+1)
			return rstr.BindingEraseResponse_builder{}.Build(), nil
		}
	}

	return nil, status.Error(codes.NotFound, "Binding not found")
}

type places struct {
	rstr.UnimplementedGroupMembershipServiceServer
	*App
}

func (a places) Add(ctx context.Context, req *rstr.GroupMembershipAddRequest) (*rstr.GroupMembership, error) {
	a.count(rstr.GroupMembershipService_Add_FullMethodName)
	a.mu.Lock()
	defer a.mu.Unlock()
	g := a.group(a.Tenant(ctx), req.GetGroup())
	if g == nil {
		return nil, status.Error(codes.NotFound, "GroupMembership: referenced entity not found")
	}
	holder := req.GetHolder().GetId()
	for _, p := range a.places {
		if !p.erased && string(p.holder) == string(holder) && string(p.group) == string(g.GetId()) {
			return nil, status.Error(codes.AlreadyExists, "GroupMembership already exists")
		}
	}
	a.places = append(a.places, &place{holder: holder, group: g.GetId()})

	return rstr.GroupMembership_builder{Id: a.id(), Holder: rstr.Holder_builder{Id: holder}.Build(), Group: rstr.Group_builder{Id: g.GetId()}.Build()}.Build(), nil
}
