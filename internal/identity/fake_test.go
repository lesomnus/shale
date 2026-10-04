package identity

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/roster/rstr"
	"github.com/lesomnus/roster/server/front"
)

// fakeRoster is an external roster's data plane, as much of it as Shale
// asks: tenants and the people in them, what each person is granted, and
// two keys answered the way roster answers them. The tenant key (`rt_`) is
// the home tenant's holder and takes no `roster-at`; the deployment key
// (`rk_`) lists its own nominations as itself, and otherwise names a
// tenant that nominated it with `roster-at` or is refused. Every read is
// walled to the tenant the key is answered in, and `down` makes it answer
// the way a roster that is not there does.
type fakeRoster struct {
	tenantKey string
	deployKey string
	tenant    *rstr.Tenant

	mu        sync.Mutex
	tenants   map[string]*rstr.Tenant
	nominated map[string]bool
	holders   map[string]*rstr.Holder
	grants    map[string][]string

	down  atomic.Bool
	calls atomic.Int32
	lists atomic.Int32
	addr  string
}

type inTenant struct{}

func newFakeRoster(t *testing.T, tenantAlias string) *fakeRoster {
	t.Helper()
	f := &fakeRoster{
		tenantKey: "rt_test",
		deployKey: "rk_test",
		tenants:   map[string]*rstr.Tenant{},
		nominated: map[string]bool{},
		holders:   map[string]*rstr.Holder{},
		grants:    map[string][]string{},
	}
	f.tenant = f.addTenant(tenantAlias)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	g := grpc.NewServer(grpc.UnaryInterceptor(f.check))
	rstr.RegisterTenantServiceServer(g, tenants{fakeRoster: f})
	rstr.RegisterHolderServiceServer(g, holders{fakeRoster: f})
	rstr.RegisterNominationServiceServer(g, nominations{fakeRoster: f})
	rstr.RegisterMeServiceServer(g, me{fakeRoster: f})
	go g.Serve(l)
	t.Cleanup(g.Stop)
	f.addr = l.Addr().String()

	return f
}

// open is a store on this roster holding the home tenant's key in
// `auth.roster.keys`, as a deployment given a tenant key each does.
func (f *fakeRoster) open(t *testing.T) *Store {
	t.Helper()

	return f.openWith(t, Config{Keys: map[string]string{f.tenant.GetAlias(): f.tenantKey}})
}

// openWith is a store on this roster, as `auth.roster` says beside the
// address.
func (f *fakeRoster) openWith(t *testing.T, cfg Config) *Store {
	t.Helper()
	cfg.Addr, cfg.Insecure = f.addr, true
	s, err := Open(context.Background(), cfg, t.TempDir(), slog.Default())
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	return s
}

func (f *fakeRoster) check(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	f.calls.Add(1)
	if f.down.Load() {
		return nil, status.Error(codes.Unavailable, "roster is down")
	}
	md, _ := metadata.FromIncomingContext(ctx)
	at := md.Get(front.HeaderAt)
	switch v := md.Get(auth.Header); {
	case len(v) == 1 && v[0] == auth.BearerScheme+" "+f.tenantKey:
		if len(at) > 0 {
			return nil, status.Error(codes.Unauthenticated, "roster-at beside a tenant key")
		}

		return next(context.WithValue(ctx, inTenant{}, f.tenant), req)
	case len(v) == 1 && v[0] == auth.BearerScheme+" "+f.deployKey:
		if info.FullMethod == rstr.NominationService_List_FullMethodName {
			if len(at) > 0 {
				return nil, status.Error(codes.InvalidArgument, "the key's own nominations are asked as the key")
			}
			f.lists.Add(1)

			return next(ctx, req)
		}
		if len(at) != 1 {
			return nil, status.Error(codes.Unauthenticated, "a deployment key holds nothing as itself")
		}
		t, ok := f.at(at[0])
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "no nomination for this key there")
		}

		return next(context.WithValue(ctx, inTenant{}, t), req)
	default:
		return nil, status.Error(codes.Unauthenticated, "not a key this roster has")
	}
}

// at is the tenant a `roster-at` value chooses, if it nominated the key.
func (f *fakeRoster) at(v string) (*rstr.Tenant, bool) {
	ref, ok := strings.CutPrefix(v, front.TenantMark)
	if !ok {
		return nil, false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for alias, t := range f.tenants {
		if (alias == ref || pdid.Id(t.GetId()).String() == ref) && f.nominated[alias] {
			return t, true
		}
	}

	return nil, false
}

func walled(ctx context.Context) *rstr.Tenant {
	t, _ := ctx.Value(inTenant{}).(*rstr.Tenant)

	return t
}

func (f *fakeRoster) addTenant(alias string) *rstr.Tenant {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := rstr.Tenant_builder{Id: pdid.New(1).Bytes(), Alias: alias, Name: alias}.Build()
	f.tenants[alias] = t

	return t
}

// nominate says a tenant nominated the deployment key, or ended it.
func (f *fakeRoster) nominate(alias string, on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nominated[alias] = on
}

// grant is what roster answers a person reaches across their tenant.
func (f *fakeRoster) grant(holder []byte, patterns ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.grants[string(holder)] = patterns
}

type tenants struct {
	rstr.UnimplementedTenantServiceServer
	*fakeRoster
}

func (f tenants) Get(ctx context.Context, req *rstr.TenantGetRequest) (*rstr.Tenant, error) {
	t := walled(ctx)
	if t == nil || (req.GetRef().GetAlias() != t.GetAlias() && string(req.GetRef().GetId()) != string(t.GetId())) {
		return nil, status.Error(codes.NotFound, "Tenant not found")
	}

	return t, nil
}

type holders struct {
	rstr.UnimplementedHolderServiceServer
	*fakeRoster
}

func (f holders) find(ctx context.Context, ref *rstr.HolderRef) (*rstr.Holder, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.holders[string(ref.GetId())]
	if !ok || string(v.GetTenant().GetId()) != string(walled(ctx).GetId()) {
		return nil, status.Error(codes.NotFound, "Holder not found")
	}

	return v, nil
}

func (f holders) Get(ctx context.Context, req *rstr.HolderGetRequest) (*rstr.Holder, error) {
	return f.find(ctx, req.GetRef())
}

func (f holders) Reaches(ctx context.Context, req *rstr.HolderReachesRequest) (*rstr.HolderReachesResponse, error) {
	v, err := f.find(ctx, req.GetRef())
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	held := f.grants[string(v.GetId())]

	// `methods` is wider than `everywhere` here on purpose: a grant bound
	// at one of roster's sites is in the first and not the second, and
	// Shale reads the second.
	return rstr.HolderReachesResponse_builder{
		Methods:    append(append([]string{}, held...), "/shale.*/*"),
		Everywhere: held,
	}.Build(), nil
}

type nominations struct {
	rstr.UnimplementedNominationServiceServer
	*fakeRoster
}

func (f nominations) List(ctx context.Context, req *rstr.NominationListRequest) (*rstr.NominationListResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*rstr.Nomination
	for alias, on := range f.nominated {
		if on {
			out = append(out, rstr.Nomination_builder{Tenant: rstr.Tenant_builder{Id: f.tenants[alias].GetId()}.Build()}.Build())
		}
	}

	return rstr.NominationListResponse_builder{Items: out}.Build(), nil
}

type me struct {
	rstr.UnimplementedMeServiceServer
	*fakeRoster
}

func (f me) Get(ctx context.Context, req *rstr.MeGetRequest) (*rstr.MeGetResponse, error) {
	return rstr.MeGetResponse_builder{Tenant: walled(ctx).GetId(), Alias: Agent}.Build(), nil
}

// addHolder is a person in the home tenant.
func (f *fakeRoster) addHolder(alias string) []byte {
	return f.addHolderIn(f.tenant, alias)
}

// addHolderIn is a person in a tenant.
func (f *fakeRoster) addHolderIn(t *rstr.Tenant, alias string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := pdid.New(2).Bytes()
	f.holders[string(id)] = rstr.Holder_builder{Id: id, Alias: alias, Name: alias, Tenant: t}.Build()

	return id
}
