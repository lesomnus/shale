package identity

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"uuid"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/roster/rstr"
)

// fakeRoster is an external roster's data plane, as much of it as Shale
// asks: one tenant, its teams and who is on them, and the people in it.
// Every call must carry the tenant key, and `down` makes it answer the way
// a roster that is not there does.
type fakeRoster struct {
	key    string
	tenant *rstr.Tenant

	mu      sync.Mutex
	teams   []*rstr.Team
	members map[string][][]byte
	holders map[string]*rstr.Holder

	down  atomic.Bool
	calls atomic.Int32
	addr  string
}

func newFakeRoster(t *testing.T, tenantAlias, key string) *fakeRoster {
	t.Helper()
	f := &fakeRoster{
		key:     key,
		tenant:  rstr.Tenant_builder{Id: pdid.New(1).Bytes(), Alias: tenantAlias, Name: tenantAlias}.Build(),
		members: map[string][][]byte{},
		holders: map[string]*rstr.Holder{},
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	g := grpc.NewServer(grpc.UnaryInterceptor(f.check))
	rstr.RegisterTenantServiceServer(g, tenants{fakeRoster: f})
	rstr.RegisterTeamServiceServer(g, teams{fakeRoster: f})
	rstr.RegisterTeamMembershipServiceServer(g, memberships{fakeRoster: f})
	rstr.RegisterHolderServiceServer(g, holders{fakeRoster: f})
	go g.Serve(l)
	t.Cleanup(g.Stop)
	f.addr = l.Addr().String()

	return f
}

// open is a store on this roster, as `auth.roster` would say it.
func (f *fakeRoster) open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), Config{Addr: f.addr, Insecure: true, Keys: map[string]string{f.tenant.GetAlias(): f.key}}, t.TempDir(), slog.Default())
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
	if v := md.Get(auth.Header); len(v) != 1 || v[0] != auth.BearerScheme+" "+f.key {
		return nil, status.Error(codes.Unauthenticated, "not the tenant key")
	}

	return next(ctx, req)
}

func (f *fakeRoster) addTeam(alias string, site bool) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := uuid.New()
	b := rstr.Team_builder{Id: id[:], Alias: alias, Tenant: rstr.Tenant_builder{Id: f.tenant.GetId()}.Build()}
	if site {
		sid := uuid.New()
		b.Site = rstr.Site_builder{Id: sid[:]}.Build()
	}
	f.teams = append(f.teams, b.Build())

	return id[:]
}

func (f *fakeRoster) join(holder, team []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.members[string(holder)] = append(f.members[string(holder)], team)
}

func (f *fakeRoster) leave(holder []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.members, string(holder))
}

type tenants struct {
	rstr.UnimplementedTenantServiceServer
	*fakeRoster
}

func (f tenants) Get(ctx context.Context, req *rstr.TenantGetRequest) (*rstr.Tenant, error) {
	if req.GetRef().GetAlias() != f.tenant.GetAlias() {
		return nil, status.Error(codes.NotFound, "Tenant not found")
	}

	return f.tenant, nil
}

type teams struct {
	rstr.UnimplementedTeamServiceServer
	*fakeRoster
}

func (f teams) Get(ctx context.Context, req *rstr.TeamGetRequest) (*rstr.Team, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, v := range f.teams {
		if v.GetAlias() == req.GetRef().GetSlug().GetAlias() && len(v.GetSite().GetId()) > 0 {
			return v, nil
		}
	}

	return nil, status.Error(codes.NotFound, "Team not found")
}

func (f teams) List(ctx context.Context, req *rstr.TeamListRequest) (*rstr.TeamListResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return rstr.TeamListResponse_builder{Items: f.teams}.Build(), nil
}

type memberships struct {
	rstr.UnimplementedTeamMembershipServiceServer
	*fakeRoster
}

func (f memberships) List(ctx context.Context, req *rstr.TeamMembershipListRequest) (*rstr.TeamMembershipListResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*rstr.TeamMembership
	for _, flt := range req.GetFilters() {
		h := flt.GetHolder().GetId()
		for _, team := range f.members[string(h)] {
			out = append(out, rstr.TeamMembership_builder{
				Holder: rstr.Holder_builder{Id: h}.Build(),
				Team:   rstr.Team_builder{Id: team}.Build(),
			}.Build())
		}
	}

	return rstr.TeamMembershipListResponse_builder{Items: out}.Build(), nil
}

type holders struct {
	rstr.UnimplementedHolderServiceServer
	*fakeRoster
}

func (f holders) Get(ctx context.Context, req *rstr.HolderGetRequest) (*rstr.Holder, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if v, ok := f.holders[string(req.GetRef().GetId())]; ok {
		return v, nil
	}

	return nil, status.Error(codes.NotFound, "Holder not found")
}

// addHolder is a person in the tenant.
func (f *fakeRoster) addHolder(alias string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := pdid.New(2).Bytes()
	f.holders[string(id)] = rstr.Holder_builder{Id: id, Alias: alias, Name: alias, Tenant: f.tenant}.Build()

	return id
}
