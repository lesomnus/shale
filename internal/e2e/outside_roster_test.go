package e2e_test

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/payday/pdpb"

	"github.com/lesomnus/roster/rstr"
)

// outsideRoster is a roster of its own that Shale reaches over the wire,
// as much of it as a deployment with `auth.roster.addr` asks while people
// call: one tenant, served with a tenant key Shale holds in
// `auth.roster.keys`; the people in it, by identifier and by name; what
// each is granted across the tenant (`HolderService.Reaches`); people's
// keys for Shale (`TokenService.Introspect`); and their addresses and the
// tenant's mail domains, answered as roster's generated lists answer them:
// walled to the tenant, an erased address never read, `date_verified`
// absent where nobody proved it, a filter by holder, only what the select
// names. A method Shale's holder is not allowed is PermissionDenied.
type outsideRoster struct {
	key    string
	tenant *rstr.Tenant
	addr   string

	mu      sync.Mutex
	holders map[string]*rstr.Holder
	grants  map[string][]string
	keys    map[string]outsideKey
	emails  []outsideEmail
	domains []string
	refused map[string]bool

	emailLists atomic.Int32
}

type outsideKey struct {
	holder  []byte
	methods []string
}

type outsideEmail struct {
	holder   []byte
	address  string
	verified *timestamppb.Timestamp
}

type outsideTenant struct{}

func newOutsideRoster(t *testing.T, tenant string) *outsideRoster {
	t.Helper()
	r := &outsideRoster{
		key:     "rt_shale_e2e",
		tenant:  rstr.Tenant_builder{Id: pdid.New(1).Bytes(), Alias: tenant, Name: tenant}.Build(),
		holders: map[string]*rstr.Holder{},
		grants:  map[string][]string{},
		keys:    map[string]outsideKey{},
		refused: map[string]bool{},
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	g := grpc.NewServer(grpc.UnaryInterceptor(r.check))
	rstr.RegisterTenantServiceServer(g, outsideTenants{outsideRoster: r})
	rstr.RegisterHolderServiceServer(g, outsideHolders{outsideRoster: r})
	pdpb.RegisterTokenServiceServer(g, outsideTokens{outsideRoster: r})
	rstr.RegisterEmailServiceServer(g, outsideEmails{outsideRoster: r})
	rstr.RegisterMailDomainServiceServer(g, outsideMailDomains{outsideRoster: r})
	go g.Serve(l)
	t.Cleanup(g.Stop)
	r.addr = l.Addr().String()

	return r
}

// check answers Shale's tenant key as Shale's holder in the tenant, and
// nobody else.
func (r *outsideRoster) check(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get(auth.Header); len(v) != 1 || v[0] != auth.BearerScheme+" "+r.key {
		return nil, status.Error(codes.Unauthenticated, "not a key this roster has")
	}
	r.mu.Lock()
	refused := r.refused[info.FullMethod]
	r.mu.Unlock()
	if refused {
		return nil, status.Errorf(codes.PermissionDenied, "%s: not a method the caller's roles name", info.FullMethod)
	}

	return next(context.WithValue(ctx, outsideTenant{}, r.tenant), req)
}

// person is somebody of the tenant, granted patterns across it.
func (r *outsideRoster) person(alias string, patterns ...string) []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := pdid.New(2).Bytes()
	r.holders[string(id)] = rstr.Holder_builder{Id: id, Alias: alias, Name: alias, Tenant: r.tenant}.Build()
	r.grants[string(id)] = patterns

	return id
}

// email is an address of a person's, proved or not.
func (r *outsideRoster) email(holder []byte, address string, proved bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := outsideEmail{holder: holder, address: address}
	if proved {
		v.verified = timestamppb.Now()
	}
	r.emails = append(r.emails, v)
}

// domain is a mail domain the tenant names, lower-cased as roster writes it.
func (r *outsideRoster) domain(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.domains = append(r.domains, strings.ToLower(name))
}

// apiKey is a person's key for Shale, naming methods and no scope.
func (r *outsideRoster) apiKey(holder []byte, methods ...string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := fmt.Sprintf("rt_person_%d", len(r.keys))
	r.keys[k] = outsideKey{holder: holder, methods: methods}

	return k
}

func (r *outsideRoster) refuse(method string, on bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refused[method] = on
}

type outsideTenants struct {
	rstr.UnimplementedTenantServiceServer
	*outsideRoster
}

func (r outsideTenants) Get(ctx context.Context, req *rstr.TenantGetRequest) (*rstr.Tenant, error) {
	if ref := req.GetRef(); ref.GetAlias() != r.tenant.GetAlias() && string(ref.GetId()) != string(r.tenant.GetId()) {
		return nil, status.Error(codes.NotFound, "Tenant not found")
	}

	return r.tenant, nil
}

type outsideHolders struct {
	rstr.UnimplementedHolderServiceServer
	*outsideRoster
}

func (r outsideHolders) find(ref *rstr.HolderRef) (*rstr.Holder, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v, ok := r.holders[string(ref.GetId())]; ok {
		return v, nil
	}
	if s := ref.GetSlug(); s != nil && s.GetTenant().GetAlias() == r.tenant.GetAlias() {
		for _, v := range r.holders {
			if v.GetAlias() == s.GetAlias() {
				return v, nil
			}
		}
	}

	return nil, status.Error(codes.NotFound, "Holder not found")
}

func (r outsideHolders) Get(_ context.Context, req *rstr.HolderGetRequest) (*rstr.Holder, error) {
	return r.find(req.GetRef())
}

func (r outsideHolders) Reaches(_ context.Context, req *rstr.HolderReachesRequest) (*rstr.HolderReachesResponse, error) {
	v, err := r.find(req.GetRef())
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	held := r.grants[string(v.GetId())]

	return rstr.HolderReachesResponse_builder{Methods: held, Everywhere: held}.Build(), nil
}

type outsideTokens struct {
	pdpb.UnimplementedTokenServiceServer
	*outsideRoster
}

// Introspect is roster's answer about a person's key for Shale: the
// holder, their tenant, the key's methods as the grant, no scope.
func (r outsideTokens) Introspect(_ context.Context, req *pdpb.TokenIntrospectRequest) (*pdpb.TokenIntrospectResponse, error) {
	r.mu.Lock()
	k, ok := r.keys[req.GetToken()]
	r.mu.Unlock()
	if !ok {
		return nil, status.Error(codes.NotFound, "no such token")
	}
	g, err := auth.Introspection(auth.Identity{Grant: frame.Whole().To(k.methods...)})
	if err != nil {
		return nil, err
	}

	return pdpb.TokenIntrospectResponse_builder{Id: k.holder, TenantId: r.tenant.GetId(), Grant: g.GetGrant()}.Build(), nil
}

type outsideEmails struct {
	rstr.UnimplementedEmailServiceServer
	*outsideRoster
}

func (r outsideEmails) List(_ context.Context, req *rstr.EmailListRequest) (*rstr.EmailListResponse, error) {
	r.emailLists.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	sel := req.GetSelect()
	var out []*rstr.Email
	for _, v := range r.emails {
		if fs := req.GetFilters(); len(fs) > 0 && !slices.ContainsFunc(fs, func(x *rstr.EmailFilter) bool { return string(x.GetHolder().GetId()) == string(v.holder) }) {
			continue
		}
		b := rstr.Email_builder{}
		if sel == nil || sel.GetAll() || sel.GetAddress() {
			b.Address = v.address
		}
		if sel == nil || sel.GetAll() || sel.GetDateVerified() {
			b.DateVerified = v.verified
		}
		out = append(out, b.Build())
	}

	return rstr.EmailListResponse_builder{Items: out}.Build(), nil
}

type outsideMailDomains struct {
	rstr.UnimplementedMailDomainServiceServer
	*outsideRoster
}

func (r outsideMailDomains) List(context.Context, *rstr.MailDomainListRequest) (*rstr.MailDomainListResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*rstr.MailDomain, 0, len(r.domains))
	for _, n := range r.domains {
		out = append(out, rstr.MailDomain_builder{Name: n}.Build())
	}

	return rstr.MailDomainListResponse_builder{Items: out}.Build(), nil
}
