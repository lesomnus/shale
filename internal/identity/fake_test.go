package identity

import (
	"context"
	"fmt"
	"log/slog"
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
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/payday/pdpb"

	"github.com/lesomnus/roster/rstr"
	"github.com/lesomnus/roster/server/front"
)

// fakeRoster is an external roster's data plane, as much of it as Shale
// asks: tenants and the people in them, what each person is granted, their
// addresses and each tenant's mail domains (listed as roster's generated
// lists answer: by holder, an erased address never read, `date_verified`
// absent where nobody proved it, only what the select names), and two
// keys answered the way roster answers them. A method refused is
// PermissionDenied, as roster's gate answers a caller whose roles do not
// name it. The tenant key (`rt_`) is
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
	// emails are the people's addresses, and domains each tenant's mail
	// domains by its alias; refused are the methods Shale's holder is not
	// allowed, answered PermissionDenied as roster's gate does.
	emails  []fakeEmail
	domains map[string][]string
	refused map[string]bool
	// emailSelects are the selects EmailService.List was asked with, and
	// emailLists and domainLists how many times each list was asked.
	emailSelects []*rstr.EmailSelect
	emailLists   atomic.Int32
	domainLists  atomic.Int32

	// exchanged is the holder each exchange token names, issued to Shale.
	exchanged map[string][]byte
	// sync is what the sync stream says next; a nil event ends the stream,
	// and noSync refuses it the way roster does a key not allowed it.
	sync   chan *rstr.SyncEvent
	noSync atomic.Bool
	syncs  atomic.Int32

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
		exchanged: map[string][]byte{},
		domains:   map[string][]string{},
		refused:   map[string]bool{},
		sync:      make(chan *rstr.SyncEvent, 16),
	}
	f.tenant = f.addTenant(tenantAlias)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	g := grpc.NewServer(grpc.UnaryInterceptor(f.check))
	rstr.RegisterTenantServiceServer(g, tenants{fakeRoster: f})
	rstr.RegisterHolderServiceServer(g, holders{fakeRoster: f})
	rstr.RegisterNominationServiceServer(g, nominations{fakeRoster: f})
	rstr.RegisterMeServiceServer(g, me{fakeRoster: f})
	pdpb.RegisterTokenServiceServer(g, tokens{fakeRoster: f})
	rstr.RegisterSyncServiceServer(g, syncs{fakeRoster: f})
	rstr.RegisterEmailServiceServer(g, emails{fakeRoster: f})
	rstr.RegisterMailDomainServiceServer(g, mailDomains{fakeRoster: f})
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
	f.mu.Lock()
	refused := f.refused[info.FullMethod]
	f.mu.Unlock()
	if refused {
		next = func(context.Context, any) (any, error) {
			return nil, status.Errorf(codes.PermissionDenied, "%s: not a method the caller's roles name", info.FullMethod)
		}
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

type tokens struct {
	pdpb.UnimplementedTokenServiceServer
	*fakeRoster
}

// Introspect answers an exchange token to the tenant's Shale, in the
// tenant it was issued in, and NotFound to anybody else.
func (f tokens) Introspect(ctx context.Context, req *pdpb.TokenIntrospectRequest) (*pdpb.TokenIntrospectResponse, error) {
	f.mu.Lock()
	holder, ok := f.exchanged[req.GetToken()]
	in := f.holders[string(holder)].GetTenant().GetId()
	f.mu.Unlock()
	t := walled(ctx)
	if !ok || t == nil || string(in) != string(t.GetId()) {
		return nil, status.Error(codes.NotFound, "no such token")
	}

	return pdpb.TokenIntrospectResponse_builder{Id: holder, TenantId: t.GetId()}.Build(), nil
}

type syncs struct {
	rstr.UnimplementedSyncServiceServer
	*fakeRoster
}

func (f syncs) Watch(_ *rstr.SyncWatchRequest, st rstr.SyncService_WatchServer) error {
	if f.noSync.Load() {
		return status.Error(codes.Unimplemented, "no broker")
	}
	f.syncs.Add(1)
	for {
		select {
		case <-st.Context().Done():
			return nil
		case e := <-f.sync:
			if e == nil {
				return status.Error(codes.Unavailable, "the stream broke")
			}
			if err := st.Send(e); err != nil {
				return err
			}
		}
	}
}

// fakeEmail is an address of a person's, as roster keeps it: proved when
// verified is set, and never read once erased.
type fakeEmail struct {
	holder   []byte
	tenant   []byte
	address  string
	verified *timestamppb.Timestamp
	erased   bool
}

// addEmail is an address of a person's, proved or not.
func (f *fakeRoster) addEmail(holder []byte, address string, proved bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v := fakeEmail{holder: holder, tenant: f.holders[string(holder)].GetTenant().GetId(), address: address}
	if proved {
		v.verified = timestamppb.Now()
	}
	f.emails = append(f.emails, v)
}

// eraseEmails erases a person's addresses.
func (f *fakeRoster) eraseEmails(holder []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.emails {
		if string(f.emails[i].holder) == string(holder) {
			f.emails[i].erased = true
		}
	}
}

// addDomain is a mail domain a tenant names, written as roster writes it:
// lower-cased.
func (f *fakeRoster) addDomain(tenant, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.domains[tenant] = append(f.domains[tenant], strings.ToLower(name))
}

// refuse makes Shale's holder not allowed a method, or allowed it again.
func (f *fakeRoster) refuse(method string, on bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refused[method] = on
}

// page is a list's page of n rows from after, as roster cuts one: the
// schema's size when nothing is said, never more than the cap.
func page(n int, size int32, after string, def, max int) (from, to int, next string) {
	k := int(size)
	if k <= 0 {
		k = def
	}
	k = min(k, max)
	if after != "" {
		fmt.Sscan(after, &from)
	}
	from = min(from, n)
	to = min(from+k, n)
	if to < n {
		next = fmt.Sprint(to)
	}

	return from, to, next
}

type emails struct {
	rstr.UnimplementedEmailServiceServer
	*fakeRoster
}

// List is roster's generated List: the rows of the caller's tenant (the
// wall), never an erased one, those of the holder a filter names -- every
// one of the tenant's with no filter, which is what a grant bound with no
// site sees -- and of each, only what the select names.
func (f emails) List(ctx context.Context, req *rstr.EmailListRequest) (*rstr.EmailListResponse, error) {
	f.emailLists.Add(1)
	t := walled(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.emailSelects = append(f.emailSelects, req.GetSelect())
	var rows []fakeEmail
	for _, v := range f.emails {
		if v.erased || string(v.tenant) != string(t.GetId()) {
			continue
		}
		if fs := req.GetFilters(); len(fs) > 0 && !slices.ContainsFunc(fs, func(x *rstr.EmailFilter) bool { return string(x.GetHolder().GetId()) == string(v.holder) }) {
			continue
		}
		rows = append(rows, v)
	}
	from, to, next := page(len(rows), req.GetSize(), req.GetAfter(), 20, 100)
	sel := req.GetSelect()
	out := make([]*rstr.Email, 0, to-from)
	for _, v := range rows[from:to] {
		b := rstr.Email_builder{}
		if sel == nil || sel.GetAll() || sel.GetAddress() {
			b.Address = v.address
		}
		if sel == nil || sel.GetAll() || sel.GetDateVerified() {
			b.DateVerified = v.verified
		}
		if sel == nil || sel.GetAll() || sel.GetHolder() != nil {
			b.Holder = rstr.Holder_builder{Id: v.holder}.Build()
		}
		out = append(out, b.Build())
	}

	return rstr.EmailListResponse_builder{Items: out, Next: next}.Build(), nil
}

type mailDomains struct {
	rstr.UnimplementedMailDomainServiceServer
	*fakeRoster
}

// List is roster's generated List: the caller's tenant's mail domains, of
// the tenant a filter names.
func (f mailDomains) List(ctx context.Context, req *rstr.MailDomainListRequest) (*rstr.MailDomainListResponse, error) {
	f.domainLists.Add(1)
	t := walled(ctx)
	if fs := req.GetFilters(); len(fs) > 0 && !slices.ContainsFunc(fs, func(x *rstr.MailDomainFilter) bool {
		return string(x.GetTenant().GetId()) == string(t.GetId()) || x.GetTenant().GetAlias() == t.GetAlias()
	}) {
		return rstr.MailDomainListResponse_builder{}.Build(), nil
	}
	f.mu.Lock()
	names := slices.Clone(f.domains[t.GetAlias()])
	f.mu.Unlock()
	from, to, next := page(len(names), req.GetSize(), req.GetAfter(), 20, 100)
	out := make([]*rstr.MailDomain, 0, to-from)
	for _, n := range names[from:to] {
		out = append(out, rstr.MailDomain_builder{Name: n}.Build())
	}

	return rstr.MailDomainListResponse_builder{Items: out, Next: next}.Build(), nil
}
