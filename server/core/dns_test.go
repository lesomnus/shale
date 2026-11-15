package core

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/netip"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/payday/config"
	_ "github.com/lesomnus/payday/config/dbsqlite3"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/z"
	"github.com/miekg/dns"
	"github.com/protobuf-orm/ent/dialect/sql"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/dnsupdate/dnsupdatetest"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/addresspolicy"
	"github.com/lesomnus/shale/internal/ent/enttest"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/bare"
	"github.com/lesomnus/shale/server/pd"
)

type dnsFixture struct {
	t      *testing.T
	ctx    context.Context
	core   Core
	sink   pd.Sink
	ent    *ent.Client
	deps   *Deps
	srv    *dnsupdatetest.Server
	secret []byte
	rec    *DnsRecords
}

func newDnsFixture(t *testing.T) *dnsFixture {
	t.Helper()
	ctx := t.Context()
	cfg := config.DbConfig{Driver: "sqlite3", Dsn: "file:" + filepath.Join(t.TempDir(), "shale.db") + "?_pragma=foreign_keys(1)"}
	db, dialect, err := cfg.Open(ctx)
	require.NoError(t, err)
	driver := sql.OpenDB(dialect, db)
	client := enttest.NewClient(t, enttest.WithOptions(ent.Driver(driver)))
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	sink, err := pd.NewSink(client, bare.WithMinter(pd.Minter()))
	require.NoError(t, err)

	kek := make(Kek, 32)
	rand.Read(kek)
	secret := make([]byte, 32)
	rand.Read(secret)
	srv, err := dnsupdatetest.Start("nodes.example.com", "shale-update", secret)
	require.NoError(t, err)
	t.Cleanup(func() { srv.Close() })

	d := &Deps{Ent: client, Drv: driver, Own: sink, Kek: func() Kek { return kek }, ClusterTenant: pdid.New(DomTenant)}
	f := &dnsFixture{t: t, core: New(sink, d), sink: sink, ent: client, deps: d, srv: srv, secret: secret, rec: NewDnsRecords(d)}
	f.ctx = frame.Into(ctx, frame.New(pdid.New(DomHolder), d.ClusterTenant, frame.Whole()))

	return f
}

func (f *dnsFixture) params() *api.AddressParams {
	return api.AddressParams_builder{
		Resolver:    "dns",
		Template:    "{alias}.nodes.example.com",
		DnsServer:   f.srv.Addr(),
		DnsZone:     "nodes.example.com",
		DnsTsigName: "shale-update",
	}.Build()
}

// policy adds a dns policy and activates it, which asks the server.
func (f *dnsFixture) policy(version int64, p *api.AddressParams, secret []byte) (*api.AddressPolicy, error) {
	f.t.Helper()
	v, err := f.core.AddressPolicy().Add(f.ctx, api.AddressPolicyAddRequest_builder{
		Alias: fmt.Sprintf("dns-%d", version), Version: version, Params: p, DnsTsigSecret: secret,
	}.Build())
	require.NoError(f.t, err)

	return f.core.AddressPolicy().Activate(f.ctx, api.AddressPolicyActivateRequest_builder{Ref: api.AddressPolicyRef_builder{Id: v.GetId()}.Build()}.Build())
}

func (f *dnsFixture) node(alias, data string, addrs ...string) pdid.Id {
	f.t.Helper()
	id := pdid.New(DomNode)
	_, err := f.sink.Node().Add(f.ctx, api.NodeAddRequest_builder{
		Id: id.Bytes(), Alias: alias, HardwareId: "hw-" + alias, State: api.HostState_HOST_STATE_ADOPTED,
		Interfaces: []*api.HostInterface{api.HostInterface_builder{Name: "eth0", Addresses: addrs}.Build()}, DataAddress: data,
	}.Build())
	require.NoError(f.t, err)

	return id
}

func (f *dnsFixture) syncStatus() *api.DnsSync {
	f.t.Helper()
	vs, err := f.sink.AddressPolicy().List(f.ctx, api.AddressPolicyListRequest_builder{}.Build())
	require.NoError(f.t, err)
	for _, v := range vs.GetItems() {
		if v.GetActive() {
			return v.GetDnsSync()
		}
	}

	return nil
}

func TestDnsRecordsFollowTheHosts(t *testing.T) {
	x := require.New(t)
	f := newDnsFixture(t)
	ctx := f.ctx

	// Somebody else's names in the zone: one beside Shale's, and one a
	// node's alias would give.
	x.NoError(f.srv.Add("www.nodes.example.com. 300 IN A 192.0.2.80", "taken.nodes.example.com. 300 IN A 192.0.2.81"))

	_, err := f.policy(1, f.params(), f.secret)
	x.NoError(err)

	// A node that joins with its data plane on every interface: its first
	// IPv4 and IPv6 address, link-local skipped. A relay on one IP: that
	// IP. A node whose name somebody holds: left alone. And a node not
	// adopted yet: no name.
	edge := f.node("edge-1", ":7410", "fe80::1/64", "192.0.2.10/24", "2001:db8::10/64", "172.17.0.1/16")
	_, err = f.sink.Relay().Add(ctx, api.RelayAddRequest_builder{
		Id: pdid.New(DomRelay).Bytes(), Alias: "relay-1", HardwareId: "hw-relay-1", State: api.HostState_HOST_STATE_ADOPTED,
		IngestAddress: "192.0.2.20:7420",
	}.Build())
	x.NoError(err)
	f.node("taken", ":7410", "192.0.2.30/24")
	_, err = f.sink.Node().Add(ctx, api.NodeAddRequest_builder{
		Id: pdid.New(DomNode).Bytes(), Alias: "pending-1", HardwareId: "hw-pending", State: api.HostState_HOST_STATE_PENDING,
		DataAddress: "192.0.2.40:7410",
	}.Build())
	x.NoError(err)

	x.NoError(f.rec.Once(ctx))
	x.Equal(1, f.rec.Syncs, "the first pass reads the whole zone")
	edgeName := "edge-1.nodes.example.com."
	x.Equal([]string{"192.0.2.10"}, f.srv.Get(edgeName, dns.TypeA))
	x.Equal([]string{"2001:db8::10"}, f.srv.Get(edgeName, dns.TypeAAAA))
	x.Equal([]string{`"` + DnsMarker(f.deps.ClusterTenant.String()) + `"`}, f.srv.Get(edgeName, dns.TypeTXT))
	x.EqualValues(DefaultDnsTTL, f.srv.TTL(edgeName, dns.TypeA))
	x.Equal([]string{"192.0.2.20"}, f.srv.Get("relay-1.nodes.example.com.", dns.TypeA))
	x.Equal([]string{"192.0.2.81"}, f.srv.Get("taken.nodes.example.com.", dns.TypeANY), "somebody else's name stays theirs")
	x.Equal([]string{"192.0.2.80"}, f.srv.Get("www.nodes.example.com.", dns.TypeANY))
	x.Empty(f.srv.Get("pending-1.nodes.example.com.", dns.TypeANY))
	st := f.syncStatus()
	x.NotNil(st.GetDateSynced())
	x.EqualValues(2, st.GetNames())
	x.Equal([]string{"taken.nodes.example.com."}, st.GetConflicts())
	x.Empty(st.GetError())

	// Its addresses change: the next tick follows, without reading the
	// zone whole.
	_, err = f.sink.Node().Patch(ctx, api.NodePatchRequest_builder{
		Ref:              api.NodeRef_builder{Id: edge.Bytes()}.Build(),
		Interfaces:       []*api.HostInterface{api.HostInterface_builder{Name: "eth0", Addresses: []string{"192.0.2.11/24"}}.Build()},
		DateUpdatedForce: z.Ptr(true),
	}.Build())
	x.NoError(err)
	x.NoError(f.rec.Once(ctx))
	x.Equal(1, f.rec.Syncs)
	x.Equal([]string{"192.0.2.11"}, f.srv.Get(edgeName, dns.TypeA))
	x.Empty(f.srv.Get(edgeName, dns.TypeAAAA))

	// It is removed: its name goes, and nothing else does.
	_, err = f.sink.Node().Erase(ctx, api.NodeRef_builder{Id: edge.Bytes()}.Build())
	x.NoError(err)
	x.NoError(f.rec.Once(ctx))
	x.Empty(f.srv.Get(edgeName, dns.TypeANY))
	x.Equal([]string{"192.0.2.80"}, f.srv.Get("www.nodes.example.com.", dns.TypeANY))
	x.Equal([]string{"192.0.2.81"}, f.srv.Get("taken.nodes.example.com.", dns.TypeANY))
	x.Equal([]string{"192.0.2.20"}, f.srv.Get("relay-1.nodes.example.com.", dns.TypeA))

	// Somebody deletes a record behind Shale's back: a tick does not ask
	// the zone about a host that did not change, the whole read repairs it.
	f.srv.Remove("relay-1.nodes.example.com.", dns.TypeA)
	x.NoError(f.rec.Once(ctx))
	x.Empty(f.srv.Get("relay-1.nodes.example.com.", dns.TypeA))
	x.NoError(f.rec.Sync(ctx))
	x.Equal(2, f.rec.Syncs)
	x.Equal([]string{"192.0.2.20"}, f.srv.Get("relay-1.nodes.example.com.", dns.TypeA))

	// A name this cluster wrote that no host wants any more, which only
	// the whole read finds: say a host erased while another replica led.
	x.NoError(f.srv.Add(
		"gone.nodes.example.com. 60 IN A 192.0.2.99",
		`gone.nodes.example.com. 60 IN TXT "`+DnsMarker(f.deps.ClusterTenant.String())+`"`,
		`other.nodes.example.com. 60 IN TXT "`+DnsMarker(pdid.New(DomTenant).String())+`"`,
		"other.nodes.example.com. 60 IN A 192.0.2.98",
	))
	x.NoError(f.rec.Sync(ctx))
	x.Empty(f.srv.Get("gone.nodes.example.com.", dns.TypeANY))
	x.Len(f.srv.Get("other.nodes.example.com.", dns.TypeANY), 2, "another cluster's name is not this one's")

	// The TTL changes with the policy: a new policy is a whole read.
	p := f.params()
	p.SetDnsTtl(30)
	_, err = f.policy(2, p, f.secret)
	x.NoError(err)
	x.NoError(f.rec.Once(ctx))
	x.Equal(4, f.rec.Syncs)
	x.EqualValues(30, f.srv.TTL("relay-1.nodes.example.com.", dns.TypeA))
}

// A key the server does not take changes nothing, and says so where an
// operator looks: on the policy, and the leader keeps the zone as it was.
func TestDnsRecordsBadKey(t *testing.T) {
	x := require.New(t)
	f := newDnsFixture(t)
	ctx := f.ctx

	wrong := make([]byte, 32)
	rand.Read(wrong)
	_, err := f.policy(1, f.params(), wrong)
	x.Equal(codes.FailedPrecondition, status.Code(err), "activation asks the server, which refuses the key")
	x.ErrorContains(err, "NOTAUTH")

	_, err = f.policy(2, f.params(), f.secret)
	x.NoError(err)
	f.node("edge-1", "192.0.2.10:7410")
	x.NoError(f.rec.Once(ctx))
	x.Equal([]string{"192.0.2.10"}, f.srv.Get("edge-1.nodes.example.com.", dns.TypeA))
	updates, _ := f.srv.Counts()

	// The secret is replaced by a wrong one on the active policy.
	pol, err := f.sink.AddressPolicy().Get(ctx, api.AddressPolicyGetRequest_builder{Ref: api.AddressPolicyRef_builder{Version: z.Ptr(int64(2))}.Build()}.Build())
	x.NoError(err)
	_, err = f.core.AddressPolicy().Patch(ctx, api.AddressPolicyPatchRequest_builder{
		Ref: api.AddressPolicyRef_builder{Id: pol.GetId()}.Build(), DnsTsigSecret: wrong, DateUpdatedForce: z.Ptr(true),
	}.Build())
	x.NoError(err)
	f.node("edge-2", "192.0.2.12:7410")
	x.NoError(f.rec.Once(ctx))
	x.Empty(f.srv.Get("edge-2.nodes.example.com.", dns.TypeANY))
	x.Equal([]string{"192.0.2.10"}, f.srv.Get("edge-1.nodes.example.com.", dns.TypeA))
	after, refused := f.srv.Counts()
	x.Equal(updates, after)
	x.Positive(refused)
	st := f.syncStatus()
	x.Contains(st.GetError(), "NOTAUTH")
	x.NotNil(st.GetDateFailed())
	x.NotContains(st.GetError(), string(wrong))
}

func TestDnsPolicyChecks(t *testing.T) {
	x := require.New(t)
	f := newDnsFixture(t)

	for i, c := range []struct {
		field string
		edit  func(*api.AddressParams)
	}{
		{"params.template", func(p *api.AddressParams) { p.SetTemplate("node.nodes.example.com") }},
		{"params.template", func(p *api.AddressParams) { p.SetTemplate("{alias}.nodes.example.org") }},
		{"params.template", func(p *api.AddressParams) { p.SetTemplate("{alias}.othernodes.example.com") }},
		{"params.template", func(p *api.AddressParams) { p.SetTemplate("{alias}.{zone}.nodes.example.com") }},
		{"params.dns_zone", func(p *api.AddressParams) { p.SetDnsZone("") }},
		{"params.dns_server", func(p *api.AddressParams) { p.SetDnsServer("192.0.2.1:dns") }},
		{"params.dns_tsig_name", func(p *api.AddressParams) { p.SetDnsTsigName("") }},
		{"params.dns_tsig_algorithm", func(p *api.AddressParams) { p.SetDnsTsigAlgorithm("hmac-md5") }},
		{"params.dns_networks", func(p *api.AddressParams) { p.SetDnsNetworks([]string{"192.0.2.0"}) }},
	} {
		p := f.params()
		c.edit(p)
		_, err := f.policy(int64(i+1), p, f.secret)
		x.Equal(codes.InvalidArgument, status.Code(err), c.field)
		x.ErrorContains(err, c.field)
	}
	_, err := f.policy(20, f.params(), nil)
	x.Equal(codes.InvalidArgument, status.Code(err), "no secret")
	x.ErrorContains(err, "dns_tsig_secret")

	// The secret is not a parameter: there, it would be answered with.
	p := f.params()
	p.SetDnsTsigSecret("c2VjcmV0")
	_, err = f.core.AddressPolicy().Add(f.ctx, api.AddressPolicyAddRequest_builder{Alias: "legacy", Version: 30, Params: p}.Build())
	x.Equal(codes.InvalidArgument, status.Code(err))

	// An active policy patched into one the leader could not use is refused.
	pol, err := f.policy(31, f.params(), f.secret)
	x.NoError(err)
	p = f.params()
	p.SetTemplate("{alias}.example.org")
	_, err = f.core.AddressPolicy().Patch(f.ctx, api.AddressPolicyPatchRequest_builder{
		Ref: api.AddressPolicyRef_builder{Id: pol.GetId()}.Build(), Params: p, DateUpdatedForce: z.Ptr(true),
	}.Build())
	x.Equal(codes.InvalidArgument, status.Code(err))
}

// The secret is sealed where it is kept and never answered with: not by
// a read, not by the write that set it.
func TestDnsSecretIsSealedAndNotAnswered(t *testing.T) {
	x := require.New(t)
	f := newDnsFixture(t)
	walled := pd.NewSecret(f.core)

	v, err := walled.AddressPolicy().Add(f.ctx, api.AddressPolicyAddRequest_builder{Alias: "dns", Version: 1, Params: f.params(), DnsTsigSecret: f.secret}.Build())
	x.NoError(err)
	x.Empty(v.GetDnsTsigSecret())
	ref := api.AddressPolicyRef_builder{Id: v.GetId()}.Build()
	v, err = walled.AddressPolicy().Activate(f.ctx, api.AddressPolicyActivateRequest_builder{Ref: ref}.Build())
	x.NoError(err)
	x.Empty(v.GetDnsTsigSecret())
	v, err = walled.AddressPolicy().Get(f.ctx, api.AddressPolicyGetRequest_builder{Ref: ref, Select: api.AddressPolicySelect_builder{All: z.Ptr(true)}.Build()}.Build())
	x.NoError(err)
	x.Empty(v.GetDnsTsigSecret())
	vs, err := walled.AddressPolicy().List(f.ctx, api.AddressPolicyListRequest_builder{}.Build())
	x.NoError(err)
	for _, v := range vs.GetItems() {
		x.Empty(v.GetDnsTsigSecret())
	}
	v, err = walled.AddressPolicy().Patch(f.ctx, api.AddressPolicyPatchRequest_builder{Ref: ref, Desc: z.Ptr("updates nodes.example.com"), DateUpdatedForce: z.Ptr(true)}.Build())
	x.NoError(err)
	x.Empty(v.GetDnsTsigSecret())

	row, err := f.ent.AddressPolicy.Query().Where(addresspolicy.Version(1)).Only(f.ctx)
	x.NoError(err)
	x.NotEmpty(row.DnsTsigSecret)
	x.False(bytes.Contains(row.DnsTsigSecret, f.secret), "sealed where it is kept")
	opened, err := f.deps.openTsig(row.DnsTsigSecret)
	x.NoError(err)
	x.Equal(f.secret, opened)

	// A row a version before wrote the secret into the parameters of
	// does not answer with it either.
	legacy := proto.CloneOf(row.Params)
	legacy.SetDnsTsigSecret("c2VjcmV0")
	x.NoError(f.ent.AddressPolicy.UpdateOneId(row.Id).SetParams(legacy).Exec(f.ctx))
	v, err = walled.AddressPolicy().Get(f.ctx, api.AddressPolicyGetRequest_builder{Ref: ref}.Build())
	x.NoError(err)
	x.Empty(v.GetParams().GetDnsTsigSecret())
}

func TestDnsAddrs(t *testing.T) {
	x := require.New(t)
	ifs := []*api.HostInterface{
		api.HostInterface_builder{Name: "lo", Addresses: []string{"127.0.0.1/8", "::1/128"}}.Build(),
		api.HostInterface_builder{Name: "eth0", Addresses: []string{"fe80::1/64", "10.0.0.5/24", "2001:db8::5/64", "203.0.113.5/24"}}.Build(),
		api.HostInterface_builder{Name: "docker0", Addresses: []string{"172.17.0.1/16"}}.Build(),
	}
	str := func(v []netip.Addr) []string {
		var out []string
		for _, a := range v {
			out = append(out, a.String())
		}
		return out
	}
	x.Equal([]string{"10.0.0.5", "2001:db8::5"}, str(dnsAddrs(ifs, ":7410", nil)))
	x.Equal([]string{"10.0.0.5", "2001:db8::5"}, str(dnsAddrs(ifs, "0.0.0.0:7410", nil)))
	x.Equal([]string{"10.0.0.5", "2001:db8::5"}, str(dnsAddrs(ifs, "storage.example.com:7410", nil)))
	x.Equal([]string{"203.0.113.5"}, str(dnsAddrs(ifs, "203.0.113.5:7410", nil)))
	x.Equal([]string{"203.0.113.5", "172.17.0.1"}, str(dnsAddrs(ifs, ":7410", []netip.Prefix{netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("172.16.0.0/12")})))
	x.Empty(dnsAddrs(nil, ":7410", nil))
}

// A host whose certificate lacks a name the active policy hands out is
// asked to renew it (§33.5), as it is once a dns or template policy is
// activated over certificates issued before.
func TestCertificateFollowsThePolicy(t *testing.T) {
	x := require.New(t)
	f := newDnsFixture(t)
	ca, err := pki.NewCA("test", time.Now())
	x.NoError(err)
	f.deps.CA = ca
	key, err := pki.NewHostKey()
	x.NoError(err)
	csr, err := key.CSR("edge-1")
	x.NoError(err)
	id := pdid.New(DomNode)
	n := NodeAddresses{Id: id, Alias: "edge-1"}
	issue := func(p *api.AddressParams) []byte {
		dns, ips := NamesFor(n, p)
		c, err := ca.IssueHost(csr, id, pki.Names{DNS: dns, IPs: ips}, time.Now())
		x.NoError(err)
		return pki.EncodeCerts(c)
	}

	before := issue(nil)
	x.False(f.core.outdated(f.ctx, n, before), "advertised names no host by a name of its own")

	_, err = f.policy(1, f.params(), f.secret)
	x.NoError(err)
	f.core.invalidatePolicies()
	x.True(f.core.outdated(f.ctx, n, before))
	x.False(f.core.outdated(f.ctx, n, issue(f.params())), "the renewal carries the policy's names")
}
