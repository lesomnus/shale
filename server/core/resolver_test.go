package core

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/placement"
)

// A caller on the CP's own host (loopback) gets loopback for the node that
// shares the host, and a routable address for every other node: the lab's
// CLI on the CP host was handed 127.0.0.1 for laminae on other nodes
// (§34.10).
func TestPickAddressLoopbackCaller(t *testing.T) {
	saved := hostIPs
	hostIPs = func() []net.IP { return []net.IP{net.ParseIP("10.1.2.73"), net.ParseIP("127.0.0.1")} }
	defer func() { hostIPs = saved }()

	ifs := func(addrs ...string) []*api.HostInterface {
		return []*api.HostInterface{api.HostInterface_builder{Name: "eth0", Addresses: addrs}.Build()}
	}
	local := ifs("10.1.2.73/24", "127.0.0.1/8")
	remote := ifs("10.1.2.75/24", "127.0.0.1/8")

	if got := pickAddress(local, "127.0.0.1", nil); got != "127.0.0.1" {
		t.Fatalf("local node for a loopback caller: %s", got)
	}
	if got := pickAddress(remote, "127.0.0.1", nil); got != "10.1.2.75" {
		t.Fatalf("remote node for a loopback caller: %s", got)
	}
	if got := pickAddress(remote, "10.1.2.63", nil); got != "10.1.2.75" {
		t.Fatalf("remote node for a LAN caller: %s", got)
	}
}

// `serve all` outside development mode (§34.7, §41.10): the node, the relay
// and the satellite share the CP's host and listen on every interface. The
// satellite calls the CP over loopback, so its routes to the node and the
// relay are loopback; a producer anywhere else is handed the satellite's
// routable address, and `satellite.advertise` wins over both (§34.10).
func TestServeAllEndpoints(t *testing.T) {
	saved := hostIPs
	hostIPs = func() []net.IP { return []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("10.1.2.73")} }
	defer func() { hostIPs = saved }()

	ifs := []*api.HostInterface{
		api.HostInterface_builder{Name: "lo", Addresses: []string{"127.0.0.1", "::1"}}.Build(),
		api.HostInterface_builder{Name: "eth0", Addresses: []string{"10.1.2.73"}}.Build(),
	}
	s := Core{d: &Deps{}}
	n := &ent.Node{Alias: "n1", Interfaces: ifs, DataAddress: "[::]:7410"}
	r := &ent.Relay{Alias: "r1", Interfaces: ifs, IngestAddress: "[::]:7430", WhepAddress: "[::]:7431"}
	sat := &ent.Satellite{Alias: "s1", Interfaces: ifs, UploadAddress: "[::]:7460"}

	// The satellite's own calls.
	require.Equal(t, "127.0.0.1", s.endpoints(n, "127.0.0.1", nil)[0].GetHost(), "its route to the node")
	require.Equal(t, "127.0.0.1", s.relayEndpoints(r, r.IngestAddress, "127.0.0.1", nil)[0].GetHost(), "its feed to the relay")
	// The producers'.
	for caller, want := range map[string]string{"127.0.0.1": "127.0.0.1", "10.1.2.63": "10.1.2.73", "203.0.113.9": "10.1.2.73"} {
		eps := s.satelliteEndpoints(sat, caller, nil)
		require.Len(t, eps, 1)
		require.Equal(t, want, eps[0].GetHost(), "a producer at %s", caller)
		require.Equal(t, int32(7460), eps[0].GetPort())
	}
	sat.UploadAddress = "192.0.2.10:7460"
	require.Equal(t, "192.0.2.10", s.satelliteEndpoints(sat, "203.0.113.9", nil)[0].GetHost())
}

// The forecast never leaves a set with nowhere to write: when it is the
// only reason every sink is out, those sinks come back (§11.1).
func TestReadmitForecast(t *testing.T) {
	a, b, c := pdid.New(14), pdid.New(14), pdid.New(14)
	sinks := []placement.Sink{{Id: a}, {Id: b}, {Id: c}}
	why := map[pdid.Id]string{a: forecastWhy, b: forecastWhy, c: "its node is down"}
	if !readmitForecast(sinks, why) {
		t.Fatal("nothing readmitted")
	}
	if !sinks[0].Eligible || !sinks[1].Eligible || sinks[2].Eligible {
		t.Fatalf("eligible: %v %v %v", sinks[0].Eligible, sinks[1].Eligible, sinks[2].Eligible)
	}
	if _, still := why[a]; still {
		t.Fatal("the reason stayed")
	}
	// One eligible sink: the forecast's exclusions stand.
	sinks = []placement.Sink{{Id: a, Eligible: true}, {Id: b}}
	why = map[pdid.Id]string{b: forecastWhy}
	if readmitForecast(sinks, why) || sinks[1].Eligible {
		t.Fatal("readmitted beside an eligible sink")
	}
}

// A name the relay reports for WHEP is handed out as it is, for the
// certificate a browser trusts (§39.4); an IP, or every interface, goes
// through the resolver as ingest does.
func TestWhepEndpoints(t *testing.T) {
	ifs := []*api.HostInterface{api.HostInterface_builder{Name: "eth0", Addresses: []string{"10.1.2.80/24"}}.Build()}
	r := &ent.Relay{Alias: "r1", Interfaces: ifs, IngestAddress: ":7440", WhepAddress: "live.example.com:443"}
	s := Core{d: &Deps{}}

	eps := s.whepEndpoints(r, "203.0.113.9", nil)
	require.Len(t, eps, 1)
	require.Equal(t, "https://live.example.com:443/whep/x", endpointURL(eps[0], "whep/x", nil))
	// Even under a template: the relay's own name wins.
	tmpl := api.AddressParams_builder{Resolver: "template", Template: "{alias}.nodes.example.com"}.Build()
	eps = s.whepEndpoints(r, "203.0.113.9", tmpl)
	require.Equal(t, "live.example.com", eps[0].GetHost())
	// Ingest is unchanged by it.
	eps = s.relayEndpoints(r, r.IngestAddress, "10.1.2.9", nil)
	require.Equal(t, "10.1.2.80", eps[0].GetHost())

	r.WhepAddress = ":7441"
	eps = s.whepEndpoints(r, "10.1.2.9", nil)
	require.Len(t, eps, 1)
	require.Equal(t, "10.1.2.80", eps[0].GetHost())
	require.Equal(t, int32(7441), eps[0].GetPort())
}

// A name a node reports for its data plane (`storage.advertise`) is handed
// out as it is, to producers, readers and the browser alike, for the
// certificate that names it (§34.10, §33.5); an IP, or every interface,
// goes through the resolver as before.
func TestNodeEndpointsByName(t *testing.T) {
	ifs := []*api.HostInterface{api.HostInterface_builder{Name: "eth0", Addresses: []string{"10.1.2.80/24"}}.Build()}
	n := &ent.Node{Alias: "n1", Interfaces: ifs, DataAddress: "storage.example.com:7420"}
	s := Core{d: &Deps{}}

	eps := s.endpoints(n, "203.0.113.9", nil)
	require.Len(t, eps, 1)
	require.Equal(t, "https://storage.example.com:7420/k/1", endpointURL(eps[0], "k/1", nil))
	// Even under a template: the node's own name wins.
	tmpl := api.AddressParams_builder{Resolver: "template", Template: "{alias}.nodes.example.com"}.Build()
	eps = s.endpoints(n, "203.0.113.9", tmpl)
	require.Equal(t, "storage.example.com", eps[0].GetHost())
	// Its host certificate names it too, for a client that trusts only
	// the shale CA.
	dns, _ := NamesFor(NodeAddresses{Alias: n.Alias, Interfaces: ifs, DataAddress: n.DataAddress}, nil)
	require.Contains(t, dns, "storage.example.com")
	// Plaintext in development mode, as every endpoint.
	require.Equal(t, "http", Core{d: &Deps{Dev: true}}.endpoints(n, "203.0.113.9", nil)[0].GetScheme())

	for _, c := range []struct{ addr, want string }{
		{":7420", "10.1.2.80"},
		{"[::]:7420", "10.1.2.80"},
		{"0.0.0.0:7420", "10.1.2.80"},
		{"10.9.9.9:7420", "10.9.9.9"},
	} {
		n.DataAddress = c.addr
		eps = s.endpoints(n, "10.1.2.9", nil)
		require.Len(t, eps, 1, c.addr)
		require.Equal(t, c.want, eps[0].GetHost(), c.addr)
		require.Equal(t, int32(7420), eps[0].GetPort(), c.addr)
	}
	// An IP under a template is the template's, as before.
	n.DataAddress = ":7420"
	require.Equal(t, "n1.nodes.example.com", s.endpoints(n, "10.1.2.9", tmpl)[0].GetHost())
}

// What an endpoint says it speaks (§34.10, §35.6): HTTP/2 over TLS, HTTP/1.1
// in development mode's plaintext, and the protocol a caller asks for by it
// on the same host and port, since HTTP/3 is served there too.
func TestEndpointProtocol(t *testing.T) {
	ifs := []*api.HostInterface{api.HostInterface_builder{Name: "eth0", Addresses: []string{"10.1.2.80/24"}}.Build()}
	n := &ent.Node{Alias: "n1", Interfaces: ifs, DataAddress: ":7420"}
	require.Equal(t, "h2", Core{d: &Deps{}}.endpoints(n, "10.1.2.9", nil)[0].GetProtocol())
	require.Equal(t, "h1", Core{d: &Deps{Dev: true}}.endpoints(n, "10.1.2.9", nil)[0].GetProtocol())

	for _, c := range []struct {
		dev         bool
		asked, want string
	}{
		{false, "", "h2"},
		{false, "h3", "h3"},
		{false, "h1", "h1"},
		{false, "quic", "h2"},
		{true, "h3", "h1"},
	} {
		eps := forProtocol(Core{d: &Deps{Dev: c.dev}}.endpoints(n, "10.1.2.9", nil), c.asked)
		require.Len(t, eps, 1)
		require.Equal(t, c.want, eps[0].GetProtocol(), c.asked)
		require.Equal(t, "10.1.2.80", eps[0].GetHost())
		require.Equal(t, int32(7420), eps[0].GetPort())
	}
}
