package dnsupdate_test

import (
	"crypto/rand"
	"net/netip"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/internal/dnsupdate"
	"github.com/lesomnus/shale/internal/dnsupdate/dnsupdatetest"
)

const marker = "heritage=shale,shale/owner=test"

func serve(t *testing.T) (*dnsupdatetest.Server, []byte) {
	t.Helper()
	secret := make([]byte, 32)
	rand.Read(secret)
	srv, err := dnsupdatetest.Start("nodes.example.com", "shale", secret)
	require.NoError(t, err)
	t.Cleanup(func() { srv.Close() })

	return srv, secret
}

func client(t *testing.T, srv *dnsupdatetest.Server, secret []byte) *dnsupdate.Client {
	t.Helper()
	c, err := dnsupdate.New(dnsupdate.Config{Server: srv.Addr(), Zone: "Nodes.Example.com", KeyName: "shale", Secret: secret})
	require.NoError(t, err)

	return c
}

func addrs(v ...string) []netip.Addr {
	var out []netip.Addr
	for _, s := range v {
		out = append(out, netip.MustParseAddr(s))
	}

	return out
}

func TestCreateReplaceDelete(t *testing.T) {
	x := require.New(t)
	ctx := t.Context()
	srv, secret := serve(t)
	c := client(t, srv, secret)
	x.NoError(c.Probe(ctx))

	name := "edge-1.nodes.example.com."
	x.NoError(c.Create(ctx, name, dnsupdate.Want{Addrs: addrs("2001:db8::10", "192.0.2.10"), TTL: 60}, marker))
	x.Equal([]string{"192.0.2.10"}, srv.Get(name, dns.TypeA))
	x.Equal([]string{"2001:db8::10"}, srv.Get(name, dns.TypeAAAA))
	x.Equal([]string{`"` + marker + `"`}, srv.Get(name, dns.TypeTXT))
	x.EqualValues(60, srv.TTL(name, dns.TypeA))

	have, err := c.Lookup(ctx, name)
	x.NoError(err)
	x.True(have.OwnedBy(marker))
	x.True(have.Holds(dnsupdate.Want{Addrs: addrs("192.0.2.10", "2001:db8::10"), TTL: 60}))
	x.False(have.Holds(dnsupdate.Want{Addrs: addrs("192.0.2.10", "2001:db8::10"), TTL: 30}))

	// A record somebody keeps beside Shale's at the same name stays.
	x.NoError(srv.Add(name + " 300 IN MX 10 mail.example.com."))
	have, err = c.Lookup(ctx, name)
	x.NoError(err)
	x.NoError(c.Replace(ctx, name, have, dnsupdate.Want{Addrs: addrs("192.0.2.11"), TTL: 60}, marker))
	x.Equal([]string{"192.0.2.11"}, srv.Get(name, dns.TypeA))
	x.Empty(srv.Get(name, dns.TypeAAAA))

	zone, err := c.Transfer(ctx)
	x.NoError(err)
	x.Equal(addrs("192.0.2.11"), zone[name].Addrs)
	x.True(zone[name].OwnedBy(marker))
	x.True(zone[name].Other, "the MX")

	x.NoError(c.Delete(ctx, name, zone[name], marker))
	x.Empty(srv.Get(name, dns.TypeA))
	x.Empty(srv.Get(name, dns.TypeTXT))
	x.Equal([]string{"10 mail.example.com."}, srv.Get(name, dns.TypeMX))
}

// The conditions are the server's to check: a name somebody holds is not
// created over, and a name whose TXT records changed since they were read
// is neither replaced nor removed.
func TestPrerequisites(t *testing.T) {
	x := require.New(t)
	ctx := t.Context()
	srv, secret := serve(t)
	c := client(t, srv, secret)

	x.NoError(srv.Add("www.nodes.example.com. 300 IN A 192.0.2.80"))
	err := c.Create(ctx, "www.nodes.example.com.", dnsupdate.Want{Addrs: addrs("192.0.2.10"), TTL: 60}, marker)
	x.ErrorIs(err, dnsupdate.ErrPrerequisite)
	x.Equal([]string{"192.0.2.80"}, srv.Get("www.nodes.example.com.", dns.TypeA))

	name := "edge-1.nodes.example.com."
	x.NoError(c.Create(ctx, name, dnsupdate.Want{Addrs: addrs("192.0.2.10"), TTL: 60}, marker))
	have, err := c.Lookup(ctx, name)
	x.NoError(err)
	x.NoError(srv.Add(name + ` 60 IN TXT "somebody else"`))
	x.ErrorIs(c.Replace(ctx, name, have, dnsupdate.Want{Addrs: addrs("192.0.2.11"), TTL: 60}, marker), dnsupdate.ErrPrerequisite)
	x.ErrorIs(c.Delete(ctx, name, have, marker), dnsupdate.ErrPrerequisite)
	x.Equal([]string{"192.0.2.10"}, srv.Get(name, dns.TypeA))

	// And a name without the marker is not Shale's to change at all.
	www, err := c.Lookup(ctx, "www.nodes.example.com.")
	x.NoError(err)
	x.ErrorIs(c.Delete(ctx, "www.nodes.example.com.", www, marker), dnsupdate.ErrPrerequisite)
}

func TestBadKeyIsRefused(t *testing.T) {
	x := require.New(t)
	ctx := t.Context()
	srv, secret := serve(t)

	wrong := make([]byte, len(secret))
	rand.Read(wrong)
	for _, cfg := range []dnsupdate.Config{
		{Server: srv.Addr(), Zone: "nodes.example.com", KeyName: "shale", Secret: wrong},
		{Server: srv.Addr(), Zone: "nodes.example.com", KeyName: "other", Secret: secret},
	} {
		c, err := dnsupdate.New(cfg)
		x.NoError(err)
		x.ErrorIs(c.Probe(ctx), dnsupdate.ErrRefused)
		x.ErrorIs(c.Create(ctx, "edge-1.nodes.example.com.", dnsupdate.Want{Addrs: addrs("192.0.2.10"), TTL: 60}, marker), dnsupdate.ErrRefused)
		_, err = c.Transfer(ctx)
		x.Error(err)
	}
	x.Empty(srv.Names())
	updates, refused := srv.Counts()
	x.Zero(updates)
	x.Positive(refused)
}

func TestConfig(t *testing.T) {
	x := require.New(t)
	for in, want := range map[string]string{
		"192.0.2.1":          "192.0.2.1:53",
		"192.0.2.1:5353":     "192.0.2.1:5353",
		"2001:db8::1":        "[2001:db8::1]:53",
		"[2001:db8::1]:53":   "[2001:db8::1]:53",
		"ns1.example.com":    "ns1.example.com:53",
		"ns1.example.com:54": "ns1.example.com:54",
	} {
		got, err := dnsupdate.ServerAddr(in)
		x.NoError(err, in)
		x.Equal(want, got, in)
	}
	for _, in := range []string{"", "192.0.2.1:0", "192.0.2.1:http", "ns1 example.com", "a/b:53"} {
		_, err := dnsupdate.ServerAddr(in)
		x.Error(err, in)
	}

	ok := dnsupdate.Config{Server: "192.0.2.1", Zone: "nodes.example.com", KeyName: "shale", Secret: []byte("k")}
	x.NoError(ok.Check())
	for _, bad := range []func(*dnsupdate.Config){
		func(c *dnsupdate.Config) { c.Zone = "" },
		func(c *dnsupdate.Config) { c.Zone = "{alias}.example.com" },
		func(c *dnsupdate.Config) { c.KeyName = "" },
		func(c *dnsupdate.Config) { c.Algorithm = "hmac-md5" },
		func(c *dnsupdate.Config) { c.Secret = nil },
	} {
		c := ok
		bad(&c)
		x.Error(c.Check())
	}

	x.True(dnsupdate.InZone("a.nodes.example.com", "nodes.example.com."))
	x.False(dnsupdate.InZone("nodes.example.com", "nodes.example.com."))
	x.False(dnsupdate.InZone("a.example.com", "nodes.example.com."))
	x.False(dnsupdate.InZone("a.othernodes.example.com", "nodes.example.com."))
}
