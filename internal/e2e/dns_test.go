package e2e_test

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/dnsupdate/dnsupdatetest"
)

// The `dns` resolver (§34.10): an operator writes a policy with a TSIG
// key, activates it, and the leader puts the built-in node's name in the
// zone; the node is handed out by that name, its certificate comes to
// carry it, and the secret is never answered with.
func TestDnsResolver(t *testing.T) {
	x := require.New(t)
	ctx := t.Context()
	c := start(t)

	secret := make([]byte, 32)
	rand.Read(secret)
	srv, err := dnsupdatetest.Start("nodes.example.com", "shale-update", secret)
	x.NoError(err)
	t.Cleanup(func() { srv.Close() })

	conn := c.dialCluster(c.operator())
	pols := api.NewAddressPolicyServiceClient(conn)
	added, err := pols.Add(ctx, api.AddressPolicyAddRequest_builder{
		Alias: "dns", Version: 1, DnsTsigSecret: secret,
		Params: api.AddressParams_builder{
			Resolver: "dns", Template: "{alias}.nodes.example.com",
			DnsServer: srv.Addr(), DnsZone: "nodes.example.com", DnsTsigName: "shale-update",
		}.Build(),
	}.Build())
	x.NoError(err)
	x.Empty(added.GetDnsTsigSecret())
	ref := api.AddressPolicyRef_builder{Id: added.GetId()}.Build()
	activated, err := pols.Activate(ctx, api.AddressPolicyActivateRequest_builder{Ref: ref}.Build())
	x.NoError(err)
	x.Empty(activated.GetDnsTsigSecret())

	nodes := api.NewNodeServiceClient(conn)
	vs, err := nodes.List(ctx, api.NodeListRequest_builder{}.Build())
	x.NoError(err)
	x.Len(vs.GetItems(), 1)
	n := vs.GetItems()[0]
	name := n.GetAlias() + ".nodes.example.com"

	x.Eventually(func() bool {
		return slices.Equal(srv.Get(name, dns.TypeA), []string{"127.0.0.1"})
	}, 20*time.Second, 100*time.Millisecond, "the leader writes the node's name")
	x.Eventually(func() bool {
		v, err := pols.Get(ctx, api.AddressPolicyGetRequest_builder{Ref: ref}.Build())
		return err == nil && v.GetDnsSync().GetNames() >= 1 && v.GetDnsSync().GetError() == ""
	}, 20*time.Second, 100*time.Millisecond, "and says so on the policy")

	v, err := pols.Get(ctx, api.AddressPolicyGetRequest_builder{Ref: ref, Select: api.AddressPolicySelect_builder{All: z.Ptr(true)}.Build()}.Build())
	x.NoError(err)
	x.Empty(v.GetDnsTsigSecret())
	all, err := pols.List(ctx, api.AddressPolicyListRequest_builder{}.Build())
	x.NoError(err)
	for _, v := range all.GetItems() {
		x.Empty(v.GetDnsTsigSecret())
	}
	err = conn.Invoke(ctx, "/shale.AddressPolicyService/Apply", &emptypb.Empty{}, &emptypb.Empty{})
	x.Equal(codes.Unimplemented, status.Code(err), "there is no patch document to store the secret unsealed")

	res, err := nodes.Resolve(ctx, api.NodeResolveRequest_builder{Ref: api.NodeRef_builder{Id: n.GetId()}.Build(), From: "203.0.113.9"}.Build())
	x.NoError(err)
	x.Len(res.GetEndpoints(), 1)
	x.Equal(name, res.GetEndpoints()[0].GetHost())

	// The node's certificate was issued before the policy: its heartbeat
	// answer asks for a renewal, which carries the name.
	x.Eventually(func() bool {
		v, err := nodes.Get(ctx, api.NodeGetRequest_builder{Ref: api.NodeRef_builder{Id: n.GetId()}.Build()}.Build())
		if err != nil {
			return false
		}
		b, _ := pem.Decode(v.GetJoin().GetCertificate())
		if b == nil {
			return false
		}
		cert, err := x509.ParseCertificate(b.Bytes)
		return err == nil && slices.ContainsFunc(cert.DNSNames, func(v string) bool { return strings.EqualFold(v, name) })
	}, 30*time.Second, 200*time.Millisecond, "the node's certificate carries its name")
}
