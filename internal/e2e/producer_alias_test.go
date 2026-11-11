package e2e_test

import (
	"testing"

	"github.com/lesomnus/payday/slug"
	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/pki"
)

func aliasHostJoin(t *testing.T, hardware, hostname string) *api.ProducerJoinRequest {
	t.Helper()
	key, err := pki.NewHostKey()
	require.NoError(t, err)
	csr, err := key.CSR(hostname)
	require.NoError(t, err)
	return api.ProducerJoinRequest_builder{Tenant: "acme", Host: api.HostJoin_builder{
		HardwareId: hardware, Hostname: hostname, Csr: csr, KeyFingerprint: key.Fingerprint(),
	}.Build()}.Build()
}

// Hostnames are inventory metadata. A producer's random server alias survives
// an OS rename, and adoption can still give it an operator-selected alias.
func TestProducerJoinAliases(t *testing.T) {
	c := start(t)
	ctx := t.Context()
	conn, err := grpc.NewClient(c.running.TenantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	joining := api.NewProducerServiceClient(conn)
	admin := c.dial("@acme/admin")
	producers := api.NewProducerServiceClient(admin)
	request := aliasHostJoin(t, "alias-hardware-a", "shared-camera-host")
	first, err := joining.Join(ctx, request)
	require.NoError(t, err)
	a := first.GetAnswer()
	require.Equal(t, api.HostState_HOST_STATE_PENDING, a.GetState())
	require.NotEmpty(t, a.GetAlias())
	_, err = slug.ParseAlias(a.GetAlias())
	require.NoError(t, err)
	require.NotEqual(t, request.GetHost().GetHostname(), a.GetAlias())
	require.Contains(t, a.GetMessage(), "shale producer adopt @acme/"+a.GetAlias()+" ")

	second, err := joining.Join(ctx, aliasHostJoin(t, "alias-hardware-b", request.GetHost().GetHostname()))
	require.NoError(t, err)
	require.NotEqual(t, a.GetId(), second.GetAnswer().GetId())
	require.NotEqual(t, a.GetAlias(), second.GetAnswer().GetAlias())
	ref := api.ProducerRef_builder{Id: a.GetId()}.Build()
	row, err := producers.Get(ctx, api.ProducerGetRequest_builder{Ref: ref}.Build())
	require.NoError(t, err)
	require.Equal(t, a.GetAlias(), row.GetAlias())
	require.Equal(t, "shared-camera-host", row.GetHostname())

	request.GetHost().SetHostname("shale-" + a.GetAlias())
	pending, err := joining.Join(ctx, request)
	require.NoError(t, err)
	require.Equal(t, a.GetId(), pending.GetAnswer().GetId())
	require.Equal(t, a.GetAlias(), pending.GetAnswer().GetAlias())
	row, err = producers.Get(ctx, api.ProducerGetRequest_builder{Ref: ref}.Build())
	require.NoError(t, err)
	require.Equal(t, request.GetHost().GetHostname(), row.GetHostname())

	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "alias-camera-set",
	}.Build())
	require.NoError(t, err)
	setRef := api.SetRef_builder{Id: set.GetId()}.Build()
	adopted, err := producers.Adopt(ctx, api.ProducerAdoptRequest_builder{Ref: ref, Set: setRef, Alias: "chosen-camera"}.Build())
	require.NoError(t, err)
	require.Equal(t, "chosen-camera", adopted.GetAlias(), "an explicit adoption alias still wins")
	issued, err := joining.Join(ctx, request)
	require.NoError(t, err)
	require.Equal(t, a.GetId(), issued.GetAnswer().GetId())
	require.Equal(t, "chosen-camera", issued.GetAnswer().GetAlias())
	require.Equal(t, api.HostState_HOST_STATE_ADOPTED, issued.GetAnswer().GetState())
	require.NotEmpty(t, issued.GetAnswer().GetCertificate())

	// A different key for the same hardware requests re-adoption, preserving
	// its row and alias rather than creating a new random identity.
	rejoining := aliasHostJoin(t, "alias-hardware-a", "another-os-name")
	rejoin, err := joining.Join(ctx, rejoining)
	require.NoError(t, err)
	require.True(t, rejoin.GetAnswer().GetRejoin())
	require.Equal(t, a.GetId(), rejoin.GetAnswer().GetId())
	require.Equal(t, "chosen-camera", rejoin.GetAnswer().GetAlias())
	readopted, err := producers.Adopt(ctx, api.ProducerAdoptRequest_builder{Ref: ref, Set: setRef}.Build())
	require.NoError(t, err)
	require.Equal(t, "chosen-camera", readopted.GetAlias(), "omitting Alias preserves the existing alias")
	issued, err = joining.Join(ctx, rejoining)
	require.NoError(t, err)
	require.Equal(t, a.GetId(), issued.GetAnswer().GetId())
	require.Equal(t, "chosen-camera", issued.GetAnswer().GetAlias())
	require.Equal(t, api.HostState_HOST_STATE_ADOPTED, issued.GetAnswer().GetState())
}
