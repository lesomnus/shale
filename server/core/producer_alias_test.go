package core

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/lesomnus/payday/config"
	_ "github.com/lesomnus/payday/config/dbsqlite3"
	"github.com/lesomnus/payday/slug"
	"github.com/protobuf-orm/ent/dialect/sql"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/enttest"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/bare"
	"github.com/lesomnus/shale/server/pd"
)

func producerAliasFixture(t *testing.T) (Core, pd.Sink, *api.Tenant) {
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
	tenant, err := sink.Tenant().Add(ctx, api.TenantAddRequest_builder{Alias: "acme"}.Build())
	require.NoError(t, err)
	return New(sink, &Deps{Ent: client, Own: sink}), sink, tenant
}

func producerAliasJoin(t *testing.T, hardware, hostname string) *api.ProducerJoinRequest {
	t.Helper()
	key, err := pki.NewHostKey()
	require.NoError(t, err)
	csr, err := key.CSR(hostname)
	require.NoError(t, err)
	return api.ProducerJoinRequest_builder{Tenant: "acme", Host: api.HostJoin_builder{
		HardwareId: hardware, Hostname: hostname, Csr: csr, KeyFingerprint: key.Fingerprint(),
	}.Build()}.Build()
}

func TestProducerJoinUsesServerNamer(t *testing.T) {
	for _, collide := range []bool{false, true} {
		t.Run(map[bool]string{false: "default generation", true: "collision retry"}[collide], func(t *testing.T) {
			core, sink, tenant := producerAliasFixture(t)
			if collide {
				_, err := sink.Producer().Add(t.Context(), api.ProducerAddRequest_builder{
					Tenant: api.TenantRef_builder{Id: tenant.GetId()}.Build(), Alias: "already-taken", HardwareId: "existing-hardware",
				}.Build())
				require.NoError(t, err)
			}
			calls := 0
			generated := ""
			named := sink.WithNamer(slug.NamerFunc(func(ctx context.Context, entity, given string, req proto.Message) (string, error) {
				require.Equal(t, "shale.Producer", entity)
				require.Empty(t, given, "Join must delegate naming instead of passing its hostname")
				add, ok := req.(*api.ProducerAddRequest)
				require.True(t, ok)
				require.Equal(t, "shared-camera-host", add.GetHostname())
				calls++
				if collide && calls == 1 {
					return "already-taken", nil
				}
				var err error
				generated, err = slug.Names().Name(ctx, entity, given, req)
				return generated, err
			}))
			core.d.Own = named
			out, err := core.Producer().Join(t.Context(), producerAliasJoin(t, "new-hardware", "shared-camera-host"))
			require.NoError(t, err)
			wantCalls := 1
			if collide {
				wantCalls++
			}
			require.Equal(t, wantCalls, calls)
			require.NotEmpty(t, generated)
			require.Equal(t, generated, out.GetAnswer().GetAlias())
			row, err := sink.Producer().Get(t.Context(), api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: out.GetAnswer().GetId()}.Build()}.Build())
			require.NoError(t, err)
			require.Equal(t, generated, row.GetAlias())
			require.Equal(t, `waiting for adoption: shale producer adopt @acme/`+generated+` '{"set":{"id":"<set>"}}'`, out.GetAnswer().GetMessage())
		})
	}
}

func TestProducerPendingJoinKeepsLegacyAlias(t *testing.T) {
	core, sink, _ := producerAliasFixture(t)
	req := producerAliasJoin(t, "old-hardware", "old-camera-host")
	out, err := core.Producer().Join(t.Context(), req)
	require.NoError(t, err)
	id := mustId(out.GetAnswer().GetId())
	_, err = core.d.Ent.Producer.UpdateOneId(id.Uuid()).SetAlias("old-camera-host").Save(t.Context())
	require.NoError(t, err)
	req.GetHost().SetHostname("renamed-os-host")
	again, err := core.Producer().Join(t.Context(), req)
	require.NoError(t, err)
	require.Equal(t, out.GetAnswer().GetId(), again.GetAnswer().GetId())
	require.Equal(t, "old-camera-host", again.GetAnswer().GetAlias())
	row, err := sink.Producer().Get(t.Context(), api.ProducerGetRequest_builder{Ref: api.ProducerRef_builder{Id: id.Bytes()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, "old-camera-host", row.GetAlias())
	require.Equal(t, "renamed-os-host", row.GetHostname())
}
