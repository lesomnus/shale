package core

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/payday/config"
	_ "github.com/lesomnus/payday/config/dbsqlite3"
	"github.com/lesomnus/payday/frame"
	"github.com/lesomnus/payday/pdid"
	"github.com/protobuf-orm/ent/dialect/sql"
	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/enttest"
	"github.com/lesomnus/shale/internal/ent/sitemember"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/server/bare"
	"github.com/lesomnus/shale/server/pd"
)

type readerFixture struct {
	core   Core
	sink   pd.Sink
	client *ent.Client
	tenant *api.Tenant
	ctx    context.Context
}

func newReaderFixture(t *testing.T) readerFixture {
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
	ca, err := pki.NewCA("shale", time.Now())
	require.NoError(t, err)
	core := New(sink, &Deps{Ent: client, Own: sink, Drv: driver, CA: ca})
	admin := frame.New(pdid.New(DomHolder), mustId(tenant.GetId()), frame.Whole())

	return readerFixture{core: core, sink: sink, client: client, tenant: tenant, ctx: frame.Into(ctx, admin)}
}

// join asks for adoption with a fresh key, as a reader does the first time
// and again once its certificate has expired (§33.4).
func (f readerFixture) join(t *testing.T) *api.JoinAnswer {
	t.Helper()
	key, err := pki.NewHostKey()
	require.NoError(t, err)
	csr, err := key.CSR("viewer")
	require.NoError(t, err)
	out, err := f.core.Reader().Join(f.ctx, api.ReaderJoinRequest_builder{Tenant: "acme", Host: api.HostJoin_builder{
		HardwareId: "viewer-hardware", Hostname: "viewer", Csr: csr, KeyFingerprint: key.Fingerprint(),
	}.Build()}.Build())
	require.NoError(t, err)

	return out.GetAnswer()
}

func (f readerFixture) site(t *testing.T, alias string) *api.SiteRef {
	t.Helper()
	v, err := f.sink.Site().Add(f.ctx, api.SiteAddRequest_builder{Tenant: api.TenantRef_builder{Id: f.tenant.GetId()}.Build(), Alias: alias}.Build())
	require.NoError(t, err)

	return api.SiteRef_builder{Id: v.GetId()}.Build()
}

func (f readerFixture) adopt(t *testing.T, id []byte, sites ...*api.SiteRef) *api.Reader {
	t.Helper()
	v, err := f.core.Reader().Adopt(f.ctx, api.ReaderAdoptRequest_builder{Ref: api.ReaderRef_builder{Id: id}.Build(), Sites: sites}.Build())
	require.NoError(t, err)

	return v
}

// sites are the sites a reader is a member of.
func (f readerFixture) sites(t *testing.T, id []byte) []pdid.Id {
	t.Helper()
	ms, err := f.client.SiteMember.Query().Where(sitemember.ReaderIdEQ(mustId(id).Uuid())).All(f.ctx)
	require.NoError(t, err)
	var out []pdid.Id
	for _, m := range ms {
		out = append(out, pdid.Id(m.SiteId))
	}

	return out
}

// The first adoption says what a reader may read: sites, or none for the
// whole tenant. Adopting it again under a new key puts it back as it was
// unless sites are named, which then replace its own: taking a reader back
// never widens what it reads (§33.4).
func TestReaderReadoptionKeepsItsSites(t *testing.T) {
	f := newReaderFixture(t)
	north, south := f.site(t, "north"), f.site(t, "south")

	first := f.join(t)
	require.Equal(t, api.HostState_HOST_STATE_PENDING, first.GetState())
	require.Equal(t, "waiting for adoption: shale reader adopt @acme/"+first.GetAlias(), first.GetMessage())
	r := f.adopt(t, first.GetId(), north)
	require.False(t, r.GetAllSites())
	require.Equal(t, []pdid.Id{mustId(north.GetId())}, f.sites(t, first.GetId()))
	serial := r.GetCertSerial()

	// Its certificate expired: a new key, "known host, new key".
	again := f.join(t)
	require.Equal(t, first.GetId(), again.GetId(), "the same row")
	require.True(t, again.GetRejoin())
	require.Equal(t, api.HostState_HOST_STATE_PENDING, again.GetState())
	r = f.adopt(t, again.GetId())
	require.False(t, r.GetAllSites(), "no sites named on a re-adoption is the sites it had")
	require.Equal(t, []pdid.Id{mustId(north.GetId())}, f.sites(t, first.GetId()))
	require.NotEqual(t, serial, r.GetCertSerial())

	// Named on a re-adoption, sites replace what it had.
	f.join(t)
	r = f.adopt(t, first.GetId(), south)
	require.False(t, r.GetAllSites())
	require.Equal(t, []pdid.Id{mustId(south.GetId())}, f.sites(t, first.GetId()))

	// One adopted for the whole tenant stays so.
	_, err := f.sink.Reader().Erase(f.ctx, api.ReaderRef_builder{Id: first.GetId()}.Build())
	require.NoError(t, err)
	whole := f.join(t)
	require.NotEqual(t, first.GetId(), whole.GetId(), "an erased reader joins as a new one")
	r = f.adopt(t, whole.GetId())
	require.True(t, r.GetAllSites(), "no sites on a first adoption is the whole tenant")
	f.join(t)
	r = f.adopt(t, whole.GetId())
	require.True(t, r.GetAllSites())
	require.Empty(t, f.sites(t, whole.GetId()))
}
