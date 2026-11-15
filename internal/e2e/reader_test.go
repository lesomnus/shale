package e2e_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/ent"
	entreader "github.com/lesomnus/shale/internal/ent/reader"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/reader"
)

// The Reader agent (§33.4) against a tenant control plane with real TLS:
// the development harness with its CP certificate, so certificates and
// their serials are what decides, as in production.

// logs is what an agent wrote, for the lines an operator reads.
type logs struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.b.Write(p)
}

func (l *logs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.b.String()
}

// clock is the agent's clock, which a test moves forward.
type clock struct {
	mu  sync.Mutex
	off time.Duration
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return time.Now().Add(c.off)
}

func (c *clock) Add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.off += d
}

// readerAgent is one `shale serve reader` in this process.
type readerAgent struct {
	t      *testing.T
	cfg    reader.Config
	log    *logs
	clock  *clock
	cancel context.CancelFunc
	done   chan error
}

// newReaderAgent maps a reader's configuration the way `serve reader`
// does, onto a state directory of its own.
func newReaderAgent(t *testing.T, c cmd.Config) *readerAgent {
	t.Helper()
	cfg, err := cli.ReaderConfig(&c)
	require.NoError(t, err)
	a := &readerAgent{t: t, cfg: cfg, log: &logs{}, clock: &clock{}}
	a.cfg.HardwareId = "viewer-machine"
	a.cfg.HeartbeatInterval = 100 * time.Millisecond
	a.cfg.RetryInterval = 100 * time.Millisecond
	a.cfg.Log = slog.New(slog.NewTextHandler(a.log, nil))
	a.cfg.Now = a.clock.Now
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("the reader agent's log:\n%s", a.log)
		}
	})

	return a
}

func (a *readerAgent) start() {
	a.t.Helper()
	r, err := reader.New(a.cfg)
	require.NoError(a.t, err)
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel, a.done = cancel, make(chan error, 1)
	go func() { a.done <- r.Run(ctx) }()
	a.t.Cleanup(a.stop)
}

func (a *readerAgent) stop() {
	if a.cancel == nil {
		return
	}
	a.cancel()
	select {
	case err := <-a.done:
		require.NoError(a.t, err)
	case <-time.After(10 * time.Second):
		a.t.Error("the reader agent did not stop")
	}
	a.cancel = nil
}

func (a *readerAgent) store() pki.Store { return pki.Store{Dir: a.cfg.StateDir} }

// serial is the serial of the certificate in the state directory, "" when
// there is none.
func (a *readerAgent) serial() string {
	chain, err := a.store().Cert()
	if err != nil || len(chain) == 0 {
		return ""
	}

	return pki.Serial(chain[0])
}

// conn is what a viewer beside the agent does: the reader's certificate
// files, read as they are now, and the CA bundle, against the tenant API's
// gRPC listener.
func (a *readerAgent) conn(cp string) *grpc.ClientConn {
	a.t.Helper()
	st := a.store()
	pair, err := tls.LoadX509KeyPair(st.CertPath(), st.KeyPath())
	require.NoError(a.t, err)
	bundle, err := os.ReadFile(st.BundlePath())
	require.NoError(a.t, err)
	pool := x509.NewCertPool()
	require.True(a.t, pool.AppendCertsFromPEM(bundle))
	conn, err := grpc.NewClient(strings.TrimPrefix(cp, "https://"), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{pair}, RootCAs: pool, MinVersion: tls.VersionTLS12,
	})))
	require.NoError(a.t, err)
	a.t.Cleanup(func() { conn.Close() })

	return conn
}

// readerRow is the live row of the machine, nil while there is none.
func readerRow(t *testing.T, s *cmd.Server) *ent.Reader {
	t.Helper()
	v, err := s.Deps.Ent.Reader.Query().Where(entreader.HardwareIdEQ("viewer-machine"), entreader.DateErasedIsNil()).Only(context.Background())
	if err != nil {
		if !ent.IsNotFound(err) {
			t.Logf("reader row: %v", err)
		}
		return nil
	}

	return v
}

// pendingReader waits for the machine to be waiting for adoption with the
// key `fingerprint`.
func pendingReader(t *testing.T, s *cmd.Server, fingerprint string) *ent.Reader {
	t.Helper()
	var row *ent.Reader
	require.Eventually(t, func() bool {
		row = readerRow(t, s)
		return row != nil && row.Join != nil && row.Join.GetKeyFingerprint() == fingerprint && len(row.Join.GetCertificate()) == 0
	}, 10*time.Second, 50*time.Millisecond, "the reader joins and waits")

	return row
}

// key is the fingerprint of the key in the agent's state directory, ""
// before there is one: read, never made here, which would race the agent
// making its own.
func (a *readerAgent) key() string {
	b, err := os.ReadFile(a.store().KeyPath())
	if err != nil {
		return ""
	}
	k, err := pki.ParseKey(b)
	if err != nil {
		return ""
	}

	return (&pki.HostKey{Key: k}).Fingerprint()
}

// keyOtherThan waits for the agent to hold a key that is not `prev`.
func (a *readerAgent) keyOtherThan(prev string) string {
	a.t.Helper()
	var v string
	require.Eventually(a.t, func() bool {
		v = a.key()
		return v != "" && v != prev
	}, 10*time.Second, 20*time.Millisecond, "the agent's key")

	return v
}

// sources lists the aliases of the cameras a connection may see.
func sources(t *testing.T, ctx context.Context, conn *grpc.ClientConn) []string {
	t.Helper()
	vs, err := api.NewSourceServiceClient(conn).List(ctx, api.SourceListRequest_builder{Size: 100}.Build())
	require.NoError(t, err)
	var out []string
	for _, v := range vs.GetItems() {
		out = append(out, v.GetAlias())
	}
	sort.Strings(out)

	return out
}

// readerSites is a tenant with a camera in a site, one in another, and one
// in a set of no site at all.
func readerSites(t *testing.T, ctx context.Context, admin *grpc.ClientConn) (north *api.SiteRef, northSet *api.Set, looseSet *api.Set) {
	t.Helper()
	north = cameraPolicySite(t, ctx, admin, "north")
	south := cameraPolicySite(t, ctx, admin, "south")
	northSet = cameraPolicySet(t, ctx, admin, "north-set", north)
	southSet := cameraPolicySet(t, ctx, admin, "south-set", south)
	looseSet = cameraPolicySet(t, ctx, admin, "loose-set", nil)
	for alias, set := range map[string]*api.Set{"door": northSet, "gate": southSet, "yard": looseSet} {
		_, err := api.NewSourceServiceClient(admin).Add(ctx, api.SourceAddRequest_builder{
			Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build(), Alias: alias,
		}.Build())
		require.NoError(t, err)
	}

	return north, northSet, looseSet
}

// pending → adopted → heartbeat → renewed, by the CP's asking and by the
// reader's own clock; reading with the certificate within the sites it was
// adopted for; refused once erased, and back as a new reader that reads the
// whole tenant, a set of no site included.
func TestReaderAgentLifecycle(t *testing.T) {
	s, cp, admin := cameraTLSControl(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	north, northSet, looseSet := readerSites(t, ctx, admin)

	// The CP's CA named explicitly (`reader.cp_ca`), as a public CA would
	// be: verified by name, nothing pinned on first contact.
	caFile := filepath.Join(t.TempDir(), "cp-ca.crt")
	require.NoError(t, os.WriteFile(caFile, s.CA.Bundle(), 0o644))
	c := cmd.Config{State: t.TempDir(), Cp: cp, Tenant: "acme"}
	c.Reader.CpCa = caFile
	agent := newReaderAgent(t, c)
	agent.start()

	// Pending: the agent says what the operator checks.
	key := agent.keyOtherThan("")
	row := pendingReader(t, s, key)
	require.Equal(t, api.HostState_HOST_STATE_PENDING, api.HostState(row.State))
	require.Eventually(t, func() bool {
		l := agent.log.String()
		return strings.Contains(l, "waiting for adoption") && strings.Contains(l, row.Alias) && strings.Contains(l, key) &&
			strings.Contains(l, "shale reader adopt @acme/"+row.Alias)
	}, 5*time.Second, 50*time.Millisecond, "alias, fingerprint and the command, in the log")
	require.Empty(t, agent.serial(), "no certificate before adoption")
	require.NoFileExists(t, agent.store().BundlePath(), "the CP's CA is named, so nothing was pinned")

	// Adopted for one site.
	id := pdid.Id(row.Id)
	_, err := api.NewReaderServiceClient(admin).Adopt(ctx, api.ReaderAdoptRequest_builder{Ref: api.ReaderRef_builder{Id: id.Bytes()}.Build(), Sites: []*api.SiteRef{north}}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		row = readerRow(t, s)
		return agent.serial() != "" && agent.serial() == row.CertSerial && row.DateSeen != nil
	}, 10*time.Second, 50*time.Millisecond, "the agent holds the issued certificate and heartbeats with it")
	first := agent.serial()
	leaf, err := agent.store().Cert()
	require.NoError(t, err)
	got, ok := pki.IdOf(leaf[0])
	require.True(t, ok)
	require.Equal(t, id, got)

	// A viewer reads with the reader's certificate, within its site.
	viewer := agent.conn(cp)
	require.Equal(t, []string{"door"}, sources(t, ctx, viewer))
	from, to := timestamppb.New(time.Now().Add(-time.Hour)), timestamppb.Now()
	_, err = api.NewLaminaServiceClient(viewer).Timeline(ctx, api.LaminaTimelineRequest_builder{Set: api.SetRef_builder{Id: northSet.GetId()}.Build(), From: from, To: to}.Build())
	require.NoError(t, err)
	_, err = api.NewLaminaServiceClient(viewer).Timeline(ctx, api.LaminaTimelineRequest_builder{Set: api.SetRef_builder{Id: looseSet.GetId()}.Build(), From: from, To: to}.Build())
	require.Error(t, err, "a set of no site is not in a site-scoped reader's sites")
	_, err = api.NewSourceServiceClient(viewer).Add(ctx, api.SourceAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Set: api.SetRef_builder{Id: northSet.GetId()}.Build(), Alias: "intruder",
	}.Build())
	require.Equal(t, codes.PermissionDenied, status.Code(err), "a reader reads")

	// The CP asks for renewal: its row says the certificate is two thirds
	// through its life.
	now := time.Now()
	_, err = s.Deps.Ent.Reader.UpdateOneId(id.Uuid()).SetDateAdopted(now.Add(-89 * 24 * time.Hour)).SetDateCertExpires(now.Add(24 * time.Hour)).Save(ctx)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		row = readerRow(t, s)
		return agent.serial() != first && agent.serial() == row.CertSerial
	}, 10*time.Second, 50*time.Millisecond, "renewed when the CP asks")
	second := agent.serial()
	_, err = api.NewSourceServiceClient(viewer).List(ctx, api.SourceListRequest_builder{}.Build())
	require.Equal(t, codes.Unauthenticated, status.Code(err), "the old serial is refused, on a connection already open too")
	viewer = agent.conn(cp)
	require.Equal(t, []string{"door"}, sources(t, ctx, viewer), "the files hold the new one")
	seen := *readerRow(t, s).DateSeen
	require.Eventually(t, func() bool { return readerRow(t, s).DateSeen.After(seen) }, 5*time.Second, 50*time.Millisecond,
		"the agent heartbeats with the new serial")

	// The reader's own clock reaches two thirds of the certificate's life.
	agent.clock.Add(61 * 24 * time.Hour)
	require.Eventually(t, func() bool {
		row = readerRow(t, s)
		return agent.serial() != second && agent.serial() == row.CertSerial
	}, 10*time.Second, 50*time.Millisecond, "renewed before it expires")
	// Back to today's clock, which the new certificate is young by; until
	// then each new one was due as well. A heartbeat after that is one no
	// renewal is in flight behind, since the agent renews between them.
	agent.clock.Add(-61 * 24 * time.Hour)
	reset := time.Now()
	require.Eventually(t, func() bool {
		row = readerRow(t, s)
		return row.DateSeen.After(reset) && agent.serial() == row.CertSerial
	}, 5*time.Second, 50*time.Millisecond)
	viewer = agent.conn(cp)
	require.Equal(t, []string{"door"}, sources(t, ctx, viewer))
	require.Eventually(t, func() bool { return strings.Count(agent.log.String(), "certificate renewed") >= 2 }, 5*time.Second, 50*time.Millisecond)

	// Erased: refused at once, and the agent asks again, as a new reader.
	_, err = api.NewReaderServiceClient(admin).Erase(ctx, api.ReaderRef_builder{Id: id.Bytes()}.Build())
	require.NoError(t, err)
	_, err = api.NewSourceServiceClient(viewer).List(ctx, api.SourceListRequest_builder{}.Build())
	require.Equal(t, codes.Unauthenticated, status.Code(err), "an erased reader's certificate")
	again := pendingReader(t, s, key)
	require.NotEqual(t, id, pdid.Id(again.Id), "a new row: the erased one stays erased")
	require.Contains(t, agent.log.String(), "refused the certificate")

	// Adopted for the whole tenant, it reads every camera, the one in a set
	// of no site too.
	_, err = api.NewReaderServiceClient(admin).Adopt(ctx, api.ReaderAdoptRequest_builder{Ref: api.ReaderRef_builder{Id: again.Id[:]}.Build()}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		row = readerRow(t, s)
		return row.CertSerial != "" && agent.serial() == row.CertSerial && row.DateSeen != nil
	}, 10*time.Second, 50*time.Millisecond)
	viewer = agent.conn(cp)
	require.Equal(t, []string{"door", "gate", "yard"}, sources(t, ctx, viewer))
	_, err = api.NewLaminaServiceClient(viewer).Timeline(ctx, api.LaminaTimelineRequest_builder{Set: api.SetRef_builder{Id: looseSet.GetId()}.Build(), From: from, To: to}.Build())
	require.NoError(t, err)
}

// `serve reader` beside `serve all --dev`: the agent joins the plaintext
// development CP and is adopted for the site of a recording, and what reads
// as the reader finds the recording in Timeline and its bytes on the node
// with the read token the answer carries (§17.1). In development the plain
// header stands in for the certificate.
func TestReaderAgentReadsARecording(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	admin := c.dial("@acme/admin")
	src, ex := exportRecording(t, c, admin)

	dir := t.TempDir()
	agent := newReaderAgent(t, cmd.Config{Dev: dir, State: dir, Cp: "http://" + c.running.TenantAddr, Tenant: "acme"})
	require.True(t, agent.cfg.Dev)
	agent.start()
	row := pendingReader(t, c.running.CP, agent.keyOtherThan(""))
	_, err := api.NewReaderServiceClient(admin).Adopt(ctx, api.ReaderAdoptRequest_builder{
		Ref: api.ReaderRef_builder{Id: row.Id[:]}.Build(), Sites: []*api.SiteRef{api.SiteRef_builder{Id: src.GetSite().GetId()}.Build()},
	}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		row = readerRow(t, c.running.CP)
		return agent.serial() != "" && agent.serial() == row.CertSerial && row.DateSeen != nil
	}, 10*time.Second, 50*time.Millisecond)

	as := c.dial(pdid.Id(row.Id).String())
	require.Equal(t, []string{"door"}, sources(t, ctx, as))
	tl, err := api.NewLaminaServiceClient(as).Timeline(ctx, api.LaminaTimelineRequest_builder{Source: ex.GetSource(), From: ex.GetFrom(), To: ex.GetTo()}.Build())
	require.NoError(t, err)
	require.Len(t, tl.GetSources(), 1)
	require.Len(t, tl.GetSources()[0].GetLaminae(), 1)
	want, err := os.ReadFile(filepath.Join("..", "fmp4", "testdata", "av.mp4"))
	require.NoError(t, err)
	require.Equal(t, want, fetch(t, tl.GetSources()[0].GetLaminae()[0].GetUrl()))
}

// A reader whose certificate expired while it was off: the CP's TLS
// refuses the certificate, so the agent joins again without it and with a
// new key, is "known host, new key" under its row, and one adopt with no
// sites puts it back as it was.
func TestReaderAgentExpiredCertificate(t *testing.T) {
	s, cp, admin := cameraTLSControl(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	north, _, _ := readerSites(t, ctx, admin)

	c := cmd.Config{State: t.TempDir(), Cp: cp, CaHash: pki.Fingerprint(s.CA.Cert), Tenant: "acme"}
	agent := newReaderAgent(t, c)
	agent.start()
	row := pendingReader(t, s, agent.keyOtherThan(""))
	id := pdid.Id(row.Id)
	_, err := api.NewReaderServiceClient(admin).Adopt(ctx, api.ReaderAdoptRequest_builder{Ref: api.ReaderRef_builder{Id: id.Bytes()}.Build(), Sites: []*api.SiteRef{north}}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		row = readerRow(t, s)
		return agent.serial() != "" && agent.serial() == row.CertSerial && row.DateSeen != nil
	}, 10*time.Second, 50*time.Millisecond)
	agent.stop()

	// Off for longer than the certificate lives: what it holds, and what
	// its row records, is a certificate that has run out.
	key, err := agent.store().Key()
	require.NoError(t, err)
	csr, err := key.CSR("viewer")
	require.NoError(t, err)
	expired, err := s.CA.IssueHost(csr, id, pki.Names{}, time.Now().Add(-pki.HostLifetime-time.Hour))
	require.NoError(t, err)
	require.NoError(t, agent.store().SetCert(pki.EncodeCerts(expired), s.CA.Bundle()))
	_, err = s.Deps.Ent.Reader.UpdateOneId(id.Uuid()).SetCertSerial(pki.Serial(expired)).SetDateCertExpires(expired.NotAfter).Save(ctx)
	require.NoError(t, err)
	_, err = api.NewSourceServiceClient(agent.conn(cp)).List(ctx, api.SourceListRequest_builder{}.Build())
	require.Error(t, err, "the CP's TLS refuses an expired certificate")

	agent.start()
	row = pendingReader(t, s, agent.keyOtherThan(key.Fingerprint()))
	require.Equal(t, id, pdid.Id(row.Id), "known host: the same row")
	require.True(t, row.Join.GetRejoin(), "new key")
	require.Contains(t, agent.log.String(), "the certificate has expired")
	require.Equal(t, pki.Serial(expired), agent.serial(), "held until a new one replaces it")

	// Adopted again, naming no sites: the sites it had.
	_, err = api.NewReaderServiceClient(admin).Adopt(ctx, api.ReaderAdoptRequest_builder{Ref: api.ReaderRef_builder{Id: id.Bytes()}.Build()}.Build())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		row = readerRow(t, s)
		return agent.serial() != pki.Serial(expired) && agent.serial() == row.CertSerial && row.DateSeen.After(expired.NotAfter)
	}, 10*time.Second, 50*time.Millisecond, "a certificate for the new key")
	require.False(t, row.AllSites)
	require.Equal(t, []string{"door"}, sources(t, ctx, agent.conn(cp)))
}
