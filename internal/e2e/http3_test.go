package e2e_test

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/spin"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/h3"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/storage"
)

// tlsCluster is the control plane as a deployment runs it as far as the
// data plane goes: both APIs over TLS from the built-in CA, endpoints
// handed out as https, and Storage Nodes beside it that join over mTLS and
// serve their data planes with their host certificates, over TCP and, with
// `storage.http3`, HTTP/3 on the same port (§35.6). Development mode is
// plaintext everywhere (§34.7), and QUIC never is; the rest of it stays:
// SQLite, the plain header, hosts adopted as they join.
type tlsCluster struct {
	t       *testing.T
	cp      *cmd.Server
	tenant  string
	cluster string
	pool    *x509.CertPool
}

func startTLS(t *testing.T) *tlsCluster {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	c := &cmd.Config{}
	cli.ApplyDev(c, dir)
	c.Server.Http.Addr = "127.0.0.1:0"
	c.Cluster.Http.Addr = "127.0.0.1:0"
	c.Control.CertFile = filepath.Join(c.StateDir("control"), cmd.CpCertFile)
	c.Control.KeyFile = filepath.Join(c.StateDir("control"), cmd.CpKeyFile)
	c.Control.DirectivesEvery = 500 * time.Millisecond
	if dsn := os.Getenv("SHALE_E2E_DB_DSN"); dsn != "" {
		c.Db.Driver, c.Db.Dsn, c.Watch.Broker = "pgx", dsn, "postgres"
		db, err := sql.Open("pgx", dsn)
		require.NoError(t, err)
		_, err = db.Exec("drop schema public cascade; create schema public")
		require.NoError(t, err)
		db.Close()
	}
	require.NoError(t, cli.Init(ctx, c, "acme", "admin", "ops", io.Discard, nil))
	s, err := cmd.Build(ctx, *c)
	require.NoError(t, err)
	require.NoError(t, s.Prepare(ctx))
	// The endpoints it hands out are the nodes' TLS, as outside
	// development mode; who may call is still development mode's.
	s.Deps.Dev = false

	tl, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cl, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, run := range []func() error{
		func() error { return spin.Run(runCtx, slices.Values(s.Spin)) },
		func() error { return s.Identity.Run(runCtx) },
		func() error { return s.Serve(runCtx, cmd.SurfaceTenant, tl) },
		func() error { return s.Serve(runCtx, cmd.SurfaceCluster, cl) },
	} {
		wg.Add(1)
		go func() { defer wg.Done(); run() }()
	}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		s.Close()
	})

	pool := x509.NewCertPool()
	pool.AddCert(s.CA.Cert)

	return &tlsCluster{t: t, cp: s, tenant: tl.Addr().String(), cluster: cl.Addr().String(), pool: pool}
}

// dial is an API as somebody, over TLS, with the plain header.
func (c *tlsCluster) dial(addr, as string) *grpc.ClientConn {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: c.pool, MinVersion: tls.VersionTLS12}))}
	opts = append(opts, auth.Inject(auth.PlainProvider(as))...)
	conn, err := grpc.NewClient(addr, opts...)
	require.NoError(c.t, err)
	c.t.Cleanup(func() { conn.Close() })

	return conn
}

// startNode runs a Storage Node that joins over mTLS, pinning the CA, and
// answers once its sink is attached. Its state and sink directories, and
// its data address once it had one, make it the same node again.
func (c *tlsCluster) startNode(name, stateDir, sinkDir, addr string, http3 bool) (*storage.Node, func()) {
	c.t.Helper()
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ctx, cancel := context.WithCancel(context.Background())
	cfg := storage.Config{
		StateDir:          stateDir,
		Cp:                "https://" + c.cluster,
		CaHash:            pki.Fingerprint(c.cp.CA.Cert),
		HardwareId:        "test-" + name,
		Addr:              addr,
		ControlAddr:       "127.0.0.1:0",
		HTTP3:             http3,
		Sinks:             []storage.SinkConfig{{Path: sinkDir, Capacity: 1 << 30, Device: "disk-" + name}},
		HeartbeatInterval: time.Second,
		Log:               slog.Default().With("node", name),
	}
	n, err := storage.New(cfg)
	require.NoError(c.t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		n.Run(ctx)
	}()
	select {
	case <-n.Ready:
	case <-done:
		cancel()
		c.t.Fatalf("node %s stopped", name)
	case <-time.After(30 * time.Second):
		cancel()
		c.t.Fatalf("node %s did not come up", name)
	}
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
		}
	}
	c.t.Cleanup(stop)

	sinks := api.NewSinkServiceClient(c.dial(c.cluster, "@cluster/ops"))
	require.Eventually(c.t, func() bool {
		vs, err := sinks.List(context.Background(), api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, s := range vs.GetItems() {
			if string(s.GetNode().GetId()) == string(n.Id().Bytes()) && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED &&
				s.GetDateSeen() != nil && time.Since(s.GetDateSeen().AsTime()) < 3*time.Second {
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "node %s's sink is attached", name)

	return n, stop
}

// source is a set of one source and its Lamina service.
func (c *tlsCluster) source(alias string) (*api.Set, *api.Source, api.LaminaServiceClient, *grpc.ClientConn) {
	c.t.Helper()
	ctx := context.Background()
	conn := c.dial(c.tenant, "@acme/admin")
	set, err := api.NewSetServiceClient(conn).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: alias}.Build())
	require.NoError(c.t, err)
	src, err := api.NewSourceServiceClient(conn).Add(ctx, api.SourceAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "cam", Set: api.SetRef_builder{Id: set.GetId()}.Build(),
		Profile: api.SegmentProfile_builder{MaxBitrate: 2_000_000}.Build(),
	}.Build())
	require.NoError(c.t, err)

	return set, src, api.NewLaminaServiceClient(conn), conn
}

func allocateAt(t *testing.T, laminae api.LaminaServiceClient, src *api.Source, started time.Time) *api.Allocation {
	t.Helper()
	al, err := laminae.Allocate(context.Background(), api.LaminaAllocateRequest_builder{
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(started),
	}.Build())
	require.NoError(t, err)
	require.NotEmpty(t, al.GetCandidates())
	require.Equal(t, "https", al.GetCandidates()[0].GetEndpoints()[0].GetScheme())

	return al
}

// protos is a client's record of the HTTP version every answer came over,
// by method.
type protos struct {
	next http.RoundTripper
	mu   sync.Mutex
	seen []string
}

func (p *protos) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := p.next.RoundTrip(req)
	if err == nil {
		p.mu.Lock()
		p.seen = append(p.seen, req.Method+" "+resp.Proto)
		p.mu.Unlock()
	}

	return resp, err
}

func (p *protos) all() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return slices.Clone(p.seen)
}

func (p *protos) reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = nil
}

// client is what a producer uploads with under `producer.http3` (§35.6):
// TCP, and HTTP/3 as the mode says, trusting the cluster's CA.
func (c *tlsCluster) client(mode h3.Mode) (*h3.Transport, *protos, *http.Client) {
	tcp := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: c.pool}, ForceAttemptHTTP2: true}
	tr := h3.NewTransport(tcp, &tls.Config{RootCAs: c.pool}, mode, slog.Default())
	c.t.Cleanup(func() { tr.Close() })
	p := &protos{next: tr}

	return tr, p, &http.Client{Transport: p}
}

// timelineURL is where a reader gets the lamina from, as Timeline hands it
// out.
func timelineURL(t *testing.T, laminae api.LaminaServiceClient, set *api.Set, al *api.Allocation) string {
	t.Helper()
	ctx := context.Background()
	require.Eventually(t, func() bool {
		v, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())
		return err == nil && v.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED
	}, 30*time.Second, 100*time.Millisecond, "the lamina is committed")
	started := al.GetDateStarted().AsTime()
	tl, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{
		Set: api.SetRef_builder{Id: set.GetId()}.Build(), From: timestamppb.New(started.Add(-time.Minute)), To: timestamppb.New(started.Add(time.Hour)), Size: 100,
	}.Build())
	require.NoError(t, err)
	for _, s := range tl.GetSources() {
		for _, l := range s.GetLaminae() {
			if string(l.GetLaminaId()) == string(al.GetLaminaId()) {
				return l.GetUrl()
			}
		}
	}
	t.Fatal("the lamina is not on the timeline")

	return ""
}

func fetchWith(t *testing.T, c *http.Client, method, url string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	require.NoError(t, err)
	resp, err := c.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp, b
}

func uploader(laminae api.LaminaServiceClient, c *http.Client, mode api.UploadMode, written bool) *producer.Uploader {
	return &producer.Uploader{
		Cfg:     producer.UploadConfig{ResumeTimeout: 20 * time.Second, PlacementRetries: 1, Part: 64 << 10, Client: c},
		Laminae: laminae, Log: slog.Default(), Mode: mode, Written: written,
	}
}

// grow writes size bytes into the segment over about d, then closes it,
// as a capture does; it answers the bytes once they are all written.
func grow(seg *producer.Segment, size int, d time.Duration, ended time.Time) <-chan []byte {
	out := make(chan []byte, 1)
	go func() {
		whole := make([]byte, size)
		rand.Read(whole)
		const steps = 40
		step := size / steps
		for i := 0; i < size; i += step {
			seg.Write(whole[i:min(i+step, size)])
			time.Sleep(d / steps)
		}
		seg.Close(ended)
		out <- whole
	}()

	return out
}

// A producer uploads to a node over HTTP/3 (§35.6): a lamina whole, and a
// live one in parts, whose first part on `auto` goes over TCP and says the
// node serves HTTP/3, and whose next parts go over it. A reader then reads
// it back with HEAD and GET over HTTP/3, and a producer that knows nothing
// of HTTP/3, as before this, uploads over TCP to the same node.
func TestHTTP3Upload(t *testing.T) {
	c := startTLS(t)
	c.startNode("h3", filepath.Join(t.TempDir(), "state"), filepath.Join(t.TempDir(), "sink"), "", true)
	set, src, laminae, _ := c.source("h3")
	ctx := context.Background()
	base := time.Now().Add(-10 * time.Hour).Truncate(time.Second)

	// A whole lamina, on `on`.
	_, seen, client := c.client(h3.On)
	al := allocateAt(t, laminae, src, base)
	body := make([]byte, 700_000)
	rand.Read(body)
	seg := producer.NewSegment(base, nil)
	seg.Write(body)
	seg.Close(base.Add(time.Minute))
	res := uploader(laminae, client, api.UploadMode_UPLOAD_MODE_BUFFERED, false).Upload(ctx, al, seg)
	require.True(t, res.Stored, "stored: %v", res.Err)
	require.Equal(t, 1, res.Attempts)
	require.Equal(t, []string{"PUT HTTP/3.0"}, seen.all())

	// Read back over HTTP/3, as a reader with the URL Timeline hands out.
	url := timelineURL(t, laminae, set, al)
	resp, b := fetchWith(t, client, http.MethodHead, url)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "HTTP/3.0", resp.Proto)
	require.Equal(t, "?1", resp.Header.Get(storage.HdrUploadComplete))
	require.Empty(t, b)
	resp, b = fetchWith(t, client, http.MethodGet, url)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "HTTP/3.0", resp.Proto)
	require.Equal(t, body, b)

	// A live lamina in parts, on `auto`: TCP until the node says it serves
	// HTTP/3, then HTTP/3.
	tr, seen, client := c.client(h3.Auto)
	started := base.Add(time.Hour)
	al = allocateAt(t, laminae, src, started)
	seg = producer.NewSegment(started, nil)
	whole := grow(seg, 600_000, 2*time.Second, started.Add(time.Minute))
	res = uploader(laminae, client, api.UploadMode_UPLOAD_MODE_LIVE, true).Upload(ctx, al, seg)
	require.True(t, res.Stored, "stored: %v", res.Err)
	require.False(t, res.Cut)
	puts := seen.all()
	require.Greater(t, len(puts), 2, "in parts")
	require.NotEqual(t, "PUT HTTP/3.0", puts[0], "nothing offered before the first answer")
	require.Equal(t, "PUT HTTP/3.0", puts[len(puts)-1])
	node := hostPort(al)
	require.True(t, tr.Speaks(node), "the offer is kept")
	_, b = fetchWith(t, client, http.MethodGet, timelineURL(t, laminae, set, al))
	require.Equal(t, <-whole, b)

	// A producer as it was: TCP, offer or not.
	tr, seen, client = c.client(h3.Off)
	started = base.Add(2 * time.Hour)
	al = allocateAt(t, laminae, src, started)
	seg = producer.NewSegment(started, nil)
	whole = grow(seg, 300_000, time.Second, started.Add(time.Minute))
	res = uploader(laminae, client, api.UploadMode_UPLOAD_MODE_LIVE, false).Upload(ctx, al, seg)
	require.True(t, res.Stored, "stored: %v", res.Err)
	for _, p := range seen.all() {
		require.NotContains(t, p, "HTTP/3")
	}
	_, b = fetchWith(t, client, http.MethodGet, timelineURL(t, laminae, set, al))
	require.Equal(t, <-whole, b)
	require.False(t, tr.Speaks(node))
}

// A node that restarts in the middle of a live upload over HTTP/3, a
// dropped connection: the uploader asks it where the upload stands once it
// is back, resumes from there over HTTP/3, and the lamina reads back whole.
func TestHTTP3ResumesAfterDroppedConnection(t *testing.T) {
	c := startTLS(t)
	state, sink := filepath.Join(t.TempDir(), "state"), filepath.Join(t.TempDir(), "sink")
	n, stop := c.startNode("h3", state, sink, "", true)
	addr := n.DataAddr
	set, src, laminae, _ := c.source("h3")
	ctx := context.Background()

	_, seen, client := c.client(h3.On)
	started := time.Now().Add(-time.Hour).Truncate(time.Second)
	al := allocateAt(t, laminae, src, started)
	seg := producer.NewSegment(started, nil)
	// Longer than the node takes to stop, which waits out its requests
	// for a while before it ends them.
	whole := grow(seg, 1_200_000, 12*time.Second, started.Add(time.Minute))
	out := make(chan producer.Result, 1)
	go func() { out <- uploader(laminae, client, api.UploadMode_UPLOAD_MODE_LIVE, false).Upload(ctx, al, seg) }()

	time.Sleep(1500 * time.Millisecond)
	require.Equal(t, []string(nil), seen.all(), "the upload is under way")
	stop()
	c.startNode("h3", state, sink, addr, true)

	res := <-out
	require.True(t, res.Stored, "stored: %v", res.Err)
	require.Equal(t, 1, res.Attempts, "the same target, resumed")
	answers := seen.all()
	require.Contains(t, answers, "HEAD HTTP/3.0", "asked where the upload stood")
	require.Equal(t, "PUT HTTP/3.0", answers[len(answers)-1], "and finished it over HTTP/3")
	_, b := fetchWith(t, client, http.MethodGet, timelineURL(t, laminae, set, al))
	require.Equal(t, <-whole, b)
}

// A node whose UDP a producer cannot reach, here one that does not serve
// HTTP/3 at all, as a node from before it: on `on` the handshake fails,
// the same request goes over TCP, and the node stays on TCP rather than
// costing a handshake per part, or per lamina. On `auto`, nothing is
// offered, and nothing but TCP is tried.
func TestHTTP3FallsBackToTCP(t *testing.T) {
	c := startTLS(t)
	c.startNode("tcp", filepath.Join(t.TempDir(), "state"), filepath.Join(t.TempDir(), "sink"), "", false)
	set, src, laminae, _ := c.source("tcp")
	ctx := context.Background()
	base := time.Now().Add(-10 * time.Hour).Truncate(time.Second)

	upload := func(tr *h3.Transport, seen *protos, client *http.Client, started time.Time) time.Duration {
		t.Helper()
		seen.reset()
		al := allocateAt(t, laminae, src, started)
		seg := producer.NewSegment(started, nil)
		whole := grow(seg, 400_000, time.Second, started.Add(time.Minute))
		began := time.Now()
		res := uploader(laminae, client, api.UploadMode_UPLOAD_MODE_LIVE, true).Upload(ctx, al, seg)
		took := time.Since(began)
		require.True(t, res.Stored, "stored: %v", res.Err)
		require.Equal(t, 1, res.Attempts)
		require.NotEmpty(t, seen.all())
		for _, p := range seen.all() {
			require.NotContains(t, p, "HTTP/3")
		}
		require.False(t, tr.Speaks(hostPort(al)))
		_, b := fetchWith(t, client, http.MethodGet, timelineURL(t, laminae, set, al))
		require.Equal(t, <-whole, b)

		return took
	}

	tr, seen, client := c.client(h3.On)
	took := upload(tr, seen, client, base)
	require.Less(t, took, h3.HandshakeTimeout+5*time.Second, "one handshake waited for")
	took = upload(tr, seen, client, base.Add(time.Hour))
	require.Less(t, took, h3.HandshakeTimeout, "and no other: the node stays on TCP")

	tr, seen, client = c.client(h3.Auto)
	took = upload(tr, seen, client, base.Add(2*time.Hour))
	require.Less(t, took, h3.HandshakeTimeout, "nothing offered, nothing tried")
}

// hostPort is where an allocation's first candidate is dialed.
func hostPort(al *api.Allocation) string {
	ep := al.GetCandidates()[0].GetEndpoints()[0]

	return net.JoinHostPort(ep.GetHost(), strconv.Itoa(int(ep.GetPort())))
}
