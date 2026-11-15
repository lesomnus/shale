package e2e_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/auth"
	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/storage"
)

// A CA rollover (§33.5) over real TLS: a storage node and a producer on
// the built-in CA, recording; the rollover begun by the leader because the
// CA is "within a year" of expiry; both take the bundle with the next CA,
// renew from it, and keep recording, heartbeating, and being dialed by the
// CP on the node's control API, through to the old CA being dropped.

// rolloverControl is a control plane with real TLS on both surfaces: the
// development harness with its CP certificate, handing out https
// endpoints, whose jobs the test runs itself one pass at a time.
func rolloverControl(t *testing.T, options ...func(*cmd.Config)) (s *cmd.Server, tenant, cluster string) {
	t.Helper()
	x := require.New(t)
	ctx := context.Background()
	c := &cmd.Config{}
	cli.ApplyDev(c, t.TempDir())
	c.Control.CertFile = filepath.Join(c.StateDir("control"), cmd.CpCertFile)
	c.Control.KeyFile = filepath.Join(c.StateDir("control"), cmd.CpKeyFile)
	c.Server.Http.Addr = "127.0.0.1:0"
	c.Cluster.Http.Addr = "127.0.0.1:0"
	c.Control.CaSettle = time.Second
	if dsn := os.Getenv("SHALE_E2E_DB_DSN"); dsn != "" {
		c.Db.Driver, c.Db.Dsn = "pgx", dsn
		db, err := sql.Open("pgx", dsn)
		x.NoError(err)
		_, err = db.Exec("drop schema public cascade; create schema public")
		x.NoError(err)
		x.NoError(db.Close())
	}
	for _, o := range options {
		o(c)
	}
	x.NoError(cli.Init(ctx, c, "acme", "admin", "ops", io.Discard, nil))
	s, err := cmd.Build(ctx, *c)
	x.NoError(err)
	t.Cleanup(func() { s.Close() })
	x.NoError(s.Prepare(ctx))
	// The endpoints a production CP hands out: the hosts below serve TLS.
	s.Deps.Dev = false

	serve := func(surface cmd.Surface) string {
		g, err := s.Grpc(ctx, surface)
		x.NoError(err)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		x.NoError(err)
		done := make(chan error, 1)
		go func() { done <- g.Serve(l) }()
		t.Cleanup(func() {
			g.Stop()
			<-done
		})

		return "https://" + l.Addr().String()
	}

	return s, serve(cmd.SurfaceTenant), serve(cmd.SurfaceCluster)
}

// trusting dials a surface as somebody, verifying the CP against `roots`
// alone, the way the CLI does with `client.ca_file`.
func trusting(t *testing.T, addr, as string, roots *x509.CertPool) *grpc.ClientConn {
	t.Helper()
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}))}
	opts = append(opts, auth.Inject(auth.PlainProvider(as))...)
	conn, err := grpc.NewClient(strings.TrimPrefix(addr, "https://"), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return conn
}

// following dials a surface as somebody, verifying the CP against the
// bundle the CP hands out at each handshake: a client that took the new
// bundle when the rollover began.
func following(t *testing.T, s *cmd.Server, addr, as string) *grpc.ClientConn {
	t.Helper()
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true, VerifyConnection: func(cs tls.ConnectionState) error {
		set, err := s.Deps.Authorities().Current(context.Background())
		if err != nil {
			return err
		}
		opts := x509.VerifyOptions{Roots: set.Pool, DNSName: "localhost", Intermediates: x509.NewCertPool()}
		for _, c := range cs.PeerCertificates[1:] {
			opts.Intermediates.AddCert(c)
		}
		_, err = cs.PeerCertificates[0].Verify(opts)

		return err
	}}
	opts := []grpc.DialOption{grpc.WithTransportCredentials(credentials.NewTLS(cfg))}
	opts = append(opts, auth.Inject(auth.PlainProvider(as))...)
	conn, err := grpc.NewClient(strings.TrimPrefix(addr, "https://"), opts...)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return conn
}

// presented is the certificate a surface serves.
func presented(t *testing.T, addr string) *x509.Certificate {
	t.Helper()
	conn, err := tls.Dial("tcp", strings.TrimPrefix(addr, "https://"), &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}})
	require.NoError(t, err)
	defer conn.Close()

	return conn.ConnectionState().PeerCertificates[0]
}

func caStatus(t *testing.T, ops *grpc.ClientConn) *api.CertificateAuthorityStatusResponse {
	t.Helper()
	v, err := api.NewCertificateAuthorityServiceClient(ops).Status(context.Background(), &api.CertificateAuthorityStatusRequest{})
	require.NoError(t, err)

	return v
}

func caCert(t *testing.T, v *api.CertificateAuthority) *x509.Certificate {
	t.Helper()
	cs, err := pki.ParseCerts(v.GetCertificate())
	require.NoError(t, err)
	require.Len(t, cs, 1)

	return cs[0]
}

func TestCaRolloverOverTls(t *testing.T) {
	sample := samplePath(t)
	x := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// "Within a year" of expiry from the start: the leader begins the
	// rollover on the first pass of its jobs, which the test runs once the
	// hosts are up.
	s, tenantAddr, clusterAddr := rolloverControl(t, func(c *cmd.Config) { c.Control.CaRolloverBefore = 20 * 365 * 24 * time.Hour })
	old := s.CA.Cert
	oldRoots := x509.NewCertPool()
	oldRoots.AddCert(old)
	admin := following(t, s, tenantAddr, "@acme/admin")
	ops := following(t, s, clusterAddr, "@cluster/ops")
	x.True(pki.IssuedBy(presented(t, tenantAddr), old), "the CP serves its certificate from init's CA")

	// Small laminae, so the recording makes several a minute.
	up, err := api.NewUploadPolicyServiceClient(ops).Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias: "small", Version: 1,
		Bounds: api.UploadBounds_builder{MinLamina: 256 << 10, TargetLamina: 512 << 10, MaxLamina: 2 << 20}.Build(),
	}.Build())
	x.NoError(err)
	_, err = api.NewUploadPolicyServiceClient(ops).Activate(ctx, api.UploadPolicyActivateRequest_builder{Ref: api.UploadPolicyRef_builder{Id: up.GetId()}.Build()}.Build())
	x.NoError(err)

	// A node, adopted on its join (development mode adopts nodes), serving
	// TLS on its data plane and its control API.
	nodeState := t.TempDir()
	node, err := storage.New(storage.Config{
		StateDir: nodeState, Cp: clusterAddr, CaHash: pki.Fingerprint(old), HardwareId: "rollover-node",
		Addr: "127.0.0.1:0", ControlAddr: "127.0.0.1:0",
		Sinks:             []storage.SinkConfig{{Path: t.TempDir(), Capacity: 1 << 30, Device: "disk-rollover"}},
		HeartbeatInterval: 300 * time.Millisecond,
		Limits:            storage.DefaultLimits,
		Log:               slog.Default().With("node", "rollover"),
	})
	x.NoError(err)
	nodeDone := make(chan error, 1)
	go func() { nodeDone <- node.Run(ctx) }()
	select {
	case <-node.Ready:
	case err := <-nodeDone:
		t.Fatalf("node: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the node did not come up")
	}
	t.Cleanup(func() {
		cancel()
		<-nodeDone
	})
	nodeId := node.Id()

	// A producer, recording the sample for a set.
	set, err := api.NewSetServiceClient(admin).Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "lobby"}.Build())
	x.NoError(err)
	p, err := producer.New(producer.Config{
		StateDir: t.TempDir(), Cp: tenantAddr, CaHash: pki.Fingerprint(old),
		Sources:           []producer.SourceConfig{{Alias: "door", Input: "raw:" + sample, Fps: 30, MaxBitrate: 2_000_000, Format: "h264"}},
		Mode:              api.UploadMode_UPLOAD_MODE_LIVE,
		HeartbeatInterval: 300 * time.Millisecond,
		AllocationHorizon: time.Minute,
		Buffer:            64 << 20,
	})
	x.NoError(err)
	prodDone := make(chan error, 1)
	go func() { prodDone <- p.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-prodDone
	})
	producers := api.NewProducerServiceClient(admin)
	var pending *api.Producer
	x.Eventually(func() bool {
		vs, err := producers.List(ctx, api.ProducerListRequest_builder{Size: 10}.Build())
		if err != nil || len(vs.GetItems()) == 0 {
			return false
		}
		pending = vs.GetItems()[0]

		return pending.GetState() == api.HostState_HOST_STATE_PENDING
	}, 30*time.Second, 100*time.Millisecond, "the producer joins")
	_, err = producers.Adopt(ctx, api.ProducerAdoptRequest_builder{Ref: api.ProducerRef_builder{Id: pending.GetId()}.Build(), Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build())
	x.NoError(err)
	producerId := mustPdid(t, pending.GetId())
	select {
	case <-p.Ready:
	case err := <-prodDone:
		t.Fatalf("producer: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the producer did not start recording")
	}

	// Laminae are allocated ahead of their time, so what says that uploads
	// go on is the number committed rising.
	laminae := api.NewLaminaServiceClient(admin)
	committed := func() int {
		vs, err := laminae.List(ctx, api.LaminaListRequest_builder{
			Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
			Size:    1000,
		}.Build())
		if err != nil {
			return 0
		}
		n := 0
		for _, o := range vs.GetItems() {
			if o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED {
				n++
			}
		}

		return n
	}
	recording := func(msg string) {
		t.Helper()
		was := committed()
		x.Eventually(func() bool { return committed() > was+1 }, 60*time.Second, 200*time.Millisecond, msg)
	}
	recording("the producer records onto the node over TLS")

	// The CP dials the node's control API: mTLS both ways, the node
	// insisting on the CP's certificate.
	var sinkId []byte
	x.Eventually(func() bool {
		vs, err := api.NewSinkServiceClient(ops).List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, v := range vs.GetItems() {
			if string(v.GetNode().GetId()) == string(nodeId.Bytes()) {
				sinkId = v.GetId()
			}
		}

		return sinkId != nil
	}, 10*time.Second, 100*time.Millisecond)
	dialsNode := func() error {
		n, err := api.NewNodeServiceClient(ops).Get(ctx, api.NodeGetRequest_builder{Ref: api.NodeRef_builder{Id: nodeId.Bytes()}.Build()}.Build())
		if err != nil {
			return err
		}
		conn, err := s.Deps.DialNode(ctx, n.GetControlAddress(), nodeId)
		if err != nil {
			return err
		}
		defer conn.Close()
		cctx, ccancel := context.WithTimeout(ctx, 5*time.Second)
		defer ccancel()
		stream, err := api.NewNodeControlClient(conn).Reconcile(cctx, api.NodeReconcileRequest_builder{SinkId: sinkId}.Build())
		if err != nil {
			return err
		}
		for {
			if _, err := stream.Recv(); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}

				return err
			}
		}
	}
	x.NoError(dialsNode(), "the CP reaches the node's control API on init's CA")

	// The rollover begins.
	x.NoError(s.Jobs.Once(ctx))
	st := caStatus(t, ops)
	x.Equal(api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_DISTRIBUTING, st.GetPhase())
	x.Len(st.GetAuthorities(), 2)
	x.Equal(pki.Fingerprint(old), st.GetAuthorities()[0].GetFingerprint(), "init's CA is recorded first")
	x.Equal(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_NEXT, st.GetAuthorities()[1].GetState())
	x.Empty(st.GetAuthorities()[1].GetPrivateKey(), "a CA key is never answered with")
	next := caCert(t, st.GetAuthorities()[1])
	both := st.GetBundleHash()
	bundle, err := pki.ParseCerts([]byte(st.GetBundle()))
	x.NoError(err)
	x.Len(bundle, 2, "the bundle carries both CAs")

	// Hosts are asked to renew, and take the bundle with both; issuance
	// stays with the old CA until every live host holds it.
	hostHash := func(dir string) string {
		b, _ := (pki.Store{Dir: dir}).Bundle()
		return pki.BundleHash(b)
	}
	x.Eventually(func() bool {
		return hostHash(nodeState) == both && len(caStatus(t, ops).GetWaiting()) == 0
	}, 30*time.Second, 100*time.Millisecond, "every live host reports the bundle")
	leaf, err := (pki.Store{Dir: nodeState}).Cert()
	x.NoError(err)
	x.True(pki.IssuedBy(leaf[0], old), "issued by the old CA while the new one is being distributed")

	// Issuance moves to the new CA, and so does the CP's own certificate.
	x.Eventually(func() bool {
		x.NoError(s.Jobs.Once(ctx))
		return caStatus(t, ops).GetPhase() == api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_REPLACING
	}, 30*time.Second, 200*time.Millisecond)
	x.Eventually(func() bool { return pki.IssuedBy(presented(t, tenantAddr), next) }, 10*time.Second, 100*time.Millisecond, "the CP serves a certificate from the new CA")
	_, err = api.NewSetServiceClient(trusting(t, tenantAddr, "@acme/admin", oldRoots)).List(ctx, api.SetListRequest_builder{}.Build())
	x.Error(err, "a client trusting the old CA alone does not verify the CP any more")

	// Hosts holding a certificate from the old CA renew from the new one.
	x.Eventually(func() bool {
		leaf, err := (pki.Store{Dir: nodeState}).Cert()
		return err == nil && pki.IssuedBy(leaf[0], next) && len(caStatus(t, ops).GetWaiting()) == 0
	}, 30*time.Second, 100*time.Millisecond, "every live host holds a certificate from the new CA")
	prow, err := s.Ent.Producer.Get(ctx, producerId.Uuid())
	x.NoError(err)
	pleaf, err := pki.ParseCerts(prow.Join.GetCertificate())
	x.NoError(err)
	x.True(pki.IssuedBy(pleaf[0], next), "the producer renewed from the new CA too")
	recording("the producer records onto the node, both on the new CA")
	x.NoError(dialsNode(), "the CP reaches the node's control API on the new CA")

	// Nothing live holds what the old CA issued: it is dropped.
	x.Eventually(func() bool {
		x.NoError(s.Jobs.Once(ctx))
		return caStatus(t, ops).GetPhase() == api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_IDLE
	}, 30*time.Second, 200*time.Millisecond)
	st = caStatus(t, ops)
	x.Len(st.GetAuthorities(), 2)
	x.Equal(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_RETIRED, st.GetAuthorities()[0].GetState())
	x.NotNil(st.GetAuthorities()[0].GetDateRetired())
	x.Equal(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_ACTIVE, st.GetAuthorities()[1].GetState())
	retired, err := s.Ent.CertificateAuthority.Get(ctx, mustPdid(t, st.GetAuthorities()[0].GetId()).Uuid())
	x.NoError(err)
	x.Empty(retired.PrivateKey, "a dropped CA's key is not kept")
	bundle, err = pki.ParseCerts([]byte(st.GetBundle()))
	x.NoError(err)
	x.Len(bundle, 1)
	x.Equal(next.Raw, bundle[0].Raw, "the bundle is the new CA alone")

	// Hosts take the bundle without the old CA and keep working.
	x.Eventually(func() bool {
		prow, err := s.Ent.Producer.Get(ctx, producerId.Uuid())
		return err == nil && hostHash(nodeState) == st.GetBundleHash() && prow.CaHash == st.GetBundleHash()
	}, 30*time.Second, 100*time.Millisecond, "the hosts report the bundle without the old CA")
	recording("the producer records onto the node after the drop")
	x.NoError(dialsNode(), "the CP reaches the node's control API after the drop")
	seen := func() time.Time {
		n, err := s.Ent.Node.Get(ctx, nodeId.Uuid())
		x.NoError(err)
		return *n.DateSeen
	}
	before := seen()
	x.Eventually(func() bool { return seen().After(before) }, 10*time.Second, 100*time.Millisecond, "the node heartbeats after the drop")
	newRoots := x509.NewCertPool()
	newRoots.AddCert(next)
	_, err = api.NewSetServiceClient(trusting(t, tenantAddr, "@acme/admin", newRoots)).List(ctx, api.SetListRequest_builder{}.Build())
	x.NoError(err, "a client that took the new bundle verifies the CP")
}

// `ca rotate` begins a rollover early, in development mode's cluster of a
// node and a relay; a second node, adopted and then gone quiet holding
// init's bundle, holds it until it is no longer live -- here, until its
// certificate expires, which the test brings forward.
func TestCaRolloverWaitsForLiveHosts(t *testing.T) {
	x := require.New(t)
	ctx := context.Background()
	c := start(t, func(c *cmd.Config) {
		c.Control.JobsEvery = 200 * time.Millisecond
		c.Control.CaSettle = time.Second
	})
	ops := api.NewCertificateAuthorityServiceClient(c.dialCluster(c.operator()))
	nodes := api.NewNodeServiceClient(c.dialCluster(c.operator()))

	st, err := ops.Status(ctx, &api.CertificateAuthorityStatusRequest{})
	x.NoError(err)
	x.Equal(api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_IDLE, st.GetPhase())
	x.Len(st.GetAuthorities(), 1, "init's CA, nothing stored yet")
	x.Empty(st.GetAuthorities()[0].GetId())
	x.True(st.GetDateRollover().AsTime().After(time.Now().Add(8*365*24*time.Hour)), "nine years away: deploying this begins nothing")
	initial := st.GetBundleHash()

	// The quiet one: adopted, heartbeating with init's bundle, then stopped.
	stale, stop := c.startNode("stale", filepath.Join(t.TempDir(), "stale-sink"))
	staleId := stale.Id()
	x.Eventually(func() bool {
		n, err := nodes.Get(ctx, api.NodeGetRequest_builder{Ref: api.NodeRef_builder{Id: staleId.Bytes()}.Build()}.Build())
		return err == nil && n.GetCaHash() == initial
	}, 30*time.Second, 100*time.Millisecond)
	stop()

	// `ca rotate`.
	next, err := ops.Rotate(ctx, &api.CertificateAuthorityRotateRequest{})
	x.NoError(err)
	x.Equal(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_NEXT, next.GetState())
	x.Empty(next.GetPrivateKey())
	_, err = ops.Rotate(ctx, &api.CertificateAuthorityRotateRequest{})
	x.Equal(codes.FailedPrecondition, status.Code(err), "one rollover at a time")
	_, err = ops.Erase(ctx, api.CertificateAuthorityRef_builder{Id: next.GetId()}.Build())
	x.Equal(codes.PermissionDenied, status.Code(err), "the CA set is the rollover's to change")

	// The node and the relay of this process take the new bundle; the
	// quiet node does not, and the rollover waits for it.
	var waiting []*api.CertificateAuthorityWaiting
	x.Eventually(func() bool {
		st, err = ops.Status(ctx, &api.CertificateAuthorityStatusRequest{})
		x.NoError(err)
		waiting = st.GetWaiting()
		return len(waiting) == 1
	}, 30*time.Second, 100*time.Millisecond, "only the quiet node still holds init's bundle")
	x.Equal(staleId.Bytes(), waiting[0].GetId())
	x.Equal("node", waiting[0].GetKind())
	x.Equal(initial, waiting[0].GetCaHash())
	x.GreaterOrEqual(st.GetLive(), int32(3))
	time.Sleep(3 * time.Second) // settle and several passes of the jobs
	st, err = ops.Status(ctx, &api.CertificateAuthorityStatusRequest{})
	x.NoError(err)
	x.Equal(api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_DISTRIBUTING, st.GetPhase(), "a live host on the old bundle holds the rollover")

	// Its certificate expires: it is no longer live, and it will be
	// adopted again when it comes back.
	x.NoError(c.running.CP.Ent.Node.UpdateOneId(staleId.Uuid()).SetDateCertExpires(time.Now().Add(-time.Minute)).Exec(ctx))
	x.Eventually(func() bool {
		st, err = ops.Status(ctx, &api.CertificateAuthorityStatusRequest{})
		x.NoError(err)
		return st.GetPhase() == api.CertificateAuthorityPhase_CERTIFICATE_AUTHORITY_PHASE_IDLE
	}, 60*time.Second, 200*time.Millisecond, "the rollover runs to its end")
	x.Len(st.GetAuthorities(), 2)
	x.Equal(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_RETIRED, st.GetAuthorities()[0].GetState())
	x.Equal(next.GetId(), st.GetAuthorities()[1].GetId())
	x.Equal(api.CertificateAuthorityState_CERTIFICATE_AUTHORITY_STATE_ACTIVE, st.GetAuthorities()[1].GetState())

	// The node and the relay of this process hold a certificate from the
	// new CA and its bundle alone.
	nextCert := caCert(t, next)
	relays := api.NewRelayServiceClient(c.dialCluster(c.operator()))
	x.Eventually(func() bool {
		ns, err := nodes.List(ctx, api.NodeListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		rs, err := relays.List(ctx, api.RelayListRequest_builder{}.Build())
		if err != nil || len(rs.GetItems()) == 0 {
			return false
		}
		for _, n := range ns.GetItems() {
			if string(n.GetId()) == string(staleId.Bytes()) {
				continue
			}
			if n.GetCaHash() != st.GetBundleHash() || !pki.IssuedBy(leafOfPem(t, n.GetJoin().GetCertificate()), nextCert) {
				return false
			}
		}
		for _, r := range rs.GetItems() {
			if r.GetCaHash() != st.GetBundleHash() || !pki.IssuedBy(leafOfPem(t, r.GetJoin().GetCertificate()), nextCert) {
				return false
			}
		}

		return true
	}, 30*time.Second, 100*time.Millisecond, "the node and the relay renewed from the new CA and hold its bundle")
}

func leafOfPem(t *testing.T, b []byte) *x509.Certificate {
	t.Helper()
	cs, err := pki.ParseCerts(b)
	if err != nil || len(cs) == 0 {
		return nil
	}

	return cs[0]
}

func mustPdid(t *testing.T, b []byte) pdid.Id {
	t.Helper()
	id, err := pdid.From(b)
	require.NoError(t, err)

	return id
}
