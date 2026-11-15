package hostagent

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/internal/pki"
)

type bootstrapWorkerServer struct {
	api.UnimplementedCameraWorkerServiceServer
}

func (bootstrapWorkerServer) Join(ctx context.Context, _ *api.CameraWorkerJoinRequest) (*api.CameraWorkerJoinResponse, error) {
	p, _ := peer.FromContext(ctx)
	if info, ok := p.AuthInfo.(credentials.TLSInfo); !ok || len(info.State.PeerCertificates) != 0 {
		return nil, status.Error(codes.PermissionDenied, "bootstrap must omit expired client certificate")
	}
	return api.CameraWorkerJoinResponse_builder{Answer: api.JoinAnswer_builder{State: api.HostState_HOST_STATE_PENDING}.Build()}.Build(), nil
}
func (bootstrapWorkerServer) Heartbeat(ctx context.Context, _ *api.CameraWorkerHeartbeatRequest) (*api.CameraWorkerHeartbeatResponse, error) {
	p, _ := peer.FromContext(ctx)
	if info, ok := p.AuthInfo.(credentials.TLSInfo); !ok || len(info.State.VerifiedChains) == 0 {
		return nil, status.Error(codes.Unauthenticated, "machine certificate required")
	}
	return api.CameraWorkerHeartbeatResponse_builder{}.Build(), nil
}

func TestExpiredCertificateHasOnlyExplicitPublicBootstrap(t *testing.T) {
	x := require.New(t)
	now := time.Now()
	ca, err := pki.NewCA("bootstrap-ca", now)
	x.NoError(err)
	a := pinned(t, ca)
	csr, err := a.key.CSR("expired-worker")
	x.NoError(err)
	id := pdid.New(31)
	expired, err := ca.IssueClientHost(csr, id, now.Add(-pki.HostLifetime-time.Hour))
	x.NoError(err)
	x.NoError(a.Store.SetCert(pki.EncodeCerts(expired), ca.Bundle()))
	x.NoError(a.Init())
	x.False(a.Ready(now))
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	x.NoError(err)
	serverCert, err := ca.IssueServer(&serverKey.PublicKey, pki.CpName, pki.Names{IPs: []net.IP{net.ParseIP("127.0.0.1")}}, time.Hour, now)
	x.NoError(err)
	pool, err := pki.Pool(ca.Bundle())
	x.NoError(err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	x.NoError(err)
	g := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{serverCert.Raw}, PrivateKey: serverKey}}, ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: pool})))
	api.RegisterCameraWorkerServiceServer(g, bootstrapWorkerServer{})
	go g.Serve(listener)
	t.Cleanup(func() { g.Stop(); listener.Close() })
	a.Cp = "https://" + listener.Addr().String()
	ctx, done := context.WithTimeout(context.Background(), 3*time.Second)
	defer done()
	normal, err := a.Dial(ctx)
	x.NoError(err)
	defer normal.Close()
	_, err = api.NewCameraWorkerServiceClient(normal).Heartbeat(ctx, api.CameraWorkerHeartbeatRequest_builder{}.Build())
	x.Error(err, "ordinary machine calls still present and reject the expired certificate")
	bootstrap, err := a.DialBootstrap(ctx)
	x.NoError(err)
	defer bootstrap.Close()
	_, err = api.NewCameraWorkerServiceClient(bootstrap).Join(ctx, api.CameraWorkerJoinRequest_builder{}.Build())
	x.NoError(err, "explicit public bootstrap reaches Join without the expired certificate")
	_, err = api.NewCameraWorkerServiceClient(bootstrap).Heartbeat(ctx, api.CameraWorkerHeartbeatRequest_builder{}.Build())
	x.Equal(codes.Unauthenticated, status.Code(err), "bootstrap never supplies a machine authority fallback")
}

// server is an HTTPS server on 127.0.0.1 with a certificate from ca.
func server(t *testing.T, ca *pki.CA) *httptest.Server {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert, err := ca.IssueServer(&key.PublicKey, "storage.example.com", pki.Names{IPs: []net.IP{net.ParseIP("127.0.0.1")}}, time.Hour, time.Now())
	require.NoError(t, err)
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	s.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{cert.Raw}, PrivateKey: key, Leaf: cert}}}
	s.StartTLS()
	t.Cleanup(s.Close)

	return s
}

// pinned is an agent that holds the shale CA's bundle, as a join leaves it.
func pinned(t *testing.T, ca *pki.CA) *Agent {
	t.Helper()
	a := &Agent{Store: pki.Store{Dir: t.TempDir()}}
	require.NoError(t, a.Init())
	require.NoError(t, a.Store.SetBundle(ca.Bundle()))

	return a
}

// roots stands in for the system's roots for the test.
func roots(t *testing.T, f func() (*x509.CertPool, error)) {
	t.Helper()
	prev := pki.SystemRoots
	pki.SystemRoots = f
	t.Cleanup(func() { pki.SystemRoots = prev })
}

func get(t *testing.T, c *http.Client, url string) error {
	t.Helper()
	resp, err := c.Get(url)
	if err != nil {
		return err
	}
	resp.Body.Close()

	return nil
}

// A host verifies a node or a relay against the shale CA and the system's
// roots both (§33.5): a data plane serving a certificate from a public CA
// is as good as one serving its host certificate.
func TestPeerTrustsSystemRoots(t *testing.T) {
	shale, err := pki.NewCA("shale", time.Now())
	require.NoError(t, err)
	public, err := pki.NewCA("public", time.Now())
	require.NoError(t, err)
	system := x509.NewCertPool()
	system.AddCert(public.Cert)
	roots(t, func() (*x509.CertPool, error) { return system.Clone(), nil })

	a := pinned(t, shale)
	c, err := a.HTTPClient()
	require.NoError(t, err)
	require.NoError(t, get(t, c, server(t, public).URL), "a public CA's certificate")
	require.NoError(t, get(t, c, server(t, shale).URL), "the shale CA's")
	stranger, err := pki.NewCA("stranger", time.Now())
	require.NoError(t, err)
	require.Error(t, get(t, c, server(t, stranger).URL), "neither")
}

// Without the system's roots, the shale CA alone, as before.
func TestPeerWithoutSystemRoots(t *testing.T) {
	shale, err := pki.NewCA("shale", time.Now())
	require.NoError(t, err)
	public, err := pki.NewCA("public", time.Now())
	require.NoError(t, err)
	roots(t, func() (*x509.CertPool, error) { return nil, errors.New("no system roots") })

	c, err := pinned(t, shale).HTTPClient()
	require.NoError(t, err)
	require.NoError(t, get(t, c, server(t, shale).URL))
	require.Error(t, get(t, c, server(t, public).URL))
}

// The CP connection stays pinned to the bundle: the system's roots are
// not asked about the control plane (§33.4).
func TestCpStaysPinned(t *testing.T) {
	shale, err := pki.NewCA("shale", time.Now())
	require.NoError(t, err)
	public, err := pki.NewCA("public", time.Now())
	require.NoError(t, err)
	system := x509.NewCertPool()
	system.AddCert(public.Cert)
	roots(t, func() (*x509.CertPool, error) { return system.Clone(), nil })

	a := pinned(t, shale)
	cfg, err := a.tlsConfig()
	require.NoError(t, err)
	require.False(t, cfg.InsecureSkipVerify)
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
	require.Error(t, get(t, c, server(t, public).URL), "a public certificate does not pass for the CP")
	require.NoError(t, get(t, c, server(t, shale).URL))
}

// Another host is never dialed before a CA is pinned: first contact pins
// the CP's CA, and only the CP's.
func TestDialAddrNeedsBundle(t *testing.T) {
	a := &Agent{Store: pki.Store{Dir: t.TempDir()}}
	require.NoError(t, a.Init())
	_, err := a.DialAddr(context.Background(), "127.0.0.1:7440", false)
	require.ErrorContains(t, err, "no CA pinned")
	cfg, err := pinned(t, mustCA(t)).PeerTLS()
	require.NoError(t, err)
	require.NotNil(t, cfg.RootCAs)
}

func mustCA(t *testing.T) *pki.CA {
	t.Helper()
	ca, err := pki.NewCA("shale", time.Now())
	require.NoError(t, err)

	return ca
}

// A CA the operator names for the CP (§33.5) is trusted beside the pinned
// bundle, and instead of the first-contact pin: a CP serving a certificate
// from a public CA verifies by its name, a stranger does not, and nothing
// is pinned on the way.
func TestCpRootsTrustedExplicitly(t *testing.T) {
	x := require.New(t)
	shale, public, stranger := mustCA(t), mustCA(t), mustCA(t)
	explicit := x509.NewCertPool()
	explicit.AddCert(public.Cert)
	a := &Agent{Store: pki.Store{Dir: t.TempDir()}, CpRoots: explicit}
	x.NoError(a.Init())

	cfg, err := a.tlsConfig()
	x.NoError(err)
	x.False(cfg.InsecureSkipVerify, "trusted explicitly, so not trust on first use")
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
	x.NoError(get(t, c, server(t, public).URL))
	x.Error(get(t, c, server(t, stranger).URL))
	x.Error(get(t, c, server(t, shale).URL), "the shale CA is not known before a join hands it over")
	bundle, err := a.Store.Bundle()
	x.NoError(err)
	x.Empty(bundle, "nothing pinned")

	// After a join, the bundle it handed over is trusted as well.
	x.NoError(a.Store.SetBundle(shale.Bundle()))
	cfg, err = a.tlsConfig()
	x.NoError(err)
	c = &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
	x.NoError(get(t, c, server(t, public).URL))
	x.NoError(get(t, c, server(t, shale).URL))
	x.Error(get(t, c, server(t, stranger).URL))
}

// issue signs a client certificate for key `k` naming `id`, issued at `at`.
func issue(t *testing.T, ca *pki.CA, k *pki.HostKey, id pdid.Id, at time.Time) []byte {
	t.Helper()
	csr, err := k.CSR("host")
	require.NoError(t, err)
	c, err := ca.IssueClientHost(csr, id, at)
	require.NoError(t, err)

	return pki.EncodeCerts(c)
}

// What the CP hands over is checked before it replaces what a host holds:
// for its key, naming its row, from the bundle (§33.4). Whether it is
// current is not this check's: a host whose clock is behind still takes a
// certificate just issued.
func TestVerifyIssued(t *testing.T) {
	x := require.New(t)
	now := time.Now()
	ca, err := pki.NewCA("shale", now.Add(-200*24*time.Hour))
	x.NoError(err)
	a := pinned(t, ca)
	id := pdid.New(23)

	_, err = a.verifyIssued(issue(t, ca, a.key, id, now), ca.Bundle(), id)
	x.NoError(err)
	_, err = a.verifyIssued(issue(t, ca, a.key, id, now), nil, id)
	x.NoError(err, "with no bundle in the answer, the pinned one")
	_, err = a.verifyIssued(issue(t, ca, a.key, id, now.Add(time.Hour)), ca.Bundle(), id)
	x.NoError(err, "this host's clock is behind the CP's")
	_, err = a.verifyIssued(issue(t, ca, a.key, id, now.Add(-100*24*time.Hour)), ca.Bundle(), id)
	x.NoError(err, "expired is Ready's question")

	_, err = a.verifyIssued(issue(t, ca, a.key, pdid.New(23), now), ca.Bundle(), id)
	x.ErrorContains(err, "names")
	other, err := pki.NewHostKey()
	x.NoError(err)
	_, err = a.verifyIssued(issue(t, ca, other, id, now), ca.Bundle(), id)
	x.ErrorContains(err, "key")
	_, err = a.verifyIssued(issue(t, mustCA(t), a.key, id, now), ca.Bundle(), id)
	x.Error(err, "not from the bundle")
	_, err = a.verifyIssued([]byte("not a certificate"), ca.Bundle(), id)
	x.Error(err)
}

// A renewal that does not verify leaves the certificate held as it was.
func TestRenewKeepsCertificateThatDoesNotVerify(t *testing.T) {
	x := require.New(t)
	now := time.Now()
	ca := mustCA(t)
	a := pinned(t, ca)
	id := pdid.New(23)
	held := issue(t, ca, a.key, id, now)
	x.NoError(a.Store.SetCert(held, ca.Bundle()))
	x.NoError(a.Init())
	x.Equal(id, a.Id())

	err := a.Renew(context.Background(), nil, func(context.Context, *grpc.ClientConn, []byte) ([]byte, []byte, error) {
		return issue(t, ca, a.key, pdid.New(23), now), ca.Bundle(), nil
	})
	x.ErrorContains(err, "does not verify")
	got, err := a.Store.CertPEM()
	x.NoError(err)
	x.Equal(held, got)

	var renewed []byte
	x.NoError(a.Renew(context.Background(), nil, func(_ context.Context, _ *grpc.ClientConn, csr []byte) ([]byte, []byte, error) {
		c, err := ca.IssueClientHost(csr, id, now)
		if err != nil {
			return nil, nil, err
		}
		renewed = pki.EncodeCerts(c)
		return renewed, ca.Bundle(), nil
	}))
	got, err = a.Store.CertPEM()
	x.NoError(err)
	x.Equal(renewed, got)
}

// An expired certificate for the current key is Expired; Rekey retires the
// key once, and the host still knows its row (§33.4).
func TestExpiredAndRekey(t *testing.T) {
	x := require.New(t)
	now := time.Now()
	ca, err := pki.NewCA("shale", now.Add(-200*24*time.Hour))
	x.NoError(err)
	a := pinned(t, ca)
	id := pdid.New(23)
	x.NoError(a.Store.SetCert(issue(t, ca, a.key, id, now.Add(-100*24*time.Hour)), ca.Bundle()))
	x.NoError(a.Init())
	x.True(a.Expired(now))
	x.False(a.Ready(now))
	x.False(a.Expired(now.Add(-50*24*time.Hour)), "not yet, by an earlier clock")

	before := a.Fingerprint()
	x.NoError(a.Rekey())
	x.NotEqual(before, a.Fingerprint())
	x.False(a.Expired(now), "the certificate held is for the old key")
	x.Equal(id, a.Id())
	again := &Agent{Store: a.Store}
	x.NoError(again.Init())
	x.Equal(a.Fingerprint(), again.Fingerprint(), "the new key is what the store holds")
	x.False(again.Expired(now))
}

// readerJoin answers each Join with the next of `answers`.
type readerJoin struct {
	api.UnimplementedReaderServiceServer
	answers chan *api.JoinAnswer
}

func (s readerJoin) Join(ctx context.Context, _ *api.ReaderJoinRequest) (*api.ReaderJoinResponse, error) {
	return api.ReaderJoinResponse_builder{Answer: <-s.answers}.Build(), nil
}

// Join does not store a certificate that is not this host's, and asks
// again.
func TestJoinRefusesCertificateThatDoesNotVerify(t *testing.T) {
	x := require.New(t)
	now := time.Now()
	ca := mustCA(t)
	a := &Agent{Store: pki.Store{Dir: t.TempDir()}, Dev: true, JoinEvery: 10 * time.Millisecond}
	x.NoError(a.Init())
	id := pdid.New(23)
	other, err := pki.NewHostKey()
	x.NoError(err)
	good := issue(t, ca, a.key, id, now)
	srv := readerJoin{answers: make(chan *api.JoinAnswer, 2)}
	srv.answers <- api.JoinAnswer_builder{Id: id.Bytes(), State: api.HostState_HOST_STATE_ADOPTED, Certificate: issue(t, ca, other, id, now), CaBundle: ca.Bundle()}.Build()
	srv.answers <- api.JoinAnswer_builder{Id: id.Bytes(), State: api.HostState_HOST_STATE_ADOPTED, Certificate: good, CaBundle: ca.Bundle()}.Build()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	x.NoError(err)
	g := grpc.NewServer()
	api.RegisterReaderServiceServer(g, srv)
	go g.Serve(l)
	t.Cleanup(g.Stop)
	a.Cp = "http://" + l.Addr().String()

	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	_, err = a.Join(ctx, func(ctx context.Context, conn *grpc.ClientConn, hj *api.HostJoin) (Answer, error) {
		v, err := api.NewReaderServiceClient(conn).Join(ctx, api.ReaderJoinRequest_builder{Host: hj}.Build())
		if err != nil {
			return Answer{}, err
		}
		return Answer{JoinAnswer: v.GetAnswer()}, nil
	})
	x.NoError(err)
	got, err := a.Store.CertPEM()
	x.NoError(err)
	x.Equal(good, got, "the second answer, the one for this key")
	x.Equal(id, a.Id())
	x.Empty(srv.answers)
}
