package satellite

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/token"
)

// node is a Storage Node's data plane as far as the proxy can tell: what
// it was sent, and the answer a test gives it.
type node struct {
	id  pdid.Id
	srv *httptest.Server

	mu   sync.Mutex
	got  []seen
	hold chan struct{}
	// answer writes the node's answer, after the body is read.
	answer func(w http.ResponseWriter)
}

type seen struct {
	method, path, rawQuery string
	header, trailer        http.Header
	body                   []byte
}

func newNode(t *testing.T) *node {
	n := &node{id: pdid.New(DomNode)}
	n.answer = func(w http.ResponseWriter) {
		w.Header().Set("Upload-Offset", "0")
		w.Header().Set("Upload-Complete", "?1")
		w.WriteHeader(http.StatusCreated)
	}
	n.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hold := n.held(); hold != nil && r.Method == http.MethodPut {
			<-hold
		}
		b, _ := io.ReadAll(r.Body)
		n.mu.Lock()
		n.got = append(n.got, seen{r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Clone(), r.Trailer.Clone(), b})
		answer := n.answer
		n.mu.Unlock()
		answer(w)
	}))
	t.Cleanup(n.srv.Close)

	return n
}

func (n *node) held() chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.hold
}

func (n *node) requests() []seen {
	n.mu.Lock()
	defer n.mu.Unlock()

	return append([]seen(nil), n.got...)
}

// rig is a satellite's proxy behind TLS from a test CA, routing to the
// nodes it was given, verifying tokens of one key, serving the producers
// assigned to it.
type rig struct {
	t      *testing.T
	ca     *pki.CA
	pool   *x509.CertPool
	key    token.Key
	proxy  *Proxy
	srv    *httptest.Server
	nodes  map[pdid.Id]*node
	asked  atomic.Int64
	routed atomic.Int64
}

func newRig(t *testing.T, budget int64, size int, nodes ...*node) *rig {
	t.Helper()
	now := time.Now()
	ca, err := pki.NewCA("test", now)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	key, err := token.Generate("k1")
	require.NoError(t, err)

	r := &rig{t: t, ca: ca, pool: pool, key: key, nodes: map[pdid.Id]*node{}}
	for _, n := range nodes {
		r.nodes[n.id] = n
	}
	v := token.NewVerifier()
	v.Set(map[string]ed25519.PublicKey{"k1": key.Public()})
	r.proxy = &Proxy{
		Verifier: v,
		Client:   &http.Client{},
		Log:      slog.New(slog.DiscardHandler),
		assigned: &assigned{},
		pool:     newPool(budget, size),
	}
	r.proxy.assigned.refresh = func(context.Context) error { r.asked.Add(1); return nil }
	r.proxy.routes = newRoutes(func(_ context.Context, id pdid.Id) (*api.SatelliteRouteResponse, error) {
		r.routed.Add(1)
		n, ok := r.nodes[id]
		if !ok {
			return nil, errors.New("no such node")
		}
		host, port, _ := net.SplitHostPort(strings.TrimPrefix(n.srv.URL, "http://"))
		p, _ := strconv.Atoi(port)

		return api.SatelliteRouteResponse_builder{
			Endpoints:   []*api.Endpoint{api.Endpoint_builder{Scheme: "http", Host: host, Port: int32(p)}.Build()},
			DateExpires: timestamppb.New(time.Now().Add(time.Minute)),
		}.Build(), nil
	}, true)

	// The satellite's own certificate, for 127.0.0.1.
	sat := r.issue(pdid.New(DomSatellite), pki.Names{IPs: []net.IP{net.ParseIP("127.0.0.1")}})
	r.srv = httptest.NewUnstartedServer(r.proxy)
	r.srv.EnableHTTP2 = true
	r.srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{sat},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}
	r.srv.StartTLS()
	t.Cleanup(r.srv.Close)

	return r
}

// issue is a host certificate from the rig's CA.
func (r *rig) issue(id pdid.Id, names pki.Names) tls.Certificate {
	r.t.Helper()
	k, err := pki.NewHostKey()
	require.NoError(r.t, err)
	csr, err := k.CSR("host")
	require.NoError(r.t, err)
	c, err := r.ca.IssueHost(csr, id, names, time.Now())
	require.NoError(r.t, err)
	kb, err := pki.EncodeKey(k.Key)
	require.NoError(r.t, err)
	pair, err := tls.X509KeyPair(pki.EncodeCerts(c, r.ca.Cert), kb)
	require.NoError(r.t, err)
	pair.Leaf = c

	return pair
}

// producer is an adopted producer: its certificate, and a client that
// presents it.
type producer struct {
	id     pdid.Id
	cert   tls.Certificate
	client *http.Client
}

func (r *rig) producer(assign bool) *producer {
	id := pdid.New(DomProducer)
	cert := r.issue(id, pki.Names{})
	p := &producer{id: id, cert: cert, client: r.client(&cert)}
	if assign {
		r.assign(p)
	}

	return p
}

func (r *rig) assign(ps ...*producer) {
	var vs []*api.SatelliteProducer
	for _, p := range ps {
		vs = append(vs, api.SatelliteProducer_builder{ProducerId: p.id.Bytes(), CertSerial: pki.Serial(p.cert.Leaf)}.Build())
	}
	r.proxy.assigned.set(vs)
}

func (r *rig) client(cert *tls.Certificate) *http.Client {
	cfg := &tls.Config{RootCAs: r.pool, MinVersion: tls.VersionTLS12}
	if cert != nil {
		cfg.Certificates = []tls.Certificate{*cert}
	}

	return &http.Client{Transport: &http.Transport{TLSClientConfig: cfg, ForceAttemptHTTP2: true}}
}

// token is a put token for key on node n, issued to producer p.
func (r *rig) token(n pdid.Id, key string, actor pdid.Id, edit ...func(*api.TokenClaims)) string {
	c := api.TokenClaims_builder{
		Exp: timestamppb.New(time.Now().Add(time.Hour)), Iat: timestamppb.Now(),
		Aud: n.Bytes(), Op: api.TokenOp_TOKEN_OP_PUT, LaminaKey: key, Actor: actor.Bytes(),
		SinkId: pdid.New(14).Bytes(),
	}.Build()
	for _, e := range edit {
		e(c)
	}
	tok, err := r.key.Sign(c)
	require.NoError(r.t, err)

	return tok
}

func (r *rig) put(p *producer, key, tok string, body []byte) *http.Response {
	r.t.Helper()
	req, err := http.NewRequest(http.MethodPut, r.srv.URL+"/"+key, bytes.NewReader(body))
	require.NoError(r.t, err)
	req.Header.Set("Authorization", token.Scheme+" "+tok)
	req.Header.Set("Upload-Offset", "0")
	resp, err := p.client.Do(req)
	require.NoError(r.t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	return resp
}

const key = "laminae/2026/11/15/00/abc"

// The request goes on to the node the token names, the path, the upload
// headers, the body and the trailers as the producer sent them, the token
// in the Authorization header even when it came in the URL; the node's
// answer comes back as it gave it: status, headers, trailers, body. The
// node's Alt-Svc describes the node's origin, not the satellite's, and is
// not passed on.
func TestProxyPassesTheRequestAndTheAnswer(t *testing.T) {
	n := newNode(t)
	n.answer = func(w http.ResponseWriter) {
		h := w.Header()
		h.Set("Trailer", "Shale-Test-Trailer")
		h.Set("Upload-Offset", "1048576")
		h.Set("Upload-Complete", "?1")
		h.Set("Shale-Incomplete", "?1")
		h.Set("Alt-Svc", `h3=":7410"; ma=3600`)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("stored\n"))
		h.Set("Shale-Test-Trailer", "after")
	}
	r := newRig(t, 64<<20, 64<<10, n)
	p := r.producer(true)
	body := bytes.Repeat([]byte("frame"), 300_000)
	tok := r.token(n.id, key, p.id)

	// Live: chunked, with the end time as a trailer (§12.2).
	ended := time.Now().UTC().Format(time.RFC3339Nano)
	tb := &trailing{r: bytes.NewReader(body)}
	req, err := http.NewRequest(http.MethodPut, r.srv.URL+"/"+key, tb)
	require.NoError(t, err)
	tb.req, tb.value = req, ended
	req.ContentLength = -1
	req.Trailer = http.Header{"Shale-Date-Ended": nil}
	req.Header.Set("Authorization", token.Scheme+" "+tok)
	req.Header.Set("Upload-Offset", "0")
	req.Header.Set("Upload-Complete", "?1")
	req.Header.Set("Shale-Size-Hint", "2000000")
	req.Header.Set("Shale-Date-Started", "2026-11-15T00:00:00Z")
	req.Header.Set(HdrProducer, pdid.New(DomProducer).String())
	req.Host = "elsewhere.example:443"
	resp, err := p.client.Do(req)
	require.NoError(t, err)
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "stored\n", string(got))
	require.Equal(t, "1048576", resp.Header.Get("Upload-Offset"))
	require.Equal(t, "?1", resp.Header.Get("Upload-Complete"))
	require.Equal(t, "?1", resp.Header.Get("Shale-Incomplete"))
	require.Empty(t, resp.Header.Get("Alt-Svc"))
	require.Empty(t, resp.Header.Get(HdrSatellite), "the node's answer, not the satellite's")
	require.Equal(t, "after", resp.Trailer.Get("Shale-Test-Trailer"))

	vs := n.requests()
	require.Len(t, vs, 1)
	v := vs[0]
	require.Equal(t, http.MethodPut, v.method)
	require.Equal(t, "/"+key, v.path)
	require.Empty(t, v.rawQuery)
	require.Equal(t, token.Scheme+" "+tok, v.header.Get("Authorization"))
	require.Equal(t, "0", v.header.Get("Upload-Offset"))
	require.Equal(t, "?1", v.header.Get("Upload-Complete"))
	require.Equal(t, "2000000", v.header.Get("Shale-Size-Hint"))
	require.Equal(t, "2026-11-15T00:00:00Z", v.header.Get("Shale-Date-Started"))
	require.Empty(t, v.header.Get(HdrProducer), "development mode's identity is the satellite's alone")
	require.Equal(t, ended, v.trailer.Get("Shale-Date-Ended"))
	require.Equal(t, body, v.body)
	require.Equal(t, int64(len(body)), r.proxy.forwarded.Load())

	// The token in the URL, a buffered body of a known length, and HEAD.
	req, err = http.NewRequest(http.MethodPut, r.srv.URL+"/"+key+"?token="+tok, bytes.NewReader(body[:1000]))
	require.NoError(t, err)
	req.Header.Set("Upload-Offset", "0")
	req.Header.Set("Upload-Length", "1000")
	resp, err = p.client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	req, err = http.NewRequest(http.MethodHead, r.srv.URL+"/"+key, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", token.Scheme+" "+tok)
	resp, err = p.client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "1048576", resp.Header.Get("Upload-Offset"))

	vs = n.requests()
	require.Len(t, vs, 3)
	require.Empty(t, vs[1].rawQuery, "the token is in no URL the node is sent")
	require.Equal(t, token.Scheme+" "+tok, vs[1].header.Get("Authorization"))
	require.Equal(t, "1000", vs[1].header.Get("Upload-Length"))
	require.Equal(t, body[:1000], vs[1].body)
	require.Equal(t, http.MethodHead, vs[2].method)
}

// trailing is a body that fills the request's trailer once it has ended,
// as the producer's live body does.
type trailing struct {
	r     io.Reader
	req   *http.Request
	value string
}

func (b *trailing) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err == io.EOF {
		b.req.Trailer.Set("Shale-Date-Ended", b.value)
	}

	return n, err
}

// The node's own answers come back as the node gave them: an offset that
// differs, a body over max_length, a node too busy, each with the headers
// that go with it. None of them is the satellite's.
func TestProxyPassesTheNodesRefusals(t *testing.T) {
	n := newNode(t)
	r := newRig(t, 64<<20, 64<<10, n)
	p := r.producer(true)
	tok := r.token(n.id, key, p.id)
	for _, c := range []struct {
		status int
		header map[string]string
	}{
		{http.StatusConflict, map[string]string{"Upload-Offset": "4096", "Upload-Complete": "?0"}},
		{http.StatusRequestEntityTooLarge, nil},
		{http.StatusServiceUnavailable, map[string]string{"Retry-After": "10"}},
		{http.StatusOK, map[string]string{"Upload-Complete": "?1", "Upload-Offset": "77", "Shale-Incomplete": "?1"}},
		{http.StatusNoContent, map[string]string{"Upload-Offset": "65536", "Upload-Complete": "?0"}},
	} {
		n.mu.Lock()
		n.answer = func(w http.ResponseWriter) {
			for k, v := range c.header {
				w.Header().Set(k, v)
			}
			w.WriteHeader(c.status)
		}
		n.mu.Unlock()
		resp := r.put(p, key, tok, []byte("bytes"))
		require.Equal(t, c.status, resp.StatusCode)
		for k, v := range c.header {
			require.Equal(t, v, resp.Header.Get(k), "%d %s", c.status, k)
		}
		require.Empty(t, resp.Header.Get(HdrSatellite), "%d", c.status)
	}
}

// Only an adopted producer assigned to this satellite is served, with the
// certificate the CP knows: no certificate is no handshake; another host's
// certificate, an unassigned producer's, or a replaced one is refused
// before anything reaches a node, and the refusal says it is the
// satellite's. A caller it does not know has the list asked for again
// first.
func TestProxyServesOnlyItsProducers(t *testing.T) {
	n := newNode(t)
	r := newRig(t, 64<<20, 64<<10, n)
	assignedP := r.producer(true)
	other := r.producer(false)

	// No client certificate: the handshake fails.
	req, err := http.NewRequest(http.MethodPut, r.srv.URL+"/"+key, strings.NewReader("x"))
	require.NoError(t, err)
	req.Header.Set("Authorization", token.Scheme+" "+r.token(n.id, key, assignedP.id))
	_, err = r.client(nil).Do(req)
	require.Error(t, err)

	refused := func(p *producer, actor pdid.Id) {
		t.Helper()
		resp := r.put(p, key, r.token(n.id, key, actor), []byte("x"))
		require.Equal(t, http.StatusForbidden, resp.StatusCode)
		require.Equal(t, ReasonUnassigned, resp.Header.Get(HdrSatellite))
	}
	asked := r.asked.Load()
	refused(other, other.id)
	require.Greater(t, r.asked.Load(), asked, "a caller it does not know has the list asked for again")

	// A node's certificate, or a relay's, is not a producer's.
	nodeCert := r.issue(pdid.New(DomNode), pki.Names{})
	refused(&producer{id: n.id, client: r.client(&nodeCert)}, n.id)

	// The assigned producer's id with a certificate the CP does not know:
	// replaced, or another key's.
	replaced := r.issue(assignedP.id, pki.Names{})
	refused(&producer{id: assignedP.id, client: r.client(&replaced)}, assignedP.id)

	require.Empty(t, n.requests(), "nothing reached the node")

	// Assigned now: served.
	r.assign(assignedP, other)
	resp := r.put(other, key, r.token(n.id, key, other.id), []byte("x"))
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Len(t, n.requests(), 1)
}

// The token is checked as the node it names would check it, and it must be
// the calling producer's own: a refusal of any of it reaches no node.
func TestProxyChecksTheToken(t *testing.T) {
	n := newNode(t)
	r := newRig(t, 64<<20, 64<<10, n)
	p := r.producer(true)
	other := pdid.New(DomProducer)
	stranger, err := token.Generate("k1")
	require.NoError(t, err)
	forged, err := stranger.Sign(api.TokenClaims_builder{
		Exp: timestamppb.New(time.Now().Add(time.Hour)), Aud: n.id.Bytes(), Op: api.TokenOp_TOKEN_OP_PUT, LaminaKey: key, Actor: p.id.Bytes(),
	}.Build())
	require.NoError(t, err)

	for _, c := range []struct {
		name   string
		tok    string
		status int
	}{
		{"none", "", http.StatusUnauthorized},
		{"malformed", "nonsense", http.StatusUnauthorized},
		{"another key's signature", forged, http.StatusUnauthorized},
		{"expired", r.token(n.id, key, p.id, func(c *api.TokenClaims) { c.SetExp(timestamppb.New(time.Now().Add(-time.Hour))) }), http.StatusUnauthorized},
		{"a get token", r.token(n.id, key, p.id, func(c *api.TokenClaims) { c.SetOp(api.TokenOp_TOKEN_OP_GET) }), http.StatusForbidden},
		{"a publish token", r.token(n.id, key, p.id, func(c *api.TokenClaims) { c.SetOp(api.TokenOp_TOKEN_OP_PUBLISH) }), http.StatusForbidden},
		{"another key", r.token(n.id, key+"x", p.id), http.StatusForbidden},
		{"another producer's", r.token(n.id, key, other), http.StatusForbidden},
		{"for a relay", r.token(pdid.New(24), key, p.id), http.StatusForbidden},
		{"for the satellite", r.token(pdid.New(DomSatellite), key, p.id), http.StatusForbidden},
	} {
		req, err := http.NewRequest(http.MethodPut, r.srv.URL+"/"+key, strings.NewReader("x"))
		require.NoError(t, err)
		req.Header.Set("Upload-Offset", "0")
		if c.tok != "" {
			req.Header.Set("Authorization", token.Scheme+" "+c.tok)
		}
		resp, err := p.client.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, c.status, resp.StatusCode, c.name)
		require.Equal(t, ReasonToken, resp.Header.Get(HdrSatellite), c.name)
	}
	require.Empty(t, n.requests())
	require.Zero(t, r.routed.Load(), "no route was asked for a token refused")
}

// The node is the one the token names, at the address the CP gives for
// it: not a host the request names, in its URL, its Host or a header, and
// not a node the CP has no route to.
func TestProxyForwardsOnlyToTheTokensNode(t *testing.T) {
	a, b := newNode(t), newNode(t)
	r := newRig(t, 64<<20, 64<<10, a, b)
	p := r.producer(true)

	// A token for B, sent with every way a request could name A.
	req, err := http.NewRequest(http.MethodPut, r.srv.URL+"/"+key, strings.NewReader("for b"))
	require.NoError(t, err)
	req.Host = strings.TrimPrefix(a.srv.URL, "http://")
	req.Header.Set("Authorization", token.Scheme+" "+r.token(b.id, key, p.id))
	req.Header.Set("Upload-Offset", "0")
	req.Header.Set("X-Forwarded-Host", req.Host)
	req.Header.Set("Forwarded", "host="+req.Host)
	resp, err := p.client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Empty(t, a.requests())
	require.Len(t, b.requests(), 1)
	require.Equal(t, []byte("for b"), b.requests()[0].body)

	// A node the CP gives no route to.
	resp = r.put(p, key, r.token(pdid.New(DomNode), key, p.id), []byte("x"))
	require.Equal(t, http.StatusBadGateway, resp.StatusCode)
	require.Equal(t, ReasonUnreachable, resp.Header.Get(HdrSatellite))

	// A path that is no upload path, and a method that is not an upload's.
	for _, c := range []struct{ method, path string }{
		{http.MethodPut, "/reports/export"},
		{http.MethodGet, "/" + key},
		{http.MethodPost, "/" + key},
	} {
		req, err := http.NewRequest(c.method, r.srv.URL+c.path, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", token.Scheme+" "+r.token(b.id, strings.TrimPrefix(c.path, "/"), p.id))
		resp, err := p.client.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, ReasonRequest, resp.Header.Get(HdrSatellite), c.method+" "+c.path)
	}
	require.Len(t, b.requests(), 1)
	require.Empty(t, a.requests())
}

// The in-flight pool bounds the requests passing through: one over it is
// refused 503 with Retry-After, as a busy node refuses one, and a HEAD,
// which has no body, is not; once a request ends its share is free again.
func TestProxyRefusesOverTheBudget(t *testing.T) {
	n := newNode(t)
	n.hold = make(chan struct{})
	const size = 64 << 10
	r := newRig(t, 2*size, size, n)
	p := r.producer(true)
	tok := r.token(n.id, key, p.id)

	first := make(chan *http.Response, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPut, r.srv.URL+"/"+key, strings.NewReader("held"))
		req.Header.Set("Authorization", token.Scheme+" "+tok)
		req.Header.Set("Upload-Offset", "0")
		resp, err := p.client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		first <- resp
	}()
	require.Eventually(t, func() bool { return r.proxy.pool.requests.Load() == 1 }, 5*time.Second, 10*time.Millisecond)
	used, _, budget := r.proxy.pool.used.Load(), 0, r.proxy.pool.budget
	require.Equal(t, budget, used)

	resp := r.put(p, key, tok, []byte("second"))
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, busyRetryAfter, resp.Header.Get("Retry-After"))
	require.Equal(t, ReasonBusy, resp.Header.Get(HdrSatellite))
	require.Equal(t, int64(1), r.proxy.refused.Load())
	req, err := http.NewRequest(http.MethodHead, r.srv.URL+"/"+key, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", token.Scheme+" "+tok)
	resp, err = p.client.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode, "a HEAD holds no buffer")

	n.mu.Lock()
	close(n.hold)
	n.hold = nil
	n.mu.Unlock()
	require.Equal(t, http.StatusCreated, (<-first).StatusCode)
	require.Eventually(t, func() bool { return r.proxy.pool.used.Load() == 0 }, 5*time.Second, 10*time.Millisecond)
	resp = r.put(p, key, tok, []byte("third"))
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, 2*size, int(r.proxy.pool.peak.Load()))
}

func TestNewRefusesABudgetBelowOneRequest(t *testing.T) {
	_, err := New(Config{StateDir: t.TempDir(), MemoryBudget: 1 << 20, RequestBuffer: 1 << 20})
	require.Error(t, err)
	_, err = New(Config{StateDir: t.TempDir()})
	require.NoError(t, err)
}
