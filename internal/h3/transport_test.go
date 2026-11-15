package h3

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/stretchr/testify/require"
)

func TestParseMode(t *testing.T) {
	for in, want := range map[string]Mode{"": Off, "off": Off, "OFF": Off, "false": Off, "auto": Auto, " Auto ": Auto, "on": On, "true": On} {
		got, err := ParseMode(in)
		require.NoError(t, err, in)
		require.Equal(t, want, got, in)
	}
	_, err := ParseMode("quic")
	require.Error(t, err)
}

// A node offers HTTP/3 on the port the client dialed, and only an offer on
// the origin's own port is taken (§35.6).
func TestAdvertised(t *testing.T) {
	for _, c := range []struct {
		field  string
		origin string
		ma     time.Duration
		ok     bool
	}{
		{AltSvc("192.0.2.1:7410"), "192.0.2.1:7410", AltSvcMaxAge, true},
		{`h3=":7410"`, "192.0.2.1:7410", 24 * time.Hour, true},
		{`h3=":7411"; ma=60`, "192.0.2.1:7410", 0, false},
		{`h3="node-1.example.com:7410"; ma=60`, "node-1.example.com:7410", time.Minute, true},
		{`h3="NODE-1.example.com:7410"; ma=60`, "node-1.example.com:7410", time.Minute, true},
		{`h3="node-2.example.com:7410"; ma=60`, "node-1.example.com:7410", 0, false},
		{`h2=":7410"`, "192.0.2.1:7410", 0, false},
		{`h3-29=":7410", h3=":7410"; ma=60; persist=1`, "192.0.2.1:7410", time.Minute, true},
		{`h3=":7410"; ma=0`, "192.0.2.1:7410", 0, false},
		{`clear`, "192.0.2.1:7410", 0, false},
		{`h3=:7410`, "192.0.2.1:7410", 0, false},
		{``, "192.0.2.1:7410", 0, false},
	} {
		h := http.Header{}
		if c.field != "" {
			h.Set("Alt-Svc", c.field)
		}
		ma, ok := Advertised(h, c.origin)
		require.Equal(t, c.ok, ok, c.field)
		require.Equal(t, c.ma, ma, c.field)
	}
	require.Equal(t, `h3=":443"; ma=3600`, AltSvc("node-1.example.com"))
}

// seen is one request as a test node had it.
type seen struct {
	proto   string
	body    string
	trailer string
}

// testNode is a TLS server on TCP and, on the same port, HTTP/3 on UDP, or
// a UDP socket that answers nothing: a firewall that drops it. Every
// answer offers HTTP/3, as a node's does.
type testNode struct {
	addr string
	pool *x509.CertPool
	tcp  *httptest.Server
	h3   *http3.Server
	seen chan seen
}

func startNode(t *testing.T, serveUDP bool) *testNode {
	t.Helper()
	n := &testNode{seen: make(chan seen, 16)}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Alt-Svc", AltSvc(r.Host))
		if r.URL.Path == "/reset" {
			panic(http.ErrAbortHandler)
		}
		b, _ := io.ReadAll(r.Body)
		n.seen <- seen{proto: r.Proto, body: string(b), trailer: r.Trailer.Get("Shale-Date-Ended")}
		w.WriteHeader(http.StatusCreated)
	})
	n.tcp = httptest.NewUnstartedServer(handler)
	n.tcp.EnableHTTP2 = true
	n.tcp.StartTLS()
	t.Cleanup(n.tcp.Close)
	n.addr = n.tcp.Listener.Addr().String()
	n.pool = x509.NewCertPool()
	n.pool.AddCert(n.tcp.Certificate())

	ua, err := net.ResolveUDPAddr("udp", n.addr)
	require.NoError(t, err)
	udp, err := net.ListenUDP("udp", ua)
	require.NoError(t, err)
	t.Cleanup(func() { udp.Close() })
	if !serveUDP {
		go func() {
			buf := make([]byte, 2048)
			for {
				if _, _, err := udp.ReadFrom(buf); err != nil {
					return
				}
			}
		}()
		return n
	}
	tr := &quic.Transport{Conn: udp}
	ln, err := tr.Listen(http3.ConfigureTLSConfig(&tls.Config{Certificates: n.tcp.TLS.Certificates}), ServerConfig())
	require.NoError(t, err)
	n.h3 = &http3.Server{Handler: handler}
	go n.h3.ServeListener(ln)
	t.Cleanup(func() { n.h3.Close(); ln.Close(); tr.Close() })

	return n
}

func (n *testNode) client(t *testing.T, mode Mode) (*Transport, *http.Client) {
	t.Helper()
	tcp := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: n.pool}, ForceAttemptHTTP2: true}
	tr := NewTransport(tcp, StaticTLS(&tls.Config{RootCAs: n.pool}), mode, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { tr.Close() })

	return tr, &http.Client{Transport: tr}
}

func (n *testNode) put(t *testing.T, c *http.Client, path string, body io.Reader) (seen, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, "https://"+n.addr+path, body)
	require.NoError(t, err)
	resp, err := c.Do(req)
	if err != nil {
		return seen{}, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	select {
	case s := <-n.seen:
		require.Equal(t, s.proto, resp.Proto, "the answer came the way the request went")
		return s, nil
	case <-time.After(5 * time.Second):
		t.Fatal("the node saw no request")
	}

	return seen{}, nil
}

// On auto, the first request goes over TCP, which says the node serves
// HTTP/3 on its port, and the next over HTTP/3.
func TestAutoTakesTheOffer(t *testing.T) {
	n := startNode(t, true)
	tr, c := n.client(t, Auto)
	require.False(t, tr.Speaks(n.addr), "nothing offered yet")

	s, err := n.put(t, c, "/laminae/a", strings.NewReader("first"))
	require.NoError(t, err)
	require.NotEqual(t, "HTTP/3.0", s.proto)
	require.True(t, tr.Speaks(n.addr), "the answer offered HTTP/3")

	s, err = n.put(t, c, "/laminae/a", strings.NewReader("second"))
	require.NoError(t, err)
	require.Equal(t, "HTTP/3.0", s.proto)
	require.Equal(t, "second", s.body)
}

// Off is TCP alone, offer or not, and opens no UDP socket.
func TestOffIsTCP(t *testing.T) {
	n := startNode(t, true)
	tr, c := n.client(t, Off)
	for range 3 {
		s, err := n.put(t, c, "/laminae/a", strings.NewReader("x"))
		require.NoError(t, err)
		require.NotEqual(t, "HTTP/3.0", s.proto)
	}
	require.Nil(t, tr.udp)
}

// A live upload over HTTP/3: a body that grows while it is sent, of no
// length known beforehand, and the end time as a trailer once it ends
// (§12.2), as the producer's uploader sends it.
func TestStreamingBodyAndTrailer(t *testing.T) {
	n := startNode(t, true)
	_, c := n.client(t, On)
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPut, "https://"+n.addr+"/laminae/live", pr)
	require.NoError(t, err)
	req.ContentLength = -1
	req.Trailer = http.Header{"Shale-Date-Ended": nil}
	var want strings.Builder
	wrote := make(chan struct{})
	go func() {
		defer close(wrote)
		for i := range 20 {
			chunk := fmt.Sprintf("part-%02d;", i)
			want.WriteString(chunk)
			pw.Write([]byte(chunk))
			time.Sleep(5 * time.Millisecond)
		}
		req.Trailer.Set("Shale-Date-Ended", "2026-11-15T00:00:00Z")
		pw.Close()
	}()
	resp, err := c.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	s := <-n.seen
	<-wrote
	require.Equal(t, "HTTP/3.0", s.proto)
	require.Equal(t, want.String(), s.body)
	require.Equal(t, "2026-11-15T00:00:00Z", s.trailer)
}

// UDP dropped on the way: the handshake times out, the same request goes
// over TCP with its body whole, and the node stays on TCP for a pause that
// doubles while HTTP/3 keeps failing, rather than a timeout every part.
func TestFallsBackWhenUDPIsDropped(t *testing.T) {
	n := startNode(t, false)
	tr, c := n.client(t, On)
	tr.h3.QUICConfig.HandshakeIdleTimeout = 300 * time.Millisecond
	now := time.Now()
	var mu sync.Mutex
	tr.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return now }

	s, err := n.put(t, c, "/laminae/a", strings.NewReader("the whole body"))
	require.NoError(t, err)
	require.NotEqual(t, "HTTP/3.0", s.proto)
	require.Equal(t, "the whole body", s.body, "nothing of it was lost to the attempt")
	require.False(t, tr.Speaks(n.addr), "paused")

	began := time.Now()
	s, err = n.put(t, c, "/laminae/a", strings.NewReader("again"))
	require.NoError(t, err)
	require.NotEqual(t, "HTTP/3.0", s.proto)
	require.Less(t, time.Since(began), 250*time.Millisecond, "no handshake is waited for while paused")

	// After the pause HTTP/3 is tried again; failing, the pause doubles.
	mu.Lock()
	now = now.Add(FirstPause + time.Second)
	mu.Unlock()
	require.True(t, tr.Speaks(n.addr))
	_, err = n.put(t, c, "/laminae/a", strings.NewReader("later"))
	require.NoError(t, err)
	key := authority(n.addr)
	tr.mu.Lock()
	paused := tr.origins[key].paused.Sub(tr.now())
	tr.mu.Unlock()
	require.Equal(t, 2*FirstPause, paused)
}

// A node that TCP does not reach either is away, not behind a firewall
// that drops UDP: HTTP/3 is not paused, and is what it is spoken to on
// once it is back.
func TestNodeAwayIsNotPaused(t *testing.T) {
	n := startNode(t, false)
	tr, c := n.client(t, On)
	tr.h3.QUICConfig.HandshakeIdleTimeout = 300 * time.Millisecond
	n.tcp.Close()
	_, err := n.put(t, c, "/laminae/a", strings.NewReader("x"))
	require.Error(t, err)
	require.True(t, tr.Speaks(n.addr))
}

// The TLS a handshake starts from is asked for at every handshake: a
// producer's roots follow the bundle it holds (§33.5), and a node it could
// not verify before is verified once the bundle carries its CA.
func TestTLSAskedAtEveryHandshake(t *testing.T) {
	n := startNode(t, true)
	var mu sync.Mutex
	roots := x509.NewCertPool()
	tlsFor := func() (*tls.Config, error) {
		mu.Lock()
		defer mu.Unlock()
		return &tls.Config{RootCAs: roots}, nil
	}
	tr := NewTransport(&http.Transport{TLSClientConfig: &tls.Config{RootCAs: x509.NewCertPool()}}, tlsFor, On, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { tr.Close() })
	c := &http.Client{Transport: tr}

	_, err := n.put(t, c, "/laminae/a", strings.NewReader("x"))
	require.Error(t, err, "a node of a CA not held")
	mu.Lock()
	roots = n.pool
	mu.Unlock()
	s, err := n.put(t, c, "/laminae/a", strings.NewReader("y"))
	require.NoError(t, err)
	require.Equal(t, "HTTP/3.0", s.proto)
}

// A request that fails after the handshake is the request's, as over TCP:
// it is not sent again elsewhere, and HTTP/3 stays.
func TestFailureAfterHandshakeIsTheRequests(t *testing.T) {
	n := startNode(t, true)
	tr, c := n.client(t, On)
	_, err := n.put(t, c, "/reset", strings.NewReader("body"))
	require.Error(t, err)
	select {
	case s := <-n.seen:
		t.Fatalf("sent again: %+v", s)
	default:
	}
	require.True(t, tr.Speaks(n.addr))
	s, err := n.put(t, c, "/laminae/a", strings.NewReader("next"))
	require.NoError(t, err)
	require.Equal(t, "HTTP/3.0", s.proto)
}

// http (development mode) is TCP's whatever the mode.
func TestPlaintextIsTCP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Alt-Svc", AltSvc(r.Host))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	tr := NewTransport(http.DefaultTransport, nil, On, slog.New(slog.DiscardHandler))
	defer tr.Close()
	resp, err := (&http.Client{Transport: tr}).Get(srv.URL)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, "HTTP/1.1", resp.Proto)
	require.Nil(t, tr.udp)
}
