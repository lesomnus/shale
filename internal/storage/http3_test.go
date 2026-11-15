package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/lesomnus/otx"
	"github.com/lesomnus/payday/pdid"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/h3"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/token"
)

// h3Node is a node with a host certificate from a shale CA, one sink and a
// signing key, serving its data plane on loopback as Run does.
type h3Node struct {
	n      *Node
	addr   string
	served bool
	ca     *pki.CA
	pool   *x509.CertPool
	reader *sdkmetric.ManualReader
	ctx    context.Context
	// sign is a token for the key; idle is its idle_timeout in seconds.
	sign func(key string, op api.TokenOp, idle int64) string
}

func serveH3(t *testing.T, cfg Config, dl net.Listener) h3Node {
	t.Helper()
	cfg.Log = slog.New(slog.DiscardHandler)
	cfg.Limits = DefaultLimits
	cfg.Limits.PartSize = 64 << 10
	n, ca := adopted(t, cfg)
	s, err := OpenSink(SinkConfig{Path: filepath.Join(t.TempDir(), "sink"), Capacity: 1 << 30, Device: "disk-h3"}, DefaultWatermarks)
	require.NoError(t, err)
	n.sinks = append(n.sinks, s)
	n.byId[s.Id] = s
	n.id = pdid.New(DomNode)
	reader := sdkmetric.NewManualReader()
	mctx := otx.Into(context.Background(), otx.New(otx.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))))
	n.m = newMetrics(mctx)
	k, err := token.Generate("k-h3")
	require.NoError(t, err)
	n.verifier.Add("k-h3", k.Public())

	tlsCfg, _, err := n.tlsConfigs()
	require.NoError(t, err)
	if dl == nil {
		dl, err = net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	g, gctx := errgroup.WithContext(ctx)
	served := n.serveData(gctx, g, dl, tlsCfg)
	t.Cleanup(func() { cancel(); g.Wait() })
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)

	sign := func(key string, op api.TokenOp, idle int64) string {
		lamina, attempt := pdid.New(pdid.Domain(9)), pdid.New(pdid.Domain(10))
		now := time.Now()
		tok, err := k.Sign(api.TokenClaims_builder{
			Exp: timestamppb.New(now.Add(time.Hour)), Iat: timestamppb.New(now),
			Aud: n.id.Bytes(), Op: op, SinkId: s.Id.Bytes(), LaminaKey: key, AttemptId: attempt.Bytes(),
			Record: api.LaminaRecord_builder{
				FormatVersion: FormatVersion, LaminaId: lamina.Bytes(), AttemptId: attempt.Bytes(),
				DateStartedMs: now.UnixMilli(), SizeHint: 1 << 20,
			}.Build(),
			MaxLength: 8 << 20, Mode: api.UploadMode_UPLOAD_MODE_LIVE,
			IdleTimeoutSeconds: idle, AbandonTimeoutSeconds: 60,
		}.Build())
		require.NoError(t, err)

		return tok
	}

	return h3Node{n: n, addr: dl.Addr().String(), served: served, ca: ca, pool: pool, reader: reader, ctx: mctx, sign: sign}
}

// client is a data-plane client of the node's CA: HTTP/3 to it from the
// first request with On, TCP alone with Off.
func (hn h3Node) client(t *testing.T, mode h3.Mode) (*h3.Transport, *http.Client) {
	t.Helper()
	tcp := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: hn.pool}, ForceAttemptHTTP2: true}
	tr := h3.NewTransport(tcp, h3.StaticTLS(&tls.Config{RootCAs: hn.pool}), mode, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { tr.Close() })

	return tr, &http.Client{Transport: tr}
}

func (hn h3Node) do(t *testing.T, c *http.Client, method, key, tok string, hdr map[string]string, body io.Reader) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, "https://"+hn.addr+"/"+key, body)
	require.NoError(t, err)
	req.Header.Set("Authorization", token.Scheme+" "+tok)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	if b, ok := body.(*bytes.Reader); ok {
		req.ContentLength = int64(b.Len())
	} else if body != nil {
		req.ContentLength = -1
	}
	resp, err := c.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })

	return resp
}

// The data plane over HTTP/3 is the same data plane (§35.6): an upload, a
// HEAD and a ranged GET answer as over TCP, and every answer, over either,
// offers HTTP/3 on the port the client came to.
func TestHTTP3DataPlane(t *testing.T) {
	hn := serveH3(t, Config{HTTP3: true}, nil)
	require.True(t, hn.served)
	_, port, _ := net.SplitHostPort(hn.addr)
	altSvc := fmt.Sprintf(`h3=":%s"; ma=3600`, port)

	_, tcp := hn.client(t, h3.Off)
	resp := hn.do(t, tcp, http.MethodGet, "healthz", "", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "HTTP/2.0", resp.Proto)
	require.Equal(t, altSvc, resp.Header.Get("Alt-Svc"))

	_, c := hn.client(t, h3.On)
	key := "laminae/h3/whole"
	body := make([]byte, 300_000)
	rand.Read(body)
	ended := time.Now().UTC().Truncate(time.Millisecond)
	resp = hn.do(t, c, http.MethodPut, key, hn.sign(key, api.TokenOp_TOKEN_OP_PUT, 30), map[string]string{
		HdrUploadOffset: "0", HdrUploadComplete: "?1", HdrUploadLength: strconv.Itoa(len(body)),
		HdrDateEnded: ended.Format(time.RFC3339Nano),
	}, bytes.NewReader(body))
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "HTTP/3.0", resp.Proto)
	require.Equal(t, "?1", resp.Header.Get(HdrUploadComplete))
	require.Equal(t, altSvc, resp.Header.Get("Alt-Svc"))

	get := hn.sign(key, api.TokenOp_TOKEN_OP_GET, 30)
	resp = hn.do(t, c, http.MethodHead, key, get, nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "HTTP/3.0", resp.Proto)
	require.Equal(t, "?1", resp.Header.Get(HdrUploadComplete))
	require.Equal(t, strconv.Itoa(len(body)), resp.Header.Get(HdrUploadOffset))

	resp = hn.do(t, c, http.MethodGet, key, get, map[string]string{"Range": "bytes=1000-1999"}, nil)
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	require.Equal(t, "HTTP/3.0", resp.Proto)
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, body[1000:2000], got)

	// Counted by version.
	var rm metricdata.ResourceMetrics
	require.NoError(t, hn.reader.Collect(hn.ctx, &rm))
	byProto := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "shale.node.requests" {
				continue
			}
			for _, p := range m.Data.(metricdata.Sum[int64]).DataPoints {
				v, _ := p.Attributes.Value("proto")
				byProto[v.AsString()] += p.Value
			}
		}
	}
	require.Equal(t, map[string]int64{"h2": 1, "h3": 3}, byProto)
}

// A live upload in parts over HTTP/3 (§12.2): a body that grows, of no
// length known, ending without completing; then the rest from the offset
// the node reported, completing, with the end time as a trailer.
func TestHTTP3LiveUpload(t *testing.T) {
	hn := serveH3(t, Config{HTTP3: true}, nil)
	_, c := hn.client(t, h3.On)
	key := "laminae/h3/live"
	tok := hn.sign(key, api.TokenOp_TOKEN_OP_PUT, 30)
	body := make([]byte, 200_000)
	rand.Read(body)

	pr, pw := io.Pipe()
	go func() {
		for i := 0; i < 100_000; i += 10_000 {
			pw.Write(body[i : i+10_000])
			time.Sleep(5 * time.Millisecond)
		}
		pw.Close()
	}()
	resp := hn.do(t, c, http.MethodPut, key, tok, map[string]string{HdrUploadOffset: "0", HdrUploadComplete: "?0", HdrSizeHint: "200000"}, pr)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.Equal(t, "HTTP/3.0", resp.Proto)
	offset, err := strconv.Atoi(resp.Header.Get(HdrUploadOffset))
	require.NoError(t, err)
	require.Equal(t, 100_000/Align*Align, offset, "the aligned prefix is on the device")

	ended := time.Now().UTC().Truncate(time.Millisecond)
	req, err := http.NewRequest(http.MethodPut, "https://"+hn.addr+"/"+key, nil)
	require.NoError(t, err)
	pr, pw = io.Pipe()
	req.Body = pr
	req.ContentLength = -1
	req.Header.Set("Authorization", token.Scheme+" "+tok)
	req.Header.Set(HdrUploadOffset, strconv.Itoa(offset))
	req.Header.Set(HdrUploadComplete, "?1")
	req.Trailer = http.Header{HdrDateEnded: nil}
	go func() {
		for i := offset; i < len(body); i += 10_000 {
			pw.Write(body[i:min(i+10_000, len(body))])
		}
		req.Trailer.Set(HdrDateEnded, ended.Format(time.RFC3339Nano))
		pw.Close()
	}()
	resp, err = c.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "HTTP/3.0", resp.Proto)

	e, ok := hn.n.sinks[0].Index.Get(key)
	require.True(t, ok)
	require.Equal(t, int64(len(body)), e.Size)
	require.Equal(t, ended, e.Ended, "the trailer's end time")
	resp = hn.do(t, c, http.MethodGet, key, hn.sign(key, api.TokenOp_TOKEN_OP_GET, 30), nil, nil)
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

// The idle timeout holds over HTTP/3 as over TCP: a body that stops is
// ended by the node, which answers where the upload stands.
func TestHTTP3IdleTimeout(t *testing.T) {
	hn := serveH3(t, Config{HTTP3: true}, nil)
	_, c := hn.client(t, h3.On)
	key := "laminae/h3/stalled"
	pr, pw := io.Pipe()
	defer pw.Close()
	go pw.Write(make([]byte, 20_000))
	began := time.Now()
	resp := hn.do(t, c, http.MethodPut, key, hn.sign(key, api.TokenOp_TOKEN_OP_PUT, 1), map[string]string{HdrUploadOffset: "0", HdrUploadComplete: "?1"}, pr)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	require.Equal(t, "HTTP/3.0", resp.Proto)
	require.Equal(t, "?0", resp.Header.Get(HdrUploadComplete))
	require.Equal(t, strconv.Itoa(20_000/Align*Align), resp.Header.Get(HdrUploadOffset))
	require.Less(t, time.Since(began), 10*time.Second)
}

// A connection that drops in the middle of an upload: the node keeps the
// aligned prefix of what arrived, and a new connection resumes from the
// offset HEAD reports and completes it (§12.5).
func TestHTTP3ResumesAfterDroppedConnection(t *testing.T) {
	hn := serveH3(t, Config{HTTP3: true}, nil)
	key := "laminae/h3/dropped"
	tok := hn.sign(key, api.TokenOp_TOKEN_OP_PUT, 30)
	body := make([]byte, 300_000)
	rand.Read(body)

	first, c := hn.client(t, h3.On)
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPut, "https://"+hn.addr+"/"+key, pr)
		req.ContentLength = -1
		req.Header.Set("Authorization", token.Scheme+" "+tok)
		req.Header.Set(HdrUploadOffset, "0")
		req.Header.Set(HdrUploadComplete, "?1")
		_, err := c.Do(req)
		done <- err
	}()
	_, err := pw.Write(body[:150_000])
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond)
	first.Close() // the connection is gone, mid-body
	require.Error(t, <-done)
	pw.Close()

	_, c = hn.client(t, h3.On)
	var offset int
	require.Eventually(t, func() bool {
		resp := hn.do(t, c, http.MethodHead, key, tok, nil, nil)
		offset, _ = strconv.Atoi(resp.Header.Get(HdrUploadOffset))
		return resp.StatusCode == http.StatusOK && offset > 0
	}, 5*time.Second, 50*time.Millisecond, "the node kept what arrived")
	require.LessOrEqual(t, offset, 150_000)
	require.Zero(t, offset%Align)

	// The node may still be ending the dropped request: a 409 says where it
	// has got to, and the upload carries on from there, as the uploader's
	// does.
	var resp *http.Response
	for range 5 {
		resp = hn.do(t, c, http.MethodPut, key, tok, map[string]string{
			HdrUploadOffset: strconv.Itoa(offset), HdrUploadComplete: "?1", HdrUploadLength: strconv.Itoa(len(body)),
		}, bytes.NewReader(body[offset:]))
		if resp.StatusCode != http.StatusConflict {
			break
		}
		offset, _ = strconv.Atoi(resp.Header.Get(HdrUploadOffset))
	}
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "HTTP/3.0", resp.Proto)
	resp = hn.do(t, c, http.MethodGet, key, hn.sign(key, api.TokenOp_TOKEN_OP_GET, 30), nil, nil)
	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, body, got)
}

// A node as configured by default, `storage.http3: off`, or one whose UDP
// port it cannot have: TCP alone, and no answer offers HTTP/3, so no
// browser takes the console's reads there (§35.6).
func TestHTTP3NotServed(t *testing.T) {
	for _, c := range []struct {
		name string
		on   bool
		busy bool
	}{{"default", false, false}, {"port taken", true, true}} {
		t.Run(c.name, func(t *testing.T) {
			dl, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			if c.busy {
				udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: dl.Addr().(*net.TCPAddr).Port})
				require.NoError(t, err)
				t.Cleanup(func() { udp.Close() })
			}
			cfg := Config{}
			if c.on {
				cfg.HTTP3 = true
			}
			hn := serveH3(t, cfg, dl)
			require.False(t, hn.served)
			if !c.busy {
				// Nor does it hold the UDP port.
				udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: dl.Addr().(*net.TCPAddr).Port})
				require.NoError(t, err)
				udp.Close()
			}
			_, tcp := hn.client(t, h3.Off)
			resp := hn.do(t, tcp, http.MethodGet, "healthz", "", nil, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Empty(t, resp.Header.Get("Alt-Svc"))
		})
	}
}

// HTTP/3 takes the TCP listener's configuration per handshake (§33.5): the
// external certificate to a client that asks for its name and the host
// certificate to one that dials the IP, both with h3 negotiated; and a
// client certificate verified against the bundle as it is then, so one
// from the next CA of a rollover is taken once a renewal brings it.
func TestHTTP3HandshakeAsOverTCP(t *testing.T) {
	public, err := pki.NewCA("public", time.Now())
	require.NoError(t, err)
	certFile, keyFile := writeServerPair(t, public, t.TempDir(), "storage.example.com")
	hn := serveH3(t, Config{HTTP3: true, CertFile: certFile, KeyFile: keyFile}, nil)
	require.True(t, hn.served)
	publicPool := x509.NewCertPool()
	publicPool.AddCert(public.Cert)

	served := func(name string, roots *x509.CertPool) *x509.Certificate {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		conn, err := quic.DialAddr(ctx, hn.addr, &tls.Config{RootCAs: roots, ServerName: name, NextProtos: []string{http3.NextProtoH3}}, nil)
		require.NoError(t, err, name)
		defer conn.CloseWithError(0, "")
		st := conn.ConnectionState().TLS
		require.Equal(t, http3.NextProtoH3, st.NegotiatedProtocol, name)

		return st.PeerCertificates[0]
	}
	require.Equal(t, []string{"storage.example.com"}, served("storage.example.com", publicPool).DNSNames)
	_, isHost := pki.IdOf(served("127.0.0.1", hn.pool))
	require.True(t, isHost, "an IP gets the host certificate")

	// A producer renewed from the next CA, before the node holds it and
	// after.
	next, err := pki.NewCA("shale-next", time.Now())
	require.NoError(t, err)
	renewed := hostClient(t, next)
	tr := h3.NewTransport(http.DefaultTransport, h3.StaticTLS(&tls.Config{
		RootCAs:              hn.pool,
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &renewed, nil },
	}), h3.On, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { tr.Close() })
	c := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	_, err = c.Get("https://" + hn.addr + "/healthz")
	require.Error(t, err, "a certificate from a CA the node does not hold")

	require.NoError(t, hn.n.agent.Store.SetBundle(append(hn.ca.Bundle(), next.Bundle()...)))
	resp, err := c.Get("https://" + hn.addr + "/healthz")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "HTTP/3.0", resp.Proto)
}
