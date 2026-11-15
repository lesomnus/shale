package h3

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

const (
	// FirstPause is how long a node whose HTTP/3 handshake failed is
	// spoken to over TCP alone before HTTP/3 is tried again; it doubles
	// with every failure in a row, up to MaxPause. A producer whose UDP is
	// blocked so pays one HandshakeTimeout every pause, not one a part.
	FirstPause = 5 * time.Minute
	MaxPause   = time.Hour
)

// Transport is a data-plane client (§35.6): every request over TCP, and
// over HTTP/3 to the nodes its mode takes it to. A handshake that does not
// complete, UDP blocked on the way or a node that does not serve it, sends
// the same request over TCP at once, since nothing of it went out, and
// keeps that node on TCP for a while, unless TCP fails as well: then the
// node is away, and its UDP may be fine. A failure after the handshake is
// the request's, as on TCP: the uploader asks the node where it stands and
// resumes (§13), on whichever protocol is to be used by then.
type Transport struct {
	tcp    http.RoundTripper
	tlsFor func() (*tls.Config, error)
	mode   Mode
	log    *slog.Logger
	h3     *http3.Transport
	// now is the clock pauses are kept by; tests move it.
	now func() time.Time

	mu      sync.Mutex
	udp     *quic.Transport
	origins map[string]*origin
}

// origin is what a Transport knows of one node, by host and port.
type origin struct {
	// offered is until when the node's Alt-Svc offers HTTP/3.
	offered time.Time
	// paused is until when HTTP/3 is not tried.
	paused time.Time
	// failures are the handshakes that failed in a row.
	failures int
	// spoken says an HTTP/3 connection was made, for the log.
	spoken bool
}

// NewTransport speaks HTTP/3 beside tcp. tlsFor is the TLS a handshake
// starts from, its roots and client certificate, asked at every
// handshake: a producer's follows the bundle it holds, as its TCP
// transport does, so a node renewed from the next CA of a rollover
// (§33.5) is verified without a restart. A connection made before goes
// on as it was verified. With Off it is tcp's.
func NewTransport(tcp http.RoundTripper, tlsFor func() (*tls.Config, error), mode Mode, log *slog.Logger) *Transport {
	if tlsFor == nil {
		tlsFor = StaticTLS(nil)
	}
	if log == nil {
		log = slog.Default()
	}
	t := &Transport{tcp: tcp, tlsFor: tlsFor, mode: mode, log: log, now: time.Now, origins: map[string]*origin{}}
	t.h3 = &http3.Transport{
		// What http3 adds to it, the server name and h3, is taken over
		// into tlsFor's at each handshake (dial).
		TLSClientConfig: &tls.Config{},
		QUICConfig:      clientConfig(),
		Dial:            t.dial,
	}

	return t
}

// StaticTLS is a tlsFor of one configuration; nil is the system's roots
// and no client certificate.
func StaticTLS(cfg *tls.Config) func() (*tls.Config, error) {
	return func() (*tls.Config, error) { return cfg, nil }
}

// RoundTrip sends a request on the protocol the node is spoken to on.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.mode == Off || t.mode == "" || req.URL.Scheme != "https" {
		return t.tcp.RoundTrip(req)
	}
	key := authority(req.URL.Host)
	if !t.wants(key) {
		resp, err := t.tcp.RoundTrip(req)
		if err == nil {
			t.learn(key, resp.Header)
		}

		return resp, err
	}

	// The body is held from http3, which closes it on any error: on a
	// failed handshake, before it has read a byte, the same request goes
	// over TCP with it.
	body := &heldBody{rc: req.Body, held: true}
	r3 := req
	if req.Body != nil && req.Body != http.NoBody {
		r3 = new(http.Request)
		*r3 = *req
		r3.Body = body
	}
	resp, err := t.h3.RoundTrip(r3)
	if err == nil {
		body.release()
		t.learn(key, resp.Header)

		return resp, nil
	}
	var de *dialError
	if errors.As(err, &de) && body.detach() {
		resp, err := t.tcp.RoundTrip(req)
		if err != nil {
			// TCP does not reach it either: the node is down or away, not
			// its UDP, and HTTP/3 is tried again as soon as it is back.
			t.unpause(key)
			return nil, err
		}
		t.learn(key, resp.Header)

		return resp, nil
	}
	body.release()

	return nil, err
}

// CloseIdleConnections closes TCP's idle connections; QUIC's end on their
// own, by the node's ConnIdle.
func (t *Transport) CloseIdleConnections() {
	if c, ok := t.tcp.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

// Close ends every QUIC connection and the socket they share.
func (t *Transport) Close() error {
	err := t.h3.Close()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.udp != nil {
		t.udp.Close()
		t.udp.Conn.Close()
		t.udp = nil
	}

	return err
}

// Speaks says whether the next request to host:port would go over HTTP/3.
func (t *Transport) Speaks(hostport string) bool {
	if t.mode == Off || t.mode == "" {
		return false
	}

	return t.wants(authority(hostport))
}

func (t *Transport) wants(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	o := t.origins[key]
	now := t.now()
	if o != nil && now.Before(o.paused) {
		return false
	}
	if t.mode == On {
		return true
	}

	return o != nil && now.Before(o.offered)
}

// learn keeps what an answer's Alt-Svc says: an offer of HTTP/3, or none,
// which forgets the last one.
func (t *Transport) learn(key string, h http.Header) {
	if t.mode != Auto {
		return
	}
	ma, ok := Advertised(h, key)
	t.mu.Lock()
	defer t.mu.Unlock()
	o := t.origin(key)
	if ok {
		o.offered = t.now().Add(ma)
	} else {
		o.offered = time.Time{}
	}
}

func (t *Transport) origin(key string) *origin {
	o := t.origins[key]
	if o == nil {
		o = &origin{}
		t.origins[key] = o
	}

	return o
}

// unpause forgets the last failed handshake with a node.
func (t *Transport) unpause(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if o := t.origins[key]; o != nil {
		o.paused = time.Time{}
		o.failures = max(o.failures-1, 0)
	}
}

// dialError is a QUIC handshake that did not complete: nothing of the
// request was sent.
type dialError struct{ err error }

func (e *dialError) Error() string { return "http3: " + e.err.Error() }
func (e *dialError) Unwrap() error { return e.err }

// dial is http3's dial: one handshake shared by the requests waiting on it,
// whose failure pauses HTTP/3 to that node.
func (t *Transport) dial(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
	conn, err := t.handshake(ctx, addr, tlsCfg, cfg)
	key := authority(addr)
	t.mu.Lock()
	o := t.origin(key)
	if err != nil {
		pause := FirstPause << min(o.failures, 8)
		if pause > MaxPause {
			pause = MaxPause
		}
		o.failures++
		o.paused = t.now().Add(pause)
		t.mu.Unlock()
		t.log.Warn("http3: no handshake with the node, TCP for a while", "node", key, "for", pause.String(), "err", err.Error())

		return nil, &dialError{err}
	}
	first := !o.spoken || o.failures > 0
	o.spoken, o.failures = true, 0
	t.mu.Unlock()
	if first {
		t.log.Info("http3: speaking HTTP/3 to the node", "node", key)
	}

	return conn, nil
}

func (t *Transport) handshake(ctx context.Context, addr string, h3Cfg *tls.Config, cfg *quic.Config) (*quic.Conn, error) {
	base, err := t.tlsFor()
	if err != nil {
		return nil, err
	}
	tlsCfg := &tls.Config{}
	if base != nil {
		tlsCfg = base.Clone()
	}
	tlsCfg.ServerName, tlsCfg.NextProtos = h3Cfg.ServerName, h3Cfg.NextProtos
	tr, err := t.socket()
	if err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	p, err := net.DefaultResolver.LookupPort(ctx, "udp", port)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, &net.DNSError{Err: "no address", Name: host, IsNotFound: true}
	}
	// IPv4 first where a name has both: there is no racing of the families
	// here, as TCP's dialer does, and a broken IPv6 path would otherwise
	// put a node on TCP for nothing.
	ip := ips[0]
	for _, v := range ips {
		if v.Unmap().Is4() {
			ip = v
			break
		}
	}

	return tr.Dial(ctx, net.UDPAddrFromAddrPort(netip.AddrPortFrom(ip.Unmap(), uint16(p))), tlsCfg, cfg)
}

// socket is the one UDP socket every connection of this transport shares,
// opened at the first handshake: a producer whose nodes never offer HTTP/3
// opens none.
func (t *Transport) socket() (*quic.Transport, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.udp != nil {
		return t.udp, nil
	}
	c, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	t.udp = &quic.Transport{Conn: c}

	return t.udp, nil
}

// authority is host:port, lower case, with HTTPS's port when it names none.
func authority(hostport string) string {
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		host, port = strings.Trim(hostport, "[]"), "443"
	}

	return net.JoinHostPort(strings.ToLower(host), port)
}

// heldBody is a request body lent to http3: its Close waits until the
// transport knows whether the body goes on to TCP instead.
type heldBody struct {
	rc io.ReadCloser

	mu       sync.Mutex
	read     bool
	held     bool
	closed   bool
	detached bool
}

func (b *heldBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.detached {
		b.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	b.read = true
	b.mu.Unlock()

	return b.rc.Read(p)
}

func (b *heldBody) Close() error {
	b.mu.Lock()
	if b.detached {
		b.mu.Unlock()
		return nil
	}
	if b.held {
		b.closed = true
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	return b.rc.Close()
}

// release ends the hold: a Close that came meanwhile closes the body now,
// and any later one does.
func (b *heldBody) release() {
	b.mu.Lock()
	b.held = false
	closed := b.closed
	b.mu.Unlock()
	if closed && b.rc != nil {
		b.rc.Close()
	}
}

// detach takes the body back for another transport, when nothing was read
// from it; http3 is done with it either way.
func (b *heldBody) detach() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.read {
		return false
	}
	b.detached = true

	return true
}
