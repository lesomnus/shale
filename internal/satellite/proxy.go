package satellite

import (
	"bufio"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/pki"
	"github.com/lesomnus/shale/internal/token"
)

// The upload proxy (§41.3): a Storage Node's upload paths and semantics
// (§35.6), served to the producers assigned to this satellite and
// forwarded, request by request, to the node each put token names. The
// satellite moves bytes and nothing else under `retain: committed` and
// `written`: the node's answer, its status, its headers and its trailers,
// is the producer's answer, so the producer's resume and failover across
// candidates are what they are without a satellite (§12.2, §13).

const (
	// HdrSatellite is on every answer the satellite makes itself, rather
	// than passes on from a node, and says why (§41.3). A node never sends
	// it, so a producer tells the satellite's refusals from a node's by it.
	HdrSatellite = "Shale-Satellite"
	// HdrProducer names the calling producer in development mode, which is
	// plaintext and has no client certificate to read it from (§34.7), as
	// the plain header names the caller to the CP. Never read otherwise.
	HdrProducer = "Shale-Producer"
)

// The reasons HdrSatellite gives.
const (
	// ReasonUnassigned: the caller is not a producer this satellite
	// serves. The producer's failure is the satellite's, not the node's.
	ReasonUnassigned = "unassigned"
	// ReasonBusy: the in-flight pool has no room; 503 with Retry-After,
	// as a busy node answers.
	ReasonBusy = "busy"
	// ReasonToken: the token is not one the node it names would take; the
	// status is the one that node would answer.
	ReasonToken = "token"
	// ReasonUnreachable: the node the token names could not be reached
	// from here, or the CP gave no way to it.
	ReasonUnreachable = "unreachable"
	// ReasonRequest: a method or a path the upload paths do not have.
	ReasonRequest = "request"
)

// busyRetryAfter is the Retry-After of a request refused for want of room,
// in seconds: requests passing through end as their segments do.
const busyRetryAfter = "5"

// Proxy is the satellite's HTTP handler.
type Proxy struct {
	// Verifier checks put tokens against the CP's key set (§33.3).
	Verifier *token.Verifier
	// Client reaches the nodes' data planes.
	Client *http.Client
	// Dev reads the caller from HdrProducer; otherwise only a verified
	// client certificate says who calls.
	Dev bool
	Log *slog.Logger

	assigned *assigned
	routes   *routes
	pool     *pool
	// live reads the live of the uploads that pass for the relays (§41.8);
	// nil reads none.
	live *liveFeeds

	forwarded atomic.Int64
	refused   atomic.Int64
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/laminae/") {
		own(w, ReasonRequest, http.StatusNotFound, "not an upload path")
		return
	}
	switch r.Method {
	case http.MethodPut, http.MethodHead:
	default:
		w.Header().Set("Allow", "PUT, HEAD")
		own(w, ReasonRequest, http.StatusMethodNotAllowed, "a satellite takes uploads: PUT and HEAD")
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/")

	// Who calls: an adopted producer this satellite is assigned (§41.3).
	caller, serial, ok := p.caller(r)
	if !ok || !p.assigned.serves(r.Context(), caller, serial, p.Dev) {
		p.refused.Add(1)
		p.Log.Info("refused: not a producer this satellite serves", "caller", caller.String(), "key", key)
		own(w, ReasonUnassigned, http.StatusForbidden, "not a producer this satellite serves")
		return
	}

	// The token, with the checks the node it names would make, and one
	// more: it is this producer's own.
	tok := token.FromHeader(r.Header.Get("Authorization"))
	if tok == "" {
		tok = r.URL.Query().Get("token")
	}
	if tok == "" {
		w.Header().Set("WWW-Authenticate", token.Scheme)
		own(w, ReasonToken, http.StatusUnauthorized, "no token")
		return
	}
	c, err := p.Verifier.Verify(tok)
	if err != nil {
		own(w, ReasonToken, http.StatusUnauthorized, err.Error())
		return
	}
	node, err := pdid.From(c.GetAud())
	if err != nil || node.Domain() != DomNode {
		own(w, ReasonToken, http.StatusForbidden, "the token names no node")
		return
	}
	if err := token.Check(c, c.GetAud(), api.TokenOp_TOKEN_OP_PUT); err != nil {
		own(w, ReasonToken, http.StatusForbidden, err.Error())
		return
	}
	if c.GetLaminaKey() != key {
		own(w, ReasonToken, http.StatusForbidden, "the token names another key")
		return
	}
	if actor, err := pdid.From(c.GetActor()); err != nil || actor != caller {
		own(w, ReasonToken, http.StatusForbidden, "the token was issued to another producer")
		return
	}

	// Where: the node the token names, at the address the CP gives, and
	// nowhere else.
	base, err := p.routes.base(r.Context(), node)
	if err != nil {
		p.Log.Info("no route to the node", "node", node.String(), "key", key, "err", err.Error())
		own(w, ReasonUnreachable, http.StatusBadGateway, "no route to the node")
		return
	}

	// Room: a body is copied through a buffer of its own, and only so many
	// fit the budget (§41.4). A HEAD has no body.
	var body io.Reader
	var live *tap
	if r.Method == http.MethodPut {
		if !p.pool.take() {
			p.refused.Add(1)
			p.Log.Info("refused: the in-flight pool is full", "key", key, "requests", p.pool.requests.Load())
			w.Header().Set("Retry-After", busyRetryAfter)
			own(w, ReasonBusy, http.StatusServiceUnavailable, "the satellite is at its memory budget")
			return
		}
		defer p.pool.give()
		body = r.Body
		if p.live != nil {
			// The live the producer hands over is read as the body passes
			// (§41.8); nothing of it holds the upload.
			live = p.live.tap(caller, c, key, r)
		}
	}

	complete := p.forward(w, r, base, tok, body, int(p.pool.each/2), live)
	if live != nil {
		live.end(complete)
	}
}

// caller is who calls: the producer the verified client certificate
// names, with its serial; in development mode the producer HdrProducer
// names, with none.
func (p *Proxy) caller(r *http.Request) (pdid.Id, string, bool) {
	if p.Dev {
		id, err := pdid.Parse(strings.TrimSpace(r.Header.Get(HdrProducer)))
		if err != nil || id.Domain() != DomProducer {
			return pdid.Id{}, "", false
		}

		return id, "", true
	}
	if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
		return pdid.Id{}, "", false
	}
	leaf := r.TLS.VerifiedChains[0][0]
	id, ok := pki.IdOf(leaf)
	if !ok || id.Domain() != DomProducer || !clientAuth(leaf) {
		return pdid.Id{}, "", false
	}

	return id, pki.Serial(leaf), true
}

// clientAuth says the certificate is good for a client, which a host
// certificate is (§33.5).
func clientAuth(c *x509.Certificate) bool {
	for _, u := range c.ExtKeyUsage {
		if u == x509.ExtKeyUsageClientAuth || u == x509.ExtKeyUsageAny {
			return true
		}
	}

	return len(c.ExtKeyUsage) == 0
}

// forward sends the request on to the node at base and its answer back,
// unchanged: the path, the upload headers and the trailers one way, the
// status, the headers and the trailers the other. The token goes in the
// Authorization header whichever way it came, so it is in no URL. body
// is nil for a HEAD. live, when not nil, is handed the body's bytes as
// they pass. It answers whether the body was read to its end.
func (p *Proxy) forward(w http.ResponseWriter, r *http.Request, base, tok string, body io.Reader, size int, live *tap) bool {
	out, err := http.NewRequestWithContext(r.Context(), r.Method, base, nil)
	if err != nil {
		own(w, ReasonUnreachable, http.StatusBadGateway, "no route to the node")
		return false
	}
	out.URL.Path = r.URL.Path
	out.URL.RawPath = r.URL.RawPath
	out.Header = requestHeader(r.Header)
	out.Header.Set("Authorization", token.Scheme+" "+tok)
	var in *inbound
	if body != nil {
		out.ContentLength = r.ContentLength
		in = &inbound{r: bufio.NewReaderSize(body, size), req: r, out: out, n: &p.forwarded, live: live}
		if len(r.Trailer) > 0 {
			// Trailers go after a chunked body.
			out.ContentLength = -1
			out.Trailer = http.Header{}
			for k := range r.Trailer {
				out.Trailer[k] = nil
			}
		}
		out.Body = in
		if out.ContentLength == 0 {
			out.Body = http.NoBody
		}
	}

	resp, err := p.Client.Do(out)
	if err != nil {
		if r.Context().Err() != nil {
			// The producer went away; the node finds out from the request
			// this one ends, and keeps what reached it (§12.2).
			return false
		}
		p.Log.Info("the node did not answer", "url", token.RedactURL(base+r.URL.Path), "err", errText(err))
		own(w, ReasonUnreachable, http.StatusBadGateway, "the node did not answer")
		return false
	}
	defer resp.Body.Close()

	h := w.Header()
	for k, vs := range resp.Header {
		if hopByHop(k) || strings.EqualFold(k, "Alt-Svc") || strings.EqualFold(k, HdrSatellite) {
			continue
		}
		h[k] = append([]string(nil), vs...)
	}
	w.WriteHeader(resp.StatusCode)
	if r.Method != http.MethodHead {
		buf := make([]byte, 32<<10)
		io.CopyBuffer(w, resp.Body, buf)
	}
	for k, vs := range resp.Trailer {
		for _, v := range vs {
			h.Add(http.TrailerPrefix+k, v)
		}
	}

	return in != nil && in.eof.Load()
}

// requestHeader is what of a producer's request headers goes on to the
// node: everything end to end, which is every header of the upload
// protocol, and not the hop's own, the identity of development mode, what
// the producer tells the satellite of live (HdrLive), or the token, which
// forward sets.
func requestHeader(in http.Header) http.Header {
	out := make(http.Header, len(in))
	conn := connectionTokens(in)
	for k, vs := range in {
		switch {
		case hopByHop(k), conn[http.CanonicalHeaderKey(k)]:
			continue
		case strings.EqualFold(k, "Authorization"), strings.EqualFold(k, HdrProducer), strings.EqualFold(k, HdrSatellite), strings.EqualFold(k, HdrLive), strings.EqualFold(k, "Host"):
			continue
		}
		out[k] = append([]string(nil), vs...)
	}

	return out
}

// hopByHop are the headers of one connection, which a proxy does not pass
// on (RFC 9110 §7.6.1).
func hopByHop(k string) bool {
	switch http.CanonicalHeaderKey(k) {
	case "Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade":
		return true
	}

	return false
}

// connectionTokens are the headers a Connection header names as the
// hop's own.
func connectionTokens(h http.Header) map[string]bool {
	out := map[string]bool{}
	for _, v := range h.Values("Connection") {
		for _, t := range strings.Split(v, ",") {
			if t = strings.TrimSpace(t); t != "" {
				out[http.CanonicalHeaderKey(t)] = true
			}
		}
	}

	return out
}

// own is an answer the satellite makes itself.
func own(w http.ResponseWriter, reason string, status int, msg string) {
	w.Header().Set(HdrSatellite, reason)
	http.Error(w, msg, status)
}

// inbound is the producer's body on its way to the node: read through the
// request's buffer, counted, and with the producer's trailers, the end
// time of a live segment (§12.2), copied over once the body has ended,
// which is when the transport reads them.
type inbound struct {
	r   *bufio.Reader
	req *http.Request
	out *http.Request
	n   *atomic.Int64
	// live is handed every byte as it passes (§41.8), nil when nothing
	// reads it; eof says the body was read to its end.
	live *tap
	eof  atomic.Bool

	once sync.Once
}

func (b *inbound) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n.Add(int64(n))
	if n > 0 && b.live != nil {
		b.live.write(p[:n])
	}
	if errors.Is(err, io.EOF) {
		b.eof.Store(true)
		if b.out.Trailer != nil {
			b.once.Do(func() {
				for k, vs := range b.req.Trailer {
					b.out.Trailer[k] = append([]string(nil), vs...)
				}
			})
		}
	}

	return n, err
}

func (b *inbound) Close() error { return nil }

// errText is an error for the log without what it may quote of a URL: a
// url.Error carries the URL, which carries no token here, but the rule is
// that a URL is not logged unredacted.
func errText(err error) string {
	var ue interface{ Unwrap() error }
	if errors.As(err, &ue) {
		if inner := ue.Unwrap(); inner != nil {
			return inner.Error()
		}
	}

	return err.Error()
}
