package satellite

import (
	"bufio"
	"context"
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
	"github.com/lesomnus/shale/internal/upload"
)

// The upload proxy (§41.3): a Storage Node's upload paths and semantics
// (§35.6), served to the producers assigned to this satellite and
// forwarded, request by request, to the node each put token names. The
// satellite moves bytes and nothing else under `retain: committed` and
// `written`: the node's answer, its status, its headers and its trailers,
// is the producer's answer, so the producer's resume and failover across
// candidates are what they are without a satellite (§12.2, §13). Under
// `retain: accepted` it takes the bytes and delivers them itself (§41.9,
// accept.go).

// The headers and reasons of the satellite's protocol are internal/upload's,
// which producers read them with (§41.3, §41.9).
const (
	HdrSatellite = upload.HdrSatellite
	HdrProducer  = upload.HdrProducer

	ReasonUnassigned  = upload.ReasonUnassigned
	ReasonBusy        = upload.ReasonBusy
	ReasonToken       = upload.ReasonToken
	ReasonUnreachable = upload.ReasonUnreachable
	ReasonRequest     = upload.ReasonRequest
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

	// Refuse passes every request under `retain: accepted` through, as
	// under `committed` (`satellite.accept: false`, §41.9).
	Refuse bool
	// Laminae is the CP's LaminaService, which the satellite calls in the
	// name of the producers it delivers for (§41.9).
	Laminae api.LaminaServiceClient

	assigned *assigned
	routes   *routes
	pool     *pool
	// retained is the pool of `retain: accepted`; nil takes none.
	retained *retained
	// record has the CP record the satellite as a lamina's holder.
	record func(ctx context.Context, lamina pdid.Id) error
	// ctx is the satellite's lifetime, which deliveries run in.
	ctx context.Context

	heldMu sync.Mutex
	held   map[pdid.Id]*held

	forwarded atomic.Int64
	refused   atomic.Int64
	// Under `retain: accepted`: bytes taken, segments delivered and given
	// up, requests passed through.
	acceptedBytes atomic.Int64
	delivered     atomic.Int64
	givenUp       atomic.Int64
	passed        atomic.Int64
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
	}

	// Store and forward, where the producer asks for it and the satellite
	// takes it (§41.9); passed through as under `committed` otherwise.
	if r.Header.Get(upload.HdrRetain) == upload.RetainAccepted && p.accepted(w, r, caller, c, key) {
		return
	}

	p.forward(w, r, base, tok, body, int(p.pool.each/2))
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
// is nil for a HEAD.
func (p *Proxy) forward(w http.ResponseWriter, r *http.Request, base, tok string, body io.Reader, size int) {
	out, err := http.NewRequestWithContext(r.Context(), r.Method, base, nil)
	if err != nil {
		own(w, ReasonUnreachable, http.StatusBadGateway, "no route to the node")
		return
	}
	out.URL.Path = r.URL.Path
	out.URL.RawPath = r.URL.RawPath
	out.Header = requestHeader(r.Header)
	out.Header.Set("Authorization", token.Scheme+" "+tok)
	if body != nil {
		out.ContentLength = r.ContentLength
		in := &inbound{r: bufio.NewReaderSize(body, size), req: r, out: out, n: &p.forwarded}
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
			return
		}
		p.Log.Info("the node did not answer", "url", token.RedactURL(base+r.URL.Path), "err", errText(err))
		own(w, ReasonUnreachable, http.StatusBadGateway, "the node did not answer")
		return
	}
	defer resp.Body.Close()

	h := w.Header()
	for k, vs := range resp.Header {
		if hopByHop(k) || strings.EqualFold(k, "Alt-Svc") || strings.EqualFold(k, HdrSatellite) || ownHeader(k) {
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
}

// ownHeader is a header of the satellite's own protocol (§41.9), which a
// node neither sends nor is sent: the handoff carries every candidate's
// token, and a node is given only its own.
func ownHeader(k string) bool {
	switch http.CanonicalHeaderKey(k) {
	case upload.HdrRetain, upload.HdrHandoff, upload.HdrAccepted, upload.HdrPass:
		return true
	}

	return false
}

// requestHeader is what of a producer's request headers goes on to the
// node: everything end to end, which is every header of the upload
// protocol, and not the hop's own, the identity of development mode, the
// satellite's own, or the token, which forward sets.
func requestHeader(in http.Header) http.Header {
	out := make(http.Header, len(in))
	conn := connectionTokens(in)
	for k, vs := range in {
		switch {
		case hopByHop(k), conn[http.CanonicalHeaderKey(k)]:
			continue
		case strings.EqualFold(k, "Authorization"), strings.EqualFold(k, HdrProducer), strings.EqualFold(k, HdrSatellite), strings.EqualFold(k, "Host"), ownHeader(k):
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

	once sync.Once
}

func (b *inbound) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n.Add(int64(n))
	if errors.Is(err, io.EOF) && b.out.Trailer != nil {
		b.once.Do(func() {
			for k, vs := range b.req.Trailer {
				b.out.Trailer[k] = append([]string(nil), vs...)
			}
		})
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
