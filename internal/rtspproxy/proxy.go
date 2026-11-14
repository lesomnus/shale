// Package rtspproxy keeps a camera's credentials out of the command lines of
// the tools that read its stream (#249). A process's arguments are readable by
// every local user (/proc/<pid>/cmdline), so ffmpeg and ffprobe are handed a
// loopback URL with a random token instead of rtsp://user:password@camera,
// and the proxy authenticates to the camera on their behalf.
//
// The proxy speaks just enough RTSP/1.0 to do that: requests are passed on
// with their URLs mapped to the camera, a CSeq of its own and the
// camera's Authorization (Basic or Digest), answered once more after a 401;
// responses come back with the client's CSeq and the camera's URLs in
// Content-Base, Content-Location, RTP-Info and the SDP mapped to the proxy.
// Interleaved media ($ frames) pass through untouched, both ways.
//
// What it leaves in a command line is the token, which reaches this one
// stream through this one proxy while it lives, not the camera account. The
// stream's query, where some cameras take a token of their own, stays with
// the proxy as well: it is put back on the requests for the stream's path.
package rtspproxy

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	dialTimeout = 5 * time.Second
	maxHeader   = 64 << 10
	maxBody     = 1 << 20
)

// Proxy is one camera stream served on loopback.
type Proxy struct {
	user, pass string
	tls        bool
	addr       string // the camera's host:port
	host       string // for TLS
	primary    string // the camera's base URL, as requests are sent there
	bases      []string
	base       string // the proxy's base URL, token included
	suffix     string // the stream's path
	query      string // the stream's query, kept from the command line
	ln         net.Listener

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

// Start serves target (rtsp or rtsps, without userinfo) on 127.0.0.1 with
// user and pass. Close it when the tool that reads it is done.
func Start(target *url.URL, user, pass string) (*Proxy, error) {
	if target == nil || target.User != nil || target.Host == "" || (target.Scheme != "rtsp" && target.Scheme != "rtsps") {
		return nil, errors.New("rtspproxy: an rtsp or rtsps URL without userinfo")
	}
	port := target.Port()
	if port == "" {
		port = "554"
		if target.Scheme == "rtsps" {
			port = "322"
		}
	}
	host := target.Hostname()
	addr := net.JoinHostPort(host, port)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("rtspproxy: listen: %w", err)
	}
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		ln.Close()
		return nil, err
	}
	p := &Proxy{
		user: user, pass: pass, tls: target.Scheme == "rtsps", addr: addr, host: host,
		primary: target.Scheme + "://" + target.Host,
		base:    "rtsp://" + ln.Addr().String() + "/" + hex.EncodeToString(token),
		suffix:  target.EscapedPath(),
		query:   target.RawQuery,
		ln:      ln, conns: map[net.Conn]struct{}{},
	}
	// Every spelling of the camera's base a response may use, longest first,
	// so that one with a port is never cut at its host.
	seen := map[string]bool{}
	for _, h := range []string{target.Host, addr, hostOnly(target.Host)} {
		b := target.Scheme + "://" + h
		if h != "" && !seen[b] {
			seen[b] = true
			p.bases = append(p.bases, b)
		}
	}
	if port != "554" && port != "322" {
		p.bases = slicesWithout(p.bases, target.Scheme+"://"+hostOnly(target.Host))
	}
	sort.Slice(p.bases, func(i, j int) bool { return len(p.bases[i]) > len(p.bases[j]) })
	p.wg.Add(1)
	go p.accept()

	return p, nil
}

// URL is what the tool reads: the camera's path under the proxy.
func (p *Proxy) URL() string { return p.base + p.suffix }

// Close stops the proxy and every connection through it.
func (p *Proxy) Close() error {
	p.mu.Lock()
	p.closed = true
	err := p.ln.Close()
	for c := range p.conns {
		c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()

	return err
}

func (p *Proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		c.Close()
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *Proxy) untrack(c net.Conn) {
	p.mu.Lock()
	delete(p.conns, c)
	p.mu.Unlock()
	c.Close()
}

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		if !p.track(c) {
			return
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer p.untrack(c)
			p.serve(c)
		}()
	}
}

func (p *Proxy) dial() (net.Conn, error) {
	d := &net.Dialer{Timeout: dialTimeout}
	if p.tls {
		// As ffmpeg reads rtsps by default: encrypted, its certificate unchecked.
		return tls.DialWithDialer(d, "tcp", p.addr, &tls.Config{ServerName: p.host, InsecureSkipVerify: true})
	}
	return d.Dial("tcp", p.addr)
}

func (p *Proxy) serve(client net.Conn) {
	cam, err := p.dial()
	if err != nil {
		return
	}
	if !p.track(cam) {
		return
	}
	defer p.untrack(cam)
	s := &session{p: p, client: client, cam: cam, pending: map[string]*request{}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.fromCamera(bufio.NewReader(cam))
		client.Close()
	}()
	s.fromClient(bufio.NewReader(client))
	cam.Close()
	<-done
}

// rewrite maps the camera's base URL to the proxy's in s.
func (p *Proxy) rewrite(s string) string {
	for _, b := range p.bases {
		var out strings.Builder
		rest := s
		for {
			i := strings.Index(rest, b)
			if i < 0 {
				out.WriteString(rest)
				break
			}
			end := i + len(b)
			out.WriteString(rest[:i])
			if end < len(rest) && !boundary(rest[end]) {
				// Another host this one is a prefix of.
				out.WriteString(b)
			} else {
				out.WriteString(p.base)
			}
			rest = rest[end:]
		}
		s = out.String()
	}
	return s
}

func boundary(c byte) bool {
	return !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '-' || c == ':' || c == '_')
}

// session is one client connection and the camera connection it uses.
type session struct {
	p              *Proxy
	client, cam    net.Conn
	cw, sw         sync.Mutex // writes to the client, to the camera
	mu             sync.Mutex
	seq            int
	pending        map[string]*request
	auth           authState
	tokenPresented bool
}

type request struct {
	clientCSeq  string
	method, uri string
	headers     []header
	body        []byte
	retried     bool
}

func (s *session) writeClient(b []byte) error {
	s.cw.Lock()
	defer s.cw.Unlock()
	_, err := s.client.Write(b)
	return err
}

func (s *session) writeCam(b []byte) error {
	s.sw.Lock()
	defer s.sw.Unlock()
	_, err := s.cam.Write(b)
	return err
}

func (s *session) fromClient(r *bufio.Reader) {
	for {
		m, err := readMessage(r)
		if err != nil {
			return
		}
		if m.frame != nil || m.isResponse() {
			// Media, and the client's answers to the camera's own requests.
			if s.writeCam(m.raw()) != nil {
				return
			}
			continue
		}
		method, uri, ok := m.request()
		if !ok {
			return
		}
		switch {
		case strings.HasPrefix(uri, s.p.base):
			s.tokenPresented = true
			rest := strings.TrimPrefix(uri, s.p.base)
			if s.p.query != "" && rest == s.p.suffix {
				rest += "?" + s.p.query
			}
			uri = s.p.primary + rest
		case uri == "*" || s.tokenPresented:
			// The connection proved it holds the URL; what it asks of the
			// camera after that is the camera's to answer.
		default:
			_ = s.writeClient([]byte("RTSP/1.0 403 Forbidden\r\nCSeq: " + m.get("CSeq") + "\r\nContent-Length: 0\r\n\r\n"))
			return
		}
		req := &request{clientCSeq: m.get("CSeq"), method: method, uri: uri, body: m.body}
		for _, h := range m.headers {
			if !strings.EqualFold(h.name, "CSeq") && !strings.EqualFold(h.name, "Authorization") {
				req.headers = append(req.headers, h)
			}
		}
		if s.send(req) != nil {
			return
		}
	}
}

// send passes a request on to the camera under a CSeq of the proxy's own.
func (s *session) send(req *request) error {
	s.mu.Lock()
	s.seq++
	cseq := strconv.Itoa(s.seq)
	s.pending[cseq] = req
	auth := s.auth.header(req.method, req.uri, s.p.user, s.p.pass)
	s.mu.Unlock()
	var b bytes.Buffer
	b.WriteString(req.method + " " + req.uri + " RTSP/1.0\r\nCSeq: " + cseq + "\r\n")
	if auth != "" {
		b.WriteString("Authorization: " + auth + "\r\n")
	}
	for _, h := range req.headers {
		b.WriteString(h.name + ": " + h.value + "\r\n")
	}
	b.WriteString("\r\n")
	b.Write(req.body)

	return s.writeCam(b.Bytes())
}

func (s *session) fromCamera(r *bufio.Reader) {
	for {
		m, err := readMessage(r)
		if err != nil {
			return
		}
		if m.frame != nil || !m.isResponse() {
			// Media, and the camera's own requests.
			if s.writeClient(m.raw()) != nil {
				return
			}
			continue
		}
		cseq := m.get("CSeq")
		s.mu.Lock()
		req := s.pending[cseq]
		delete(s.pending, cseq)
		s.mu.Unlock()
		if req != nil && m.status() == 401 && !req.retried && (s.p.user != "" || s.p.pass != "") {
			s.mu.Lock()
			ok := s.auth.challenge(m.all("WWW-Authenticate"))
			s.mu.Unlock()
			if ok {
				req.retried = true
				if s.send(req) != nil {
					return
				}
				continue
			}
		}
		if req != nil {
			m.set("CSeq", req.clientCSeq)
		}
		for i, h := range m.headers {
			switch strings.ToLower(h.name) {
			case "content-base", "content-location", "rtp-info", "location":
				m.headers[i].value = s.p.rewrite(h.value)
			}
		}
		if len(m.body) > 0 {
			if body := s.p.rewrite(string(m.body)); body != string(m.body) {
				m.body = []byte(body)
				m.set("Content-Length", strconv.Itoa(len(m.body)))
			}
		}
		if s.writeClient(m.raw()) != nil {
			return
		}
	}
}

// authState is the camera's last challenge, which every request answers.
type authState struct {
	scheme                               string
	realm, nonce, opaque, algorithm, qop string
	nc                                   int
}

// challenge takes the camera's WWW-Authenticate headers: Digest when it
// offers it in a form this answers, else Basic.
func (a *authState) challenge(values []string) bool {
	var basic bool
	for _, v := range values {
		scheme, rest, _ := strings.Cut(strings.TrimSpace(v), " ")
		switch strings.ToLower(scheme) {
		case "digest":
			params := parseParams(rest)
			alg := strings.ToUpper(params["algorithm"])
			if alg != "" && alg != "MD5" && alg != "SHA-256" {
				continue
			}
			qop := ""
			for _, q := range strings.Split(params["qop"], ",") {
				if strings.TrimSpace(q) == "auth" {
					qop = "auth"
				}
			}
			if params["qop"] != "" && qop == "" {
				continue
			}
			*a = authState{scheme: "digest", realm: params["realm"], nonce: params["nonce"], opaque: params["opaque"], algorithm: params["algorithm"], qop: qop}
			return true
		case "basic":
			basic = true
		}
	}
	if basic {
		*a = authState{scheme: "basic"}
		return true
	}
	return false
}

func (a *authState) header(method, uri, user, pass string) string {
	switch a.scheme {
	case "basic":
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
	case "digest":
		h := func(s string) string {
			if strings.EqualFold(a.algorithm, "SHA-256") {
				sum := sha256.Sum256([]byte(s))
				return hex.EncodeToString(sum[:])
			}
			sum := md5.Sum([]byte(s))
			return hex.EncodeToString(sum[:])
		}
		ha1 := h(user + ":" + a.realm + ":" + pass)
		ha2 := h(method + ":" + uri)
		v := fmt.Sprintf(`Digest username=%s, realm=%s, nonce=%s, uri=%s`, quote(user), quote(a.realm), quote(a.nonce), quote(uri))
		if a.qop == "auth" {
			a.nc++
			nc := fmt.Sprintf("%08x", a.nc)
			c := make([]byte, 8)
			_, _ = rand.Read(c)
			cnonce := hex.EncodeToString(c)
			v += fmt.Sprintf(`, response="%s", qop=auth, nc=%s, cnonce="%s"`, h(ha1+":"+a.nonce+":"+nc+":"+cnonce+":auth:"+ha2), nc, cnonce)
		} else {
			v += fmt.Sprintf(`, response="%s"`, h(ha1+":"+a.nonce+":"+ha2))
		}
		if a.opaque != "" {
			v += ", opaque=" + quote(a.opaque)
		}
		if a.algorithm != "" {
			v += ", algorithm=" + a.algorithm
		}
		return v
	}
	return ""
}

func quote(s string) string { return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"` }

// parseParams reads `key=value, key="quoted, value"`.
func parseParams(s string) map[string]string {
	out := map[string]string{}
	for s = strings.TrimSpace(s); s != ""; s = strings.TrimSpace(s) {
		k, rest, ok := strings.Cut(s, "=")
		if !ok {
			break
		}
		k = strings.ToLower(strings.TrimSpace(k))
		rest = strings.TrimSpace(rest)
		var v string
		if strings.HasPrefix(rest, `"`) {
			var b strings.Builder
			i := 1
			for ; i < len(rest); i++ {
				if rest[i] == '\\' && i+1 < len(rest) {
					i++
					b.WriteByte(rest[i])
					continue
				}
				if rest[i] == '"' {
					break
				}
				b.WriteByte(rest[i])
			}
			v, rest = b.String(), rest[min(i+1, len(rest)):]
		} else {
			v, rest, _ = strings.Cut(rest, ",")
			v = strings.TrimSpace(v)
			rest = "," + rest
		}
		out[k] = v
		_, rest, _ = strings.Cut(rest, ",")
		s = rest
	}
	return out
}

func hostOnly(h string) string {
	if host, _, err := net.SplitHostPort(h); err == nil {
		if strings.Contains(host, ":") {
			return "[" + host + "]"
		}
		return host
	}
	return h
}

func slicesWithout(xs []string, x string) []string {
	out := xs[:0]
	for _, v := range xs {
		if v != x {
			out = append(out, v)
		}
	}
	return out
}

// message is one RTSP message or one interleaved frame.
type message struct {
	frame   []byte // the whole $ frame, header included
	start   string
	headers []header
	body    []byte
}

type header struct{ name, value string }

func (m *message) isResponse() bool { return strings.HasPrefix(m.start, "RTSP/") }

func (m *message) request() (method, uri string, ok bool) {
	parts := strings.Fields(m.start)
	if len(parts) != 3 || !strings.HasPrefix(parts[2], "RTSP/") {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func (m *message) status() int {
	parts := strings.Fields(m.start)
	if len(parts) < 2 {
		return 0
	}
	n, _ := strconv.Atoi(parts[1])
	return n
}

func (m *message) get(name string) string {
	for _, h := range m.headers {
		if strings.EqualFold(h.name, name) {
			return h.value
		}
	}
	return ""
}

func (m *message) all(name string) []string {
	var out []string
	for _, h := range m.headers {
		if strings.EqualFold(h.name, name) {
			out = append(out, h.value)
		}
	}
	return out
}

func (m *message) set(name, value string) {
	for i, h := range m.headers {
		if strings.EqualFold(h.name, name) {
			m.headers[i].value = value
			return
		}
	}
	m.headers = append(m.headers, header{name, value})
}

func (m *message) raw() []byte {
	if m.frame != nil {
		return m.frame
	}
	var b bytes.Buffer
	b.WriteString(m.start + "\r\n")
	for _, h := range m.headers {
		b.WriteString(h.name + ": " + h.value + "\r\n")
	}
	b.WriteString("\r\n")
	b.Write(m.body)
	return b.Bytes()
}

func readMessage(r *bufio.Reader) (*message, error) {
	for {
		c, err := r.Peek(1)
		if err != nil {
			return nil, err
		}
		if c[0] != '\r' && c[0] != '\n' {
			break
		}
		_, _ = r.ReadByte()
	}
	if c, _ := r.Peek(1); c[0] == '$' {
		head := make([]byte, 4)
		if _, err := io.ReadFull(r, head); err != nil {
			return nil, err
		}
		n := int(head[2])<<8 | int(head[3])
		frame := make([]byte, 4+n)
		copy(frame, head)
		if _, err := io.ReadFull(r, frame[4:]); err != nil {
			return nil, err
		}
		return &message{frame: frame}, nil
	}
	m := &message{}
	size := 0
	for first := true; ; first = false {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if size += len(line); size > maxHeader {
			return nil, errors.New("rtspproxy: header too long")
		}
		line = strings.TrimRight(line, "\r\n")
		if first {
			m.start = line
			continue
		}
		if line == "" {
			break
		}
		if (line[0] == ' ' || line[0] == '\t') && len(m.headers) > 0 {
			m.headers[len(m.headers)-1].value += " " + strings.TrimSpace(line)
			continue
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return nil, errors.New("rtspproxy: malformed header")
		}
		m.headers = append(m.headers, header{strings.TrimSpace(name), strings.TrimSpace(value)})
	}
	if v := m.get("Content-Length"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > maxBody {
			return nil, errors.New("rtspproxy: bad content length")
		}
		m.body = make([]byte, n)
		if _, err := io.ReadFull(r, m.body); err != nil {
			return nil, err
		}
	}
	return m, nil
}
