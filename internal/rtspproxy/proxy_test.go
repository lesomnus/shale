package rtspproxy

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// camera is a scripted RTSP server: it answers each request through answer,
// records what it was sent, and sends an interleaved frame after PLAY.
type camera struct {
	t      *testing.T
	ln     net.Listener
	mu     sync.Mutex
	seen   []*message
	answer func(m *message) string
}

func newCamera(t *testing.T, answer func(m *message) string) *camera {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	c := &camera{t: t, ln: ln, answer: answer}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go c.serve(conn)
		}
	}()
	return c
}

func (c *camera) serve(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		m, err := readMessage(r)
		if err != nil {
			return
		}
		if m.frame != nil {
			c.mu.Lock()
			c.seen = append(c.seen, m)
			c.mu.Unlock()
			continue
		}
		c.mu.Lock()
		c.seen = append(c.seen, m)
		c.mu.Unlock()
		if _, err := io.WriteString(conn, c.answer(m)); err != nil {
			return
		}
		if method, _, _ := m.request(); method == "PLAY" {
			_, _ = conn.Write([]byte{'$', 0, 0, 3, 'r', 't', 'p'})
		}
	}
}

func (c *camera) requests() []*message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*message(nil), c.seen...)
}

func (c *camera) url(path string) *url.URL {
	return &url.URL{Scheme: "rtsp", Host: c.ln.Addr().String(), Path: path}
}

func md5hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// client is the tool's side: one connection to the proxy.
type client struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
	seq  int
}

func dialProxy(t *testing.T, p *Proxy) *client {
	u, err := url.Parse(p.URL())
	require.NoError(t, err)
	conn, err := net.Dial("tcp", u.Host)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return &client{t: t, conn: conn, r: bufio.NewReader(conn)}
}

func (c *client) do(method, uri string, extra string) *message {
	c.t.Helper()
	c.seq += 10
	_, err := fmt.Fprintf(c.conn, "%s %s RTSP/1.0\r\nCSeq: %d\r\n%s\r\n", method, uri, c.seq, extra)
	require.NoError(c.t, err)
	require.NoError(c.t, c.conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	m, err := readMessage(c.r)
	require.NoError(c.t, err)
	return m
}

// Requests reach the camera with its own URL, a CSeq of the proxy's and its
// Digest answer to the camera's challenge; responses come back with the
// client's CSeq and every camera URL mapped to the proxy, and media passes
// through both ways (#249).
func TestDigestSessionThroughTheProxy(t *testing.T) {
	const user, pass = "viewer", `s3cret:@/?%"pass`
	var host string
	nonce := "abc123"
	cam := newCamera(t, func(m *message) string {
		method, uri, _ := m.request()
		cseq := m.get("CSeq")
		auth := m.get("Authorization")
		if method != "OPTIONS" {
			p := parseParams(strings.TrimPrefix(auth, "Digest "))
			ha1 := md5hex(user + ":cam:" + pass)
			ha2 := md5hex(method + ":" + uri)
			want := md5hex(ha1 + ":" + nonce + ":" + p["nc"] + ":" + p["cnonce"] + ":auth:" + ha2)
			if p["response"] != want || p["uri"] != uri || p["qop"] != "auth" {
				return "RTSP/1.0 401 Unauthorized\r\nCSeq: " + cseq + "\r\nWWW-Authenticate: Basic realm=\"cam\"\r\nWWW-Authenticate: Digest realm=\"cam\", nonce=\"" + nonce + "\", qop=\"auth,auth-int\", opaque=\"op\"\r\nContent-Length: 0\r\n\r\n"
			}
		}
		switch method {
		case "DESCRIBE":
			body := "v=0\r\nm=video 0 RTP/AVP 96\r\na=control:rtsp://" + host + "/stream/trackID=1\r\n"
			return "RTSP/1.0 200 OK\r\nCSeq: " + cseq + "\r\nContent-Base: rtsp://" + host + "/stream/\r\nContent-Type: application/sdp\r\nContent-Length: " + fmt.Sprint(len(body)) + "\r\n\r\n" + body
		case "PLAY":
			return "RTSP/1.0 200 OK\r\nCSeq: " + cseq + "\r\nSession: s\r\nRTP-Info: url=rtsp://" + host + "/stream/trackID=1;seq=0\r\n\r\n"
		}
		return "RTSP/1.0 200 OK\r\nCSeq: " + cseq + "\r\nSession: s\r\n\r\n"
	})
	host = cam.ln.Addr().String()
	p, err := Start(cam.url("/stream"), user, pass)
	require.NoError(t, err)
	defer p.Close()
	require.NotContains(t, p.URL(), pass)
	require.True(t, strings.HasPrefix(p.URL(), "rtsp://127.0.0.1:"))
	require.True(t, strings.HasSuffix(p.URL(), "/stream"))

	c := dialProxy(t, p)
	require.Equal(t, "10", c.do("OPTIONS", p.URL(), "").get("CSeq"))
	d := c.do("DESCRIBE", p.URL(), "Accept: application/sdp\r\n")
	require.Equal(t, 200, d.status())
	require.Equal(t, "20", d.get("CSeq"), "the client's CSeq comes back, whatever the proxy sent")
	require.Equal(t, p.base+"/stream/", d.get("Content-Base"))
	require.Contains(t, string(d.body), "a=control:"+p.base+"/stream/trackID=1")
	require.NotContains(t, string(d.body), host)
	require.Equal(t, fmt.Sprint(len(d.body)), d.get("Content-Length"))
	require.Equal(t, 200, c.do("SETUP", p.base+"/stream/trackID=1", "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n").status())
	play := c.do("PLAY", p.URL()+"/", "Session: s\r\n")
	require.Equal(t, "url="+p.base+"/stream/trackID=1;seq=0", play.get("RTP-Info"))
	frame, err := readMessage(c.r)
	require.NoError(t, err)
	require.Equal(t, []byte{'$', 0, 0, 3, 'r', 't', 'p'}, frame.frame, "media passes through untouched")
	_, err = c.conn.Write([]byte{'$', 1, 0, 4, 'r', 't', 'c', 'p'})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, m := range cam.requests() {
			if m.frame != nil {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "the client's media reaches the camera")
	var methods []string
	ncs := map[string]bool{}
	for _, m := range cam.requests() {
		if m.frame != nil {
			continue
		}
		method, uri, _ := m.request()
		methods = append(methods, method)
		require.True(t, strings.HasPrefix(uri, "rtsp://"+host+"/stream"), "the camera sees its own URL: %s", uri)
		if a := m.get("Authorization"); a != "" {
			require.True(t, strings.HasPrefix(a, "Digest "), "Digest is preferred over Basic")
			nc := parseParams(strings.TrimPrefix(a, "Digest "))["nc"]
			require.False(t, ncs[nc], "each answer counts its nonce use")
			ncs[nc] = true
		}
	}
	// DESCRIBE was challenged once and answered; what came after carried the answer.
	require.Equal(t, []string{"OPTIONS", "DESCRIBE", "DESCRIBE", "SETUP", "PLAY"}, methods)
}

// A connection has to name the token before the proxy signs in for it.
func TestATokenlessRequestIsRefused(t *testing.T) {
	cam := newCamera(t, func(m *message) string { return "RTSP/1.0 200 OK\r\nCSeq: " + m.get("CSeq") + "\r\n\r\n" })
	p, err := Start(cam.url("/stream"), "u", "p")
	require.NoError(t, err)
	defer p.Close()
	c := dialProxy(t, p)
	u, err := url.Parse(p.URL())
	require.NoError(t, err)
	refused := c.do("DESCRIBE", "rtsp://"+u.Host+"/stream", "")
	require.Equal(t, 403, refused.status())
	require.Empty(t, cam.requests(), "the camera never heard of it")
}

// Closing the proxy ends what is connected through it.
func TestCloseEndsConnections(t *testing.T) {
	cam := newCamera(t, func(m *message) string { return "RTSP/1.0 200 OK\r\nCSeq: " + m.get("CSeq") + "\r\n\r\n" })
	p, err := Start(cam.url("/stream"), "u", "p")
	require.NoError(t, err)
	c := dialProxy(t, p)
	require.Equal(t, 200, c.do("OPTIONS", p.URL(), "").status())
	require.NoError(t, p.Close())
	require.NoError(t, c.conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, err = c.r.ReadByte()
	require.Error(t, err)
}

func TestRewriteKeepsToHostBoundaries(t *testing.T) {
	p := &Proxy{base: "rtsp://127.0.0.1:1/tok", bases: []string{"rtsp://10.0.0.1:554", "rtsp://10.0.0.1"}}
	require.Equal(t, "url=rtsp://127.0.0.1:1/tok/a;seq=1,url=rtsp://10.0.0.12/b", p.rewrite("url=rtsp://10.0.0.1/a;seq=1,url=rtsp://10.0.0.12/b"))
	require.Equal(t, "rtsp://127.0.0.1:1/tok/x", p.rewrite("rtsp://10.0.0.1:554/x"))
	require.Equal(t, "rtsp://10.0.0.1:8554/x", p.rewrite("rtsp://10.0.0.1:8554/x"), "another port is another server")
}

func TestParseParams(t *testing.T) {
	require.Equal(t, map[string]string{"realm": `a "b", c`, "nonce": "n", "qop": "auth,auth-int", "stale": "TRUE"},
		parseParams(`realm="a \"b\", c", nonce=n, qop="auth,auth-int", stale=TRUE`))
}

// A query, where some cameras take a token, stays off the command line and
// goes on the stream's requests to the camera.
func TestTheQueryStaysWithTheProxy(t *testing.T) {
	cam := newCamera(t, func(m *message) string { return "RTSP/1.0 200 OK\r\nCSeq: " + m.get("CSeq") + "\r\n\r\n" })
	target := cam.url("/live")
	target.RawQuery = "opaque_auth=s3cret%ff&profile=main"
	p, err := Start(target, "", "")
	require.NoError(t, err)
	defer p.Close()
	require.NotContains(t, p.URL(), "s3cret")
	require.NotContains(t, p.URL(), "?")
	c := dialProxy(t, p)
	require.Equal(t, 200, c.do("DESCRIBE", p.URL(), "").status())
	require.Equal(t, 200, c.do("SETUP", p.URL()+"/trackID=1", "").status())
	reqs := cam.requests()
	require.Len(t, reqs, 2)
	_, describe, _ := reqs[0].request()
	_, setup, _ := reqs[1].request()
	require.Equal(t, target.String(), describe, "the stream's request carries its query")
	require.Equal(t, "rtsp://"+target.Host+"/live/trackID=1", setup)
}
