// Package h3 is HTTP/3 on a Storage Node's data plane (§35.6): the QUIC
// settings both ends use, the Alt-Svc a node answers with, and the client
// transport that takes a node's HTTP/3 where it can and its TCP where it
// cannot.
//
// It is apart from internal/hostagent, which the control plane links and
// with it the sandbox (§40.3): quic-go is linked by the hosts that speak
// it, a node and a producer, and by nothing the page is built from.
package h3

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/quic-go/quic-go"
)

// Mode is when a client takes a node's HTTP/3: `producer.http3` (§36.1).
type Mode string

const (
	// Off is TCP alone, as before HTTP/3: the default.
	Off Mode = "off"
	// Auto is HTTP/3 to a node that said it serves it, in the Alt-Svc of
	// an earlier answer, and TCP otherwise.
	Auto Mode = "auto"
	// On is HTTP/3 to every node from the first request, without waiting
	// to be told.
	On Mode = "on"
)

// ParseMode reads `off`, `auto` or `on`; empty is Off.
func ParseMode(s string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "off", "false":
		return Off, nil
	case "auto":
		return Auto, nil
	case "on", "true":
		return On, nil
	}

	return "", fmt.Errorf("%q: off, auto or on", s)
}

const (
	// HandshakeTimeout is how long a client waits for a node's QUIC
	// handshake before it takes TCP instead: what a node whose UDP is
	// blocked costs, once per FirstPause at most.
	HandshakeTimeout = 5 * time.Second
	// IdleTimeout ends a QUIC connection nothing was heard on, as a
	// lost path; the client's keepalive is well inside it.
	IdleTimeout = 30 * time.Second
	// KeepAlive is how often a client pings an idle connection: a
	// producer on WiFi finds a dead path in seconds, and a NAT keeps the
	// binding of a connection waiting on a camera.
	KeepAlive = 10 * time.Second
	// ConnIdle is how long a node keeps a connection that carries no
	// request, pings or not, as a TCP client keeps an idle connection.
	ConnIdle = 90 * time.Second
	// AltSvcMaxAge is how long a node's Alt-Svc holds: every answer
	// renews it, and a node that stops serving HTTP/3 is forgotten
	// within it.
	AltSvcMaxAge = time.Hour
)

// ServerConfig is a node's QUIC. No 0-RTT, since an upload is not a request
// to replay. The receive windows are what a producer may have in flight to
// the node, and so what it keeps for retransmission: 2 MB a stream, 4 MB a
// connection, about what TCP's buffers let it, and far more than a
// camera's rate needs at any round trip a site has. quic-go's own maxima
// (6 and 15 MB) are sized for downloads on fast paths, and a Raspberry Pi
// holds them in RAM.
func ServerConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout:           HandshakeTimeout,
		MaxIdleTimeout:                 IdleTimeout,
		InitialStreamReceiveWindow:     512 << 10,
		MaxStreamReceiveWindow:         2 << 20,
		InitialConnectionReceiveWindow: 768 << 10,
		MaxConnectionReceiveWindow:     4 << 20,
	}
}

// clientConfig is a producer's QUIC: the answers it receives are small, so
// its windows are too; what it sends is bounded by the node's.
func clientConfig() *quic.Config {
	return &quic.Config{
		HandshakeIdleTimeout:           HandshakeTimeout,
		MaxIdleTimeout:                 IdleTimeout,
		KeepAlivePeriod:                KeepAlive,
		InitialStreamReceiveWindow:     64 << 10,
		MaxStreamReceiveWindow:         1 << 20,
		InitialConnectionReceiveWindow: 128 << 10,
		MaxConnectionReceiveWindow:     2 << 20,
	}
}

// AltSvc is the Alt-Svc a node answers with when it serves HTTP/3 (RFC
// 7838): on the port the request came to, which is the port the client
// dialed; the node serves UDP on the port it serves TCP on, and anything
// that maps one in front of it maps the other alike (§34.5).
func AltSvc(host string) string {
	return fmt.Sprintf(`h3=":%s"; ma=%d`, portOf(host), int(AltSvcMaxAge.Seconds()))
}

// portOf is the port of a Host header, 443 when it names none.
func portOf(host string) string {
	if _, port, err := net.SplitHostPort(host); err == nil && port != "" {
		return port
	}

	return "443"
}

// Advertised reads an Alt-Svc field for HTTP/3 on the origin's own host and
// port, the only alternative a client here takes: HTTP/3 is on the same
// port (§35.6). It answers how long the offer holds; false when there is
// none, or when the field clears it.
func Advertised(h http.Header, origin string) (time.Duration, bool) {
	host, port, err := net.SplitHostPort(origin)
	if err != nil {
		return 0, false
	}
	for _, field := range h.Values("Alt-Svc") {
		for _, alt := range strings.Split(field, ",") {
			parts := strings.Split(alt, ";")
			id, authority, ok := strings.Cut(strings.TrimSpace(parts[0]), "=")
			if !ok {
				if strings.EqualFold(strings.TrimSpace(parts[0]), "clear") {
					return 0, false
				}
				continue
			}
			if strings.TrimSpace(id) != "h3" {
				continue
			}
			authority, err := strconv.Unquote(strings.TrimSpace(authority))
			if err != nil {
				continue
			}
			ah, ap, err := net.SplitHostPort(authority)
			if err != nil || ap != port || (ah != "" && !strings.EqualFold(ah, host)) {
				continue
			}
			ma := 24 * time.Hour
			for _, p := range parts[1:] {
				k, v, _ := strings.Cut(strings.TrimSpace(p), "=")
				if strings.EqualFold(strings.TrimSpace(k), "ma") {
					if s, err := strconv.ParseInt(strings.Trim(strings.TrimSpace(v), `"`), 10, 64); err == nil && s >= 0 {
						ma = time.Duration(s) * time.Second
					}
				}
			}
			if ma <= 0 {
				continue
			}

			return ma, true
		}
	}

	return 0, false
}
