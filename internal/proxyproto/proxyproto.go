// Package proxyproto reads the PROXY protocol header (v1 and v2) that a
// trusted proxy puts in front of a connection, so a listener behind an
// Ingress or a load balancer that passes TLS through still knows the
// client's address (§34.10, `trusted_proxies`). A connection from anybody
// else is passed through untouched: the header is only believed from a
// proxy the policy names.
package proxyproto

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// v2Signature opens a binary header.
var v2Signature = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

// headerTimeout bounds how long a trusted proxy may take to send the
// header: a proxy sends it with the first bytes.
const headerTimeout = 5 * time.Second

// Listener wraps another listener: connections from a trusted proxy carry
// the address the header names.
type Listener struct {
	net.Listener
	// Trusted says whether a peer is a proxy whose header is believed.
	Trusted func(ip net.IP) bool
	// Rejected, when set, hears of a trusted peer whose connection carried
	// no header or a bad one: a probe, a scan, or a proxy misconfigured.
	Rejected func(remote net.Addr, err error)
}

// Listen wraps `inner`.
func Listen(inner net.Listener, trusted func(ip net.IP) bool) *Listener {
	return &Listener{Listener: inner, Trusted: trusted}
}

// Accept answers the next connection, having read the header when the
// peer is trusted. A trusted peer that sends no header, or a bad one, has
// its connection closed and nothing else: a server takes an error from
// Accept as the listener's own and stops, and one bad connection, a health
// probe from the proxy's host for instance, must not take the server down.
func (l *Listener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		host, _, _ := net.SplitHostPort(c.RemoteAddr().String())
		ip := net.ParseIP(host)
		if ip == nil || l.Trusted == nil || !l.Trusted(ip) {
			return c, nil
		}
		c.SetReadDeadline(time.Now().Add(headerTimeout))
		r := bufio.NewReader(c)
		src, err := ReadHeader(r)
		c.SetReadDeadline(time.Time{})
		if err != nil {
			c.Close()
			if l.Rejected != nil {
				l.Rejected(c.RemoteAddr(), fmt.Errorf("proxy protocol: %w", err))
			}
			continue
		}
		if src == nil {
			// LOCAL, or a health check: the proxy's own address stands.
			return &conn{Conn: c, r: r, remote: c.RemoteAddr()}, nil
		}

		return &conn{Conn: c, r: r, remote: src}, nil
	}
}

// conn is a connection whose first bytes were read for the header.
type conn struct {
	net.Conn
	r      *bufio.Reader
	remote net.Addr
}

func (c *conn) Read(p []byte) (int, error) { return c.r.Read(p) }
func (c *conn) RemoteAddr() net.Addr       { return c.remote }

// ReadHeader reads one header and answers the source address it names, or
// nil for a LOCAL header (the proxy speaks for itself).
func ReadHeader(r *bufio.Reader) (net.Addr, error) {
	head, err := r.Peek(12)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(head, v2Signature) {
		return readV2(r)
	}
	if bytes.HasPrefix(head, []byte("PROXY ")) {
		return readV1(r)
	}

	return nil, errors.New("no header")
}

func readV1(r *bufio.Reader) (net.Addr, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) > 108 || !strings.HasSuffix(line, "\r\n") {
		return nil, errors.New("v1: malformed line")
	}
	f := strings.Fields(strings.TrimSuffix(line, "\r\n"))
	if len(f) < 2 || f[0] != "PROXY" {
		return nil, errors.New("v1: malformed line")
	}
	if f[1] == "UNKNOWN" {
		return nil, nil
	}
	if len(f) != 6 || (f[1] != "TCP4" && f[1] != "TCP6") {
		return nil, errors.New("v1: malformed line")
	}
	ip := net.ParseIP(f[2])
	port, perr := strconv.Atoi(f[4])
	if ip == nil || perr != nil || port < 0 || port > 65535 {
		return nil, errors.New("v1: bad source")
	}

	return &net.TCPAddr{IP: ip, Port: port}, nil
}

func readV2(r *bufio.Reader) (net.Addr, error) {
	var h [16]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	if h[12]>>4 != 2 {
		return nil, errors.New("v2: not version 2")
	}
	cmd := h[12] & 0x0f
	fam := h[13] >> 4
	length := int(binary.BigEndian.Uint16(h[14:16]))
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	if cmd == 0 { // LOCAL
		return nil, nil
	}
	if cmd != 1 {
		return nil, errors.New("v2: unknown command")
	}
	switch fam {
	case 1: // AF_INET
		if length < 12 {
			return nil, errors.New("v2: short address")
		}

		return &net.TCPAddr{IP: net.IP(body[0:4]), Port: int(binary.BigEndian.Uint16(body[8:10]))}, nil
	case 2: // AF_INET6
		if length < 36 {
			return nil, errors.New("v2: short address")
		}

		return &net.TCPAddr{IP: net.IP(body[0:16]), Port: int(binary.BigEndian.Uint16(body[32:34]))}, nil
	}

	return nil, nil
}

// Trusted answers a check over CIDRs and addresses.
func Trusted(entries []string) func(ip net.IP) bool {
	var nets []*net.IPNet
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		if _, n, err := net.ParseCIDR(e); err == nil {
			nets = append(nets, n)
			continue
		}
		if ip := net.ParseIP(e); ip != nil {
			bits := 32
			if ip.To4() == nil {
				bits = 128
			}
			nets = append(nets, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
		}
	}

	return func(ip net.IP) bool {
		for _, n := range nets {
			if n.Contains(ip) {
				return true
			}
		}

		return false
	}
}
