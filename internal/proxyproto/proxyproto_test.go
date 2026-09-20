package proxyproto

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestV1(t *testing.T) {
	a, err := ReadHeader(bufio.NewReader(bytes.NewBufferString("PROXY TCP4 203.0.113.7 10.0.0.1 51234 7400\r\nhello")))
	if err != nil {
		t.Fatal(err)
	}
	if a.String() != "203.0.113.7:51234" {
		t.Fatalf("got %s", a)
	}
	if a, err := ReadHeader(bufio.NewReader(bytes.NewBufferString("PROXY UNKNOWN\r\n"))); err != nil || a != nil {
		t.Fatalf("UNKNOWN: %v %v", a, err)
	}
}

func TestV2(t *testing.T) {
	var b bytes.Buffer
	b.Write(v2Signature)
	b.WriteByte(0x21)        // version 2, PROXY
	b.WriteByte(0x11)        // AF_INET, STREAM
	body := make([]byte, 12) // src, dst, sport, dport
	copy(body[0:4], net.ParseIP("198.51.100.9").To4())
	copy(body[4:8], net.ParseIP("10.0.0.1").To4())
	binary.BigEndian.PutUint16(body[8:10], 40000)
	binary.BigEndian.PutUint16(body[10:12], 7400)
	binary.Write(&b, binary.BigEndian, uint16(len(body)))
	b.Write(body)
	b.WriteString("payload")
	r := bufio.NewReader(&b)
	a, err := ReadHeader(r)
	if err != nil {
		t.Fatal(err)
	}
	if a.String() != "198.51.100.9:40000" {
		t.Fatalf("got %s", a)
	}
	rest, _ := r.Peek(7)
	if string(rest) != "payload" {
		t.Fatalf("the payload follows the header, got %q", rest)
	}
}

func TestNotAHeader(t *testing.T) {
	if _, err := ReadHeader(bufio.NewReader(bytes.NewBufferString("GET / HTTP/1.1\r\n\r\n"))); err == nil {
		t.Fatal("a plain request is not a header")
	}
}

// A listener believes the header from a trusted peer and nobody else.
func TestListener(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()

	for _, trusted := range []bool{true, false} {
		l := Listen(inner, func(net.IP) bool { return trusted })
		done := make(chan string, 1)
		go func() {
			c, err := l.Accept()
			if err != nil {
				done <- "accept: " + err.Error()
				return
			}
			defer c.Close()
			buf := make([]byte, 64)
			n, _ := c.Read(buf)
			done <- c.RemoteAddr().String() + " " + string(buf[:n])
		}()
		c, err := net.Dial("tcp", inner.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		c.Write([]byte("PROXY TCP4 203.0.113.7 10.0.0.1 51234 7400\r\nhi"))
		got := <-done
		c.Close()
		if trusted && got != "203.0.113.7:51234 hi" {
			t.Fatalf("trusted: got %q", got)
		}
		if !trusted && !bytes.HasPrefix([]byte(got), []byte("127.0.0.1:")) {
			t.Fatalf("untrusted: got %q", got)
		}
	}
}

func TestTrusted(t *testing.T) {
	f := Trusted([]string{"10.0.0.0/8", "192.0.2.1", " "})
	if !f(net.ParseIP("10.1.2.3")) || !f(net.ParseIP("192.0.2.1")) || f(net.ParseIP("192.0.2.2")) {
		t.Fatal("wrong answer")
	}
}

// A trusted peer that sends no header, a TCP health probe say, has its
// connection closed; the listener goes on and the next connection, with
// a header, is accepted as the address the header names (§34.10).
func TestBadHeaderClosesOneConnection(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	rejected := 0
	l := Listen(inner, func(ip net.IP) bool { return ip.IsLoopback() })
	l.Rejected = func(net.Addr, error) { rejected++ }

	got := make(chan net.Conn, 1)
	errs := make(chan error, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			errs <- err
			return
		}
		got <- c
	}()

	// The probe: connect, say nothing that is a header, leave.
	probe, err := net.Dial("tcp", inner.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	probe.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
	probe.Close()

	// A real proxy connection.
	real, err := net.Dial("tcp", inner.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer real.Close()
	real.Write([]byte("PROXY TCP4 203.0.113.7 10.0.0.1 51234 7400\r\n"))

	select {
	case c := <-got:
		if c.RemoteAddr().String() != "203.0.113.7:51234" {
			t.Fatalf("remote %s", c.RemoteAddr())
		}
		c.Close()
	case err := <-errs:
		t.Fatalf("Accept failed the listener: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("no connection accepted")
	}
	if rejected != 1 {
		t.Fatalf("rejected %d connections", rejected)
	}
}
