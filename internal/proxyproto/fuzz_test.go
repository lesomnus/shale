package proxyproto

import (
	"bufio"
	"bytes"
	"testing"
)

// A header is read without panicking, whatever the proxy sent (§57).
func FuzzReadHeader(f *testing.F) {
	f.Add([]byte("PROXY TCP4 1.2.3.4 5.6.7.8 1 2\r\n"))
	f.Add(append(append([]byte{}, v2Signature...), 0x21, 0x11, 0, 12, 1, 2, 3, 4, 5, 6, 7, 8, 0, 1, 0, 2))
	f.Add([]byte("GET / HTTP/1.1\r\n"))
	f.Fuzz(func(t *testing.T, b []byte) {
		ReadHeader(bufio.NewReader(bytes.NewReader(b)))
	})
}
