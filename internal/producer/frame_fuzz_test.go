package producer

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

// A raw stream is whatever a process on the host writes (§38.9): the
// reader must not panic on any of it, and what WriteFrame wrote must read
// back as it was (#57).
func FuzzFrameReader(f *testing.F) {
	var seed bytes.Buffer
	WriteFrame(&seed, &Frame{Kind: FramePrefix, Payload: []byte("HDR")})
	WriteFrame(&seed, &Frame{Kind: FrameData, Time: time.Unix(1700000000, 42), Payload: bytes.Repeat([]byte{9}, 300)})
	f.Add(seed.Bytes())
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{7, 1, 2, 3})
	f.Fuzz(func(t *testing.T, b []byte) {
		r := NewFrameReader(bytes.NewReader(b))
		var fr Frame
		n := 0
		for {
			err := r.Next(&fr)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, errFrameKind) && !errors.Is(err, errFrameSize) {
					t.Fatalf("an error nobody defined: %v", err)
				}
				break
			}
			if len(fr.Payload) > MaxFrame {
				t.Fatal("a frame above the maximum came through")
			}
			n++
			if n > len(b) {
				t.Fatal("more frames than bytes")
			}
		}
	})
}

// What is written is what is read, for any payload and any time.
func FuzzFrameRoundTrip(f *testing.F) {
	f.Add(byte(0), int64(0), []byte("x"))
	f.Add(byte(1), int64(1700000000123456789), []byte{})
	f.Fuzz(func(t *testing.T, kind byte, ns int64, payload []byte) {
		if kind > 1 {
			kind = kind % 2
		}
		in := Frame{Kind: FrameKind(kind), Payload: payload}
		if ns != 0 {
			in.Time = time.Unix(0, ns).UTC()
		}
		var buf bytes.Buffer
		if err := WriteFrame(&buf, &in); err != nil {
			t.Fatal(err)
		}
		var out Frame
		if err := NewFrameReader(bytes.NewReader(buf.Bytes())).Next(&out); err != nil {
			t.Fatal(err)
		}
		if out.Kind != in.Kind || !out.Time.Equal(in.Time) || !bytes.Equal(out.Payload, in.Payload) {
			t.Fatalf("round trip: %+v != %+v", out, in)
		}
	})
}
