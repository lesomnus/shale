package producer

import (
	"bytes"
	"io"
	"testing"
)

// The TS reader takes any bytes without panicking (§57).
func FuzzReader(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x47, 0x40, 0x00, 0x10})
	pkt := make([]byte, PacketSize*3)
	for i := 0; i < len(pkt); i += PacketSize {
		pkt[i] = 0x47
	}
	f.Add(pkt)
	f.Fuzz(func(t *testing.T, b []byte) {
		r := NewReader(bytes.NewReader(b))
		var pk Packet
		for range 64 {
			if err := r.Next(&pk); err != nil {
				if err != io.EOF && err != ErrSync && err != io.ErrUnexpectedEOF {
					_ = err
				}
				break
			}
			r.IsKeyframe(&pk)
			r.IsVideoFrame(&pk)
			r.Tables()
		}
	})
}
