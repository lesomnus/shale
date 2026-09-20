package mpegts

import "testing"

// The demuxer takes any whole packets without panicking (§57).
func FuzzDemux(f *testing.F) {
	f.Add(make([]byte, PacketSize))
	p := make([]byte, PacketSize*2)
	p[0], p[1], p[2], p[3] = 0x47, 0x40, 0x00, 0x10
	p[PacketSize] = 0x47
	f.Add(p)
	f.Fuzz(func(t *testing.T, b []byte) {
		b = b[:len(b)-len(b)%PacketSize]
		d := New()
		d.Write(b)
		d.Flush()
	})
}
