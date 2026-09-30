package producer

import (
	"bytes"
	"os"
	"testing"
)

// The MP4 reader takes any bytes without panicking (§57).
func FuzzReader(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("\x00\x00\x00\x08free"))
	if b, err := os.ReadFile("testdata/aac.mp4"); err == nil {
		f.Add(b[:2000])
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		r := NewMP4Reader(bytes.NewReader(b))
		var fr Frame
		for range 64 {
			if err := r.Next(&fr); err != nil {
				break
			}
		}
	})
}
