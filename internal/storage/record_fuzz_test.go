package storage

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/lesomnus/shale/api"
)

// The xattr record is read back from every file a sink holds (§23.1),
// including files written by another version or damaged on disk: decoding
// must not panic on any bytes, and what EncodeRecord wrote must decode as
// it was (#57).
func FuzzDecodeRecord(f *testing.F) {
	good, _ := EncodeRecord(api.LaminaRecord_builder{
		FormatVersion: 1, LaminaId: bytes.Repeat([]byte{1}, 16), AttemptId: bytes.Repeat([]byte{2}, 16),
		DateStartedMs: 1700000000000, Size: 123,
	}.Build())
	f.Add(good)
	f.Add([]byte{})
	f.Add([]byte{0xff})
	f.Add(bytes.Repeat([]byte{0xff}, 300))
	f.Fuzz(func(t *testing.T, b []byte) {
		// Whatever comes back, it came back rather than panicking; a record
		// a newer node wrote decodes for the fields this one knows.
		decode(b)
	})
}

func FuzzRecordRoundTrip(f *testing.F) {
	f.Add(int32(1), int64(1700000000000), int64(0), int64(4096), "laminae/2026/09/21/05/a.b")
	f.Fuzz(func(t *testing.T, version int32, startedMs, endedMs, size int64, key string) {
		in := api.LaminaRecord_builder{
			FormatVersion: version, LaminaId: bytes.Repeat([]byte{3}, 16), AttemptId: bytes.Repeat([]byte{4}, 16),
			DateStartedMs: startedMs, DateEndedMs: endedMs, Size: size,
		}.Build()
		b, err := EncodeRecord(in)
		if err != nil {
			// A record that does not fit is refused, not written.
			return
		}
		if len(b) > 255 {
			t.Fatalf("encoded %d bytes, above the inline limit", len(b))
		}
		out, err := decode(b)
		if err != nil {
			t.Fatalf("decode what was encoded: %v", err)
		}
		if !proto.Equal(in, out) {
			t.Fatalf("round trip: %v != %v", out, in)
		}
	})
}
