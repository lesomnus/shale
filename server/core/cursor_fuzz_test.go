package core

import (
	"testing"
	"time"

	"github.com/lesomnus/payday/pdid"
)

// A timeline's cursor comes back from any caller (§17.1): decoding must not
// panic on any string, and what encodeCursor made must decode as it was
// (#57).
func FuzzDecodeCursor(f *testing.F) {
	id := pdid.New(DomLamina)
	ends := map[pdid.Id]time.Time{pdid.New(DomSource): time.Unix(1700000000, 0).UTC()}
	f.Add(encodeCursor(time.Unix(1700000000, 123).UTC(), id, ends))
	f.Add("")
	f.Add("!!!")
	f.Add("MTIzNHw=")
	f.Fuzz(func(t *testing.T, s string) {
		decodeCursor(s)
	})
}

func FuzzCursorRoundTrip(f *testing.F) {
	f.Add(int64(1700000000123456789), int64(1700000000000000000))
	f.Fuzz(func(t *testing.T, ns, endNs int64) {
		id := pdid.New(DomLamina)
		src := pdid.New(DomSource)
		at := time.Unix(0, ns).UTC()
		ends := map[pdid.Id]time.Time{src: time.Unix(0, endNs).UTC()}
		got, gotId, gotEnds, err := decodeCursor(encodeCursor(at, id, ends))
		if err != nil {
			t.Fatal(err)
		}
		if !got.Equal(at) || gotId != id || !gotEnds[src].Equal(ends[src]) {
			t.Fatalf("round trip: %s %s %v != %s %s %v", got, gotId, gotEnds, at, id, ends)
		}
	})
}
