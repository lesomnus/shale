package producer

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Under `retain: written` the bytes a node reported durable are released:
// the length still counts them, the RAM does not, a reader below the
// offset fails, and one at it carries on (§12.2).
func TestSegmentRelease(t *testing.T) {
	s := NewSegment(time.Now(), []byte("tables"))
	s.Write(make([]byte, 100))
	s.Write(make([]byte, 100))
	require.Equal(t, int64(206), s.Len())
	require.Equal(t, int64(206), s.Held())
	require.Zero(t, s.Released())

	s.Release(150)
	require.Equal(t, int64(206), s.Len(), "the length counts released bytes")
	require.Equal(t, int64(56), s.Held(), "the RAM does not")
	require.Equal(t, int64(150), s.Released())
	s.Release(100)
	require.Equal(t, int64(150), s.Released(), "never backwards")
	s.Release(1000)
	require.Equal(t, int64(206), s.Released(), "never past the end")
	require.Zero(t, s.Held())

	s.Write([]byte("more"))
	require.Equal(t, int64(210), s.Len())
	require.Equal(t, int64(4), s.Held())
	_, err := s.ReaderFrom(100).Read(make([]byte, 8))
	require.ErrorIs(t, err, errReleased, "below the offset there is nothing to send")
	s.Close(time.Now())
	b, err := io.ReadAll(s.ReaderFrom(206))
	require.NoError(t, err)
	require.Equal(t, "more", string(b), "at the offset the rest reads on")

	// A discarded segment counts what arrives and keeps none.
	d := NewSegment(time.Now(), []byte("tables"))
	d.Write(make([]byte, 100))
	d.Discard()
	d.Write(make([]byte, 50))
	require.Equal(t, int64(156), d.Len())
	require.Zero(t, d.Held())
}

// A segment larger than a chunk reads back as it was written, across chunk
// boundaries and from any offset, and releasing below an offset lets the
// whole chunks under it go.
func TestSegmentChunks(t *testing.T) {
	want := make([]byte, 2*segChunk+segChunk/2+7)
	for i := range want {
		want[i] = byte(i * 7)
	}
	s := NewSegment(time.Now(), want[:100])
	for off := 100; off < len(want); {
		n := min(len(want)-off, 300_001)
		s.Write(want[off : off+n])
		off += n
	}
	require.Equal(t, int64(len(want)), s.Len())
	require.Equal(t, int64(len(want)), s.Held())
	require.Len(t, s.chunks, 3)
	s.Close(time.Now())

	b, err := io.ReadAll(s.ReaderFrom(0))
	require.NoError(t, err)
	require.Equal(t, want, b)
	b, err = io.ReadAll(s.ReaderFrom(segChunk - 3))
	require.NoError(t, err)
	require.Equal(t, want[segChunk-3:], b, "from inside the first chunk, across the second")
	require.Equal(t, want, s.Bytes())

	upTo := int64(segChunk + segChunk/2)
	s.Release(upTo)
	require.Len(t, s.chunks, 2, "the first chunk went")
	require.Equal(t, int64(len(want))-upTo, s.Held())
	require.Equal(t, upTo, s.Released())
	b, err = io.ReadAll(s.ReaderFrom(upTo))
	require.NoError(t, err)
	require.Equal(t, want[upTo:], b)
	require.Equal(t, want[upTo:], s.Bytes())
	_, err = s.ReaderFrom(upTo - 1).Read(make([]byte, 1))
	require.ErrorIs(t, err, errReleased)

	// Everything released, then more written: it goes on from there.
	s2 := NewSegment(time.Now(), []byte("ab"))
	s2.Release(2)
	require.Zero(t, s2.Held())
	s2.Write([]byte("cd"))
	require.Equal(t, int64(4), s2.Len())
	require.Equal(t, "cd", string(s2.Bytes()))
}

// Writing a lamina's worth allocates about what it holds, in chunks, not
// the repeated doubling of one slice.
func TestSegmentWriteAllocations(t *testing.T) {
	frag := make([]byte, 120_000)
	var s *Segment
	allocs := testing.AllocsPerRun(1, func() {
		s = NewSegment(time.Now(), nil)
		for i := 0; i < 500; i++ { // 60 MB, a 244 s lamina at 2 Mbps
			s.Write(frag)
		}
	})
	require.Less(t, allocs, float64(120), "about one allocation per MiB")
	require.Equal(t, int64(500*len(frag)), s.Held())
}
