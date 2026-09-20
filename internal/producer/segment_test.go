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
