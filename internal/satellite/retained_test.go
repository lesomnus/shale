package satellite

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/internal/upload"
)

// §41.9: a new segment is admitted within its producer's share, the pool
// over the producers using it, and within the pool; a producer stuck
// behind a failing node fills its share and no more, so another still
// gets its own. Nothing is evicted: bytes leave as segments do.
func TestRetainedShares(t *testing.T) {
	now := time.Now()
	r := newRetained(100, 0)
	r.now = func() time.Time { return now }
	a, b, c := pdid.New(DomProducer), pdid.New(DomProducer), pdid.New(DomProducer)

	// Alone, a producer's share is the pool.
	ok, _ := r.admit(a, 60)
	require.True(t, ok)
	ok, why := r.admit(a, 50)
	require.False(t, ok, "past the pool")
	require.Equal(t, upload.PassFull, why)
	require.Equal(t, int64(100), r.shares()[0].GetShare(), "alone, the share is the pool")

	// Another producer comes: the pool is shared by two, and the first,
	// past its half, takes nothing new; the second has its half.
	ok, why = r.admit(b, 30)
	require.True(t, ok, why)
	ok, why = r.admit(a, 1)
	require.False(t, ok)
	require.Equal(t, upload.PassShare, why)
	ok, why = r.admit(b, 20)
	require.False(t, ok, "within its share, past the pool")
	require.Equal(t, upload.PassFull, why)
	used, peak, budget := r.usage()
	require.Equal(t, int64(90), used)
	require.Equal(t, int64(90), peak)
	require.Equal(t, int64(100), budget)

	// A segment already taken grows within the pool alone, whatever the
	// shares say now; never past it.
	require.Equal(t, int64(10), r.grow(a, 10))
	require.Zero(t, r.grow(a, 1), "the pool is full")

	// What is delivered leaves; nothing else does.
	r.free(a, 70)
	require.Equal(t, int64(0), r.holding(a))
	require.Equal(t, int64(30), r.holding(b))
	ok, why = r.admit(c, 30)
	require.True(t, ok, why)

	// A third producer asking: three share the pool.
	shares := r.shares()
	require.Len(t, shares, 3)
	for _, s := range shares {
		require.Equal(t, int64(33), s.GetShare())
	}

	// One that has asked for nothing for a while and holds nothing no
	// longer counts.
	now = now.Add(activeFor + time.Second)
	require.Len(t, r.shares(), 2, "a and its empty share are gone")
	ok, why = r.admit(b, 20)
	require.True(t, ok, "two share it now: %s", why)
}

// The cap bounds a producer's share below the equal one: alone, it does
// not take the whole pool.
func TestRetainedCap(t *testing.T) {
	r := newRetained(100, 40)
	a := pdid.New(DomProducer)
	ok, _ := r.admit(a, 40)
	require.True(t, ok)
	ok, why := r.admit(a, 1)
	require.False(t, ok)
	require.Equal(t, upload.PassShare, why)
	require.Equal(t, int64(40), r.shares()[0].GetShare())

	// No pool at all takes nothing.
	var none *retained
	ok, why = none.admit(a, 1)
	require.False(t, ok)
	require.Equal(t, upload.PassRefused, why)
	used, _, budget := none.usage()
	require.Zero(t, used)
	require.Zero(t, budget)
}
