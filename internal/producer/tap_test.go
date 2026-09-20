package producer

import (
	"bytes"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// The tee under a saturated uplink (§39.6): when the relay stream cannot
// take the bytes, the tap drops them and counts, and the capture loop that
// feeds it never waits, so the recording is what keeps flowing.
func TestTapDropsRatherThanBlocks(t *testing.T) {
	b, err := os.ReadFile("testdata/av.ts")
	require.NoError(t, err)
	p := &Producer{cfg: Config{}, log: slog.Default()}
	l := newRelayLink(p)
	id := pdid.New(pdid.Domain(23))
	s := &source{cfg: SourceConfig{Alias: "door"}, row: api.Source_builder{Id: id.Bytes()}.Build()}
	l.start(id)

	// Nobody reads l.send: the relay stream is stuck. The recording plays
	// twenty times through the tap, ten thousand packets past the queue.
	began := time.Now()
	packets := 0
	for i := 0; i < 20; i++ {
		r := NewReader(bytes.NewReader(b))
		var pk Packet
		for r.Next(&pk) == nil {
			l.feed(s, &pk, r)
			packets++
		}
	}
	took := time.Since(began)

	require.Equal(t, sendQueue, len(l.send), "the queue is full")
	l.mu.Lock()
	dropped := l.dropped
	l.mu.Unlock()
	require.Greater(t, dropped, int64(0), "batches were dropped")
	require.Less(t, took, 2*time.Second, "%d packets through the tap took %s: the tap held the capture loop", packets, took)
	// What was queued plus what was dropped is what came in, in batches.
	require.InDelta(t, packets/32, int(dropped)+sendQueue, 2)
}
