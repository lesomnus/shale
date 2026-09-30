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
	b, err := os.ReadFile("testdata/av.mp4")
	require.NoError(t, err)
	p := &Producer{cfg: Config{}, log: slog.Default()}
	l := newRelayLink(p)
	id := pdid.New(pdid.Domain(23))
	s := &source{cfg: SourceConfig{Alias: "door"}, row: api.Source_builder{Id: id.Bytes()}.Build()}
	l.start(id)

	// Nobody reads l.send: the relay stream is stuck. The recording plays
	// twenty times through the tap, hundreds of fragments past the queue.
	began := time.Now()
	sent, frags := 0, 0
	for i := 0; i < 20; i++ {
		r := NewMP4Reader(bytes.NewReader(b))
		var f Frame
		keyed := false
		for r.Next(&f) == nil {
			l.feed(s, &f, r.Init())
			// What the tap sends: from the first key fragment on, the
			// init segment in front of it.
			switch {
			case f.Kind == FramePrefix:
				keyed = false
			case !keyed && f.Key:
				keyed, sent = true, sent+2
			case keyed:
				sent++
			}
			if f.Kind == FrameData {
				frags++
			}
		}
	}
	took := time.Since(began)

	require.Equal(t, sendQueue, len(l.send), "the queue is full")
	l.mu.Lock()
	dropped := l.dropped
	l.mu.Unlock()
	require.Greater(t, dropped, int64(0), "fragments were dropped")
	require.Less(t, took, 2*time.Second, "%d fragments through the tap took %s: the tap held the capture loop", frags, took)
	// What was queued plus what was dropped is what was sent.
	require.Equal(t, sent, int(dropped)+sendQueue)
}
