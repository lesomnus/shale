package producer

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/internal/mpegts"
)

// The live helper (§38.7): the tee of a camera recording AAC goes through
// ffmpeg, and what reaches the relay's queue is the same video with the
// audio as Opus, starting at a keyframe, in whole packets.
func TestLiveHelperTranscodes(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("no ffmpeg on this host")
	}
	b, err := os.ReadFile("testdata/aac.ts")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p := &Producer{cfg: Config{Ffmpeg: "ffmpeg"}, log: slog.Default()}
	l := newRelayLink(p)
	l.ctx = ctx
	id := pdid.New(pdid.Domain(23))
	s := &source{cfg: SourceConfig{Alias: "aac"}}
	l.active[id] = &tap{}
	h, err := l.startHelper(id, s)
	require.NoError(t, err)
	l.active[id].h = h
	require.Equal(t, int64(1), l.transcodes())

	// The tee's batches, as feed hands them over.
	for i := 0; i < len(b); i += tapFlush {
		require.True(t, h.write(b[i:min(i+tapFlush, len(b))]), "the helper keeps up")
	}
	l.stop(id)
	select {
	case <-h.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the helper did not exit after its input ended")
	}

	d := mpegts.New()
	video, audio := 0, 0
	keyFirst, first := false, true
	for done := false; !done; {
		select {
		case msg := <-l.send:
			units, au, err := d.WriteAll(msg.GetData().GetPayload())
			require.NoError(t, err, "whole packets")
			for _, u := range units {
				if first {
					keyFirst, first = u.Keyframe, false
				}
				video++
			}
			audio += len(au)
		default:
			done = true
		}
	}
	require.True(t, d.HasOpus(), "the helper's output names an Opus stream")
	require.True(t, keyFirst, "the output starts at a keyframe")
	require.GreaterOrEqual(t, video, 118, "4 s at 30 fps, none lost to the probe")
	require.GreaterOrEqual(t, audio, 190, "4 s of 20 ms packets")
	require.Equal(t, int64(0), l.transcodes())
}
