package producer

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/internal/fmp4"
)

// The live helper (§38.7): a stream whose audio is AAC goes in, the same
// video comes out with the audio as Opus, as an init segment and
// fragments the relay takes as they are, from a key fragment.
func TestLiveHelperTranscodes(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("no ffmpeg on this host")
	}
	b, err := os.ReadFile("testdata/aac.mp4")
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

	// The tee's units, as feed hands them over: the init segment, then
	// the fragments.
	r := fmp4.NewReader(bytes.NewReader(b))
	for {
		u, err := r.Next()
		if err != nil {
			break
		}
		require.True(t, h.write(u.Bytes()), "the helper keeps up")
	}
	l.stop(id)
	select {
	case <-h.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the helper did not exit after its input ended")
	}

	// One unit per message: the messages read back as as many units.
	var all []byte
	messages := 0
	for done := false; !done; {
		select {
		case msg := <-l.send:
			all = append(all, msg.GetData().GetPayload()...)
			messages++
		default:
			done = true
		}
	}
	out := fmp4.NewReader(bytes.NewReader(all))
	var init *fmp4.Init
	units, video, keyFirst, first := 0, 0, false, true
	for {
		u, err := out.Next()
		if err != nil {
			break
		}
		units++
		if u.Init != nil {
			init = u.Init
			continue
		}
		require.NotNil(t, init, "the init segment comes first")
		if v := u.Frag.Video(init); v != nil {
			if first {
				keyFirst, first = v.Key(), false
			}
			video += len(v.Samples)
		}
	}
	require.Equal(t, messages, units, "a whole unit per message")
	require.NotNil(t, init)
	require.NotNil(t, init.Opus(), "the helper's output has an Opus track")
	require.Len(t, init.Audio(), 1, "and only that: the AAC was replaced")
	require.True(t, keyFirst, "the output starts at a key fragment")
	// 4 s at 30 fps is 120 frames; an ffmpeg may hold back a few at the
	// tail when its input ends.
	require.GreaterOrEqual(t, video, 105, "4 s at 30 fps, none lost to the probe")
	require.Equal(t, int64(0), l.transcodes())
}
