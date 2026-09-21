package producer

import (
	"context"
	"io"
	"log/slog"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The audio of a capture (§38.3): a camera's own audio is copied, a
// microphone is a second input that is encoded, a USB camera has none,
// and `audio.codec` overrides.
func TestArgsAudio(t *testing.T) {
	cases := []struct {
		name    string
		cfg     SourceConfig
		encoder string
		want    []string
		not     []string
	}{
		{"a camera's audio is copied", SourceConfig{Input: "rtsp://cam/1", Format: "h264"}, "copy",
			[]string{"-c:v copy", "-c:a copy"}, []string{"-an", "-map"}},
		{"a camera re-encoded keeps its audio", SourceConfig{Input: "rtsp://cam/1", Format: "mjpeg"}, "libx264",
			[]string{"-c:v libx264", "-c:a copy"}, []string{"-an"}},
		{"a USB camera has no audio", SourceConfig{Input: "v4l2:/dev/video0", Format: "mjpeg"}, "libx264",
			[]string{"-an"}, []string{"-c:a"}},
		{"a microphone is encoded and mapped", SourceConfig{Input: "v4l2:/dev/video0", Format: "mjpeg", Audio: &AudioConfig{Device: "alsa:hw:1"}}, "libx264",
			[]string{"-f alsa -i hw:1", "-map 0:v:0 -map 1:a:0", "-c:a aac -b:a 64000"}, []string{"-an"}},
		{"a microphone as Opus", SourceConfig{Input: "v4l2:/dev/video0", Format: "mjpeg", Audio: &AudioConfig{Device: "alsa:hw:1", Codec: "opus", Bitrate: 48000}}, "libx264",
			[]string{"-c:a libopus -b:a 48000"}, []string{"-an"}},
		{"a camera's audio as AAC", SourceConfig{Input: "rtsp://cam/1", Format: "h264", Audio: &AudioConfig{Codec: "aac", Bitrate: 32000}}, "copy",
			[]string{"-c:a aac -b:a 32000"}, []string{"-map", "-an"}},
		{"a camera's audio as Opus", SourceConfig{Input: "rtsp://cam/1", Format: "h264", Audio: &AudioConfig{Codec: "opus"}}, "copy",
			[]string{"-c:a libopus -b:a 64000"}, []string{"-an"}},
		{"none drops it", SourceConfig{Input: "rtsp://cam/1", Format: "h264", Audio: &AudioConfig{Codec: "none"}}, "copy",
			[]string{"-an"}, []string{"-c:a"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			args := strings.Join(Args(c.cfg, c.encoder, 2_000_000, 2*time.Second), " ")
			for _, w := range c.want {
				require.Contains(t, args, w)
			}
			for _, n := range c.not {
				require.NotContains(t, args, n)
			}
		})
	}
}

// A camera whose audio TS has no type for (G.711 here) is copied into a
// private stream nothing names; the first PMT shows it, and the capture is
// restarted once encoding the audio as AAC (§38.3).
func TestCaptureAudioFallback(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("no ffmpeg on this host")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := &Capture{
		Ffmpeg:  ffmpeg,
		Log:     slog.Default(),
		Profile: func() (int64, time.Duration) { return 2_000_000, 2 * time.Second },
		Source:  SourceConfig{Alias: "ulaw", Input: "file:testdata/ulaw.mkv", Format: "h264"},
	}
	var runs []Streams
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx, func(r io.Reader) {
			rd := NewReader(r)
			var p Packet
			for {
				if err := rd.Next(&p); err != nil {
					return
				}
				if len(rd.Streams().PMT) == 0 {
					continue
				}
				st := rd.Streams()
				runs = append(runs, st)
				if c.CheckAudio(st) {
					// The first run: restarted with the audio encoded.
					return
				}
				// The second run: seen enough.
				cancel()

				return
			}
		})
	}()
	select {
	case <-done:
	case <-time.After(40 * time.Second):
		t.Fatal("the capture did not end")
	}
	require.Len(t, runs, 2)
	require.True(t, runs[0].AudioAnon, "G.711 copied into TS is a private stream nothing names")
	require.True(t, runs[0].HasAudio())
	require.Equal(t, []byte{0x0f}, runs[1].AudioTypes, "the restart encodes it as AAC")
	require.False(t, runs[1].AudioAnon)
	require.Equal(t, "aac", c.AudioFallback())
	require.Equal(t, int64(0), c.Restarts(), "a restart on purpose is not a restart")
}

// A source with `idle:` runs ffmpeg at info level with a measuring branch
// beside the encoder (§38.10).
func TestArgsIdle(t *testing.T) {
	src := SourceConfig{Alias: "a", Input: "v4l2:/dev/video0", Format: "mjpeg", Size: "1280x720", Fps: 30, Idle: &IdleConfig{}}
	args := strings.Join(Args(src, "libx264", 2_000_000, 2*time.Second), " ")
	for _, want := range []string{"-loglevel info", "-vf split[v][m];[m]fps=1,blackframe=amount=98:threshold=26,nullsink;[v]null", "-pix_fmt yuv420p"} {
		if !strings.Contains(args, want) {
			t.Fatalf("args lack %q: %s", want, args)
		}
	}
	src.Idle = nil
	args = strings.Join(Args(src, "libx264", 2_000_000, 2*time.Second), " ")
	if strings.Contains(args, "-vf") || !strings.Contains(args, "-loglevel warning") {
		t.Fatalf("args without idle: %s", args)
	}
}
