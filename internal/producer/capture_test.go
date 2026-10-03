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

	"github.com/lesomnus/shale/internal/fmp4"
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
		// The archive's track and Opus beside it for live (§38.7): the
		// camera's audio is mapped twice, copied once and encoded once.
		{"a camera's audio is copied, and Opus beside it", SourceConfig{Input: "rtsp://cam/1", Format: "h264"}, "copy",
			[]string{"-c:v copy", "-map 0:v:0 -map 0:a:0? -map 0:a:0?", "-c:a:0 copy", "-c:a:1 libopus -b:a:1 64000"}, []string{"-an", "-c:a copy", "-b:a:0"}},
		{"a camera re-encoded keeps its audio", SourceConfig{Input: "rtsp://cam/1", Format: "mjpeg"}, "libx264",
			[]string{"-c:v libx264", "-c:a:0 copy", "-c:a:1 libopus"}, []string{"-an"}},
		{"a USB camera has no audio", SourceConfig{Input: "v4l2:/dev/video0", Format: "mjpeg"}, "libx264",
			[]string{"-an"}, []string{"-c:a", "-map"}},
		{"a microphone is encoded and mapped, twice", SourceConfig{Input: "v4l2:/dev/video0", Format: "mjpeg", Audio: &AudioConfig{Device: "alsa:hw:1"}}, "libx264",
			[]string{"-f alsa -i hw:1", "-map 0:v:0 -map 1:a:0 -map 1:a:0", "-c:a:0 aac -b:a:0 64000", "-c:a:1 libopus -b:a:1 64000"}, []string{"-an"}},
		// Opus is what live plays: one track does for both.
		{"a microphone as Opus", SourceConfig{Input: "v4l2:/dev/video0", Format: "mjpeg", Audio: &AudioConfig{Device: "alsa:hw:1", Codec: "opus", Bitrate: 48000}}, "libx264",
			[]string{"-map 0:v:0 -map 1:a:0 -c:a libopus -b:a 48000"}, []string{"-an", "-c:a:1", "-map 1:a:0 -map 1:a:0"}},
		{"a camera's audio as AAC, and Opus beside it", SourceConfig{Input: "rtsp://cam/1", Format: "h264", Audio: &AudioConfig{Codec: "aac", Bitrate: 32000}}, "copy",
			[]string{"-c:a:0 aac -b:a:0 32000", "-c:a:1 libopus -b:a:1 32000"}, []string{"-an"}},
		{"a camera's audio as Opus", SourceConfig{Input: "rtsp://cam/1", Format: "h264", Audio: &AudioConfig{Codec: "opus"}}, "copy",
			[]string{"-c:a libopus -b:a 64000"}, []string{"-an", "-map", "-c:a:1"}},
		{"none drops it", SourceConfig{Input: "rtsp://cam/1", Format: "h264", Audio: &AudioConfig{Codec: "none"}}, "copy",
			[]string{"-an"}, []string{"-c:a", "-map"}},
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

// A camera whose audio MP4 has no entry for (G.711 here) makes the muxer
// refuse at start; the refusal on stderr restarts the capture once,
// encoding the audio as AAC, with Opus beside it (§38.3, §38.7).
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
	var inits []*fmp4.Init
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx, func(r io.Reader) {
			rd := NewMP4Reader(r)
			var f Frame
			for {
				if err := rd.Next(&f); err != nil {
					return
				}
				if f.Kind == FramePrefix {
					inits = append(inits, rd.Init())
					// Seen enough: the run that muxed is the one after
					// the refusal.
					cancel()

					return
				}
			}
		})
	}()
	select {
	case <-done:
	case <-time.After(40 * time.Second):
		t.Fatal("the capture did not end")
	}
	require.Len(t, inits, 1, "the first run wrote nothing: the muxer refused")
	audio := inits[0].Audio()
	require.Len(t, audio, 2)
	require.Equal(t, "mp4a", audio[0].Entry, "the restart encodes it as AAC")
	require.Equal(t, "Opus", audio[1].Entry, "with Opus beside it for live (§38.7)")
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

// An encoder that hands ffmpeg no parameter sets goes through a second
// stage (§38.3): the first writes TS, the second remuxes it to fragmented
// MP4. Forced here on the software encoder, the output is what a capture
// writes: an init segment with the tracks and fragments from a keyframe.
func TestCaptureRemux(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("no ffmpeg on this host")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := &Capture{
		Ffmpeg:  ffmpeg,
		Log:     slog.Default(),
		Remux:   true,
		Profile: func() (int64, time.Duration) { return 2_000_000, 2 * time.Second },
		Source:  DemoSources(1)[0],
	}
	var init *fmp4.Init
	var frames []Frame
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx, func(r io.Reader) {
			rd := NewMP4Reader(r)
			var f Frame
			for {
				if err := rd.Next(&f); err != nil {
					return
				}
				if f.Kind == FramePrefix {
					init = rd.Init()
					continue
				}
				frames = append(frames, Frame{Key: f.Key, Frames: f.Frames, Seq: f.Seq})
				if len(frames) >= 6 {
					cancel()

					return
				}
			}
		})
	}()
	select {
	case <-done:
	case <-time.After(40 * time.Second):
		t.Fatal("the capture did not end")
	}
	require.NotNil(t, init, "an init segment came through the second stage")
	require.Equal(t, "avc1", init.Video().Entry)
	require.NotEmpty(t, init.Video().ParamSets(), "the parameter sets are in avcC")
	require.Equal(t, []string{"mp4a", "Opus"}, func() []string {
		var out []string
		for _, a := range init.Audio() {
			out = append(out, a.Entry)
		}
		return out
	}())
	require.NotEmpty(t, audioSpecificConfig(init.Audio()[0].Config), "the AAC's configuration is in its esds: a browser decodes nothing without it")
	require.GreaterOrEqual(t, len(frames), 6)
	require.True(t, frames[0].Key, "the first fragment starts at a keyframe")
	keys := 0
	for i, f := range frames {
		if f.Key {
			keys++
		}
		require.Equal(t, uint32(i+1), f.Seq, "fragments numbered in order")
	}
	require.LessOrEqual(t, keys, 2, "keyframes every 2 s, not every frame: the second stage's parser decides")
	require.Equal(t, int64(0), c.Restarts())
}

// The Raspberry Pi's encoder is remuxed (§38.3): the first stage writes TS,
// the second reads it and writes fragmented MP4, with the AAC re-framed.
func TestArgsRemux(t *testing.T) {
	src := DemoSources(1)[0]
	first := strings.Join(Args(src, "h264_v4l2m2m", 2_000_000, 2*time.Second), " ")
	require.True(t, strings.HasSuffix(first, "-f mpegts -"), first)
	require.NotContains(t, first, "movflags")
	second := strings.Join(RemuxArgs(src.aacArchive()), " ")
	require.Contains(t, second, "-f mpegts -i pipe:0 -map 0 -c copy -bsf:a:0 aac_adtstoasc -f mp4 -movflags frag_keyframe+empty_moov+delay_moov+default_base_moof -frag_duration 500000 pipe:1")
	t.Logf("pi pipeline: ffmpeg %s | ffmpeg %s", first, second)
	// Opus alone needs no re-framing; a silent source has no audio to re-frame.
	src.Audio = &AudioConfig{Codec: "opus"}
	require.NotContains(t, strings.Join(RemuxArgs(src.aacArchive()), " "), "aac_adtstoasc")
	usb := SourceConfig{Input: "v4l2:/dev/video0"}
	require.False(t, usb.aacArchive())
	// Every other encoder muxes in one stage.
	require.Contains(t, strings.Join(Args(src, "libx264", 2_000_000, 2*time.Second), " "), "-f mp4 -movflags")
}

// audioSpecificConfig is the DecoderSpecificInfo of an esds payload (ISO
// 14496-1): the ES descriptor, its decoder configuration, and the
// configuration inside that; nil when there is none.
func audioSpecificConfig(esds []byte) []byte {
	p := 4 // version and flags
	next := func() (tag byte, body []byte) {
		if p+2 > len(esds) {
			return 0, nil
		}
		tag = esds[p]
		p++
		n := 0
		for i := 0; i < 4 && p < len(esds); i++ {
			x := esds[p]
			p++
			n = n<<7 | int(x&0x7f)
			if x&0x80 == 0 {
				break
			}
		}
		end := min(len(esds), p+n)
		body = esds[p:end]

		return tag, body
	}
	tag, es := next()
	if tag != 0x03 || len(es) < 3 {
		return nil
	}
	flags := es[2]
	p += 3
	if flags&0x80 != 0 {
		p += 2
	}
	if flags&0x40 != 0 && p < len(esds) {
		p += 1 + int(esds[p])
	}
	if flags&0x20 != 0 {
		p += 2
	}
	tag, _ = next()
	if tag != 0x04 {
		return nil
	}
	p += 13
	tag, dsi := next()
	if tag != 0x05 {
		return nil
	}

	return dsi
}
