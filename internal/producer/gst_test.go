package producer

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The Pi's three cameras as they record (§38.3): MJPEG decoded in
// software, the hardware encoder variable-rate at 90% of the video
// ceiling, a keyframe every 60 frames with its parameter sets, TS out.
func TestGstArgsPi(t *testing.T) {
	c := SourceConfig{Alias: "cam-1", Capture: CaptureGStreamer, Input: "v4l2:/dev/v4l/by-path/usb-0:1.1:1.0-video-index0", Format: "mjpeg", Size: "1280x720", Fps: 30}
	args := GstArgs(c, "v4l2h264enc", 2_200_000, 2*time.Second)
	require.Equal(t, []string{
		"-q", "-e",
		"v4l2src", "device=/dev/v4l/by-path/usb-0:1.1:1.0-video-index0", "!",
		"image/jpeg,width=1280,height=720,framerate=30/1", "!",
		"jpegdec", "!", "videoconvert", "!", "video/x-raw,format=I420", "!",
		// (2.2 Mbps / 1.05) × 0.9
		"v4l2h264enc", "extra-controls=controls,video_bitrate=1885714,video_bitrate_mode=0,h264_i_frame_period=60,repeat_sequence_header=1", "!",
		"video/x-h264,level=(string)4.1", "!",
		"h264parse", "config-interval=-1", "!", "mpegtsmux", "!", "fdsink", "fd=1",
	}, args)
}

func TestGstArgsOthers(t *testing.T) {
	// YUYV, x264 in software, a keyframe every 4 s at 15 fps.
	c := SourceConfig{Capture: CaptureGStreamer, Input: "v4l2:/dev/video0", Format: "yuyv", Size: "640x480", Fps: 15}
	got := strings.Join(GstArgs(c, "x264enc", 1_050_000, 4*time.Second), " ")
	require.Contains(t, got, "video/x-raw,format=YUY2,width=640,height=480,framerate=15/1 ! videoconvert")
	require.Contains(t, got, "x264enc speed-preset=veryfast tune=zerolatency bitrate=900 vbv-buf-capacity=1000 key-int-max=60 !")
	require.NotContains(t, got, "jpegdec")

	// A camera's own H.264, copied: parsed and muxed, nothing decoded.
	c = SourceConfig{Capture: CaptureGStreamer, Input: "v4l2:/dev/video2", Format: "h264", Size: "1920x1080", Fps: 30}
	got = strings.Join(GstArgs(c, "copy", 4_000_000, 2*time.Second), " ")
	require.Equal(t, "-q -e v4l2src device=/dev/video2 ! video/x-h264,width=1920,height=1080,framerate=30/1 ! h264parse config-interval=-1 ! mpegtsmux ! fdsink fd=1", got)

	// No size or rate said: 720p30, as the Pi's cameras run.
	c = SourceConfig{Capture: CaptureGStreamer, Input: "v4l2:/dev/video0"}
	got = strings.Join(GstArgs(c, "v4l2h264enc", 2_000_000, 0), " ")
	require.Contains(t, got, "image/jpeg,width=1280,height=720,framerate=30/1")
	require.Contains(t, got, "h264_i_frame_period=60,")

	// Above 1080p30 the level rises with it.
	c = SourceConfig{Capture: CaptureGStreamer, Input: "v4l2:/dev/video0", Size: "1920x1080", Fps: 60}
	require.Contains(t, strings.Join(GstArgs(c, "v4l2h264enc", 8_000_000, 0), " "), "level=(string)4.2")
}

// What a GStreamer source may say, refused at start rather than ignored.
func TestCheckGst(t *testing.T) {
	ok := SourceConfig{Alias: "a", Capture: "GStreamer", Input: "v4l2:/dev/video0", Format: "mjpeg", Encoder: "auto"}
	require.NoError(t, ok.checkGst())
	require.NoError(t, SourceConfig{Input: "rtsp://x"}.checkGst(), "ffmpeg, the default, is not checked here")
	require.NoError(t, SourceConfig{Capture: "ffmpeg", Input: "rtsp://x"}.checkGst())

	for name, mut := range map[string]func(*SourceConfig){
		"tool":     func(c *SourceConfig) { c.Capture = "vlc" },
		"command":  func(c *SourceConfig) { c.Command = "true" },
		"rtsp":     func(c *SourceConfig) { c.Input = "rtsp://10.0.0.1/s" },
		"mic":      func(c *SourceConfig) { c.Audio = &AudioConfig{Device: "alsa:hw:1"} },
		"codec":    func(c *SourceConfig) { c.Audio = &AudioConfig{Codec: "aac"} },
		"idle":     func(c *SourceConfig) { c.Idle = &IdleConfig{} },
		"tier2":    func(c *SourceConfig) { c.ExtraOutputArgs = []string{"-x"} },
		"format":   func(c *SourceConfig) { c.Format = "h265" },
		"encoder":  func(c *SourceConfig) { c.Encoder = "h264_v4l2m2m" },
		"copy raw": func(c *SourceConfig) { c.Encoder = "copy" },
	} {
		c := ok
		mut(&c)
		require.Error(t, c.checkGst(), name)
	}
	silent := ok
	silent.Audio = &AudioConfig{Codec: "none"}
	require.NoError(t, silent.checkGst(), "audio: none is what it does anyway")

	_, err := New(Config{Sources: []SourceConfig{{Alias: "a", Capture: CaptureGStreamer, Input: "rtsp://x"}}})
	require.ErrorContains(t, err, "source a: capture: gstreamer takes a V4L2 camera")
}

// A capture is stopped with an interrupt to its process group, and a
// pipeline in it hears it: what lets gst-launch -e drain the Pi's encoder.
func TestCaptureStopsGracefully(t *testing.T) {
	dir := t.TempDir()
	mark := filepath.Join(dir, "interrupted")
	child := filepath.Join(dir, "child")
	// Both sides of a pipeline trap the interrupt, as gst-launch -e and
	// the shell around it would; the stdout stays open until they end.
	cmd := "(trap 'echo first >>" + mark + "; exit 0' INT; echo up >" + child + "; while :; do echo f; sleep 0.05; done)" +
		" | (trap 'echo second >>" + mark + "; exit 0' INT; cat)"
	c := &Capture{
		Source:  SourceConfig{Alias: "a", Command: cmd},
		Log:     slogOf(nil),
		Profile: func() (int64, time.Duration) { return 1_000_000, 2 * time.Second },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx, func(r io.Reader) { io.Copy(io.Discard, r) })
	}()
	require.Eventually(t, func() bool { _, err := os.Stat(child); return err == nil }, 5*time.Second, 20*time.Millisecond)
	began := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(stopGrace + 5*time.Second):
		t.Fatal("the capture did not end")
	}
	require.Less(t, time.Since(began), stopGrace, "ended on the interrupt, not the kill")
	b, err := os.ReadFile(mark)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"first", "second"}, strings.Fields(string(b)), "the whole group heard it")
}

// A capture that ignores the interrupt is killed, group and all, once the
// grace is over.
func TestCaptureKilledAfterGrace(t *testing.T) {
	c := &Capture{
		Source:  SourceConfig{Alias: "a", Command: "trap '' INT; (trap '' INT; while :; do sleep 0.05; done) | cat"},
		Log:     slogOf(nil),
		Profile: func() (int64, time.Duration) { return 1_000_000, 2 * time.Second },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx, func(r io.Reader) { io.Copy(io.Discard, r) })
	}()
	require.Eventually(t, c.Up, 5*time.Second, 20*time.Millisecond)
	began := time.Now()
	cancel()
	select {
	case <-done:
	case <-time.After(stopGrace + 5*time.Second):
		t.Fatal("the capture outlived its kill")
	}
	require.GreaterOrEqual(t, time.Since(began), stopGrace-100*time.Millisecond, "the interrupt was ignored, so the kill ended it")
}
