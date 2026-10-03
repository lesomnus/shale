package producer

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The Pi's three cameras as they record (§38.3): MJPEG decoded in
// software, the hardware encoder variable-rate at 90% of the video
// ceiling, a keyframe every 60 frames with its parameter sets, TS out.
func TestGstArgsPi(t *testing.T) {
	c := SourceConfig{Alias: "cam-1", Capture: CaptureGStreamer, Input: "v4l2:/dev/v4l/by-path/usb-0:1.1:1.0-video-index0", Format: "mjpeg", Size: "1280x720", Fps: 30}
	args := GstArgs(c, "v4l2h264enc", 2_200_000, 2*time.Second, "voaacenc")
	require.Equal(t, []string{
		"-q", "-e", "mpegtsmux", "name=mux", "!", "fdsink", "fd=1",
		"v4l2src", "device=/dev/v4l/by-path/usb-0:1.1:1.0-video-index0", "!",
		"image/jpeg,width=1280,height=720,framerate=30/1", "!",
		"jpegdec", "!", "videoconvert", "!", "video/x-raw,format=I420", "!",
		// (2.2 Mbps / 1.05) × 0.9
		"v4l2h264enc", "extra-controls=controls,video_bitrate=1885714,video_bitrate_mode=0,h264_i_frame_period=60,repeat_sequence_header=1", "!",
		"video/x-h264,level=(string)4.1", "!",
		"h264parse", "config-interval=-1", "!", "queue", "!", "mux.sink_65",
	}, args)
	require.Equal(t, []string{
		"-hide_banner", "-loglevel", "warning", "-nostats", "-probesize", "262144", "-analyzeduration", "500000",
		"-f", "mpegts", "-i", "pipe:0", "-map", "0:v:0", "-map", "0:a?", "-c", "copy",
		"-f", "mp4", "-movflags", "frag_keyframe+empty_moov+delay_moov+default_base_moof", "-frag_duration", "500000", "pipe:1",
	}, GstRemuxArgs(false, false))
}

// A camera's microphone (§38.3, §38.7): AAC for the archive and Opus beside
// it, each at its own PID after the video, and both out of the ceiling.
func TestGstArgsAudio(t *testing.T) {
	c := SourceConfig{Capture: CaptureGStreamer, Input: "v4l2:/dev/video0", Format: "mjpeg", Size: "1280x720", Fps: 30,
		Audio: &AudioConfig{Device: "alsa:hw:2,0"}}
	got := strings.Join(GstArgs(c, "v4l2h264enc", 2_200_000, 2*time.Second, "voaacenc"), " ")
	// (2.2 Mbps − 2 × 64 kbps) / 1.05 × 0.9
	require.Contains(t, got, "video_bitrate=1775999,")
	require.Contains(t, got, "h264parse config-interval=-1 ! queue ! mux.sink_65 "+
		"alsasrc device=hw:2,0 ! queue ! audioconvert ! audioresample ! audio/x-raw,rate=48000,channels=1 ! tee name=mic "+
		"mic. ! queue ! voaacenc bitrate=64000 ! aacparse ! queue ! mux.sink_66 "+
		"mic. ! queue ! opusenc bitrate=64000 ! queue ! mux.sink_67")
	require.True(t, strings.HasSuffix(got, "mux.sink_67"))
	remux := strings.Join(GstRemuxArgs(true, true), " ")
	require.Contains(t, remux, "-probesize 5000000 -analyzeduration 3000000", "the sound starts before the picture")
	require.Contains(t, remux, "-map 0:v:0 -map 0:a? -c copy -bsf:a:0 aac_adtstoasc")

	// Opus alone, at 32 kbps: one track, one share.
	c.Audio = &AudioConfig{Device: "alsa:hw:2,0", Codec: "opus", Bitrate: 32_000}
	got = strings.Join(GstArgs(c, "v4l2h264enc", 2_200_000, 2*time.Second, "voaacenc"), " ")
	require.Contains(t, got, "video_bitrate=1858284,")
	require.Contains(t, got, "audio/x-raw,rate=48000,channels=1 ! opusenc bitrate=32000 ! queue ! mux.sink_67")
	require.NotContains(t, got, "tee")
	require.NotContains(t, got, "voaacenc")
	require.NotContains(t, strings.Join(GstRemuxArgs(false, true), " "), "aac_adtstoasc")

	// `none`: the microphone is not opened.
	c.Audio = &AudioConfig{Device: "alsa:hw:2,0", Codec: "none"}
	got = strings.Join(GstArgs(c, "v4l2h264enc", 2_200_000, 2*time.Second, "voaacenc"), " ")
	require.NotContains(t, got, "alsasrc")
	require.Contains(t, got, "video_bitrate=1885714,")
}

func TestGstArgsOthers(t *testing.T) {
	// YUYV, x264 in software, a keyframe every 4 s at 15 fps.
	c := SourceConfig{Capture: CaptureGStreamer, Input: "v4l2:/dev/video0", Format: "yuyv", Size: "640x480", Fps: 15}
	got := strings.Join(GstArgs(c, "x264enc", 1_050_000, 4*time.Second, ""), " ")
	require.Contains(t, got, "video/x-raw,format=YUY2,width=640,height=480,framerate=15/1 ! videoconvert")
	require.Contains(t, got, "x264enc speed-preset=veryfast tune=zerolatency bitrate=900 vbv-buf-capacity=1000 key-int-max=60 !")
	require.NotContains(t, got, "jpegdec")

	// A camera's own H.264, copied: parsed and muxed, nothing decoded.
	c = SourceConfig{Capture: CaptureGStreamer, Input: "v4l2:/dev/video2", Format: "h264", Size: "1920x1080", Fps: 30}
	got = strings.Join(GstArgs(c, "copy", 4_000_000, 2*time.Second, ""), " ")
	require.Equal(t, "-q -e mpegtsmux name=mux ! fdsink fd=1 v4l2src device=/dev/video2 ! video/x-h264,width=1920,height=1080,framerate=30/1 ! h264parse config-interval=-1 ! queue ! mux.sink_65", got)

	// No size or rate said: 720p30, as the Pi's cameras run.
	c = SourceConfig{Capture: CaptureGStreamer, Input: "v4l2:/dev/video0"}
	got = strings.Join(GstArgs(c, "v4l2h264enc", 2_000_000, 0, ""), " ")
	require.Contains(t, got, "image/jpeg,width=1280,height=720,framerate=30/1")
	require.Contains(t, got, "h264_i_frame_period=60,")

	// Above 1080p30 the level rises with it.
	c = SourceConfig{Capture: CaptureGStreamer, Input: "v4l2:/dev/video0", Size: "1920x1080", Fps: 60}
	require.Contains(t, strings.Join(GstArgs(c, "v4l2h264enc", 8_000_000, 0, ""), " "), "level=(string)4.2")
}

// What a GStreamer source may say, refused at start rather than ignored.
func TestCheckGst(t *testing.T) {
	ok := SourceConfig{Alias: "a", Capture: "GStreamer", Input: "v4l2:/dev/video0", Format: "mjpeg", Encoder: "auto"}
	require.NoError(t, ok.checkGst())
	require.NoError(t, SourceConfig{Input: "rtsp://x"}.checkGst(), "ffmpeg, the default, is not checked here")
	require.NoError(t, SourceConfig{Capture: "ffmpeg", Input: "rtsp://x"}.checkGst())

	for name, mut := range map[string]func(*SourceConfig){
		"tool":      func(c *SourceConfig) { c.Capture = "vlc" },
		"command":   func(c *SourceConfig) { c.Command = "true" },
		"rtsp":      func(c *SourceConfig) { c.Input = "rtsp://10.0.0.1/s" },
		"no device": func(c *SourceConfig) { c.Audio = &AudioConfig{Codec: "aac"} },
		"mic copy":  func(c *SourceConfig) { c.Audio = &AudioConfig{Device: "alsa:hw:1", Codec: "copy"} },
		"tier2":     func(c *SourceConfig) { c.ExtraOutputArgs = []string{"-x"} },
		"format":    func(c *SourceConfig) { c.Format = "h265" },
		"encoder":   func(c *SourceConfig) { c.Encoder = "h264_v4l2m2m" },
		"copy raw":  func(c *SourceConfig) { c.Encoder = "copy" },
	} {
		c := ok
		mut(&c)
		require.Error(t, c.checkGst(), name)
	}
	for name, a := range map[string]*AudioConfig{
		"none":      {Codec: "none"},
		"mic":       {Device: "alsa:hw:1"},
		"mic aac":   {Device: "alsa:/dev/snd/by-path/usb-0:1.1:1.2", Codec: "AAC"},
		"mic opus":  {Device: "alsa:hw:1", Codec: "opus"},
		"mic, none": {Device: "alsa:hw:1", Codec: "none"},
	} {
		c := ok
		c.Audio = a
		require.NoError(t, c.checkGst(), name)
	}

	_, err := New(Config{Sources: []SourceConfig{{Alias: "a", Capture: CaptureGStreamer, Input: "rtsp://x"}}})
	require.ErrorContains(t, err, "source a: capture: gstreamer takes a V4L2 camera")
}

// A microphone by the path of its USB port (§38.3): the card's control or
// capture node, wherever the port's link points this time; ALSA's own names
// pass as they are.
func TestAlsaDevice(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "controlC2"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pcmC1D3c"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "timer"), nil, 0o644))
	byPath := filepath.Join(dir, "by-path")
	require.NoError(t, os.Mkdir(byPath, 0o755))
	link := func(name, target string) string {
		p := filepath.Join(byPath, name)
		require.NoError(t, os.Symlink("../"+target, p))
		return p
	}

	for in, want := range map[string]string{
		"alsa:hw:1":                            "hw:1",
		"alsa:hw:CARD=WEBCAM,DEV=0":            "hw:CARD=WEBCAM,DEV=0",
		"plughw:0":                             "plughw:0",
		"alsa:" + link("usb-1.3", "controlC2"): "hw:2,0",
		link("usb-1.1", "pcmC1D3c"):            "hw:1,3",
	} {
		got, err := alsaDevice(in)
		require.NoError(t, err, in)
		require.Equal(t, want, got, in)
	}
	_, err := alsaDevice("alsa:" + link("hdmi", "timer"))
	require.ErrorContains(t, err, "not a sound card's control or capture node")
	_, err = alsaDevice("alsa:" + filepath.Join(byPath, "unplugged"))
	require.Error(t, err)
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

// A source with `idle:` (§38.10): the picture the encoder takes, teed, one
// a second, grey and 160 wide, to fd 3; none of it without.
func TestGstArgsDark(t *testing.T) {
	c := SourceConfig{Capture: CaptureGStreamer, Input: "v4l2:/dev/video0", Format: "mjpeg", Size: "1280x720", Fps: 30, Idle: &IdleConfig{}}
	got := strings.Join(GstArgs(c, "v4l2h264enc", 2_200_000, 2*time.Second, ""), " ")
	require.Contains(t, got, "videoconvert ! video/x-raw,format=I420 ! tee name=pic pic. ! queue ! identity drop-allocation=true ! v4l2h264enc ")
	require.True(t, strings.HasSuffix(got, " pic. ! queue leaky=downstream max-size-buffers=2 ! videorate drop-only=true ! video/x-raw,framerate=1/1 ! "+
		"videoscale ! videoconvert ! video/x-raw,format=GRAY8,width=160,height=90 ! fdsink fd=3"), got)

	// With a microphone the measuring branch still comes last.
	c.Audio = &AudioConfig{Device: "alsa:hw:1,0"}
	got = strings.Join(GstArgs(c, "v4l2h264enc", 2_200_000, 2*time.Second, "voaacenc"), " ")
	require.Contains(t, got, "mux.sink_67 pic. ! queue leaky=downstream")

	c = SourceConfig{Capture: CaptureGStreamer, Input: "v4l2:/dev/video0"}
	got = strings.Join(GstArgs(c, "v4l2h264enc", 2_200_000, 2*time.Second, ""), " ")
	require.NotContains(t, got, "tee")
	require.NotContains(t, got, "fd=3")

	_, err := New(Config{Sources: []SourceConfig{{Alias: "a", Capture: CaptureGStreamer, Input: "v4l2:/dev/video0", Idle: &IdleConfig{}}}})
	require.NoError(t, err, "idle is measured under GStreamer now")
}

func TestDarkFrame(t *testing.T) {
	for size, want := range map[string][2]int{"1280x720": {160, 90}, "": {160, 90}, "1920x1080": {160, 90}, "640x480": {160, 120}, "800x600": {160, 120}, "1024x576": {160, 90}} {
		w, h := darkSize(size)
		require.Equal(t, want, [2]int{w, h}, size)
	}
	pic := make([]byte, 100)
	for i := range pic {
		pic[i] = 20
	}
	require.True(t, darkFrame(pic, 26), "every pixel at or below")
	pic[0], pic[1] = 200, 200
	require.True(t, darkFrame(pic, 26), "98% is dark")
	pic[2] = 27
	require.False(t, darkFrame(pic, 26), "97% is not")
	require.False(t, darkFrame(nil, 26))
}

// The pictures arrive whole or not at all: a pipe that ends inside one
// counts the ones before it.
func TestReadDark(t *testing.T) {
	dark, lit := make([]byte, 160*90), bytes.Repeat([]byte{0xff}, 160*90)
	stream := append(append(append(append([]byte{}, dark...), lit...), dark...), dark[:100]...)
	n := 0
	readDark(bytes.NewReader(stream), 160, 90, 26, func() { n++ })
	require.Equal(t, 2, n)
}

// A GStreamer capture with `idle:` reads the measured pictures from fd 3
// of the process it runs, and tells every dark one (§38.10). gst-launch,
// gst-inspect and the second stage are scripts here: three dark pictures
// and a lit one, then a wait for the interrupt.
func TestCaptureGstDark(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0o755))
		return p
	}
	write("gst-inspect-1.0", "exit 0")
	gst := write("gst-launch-1.0", `
dark() { head -c 14400 /dev/zero; }
lit() { head -c 14400 /dev/zero | tr '\000' '\377'; }
{ dark; dark; dark; lit; } >&3
trap 'exit 0' INT
while :; do sleep 0.05; done`)
	remux := write("remux", "cat >/dev/null")
	var n atomic.Int64
	c := &Capture{
		Ffmpeg:  remux,
		Gst:     gst,
		Log:     slogOf(nil),
		Source:  SourceConfig{Alias: "a", Capture: CaptureGStreamer, Input: "v4l2:/dev/video0", Size: "1280x720", Idle: &IdleConfig{}},
		Profile: func() (int64, time.Duration) { return 2_000_000, 2 * time.Second },
		OnDark:  func() { n.Add(1) },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx, func(r io.Reader) { io.Copy(io.Discard, r) })
	}()
	require.Eventually(t, func() bool { return n.Load() == 3 }, 5*time.Second, 20*time.Millisecond)
	cancel()
	<-done
	require.Equal(t, int64(3), n.Load(), "the lit one is not")
	require.Equal(t, "v4l2h264enc", c.Encoder)
}
