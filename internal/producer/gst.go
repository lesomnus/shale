package producer

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// The GStreamer capture (§38.3): `capture: gstreamer` runs a camera through
// gst-launch instead of ffmpeg, for the Raspberry Pi's encoder above all.
// Through ffmpeg (`h264_v4l2m2m`) that encoder is constant-rate and three
// MJPEG cameras cost a Pi 400 3.35 cores; through GStreamer's
// `v4l2h264enc` its variable-rate mode is a control away, and the same
// three cost 0.85 ([producer bench](../../docs/producer-bench.md)). The
// pipeline writes MPEG-TS, and the second stage that `h264_v4l2m2m` uses
// already turns it into fragmented MP4 (RemuxArgs), so what the producer
// reads is the same as from any capture.

// The capture tools of `capture` (§38.3).
const (
	CaptureFFmpeg    = "ffmpeg"
	CaptureGStreamer = "gstreamer"
)

// GstEncoders are the encoders a GStreamer capture takes, in the order
// `auto` tries them: the Pi's hardware encoder, else x264 in software.
var GstEncoders = []string{"v4l2h264enc", "x264enc"}

// gstVBRShare is the share of the video ceiling a variable-rate encoder is
// asked to average. The Pi's encoder has no peak cap, only a target, and
// averaged 5% above it on three cameras (1.99 Mbps asked 1.9), so the
// target sits below the ceiling by more than that.
const gstVBRShare = 0.9

// gstreamer says the source is captured through GStreamer.
func (c SourceConfig) gstreamer() bool {
	return strings.EqualFold(strings.TrimSpace(c.Capture), CaptureGStreamer)
}

// checkGst is what a GStreamer source may say (§38.3): a V4L2 camera, video
// only, the tier-1 fields; what only ffmpeg understands is refused rather
// than ignored.
func (c SourceConfig) checkGst() error {
	switch strings.ToLower(strings.TrimSpace(c.Capture)) {
	case "", CaptureFFmpeg:
		return nil
	case CaptureGStreamer:
	default:
		return fmt.Errorf("capture %q is neither %s nor %s", c.Capture, CaptureFFmpeg, CaptureGStreamer)
	}
	switch {
	case c.Command != "":
		return fmt.Errorf("capture: %s and command are two ways to say the same thing; a command is run as it is", CaptureGStreamer)
	case !strings.HasPrefix(c.Input, "v4l2:"):
		return fmt.Errorf("capture: %s takes a V4L2 camera (input: v4l2:…), not %q", CaptureGStreamer, c.Input)
	case c.hasMic() || (c.Audio != nil && c.audioCodec() != "" && c.audioCodec() != "none"):
		return fmt.Errorf("capture: %s records video only; audio needs capture: %s", CaptureGStreamer, CaptureFFmpeg)
	case c.Idle != nil:
		return fmt.Errorf("capture: %s cannot measure a dark scene; idle needs capture: %s", CaptureGStreamer, CaptureFFmpeg)
	case len(c.EncoderOptions) > 0 || len(c.ExtraInputArgs) > 0 || len(c.ExtraOutputArgs) > 0:
		return fmt.Errorf("capture: %s has no ffmpeg to pass encoder_options or extra_*_args to", CaptureGStreamer)
	}
	switch strings.ToLower(c.Format) {
	case "", "mjpeg", "yuyv", "h264":
	default:
		return fmt.Errorf("capture: %s takes format mjpeg, yuyv or h264, not %q", CaptureGStreamer, c.Format)
	}
	switch c.Encoder {
	case "", "auto", "copy", "v4l2h264enc", "x264enc":
	default:
		return fmt.Errorf("capture: %s encodes with auto, v4l2h264enc, x264enc or copy, not %q", CaptureGStreamer, c.Encoder)
	}
	if c.Encoder == "copy" && strings.ToLower(c.Format) != "h264" {
		return fmt.Errorf("capture: %s copies only what a camera already encodes (format: h264)", CaptureGStreamer)
	}

	return nil
}

// GstArgs is the gst-launch command line for a source (§38.3): the camera,
// decoded and encoded to H.264 unless it sends H.264 itself, parsed so every
// keyframe carries its parameter sets, as MPEG-TS on standard output for
// the second stage. `ceiling` is the agreed max_bitrate and `keyframe` the
// agreed interval. `-e` makes an interrupt end the stream rather than the
// process, which is how the capture is stopped (gracefully).
func GstArgs(c SourceConfig, encoder string, ceiling int64, keyframe time.Duration) []string {
	w, h := parseSize(c.Size)
	if c.Size == "" {
		w, h = 1280, 720
	}
	fps := c.Fps
	if fps <= 0 {
		fps = 30
	}
	mode := fmt.Sprintf("width=%d,height=%d,framerate=%d/1", w, h, fps)
	args := []string{"-q", "-e", "v4l2src", "device=" + strings.TrimPrefix(c.Input, "v4l2:"), "!"}
	format := strings.ToLower(c.Format)
	if format == "h264" {
		args = append(args, "video/x-h264,"+mode, "!")
		if encoder == "copy" || encoder == "" {
			return append(args, gstTail()...)
		}
		args = append(args, "h264parse", "!", "avdec_h264", "!")
	} else if format == "yuyv" {
		args = append(args, "video/x-raw,format=YUY2,"+mode, "!")
	} else {
		// MJPEG, what a USB camera sends at 720p and above. Decoded in
		// software: the Pi's JPEG decoder takes two streams at most, and a
		// third leaves it wedged until a reboot.
		args = append(args, "image/jpeg,"+mode, "!", "jpegdec", "!")
	}
	args = append(args, "videoconvert", "!", "video/x-raw,format=I420", "!")

	if keyframe <= 0 {
		keyframe = 2 * time.Second
	}
	gop := int(float64(fps) * keyframe.Seconds())
	target := int64(float64(videoCeiling(ceiling, 0)) * gstVBRShare)
	switch encoder {
	case "x264enc":
		// Average bitrate in kbit/s with a one-second buffer around it.
		args = append(args, "x264enc", "speed-preset=veryfast", "tune=zerolatency",
			"bitrate="+strconv.FormatInt(target/1000, 10), "vbv-buf-capacity=1000",
			"key-int-max="+strconv.Itoa(gop), "!")
	default:
		// The Pi's encoder (bcm2835-codec), variable-rate: in its
		// constant-rate mode three cameras got about 12 frames a second
		// each. The level allows 1080p30 at the rates a camera is given;
		// left to itself it picks one too low for 720p30.
		level := "4.1"
		if w*h*fps > 1920*1080*30 {
			level = "4.2"
		}
		args = append(args, "v4l2h264enc", "extra-controls=controls,video_bitrate="+strconv.FormatInt(target, 10)+
			",video_bitrate_mode=0,h264_i_frame_period="+strconv.Itoa(gop)+",repeat_sequence_header=1", "!",
			"video/x-h264,level=(string)"+level, "!")
	}

	return append(args, gstTail()...)
}

// gstTail parses the H.264 so every keyframe carries its parameter sets,
// and writes MPEG-TS to standard output.
func gstTail() []string {
	return []string{"h264parse", "config-interval=-1", "!", "mpegtsmux", "!", "fdsink", "fd=1"}
}

// videoCeiling is the video's share of a ceiling (§38.3): what the audio
// tracks take, then the container's slack, off; never below 100 kbps.
func videoCeiling(ceiling, audioBps int64) int64 {
	v := int64(float64(ceiling-audioBps) / TsOverhead)
	if v < 100_000 {
		v = 100_000
	}

	return v
}

// gstInspect is gst-inspect beside a gst-launch binary, or on PATH.
func gstInspect(launch string) string {
	if dir := filepath.Dir(launch); dir != "." && dir != "" {
		return filepath.Join(dir, "gst-inspect-1.0")
	}

	return "gst-inspect-1.0"
}

// PickGstEncoder is `auto` for a GStreamer capture: the first of
// GstEncoders this host's GStreamer has, else x264enc, which fails at
// start with a message saying what is missing.
func PickGstEncoder(ctx context.Context, launch string) string {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, e := range GstEncoders {
		if exec.CommandContext(ctx, gstInspect(launch), "--exists", e).Run() == nil {
			return e
		}
	}

	return "x264enc"
}
