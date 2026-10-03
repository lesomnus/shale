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
// already turns it into fragmented MP4 (GstRemuxArgs), so what the producer
// reads is the same as from any capture. A microphone (`audio.device`) goes
// into the same TS as AAC with Opus beside it, or as Opus alone, as ffmpeg
// records one (§38.7).

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

// checkGst is what a GStreamer source may say (§38.3): a V4L2 camera, a
// microphone beside it or none, `idle:`, the tier-1 fields; what only
// ffmpeg understands is refused rather than ignored.
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
	case c.Audio != nil && !c.hasMic() && c.audioCodec() != "" && c.audioCodec() != "none":
		return fmt.Errorf("capture: %s records a camera's microphone by its device (audio.device: alsa:…); a V4L2 camera sends no audio of its own", CaptureGStreamer)
	case c.hasMic() && c.audioCodec() != "" && c.audioCodec() != "aac" && c.audioCodec() != "opus" && c.audioCodec() != "none":
		return fmt.Errorf("capture: %s encodes a microphone as aac or opus, not %q", CaptureGStreamer, c.audioCodec())
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

// The TS's PIDs, fixed so the streams come out of the second stage in one
// order: the video, then the archive's audio, then the Opus beside it.
const (
	gstPidVideo = 65
	gstPidAudio = 66
	gstPidOpus  = 67
)

// gstAudio says what a GStreamer source records of its microphone: nothing,
// AAC with Opus beside it, or Opus alone (§38.7).
func (c SourceConfig) gstAudio() (aac, opus bool) {
	if !c.hasMic() || c.audioCodec() == "none" {
		return false, false
	}
	if c.audioCodec() == "opus" {
		return false, true
	}

	return true, true
}

// GstArgs is the gst-launch command line for a source (§38.3): the camera,
// decoded and encoded to H.264 unless it sends H.264 itself, parsed so every
// keyframe carries its parameter sets, and its microphone when it has one,
// as MPEG-TS on standard output for the second stage. `ceiling` is the
// agreed max_bitrate and `keyframe` the agreed interval; `aacEnc` is the
// AAC encoder this host has. `-e` makes an interrupt end the stream rather
// than the process, which is how the capture is stopped (gracefully).
func GstArgs(c SourceConfig, encoder string, ceiling int64, keyframe time.Duration, aacEnc string) []string {
	w, h := parseSize(c.Size)
	if c.Size == "" {
		w, h = 1280, 720
	}
	fps := c.Fps
	if fps <= 0 {
		fps = 30
	}
	mode := fmt.Sprintf("width=%d,height=%d,framerate=%d/1", w, h, fps)
	args := []string{"-q", "-e", "mpegtsmux", "name=mux", "!", "fdsink", "fd=1"}
	args = append(args, "v4l2src", "device="+strings.TrimPrefix(c.Input, "v4l2:"), "!")
	format := strings.ToLower(c.Format)
	if format == "h264" {
		args = append(args, "video/x-h264,"+mode, "!")
		if encoder == "copy" || encoder == "" {
			return append(append(args, gstVideoTail()...), gstAudioArgs(c, aacEnc)...)
		}
		args = append(args, "h264parse", "!", "avdec_h264", "!")
	} else if format == "yuyv" {
		args = append(args, "video/x-raw,format=YUY2,"+mode, "!")
		args = append(args, gstDarkTee(c)...)
	} else {
		// MJPEG, what a USB camera sends at 720p and above. Decoded in
		// software: the Pi's JPEG decoder takes two streams at most, and a
		// third leaves it wedged until a reboot.
		args = append(args, "image/jpeg,"+mode, "!")
		args = append(args, gstDarkTee(c)...)
		args = append(args, "jpegdec", "!")
	}
	args = append(args, "videoconvert", "!", "video/x-raw,format=I420", "!")

	if keyframe <= 0 {
		keyframe = 2 * time.Second
	}
	gop := int(float64(fps) * keyframe.Seconds())
	audioBps := int64(0)
	if aac, opus := c.gstAudio(); aac || opus {
		audioBps = c.audioShare()
	}
	target := int64(float64(videoCeiling(ceiling, audioBps)) * gstVBRShare)
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

	args = append(append(args, gstVideoTail()...), gstAudioArgs(c, aacEnc)...)

	return append(args, gstDarkArgs(c)...)
}

// gstDarkTee splits what the camera sends, for a source with `idle:`
// (§38.10): the recording goes on through a queue that holds two of the
// camera's buffers at most, and gstDarkArgs takes the other branch.
//
// The split is before anything is decoded, so the encoder takes its
// pictures from the decoder exactly as it does without `idle:`. A tee in
// front of the Pi's v4l2h264enc, after the decoder, first made gst-launch
// abort asking for 4 GiB (the encoder's allocation query answered through
// the tee), and with that query dropped the encoder recorded rows of other
// pictures, shifted sideways, and lost frames in bursts.
func gstDarkTee(c SourceConfig) []string {
	if c.Idle == nil {
		return nil
	}

	return []string{"tee", "name=pic", "pic.", "!", "queue", "max-size-buffers=2", "!"}
}

// gstDarkArgs is the measuring branch of a source with `idle:` (§38.10):
// what the camera sends, one picture a second, decoded when it is MJPEG,
// grey and darkWidth wide, to fd 3, which the producer reads (readDark).
// Its queue drops rather than holds the recording up should the reader fall
// behind, and the rate is cut before the decoder, so one JPEG a second is
// decoded.
func gstDarkArgs(c SourceConfig) []string {
	if c.Idle == nil || strings.ToLower(c.Format) == "h264" {
		// A camera's own H.264 is not measured: idle is refused at start.
		return nil
	}
	w, h := darkSize(c.Size)
	args := []string{"pic.", "!", "queue", "leaky=downstream", "max-size-buffers=1", "!", "videorate", "drop-only=true", "!"}
	if strings.ToLower(c.Format) == "yuyv" {
		args = append(args, "video/x-raw,framerate=1/1", "!")
	} else {
		args = append(args, "image/jpeg,framerate=1/1", "!", "jpegdec", "!")
	}

	return append(args, "videoscale", "!", "videoconvert", "!", fmt.Sprintf("video/x-raw,format=GRAY8,width=%d,height=%d", w, h), "!",
		"fdsink", "fd=3")
}

// gstVideoTail parses the H.264 so every keyframe carries its parameter
// sets, into the muxer at the video's PID.
func gstVideoTail() []string {
	return []string{"h264parse", "config-interval=-1", "!", "queue", "!", "mux.sink_" + strconv.Itoa(gstPidVideo)}
}

// gstAudioArgs is the microphone's branch (§38.3, §38.7): ALSA at 48 kHz
// mono, encoded as AAC for the archive with Opus beside it for live, or as
// Opus alone, each into the muxer at its PID; nothing when there is no
// microphone.
func gstAudioArgs(c SourceConfig, aacEnc string) []string {
	aac, opus := c.gstAudio()
	if !aac && !opus {
		return nil
	}
	dev := strings.TrimPrefix(c.Audio.Device, "alsa:")
	bps := strconv.FormatInt(c.audioBitrate(), 10)
	args := []string{"alsasrc", "device=" + dev, "!", "queue", "!", "audioconvert", "!", "audioresample", "!", "audio/x-raw,rate=48000,channels=1", "!"}
	opusArgs := []string{"opusenc", "bitrate=" + bps, "!", "queue", "!", "mux.sink_" + strconv.Itoa(gstPidOpus)}
	if !aac {
		return append(args, opusArgs...)
	}
	args = append(args, "tee", "name=mic",
		"mic.", "!", "queue", "!", aacEnc, "bitrate="+bps, "!", "aacparse", "!", "queue", "!", "mux.sink_"+strconv.Itoa(gstPidAudio),
		"mic.", "!", "queue", "!")

	return append(args, opusArgs...)
}

// GstRemuxArgs is the second stage of a GStreamer capture (§38.3): the
// pipeline's TS in, the video first and then its audio out as fragmented
// MP4. With a microphone the probe is longer, since the sound starts before
// the camera's first frame and a short probe ends with no picture in it.
func GstRemuxArgs(aac, audio bool) []string {
	probe := []string{"-probesize", "262144", "-analyzeduration", "500000"}
	if audio {
		probe = []string{"-probesize", "5000000", "-analyzeduration", "3000000"}
	}
	args := []string{"-hide_banner", "-loglevel", "warning", "-nostats"}
	args = append(args, probe...)
	args = append(args, "-f", "mpegts", "-i", "pipe:0", "-map", "0:v:0", "-map", "0:a?", "-c", "copy")
	if aac {
		args = append(args, "-bsf:a:0", "aac_adtstoasc")
	}
	args = append(args, RemuxMuxArgs()...)

	return append(args, "pipe:1")
}

// GstAACEncoders are the AAC encoders a GStreamer capture takes, the first
// this host has: libav's, Fraunhofer's, then VisualOn's, which the
// distribution's good plugins carry.
var GstAACEncoders = []string{"avenc_aac", "fdkaacenc", "voaacenc"}

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
	return gstFirst(ctx, launch, GstEncoders)
}

// PickGstAAC is the AAC encoder of GstAACEncoders this host has, else
// voaacenc, which fails at start with a message saying what is missing.
func PickGstAAC(ctx context.Context, launch string) string {
	return gstFirst(ctx, launch, GstAACEncoders)
}

// gstFirst is the first element of a list this host's GStreamer has, else
// the last.
func gstFirst(ctx context.Context, launch string, elements []string) string {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for _, e := range elements {
		if exec.CommandContext(ctx, gstInspect(launch), "--exists", e).Run() == nil {
			return e
		}
	}

	return elements[len(elements)-1]
}
