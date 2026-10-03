package producer

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lesomnus/shale/internal/fmp4"
)

// Managed capture (§38.3): one capture process per source, ffmpeg by
// default, its standard output the source's fragmented MP4 stream,
// restarted with backoff when it exits.

// SourceConfig is one source as configured (§38.3, the three tiers).
type SourceConfig struct {
	Alias string
	Name  string
	// Input: `v4l2:/dev/video0`, `rtsp://...`, `file:/path.mp4` (tests),
	// or `push` for a stream a process writes to the producer's listener
	// (§38.1, §38.9).
	Input string
	// Kind is what a pushed stream is: `mp4` (the default), cut at
	// keyframes, or `raw`, frames cut at frame boundaries (§38.9).
	Kind string
	// ContentType is what the laminae of a raw source are, for whoever
	// reads them, e.g. `application/x-mcap`; proposed to the CP, which
	// keeps it unless a person set another (§38.9). A stream source is
	// `video/mp4`.
	ContentType string
	// Format is what the camera delivers: mjpeg | yuyv | h264 | h265.
	Format string
	Size   string
	Fps    int
	// Capture is the tool that runs the camera: ffmpeg (the default) or
	// gstreamer, for a V4L2 camera on the Raspberry Pi's encoder (§38.3).
	Capture string
	// Encoder: auto | h264_v4l2m2m | h264_vaapi | h264_qsv | h264_nvenc |
	// libx264 | copy; under gstreamer auto | v4l2h264enc | x264enc | copy.
	Encoder string
	// MaxBitrate is the declared ceiling in bits per second; 0 is auto.
	MaxBitrate int64
	// MaxBitrateAuto says the ceiling is chosen from the mode (§38.5).
	MaxBitrateAuto   bool
	KeyframeInterval time.Duration
	Audio            *AudioConfig
	// Controls are V4L2 controls set on a `v4l2:` device by name before
	// every capture start, e.g. exposure_dynamic_framerate: "0" (§38.3).
	Controls map[string]string
	// Idle, when set, skips the segments of a scene dark for longer than
	// it allows (§38.10); only a source the producer encodes can be
	// measured.
	Idle *IdleConfig
	// Tier 2: options passed through.
	EncoderOptions  map[string]string
	ExtraInputArgs  []string
	ExtraOutputArgs []string
	// Tier 3: a whole command, run with `sh -c`; its stdout is fragmented
	// MP4 (§38.1).
	Command string
	Zone    string
	// RawLoops is how many times a `raw:` input plays before the capture
	// ends, as a camera that stops would; 0 is forever. Tests only.
	RawLoops int
}

// AudioConfig is a source's audio (§38.3): a microphone beside the camera,
// and what the stream carries.
type AudioConfig struct {
	// Device is an ALSA capture device, `alsa:hw:1`, or a path under
	// /dev/snd, `alsa:/dev/snd/by-path/…`, resolved to its card at every
	// start; empty means the camera's own audio, if it sends any.
	Device string
	// Bitrate is the encoded rate in bits per second, 64 kbps by default;
	// it also sizes the Opus track for live (§38.7).
	Bitrate int64
	// Codec is one of AudioCodecs; empty is `copy` for a camera's audio and
	// `aac` for a microphone. Either gets an Opus track beside it (§38.7).
	Codec string
}

// AudioCodecs are the values of `audio.codec` (§38.3).
var AudioCodecs = map[string]string{
	"copy": "the camera's audio as it sends it, the default for a camera",
	"aac":  "encoded as AAC, the default for a microphone",
	"opus": "encoded as Opus only, one track for the archive and live both (§38.7)",
	"none": "no audio",
}

// DefaultAudioBitrate is the encoded rate when none is configured.
const DefaultAudioBitrate = 64_000

// audioBitrate is the configured rate or the default.
func (c SourceConfig) audioBitrate() int64 {
	if c.Audio != nil && c.Audio.Bitrate > 0 {
		return c.Audio.Bitrate
	}

	return DefaultAudioBitrate
}

// audioCodec is the configured codec, lower-cased, or empty.
func (c SourceConfig) audioCodec() string {
	if c.Audio == nil {
		return ""
	}

	return strings.ToLower(strings.TrimSpace(c.Audio.Codec))
}

// audioShare is what a source's audio takes of its ceiling: its track, and
// the Opus track beside it unless the archive's is Opus already (§38.7).
func (c SourceConfig) audioShare() int64 {
	if c.audioCodec() == "opus" {
		return c.audioBitrate()
	}

	return 2 * c.audioBitrate()
}

// hasMic says a microphone is configured.
func (c SourceConfig) hasMic() bool { return c.Audio != nil && c.Audio.Device != "" }

// The content types a source proposes when its configuration names none
// (§38.9).
const (
	ContentTypeMP4 = "video/mp4"
	ContentTypeRaw = "application/octet-stream"
)

// contentType is what this source's laminae are (§38.9).
func (c SourceConfig) contentType() string {
	if c.ContentType != "" {
		return c.ContentType
	}
	if c.Kind == KindRaw {
		return ContentTypeRaw
	}

	return ContentTypeMP4
}

// FragDuration bounds a fragment of a capture's stream (§38.2): the live
// tee sends whole fragments, so it is how far behind the recording a
// viewer is at most; every keyframe starts a fragment too.
const FragDuration = 500 * time.Millisecond

// MuxArgs are the muxer's arguments: fragmented MP4 that plays as it
// streams (an init segment first, fragments with offsets of their own).
func MuxArgs() []string {
	return []string{
		"-f", "mp4",
		"-movflags", "frag_keyframe+empty_moov+default_base_moof",
		"-frag_duration", strconv.FormatInt(FragDuration.Microseconds(), 10),
	}
}

// RemuxMuxArgs are the muxer's arguments in a second stage (§38.3): the
// same, with the init segment held until the first fragment. AAC out of a
// TS has its configuration only once `aac_adtstoasc` saw a packet, and an
// init segment written before that has an `esds` without it, which no
// browser decodes; `delay_moov` writes it once the configuration is known.
func RemuxMuxArgs() []string {
	return []string{
		"-f", "mp4",
		"-movflags", "frag_keyframe+empty_moov+delay_moov+default_base_moof",
		"-frag_duration", strconv.FormatInt(FragDuration.Microseconds(), 10),
	}
}

// StartingCeiling is the table of §38.5: a ceiling from the mode, with no
// measurement.
func StartingCeiling(size string, fps int, codec string) int64 {
	w, h := parseSize(size)
	if fps <= 0 {
		fps = 30
	}
	pixels := w * h
	var mbps float64
	switch {
	case pixels <= 640*480:
		mbps = 1
	case pixels <= 1280*720:
		mbps = 2
	case pixels <= 1920*1080:
		mbps = 4
		if fps <= 15 {
			mbps = 2.5
		}
	case pixels <= 2560*1440:
		mbps = 8
	default:
		mbps = 12
	}
	if fps > 30 {
		mbps *= float64(fps) / 30
	}
	if strings.Contains(strings.ToLower(codec), "265") || strings.Contains(strings.ToLower(codec), "hevc") {
		mbps *= 0.6
	}

	return int64(mbps * 1e6)
}

func parseSize(v string) (int, int) {
	parts := strings.SplitN(strings.ToLower(v), "x", 2)
	if len(parts) != 2 {
		return 1920, 1080
	}
	w, _ := strconv.Atoi(parts[0])
	h, _ := strconv.Atoi(parts[1])
	if w <= 0 || h <= 0 {
		return 1920, 1080
	}

	return w, h
}

// EncoderOrder is what `auto` tries (§38.3).
var EncoderOrder = []string{"h264_v4l2m2m", "h264_vaapi", "h264_qsv", "h264_nvenc", "libx264"}

// TsOverhead is the container's share of the ceiling (§38.3).
const TsOverhead = 1.05

// Args builds the ffmpeg command line for a source (§38.3, tier 1 and 2).
// `ceiling` is the agreed max_bitrate and `keyframe` the agreed interval.
func Args(c SourceConfig, encoder string, ceiling int64, keyframe time.Duration) []string {
	var args []string
	level := "warning"
	if c.Idle != nil {
		// blackframe reports at info level (§38.10); the lines the level
		// lets through are filtered where they are read.
		level = "info"
	}
	args = append(args, "-hide_banner", "-loglevel", level, "-nostats")
	args = append(args, c.ExtraInputArgs...)

	input := c.Input
	switch {
	case strings.HasPrefix(input, "v4l2:"):
		args = append(args, "-f", "v4l2")
		if c.Format != "" && c.Format != "h264" && c.Format != "h265" {
			args = append(args, "-input_format", c.Format)
		} else if c.Format == "h264" {
			args = append(args, "-input_format", "h264")
		}
		if c.Size != "" {
			args = append(args, "-video_size", c.Size)
		}
		if c.Fps > 0 {
			args = append(args, "-framerate", strconv.Itoa(c.Fps))
		}
		args = append(args, "-i", strings.TrimPrefix(input, "v4l2:"))
	case strings.HasPrefix(input, "rtsp://"):
		args = append(args, "-rtsp_transport", "tcp", "-timeout", "5000000", "-i", input)
	case strings.HasPrefix(input, "file:"):
		// A recording played at its own pace, for tests.
		args = append(args, "-re", "-stream_loop", "-1", "-i", strings.TrimPrefix(input, "file:"))
	case isDemo(input):
		// A picture ffmpeg draws for itself, paced to the clock so it
		// records like a camera (§38.1). New refused an unknown pattern
		// before any of this ran.
		d, _ := demoOf(input)
		args = append(args, "-re", "-f", "lavfi", "-i", d.Filter(c.Size, c.Fps))
	default:
		args = append(args, "-i", input)
	}

	// Audio (§38.3): a microphone is a second input and is encoded; a
	// camera's own audio is copied as it is unless `audio.codec` says to
	// encode or drop it; a USB camera has none. What a camera sends and MP4
	// cannot carry is caught by audioRefused once the muxer says so.
	mic := c.hasMic()
	codec := c.audioCodec()
	usb := strings.HasPrefix(input, "v4l2:")
	demo := isDemo(input)
	silent := codec == "none" || (usb && !mic)
	switch {
	case mic:
		args = append(args, "-f", "alsa", "-i", strings.TrimPrefix(c.Audio.Device, "alsa:"))
	case demo && !silent:
		// A demo source's tone is a second input, as a microphone is:
		// there is no camera stream to copy it from.
		d, _ := demoOf(input)
		args = append(args, "-f", "lavfi", "-i", fmt.Sprintf("sine=frequency=%d:sample_rate=%d", d.Tone, DemoSampleRate))
	}

	// Video. The audio's share of the ceiling is its track, and the Opus
	// track beside it unless the archive's is Opus already (§38.7).
	audioBps := int64(0)
	if !silent {
		audioBps = c.audioShare()
	}
	videoCeiling := videoCeiling(ceiling, audioBps)

	if encoder == "copy" || ((c.Format == "h264" || c.Format == "h265") && encoder == "") {
		args = append(args, "-c:v", "copy")
	} else {
		args = append(args, "-c:v", encoder)
		fps := c.Fps
		if fps <= 0 {
			fps = 30
		}
		if keyframe <= 0 {
			keyframe = 2 * time.Second
		}
		g := int(float64(fps) * keyframe.Seconds())
		args = append(args, "-g", strconv.Itoa(g), "-force_key_frames", fmt.Sprintf("expr:gte(t,n_forced*%g)", keyframe.Seconds()))
		switch encoder {
		case "h264_v4l2m2m":
			// Takes a target only: CBR at the video ceiling (§38.3, bench).
			// Its four default capture buffers run out whenever the pipe is
			// read a moment late ("All capture buffers returned to
			// userspace"), which drops frames on a Pi carrying three cameras.
			args = append(args, "-b:v", strconv.FormatInt(videoCeiling, 10), "-num_capture_buffers", "16")
		case "libx264":
			args = append(args, "-preset", "veryfast", "-crf", "23", "-maxrate", strconv.FormatInt(videoCeiling, 10), "-bufsize", strconv.FormatInt(2*videoCeiling, 10))
		default:
			args = append(args, "-maxrate", strconv.FormatInt(videoCeiling, 10), "-bufsize", strconv.FormatInt(2*videoCeiling, 10), "-b:v", strconv.FormatInt(videoCeiling*3/4, 10))
		}
		// Every encoder here takes yuv420p; a camera's MJPEG decodes to
		// yuvj422p, which h264_v4l2m2m refuses outright.
		args = append(args, "-pix_fmt", "yuv420p")
		if c.Idle != nil {
			args = append(args, "-vf", c.Idle.filter())
		}
	}
	bps := strconv.FormatInt(c.audioBitrate(), 10)
	switch {
	case silent:
		args = append(args, "-an")
	case codec == "opus":
		// Opus is what live plays, so one track does for both (§38.7).
		if mic || demo {
			args = append(args, "-map", "0:v:0", "-map", "1:a:0")
		}
		args = append(args, "-c:a", "libopus", "-b:a", bps)
	default:
		// Two tracks (§38.7): the archive's, which is the camera's audio
		// as it sends it or AAC when `codec` or a microphone says so, and
		// Opus beside it for the relay, so nothing runs while a camera is
		// watched. The audio is mapped twice; a camera without any matches
		// neither map and records silent.
		audio := "0:a:0?"
		if mic || demo {
			// The second input's sound with the first input's picture,
			// whatever else either carries; raw PCM has to be encoded.
			audio = "1:a:0"
		}
		args = append(args, "-map", "0:v:0", "-map", audio, "-map", audio)
		if codec == "aac" || mic || demo {
			args = append(args, "-c:a:0", "aac", "-b:a:0", bps)
		} else {
			// `copy`, or nothing said: the camera's audio as it sends it.
			args = append(args, "-c:a:0", "copy")
		}
		args = append(args, "-c:a:1", "libopus", "-b:a:1", bps)
	}
	for k, v := range c.EncoderOptions {
		args = append(args, "-"+k, v)
	}
	args = append(args, c.ExtraOutputArgs...)
	if Remuxed(encoder) {
		// The second stage muxes (§38.3): this one writes TS for it.
		args = append(args, "-f", "mpegts")
	} else {
		args = append(args, MuxArgs()...)
	}
	args = append(args, "-")

	return args
}

// RemuxEncoders are the encoders whose output goes through a second ffmpeg
// before it is fragmented MP4 (§38.3): they hand ffmpeg no parameter sets,
// so its mp4 muxer writes an empty `avcC` and leaves the samples in Annex
// B, and they flag every frame a keyframe. A TS in between fixes both: the
// demuxer's parser finds the parameter sets and the keyframes.
var RemuxEncoders = map[string]bool{"h264_v4l2m2m": true}

// Remuxed says whether an encoder's output goes through the second stage.
func Remuxed(encoder string) bool { return RemuxEncoders[encoder] }

// RemuxArgs is the second stage: the first's TS in, the same streams out
// as fragmented MP4. AAC out of a TS is ADTS-framed and MP4 wants it raw
// (`aac_adtstoasc`); the probe is kept short since it is what the first
// keyframe waits for.
func RemuxArgs(aac bool) []string {
	args := []string{
		"-hide_banner", "-loglevel", "warning", "-nostats",
		"-probesize", "262144", "-analyzeduration", "500000",
		"-f", "mpegts", "-i", "pipe:0",
		"-map", "0", "-c", "copy",
	}
	if aac {
		args = append(args, "-bsf:a:0", "aac_adtstoasc")
	}
	args = append(args, RemuxMuxArgs()...)

	return append(args, "pipe:1")
}

// raw is the file input of §38.1 with no capture process: the recording,
// fragmented MP4, is read at its own pace, a fragment's duration between
// fragments, looped `RawLoops` times (forever when 0), which is what tests
// use where there is no ffmpeg.
func (c *Capture) raw(ctx context.Context, read func(r io.Reader)) error {
	path := strings.TrimPrefix(c.Source.Input, "raw:")
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	pr, pw := io.Pipe()
	c.mu.Lock()
	c.up = true
	c.started = time.Now()
	c.mu.Unlock()
	go func() {
		defer pw.Close()
		next := time.Now()
		for loop := 0; c.RawLoops == 0 || loop < c.RawLoops; loop++ {
			r := fmp4.NewReader(bytes.NewReader(b))
			for {
				u, err := r.Next()
				if err != nil {
					break
				}
				if u.Frag != nil {
					if v := u.Frag.Video(r.Init()); v != nil && r.Init().Video().Timescale > 0 {
						next = next.Add(time.Duration(v.Duration()) * time.Second / time.Duration(r.Init().Video().Timescale))
					}
					if d := time.Until(next); d > 0 {
						select {
						case <-ctx.Done():
							return
						case <-time.After(d):
						}
					}
				}
				if _, err := pw.Write(u.Bytes()); err != nil {
					return
				}
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	read(pr)
	pr.Close()
	c.mu.Lock()
	c.up = false
	c.mu.Unlock()
	if ctx.Err() != nil {
		return nil
	}

	return errRawEnded
}

var errRawEnded = fmt.Errorf("recording ended")

// AvailableEncoders lists the encoders ffmpeg on this host offers.
func AvailableEncoders(ctx context.Context, ffmpeg string) ([]string, error) {
	out, err := exec.CommandContext(ctx, ffmpeg, "-hide_banner", "-encoders").Output()
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`(?m)^\s*V[A-Z.]{5}\s+(\S+)`)
	var vs []string
	for _, m := range re.FindAllStringSubmatch(string(out), -1) {
		vs = append(vs, m[1])
	}

	return vs, nil
}

// PickEncoder is `auto` (§38.3): the first of EncoderOrder ffmpeg offers
// and that can open, else libx264 with a warning about CPU.
func PickEncoder(ctx context.Context, ffmpeg string, log *slog.Logger) string {
	vs, err := AvailableEncoders(ctx, ffmpeg)
	if err != nil {
		return "libx264"
	}
	have := map[string]bool{}
	for _, v := range vs {
		have[v] = true
	}
	for _, e := range EncoderOrder {
		if !have[e] {
			continue
		}
		if e == "libx264" {
			log.Warn("no hardware encoder found; libx264 costs CPU per stream")
			return e
		}
		if encoderOpens(ctx, ffmpeg, e) {
			return e
		}
	}

	return "libx264"
}

// encoderOpens tries a one-frame encode, since an encoder ffmpeg lists is
// not always one this machine can open (a VAAPI build without a GPU).
func encoderOpens(ctx context.Context, ffmpeg, encoder string) bool {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	args := []string{"-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=black:s=320x240:r=5", "-frames:v", "3", "-c:v", encoder}
	if encoder == "h264_v4l2m2m" {
		args = append(args, "-b:v", "500k", "-pix_fmt", "yuv420p")
	}
	args = append(args, "-f", "null", "-")
	cmd := exec.CommandContext(ctx, ffmpeg, args...)

	return cmd.Run() == nil
}

// Capture runs one source's capture process and hands its output to a
// reader function until the context ends.
type Capture struct {
	Ffmpeg string
	// Gst is gst-launch, for a source captured through GStreamer.
	Gst    string
	Source SourceConfig
	Log    *slog.Logger
	// Encoder is what auto chose, once it did.
	Encoder string
	// aacEnc is the AAC encoder a GStreamer capture chose, once it did.
	aacEnc string
	// Profile answers the agreed ceiling and keyframe interval when a
	// process starts, so a new cap takes effect at the next start (§38.5).
	Profile func() (ceiling int64, keyframe time.Duration)
	// RawLoops is how many times a `raw:` input is played; 0 is forever.
	RawLoops int
	// Remux runs the second stage whatever the encoder (tests).
	Remux bool
	// OnLine sees every line the process writes to stderr first, and
	// answers true for one it consumed, which is then neither logged nor
	// kept as the last error (§38.10).
	OnLine func(line string) bool
	// OnDark is told every dark second a GStreamer capture measured, for a
	// source with `idle:` (§38.10); ffmpeg's are lines, for OnLine.
	OnDark func()

	mu       sync.Mutex
	restarts int64
	up       bool
	started  time.Time
	lastErr  string
	// audioFallback is the codec the camera's audio is encoded with from
	// the next start on, set by CheckAudio (§38.3).
	audioFallback string
	// cancel ends the running process; kicked says it was ended on purpose.
	cancel context.CancelFunc
	kicked bool
}

// audioRefused looks at what the capture says on stderr (§38.3): a
// camera's audio in a codec MP4 has no entry for (G.711 above all) makes
// the muxer refuse at start, and the capture is restarted encoding it as
// AAC, once. It answers true when the line was that refusal.
func (c *Capture) audioRefused(line string) bool {
	if !strings.Contains(line, "Could not find tag for codec") || !strings.Contains(line, "codec not currently supported in container") {
		return false
	}
	in := c.Source.Input
	if c.Source.Command != "" || strings.HasPrefix(in, "raw:") || strings.HasPrefix(in, "v4l2:") || isDemo(in) || c.Source.hasMic() {
		return false
	}
	if codec := c.Source.audioCodec(); codec != "" && codec != "copy" {
		return false
	}
	c.mu.Lock()
	if c.audioFallback != "" {
		c.mu.Unlock()
		return false
	}
	c.audioFallback = "aac"
	c.kicked = true
	c.mu.Unlock()
	c.Log.Warn("the camera's audio cannot be stored as it is (MP4 has no entry for it); encoding it as AAC from now on, or set audio.codec", "source", c.Source.Alias, "said", line)

	return true
}

// AudioFallback is the codec CheckAudio settled on, or empty.
func (c *Capture) AudioFallback() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.audioFallback
}

// Restarts is how many times the process was restarted.
func (c *Capture) Restarts() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.restarts
}

// Up says whether the process is running now.
func (c *Capture) Up() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.up
}

// LastError is what the process said last on stderr.
func (c *Capture) LastError() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.lastErr
}

// Run starts the process, feeds `read` its output, and restarts it with
// backoff when it exits. `read` returns when the stream ends.
func (c *Capture) Run(ctx context.Context, read func(r io.Reader)) error {
	backoff := time.Second
	for {
		began := time.Now()
		err := c.once(ctx, read)
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, errKicked) {
			// Ended on purpose, to start again with other arguments.
			continue
		}
		if time.Since(began) > healthyRun {
			// A run that lasted is not part of a crash loop: the next
			// restart is quick again.
			backoff = time.Second
		}
		if err != nil {
			c.mu.Lock()
			c.restarts++
			c.lastErr = err.Error()
			c.mu.Unlock()
			c.Log.Warn("capture exited", "source", c.Source.Alias, "err", err.Error(), "restart_in", backoff.String())
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// applyControls sets the source's V4L2 controls on its device before a
// start (§38.3): a camera forgets them when re-plugged, and a restart is
// when that shows.
func (c *Capture) applyControls() {
	dev, ok := strings.CutPrefix(c.Source.Input, "v4l2:")
	if !ok || len(c.Source.Controls) == 0 {
		return
	}
	set, errs := setControls(dev, c.Source.Controls)
	if len(set) > 0 {
		c.Log.Info("controls", "source", c.Source.Alias, "set", strings.Join(set, " "))
	}
	for _, err := range errs {
		c.Log.Warn("control not set", "source", c.Source.Alias, "err", err.Error())
	}
}

// errKicked is a process ended, or that ended, for the audio to be
// encoded (audioRefused): started again at once.
var errKicked = errors.New("capture restarted with the audio encoded")

// stopGrace is how long a capture process has, once interrupted, to end
// on its own before its process group is killed (§38.3).
const stopGrace = 5 * time.Second

// gst is gst-launch: as configured, else on PATH.
func (c *Capture) gst() string {
	if c.Gst != "" {
		return c.Gst
	}

	return "gst-launch-1.0"
}

// healthyRun is how long a capture has to run for its exit to count as an
// ordinary stop rather than the next turn of a crash loop.
const healthyRun = time.Minute

func (c *Capture) once(ctx context.Context, read func(r io.Reader)) error {
	if strings.HasPrefix(c.Source.Input, "raw:") {
		return c.raw(ctx, read)
	}
	ceiling, keyframe := c.Profile()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.mu.Lock()
	c.cancel, c.kicked = cancel, false
	fallback := c.audioFallback
	c.mu.Unlock()
	src := c.Source
	if src.hasMic() {
		dev, err := alsaDevice(src.Audio.Device)
		if err != nil {
			return err
		}
		a := *src.Audio
		a.Device = "alsa:" + dev
		src.Audio = &a
	}
	if fallback != "" {
		a := AudioConfig{}
		if src.Audio != nil {
			a = *src.Audio
		}
		a.Codec = fallback
		src.Audio = &a
	}
	var cmd *exec.Cmd
	// darkPipe, when set, starts reading a GStreamer capture's measured
	// pictures once the process holds its end of the pipe.
	var darkPipe func()
	remux := c.Remux
	// remuxArgs is the second stage's command line, when it differs from
	// the one RemuxArgs makes for ffmpeg's TS.
	var remuxArgs []string
	if c.Source.Command != "" {
		cmd = exec.CommandContext(ctx, "/bin/sh", "-c", c.Source.Command)
	} else if c.Source.gstreamer() {
		encoder := c.Source.Encoder
		if encoder == "" || encoder == "auto" {
			if c.Source.Format == "h264" {
				encoder = "copy"
			} else {
				if c.Encoder == "" {
					c.Encoder = PickGstEncoder(ctx, c.gst())
					c.Log.Info("encoder", "source", c.Source.Alias, "chosen", c.Encoder)
				}
				encoder = c.Encoder
			}
		}
		aac, opus := src.gstAudio()
		if aac && c.aacEnc == "" {
			c.aacEnc = PickGstAAC(ctx, c.gst())
			c.Log.Info("audio encoder", "source", c.Source.Alias, "chosen", c.aacEnc)
		}
		// The pipeline writes TS, which the second stage makes fragmented
		// MP4, as for h264_v4l2m2m (§38.3).
		remux = true
		remuxArgs = GstRemuxArgs(aac, aac || opus)
		args := GstArgs(src, encoder, ceiling, keyframe, c.aacEnc)
		c.Log.Info("capture", "source", c.Source.Alias, "cmd", c.gst()+" "+strings.Join(args, " ")+" | "+c.Ffmpeg+" "+strings.Join(remuxArgs, " "))
		cmd = exec.CommandContext(ctx, c.gst(), args...)
		if src.Idle != nil && encoder != "copy" {
			// The measured pictures come on fd 3 (§38.10).
			r, w, err := os.Pipe()
			if err != nil {
				return err
			}
			cmd.ExtraFiles = []*os.File{w}
			defer r.Close()
			dw, dh := darkSize(src.Size)
			darkPipe = func() {
				w.Close()
				go readDark(r, dw, dh, src.Idle.threshold(), func() {
					if c.OnDark != nil {
						c.OnDark()
					}
				})
			}
			defer w.Close()
		}
	} else {
		encoder := c.Source.Encoder
		if encoder == "" || encoder == "auto" {
			if c.Source.Format == "h264" || c.Source.Format == "h265" {
				encoder = "copy"
			} else {
				if c.Encoder == "" {
					c.Encoder = PickEncoder(ctx, c.Ffmpeg, c.Log)
					c.Log.Info("encoder", "source", c.Source.Alias, "chosen", c.Encoder)
				}
				encoder = c.Encoder
			}
		}
		if c.Remux && !Remuxed(encoder) {
			// Forced: the first stage writes TS as a remuxed encoder's does.
			RemuxEncoders[encoder] = true
			defer delete(RemuxEncoders, encoder)
		}
		remux = Remuxed(encoder)
		args := Args(src, encoder, ceiling, keyframe)
		cmdline := c.Ffmpeg + " " + strings.Join(args, " ")
		if remux {
			cmdline += " | " + c.Ffmpeg + " " + strings.Join(RemuxArgs(src.aacArchive()), " ")
		}
		c.Log.Info("capture", "source", c.Source.Alias, "cmd", cmdline)
		cmd = exec.CommandContext(ctx, c.Ffmpeg, args...)
	}
	cmd.Env = append(os.Environ(), "AV_LOG_FORCE_NOCOLOR=1")
	stopped := stopGracefully(cmd, true, stopGrace)
	defer stopped()
	c.applyControls()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	// The second stage (§38.3), when the encoder needs one: the first's
	// TS in, fragmented MP4 out.
	var second *exec.Cmd
	if remux {
		if remuxArgs == nil {
			remuxArgs = RemuxArgs(src.aacArchive())
		}
		second = exec.CommandContext(ctx, c.Ffmpeg, remuxArgs...)
		second.Env = cmd.Env
		// It ends when the first stage does, having written what that
		// flushed on its way out.
		secondStopped := stopGracefully(second, false, stopGrace)
		defer secondStopped()
		second.Stdin = stdout
		out, err := second.StdoutPipe()
		if err != nil {
			return err
		}
		errs, err := second.StderrPipe()
		if err != nil {
			return err
		}
		stdout = out
		go c.scanStderr(errs)
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	if darkPipe != nil {
		darkPipe()
	}
	if second != nil {
		if err := second.Start(); err != nil {
			cancel()
			cmd.Wait()

			return err
		}
	}
	c.mu.Lock()
	c.up = true
	c.started = time.Now()
	c.mu.Unlock()
	go c.scanStderr(stderr)

	read(stdout)
	err = cmd.Wait()
	if second != nil {
		if err2 := second.Wait(); err == nil {
			err = err2
		}
	}
	c.mu.Lock()
	c.up = false
	c.cancel = nil
	kicked := c.kicked
	c.mu.Unlock()
	if kicked {
		return errKicked
	}
	if err != nil {
		return err
	}

	return fmt.Errorf("capture ended")
}

// scanStderr reads what a stage says on stderr: the dark detector's lines,
// the muxer's refusal of the audio, and the rest as the last error.
func (c *Capture) scanStderr(stderr io.Reader) {
	sc := bufio.NewScanner(stderr)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if c.OnLine != nil && c.OnLine(line) {
			continue
		}
		if c.audioRefused(line) {
			continue
		}
		if c.Source.Idle != nil && !c.Source.gstreamer() && !strings.HasPrefix(line, "[") {
			// At info level ffmpeg also describes its inputs and
			// outputs at the start; what a component says is tagged.
			// GStreamer measures elsewhere and says only what is wrong.
			continue
		}
		c.mu.Lock()
		c.lastErr = line
		c.mu.Unlock()
		c.Log.Info("ffmpeg", "source", c.Source.Alias, "line", line)
	}
}

// aacArchive says the archive's audio track is AAC: what a camera's copied
// audio is taken to be, or what `codec: aac` and a microphone make.
func (c SourceConfig) aacArchive() bool {
	codec := c.audioCodec()
	usb := strings.HasPrefix(c.Input, "v4l2:")
	silent := codec == "none" || (usb && !c.hasMic())

	return !silent && codec != "opus"
}

// LiveArgs is the ffmpeg command of the live helper (§38.7): a source's
// fragmented MP4 in, the same video and its audio as Opus out, in
// fragments of its own, so a viewer is a fragment behind the recording.
// The probe is kept short, since it is what a viewer waits for.
func LiveArgs(bitrate int64) []string {
	if bitrate <= 0 {
		bitrate = DefaultAudioBitrate
	}
	args := []string{
		"-hide_banner", "-loglevel", "warning", "-nostats",
		"-probesize", "262144", "-analyzeduration", "500000",
		"-f", "mp4", "-i", "pipe:0",
		"-map", "0:v:0", "-map", "0:a:0?",
		"-c:v", "copy", "-c:a", "libopus", "-b:a", strconv.FormatInt(bitrate, 10),
		"-flush_packets", "1",
	}
	args = append(args, MuxArgs()...)

	return append(args, "pipe:1")
}
