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
)

// Managed capture (§38.3): one capture process per source, ffmpeg by
// default, its standard output the source's TS stream, restarted with
// backoff when it exits.

// SourceConfig is one source as configured (§38.3, the three tiers).
type SourceConfig struct {
	Alias string
	Name  string
	// Input: `v4l2:/dev/video0`, `rtsp://...`, `file:/path.ts` (tests),
	// or `push` for a stream a process writes to the producer's listener
	// (§38.1, §38.9).
	Input string
	// Kind is what a pushed stream is: `ts` (the default) or `raw`,
	// frames cut at frame boundaries (§38.9).
	Kind string
	// ContentType is what the laminae of a raw source are, for whoever
	// reads them, e.g. `application/x-mcap`; proposed to the CP, which
	// keeps it unless a person set another (§38.9). A TS source is
	// `video/mp2t`.
	ContentType string
	// Format is what the camera delivers: mjpeg | yuyv | h264 | h265.
	Format string
	Size   string
	Fps    int
	// Encoder: auto | h264_v4l2m2m | h264_vaapi | h264_qsv | h264_nvenc |
	// libx264 | copy.
	Encoder string
	// MaxBitrate is the declared ceiling in bits per second; 0 is auto.
	MaxBitrate int64
	// MaxBitrateAuto says the ceiling is chosen from the mode (§38.5).
	MaxBitrateAuto   bool
	KeyframeInterval time.Duration
	Audio            *AudioConfig
	// Tier 2: options passed through.
	EncoderOptions  map[string]string
	ExtraInputArgs  []string
	ExtraOutputArgs []string
	// Tier 3: a whole command, run with `sh -c`; its stdout is TS.
	Command string
	Zone    string
	// RawLoops is how many times a `raw:` input plays before the capture
	// ends, as a camera that stops would; 0 is forever. Tests only.
	RawLoops int
}

// AudioConfig is a source's audio (§38.3): a microphone beside the camera,
// and what the TS carries.
type AudioConfig struct {
	// Device is an ALSA capture device, `alsa:hw:1`; empty means the
	// camera's own audio, if it sends any.
	Device string
	// Bitrate is the encoded rate in bits per second, 64 kbps by default;
	// it also sizes the live helper's Opus (§38.7).
	Bitrate int64
	// Codec is one of AudioCodecs; empty is `copy` for a camera's audio and
	// `aac` for a microphone.
	Codec string
}

// AudioCodecs are the values of `audio.codec` (§38.3).
var AudioCodecs = map[string]string{
	"copy": "the camera's audio as it sends it, the default for a camera",
	"aac":  "encoded as AAC, the default for a microphone",
	"opus": "encoded as Opus, which the relay passes through as it is (§39.4)",
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

// hasMic says a microphone is configured.
func (c SourceConfig) hasMic() bool { return c.Audio != nil && c.Audio.Device != "" }

// The content types a source proposes when its configuration names none
// (§38.9).
const (
	ContentTypeTS  = "video/mp2t"
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

	return ContentTypeTS
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
	args = append(args, "-hide_banner", "-loglevel", "warning", "-nostats")
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
	default:
		args = append(args, "-i", input)
	}

	// Audio (§38.3): a microphone is a second input and is encoded; a
	// camera's own audio is copied as it is unless `audio.codec` says to
	// encode or drop it; a USB camera has none. What a camera sends and TS
	// cannot carry is caught by CheckAudio once the stream shows it.
	mic := c.hasMic()
	codec := c.audioCodec()
	usb := strings.HasPrefix(input, "v4l2:")
	silent := codec == "none" || (usb && !mic)
	if mic {
		args = append(args, "-f", "alsa", "-i", strings.TrimPrefix(c.Audio.Device, "alsa:"))
	}

	// Video.
	audioBps := int64(0)
	if !silent {
		audioBps = c.audioBitrate()
	}
	videoCeiling := int64(float64(ceiling-audioBps) / TsOverhead)
	if videoCeiling < 100_000 {
		videoCeiling = 100_000
	}

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
			args = append(args, "-b:v", strconv.FormatInt(videoCeiling, 10))
		case "libx264":
			args = append(args, "-preset", "veryfast", "-crf", "23", "-maxrate", strconv.FormatInt(videoCeiling, 10), "-bufsize", strconv.FormatInt(2*videoCeiling, 10))
		default:
			args = append(args, "-maxrate", strconv.FormatInt(videoCeiling, 10), "-bufsize", strconv.FormatInt(2*videoCeiling, 10), "-b:v", strconv.FormatInt(videoCeiling*3/4, 10))
		}
		// Every encoder here takes yuv420p; a camera's MJPEG decodes to
		// yuvj422p, which h264_v4l2m2m refuses outright.
		args = append(args, "-pix_fmt", "yuv420p")
	}
	switch {
	case silent:
		args = append(args, "-an")
	case mic:
		// The microphone's sound with the camera's picture, whatever else
		// either carries; raw PCM has to be encoded.
		enc := "aac"
		if codec == "opus" {
			enc = "libopus"
		}
		args = append(args, "-map", "0:v:0", "-map", "1:a:0", "-c:a", enc, "-b:a", strconv.FormatInt(audioBps, 10))
	case codec == "aac":
		args = append(args, "-c:a", "aac", "-b:a", strconv.FormatInt(audioBps, 10))
	case codec == "opus":
		args = append(args, "-c:a", "libopus", "-b:a", strconv.FormatInt(audioBps, 10))
	default:
		// `copy`, or nothing said: the camera's audio as it sends it.
		args = append(args, "-c:a", "copy")
	}
	for k, v := range c.EncoderOptions {
		args = append(args, "-"+k, v)
	}
	args = append(args, c.ExtraOutputArgs...)
	args = append(args, "-f", "mpegts", "-")

	return args
}

// raw is the file input of §38.1 with no capture process: the recording is
// read at its own frame rate, looped `RawLoops` times (forever when 0),
// which is what tests use where there is no ffmpeg.
func (c *Capture) raw(ctx context.Context, read func(r io.Reader)) error {
	path := strings.TrimPrefix(c.Source.Input, "raw:")
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	fps := c.Source.Fps
	if fps <= 0 {
		fps = 30
	}
	pr, pw := io.Pipe()
	c.mu.Lock()
	c.up = true
	c.started = time.Now()
	c.mu.Unlock()
	go func() {
		defer pw.Close()
		r := NewReader(bytes.NewReader(b))
		var p Packet
		frame := time.Second / time.Duration(fps)
		next := time.Now()
		for loop := 0; c.RawLoops == 0 || loop < c.RawLoops; loop++ {
			r = NewReader(bytes.NewReader(b))
			for {
				if err := r.Next(&p); err != nil {
					break
				}
				if r.IsVideoFrame(&p) {
					next = next.Add(frame)
					if d := time.Until(next); d > 0 {
						select {
						case <-ctx.Done():
							return
						case <-time.After(d):
						}
					}
				}
				if _, err := pw.Write(p.Data[:]); err != nil {
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
	Source SourceConfig
	Log    *slog.Logger
	// Encoder is what auto chose, once it did.
	Encoder string
	// Profile answers the agreed ceiling and keyframe interval when a
	// process starts, so a new cap takes effect at the next start (§38.5).
	Profile func() (ceiling int64, keyframe time.Duration)
	// RawLoops is how many times a `raw:` input is played; 0 is forever.
	RawLoops int

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

// CheckAudio looks at what the tables say a capture produces (§38.3): a
// camera's audio copied into a private stream nothing names is audio no
// player will find, so the capture is restarted encoding it as AAC, once.
// It answers true when it restarted; the caller stops reading the stream.
func (c *Capture) CheckAudio(st Streams) bool {
	in := c.Source.Input
	if !st.AudioAnon || c.Source.Command != "" || strings.HasPrefix(in, "raw:") || strings.HasPrefix(in, "v4l2:") || c.Source.hasMic() {
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
	cancel := c.cancel
	c.mu.Unlock()
	c.Log.Warn("the camera's audio cannot be stored as it is (TS has no type for it); encoding it as AAC from now on, or set audio.codec", "source", c.Source.Alias)
	if cancel != nil {
		cancel()
	}

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

// errKicked is a process ended by CheckAudio, to be started again at once.
var errKicked = errors.New("capture restarted with the audio encoded")

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
	if fallback != "" {
		a := AudioConfig{}
		if src.Audio != nil {
			a = *src.Audio
		}
		a.Codec = fallback
		src.Audio = &a
	}
	var cmd *exec.Cmd
	if c.Source.Command != "" {
		cmd = exec.CommandContext(ctx, "/bin/sh", "-c", c.Source.Command)
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
		args := Args(src, encoder, ceiling, keyframe)
		c.Log.Info("capture", "source", c.Source.Alias, "cmd", c.Ffmpeg+" "+strings.Join(args, " "))
		cmd = exec.CommandContext(ctx, c.Ffmpeg, args...)
	}
	cmd.Env = append(os.Environ(), "AV_LOG_FORCE_NOCOLOR=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	c.mu.Lock()
	c.up = true
	c.started = time.Now()
	c.mu.Unlock()

	go func() {
		sc := bufio.NewScanner(stderr)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			c.mu.Lock()
			c.lastErr = line
			c.mu.Unlock()
			c.Log.Info("ffmpeg", "source", c.Source.Alias, "line", line)
		}
	}()

	read(stdout)
	err = cmd.Wait()
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

// LiveArgs is the ffmpeg command of the live helper (§38.7): a source's TS
// in, the same video and its audio as Opus out, flushed packet by packet
// so a viewer is a few frames behind the recording. The probe is kept
// short, since it is what a viewer waits for; `nobuffer` would shorten it
// further but discards the packets it probed, the keyframe the tee began
// with among them.
func LiveArgs(bitrate int64) []string {
	if bitrate <= 0 {
		bitrate = DefaultAudioBitrate
	}

	return []string{
		"-hide_banner", "-loglevel", "warning", "-nostats",
		"-probesize", "262144", "-analyzeduration", "500000",
		"-f", "mpegts", "-i", "pipe:0",
		"-map", "0:v:0", "-map", "0:a:0?",
		"-c:v", "copy", "-c:a", "libopus", "-b:a", strconv.FormatInt(bitrate, 10),
		"-muxdelay", "0", "-muxpreload", "0", "-flush_packets", "1",
		"-f", "mpegts", "pipe:1",
	}
}
