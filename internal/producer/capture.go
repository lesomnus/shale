package producer

import (
	"bufio"
	"bytes"
	"context"
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
	// `tcp://:port` or `unix:/path` for a push source (§38.1).
	Input string
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
}

// AudioConfig is a source's audio (§38.3).
type AudioConfig struct {
	Device  string
	Bitrate int64
	Codec   string
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

	if c.Audio != nil && c.Audio.Device != "" {
		args = append(args, "-f", "alsa", "-i", strings.TrimPrefix(c.Audio.Device, "alsa:"))
	}

	// Video.
	audioBps := int64(0)
	if c.Audio != nil {
		audioBps = c.Audio.Bitrate
		if audioBps == 0 {
			audioBps = 64_000
		}
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
		if c.Audio == nil || c.Audio.Device == "" {
			args = append(args, "-an")
		}
		// Every encoder here takes yuv420p; a camera's MJPEG decodes to
		// yuvj422p, which h264_v4l2m2m refuses outright.
		args = append(args, "-pix_fmt", "yuv420p")
	}
	if c.Audio != nil && c.Audio.Device != "" {
		codec := c.Audio.Codec
		if codec == "" {
			codec = "aac"
		}
		if codec == "opus" {
			codec = "libopus"
		}
		args = append(args, "-c:a", codec, "-b:a", strconv.FormatInt(audioBps, 10))
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
		if err := c.once(ctx, read); err != nil && ctx.Err() == nil {
			c.mu.Lock()
			c.restarts++
			c.lastErr = err.Error()
			c.mu.Unlock()
			c.Log.Warn("capture exited", "source", c.Source.Alias, "err", err.Error(), "restart_in", backoff.String())
		}
		if ctx.Err() != nil {
			return nil
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

func (c *Capture) once(ctx context.Context, read func(r io.Reader)) error {
	if strings.HasPrefix(c.Source.Input, "raw:") {
		return c.raw(ctx, read)
	}
	ceiling, keyframe := c.Profile()
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
		args := Args(c.Source, encoder, ceiling, keyframe)
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
	c.mu.Unlock()
	if err != nil {
		return err
	}

	return fmt.Errorf("capture ended")
}
