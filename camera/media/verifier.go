// Package media qualifies a camera stream independently of Source registration.
// Only normalized facts leave a probe; pictures and media packets are discarded.
package media

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/lesomnus/shale/camera"
	"github.com/lesomnus/shale/camera/credential"
)

var (
	ErrIncomplete = errors.New("media qualification ended before a complete observation")
	ErrMismatch   = errors.New("media does not match requested profile")
	ErrDecode     = errors.New("media could not be independently decoded")
)

// Verifier runs bounded ffprobe and, when requested, an independent ffmpeg
// decoder on the worker. NetworkScope must match the assigned Producer network.
// AllowFile is only for controlled synthetic fixtures; normal requests use RTSP.
type Verifier struct {
	AuthProvider    camera.AuthProvider
	Ffmpeg, Ffprobe string
	NetworkScope    string
	MaxDuration     time.Duration
	StartupTimeout  time.Duration
	AllowFile       bool
}

func (v *Verifier) Verify(ctx context.Context, request camera.MediaRequest) (camera.VerifiedState, error) {
	var result camera.VerifiedState
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if request.NetworkScope == "" || request.NetworkScope != v.NetworkScope || request.Scope.NetworkID != request.NetworkScope {
		return result, camera.ErrForbidden
	}
	d := request.Duration
	if d <= 0 {
		d = 5 * time.Second
	}
	maxd := v.MaxDuration
	if maxd <= 0 {
		maxd = 30 * time.Second
	}
	if d > maxd || d < 100*time.Millisecond {
		return result, camera.ErrInvalid
	}
	raw := request.Profile.StreamURI
	u, err := url.Parse(raw)
	if err != nil {
		return result, camera.ErrInvalid
	}
	isFile := u.Scheme == "file" && v.AllowFile
	if !isFile && (u.Host == "" || (u.Scheme != "rtsp" && u.Scheme != "rtsps")) {
		return result, camera.ErrInvalid
	}
	if request.CredentialRef != nil && u.User != nil {
		return result, camera.ErrInvalid
	}
	// Work on a copy. Never return an input credential even for legacy URLs.
	request.Profile.StreamURI = camera.SanitizeURI(raw)
	result = camera.VerifiedState{Profile: request.Profile, ObservedAt: time.Now().UTC(), Firmware: request.Device.Firmware, ProfileRevision: request.Desired.Revision}
	operation := func(input string, sanitize credential.Sanitizer) error {
		if isFile {
			input = u.Path
		}
		observed, err := v.observe(ctx, input, d, sanitize, isFile)
		if err != nil {
			return err
		}
		result.Profile.Codec = observed.codec
		result.Profile.Width = observed.width
		result.Profile.Height = observed.height
		result.Profile.FPS = observed.ptsFPS
		result.Profile.Bitrate = observed.ptsBitrate
		result.Profile.GOP = observed.gop
		result.AudioCodec = observed.audio
		result.ArrivalFPS = observed.arrivalFPS
		result.ArrivalBitrate = observed.arrivalBitrate
		result.Duration = observed.duration
		result.MaxGap = observed.maxGap
		result.Complete = observed.complete
		result.Evidence = []camera.Evidence{{Source: "ffprobe:actual-packets", At: result.ObservedAt, Firmware: result.Firmware, Authenticated: request.CredentialRef != nil || u.User != nil}}
		if !observed.complete {
			return ErrIncomplete
		}
		if !matches(result.Profile, request.Desired, request.Profile) {
			return ErrMismatch
		}
		if request.RequireDecode {
			frames, decodeErr := v.decode(ctx, input, d, sanitize, isFile)
			if decodeErr != nil {
				return decodeErr
			}
			result.Decoded = true
			result.DecodedFrames = frames
			result.Evidence = append(result.Evidence, camera.Evidence{Source: "ffmpeg:independent-decode", At: time.Now().UTC(), Firmware: result.Firmware, Authenticated: request.CredentialRef != nil || u.User != nil})
		}
		return nil
	}
	if request.CredentialRef != nil {
		err = credential.WithInput(ctx, v.AuthProvider, request.Scope, request.Device, request.CredentialRef, raw, operation)
	} else {
		var values []string
		if u.User != nil {
			values = append(values, u.User.Username())
			if p, ok := u.User.Password(); ok {
				values = append(values, p)
			}
		}
		err = operation(raw, credential.NewSanitizer(values...))
	}
	return result, err
}

func canonicalCodec(s string) string {
	switch strings.ToLower(s) {
	case "h265", "hevc":
		return "hevc"
	case "h264", "avc":
		return "h264"
	}
	return strings.ToLower(s)
}
func matches(got camera.MediaProfile, desired camera.DesiredState, reported camera.MediaProfile) bool {
	codec := desired.Codec
	if codec == "" {
		codec = reported.Codec
	}
	width, height := desired.Width, desired.Height
	if width == 0 {
		width = reported.Width
	}
	if height == 0 {
		height = reported.Height
	}
	fps := desired.FPS
	if fps == 0 {
		fps = reported.FPS
	}
	if codec != "" && canonicalCodec(codec) != canonicalCodec(got.Codec) {
		return false
	}
	if width > 0 && width != got.Width || height > 0 && height != got.Height {
		return false
	}
	if fps > 0 && (got.FPS < fps*.85 || got.FPS > fps*1.15) {
		return false
	}
	// Camera VBR may use less than its configured ceiling; it may not exceed an
	// explicit requested ceiling beyond bounded container/measurement overhead.
	if desired.Bitrate > 0 && got.Bitrate > desired.Bitrate*13/10 {
		return false
	}
	if desired.GOP > 0 && got.GOP > desired.GOP+2 {
		return false
	}
	return true
}

type packetStats struct {
	count, bytes              int64
	seenPTS                   map[float64]bool
	seenKeys                  map[float64]bool
	firstPTS, lastPTS         float64
	hasPTS                    bool
	firstArrival, lastArrival time.Time
	maxGap                    time.Duration
	keys                      []float64
	lastDTS                   float64
	hasDTS                    bool
	discontinuous             bool
}
type observation struct {
	codec, audio               string
	width, height              int
	ptsFPS, arrivalFPS         float64
	ptsBitrate, arrivalBitrate int64
	gop                        int
	duration, maxGap           time.Duration
	complete                   bool
}

func (v *Verifier) startup() time.Duration {
	if v.StartupTimeout > 0 {
		return v.StartupTimeout
	}
	return 10 * time.Second
}
func (v *Verifier) observe(ctx context.Context, input string, d time.Duration, sanitize credential.Sanitizer, isFile bool) (observation, error) {
	var out observation
	limit, cancel := context.WithTimeout(ctx, d+v.startup())
	defer cancel()
	binary := v.Ffprobe
	if binary == "" {
		binary = "ffprobe"
	}
	args := []string{"-v", "error", "-analyzeduration", "100000", "-probesize", "4096", "-fpsprobesize", "2"}
	if !isFile {
		args = append(args, "-rtsp_transport", "tcp", "-timeout", strconv.FormatInt(v.startup().Microseconds(), 10))
	}
	args = append(args, "-read_intervals", "%+"+strconv.FormatFloat(d.Seconds(), 'f', 3, 64), "-show_packets", "-show_streams", "-show_entries", "stream=index,codec_type,codec_name,width,height,avg_frame_rate,r_frame_rate,bit_rate:packet=stream_index,pts_time,dts_time,size,flags", "-of", "compact=p=1:nk=0", input)
	cmd := exec.CommandContext(limit, binary, args...)
	boundedProcess(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return out, errors.New("cannot open media probe")
	}
	stderr := &limitedBuffer{max: 16 << 10}
	cmd.Stderr = stderr
	if err = cmd.Start(); err != nil {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		return out, errors.New("cannot start media probe")
	}
	stats := map[int]*packetStats{}
	streams := map[int]map[string]string{}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64<<10), 256<<10)
	count := 0
	outputBytes := 0
	exhausted := false
	for scanner.Scan() {
		count++
		outputBytes += len(scanner.Bytes())
		if count > 100000 || outputBytes > 8<<20 {
			exhausted = true
			cancel()
			break
		}
		line := scanner.Text()
		fields := strings.Split(line, "|")
		if len(fields) == 0 {
			continue
		}
		values := map[string]string{}
		for _, entry := range fields[1:] {
			k, val, ok := strings.Cut(entry, "=")
			if ok {
				values[k] = val
			}
		}
		index, _ := strconv.Atoi(values["stream_index"])
		if fields[0] == "stream" {
			index, _ = strconv.Atoi(values["index"])
			streams[index] = values
			continue
		}
		if fields[0] != "packet" {
			continue
		}
		p := stats[index]
		if p == nil {
			p = &packetStats{seenPTS: map[float64]bool{}, seenKeys: map[float64]bool{}}
			stats[index] = p
		}
		now := time.Now()
		if p.firstArrival.IsZero() {
			p.firstArrival = now
		}
		if !p.lastArrival.IsZero() && now.Sub(p.lastArrival) > p.maxGap {
			p.maxGap = now.Sub(p.lastArrival)
		}
		p.lastArrival = now
		p.count++
		size, _ := strconv.ParseInt(values["size"], 10, 64)
		if size > 0 {
			p.bytes += size
		}
		pts, e := strconv.ParseFloat(values["pts_time"], 64)
		if e == nil && !math.IsInf(pts, 0) && !math.IsNaN(pts) {
			p.seenPTS[pts] = true
			if !p.hasPTS {
				p.firstPTS = pts
				p.lastPTS = pts
				p.hasPTS = true
			}
			if pts < p.firstPTS {
				p.firstPTS = pts
			}
			if pts > p.lastPTS {
				p.lastPTS = pts
			}
			if strings.Contains(values["flags"], "K") && !p.seenKeys[pts] {
				p.seenKeys[pts] = true
				p.keys = append(p.keys, pts)
			}
		}
		dts, e := strconv.ParseFloat(values["dts_time"], 64)
		if e == nil {
			if p.hasDTS && (dts < p.lastDTS-.001 || dts-p.lastDTS > 2) {
				p.discontinuous = true
			}
			p.lastDTS = dts
			p.hasDTS = true
		}
	}
	scanErr := scanner.Err()
	if scanErr != nil {
		cancel()
	}
	err = cmd.Wait()
	if ctx.Err() != nil {
		return out, ctx.Err()
	}
	if exhausted || scanErr != nil {
		return out, errors.New("media probe resource limit reached")
	}
	if limit.Err() != nil {
		return out, ErrIncomplete
	}
	if err != nil {
		return out, fmt.Errorf("media probe failed: %s", sanitize.Redact(stderr.String()))
	}
	videoIndex := -1
	for index, stream := range streams {
		if stream["codec_type"] == "audio" {
			out.audio = stream["codec_name"]
		}
		if stream["codec_type"] != "video" {
			continue
		}
		videoIndex = index
		out.codec = canonicalCodec(stream["codec_name"])
		out.width, _ = strconv.Atoi(stream["width"])
		out.height, _ = strconv.Atoi(stream["height"])
	}
	p := stats[videoIndex]
	if p == nil || p.count < 2 || !p.hasPTS {
		return out, ErrIncomplete
	}
	span := p.lastPTS - p.firstPTS
	if span <= 0 {
		return out, ErrIncomplete
	}
	out.duration = time.Duration(span * float64(time.Second))
	out.ptsFPS = float64(len(p.seenPTS)-1) / span
	out.ptsBitrate = int64(float64(p.bytes) * 8 / span)
	out.maxGap = p.maxGap
	arrival := p.lastArrival.Sub(p.firstArrival).Seconds()
	if arrival > 0 {
		out.arrivalFPS = float64(len(p.seenPTS)-1) / arrival
		out.arrivalBitrate = int64(float64(p.bytes) * 8 / arrival)
	}
	if len(p.keys) > 1 {
		out.gop = int(math.Round((p.keys[len(p.keys)-1] - p.keys[0]) * out.ptsFPS / float64(len(p.keys)-1)))
	}
	out.complete = span >= d.Seconds()*.8 && !p.discontinuous
	if !isFile {
		out.complete = out.complete && arrival >= d.Seconds()*.7 && p.maxGap <= 2*time.Second
	}
	return out, nil
}
func (v *Verifier) decode(ctx context.Context, input string, d time.Duration, sanitize credential.Sanitizer, isFile bool) (int64, error) {
	limit, cancel := context.WithTimeout(ctx, d+v.startup())
	defer cancel()
	binary := v.Ffmpeg
	if binary == "" {
		binary = "ffmpeg"
	}
	args := []string{"-hide_banner", "-nostdin", "-v", "error", "-analyzeduration", "100000", "-probesize", "4096", "-xerror", "-err_detect", "explode", "-threads", "2"}
	if !isFile {
		args = append(args, "-rtsp_transport", "tcp", "-timeout", strconv.FormatInt(v.startup().Microseconds(), 10))
	}
	args = append(args, "-i", input, "-t", strconv.FormatFloat(d.Seconds(), 'f', 3, 64), "-map", "0:v:0", "-map", "0:a:0?", "-progress", "pipe:1", "-nostats", "-f", "null", "-")
	cmd := exec.CommandContext(limit, binary, args...)
	boundedProcess(cmd)
	stdout := &limitedBuffer{max: 64 << 10}
	stderr := &limitedBuffer{max: 16 << 10}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	if limit.Err() != nil {
		return 0, ErrIncomplete
	}
	if err != nil {
		return 0, fmt.Errorf("%w: %s", ErrDecode, sanitize.Redact(stderr.String()))
	}
	frames := int64(0)
	for _, line := range strings.Split(stdout.String(), "\n") {
		if val, ok := strings.CutPrefix(line, "frame="); ok {
			if n, e := strconv.ParseInt(strings.TrimSpace(val), 10, 64); e == nil && n > frames {
				frames = n
			}
		}
	}
	if frames < 2 {
		return 0, ErrDecode
	}
	return frames, nil
}

type limitedBuffer struct {
	bytes.Buffer
	max int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	left := b.max - b.Len()
	if left > 0 {
		if left > len(p) {
			left = len(p)
		}
		b.Buffer.Write(p[:left])
	}
	return n, nil
}
