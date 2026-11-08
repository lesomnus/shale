package mediaprobe

import (
	"bytes"
	"context"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// pixelFrames consumes actual gray Y4M pixels, not decoder progress counters or
// NAL markers. Pixel bytes are streamed/discarded; no frame-sized Go allocation
// is made. A truncated header/frame is never counted as decoded evidence.
type pixelFrames struct {
	maxPixels, maxFrames  int
	header                []byte
	initialized           bool
	remaining             int
	width, height, frames int
	first, last           time.Time
}

func (p *pixelFrames) Write(data []byte) (int, error) {
	original := len(data)
	for len(data) > 0 {
		if p.remaining > 0 {
			n := min(p.remaining, len(data))
			p.remaining -= n
			data = data[n:]
			if p.remaining == 0 {
				now := time.Now()
				if p.frames == 0 {
					p.first = now
				}
				p.last = now
				p.frames++
			}
			continue
		}
		if p.frames >= p.maxFrames {
			return 0, ErrLimit
		}
		newline := bytes.IndexByte(data, '\n')
		n := len(data)
		if newline >= 0 {
			n = newline + 1
		}
		if len(p.header)+n > 4096 {
			return 0, ErrLimit
		}
		p.header = append(p.header, data[:n]...)
		data = data[n:]
		if newline < 0 {
			continue
		}
		fields := strings.Fields(string(p.header))
		p.header = p.header[:0]
		if !p.initialized {
			if len(fields) == 0 || fields[0] != "YUV4MPEG2" {
				return 0, ErrDecode
			}
			mono := false
			for _, field := range fields[1:] {
				if len(field) < 2 {
					continue
				}
				switch field[0] {
				case 'W':
					p.width, _ = strconv.Atoi(field[1:])
				case 'H':
					p.height, _ = strconv.Atoi(field[1:])
				case 'C':
					mono = field == "Cmono"
				}
			}
			if p.width < 1 || p.height < 1 || p.width > 8192 || p.height > 8192 || int64(p.width)*int64(p.height) > int64(p.maxPixels) || !mono {
				return 0, ErrLimit
			}
			p.initialized = true
		} else {
			if len(fields) == 0 || fields[0] != "FRAME" {
				return 0, ErrDecode
			}
			p.remaining = p.width * p.height
		}
	}
	return original, nil
}

func decode(ctx context.Context, o Options, data []byte) (*pixelFrames, error) {
	cmd := exec.CommandContext(ctx, o.FFmpeg, "-hide_banner", "-loglevel", "error", "-xerror", "-err_detect", "explode", "-max_alloc", "67108864", "-max_pixels", strconv.Itoa(o.MaxPixels), "-threads", "1", "-f", "h264", "-i", "pipe:0", "-an", "-fps_mode", "passthrough", "-frames:v", strconv.Itoa(o.Frames), "-threads", "1", "-pix_fmt", "gray", "-strict", "-1", "-f", "yuv4mpegpipe", "pipe:1")
	cmd.WaitDelay = 2 * time.Second
	cmd.Stdin = bytes.NewReader(data)
	pixels := &pixelFrames{maxPixels: o.MaxPixels, maxFrames: o.Frames}
	cmd.Stdout = pixels
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if cmd.Process == nil {
			return nil, ErrDecoderUnavailable
		}
		return nil, ErrDecode
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !pixels.initialized || pixels.remaining != 0 || len(pixels.header) > 0 || pixels.frames < 2 {
		return nil, ErrDecode
	}
	return pixels, nil
}
