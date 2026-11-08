// Package mediaprobe qualifies a bounded WHEP H264 stream using actual decoded
// pixels. It confers no CP authority: callers must obtain an endpoint capability
// for the current job/Source epoch and cancel when that authority changes.
package mediaprobe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

var (
	ErrOptions            = errors.New("media probe: invalid options")
	ErrEndpoint           = errors.New("media probe: invalid endpoint")
	ErrNegotiation        = errors.New("media probe: WHEP negotiation failed")
	ErrMedia              = errors.New("media probe: incomplete or invalid H264 media")
	ErrLimit              = errors.New("media probe: resource limit exceeded")
	ErrDecode             = errors.New("media probe: independent H264 decode failed")
	ErrDecoderUnavailable = errors.New("media probe: decoder unavailable")
)

// Endpoint is a short-lived WHEP capability, never a persistent/wire DTO.
type Endpoint struct{ URL, Token string }

const redactedEndpoint = "[media probe endpoint]"

func (Endpoint) Format(s fmt.State, _ rune)   { _, _ = io.WriteString(s, redactedEndpoint) }
func (Endpoint) String() string               { return redactedEndpoint }
func (Endpoint) GoString() string             { return redactedEndpoint }
func (Endpoint) MarshalJSON() ([]byte, error) { return []byte(`"` + redactedEndpoint + `"`), nil }
func (Endpoint) MarshalYAML() (any, error)    { return redactedEndpoint, nil }

// Options has hard ceilings as well as conservative defaults. HTTPClient is
// copied; its Transport/CA configuration is retained and redirects are disabled
// for both POST and DELETE without mutating the caller's client.
type Options struct {
	Timeout    time.Duration
	Frames     int
	MaxBytes   int
	MaxPackets int
	MaxPixels  int
	FFmpeg     string
	HTTPClient *http.Client
}

// Evidence separates receiver-local receipt times and RTP progress from the
// later independent decoder's pixel-output times. None identifies the camera's
// capture epoch. Callers must correlate the current Producer/Source epoch.
type Evidence struct {
	AccessUnits, DecodedFrames      int
	Width, Height                   int
	RTPClockRate                    uint32
	FirstRTP, LastRTP               uint32
	RTPAdvances                     int
	FirstReceivedAt, LastReceivedAt time.Time
	FirstDecodedAt, LastDecodedAt   time.Time
	PayloadBytes, EncodedBytes      int
}

func (o Options) normalized() (Options, error) {
	if o.Timeout == 0 {
		o.Timeout = 15 * time.Second
	}
	if o.Frames == 0 {
		o.Frames = 8
	}
	if o.MaxBytes == 0 {
		o.MaxBytes = 16 << 20
	}
	if o.MaxPackets == 0 {
		o.MaxPackets = 32768
	}
	if o.MaxPixels == 0 {
		o.MaxPixels = 16 << 20
	}
	if o.FFmpeg == "" {
		o.FFmpeg = "ffmpeg"
	}
	if o.Timeout <= 0 || o.Timeout > time.Minute || o.Frames < 2 || o.Frames > 32 || o.MaxBytes < 1024 || o.MaxBytes > 64<<20 || o.MaxPackets < 2 || o.MaxPackets > 50000 || o.MaxPixels < 1 || o.MaxPixels > 16<<20 {
		return Options{}, ErrOptions
	}
	source := o.HTTPClient
	if source == nil {
		source = http.DefaultClient
	}
	client := *source
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	o.HTTPClient = &client
	return o, nil
}

func (e Endpoint) validate() error {
	if len(e.URL) > 4096 || len(e.Token) == 0 || len(e.Token) > 16384 {
		return ErrEndpoint
	}
	u, err := url.Parse(e.URL)
	if err != nil || u.User != nil || u.Hostname() == "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return ErrEndpoint
	}
	for _, b := range []byte(e.Token) {
		if b < 0x21 || b > 0x7e {
			return ErrEndpoint
		}
	}
	return nil
}

// Probe returns evidence only after at least two complete advancing access units
// have independently produced complete decoded pixel frames. Failures return a
// zero Evidence and fixed diagnostics; URL, token and raw decoder text never
// become error messages. FFmpeg is an external installed dependency.
func Probe(ctx context.Context, options Options, endpoint Endpoint) (Evidence, error) {
	if err := ctx.Err(); err != nil {
		return Evidence{}, err
	}
	o, err := options.normalized()
	if err != nil {
		return Evidence{}, err
	}
	if err = endpoint.validate(); err != nil {
		return Evidence{}, err
	}
	bounded, stop := context.WithTimeout(ctx, o.Timeout)
	defer stop()
	captured, err := capture(bounded, o, endpoint)
	if err != nil {
		return Evidence{}, err
	}
	if err := bounded.Err(); err != nil {
		return Evidence{}, err
	}
	pixels, err := decode(bounded, o, captured.data)
	if err != nil {
		return Evidence{}, err
	}
	if pixels.frames < 2 || pixels.frames > captured.evidence.AccessUnits || !pixels.last.After(pixels.first) {
		return Evidence{}, ErrDecode
	}
	if err := bounded.Err(); err != nil {
		return Evidence{}, err
	}
	e := captured.evidence
	e.DecodedFrames = pixels.frames
	e.Width = pixels.width
	e.Height = pixels.height
	e.FirstDecodedAt = pixels.first
	e.LastDecodedAt = pixels.last
	return e, nil
}
