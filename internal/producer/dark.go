package producer

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// Dark scenes (§38.10): a source with `idle:` stops storing once its scene
// has been dark for `dark_after`, and stores again the moment it is lit,
// for at least that long. The capture's ffmpeg measures it: a
// one-frame-per-second branch of the video runs through blackframe, which
// prints a line per dark second on stderr, and the producer reads those
// lines like every other line the process writes. Capture goes on
// throughout, so live viewing and the measurement itself do; only the
// segments are skipped, and told to the CP as such.

// IdleConfig is a source's `idle:` (§38.10).
type IdleConfig struct {
	// DarkAfter is how long the scene stays dark before its segments are
	// skipped; DefaultDarkAfter when zero.
	DarkAfter time.Duration
	// Threshold is the luma, as a fraction of full scale, at or below which
	// a pixel is dark; DefaultDarkThreshold when zero.
	Threshold float64
}

const (
	DefaultDarkAfter     = 10 * time.Minute
	DefaultDarkThreshold = 0.10
	// darkShare is the share of dark pixels, in percent, for a frame to be
	// dark: blackframe's default.
	darkShare = 98
	// litAfter is how long without a dark line means the scene is lit: the
	// lines come once a second.
	litAfter = 2500 * time.Millisecond
)

func (c *IdleConfig) darkAfter() time.Duration {
	if c == nil || c.DarkAfter <= 0 {
		return DefaultDarkAfter
	}

	return c.DarkAfter
}

// threshold is the pixel threshold on blackframe's 0..255 scale.
func (c *IdleConfig) threshold() int {
	t := DefaultDarkThreshold
	if c != nil && c.Threshold > 0 {
		t = c.Threshold
	}
	v := int(t*255 + 0.5)
	if v < 1 {
		v = 1
	}
	if v > 255 {
		v = 255
	}

	return v
}

// filter is the video filter chain: the video to the encoder as it is,
// and a one-frame-per-second copy through blackframe into nothing.
func (c *IdleConfig) filter() string {
	return fmt.Sprintf("split[v][m];[m]fps=1,blackframe=amount=%d:threshold=%d,nullsink;[v]null", darkShare, c.threshold())
}

// darkLine says whether a line of stderr is blackframe's.
func darkLine(line string) bool {
	return strings.Contains(line, "pblack:") && strings.Contains(line, "blackframe")
}

// darkTracker is one source's dark state (§38.10).
type darkTracker struct {
	after time.Duration
	now   func() time.Time
	// onChange is told when storing is suspended (true) or resumed.
	onChange func(suppressed bool)

	mu         sync.Mutex
	lastDark   time.Time
	darkSince  time.Time
	suppressed bool
	seconds    int64
}

func newDarkTracker(after time.Duration, now func() time.Time) *darkTracker {
	return &darkTracker{after: after, now: now}
}

// observe takes one line of the capture's stderr and answers whether it
// was blackframe's, which is a dark second and is consumed rather than
// logged.
func (d *darkTracker) observe(line string) bool {
	if !darkLine(line) {
		return false
	}
	d.darkSecond()

	return true
}

// darkSecond is one dark second, however it was measured: a blackframe
// line, or a GStreamer capture's picture (darkFrame).
func (d *darkTracker) darkSecond() {
	now := d.now()
	d.mu.Lock()
	d.lastDark = now
	if d.darkSince.IsZero() {
		d.darkSince = now
	}
	d.seconds++
	d.mu.Unlock()
	d.tick(now)
}

// tick runs every second: a scene is lit again when the lines stop, and
// dark for good when they have gone on for `dark_after`.
func (d *darkTracker) tick(now time.Time) {
	d.mu.Lock()
	var change *bool
	switch {
	case !d.lastDark.IsZero() && now.Sub(d.lastDark) > litAfter:
		d.lastDark, d.darkSince = time.Time{}, time.Time{}
		if d.suppressed {
			d.suppressed = false
			change = new(bool)
		}
	case !d.darkSince.IsZero() && !d.suppressed && now.Sub(d.darkSince) >= d.after:
		d.suppressed = true
		v := true
		change = &v
	}
	d.mu.Unlock()
	if change != nil && d.onChange != nil {
		d.onChange(*change)
	}
}

// Suppressed says whether segments are being skipped.
func (d *darkTracker) Suppressed() bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.suppressed
}

// Dark says whether the scene is dark now, past the threshold or not.
func (d *darkTracker) Dark() bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	return !d.darkSince.IsZero()
}

// Since is when the current dark spell began, zero when lit.
func (d *darkTracker) Since() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.darkSince
}

// A GStreamer capture has no blackframe (§38.10). Its pipeline sends one
// picture a second, scaled down to darkWidth and grey, to a pipe of its own
// (fd 3), and the producer counts its dark pixels as blackframe would: the
// same share, the same threshold on the luma.

// darkWidth is how wide the measured picture is: enough pixels for a share,
// few enough that the scaling costs nothing.
const darkWidth = 160

// darkSize is the measured picture's size for a source's mode: darkWidth
// wide, as tall as its aspect makes it, rounded to an even number.
func darkSize(size string) (w, h int) {
	sw, sh := parseSize(size)
	if size == "" {
		sw, sh = 1280, 720
	}
	h = (darkWidth*sh/sw + 1) &^ 1
	if h < 2 {
		h = 2
	}

	return darkWidth, h
}

// darkFrame says whether a grey picture is dark: darkShare percent of its
// pixels at or below the threshold.
func darkFrame(pic []byte, threshold int) bool {
	if len(pic) == 0 {
		return false
	}
	n := 0
	for _, y := range pic {
		if int(y) <= threshold {
			n++
		}
	}

	return n*100 >= darkShare*len(pic)
}

// readDark reads a pipeline's pictures of w×h grey bytes until the pipe
// ends, telling dark for every dark one.
func readDark(r io.Reader, w, h, threshold int, dark func()) {
	pic := make([]byte, w*h)
	for {
		if _, err := io.ReadFull(r, pic); err != nil {
			return
		}
		if darkFrame(pic, threshold) {
			dark()
		}
	}
}
