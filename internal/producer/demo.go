package producer

import (
	"fmt"
	"strings"
)

// A demo source is a picture and a tone ffmpeg draws for itself (§38.1).
// Everything a producer does — registering its cameras, negotiating a
// ceiling, cutting on the phase, uploading, feeding the relay — needs a
// stream, and a tutorial, a walk-through or a bench has no camera to hand
// and no recording to point at. `demo:` is that stream, made where the
// capture would have been, so nothing downstream can tell the difference.
//
// Three of them are told apart on sight and by ear, which is what a wall
// of live tiles is for.

// DemoPrefix is what a demo source's input starts with.
const DemoPrefix = "demo:"

// DemoPattern is one demo source's look and sound.
type DemoPattern struct {
	// Pattern is the lavfi source ffmpeg draws, and the name `demo:` takes.
	Pattern string
	// Options are the lavfi source's own settings, beside the size and the
	// rate every one of them takes.
	Options string
	// Tone is the sine's frequency in hertz.
	Tone int
}

// DemoPatterns are the patterns DemoSources hands out in turn.
//
// Every one of them **moves**, which is the whole point: a still image
// encodes to nearly nothing (colour bars are 85 kbps at 720p30 where these
// are 2 Mbps), so a demo drawn from one would fill a segment in a day, sit
// far under its ceiling, and teach a reader the wrong numbers. These three
// each sit at the ceiling a 720p30 camera is given (§38.5), and together
// they cost about two thirds of a core to encode.
//
// The tones are an A major triad, so three sources heard together are
// three sources.
var DemoPatterns = []DemoPattern{
	{Pattern: "testsrc2", Tone: 440},
	{Pattern: "cellauto", Tone: 554},
	{Pattern: "life", Options: "mold=10:life_color=#22aa55:death_color=#113322:ratio=0.1", Tone: 659},
}

// What a demo source records at when its configuration says nothing:
// §38.5's starting ceiling for this mode is 2 Mbps, which the producer
// proposes for itself.
const (
	DemoSize = "1280x720"
	DemoFps  = 30
	// DemoSampleRate is the tone's sample rate; 48 kHz is what every
	// encoder here takes without resampling.
	DemoSampleRate = 48000
)

// Filter is the lavfi source ffmpeg is given for this pattern, at the size
// and rate a source records.
func (p DemoPattern) Filter(size string, fps int) string {
	if size == "" {
		size = DemoSize
	}
	if fps <= 0 {
		fps = DemoFps
	}
	out := fmt.Sprintf("%s=size=%s:rate=%d", p.Pattern, size, fps)
	if p.Options != "" {
		out += ":" + p.Options
	}

	return out
}

// isDemo says whether an input is a demo source.
func isDemo(input string) bool { return strings.HasPrefix(input, DemoPrefix) }

// demoOf answers the pattern a `demo:` input names; `demo:` alone is the
// first one. An unknown name is refused rather than drawn as something
// else, and `New` asks before a capture ever starts.
func demoOf(input string) (DemoPattern, error) {
	name := strings.TrimPrefix(input, DemoPrefix)
	if name == "" {
		return DemoPatterns[0], nil
	}
	names := make([]string, 0, len(DemoPatterns))
	for _, p := range DemoPatterns {
		if p.Pattern == name {
			return p, nil
		}
		names = append(names, p.Pattern)
	}

	return DemoPattern{}, fmt.Errorf("%q is not a pattern this build draws: %s", name, strings.Join(names, ", "))
}

// DemoSources are `n` sources a producer records with nothing attached,
// the patterns handed out in turn and their aliases numbered from one.
func DemoSources(n int) []SourceConfig {
	out := make([]SourceConfig, 0, max(n, 0))
	for i := range n {
		p := DemoPatterns[i%len(DemoPatterns)]
		out = append(out, SourceConfig{
			Alias: fmt.Sprintf("demo-%02d", i+1),
			Name:  fmt.Sprintf("%s, %d Hz", p.Pattern, p.Tone),
			Input: DemoPrefix + p.Pattern,
			Size:  DemoSize,
			Fps:   DemoFps,
		})
	}

	return out
}
