package fmp4

import (
	"errors"
	"fmt"
)

// Track is one track as the init segment describes it.
type Track struct {
	ID uint32
	// Timescale is the track's ticks per second: what `tfdt` and sample
	// durations count in.
	Timescale uint32
	// Handler is `vide` or `soun` (or another handler type).
	Handler string
	// Entry is the sample entry's type: `avc1`/`avc3` for H.264, `hvc1`/
	// `hev1` for H.265, `mp4a` for AAC, `Opus` for Opus.
	Entry string
	// Config is the codec configuration box's payload: `avcC`, `hvcC`,
	// `dOps`, or `esds`; nil when the entry has none.
	Config []byte
	// Channels and SampleRate of an audio track.
	Channels   int
	SampleRate int
	// Defaults from `trex`, for fragments that state none.
	DefaultDuration, DefaultSize, DefaultFlags uint32
}

// Video says the track is video; Audio that it is sound.
func (t *Track) Video() bool { return t.Handler == "vide" }
func (t *Track) Audio() bool { return t.Handler == "soun" }

// Init is an init segment: `ftyp` and `moov`, and what they say.
type Init struct {
	// Bytes is the segment as it came, what every lamina starts with.
	Bytes  []byte
	Tracks []*Track
}

// Video is the first video track, or nil.
func (i *Init) Video() *Track {
	for _, t := range i.Tracks {
		if t.Video() {
			return t
		}
	}

	return nil
}

// Audio is the audio tracks, in order.
func (i *Init) Audio() []*Track {
	var out []*Track
	for _, t := range i.Tracks {
		if t.Audio() {
			out = append(out, t)
		}
	}

	return out
}

// Track is the track with the ID, or nil.
func (i *Init) Track(id uint32) *Track {
	for _, t := range i.Tracks {
		if t.ID == id {
			return t
		}
	}

	return nil
}

// Opus is the first Opus audio track, or nil.
func (i *Init) Opus() *Track {
	for _, t := range i.Tracks {
		if t.Audio() && t.Entry == "Opus" {
			return t
		}
	}

	return nil
}

var errNoMoov = errors.New("fmp4: no moov in the init segment")

// ParseInit reads an init segment: `ftyp` (optional) and `moov`.
func ParseInit(b []byte) (*Init, error) {
	moov, ok := Find(b, "moov")
	if !ok {
		return nil, errNoMoov
	}
	init := &Init{Bytes: b}
	boxes, err := Children(moov.Payload(b))
	if err != nil {
		return nil, fmt.Errorf("fmp4: moov: %w", err)
	}
	mp := moov.Payload(b)
	for _, x := range boxes {
		switch x.Type {
		case "trak":
			t, err := parseTrak(x.Payload(mp))
			if err != nil {
				return nil, err
			}
			init.Tracks = append(init.Tracks, t)
		case "mvex":
			for _, y := range children(x.Payload(mp)) {
				if y.Type != "trex" {
					continue
				}
				p := y.Payload(x.Payload(mp))
				if t := init.Track(u32(p, 4)); t != nil {
					t.DefaultDuration, t.DefaultSize, t.DefaultFlags = u32(p, 12), u32(p, 16), u32(p, 20)
				}
			}
		}
	}
	if len(init.Tracks) == 0 {
		return nil, errors.New("fmp4: moov names no track")
	}

	return init, nil
}

// children is Children without the error: what could be read.
func children(b []byte) []Box {
	out, _ := Children(b)

	return out
}

func parseTrak(trak []byte) (*Track, error) {
	t := &Track{}
	for _, x := range children(trak) {
		p := x.Payload(trak)
		switch x.Type {
		case "tkhd":
			if v, _ := fullBox(p); v == 1 {
				t.ID = u32(p, 20)
			} else {
				t.ID = u32(p, 12)
			}
		case "mdia":
			for _, y := range children(p) {
				q := y.Payload(p)
				switch y.Type {
				case "mdhd":
					if v, _ := fullBox(q); v == 1 {
						t.Timescale = u32(q, 20)
					} else {
						t.Timescale = u32(q, 12)
					}
				case "hdlr":
					if len(q) >= 12 {
						t.Handler = string(q[8:12])
					}
				case "minf":
					if stbl, ok := Find(q, "stbl"); ok {
						if stsd, ok := Find(stbl.Payload(q), "stsd"); ok {
							parseStsd(t, stsd.Payload(stbl.Payload(q)))
						}
					}
				}
			}
		}
	}
	if t.ID == 0 {
		return nil, errors.New("fmp4: a trak without a track ID")
	}
	if t.Timescale == 0 {
		return nil, fmt.Errorf("fmp4: track %d has no timescale", t.ID)
	}

	return t, nil
}

// parseStsd reads the first sample entry: its type and its codec
// configuration box.
func parseStsd(t *Track, p []byte) {
	// version/flags, entry_count, then the entries.
	if len(p) < 8 {
		return
	}
	entries := children(p[8:])
	if len(entries) == 0 {
		return
	}
	e := entries[0]
	t.Entry = e.Type
	body := e.Payload(p[8:])
	// SampleEntry: 6 reserved, 2 data_reference_index; then the visual or
	// audio fields, then the child boxes.
	var fields int
	switch {
	case t.Handler == "vide":
		fields = 8 + 70
	case t.Handler == "soun":
		fields = 8 + 20
		if len(body) >= 28 {
			t.Channels = int(u32(body, 16) >> 16)
			t.SampleRate = int(u32(body, 24) >> 16)
		}
	default:
		return
	}
	if fields > len(body) {
		return
	}
	for _, c := range children(body[fields:]) {
		switch c.Type {
		case "avcC", "hvcC", "dOps", "esds", "vpcC", "av1C":
			t.Config = append([]byte(nil), c.Payload(body[fields:])...)
			return
		}
	}
}
