package fmp4

import (
	"errors"
	"fmt"
)

// Sample is one sample of a fragment's track: where its bytes are in the
// fragment, how long it lasts, and its flags.
type Sample struct {
	// Off and Size place the sample in Fragment.Bytes.
	Off, Size int
	Duration  uint32
	Flags     uint32
	// CTS is the composition time offset, signed.
	CTS int32
}

// Sync says the sample is a sync sample (a keyframe): the
// sample_is_non_sync_sample bit is clear.
func (s Sample) Sync() bool { return s.Flags&0x10000 == 0 }

// Traf is one track's part of a fragment.
type Traf struct {
	TrackID uint32
	// Time is `tfdt`, the decode time of the first sample, in the
	// track's timescale.
	Time    int64
	Samples []Sample
}

// Key says the traf begins with a sync sample.
func (t *Traf) Key() bool { return len(t.Samples) > 0 && t.Samples[0].Sync() }

// Duration is the samples' durations together, in the track's timescale.
func (t *Traf) Duration() int64 {
	var d int64
	for _, s := range t.Samples {
		d += int64(s.Duration)
	}

	return d
}

// Fragment is one `moof` with its `mdat`, as it came.
type Fragment struct {
	Bytes []byte
	// Seq is `mfhd`'s sequence number.
	Seq   uint32
	Trafs []*Traf
}

// Traf is the track's part, or nil.
func (f *Fragment) Traf(track uint32) *Traf {
	for _, t := range f.Trafs {
		if t.TrackID == track {
			return t
		}
	}

	return nil
}

// Video is the video track's part, or nil.
func (f *Fragment) Video(init *Init) *Traf {
	if v := init.Video(); v != nil {
		return f.Traf(v.ID)
	}

	return nil
}

// Key says the fragment's video begins with a keyframe: a lamina may be
// cut before it, a viewer may start at it. The first sample's slice
// decides where it can (KeyByNAL); its sync flag otherwise.
func (f *Fragment) Key(init *Init) bool {
	video := init.Video()
	if video == nil {
		return false
	}
	t := f.Traf(video.ID)
	if t == nil || len(t.Samples) == 0 {
		return false
	}
	s := t.Samples[0]

	return video.SampleKey(f.Bytes[s.Off:s.Off+s.Size], s)
}

var errNoMoof = errors.New("fmp4: no moof in the fragment")

// ParseFragment reads a fragment: `moof` then `mdat`, with `styp` or
// `sidx` allowed in front. `init` gives the tracks' defaults.
func ParseFragment(b []byte, init *Init) (*Fragment, error) {
	boxes, err := Children(b)
	if err != nil {
		return nil, err
	}
	var moof *Box
	for i := range boxes {
		if boxes[i].Type == "moof" {
			moof = &boxes[i]
			break
		}
	}
	if moof == nil {
		return nil, errNoMoof
	}
	f := &Fragment{Bytes: b}
	mp := moof.Payload(b)
	// With default-base-is-moof (what ffmpeg writes for MSE) offsets count
	// from the moof; without it, and without a base of their own, a traf's
	// data follows the previous traf's.
	next := int64(moof.Off)
	for _, x := range children(mp) {
		switch x.Type {
		case "mfhd":
			f.Seq = u32(x.Payload(mp), 4)
		case "traf":
			t, end, err := parseTraf(x.Payload(mp), int64(moof.Off), next, init, len(b))
			if err != nil {
				return nil, err
			}
			f.Trafs = append(f.Trafs, t)
			next = end
		}
	}

	return f, nil
}

func parseTraf(p []byte, moofOff, prevEnd int64, init *Init, limit int) (*Traf, int64, error) {
	t := &Traf{}
	var tfhdFlags uint32
	var base int64
	var defDur, defSize, defFlags uint32
	baseSet := false
	var truns [][]byte
	for _, x := range children(p) {
		q := x.Payload(p)
		switch x.Type {
		case "tfhd":
			_, tfhdFlags = fullBox(q)
			t.TrackID = u32(q, 4)
			off := 8
			if tfhdFlags&0x1 != 0 {
				base, baseSet = int64(u64(q, off)), true
				off += 8
			}
			if tfhdFlags&0x2 != 0 {
				off += 4
			}
			if tfhdFlags&0x8 != 0 {
				defDur = u32(q, off)
				off += 4
			}
			if tfhdFlags&0x10 != 0 {
				defSize = u32(q, off)
				off += 4
			}
			if tfhdFlags&0x20 != 0 {
				defFlags = u32(q, off)
			}
		case "tfdt":
			if v, _ := fullBox(q); v == 1 {
				t.Time = int64(u64(q, 4))
			} else {
				t.Time = int64(u32(q, 4))
			}
		case "trun":
			truns = append(truns, q)
		}
	}
	if t.TrackID == 0 {
		return nil, 0, errors.New("fmp4: a traf without a track ID")
	}
	if tr := init.Track(t.TrackID); tr != nil {
		if tfhdFlags&0x8 == 0 {
			defDur = tr.DefaultDuration
		}
		if tfhdFlags&0x10 == 0 {
			defSize = tr.DefaultSize
		}
		if tfhdFlags&0x20 == 0 {
			defFlags = tr.DefaultFlags
		}
	}
	switch {
	case baseSet:
	case tfhdFlags&0x20000 != 0:
		base = moofOff
	default:
		base = prevEnd
	}
	end := base
	for _, q := range truns {
		v, flags := fullBox(q)
		count := int(u32(q, 4))
		off := 8
		pos := base
		if flags&0x1 != 0 {
			pos = base + int64(int32(u32(q, off)))
			off += 4
		}
		var first uint32
		hasFirst := flags&0x4 != 0
		if hasFirst {
			first = u32(q, off)
			off += 4
		}
		for i := 0; i < count; i++ {
			s := Sample{Duration: defDur, Size: int(defSize), Flags: defFlags}
			if flags&0x100 != 0 {
				s.Duration = u32(q, off)
				off += 4
			}
			if flags&0x200 != 0 {
				s.Size = int(u32(q, off))
				off += 4
			}
			if flags&0x400 != 0 {
				s.Flags = u32(q, off)
				off += 4
			} else if i == 0 && hasFirst {
				s.Flags = first
			}
			if flags&0x800 != 0 {
				c := u32(q, off)
				if v == 0 {
					s.CTS = int32(c)
				} else {
					s.CTS = int32(c)
				}
				off += 4
			}
			if off > len(q) {
				return nil, 0, fmt.Errorf("fmp4: track %d: trun runs past its box", t.TrackID)
			}
			s.Off = int(pos)
			if pos < 0 || pos+int64(s.Size) > int64(limit) {
				return nil, 0, fmt.Errorf("fmp4: track %d: a sample runs past the fragment", t.TrackID)
			}
			pos += int64(s.Size)
			t.Samples = append(t.Samples, s)
		}
		end = pos
	}

	return t, end, nil
}
