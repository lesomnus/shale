package mp4export

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/lesomnus/shale/internal/fmp4"
)

type extent struct {
	typ      string
	off, end int64
	hdr      int
}

func readHeader(r io.ReaderAt, at, size int64) (extent, error) {
	if at < 0 || at > size-8 {
		return extent{}, io.ErrUnexpectedEOF
	}
	var b [16]byte
	if _, err := r.ReadAt(b[:8], at); err != nil {
		return extent{}, err
	}
	n := int64(binary.BigEndian.Uint32(b[:]))
	hdr := 8
	if n == 1 {
		if at > size-16 {
			return extent{}, io.ErrUnexpectedEOF
		}
		if _, err := r.ReadAt(b[8:], at+8); err != nil {
			return extent{}, err
		}
		u := binary.BigEndian.Uint64(b[8:])
		if u > math.MaxInt64 {
			return extent{}, errors.New("box size overflow")
		}
		n = int64(u)
		hdr = 16
	}
	if n < int64(hdr) || n > size-at {
		return extent{}, fmt.Errorf("invalid %q box size %d", b[4:8], n)
	}
	return extent{typ: string(b[4:8]), off: at, end: at + n, hdr: hdr}, nil
}

type metadataBudget struct {
	bytes   int64
	samples int
}

func readMetadata(r io.ReaderAt, x extent, budget *metadataBudget) ([]byte, error) {
	if x.end-x.off > maxMetadataBox {
		return nil, fmt.Errorf("%s metadata exceeds %d bytes", x.typ, maxMetadataBox)
	}
	if x.end-x.off > MaxMetadataBytes-budget.bytes {
		return nil, fmt.Errorf("metadata exceeds %d bytes; select a shorter range", MaxMetadataBytes)
	}
	budget.bytes += x.end - x.off
	b := make([]byte, x.end-x.off)
	_, err := r.ReadAt(b, x.off)
	return b, err
}
func scan(ctx context.Context, s Source, budget *metadataBudget) (*scanned, error) {
	if s.Reader == nil || s.Size < 8 || s.Start.IsZero() {
		return nil, errors.New("invalid source reader, size or start")
	}
	x := &scanned{tracks: make(map[uint32]*track)}
	var moofs, mdats []extent
	boxCount := 0
	for at := int64(0); at < s.Size; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		h, err := readHeader(s.Reader, at, s.Size)
		if err != nil {
			return nil, err
		}
		boxCount++
		if boxCount > 262144 {
			return nil, errors.New("too many container boxes; select a shorter range")
		}
		switch h.typ {
		case "ftyp":
			if x.ftyp != nil {
				return nil, errors.New("duplicate ftyp")
			}
			x.ftyp, err = readMetadata(s.Reader, h, budget)
		case "moov":
			if x.moov != nil {
				return nil, errors.New("duplicate moov")
			}
			x.moov, err = readMetadata(s.Reader, h, budget)
		case "moof":
			moofs = append(moofs, h)
		case "mdat":
			mdats = append(mdats, h)
		}
		if err != nil {
			return nil, err
		}
		at = h.end
	}
	if x.moov == nil || len(moofs) == 0 {
		return nil, ErrEmpty
	}
	init, err := fmp4.ParseInit(append(append([]byte(nil), x.ftyp...), x.moov...))
	if err != nil {
		return nil, err
	}
	x.init = init
	if len(init.Tracks) > 16 {
		return nil, errors.New("more than 16 tracks are not supported")
	}
	if len(x.ftyp) == 0 {
		x.ftyp = box("ftyp", []byte("isom\x00\x00\x02\x00isomiso2mp41"))
	}
	moov, _ := fmp4.Find(x.moov, "moov")
	mp := moov.Payload(x.moov)
	children, err := fmp4.Children(mp)
	if err != nil {
		return nil, err
	}
	for _, b := range children {
		if b.Type != "trak" {
			continue
		}
		raw := b.Whole(mp)
		tkhd, err := childPayload(b.Payload(mp), "tkhd")
		if err != nil {
			return nil, err
		}
		idOff := 12
		if len(tkhd) > 0 && tkhd[0] == 1 {
			idOff = 20
		}
		if len(tkhd) < idOff+4 {
			return nil, errors.New("short tkhd")
		}
		id := binary.BigEndian.Uint32(tkhd[idOff:])
		meta := init.Track(id)
		if meta == nil {
			return nil, errors.New("unrecognized track")
		}
		if _, ok := x.tracks[id]; ok {
			return nil, errors.New("duplicate track ID")
		}
		switch meta.Entry {
		case "avc1", "avc3", "mp4a", "Opus":
		default:
			return nil, fmt.Errorf("unsupported sample entry %q", meta.Entry)
		}
		mdia, err := childPayload(b.Payload(mp), "mdia")
		if err != nil {
			return nil, err
		}
		minf, err := childPayload(mdia, "minf")
		if err != nil {
			return nil, err
		}
		stbl, err := childPayload(minf, "stbl")
		if err != nil {
			return nil, err
		}
		stsd, ok := fmp4.Find(stbl, "stsd")
		if !ok {
			return nil, errors.New("no stsd")
		}
		sd := stsd.Payload(stbl)
		if len(sd) < 8 || binary.BigEndian.Uint32(sd[4:]) != 1 {
			return nil, errors.New("exactly one sample description required")
		}
		entries, err := fmp4.Children(sd[8:])
		if err != nil || len(entries) != 1 {
			return nil, errors.New("invalid sample description")
		}
		x.tracks[id] = &track{meta: meta, raw: append([]byte(nil), raw...), stsd: append([]byte(nil), stsd.Whole(stbl)...)}
	}
	for _, h := range moofs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, err := readMetadata(s.Reader, h, budget)
		if err != nil {
			return nil, err
		}
		f, err := parseMoof(b, h.off, x.init, budget)
		if err != nil {
			return nil, err
		}
		for _, t := range f.trafs {
			for _, sm := range t.samples {
				if sm.duration == 0 || sm.size == 0 {
					return nil, errors.New("zero-size or zero-duration sample")
				}
				pos := sortExtent(mdats, sm.off)
				if pos < 0 || sm.off < mdats[pos].off+int64(mdats[pos].hdr) || int64(sm.size) > mdats[pos].end-sm.off {
					return nil, errors.New("sample outside mdat payload")
				}
			}
		}
		x.frags = append(x.frags, f)
	}
	for _, f := range x.frags {
		if !f.key {
			continue
		}
		for _, t := range f.trafs {
			if len(t.samples) > 0 && (init.Video() == nil || t.id == init.Video().ID) {
				x.anchor = t.time
				x.scale = x.tracks[t.id].meta.Timescale
				x.zero = s.Start.Add(-ticksDuration(x.anchor, x.scale))
				return x, nil
			}
		}
	}
	return nil, errors.New("no indexed sync fragment")
}
func sortExtent(xs []extent, off int64) int {
	lo, hi := 0, len(xs)
	for lo < hi {
		m := (lo + hi) / 2
		if xs[m].off <= off {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo - 1
}
func childPayload(b []byte, typ string) ([]byte, error) {
	xs, err := fmp4.Children(b)
	if err != nil {
		return nil, err
	}
	for _, x := range xs {
		if x.Type == typ {
			return x.Payload(b), nil
		}
	}
	return nil, fmt.Errorf("missing %s", typ)
}

type cursor struct {
	b   []byte
	off int
	err error
}

func (c *cursor) u32() uint32 {
	if c.err != nil {
		return 0
	}
	if len(c.b)-c.off < 4 {
		c.err = io.ErrUnexpectedEOF
		return 0
	}
	v := binary.BigEndian.Uint32(c.b[c.off:])
	c.off += 4
	return v
}
func (c *cursor) u64() uint64 { a, b := c.u32(), c.u32(); return uint64(a)<<32 | uint64(b) }

func parseMoof(b []byte, moofOff int64, init *fmp4.Init, budget *metadataBudget) (fragment, error) {
	var f fragment
	moof, ok := fmp4.Find(b, "moof")
	if !ok {
		return f, errors.New("no moof")
	}
	p := moof.Payload(b)
	boxes, err := fmp4.Children(p)
	if err != nil {
		return f, err
	}
	prevEnd := moofOff
	for _, tb := range boxes {
		if tb.Type != "traf" {
			continue
		}
		q := tb.Payload(p)
		fields, err := fmp4.Children(q)
		if err != nil {
			return f, err
		}
		var id, flags, defDur, defSize, defFlags uint32
		var base, decode int64
		var haveTfhd, haveTfdt, baseSet bool
		var truns [][]byte
		for _, z := range fields {
			r := cursor{b: z.Payload(q)}
			switch z.Type {
			case "tfhd":
				if haveTfhd {
					return f, errors.New("duplicate tfhd")
				}
				haveTfhd = true
				full := r.u32()
				if full>>24 != 0 {
					return f, errors.New("unsupported tfhd version")
				}
				flags = full & 0xffffff
				id = r.u32()
				meta := init.Track(id)
				if meta == nil {
					return f, fmt.Errorf("unknown track %d", id)
				}
				defDur, defSize, defFlags = meta.DefaultDuration, meta.DefaultSize, meta.DefaultFlags
				if flags&1 != 0 {
					u := r.u64()
					if u > math.MaxInt64 {
						return f, errors.New("base data offset overflow")
					}
					base = int64(u)
					baseSet = true
				}
				if flags&2 != 0 && r.u32() != 1 {
					return f, errors.New("sample description changed")
				}
				if flags&8 != 0 {
					defDur = r.u32()
				}
				if flags&16 != 0 {
					defSize = r.u32()
				}
				if flags&32 != 0 {
					defFlags = r.u32()
				}
			case "tfdt":
				if haveTfdt {
					return f, errors.New("duplicate tfdt")
				}
				haveTfdt = true
				v := r.u32() >> 24
				if v == 1 {
					u := r.u64()
					if u > math.MaxInt64 {
						return f, errors.New("decode time overflow")
					}
					decode = int64(u)
				} else if v == 0 {
					decode = int64(r.u32())
				} else {
					return f, errors.New("unsupported tfdt version")
				}
			case "trun":
				truns = append(truns, z.Payload(q))
			}
			if r.err != nil {
				return f, r.err
			}
		}
		if !haveTfhd || !haveTfdt {
			return f, errors.New("traf needs tfhd and tfdt")
		}
		// time.Duration is also used to place the session on the wall clock.
		maxTicks := int64(math.MaxInt64)
		if init.Track(id).Timescale <= 1_000_000_000 {
			maxTicks = durationTicks(math.MaxInt64, init.Track(id).Timescale)
		}
		if decode > maxTicks {
			return f, errors.New("decode time exceeds supported clock range")
		}
		if !baseSet {
			if flags&0x20000 != 0 {
				base = moofOff
			} else {
				base = prevEnd
			}
		}
		tr := traf{id: id, time: decode}
		next := base
		for _, rp := range truns {
			r := cursor{b: rp}
			full := r.u32()
			version := full >> 24
			tf := full & 0xffffff
			if version > 1 {
				return f, errors.New("unsupported trun version")
			}
			count := r.u32()
			pos := next
			if tf&1 != 0 {
				d := int64(int32(r.u32()))
				if d > 0 && base > math.MaxInt64-d {
					return f, errors.New("sample offset overflow")
				}
				pos = base + d
			}
			first := defFlags
			if tf&4 != 0 {
				first = r.u32()
			}
			fields := 0
			for _, flag := range []uint32{0x100, 0x200, 0x400, 0x800} {
				if tf&flag != 0 {
					fields++
				}
			}
			if r.err != nil {
				return f, r.err
			}
			// Even all-default runs need a finite, reasonable metadata representation.
			if uint64(count) > uint64(maxMetadataBox/24) || uint64(count)*uint64(fields)*4 > uint64(len(rp)-r.off) {
				return f, errors.New("trun sample count exceeds metadata")
			}
			if uint64(count) > uint64(MaxSamples-budget.samples) {
				return f, fmt.Errorf("more than %d samples; select a shorter range", MaxSamples)
			}
			budget.samples += int(count)
			for i := uint32(0); i < count; i++ {
				dur, size, sflags := defDur, defSize, defFlags
				if tf&0x100 != 0 {
					dur = r.u32()
				}
				if tf&0x200 != 0 {
					size = r.u32()
				}
				if tf&0x400 != 0 {
					sflags = r.u32()
				} else if i == 0 && tf&4 != 0 {
					sflags = first
				}
				var cts int32
				if tf&0x800 != 0 {
					u := r.u32()
					if version == 0 && u > math.MaxInt32 {
						return f, errors.New("composition offset exceeds signed range")
					}
					cts = int32(u)
				}
				if pos < 0 || pos > math.MaxInt64-int64(size) {
					return f, errors.New("sample byte offset overflow")
				}
				if decode > maxTicks-int64(dur) {
					return f, errors.New("decode time overflow")
				}
				tr.samples = append(tr.samples, inputSample{sample: sample{duration: dur, size: size, cts: cts, sync: sflags&0x10000 == 0}, off: pos})
				pos += int64(size)
				decode += int64(dur)
			}
			if r.err != nil || r.off != len(rp) {
				return f, errors.New("invalid trun length")
			}
			next = pos
		}
		prevEnd = next
		if len(tr.samples) > 0 && (init.Video() == nil || id == init.Video().ID) && tr.samples[0].sync {
			f.key = true
		}
		f.trafs = append(f.trafs, tr)
	}
	if len(f.trafs) == 0 {
		return f, errors.New("moof has no traf")
	}
	return f, nil
}
