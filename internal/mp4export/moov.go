package mp4export

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/lesomnus/shale/internal/fmp4"
)

func box(typ string, payload ...[]byte) []byte {
	n := 8
	for _, b := range payload {
		n += len(b)
	}
	out := make([]byte, 8, n)
	binary.BigEndian.PutUint32(out, uint32(n))
	copy(out[4:], typ)
	for _, b := range payload {
		out = append(out, b...)
	}
	return out
}
func full(typ string, version byte, payload ...[]byte) []byte {
	return box(typ, append([][]byte{{version, 0, 0, 0}}, payload...)...)
}
func u32s(xs ...uint32) []byte {
	b := make([]byte, 0, 4*len(xs))
	for _, x := range xs {
		b = binary.BigEndian.AppendUint32(b, x)
	}
	return b
}
func movieTicks(t int64, scale uint32) int64 {
	return t/int64(scale)*movieScale + int64(math.Round(float64(t%int64(scale))*movieScale/float64(scale)))
}

type edit struct {
	duration uint64
	media    int64
}

// trackEdits uses an optional leading empty edit and its following media edit.
// Interior empty edits are inconsistently handled by common MP4 demuxers. Gaps
// instead live in stts as the interval until the next sample's decode time:
// video holds its previous frame and coded audio resumes at the later timestamp.
func trackEdits(t *track) ([]edit, int64, int64, error) {
	if len(t.samples) == 0 {
		return nil, 0, 0, ErrEmpty
	}
	first := t.samples[0].time
	var decodeEnd, presentationEnd int64
	for i, s := range t.samples {
		end := s.time + int64(s.duration)
		decodeEnd = max(decodeEnd, end)
		presentationEnd = max(presentationEnd, end+int64(s.cts))
		if i+1 < len(t.samples) && t.samples[i+1].time-s.time > math.MaxUint32 {
			return nil, 0, 0, errors.New("mp4export: gap exceeds the MP4 sample-duration field; select separate ranges around the gap")
		}
	}
	end := movieTicks(max(decodeEnd, presentationEnd), t.meta.Timescale)
	lead := movieTicks(first, t.meta.Timescale)
	if first < 0 || end <= lead {
		return nil, 0, 0, errors.New("mp4export: invalid track duration")
	}
	var out []edit
	if lead > 0 {
		out = append(out, edit{duration: uint64(lead), media: -1})
		out = append(out, edit{duration: uint64(end - lead), media: 0})
	}
	return out, decodeEnd - first, end, nil
}
func makeElst(edits []edit) []byte {
	p := u32s(uint32(len(edits)))
	for _, e := range edits {
		p = binary.BigEndian.AppendUint64(p, e.duration)
		p = binary.BigEndian.AppendUint64(p, uint64(e.media))
		p = append(p, 0, 1, 0, 0)
	}
	return box("edts", full("elst", 1, p))
}

func (p *Plan) makeMoov(raw []byte) ([]byte, error) {
	moov, ok := fmp4.Find(raw, "moov")
	if !ok {
		return nil, errors.New("mp4export: no moov")
	}
	mp := moov.Payload(raw)
	children, err := fmp4.Children(mp)
	if err != nil {
		return nil, err
	}
	tracks := make(map[uint32][]byte)
	var duration int64
	creation := p.start.Unix() + epoch1904
	if creation < 0 {
		return nil, errors.New("mp4export: creation time before 1904")
	}
	for _, t := range p.tracks {
		edits, media, wall, err := trackEdits(t)
		if err != nil {
			return nil, err
		}
		duration = max(duration, wall)
		b, err := p.makeTrack(t, uint64(creation), uint64(media), uint64(wall), edits)
		if err != nil {
			return nil, err
		}
		tracks[t.meta.ID] = b
	}
	var out [][]byte
	haveMvhd := false
	for _, b := range children {
		switch b.Type {
		case "mvex": // A normal MP4 has sample tables, not fragment defaults.
		case "mvhd":
			haveMvhd = true
			payload := b.Payload(mp)
			v1, err := durationBox(payload, "mvhd", uint64(creation), uint64(duration), movieScale)
			if err != nil {
				return nil, err
			}
			out = append(out, box("mvhd", v1))
		case "trak":
			tkhd, err := childPayload(b.Payload(mp), "tkhd")
			if err != nil {
				return nil, err
			}
			off := 12
			if tkhd[0] == 1 {
				off = 20
			}
			id := binary.BigEndian.Uint32(tkhd[off:])
			out = append(out, tracks[id])
		default:
			out = append(out, b.Whole(mp))
		}
	}
	if duration <= 0 {
		return nil, ErrEmpty
	}
	if !haveMvhd {
		return nil, errors.New("mp4export: no movie header")
	}
	if duration > math.MaxInt64/int64(time.Microsecond) {
		return nil, errors.New("mp4export: movie duration exceeds supported clock range")
	}
	p.duration = time.Duration(duration) * time.Microsecond
	return box("moov", out...), nil
}
func (p *Plan) makeTrack(t *track, creation, media, wall uint64, edits []edit) ([]byte, error) {
	tr, _ := fmp4.Find(t.raw, "trak")
	payload := tr.Payload(t.raw)
	children, err := fmp4.Children(payload)
	if err != nil {
		return nil, err
	}
	var out [][]byte
	foundEdts := false
	for _, b := range children {
		switch b.Type {
		case "tkhd":
			v, err := durationBox(b.Payload(payload), "tkhd", creation, wall, 0)
			if err != nil {
				return nil, err
			}
			out = append(out, box("tkhd", v))
		case "edts":
			if !foundEdts {
				out = append(out, makeElst(edits))
				foundEdts = true
			}
		case "mdia":
			mdia, err := p.makeMdia(t, b.Payload(payload), creation, media)
			if err != nil {
				return nil, err
			}
			out = append(out, mdia)
		default:
			out = append(out, b.Whole(payload))
		}
	}
	if !foundEdts && len(edits) > 0 {
		out = append(out, makeElst(edits))
	}
	return box("trak", out...), nil
}
func (p *Plan) makeMdia(t *track, payload []byte, creation, media uint64) ([]byte, error) {
	children, err := fmp4.Children(payload)
	if err != nil {
		return nil, err
	}
	var out [][]byte
	for _, b := range children {
		switch b.Type {
		case "mdhd":
			v, err := durationBox(b.Payload(payload), "mdhd", creation, media, t.meta.Timescale)
			if err != nil {
				return nil, err
			}
			out = append(out, box("mdhd", v))
		case "minf":
			inner := b.Payload(payload)
			fields, err := fmp4.Children(inner)
			if err != nil {
				return nil, err
			}
			var minf [][]byte
			for _, f := range fields {
				if f.Type == "stbl" {
					minf = append(minf, p.sampleTable(t))
				} else {
					minf = append(minf, f.Whole(inner))
				}
			}
			out = append(out, box("minf", minf...))
		default:
			out = append(out, b.Whole(payload))
		}
	}
	return box("mdia", out...), nil
}

// durationBox upgrades mvhd/tkhd/mdhd to version one, keeping their remaining
// fields (matrix, dimensions, language, flags, next track ID) byte for byte.
func durationBox(src []byte, typ string, creation, duration uint64, scale uint32) ([]byte, error) {
	if len(src) < 4 || src[0] > 1 {
		return nil, fmt.Errorf("mp4export: invalid %s version", typ)
	}
	oldTail := 20
	if src[0] == 1 {
		oldTail = 32
	}
	if typ == "tkhd" {
		oldTail = 24
		if src[0] == 1 {
			oldTail = 36
		}
	}
	if len(src) < oldTail {
		return nil, fmt.Errorf("mp4export: short %s", typ)
	}
	minimum := map[string]int{"mvhd": 100, "tkhd": 84, "mdhd": 24}[typ]
	if src[0] == 1 {
		minimum += 12
	}
	if len(src) < minimum {
		return nil, fmt.Errorf("mp4export: short %s", typ)
	}
	out := append([]byte(nil), src[:4]...)
	out[0] = 1
	out = binary.BigEndian.AppendUint64(out, creation)
	out = binary.BigEndian.AppendUint64(out, creation)
	if typ == "tkhd" {
		idoff := 12
		if src[0] == 1 {
			idoff = 20
		}
		out = append(out, src[idoff:idoff+8]...)
	} else {
		out = binary.BigEndian.AppendUint32(out, scale)
	}
	out = binary.BigEndian.AppendUint64(out, duration)
	return append(out, src[oldTail:]...), nil
}

func (p *Plan) sampleTable(t *track) []byte {
	var boxes [][]byte
	boxes = append(boxes, t.stsd)
	// Time-to-sample and composition offsets are run-length encoded.
	type run struct{ count, value uint32 }
	var times, ctts []run
	var haveCTS bool
	add := func(xs []run, v uint32) []run {
		if len(xs) > 0 && xs[len(xs)-1].value == v {
			xs[len(xs)-1].count++
		} else {
			xs = append(xs, run{1, v})
		}
		return xs
	}
	for i, s := range t.samples {
		duration := s.duration
		if i+1 < len(t.samples) {
			duration = uint32(t.samples[i+1].time - s.time)
		}
		times = add(times, duration)
		ctts = add(ctts, uint32(s.cts))
		haveCTS = haveCTS || s.cts != 0
	}
	encode := func(xs []run) []byte {
		b := u32s(uint32(len(xs)))
		for _, r := range xs {
			b = append(b, u32s(r.count, r.value)...)
		}
		return b
	}
	boxes = append(boxes, full("stts", 0, encode(times)))
	if haveCTS {
		boxes = append(boxes, full("ctts", 1, encode(ctts)))
	}
	var sc []uint32
	var previous uint32
	for i, ci := range t.chunks {
		n := p.chunks[ci].count
		if i == 0 || n != previous {
			sc = append(sc, uint32(i+1), n, 1)
			previous = n
		}
	}
	boxes = append(boxes, full("stsc", 0, u32s(uint32(len(sc)/3)), u32s(sc...)))
	sizes := u32s(0, uint32(len(t.samples)))
	for _, s := range t.samples {
		sizes = binary.BigEndian.AppendUint32(sizes, s.size)
	}
	boxes = append(boxes, full("stsz", 0, sizes))
	offsets := u32s(uint32(len(t.chunks)))
	for _, ci := range t.chunks {
		offsets = binary.BigEndian.AppendUint64(offsets, uint64(p.chunks[ci].out))
	}
	boxes = append(boxes, full("co64", 0, offsets))
	if t.meta.Video() {
		var sync []uint32
		for i, s := range t.samples {
			if s.sync {
				sync = append(sync, uint32(i+1))
			}
		}
		boxes = append(boxes, full("stss", 0, u32s(uint32(len(sync))), u32s(sync...)))
	}
	return box("stbl", boxes...)
}
