// Package producer is the host program that turns cameras into laminae
// (§38): it reads MPEG-TS from capture processes, cuts segments at keyframes
// on a staggered schedule, and uploads them with the allocations the
// Control Plane hands it. It never decodes or encodes; it reads packet
// headers and nothing inside them (§38.1).
package producer

import (
	"bufio"
	"errors"
	"io"
)

// PacketSize is an MPEG-TS packet.
const PacketSize = 188

const syncByte = 0x47

// Packet is what the reader says about one packet: enough to find the
// tables and the keyframes, nothing about the payload.
type Packet struct {
	Data [PacketSize]byte
	PID  uint16
	// PUSI is payload_unit_start_indicator: the start of a PES packet or a
	// table section.
	PUSI bool
	// RAI is random_access_indicator in the adaptation field: set on the
	// packet that starts a keyframe by every standard muxer.
	RAI bool
	// Payload is where the payload starts, or PacketSize when there is none.
	Payload int
}

// Streams is what the tables say: the PMT's PID and the elementary streams.
type Streams struct {
	PmtPID   uint16
	VideoPID uint16
	// Video is the stream type: 0x1B H.264, 0x24 H.265, 0x02 MPEG-2,
	// 0x10 MPEG-4.
	Video     byte
	AudioPIDs []uint16
	// AudioTypes are the stream types of AudioPIDs, in order: 0x0F AAC,
	// 0x11 AAC in LATM, 0x03/0x04 MPEG audio, 0x81/0x87 AC-3/E-AC-3, 0x06 a
	// private stream its descriptors name.
	AudioTypes []byte
	// AudioOpus says an audio stream is Opus: a private stream with a
	// registration descriptor saying so, as ffmpeg writes it (§39.4).
	AudioOpus bool
	// AudioAnon says an audio stream is a private stream no descriptor
	// names, which is what ffmpeg writes for a codec TS has no type for,
	// G.711 copied from a camera being the usual case: no player finds it
	// (§38.3).
	AudioAnon bool
	// PAT and PMT are the last table packets seen, prepended to every
	// segment so it plays on its own (§38.2).
	PAT []byte
	PMT []byte
}

// HasAudio says whether the tables named an audio stream at all.
func (s Streams) HasAudio() bool { return len(s.AudioPIDs) > 0 }

// Reader reads packets and keeps the tables.
type Reader struct {
	r      *bufio.Reader
	s      Streams
	synced bool
}

// NewReader reads TS from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 64*PacketSize)}
}

// Streams is what the tables have said so far.
func (r *Reader) Streams() Streams { return r.s }

// ErrSync is a stream that lost sync and could not find it again.
var ErrSync = errors.New("ts: cannot find sync")

// Next reads one packet, resyncing on the sync byte when needed.
func (r *Reader) Next(p *Packet) error {
	for tries := 0; tries < 4*PacketSize; tries++ {
		b, err := r.r.Peek(1)
		if err != nil {
			return err
		}
		if b[0] != syncByte {
			r.r.ReadByte()
			r.synced = false
			continue
		}
		if !r.synced {
			// Two sync bytes a packet apart before trusting the stream.
			if b2, err := r.r.Peek(PacketSize + 1); err == nil && b2[PacketSize] != syncByte {
				r.r.ReadByte()
				continue
			}
			r.synced = true
		}
		if _, err := io.ReadFull(r.r, p.Data[:]); err != nil {
			return err
		}
		r.parse(p)

		return nil
	}

	return ErrSync
}

func (r *Reader) parse(p *Packet) {
	d := p.Data[:]
	p.PID = uint16(d[1]&0x1f)<<8 | uint16(d[2])
	p.PUSI = d[1]&0x40 != 0
	afc := (d[3] >> 4) & 3
	p.RAI = false
	p.Payload = 4
	if afc&2 != 0 {
		afl := int(d[4])
		if afl > 0 && d[5]&0x40 != 0 {
			p.RAI = true
		}
		p.Payload = 5 + afl
	}
	if afc&1 == 0 {
		p.Payload = PacketSize
	}
	if p.Payload > PacketSize {
		p.Payload = PacketSize
	}

	switch {
	case p.PID == 0 && p.PUSI:
		r.parsePAT(p)
	case r.s.PmtPID != 0 && p.PID == r.s.PmtPID && p.PUSI:
		r.parsePMT(p)
	}
}

// section is the table section a packet starts, after the pointer field.
func section(p *Packet) []byte {
	if p.Payload >= PacketSize {
		return nil
	}
	d := p.Data[p.Payload:]
	ptr := int(d[0])
	if 1+ptr >= len(d) {
		return nil
	}
	sec := d[1+ptr:]
	if len(sec) < 3 {
		return nil
	}
	l := int(sec[1]&0x0f)<<8 | int(sec[2])
	if 3+l > len(sec) {
		l = len(sec) - 3
	}

	return sec[:3+l]
}

func (r *Reader) parsePAT(p *Packet) {
	sec := section(p)
	if sec == nil || sec[0] != 0 || len(sec) < 12 {
		return
	}
	body := sec[8 : len(sec)-4]
	for i := 0; i+4 <= len(body); i += 4 {
		num := uint16(body[i])<<8 | uint16(body[i+1])
		pid := uint16(body[i+2]&0x1f)<<8 | uint16(body[i+3])
		if num != 0 {
			r.s.PmtPID = pid
			break
		}
	}
	r.s.PAT = append(r.s.PAT[:0], p.Data[:]...)
}

func (r *Reader) parsePMT(p *Packet) {
	sec := section(p)
	if sec == nil || sec[0] != 2 || len(sec) < 16 {
		return
	}
	pil := int(sec[10]&0x0f)<<8 | int(sec[11])
	es := sec[12+pil : len(sec)-4]
	var video uint16
	var vt byte
	var audio []uint16
	var types []byte
	opus, anon := false, false
	for i := 0; i+5 <= len(es); {
		st := es[i]
		pid := uint16(es[i+1]&0x1f)<<8 | uint16(es[i+2])
		esl := int(es[i+3]&0x0f)<<8 | int(es[i+4])
		desc := es[i+5 : min(i+5+esl, len(es))]
		i += 5 + esl
		switch st {
		case 0x1b, 0x24, 0x02, 0x10:
			if video == 0 {
				video, vt = pid, st
			}
		case 0x0f, 0x11, 0x03, 0x04, 0x81, 0x87, 0x8a:
			audio, types = append(audio, pid), append(types, st)
		case 0x06:
			// A private stream is whatever its descriptors say: Opus by its
			// registration, Dolby or DTS by their DVB descriptors, and with
			// none at all the codec TS has no type for (§38.3). Anything
			// else named (KLV, teletext, subtitles) is not audio.
			switch {
			case hasRegistration(desc, "Opus"):
				audio, types, opus = append(audio, pid), append(types, st), true
			case hasDescriptor(desc, 0x6a, 0x7a, 0x7b):
				audio, types = append(audio, pid), append(types, st)
			case len(desc) == 0:
				audio, types, anon = append(audio, pid), append(types, st), true
			}
		}
	}
	r.s.VideoPID, r.s.Video = video, vt
	r.s.AudioPIDs, r.s.AudioTypes, r.s.AudioOpus, r.s.AudioAnon = audio, types, opus, anon
	r.s.PMT = append(r.s.PMT[:0], p.Data[:]...)
}

// hasRegistration looks for a registration descriptor (tag 5) with the
// format identifier among an elementary stream's descriptors.
func hasRegistration(desc []byte, format string) bool {
	for i := 0; i+2 <= len(desc); {
		tag, n := desc[i], int(desc[i+1])
		if i+2+n > len(desc) {
			return false
		}
		if tag == 0x05 && n >= 4 && string(desc[i+2:i+6]) == format {
			return true
		}
		i += 2 + n
	}

	return false
}

// hasDescriptor says whether any of the tags is among the descriptors.
func hasDescriptor(desc []byte, tags ...byte) bool {
	for i := 0; i+2 <= len(desc); {
		tag, n := desc[i], int(desc[i+1])
		if i+2+n > len(desc) {
			return false
		}
		for _, t := range tags {
			if tag == t {
				return true
			}
		}
		i += 2 + n
	}

	return false
}

// IsKeyframe says whether this packet starts a keyframe of the video
// stream (§38.2): payload_unit_start_indicator with random_access_indicator.
//
// One check inside the payload, and only because a muxer can be wrong about
// the flag: the Raspberry Pi's hardware encoder marks every frame a
// keyframe, so ffmpeg sets RAI on every PES start. The NAL types in the
// first packet settle it: an IDR slice is a keyframe, a non-IDR slice is
// not, and when neither is in the first packet the flag is believed.
func (r *Reader) IsKeyframe(p *Packet) bool {
	if r.s.VideoPID == 0 || p.PID != r.s.VideoPID || !p.PUSI || !p.RAI {
		return false
	}
	switch nalKeyframe(p.Data[p.Payload:], r.s.Video) {
	case 1:
		return true
	case -1:
		return false
	}

	return true
}

// nalKeyframe scans the PES payload of a packet for slice NAL units: 1 for
// a keyframe slice, -1 for a non-keyframe slice, 0 for neither.
func nalKeyframe(pl []byte, streamType byte) int {
	if len(pl) < 9 || pl[0] != 0 || pl[1] != 0 || pl[2] != 1 {
		return 0
	}
	hdl := int(pl[8])
	if 9+hdl > len(pl) {
		return 0
	}
	es := pl[9+hdl:]
	for i := 0; i+3 < len(es); i++ {
		if es[i] != 0 || es[i+1] != 0 || es[i+2] != 1 {
			continue
		}
		h := es[i+3]
		i += 3
		switch streamType {
		case 0x24: // H.265
			t := (h >> 1) & 0x3f
			switch {
			case t >= 16 && t <= 21:
				return 1
			case t <= 9:
				return -1
			}
		default: // H.264
			switch h & 0x1f {
			case 5:
				return 1
			case 1:
				return -1
			}
		}
	}

	return 0
}

// IsVideoFrame says whether this packet starts a video PES packet, which
// counts frames.
func (r *Reader) IsVideoFrame(p *Packet) bool {
	return r.s.VideoPID != 0 && p.PID == r.s.VideoPID && p.PUSI
}

// Tables is PAT then PMT, or nil until both were seen.
func (r *Reader) Tables() []byte {
	if len(r.s.PAT) == 0 || len(r.s.PMT) == 0 {
		return nil
	}
	out := make([]byte, 0, 2*PacketSize)
	out = append(out, r.s.PAT...)
	out = append(out, r.s.PMT...)

	return out
}
