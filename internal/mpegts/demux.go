// Package mpegts takes the video out of an MPEG-TS stream as access units
// with their presentation times, which is what a WebRTC track wants
// (§39.4). It reads the tables to find the video stream, follows the PES
// packets of that stream, and hands over one access unit per PES packet,
// which is how every muxer Shale meets (ffmpeg's included) lays them out.
// Nothing is decoded and nothing is transcoded.
package mpegts

import (
	"errors"
	"time"
)

const (
	// PacketSize is the size of a TS packet.
	PacketSize = 188
	syncByte   = 0x47

	// Codecs by stream type.
	StreamH264 byte = 0x1b
	StreamH265 byte = 0x24
)

// AccessUnit is one video frame's worth of NAL units in Annex B form, with
// its presentation time.
type AccessUnit struct {
	Data []byte
	// PTS is the presentation time stamp, in 90 kHz ticks.
	PTS int64
	// Keyframe says whether the unit starts with a decodable picture (an
	// IDR, or an IRAP for H.265).
	Keyframe bool
}

// AudioUnit is one Opus packet with its presentation time (§39.3): the
// producer recorded Opus, and the relay passes it through.
type AudioUnit struct {
	Data []byte
	PTS  int64
}

// Demuxer turns TS packets into access units of the video stream, and
// Opus packets of the audio stream when there is one.
type Demuxer struct {
	pmtPID   uint16
	videoPID uint16
	codec    byte
	audioPID uint16
	opus     bool

	// The PES packets being assembled.
	buf     []byte
	pts     int64
	open    bool
	pending []AccessUnit

	// params are the last parameter sets the video stream carried, with
	// their start codes: H.264's SPS and PPS, H.265's VPS, SPS and PPS. A
	// keyframe that arrives without them gets them, so a viewer joining at
	// any point decodes from the first keyframe it is handed (§39.4): some
	// encoders write them once and never again (#84).
	params []byte

	abuf     []byte
	apts     int64
	aopen    bool
	apending []AudioUnit
}

// New makes a demuxer that learns the streams from the tables.
func New() *Demuxer { return &Demuxer{} }

// Codec is the video stream type once the PMT was seen: StreamH264 or
// StreamH265, else 0.
func (d *Demuxer) Codec() byte { return d.codec }

// ErrSync is a byte stream that is not TS.
var ErrSync = errors.New("mpegts: no sync byte")

// Write feeds a whole number of TS packets and answers the access units
// that completed. A partial packet is an error.
func (d *Demuxer) Write(b []byte) ([]AccessUnit, error) {
	v, _, err := d.WriteAll(b)

	return v, err
}

// WriteAll is Write with the audio too: the Opus packets that completed.
func (d *Demuxer) WriteAll(b []byte) ([]AccessUnit, []AudioUnit, error) {
	if len(b)%PacketSize != 0 {
		return nil, nil, errors.New("mpegts: not a whole number of packets")
	}
	d.pending = d.pending[:0]
	d.apending = d.apending[:0]
	for i := 0; i+PacketSize <= len(b); i += PacketSize {
		if err := d.packet(b[i : i+PacketSize]); err != nil {
			return nil, nil, err
		}
	}
	out := make([]AccessUnit, len(d.pending))
	copy(out, d.pending)
	var audio []AudioUnit
	if len(d.apending) > 0 {
		audio = make([]AudioUnit, len(d.apending))
		copy(audio, d.apending)
	}

	return out, audio, nil
}

// HasOpus says whether the tables named an Opus audio stream.
func (d *Demuxer) HasOpus() bool { return d.opus }

// Flush closes the access unit being assembled, at the end of a stream.
func (d *Demuxer) Flush() []AccessUnit {
	d.pending = d.pending[:0]
	d.apending = d.apending[:0]
	d.finishAudio()
	d.finish()
	out := make([]AccessUnit, len(d.pending))
	copy(out, d.pending)

	return out
}

func (d *Demuxer) packet(p []byte) error {
	if p[0] != syncByte {
		return ErrSync
	}
	pid := uint16(p[1]&0x1f)<<8 | uint16(p[2])
	pusi := p[1]&0x40 != 0
	afc := (p[3] >> 4) & 3
	payload := 4
	if afc&2 != 0 {
		payload = 5 + int(p[4])
	}
	if afc&1 == 0 || payload >= PacketSize {
		return nil
	}
	body := p[payload:]

	switch {
	case pid == 0 && pusi:
		d.parsePAT(section(body))
	case d.pmtPID != 0 && pid == d.pmtPID && pusi:
		d.parsePMT(section(body))
	case d.videoPID != 0 && pid == d.videoPID:
		d.video(body, pusi)
	case d.audioPID != 0 && pid == d.audioPID:
		d.audio(body, pusi)
	}

	return nil
}

// section is a table section after the pointer field.
func section(body []byte) []byte {
	if len(body) < 1 {
		return nil
	}
	ptr := int(body[0])
	if 1+ptr >= len(body) {
		return nil
	}

	return body[1+ptr:]
}

func (d *Demuxer) parsePAT(s []byte) {
	if len(s) < 8 || s[0] != 0 {
		return
	}
	length := int(s[1]&0x0f)<<8 | int(s[2])
	end := 3 + length - 4 // minus the CRC
	if end > len(s) {
		end = len(s)
	}
	for i := 8; i+4 <= end; i += 4 {
		program := uint16(s[i])<<8 | uint16(s[i+1])
		pid := uint16(s[i+2]&0x1f)<<8 | uint16(s[i+3])
		if program != 0 {
			d.pmtPID = pid
			return
		}
	}
}

func (d *Demuxer) parsePMT(s []byte) {
	if len(s) < 12 || s[0] != 2 {
		return
	}
	length := int(s[1]&0x0f)<<8 | int(s[2])
	end := 3 + length - 4
	if end > len(s) {
		end = len(s)
	}
	infoLen := int(s[10]&0x0f)<<8 | int(s[11])
	i := 12 + infoLen
	var video, audio uint16
	var codec byte
	opus := false
	for i+5 <= end {
		typ := s[i]
		pid := uint16(s[i+1]&0x1f)<<8 | uint16(s[i+2])
		esLen := int(s[i+3]&0x0f)<<8 | int(s[i+4])
		desc := s[i+5 : min(i+5+esLen, end)]
		i += 5 + esLen
		if (typ == StreamH264 || typ == StreamH265) && video == 0 {
			video, codec = pid, typ
		}
		// Opus rides a private stream with a registration descriptor
		// saying so, as ffmpeg writes it.
		if typ == 0x06 && audio == 0 && hasRegistration(desc, "Opus") {
			audio, opus = pid, true
		}
	}
	// The tables may change under a session: the producer's tee switches
	// between the camera's bytes and its live helper's (§38.7), whose PIDs
	// need not agree. A unit being assembled belongs to the old stream.
	if video != d.videoPID || codec != d.codec {
		d.videoPID, d.codec = video, codec
		d.open, d.buf = false, d.buf[:0]
	}
	if audio != d.audioPID || opus != d.opus {
		d.audioPID, d.opus = audio, opus
		d.aopen, d.abuf = false, d.abuf[:0]
	}
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

// audio collects the PES packets of the Opus stream.
func (d *Demuxer) audio(body []byte, pusi bool) {
	if pusi {
		d.finishAudio()
		if len(body) < 9 || body[0] != 0 || body[1] != 0 || body[2] != 1 {
			return
		}
		flags := body[7]
		hdl := int(body[8])
		if 9+hdl > len(body) {
			return
		}
		d.apts = -1
		if flags&0x80 != 0 && hdl >= 5 {
			h := body[9:14]
			d.apts = int64(h[0]&0x0e)<<29 | int64(h[1])<<22 | int64(h[2]&0xfe)<<14 | int64(h[3])<<7 | int64(h[4]>>1)
		}
		d.aopen = true
		d.abuf = append(d.abuf[:0], body[9+hdl:]...)

		return
	}
	if d.aopen {
		d.abuf = append(d.abuf, body...)
	}
}

// finishAudio splits a PES payload into Opus packets: each is preceded by
// a control header, 0x7FE0 with three flag bits, the size in bytes as
// 0xFF continuation bytes plus a last byte, then optional trims and an
// extension, then the packet.
func (d *Demuxer) finishAudio() {
	if !d.aopen {
		return
	}
	d.aopen = false
	b := d.abuf
	pts := d.apts
	for len(b) >= 2 {
		if b[0] != 0x7f || b[1]&0xe0 != 0xe0 {
			break
		}
		startTrim, endTrim, ext := b[1]&0x10 != 0, b[1]&0x08 != 0, b[1]&0x04 != 0
		i := 2
		size := 0
		for i < len(b) && b[i] == 0xff {
			size += 255
			i++
		}
		if i >= len(b) {
			break
		}
		size += int(b[i])
		i++
		if startTrim {
			i += 2
		}
		if endTrim {
			i += 2
		}
		if ext {
			if i >= len(b) {
				break
			}
			i += 1 + int(b[i])
		}
		if i+size > len(b) || size == 0 {
			break
		}
		d.apending = append(d.apending, AudioUnit{Data: append([]byte(nil), b[i:i+size]...), PTS: pts})
		b = b[i+size:]
		// Packets after the first in one PES follow it in time; 20 ms is
		// Opus's common frame, and the next PES stamps again.
		if pts >= 0 {
			pts += 20 * 90
		}
	}
	d.abuf = d.abuf[:0]
}

// video collects the PES packets of the video stream: a packet with the
// unit start closes the previous access unit and opens the next.
func (d *Demuxer) video(body []byte, pusi bool) {
	if pusi {
		d.finish()
		if len(body) < 9 || body[0] != 0 || body[1] != 0 || body[2] != 1 {
			return
		}
		flags := body[7]
		hdl := int(body[8])
		if 9+hdl > len(body) {
			return
		}
		d.pts = -1
		if flags&0x80 != 0 && hdl >= 5 {
			h := body[9:14]
			d.pts = int64(h[0]&0x0e)<<29 | int64(h[1])<<22 | int64(h[2]&0xfe)<<14 | int64(h[3])<<7 | int64(h[4]>>1)
		}
		d.open = true
		d.buf = append(d.buf[:0], body[9+hdl:]...)

		return
	}
	if d.open {
		d.buf = append(d.buf, body...)
	}
}

func (d *Demuxer) finish() {
	if !d.open || len(d.buf) == 0 {
		d.open = false
		return
	}
	key := keyframe(d.buf, d.codec)
	var data []byte
	if ps := ParamSets(d.buf, d.codec); ps != nil {
		d.params = append(d.params[:0], ps...)
	} else if key && len(d.params) > 0 {
		data = WithParams(d.buf, d.params)
	}
	if data == nil {
		data = append([]byte(nil), d.buf...)
	}
	au := AccessUnit{Data: data, PTS: d.pts, Keyframe: key}
	d.pending = append(d.pending, au)
	d.open = false
	d.buf = d.buf[:0]
}

// Params is the last complete set of parameter sets seen, or nil.
func (d *Demuxer) Params() []byte { return d.params }

// SetParams hands a new demuxer what an earlier one saw, for a stream that
// continues from where the last one left off.
func (d *Demuxer) SetParams(ps []byte) { d.params = append([]byte(nil), ps...) }

// NALUnits splits Annex B bytes into NAL units, each with the start code
// in front of it, so that joining them gives the bytes back.
func NALUnits(es []byte) [][]byte {
	var out [][]byte
	start := -1
	for i := 0; i+2 < len(es); i++ {
		if es[i] != 0 || es[i+1] != 0 || es[i+2] != 1 {
			continue
		}
		s := i
		if s > 0 && es[s-1] == 0 {
			s--
		}
		if start >= 0 && s > start {
			out = append(out, es[start:s])
		}
		start = s
		i += 2
	}
	if start >= 0 {
		out = append(out, es[start:])
	}

	return out
}

// NALType is the type of a NAL unit with its start code in front: the
// five low bits of the header for H.264, the six after the first for
// H.265; -1 for a unit too short to have one.
func NALType(nal []byte, codec byte) int {
	i := 0
	for i+2 < len(nal) && !(nal[i] == 0 && nal[i+1] == 0 && nal[i+2] == 1) {
		i++
	}
	i += 3
	if i >= len(nal) {
		return -1
	}
	if codec == StreamH265 {
		return int(nal[i]>>1) & 0x3f
	}

	return int(nal[i] & 0x1f)
}

// isParam says whether a NAL type is a parameter set of the codec.
func isParam(t int, codec byte) bool {
	if codec == StreamH265 {
		return t >= 32 && t <= 34
	}

	return t == 7 || t == 8
}

// isAUD says whether a NAL type is an access unit delimiter.
func isAUD(t int, codec byte) bool {
	if codec == StreamH265 {
		return t == 35
	}

	return t == 9
}

// ParamSets is the parameter sets an access unit carries, with their
// start codes, or nil unless it carries the whole set the codec needs
// (SPS and PPS; VPS, SPS and PPS for H.265): half of one is no use in
// front of a keyframe.
func ParamSets(es []byte, codec byte) []byte {
	var out []byte
	seen := map[int]bool{}
	for _, nal := range NALUnits(es) {
		t := NALType(nal, codec)
		if !isParam(t, codec) {
			continue
		}
		seen[t] = true
		out = append(out, nal...)
	}
	need := []int{7, 8}
	if codec == StreamH265 {
		need = []int{32, 33, 34}
	}
	for _, t := range need {
		if !seen[t] {
			return nil
		}
	}

	return out
}

// WithParams is an access unit with parameter sets put in front of its
// first picture: after its access unit delimiter when it has one, so the
// unit still begins the way it did.
func WithParams(es, params []byte, codec ...byte) []byte {
	c := byte(StreamH264)
	if len(codec) > 0 {
		c = codec[0]
	}
	units := NALUnits(es)
	out := make([]byte, 0, len(es)+len(params))
	if len(units) > 0 && isAUD(NALType(units[0], c), c) {
		out = append(out, units[0]...)
		out = append(out, params...)
		out = append(out, es[len(units[0]):]...)

		return out
	}
	out = append(out, params...)
	out = append(out, es...)

	return out
}

// keyframe scans the NAL units of an access unit for a decodable picture.
func keyframe(es []byte, codec byte) bool {
	for i := 0; i+3 < len(es); i++ {
		if es[i] != 0 || es[i+1] != 0 || es[i+2] != 1 {
			continue
		}
		h := es[i+3]
		i += 3
		switch codec {
		case StreamH265:
			t := (h >> 1) & 0x3f
			if t >= 16 && t <= 21 {
				return true
			}
			if t <= 9 {
				return false
			}
		default:
			switch h & 0x1f {
			case 5:
				return true
			case 1:
				return false
			}
		}
	}

	return false
}

// Duration is the time between two presentation stamps, or `fallback`
// when either is unknown or the clock went backwards.
func Duration(prev, cur int64, fallback time.Duration) time.Duration {
	if prev < 0 || cur < 0 || cur <= prev {
		return fallback
	}
	d := time.Duration(cur-prev) * time.Second / 90000
	if d > 2*time.Second {
		return fallback
	}

	return d
}
