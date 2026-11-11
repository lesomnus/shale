package mediaprobe

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/samplebuilder"
)

// Reset at each fragmented NAL start so a dropped old FU cannot poison a new
// NAL. Completeness is checked separately against all original RTP headers.
type depacketizer struct{ codecs.H264Packet }

func (d *depacketizer) Unmarshal(p []byte) ([]byte, error) {
	if len(p) > 1 && p[0]&31 == 28 && p[1]&0x80 != 0 {
		d.H264Packet = codecs.H264Packet{}
	}
	return d.H264Packet.Unmarshal(p)
}

type packetFacts struct {
	timestamp  uint32
	marker     bool
	kind       byte
	nal        byte
	start, end bool
	at         time.Time
	hash       [32]byte
}
type unitBudget struct{ packets, nals, bytes int }
type captured struct {
	data     []byte
	evidence Evidence
}
type receiver struct {
	options          Options
	builder          *samplebuilder.SampleBuilder
	facts            map[uint16]packetFacts
	budgets          map[uint32]unitBudget
	packets, payload int
	chain            chain
}

func newReceiver(o Options) *receiver {
	return &receiver{options: o, builder: samplebuilder.New(4096, &depacketizer{}, 90000, samplebuilder.WithMaxTimeDelay(250*time.Millisecond), samplebuilder.WithRTPHeaders(true)), facts: map[uint16]packetFacts{}, budgets: map[uint32]unitBudget{}, chain: chain{target: o.Frames, limit: o.MaxBytes}}
}

// Inspect before the library parser: Pion accepts empty STAP entries and ignores
// trailing bytes. Reject those and bound NAL count before any reassembly grows.
func inspectPayload(p []byte) (packetFacts, int, error) {
	if len(p) < 2 || p[0]&0x80 != 0 {
		return packetFacts{}, 0, ErrMedia
	}
	f := packetFacts{kind: p[0] & 31}
	switch {
	case f.kind > 0 && f.kind < 24:
		return f, 1, nil
	case f.kind == 24:
		count := 0
		for off := 1; off < len(p); {
			if off+2 > len(p) {
				return f, 0, ErrMedia
			}
			n := int(binary.BigEndian.Uint16(p[off:]))
			off += 2
			if n == 0 || n > len(p)-off || p[off]&0x80 != 0 || p[off]&31 == 0 || p[off]&31 >= 24 {
				return f, 0, ErrMedia
			}
			count++
			off += n
			if count > 1024 {
				return f, 0, ErrLimit
			}
		}
		if count == 0 {
			return f, 0, ErrMedia
		}
		return f, count, nil
	case f.kind == 28:
		if len(p) < 3 || p[1]&31 == 0 || p[1]&31 >= 24 || p[1]&0x20 != 0 {
			return f, 0, ErrMedia
		}
		f.start = p[1]&0x80 != 0
		f.end = p[1]&0x40 != 0
		f.nal = p[0]&0xe0 | p[1]&31
		if f.start && f.end {
			return f, 0, ErrMedia
		}
		if f.start {
			return f, 1, nil
		}
		return f, 0, nil
	default:
		return f, 0, ErrMedia
	}
}

func (r *receiver) push(p *rtp.Packet, at time.Time) (*captured, error) {
	r.packets++
	r.payload += len(p.Payload)
	if r.packets > r.options.MaxPackets || r.payload > r.options.MaxBytes {
		return nil, ErrLimit
	}
	f, nals, err := inspectPayload(p.Payload)
	if err != nil {
		return nil, err
	}
	f.timestamp = p.Timestamp
	f.marker = p.Marker
	f.at = at
	f.hash = sha256.Sum256(p.Payload)
	if p.Marker && f.kind == 28 && !f.end {
		return nil, ErrMedia
	}
	if old, ok := r.facts[p.SequenceNumber]; ok {
		if old.timestamp != f.timestamp || old.marker != f.marker || old.hash != f.hash {
			return nil, ErrMedia
		}
		return nil, nil
	}
	budget := r.budgets[p.Timestamp]
	budget.packets++
	budget.nals += nals
	budget.bytes += len(p.Payload)
	if budget.packets > 4096 || budget.nals > 1024 || budget.bytes > r.options.MaxBytes {
		return nil, ErrLimit
	}
	r.budgets[p.Timestamp] = budget
	r.facts[p.SequenceNumber] = f
	r.builder.Push(p)
	for sample := r.builder.Pop(); sample != nil; sample = r.builder.Pop() {
		complete, received := r.complete(sample)
		if !complete || sample.PrevDroppedPackets > 0 {
			r.chain.reset()
		}
		if !complete {
			continue
		}
		ready, err := r.chain.add(sample.Data, sample.PacketTimestamp, received)
		if err != nil {
			return nil, err
		}
		if ready {
			r.chain.evidence.PayloadBytes = r.payload
			return &captured{data: r.chain.data, evidence: r.chain.evidence}, nil
		}
	}
	return nil, nil
}

func (r *receiver) complete(s *media.Sample) (bool, time.Time) {
	if len(s.RTPHeaders) == 0 || !s.RTPHeaders[len(s.RTPHeaders)-1].Marker {
		return false, time.Time{}
	}
	var at time.Time
	open := false
	var fragmentedNAL byte
	var sequence uint16
	for i, h := range s.RTPHeaders {
		f, ok := r.facts[h.SequenceNumber]
		if !ok || h.Timestamp != s.PacketTimestamp || f.timestamp != s.PacketTimestamp || f.marker != h.Marker || (i > 0 && h.SequenceNumber != sequence+1) {
			return false, at
		}
		sequence = h.SequenceNumber
		if f.at.After(at) {
			at = f.at
		}
		if f.kind == 28 {
			if f.start {
				if open {
					return false, at
				}
				open = true
				fragmentedNAL = f.nal
			} else if !open {
				return false, at
			}
			if f.nal != fragmentedNAL {
				return false, at
			}
			if f.end {
				open = false
			}
		} else if open {
			return false, at
		}
	}
	return !open, at
}

type chain struct {
	target, limit  int
	data, sps, pps []byte
	evidence       Evidence
}

func (c *chain) reset() { c.data = nil; c.sps = nil; c.pps = nil; c.evidence = Evidence{} }
func (c *chain) add(data []byte, ts uint32, at time.Time) (bool, error) {
	if c.evidence.AccessUnits > 0 && int32(ts-c.evidence.LastRTP) <= 0 {
		c.reset()
	}
	nals, err := annexBNALs(data)
	if err != nil {
		return false, err
	}
	key, vcl, hasSPS, hasPPS := false, false, false, false
	for _, n := range nals {
		switch n[0] & 31 {
		case 7:
			if len(n) > 64<<10 {
				return false, ErrLimit
			}
			c.sps = bytes.Clone(n)
			hasSPS = true
		case 8:
			if len(n) > 64<<10 {
				return false, ErrLimit
			}
			c.pps = bytes.Clone(n)
			hasPPS = true
		case 5:
			key = true
			vcl = true
		case 1, 2, 3, 4:
			vcl = true
		}
	}
	if !vcl {
		return false, nil
	}
	if c.evidence.AccessUnits == 0 {
		if !key || len(c.sps) == 0 || len(c.pps) == 0 {
			return false, nil
		}
		if !hasSPS || !hasPPS {
			for _, n := range [][]byte{c.sps, c.pps} {
				if len(c.data)+len(n)+4 > c.limit {
					return false, ErrLimit
				}
				c.data = append(c.data, 0, 0, 0, 1)
				c.data = append(c.data, n...)
			}
		}
		c.evidence.FirstRTP = ts
		c.evidence.FirstReceivedAt = at
		c.evidence.RTPClockRate = 90000
	} else {
		c.evidence.RTPAdvances++
	}
	if len(data) > c.limit-len(c.data) {
		return false, ErrLimit
	}
	c.data = append(c.data, data...)
	c.evidence.AccessUnits++
	c.evidence.LastRTP = ts
	c.evidence.LastReceivedAt = at
	c.evidence.EncodedBytes = len(c.data)
	return c.evidence.AccessUnits >= c.target, nil
}

// The depacketizer emits four-byte Annex-B starts. Parse bounded views, without
// allocating/copying each NAL; empty or unsupported units cannot qualify.
func annexBNALs(data []byte) ([][]byte, error) {
	marker := []byte{0, 0, 0, 1}
	if !bytes.HasPrefix(data, marker) {
		return nil, ErrMedia
	}
	var out [][]byte
	for len(data) > 0 {
		data = data[4:]
		next := bytes.Index(data, marker)
		if next < 0 {
			next = len(data)
		}
		n := data[:next]
		if len(n) == 0 || n[0]&0x80 != 0 || n[0]&31 == 0 || n[0]&31 >= 24 {
			return nil, ErrMedia
		}
		out = append(out, n)
		if len(out) > 1024 {
			return nil, ErrLimit
		}
		data = data[next:]
	}
	return out, nil
}
