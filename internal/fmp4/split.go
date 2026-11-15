package fmp4

import (
	"encoding/binary"
	"fmt"
)

// Splitter is a Reader turned around: the bytes are written into it as they
// pass, and each unit is handed over the moment its last byte is in. It
// never waits for more and never holds the writer: what a satellite reads
// out of an upload on its way to a node (§41.8) is parsed as it passes, and
// the upload goes on whatever becomes of it.
//
// Units are what a Reader answers: an init segment (`ftyp` and `moov`), or
// a fragment, a `moof` with its `mdat` and any `styp` or `sidx` in front.
// Boxes that say nothing about the media, an `mfra` among them, are passed
// over.
type Splitter struct {
	// Want says whether the fragment whose first box begins at offset `at`
	// of the stream is wanted. One that is not goes by and is kept
	// nowhere, whole. Nil wants them all. Init segments are always kept.
	Want func(at int64) bool
	// Max bounds a box that is kept, MaxBox when zero: a fragment with a
	// larger box in it is skipped, an init segment with one ends the
	// stream.
	Max int64
	// Unit is handed each unit as it completes, in order, on the writer's
	// goroutine.
	Unit func(*Unit)

	// The header of the box being read, as far as it has come.
	hdr  [16]byte
	hdrN int
	// The box being read: its type, the bytes still to come, and whether
	// they are kept, in box.
	typ  string
	left int64
	keep bool
	box  []byte

	init    *Init
	ftyp    []byte
	pending []byte
	// moof waits for the mdat that follows it.
	moof []byte
	err  error
	// n is the bytes written so far.
	n int64
}

// Init is the last init segment seen, or nil.
func (s *Splitter) Init() *Init { return s.init }

// Err is why the stream lost its framing, after which nothing more is
// read from it; nil while it holds.
func (s *Splitter) Err() error { return s.err }

func (s *Splitter) max() int64 {
	if s.Max > 0 {
		return s.Max
	}

	return MaxBox
}

func (s *Splitter) want(at int64) bool { return s.Want == nil || s.Want(at) }

// Offset is how many bytes of the stream were written so far.
func (s *Splitter) Offset() int64 { return s.n }

// Write takes the next bytes of the stream. It answers len(p) and no error
// whatever they hold, so it can sit on any writer's path: a stream that
// lost its framing is told by Err.
func (s *Splitter) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 && s.err == nil {
		if s.left == 0 {
			p = s.header(p)
			continue
		}
		k := int64(len(p))
		if k > s.left {
			k = s.left
		}
		if s.keep {
			s.box = append(s.box, p[:k]...)
		}
		s.n += k
		s.left -= k
		p = p[k:]
		if s.left == 0 {
			s.end()
		}
	}

	return n, nil
}

// header reads as much of a box header as p has, and begins the box once
// it is whole; it answers what of p is left.
func (s *Splitter) header(p []byte) []byte {
	need := 8
	if s.hdrN >= 8 && binary.BigEndian.Uint32(s.hdr[:4]) == 1 {
		need = 16
	}
	k := copy(s.hdr[s.hdrN:need], p)
	s.hdrN += k
	s.n += int64(k)
	p = p[k:]
	if s.hdrN < 8 {
		return p
	}
	if s.hdrN == 8 && binary.BigEndian.Uint32(s.hdr[:4]) == 1 {
		// A 64-bit size follows.
		return p
	}
	typ, hdrLen, size, err := header(s.hdr[:s.hdrN], 0)
	if err != nil {
		s.err = err
		return nil
	}
	if size > MaxBox {
		s.err = fmt.Errorf("fmp4: box %q of %d bytes", typ, size)
		return nil
	}
	s.begin(typ, s.hdr[:hdrLen], size)
	s.hdrN = 0
	if s.left == 0 {
		s.end()
	}

	return p
}

// begin starts a box whose header is in: it is kept, with its header, or
// passed over.
func (s *Splitter) begin(typ string, hdr []byte, size int64) {
	if s.moof != nil && typ != "mdat" {
		// A moof with no mdat after it is a fragment of no samples.
		s.fragment(s.moof)
		s.moof = nil
	}
	s.typ, s.left, s.box = typ, size-int64(len(hdr)), nil
	switch typ {
	case "ftyp", "moov":
		s.keep = true
		if size > s.max() {
			s.err = fmt.Errorf("fmp4: %s of %d bytes", typ, size)
			return
		}
	case "styp", "sidx", "moof":
		s.keep = s.init != nil && size <= s.max() && s.want(s.n-int64(len(hdr)))
		if !s.keep {
			s.pending = nil
		}
	case "mdat":
		s.keep = s.moof != nil && int64(len(s.moof))+size <= s.max()
		if !s.keep {
			s.moof = nil
		}
	default:
		s.keep = false
	}
	if s.keep {
		s.box = make([]byte, 0, size)
		s.box = append(s.box, hdr...)
	}
}

// end finishes the box just read.
func (s *Splitter) end() {
	b, typ := s.box, s.typ
	s.box, s.keep = nil, false
	switch typ {
	case "ftyp":
		s.ftyp = b
	case "moov":
		init, err := ParseInit(append(s.ftyp, b...))
		if err != nil {
			s.err = err
			return
		}
		s.ftyp, s.pending, s.moof = nil, nil, nil
		s.init = init
		if s.Unit != nil {
			s.Unit(&Unit{Init: init})
		}
	case "styp", "sidx":
		if b != nil {
			s.pending = append(s.pending, b...)
		}
	case "moof":
		if b != nil {
			s.moof = append(s.pending, b...)
		}
		s.pending = nil
	case "mdat":
		if b != nil && s.moof != nil {
			s.fragment(append(s.moof, b...))
		}
		s.moof = nil
	}
}

// fragment hands over one fragment, unless it does not parse.
func (s *Splitter) fragment(b []byte) {
	f, err := ParseFragment(b, s.init)
	if err != nil || s.Unit == nil {
		return
	}
	s.Unit(&Unit{Frag: f})
}
