package fmp4

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// Unit is what a Reader hands over: an init segment or a fragment.
type Unit struct {
	Init *Init
	Frag *Fragment
}

// Bytes is the unit as it came.
func (u *Unit) Bytes() []byte {
	if u.Init != nil {
		return u.Init.Bytes
	}

	return u.Frag.Bytes
}

// MaxBox bounds a box a stream may carry; a size above it is a stream that
// lost its framing, not a box.
const MaxBox = 64 << 20

// Reader reads a stream of fragmented MP4: an init segment, then
// fragments. Boxes that say nothing about the media (`free`, `mfra`, what
// ffmpeg appends when it exits, and so on) are dropped; `styp` and `sidx`
// stay in front of the fragment they precede.
type Reader struct {
	r    *bufio.Reader
	init *Init
	// pending is what precedes the next fragment: a styp, a sidx.
	pending []byte
	// ftyp waits for its moov.
	ftyp []byte
}

// NewReader reads from r.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 256<<10)}
}

// Init is the last init segment seen, or nil.
func (r *Reader) Init() *Init { return r.init }

var errNoInit = errors.New("fmp4: a fragment before any init segment")

// Next reads the next unit. It answers io.EOF at a clean end, between
// boxes, and io.ErrUnexpectedEOF inside one.
func (r *Reader) Next() (*Unit, error) {
	for {
		hdr, err := r.r.Peek(8)
		if err != nil {
			if errors.Is(err, io.EOF) && len(hdr) == 0 {
				return nil, io.EOF
			}
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}

			return nil, err
		}
		typ, hdrLen, size, err := header(hdr, 0)
		if err != nil && !errors.Is(err, errShort) {
			return nil, err
		}
		if hdrLen == 16 {
			// A 64-bit size: peek the rest of the header.
			full, err := r.r.Peek(16)
			if err != nil {
				return nil, io.ErrUnexpectedEOF
			}
			typ, hdrLen, size, err = header(full, 0)
			if err != nil {
				return nil, err
			}
		}
		if size > MaxBox {
			return nil, fmt.Errorf("fmp4: box %q of %d bytes", typ, size)
		}
		b := make([]byte, size)
		if _, err := io.ReadFull(r.r, b); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}

			return nil, err
		}
		switch typ {
		case "ftyp":
			r.ftyp = b
		case "moov":
			init, err := ParseInit(append(append([]byte(nil), r.ftyp...), b...))
			if err != nil {
				return nil, err
			}
			r.ftyp = nil
			r.init = init

			return &Unit{Init: init}, nil
		case "styp", "sidx":
			r.pending = append(r.pending, b...)
		case "moof":
			if r.init == nil {
				return nil, errNoInit
			}
			frag := append(r.pending, b...)
			r.pending = nil
			// The mdat that follows; a moof without one is a fragment
			// of no samples and stands alone.
			if next, err := r.r.Peek(8); err == nil && string(next[4:8]) == "mdat" {
				_, _, msize, err := header(next, 0)
				if err != nil {
					return nil, err
				}
				if msize > MaxBox {
					return nil, fmt.Errorf("fmp4: mdat of %d bytes", msize)
				}
				m := make([]byte, msize)
				if _, err := io.ReadFull(r.r, m); err != nil {
					if errors.Is(err, io.EOF) {
						err = io.ErrUnexpectedEOF
					}

					return nil, err
				}
				frag = append(frag, m...)
			}
			f, err := ParseFragment(frag, r.init)
			if err != nil {
				return nil, err
			}

			return &Unit{Frag: f}, nil
		default:
			// free, skip, mfra, mfro, prft, emsg, uuid, a stray mdat:
			// nothing the media needs.
		}
	}
}
