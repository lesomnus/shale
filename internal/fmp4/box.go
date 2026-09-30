// Package fmp4 reads fragmented MP4 (ISO BMFF) as Shale stores it (§23.1,
// §38.2): an init segment (`ftyp` and `moov`) that describes the tracks,
// then fragments, each a `moof` and its `mdat`, and at the end of a lamina
// an `mfra` that says where the keyframes are. The producer cuts laminae
// at fragments that begin with a sync sample and indexes them; the relay
// takes the samples out of fragments for WebRTC; a viewer seeks by the
// index. Nothing is decoded and nothing is transcoded.
package fmp4

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Box is one box of a byte slice: its type, where its payload begins and
// where the box ends.
type Box struct {
	Type string
	// Off is the box's own offset in the slice it was found in, Hdr the
	// header's length, End the box's end.
	Off, Hdr, End int
}

// Payload is the box's bytes after the header.
func (b Box) Payload(in []byte) []byte { return in[b.Off+b.Hdr : b.End] }

// Whole is the box with its header.
func (b Box) Whole(in []byte) []byte { return in[b.Off:b.End] }

var (
	errShort = errors.New("fmp4: box runs past its container")
	errSize  = errors.New("fmp4: a box of size zero (to end of file) is not a stream")
)

// header reads a box header at off: its type, header length and size. A
// size of 1 means a 64-bit size follows; 0 means "to the end", which a
// stream cannot carry.
func header(b []byte, off int) (typ string, hdr int, size int64, err error) {
	if off+8 > len(b) {
		return "", 0, 0, errShort
	}
	size = int64(binary.BigEndian.Uint32(b[off:]))
	typ = string(b[off+4 : off+8])
	hdr = 8
	switch size {
	case 1:
		if off+16 > len(b) {
			return "", 0, 0, errShort
		}
		size = int64(binary.BigEndian.Uint64(b[off+8:]))
		hdr = 16
	case 0:
		return "", 0, 0, errSize
	}
	if size < int64(hdr) {
		return "", 0, 0, fmt.Errorf("fmp4: box %q of size %d", typ, size)
	}

	return typ, hdr, size, nil
}

// Children walks the boxes of a slice (a container's payload, or a whole
// file), in order.
func Children(b []byte) ([]Box, error) {
	var out []Box
	for off := 0; off < len(b); {
		typ, hdr, size, err := header(b, off)
		if err != nil {
			return out, err
		}
		end := int64(off) + size
		if end > int64(len(b)) {
			return out, errShort
		}
		out = append(out, Box{Type: typ, Off: off, Hdr: hdr, End: int(end)})
		off = int(end)
	}

	return out, nil
}

// Find is the first child of the given type, or false.
func Find(b []byte, typ string) (Box, bool) {
	boxes, _ := Children(b)
	for _, x := range boxes {
		if x.Type == typ {
			return x, true
		}
	}

	return Box{}, false
}

// fullBox is the version and flags of a full box's payload.
func fullBox(p []byte) (version byte, flags uint32) {
	if len(p) < 4 {
		return 0, 0
	}

	return p[0], uint32(p[1])<<16 | uint32(p[2])<<8 | uint32(p[3])
}

// u32 and u64 read big-endian integers, 0 past the end.
func u32(b []byte, off int) uint32 {
	if off < 0 || off+4 > len(b) {
		return 0
	}

	return binary.BigEndian.Uint32(b[off:])
}

func u64(b []byte, off int) uint64 {
	if off < 0 || off+8 > len(b) {
		return 0
	}

	return binary.BigEndian.Uint64(b[off:])
}

// box makes a box of the given type around a payload.
func box(typ string, payload ...[]byte) []byte {
	n := 8
	for _, p := range payload {
		n += len(p)
	}
	out := make([]byte, 8, n)
	binary.BigEndian.PutUint32(out, uint32(n))
	copy(out[4:], typ)
	for _, p := range payload {
		out = append(out, p...)
	}

	return out
}
