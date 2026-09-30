package fmp4

import (
	"encoding/binary"
	"errors"
)

// KeyAt is one entry of a lamina's index: a fragment that begins with a
// sync sample, where it is in the lamina and when it plays.
type KeyAt struct {
	// Time is the fragment's `tfdt`, in the track's timescale.
	Time int64
	// Offset is where the fragment's `moof` begins, in bytes from the
	// start of the lamina.
	Offset int64
}

// Mfra is the index a lamina ends with (§23.1): an `mfra` holding one
// `tfra` for the track, and the `mfro` that says how big the whole is, so
// a reader finds it from the end of the file. Every entry points at the
// first sample of the first run of the first traf of a fragment that
// begins with a sync sample.
func Mfra(track uint32, keys []KeyAt) []byte {
	// tfra, version 1: 64-bit times and offsets, one byte for each of the
	// traf, trun and sample numbers.
	tfra := make([]byte, 0, 16+19*len(keys))
	tfra = append(tfra, 1, 0, 0, 0)
	tfra = binary.BigEndian.AppendUint32(tfra, track)
	tfra = binary.BigEndian.AppendUint32(tfra, 0)
	tfra = binary.BigEndian.AppendUint32(tfra, uint32(len(keys)))
	for _, k := range keys {
		tfra = binary.BigEndian.AppendUint64(tfra, uint64(k.Time))
		tfra = binary.BigEndian.AppendUint64(tfra, uint64(k.Offset))
		tfra = append(tfra, 1, 1, 1)
	}
	tfraBox := box("tfra", tfra)
	// mfra = header + tfra + mfro(16).
	total := uint32(8 + len(tfraBox) + 16)
	mfro := make([]byte, 0, 8)
	mfro = append(mfro, 0, 0, 0, 0)
	mfro = binary.BigEndian.AppendUint32(mfro, total)

	return box("mfra", tfraBox, box("mfro", mfro))
}

var errNoMfra = errors.New("fmp4: no mfra at the end")

// MfraSize is what the last 16 bytes of a lamina say the index is, or 0
// when they are not an `mfro`.
func MfraSize(tail []byte) int {
	if len(tail) < 16 {
		return 0
	}
	t := tail[len(tail)-16:]
	if string(t[4:8]) != "mfro" {
		return 0
	}

	return int(binary.BigEndian.Uint32(t[12:]))
}

// ParseMfra reads an index: the track and its entries.
func ParseMfra(b []byte) (track uint32, keys []KeyAt, err error) {
	mfra, ok := Find(b, "mfra")
	if !ok {
		return 0, nil, errNoMfra
	}
	tfra, ok := Find(mfra.Payload(b), "tfra")
	if !ok {
		return 0, nil, errNoMfra
	}
	p := tfra.Payload(mfra.Payload(b))
	version, _ := fullBox(p)
	track = u32(p, 4)
	sizes := u32(p, 8)
	n := int(u32(p, 12))
	lt, ln, ls := int(sizes>>4&3)+1, int(sizes>>2&3)+1, int(sizes&3)+1
	off := 16
	for i := 0; i < n; i++ {
		var k KeyAt
		if version == 1 {
			k.Time, k.Offset = int64(u64(p, off)), int64(u64(p, off+8))
			off += 16
		} else {
			k.Time, k.Offset = int64(u32(p, off)), int64(u32(p, off+4))
			off += 8
		}
		off += lt + ln + ls
		if off > len(p) {
			return 0, nil, errShort
		}
		keys = append(keys, k)
	}

	return track, keys, nil
}
