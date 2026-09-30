package fmp4

// The video samples of an MP4 are NAL units with a length in front of
// each (AVCC); what a WebRTC packetizer wants is Annex B, a start code in
// front of each, with the parameter sets from the init segment's `avcC`
// or `hvcC` in front of a keyframe (§39.4).

var startCode = []byte{0, 0, 0, 1}

// H265 says the track's video is H.265.
func (t *Track) H265() bool { return t.Entry == "hvc1" || t.Entry == "hev1" }

// LengthSize is how many bytes the length in front of a NAL unit takes,
// from the codec configuration; 4 when it does not say.
func (t *Track) LengthSize() int {
	switch {
	case t.H265() && len(t.Config) > 21:
		return int(t.Config[21]&3) + 1
	case !t.H265() && len(t.Config) > 4:
		return int(t.Config[4]&3) + 1
	}

	return 4
}

// AnnexB is a sample's NAL units with start codes in place of lengths.
func AnnexB(sample []byte, lengthSize int) []byte {
	out := make([]byte, 0, len(sample)+16)
	for len(sample) >= lengthSize {
		var n int
		for i := 0; i < lengthSize; i++ {
			n = n<<8 | int(sample[i])
		}
		sample = sample[lengthSize:]
		if n <= 0 || n > len(sample) {
			break
		}
		out = append(out, startCode...)
		out = append(out, sample[:n]...)
		sample = sample[n:]
	}

	return out
}

// ParamSets is the track's parameter sets in Annex B form: the SPS and
// PPS of an `avcC`, the VPS, SPS and PPS of an `hvcC`; nil when the
// configuration has none.
func (t *Track) ParamSets() []byte {
	c := t.Config
	if t.H265() {
		// hvcC: 22 bytes of fields, then numOfArrays, then arrays of
		// (type, count, (len, nal)*).
		if len(c) < 23 {
			return nil
		}
		var out []byte
		off := 23
		for a := 0; a < int(c[22]); a++ {
			if off+3 > len(c) {
				break
			}
			count := int(c[off+1])<<8 | int(c[off+2])
			off += 3
			for i := 0; i < count; i++ {
				if off+2 > len(c) {
					return out
				}
				n := int(c[off])<<8 | int(c[off+1])
				off += 2
				if off+n > len(c) {
					return out
				}
				out = append(out, startCode...)
				out = append(out, c[off:off+n]...)
				off += n
			}
		}

		return out
	}
	// avcC: version, profile, compat, level, lengthSizeMinusOne, then
	// numOfSPS (low 5 bits) and the SPSs, then numOfPPS and the PPSs.
	if len(c) < 6 {
		return nil
	}
	var out []byte
	off := 6
	for _, num := range []int{int(c[5] & 0x1f), -1} {
		if num < 0 {
			if off >= len(c) {
				return out
			}
			num = int(c[off])
			off++
		}
		for i := 0; i < num; i++ {
			if off+2 > len(c) {
				return out
			}
			n := int(c[off])<<8 | int(c[off+1])
			off += 2
			if off+n > len(c) {
				return out
			}
			out = append(out, startCode...)
			out = append(out, c[off:off+n]...)
			off += n
		}
	}

	return out
}

// KeyByNAL looks inside a video sample for the slice it carries: 1 for an
// IDR (or an IRAP, for H.265), -1 for a slice that is not one, 0 when the
// sample holds no slice this can tell. Some encoders flag every frame a
// sync sample (the Raspberry Pi's, #84), so a flag is believed only when
// the bitstream does not say.
func KeyByNAL(sample []byte, lengthSize int, h265 bool) int {
	for len(sample) > lengthSize {
		var n int
		for i := 0; i < lengthSize; i++ {
			n = n<<8 | int(sample[i])
		}
		sample = sample[lengthSize:]
		if n <= 0 || n > len(sample) {
			return 0
		}
		h := sample[0]
		sample = sample[n:]
		if h265 {
			switch t := (h >> 1) & 0x3f; {
			case t >= 16 && t <= 21:
				return 1
			case t <= 9:
				return -1
			}

			continue
		}
		switch h & 0x1f {
		case 5:
			return 1
		case 1:
			return -1
		}
	}

	return 0
}

// SampleKey says whether a video sample is a keyframe: what its slice says
// when it says, else its sync flag.
func (t *Track) SampleKey(data []byte, s Sample) bool {
	switch KeyByNAL(data, t.LengthSize(), t.H265()) {
	case 1:
		return true
	case -1:
		return false
	}

	return s.Sync()
}
