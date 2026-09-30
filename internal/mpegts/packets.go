package mpegts

// TS packets written by hand (§38.2, §39.4): the parameter sets a keyframe
// needs in front of it when its encoder wrote them once and never again
// (#84), as one PES on the video PID.

// ParamPackets is one PES holding the parameter sets, on the video PID,
// with the given 5-byte PTS field when there is one, as TS packets whose
// continuity counters end just before `keyCC`, the keyframe packet's, so
// the keyframe follows without a skip.
func ParamPackets(pid uint16, pts []byte, keyCC byte, params []byte) []byte {
	pes := []byte{0, 0, 1, 0xe0, 0, 0, 0x80, 0, 0}
	if len(pts) == 5 {
		pes[7] = 0x80
		pes[8] = 5
		pes = append(pes, pts...)
	}
	pes = append(pes, params...)
	n := len(pes) - 6
	pes[4], pes[5] = byte(n>>8), byte(n)

	count := (len(pes) + PacketSize - 5) / (PacketSize - 4)
	// The first packet's counter is the keyframe's less the count, so the
	// last one's is the keyframe's less one.
	cc := int(keyCC) - count - 1
	var out []byte
	for i := 0; len(pes) > 0; i++ {
		cc++
		hdr := []byte{syncByte, byte(pid >> 8 & 0x1f), byte(pid), 0x10 | byte(cc&0x0f)}
		if i == 0 {
			hdr[1] |= 0x40
		}
		room := PacketSize - 4
		if len(pes) < room {
			// Stuffing: an adaptation field fills what the payload does not.
			afl := room - len(pes) - 1
			hdr[3] |= 0x20
			hdr = append(hdr, byte(afl))
			if afl > 0 {
				hdr = append(hdr, 0)
				for j := 1; j < afl; j++ {
					hdr = append(hdr, 0xff)
				}
			}
			room = len(pes)
		}
		out = append(out, hdr...)
		out = append(out, pes[:room]...)
		pes = pes[room:]
	}

	return out
}

// PTSBytes is a presentation time stamp as the 5 bytes of a PES header's
// PTS field, marker bits and all.
func PTSBytes(pts int64) []byte {
	return []byte{
		0x21 | byte(pts>>29)&0x0e,
		byte(pts >> 22),
		0x01 | byte(pts>>14)&0xfe,
		byte(pts >> 7),
		0x01 | byte(pts<<1),
	}
}
