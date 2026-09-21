package mpegts

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// A stream whose encoder wrote its parameter sets once and never again
// (#84): the head of a lamina from a Pi 400 through ffmpeg 8's
// h264_v4l2m2m, every keyframe bare. The synthetic sample stream has them
// with every frame.
const (
	bareStream = "../producer/testdata/nosps.ts"
	fullStream = "../producer/testdata/sample.ts"
)

func demuxAll(t *testing.T, d *Demuxer, path string) []AccessUnit {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	units, err := d.Write(b[:len(b)/PacketSize*PacketSize])
	require.NoError(t, err)

	return append(units, d.Flush()...)
}

func types(es []byte) []int {
	var ts []int
	for _, u := range NALUnits(es) {
		ts = append(ts, NALType(u, StreamH264))
	}

	return ts
}

func TestNALUnits(t *testing.T) {
	es := []byte{0, 0, 0, 1, 0x67, 1, 2, 0, 0, 1, 0x68, 3, 0, 0, 0, 1, 0x65, 4, 5}
	units := NALUnits(es)
	require.Len(t, units, 3)
	require.Equal(t, []byte{0, 0, 0, 1, 0x67, 1, 2}, units[0])
	require.Equal(t, []byte{0, 0, 1, 0x68, 3}, units[1])
	require.Equal(t, []byte{0, 0, 0, 1, 0x65, 4, 5}, units[2])
	require.Equal(t, []int{7, 8, 5}, types(es))
	require.Equal(t, -1, NALType([]byte{0, 0, 1}, StreamH264))
	require.Equal(t, 33, NALType([]byte{0, 0, 1, 0x42, 0x01}, StreamH265))

	// The set, whole or not at all.
	require.Equal(t, es[:12], ParamSets(es, StreamH264))
	require.Nil(t, ParamSets(es[7:], StreamH264))
	require.Nil(t, ParamSets(es[12:], StreamH264))

	// In front of the picture, after the delimiter when there is one.
	ps := es[:12]
	require.Equal(t, []int{7, 8, 5}, types(WithParams(es[12:], ps)))
	aud := append([]byte{0, 0, 0, 1, 0x09, 0xf0}, es[12:]...)
	require.Equal(t, []int{9, 7, 8, 5}, types(WithParams(aud, ps)))
	require.Equal(t, append(append(append([]byte(nil), aud[:6]...), ps...), aud[6:]...), WithParams(aud, ps))
}

func TestDemuxerCarriesParams(t *testing.T) {
	full := demuxAll(t, New(), fullStream)
	require.NotEmpty(t, full)
	ps := ParamSets(full[0].Data, StreamH264)
	require.NotNil(t, ps, "the sample stream carries its parameter sets")

	// Bare: keyframes without parameter sets stay bare when none were seen.
	bare := demuxAll(t, New(), bareStream)
	require.NotEmpty(t, bare)
	require.True(t, bare[0].Keyframe)
	require.Equal(t, []int{9, 5}, types(bare[0].Data)[:2])
	require.Nil(t, ParamSets(bare[0].Data, StreamH264))

	// Told what an earlier stream carried, every keyframe gets them, after
	// its delimiter; the pictures between are left alone.
	d := New()
	d.SetParams(ps)
	units := demuxAll(t, d, bareStream)
	require.Equal(t, len(bare), len(units))
	keys := 0
	for i, u := range units {
		if u.Keyframe {
			keys++
			require.Equal(t, []int{9, 7, 8, 5}, types(u.Data)[:4], "unit %d", i)
			require.Equal(t, ps, ParamSets(u.Data, StreamH264))
			require.Equal(t, len(bare[i].Data)+len(ps), len(u.Data))
		} else {
			require.Equal(t, bare[i].Data, u.Data, "unit %d", i)
		}
	}
	require.GreaterOrEqual(t, keys, 1)

	// And keeps what it saw for the next demuxer.
	require.Equal(t, ps, d.Params())
	next := New()
	next.SetParams(d.Params())
	require.Equal(t, ps, next.Params())
}
