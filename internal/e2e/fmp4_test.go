package e2e_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/internal/fmp4"
)

// laminaOf reads a lamina, or the relay's recent window, as fragmented
// MP4: the init segment and the fragments, whole.
func laminaOf(t *testing.T, b []byte) (*fmp4.Init, []*fmp4.Fragment) {
	t.Helper()
	r := fmp4.NewReader(bytes.NewReader(b))
	var init *fmp4.Init
	var frags []*fmp4.Fragment
	for {
		u, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "whole boxes")
		if u.Init != nil {
			init = u.Init
		} else {
			frags = append(frags, u.Frag)
		}
	}
	require.NotNil(t, init, "an init segment")

	return init, frags
}

// playsOnItsOwn is what every lamina is (§23.1): the init segment first, a
// key fragment first, and the index at the end that says where the keys
// are.
func playsOnItsOwn(t *testing.T, b []byte) (*fmp4.Init, []*fmp4.Fragment) {
	t.Helper()
	require.Equal(t, "ftyp", string(b[4:8]), "the init segment first")
	init, frags := laminaOf(t, b)
	require.NotEmpty(t, frags)
	require.True(t, frags[0].Key(init), "then a key fragment")
	n := fmp4.MfraSize(b[len(b)-16:])
	require.Greater(t, n, 0, "the index at the end")
	track, keys, err := fmp4.ParseMfra(b[len(b)-n:])
	require.NoError(t, err)
	require.Equal(t, init.Video().ID, track)
	require.NotEmpty(t, keys)
	require.Equal(t, int64(len(init.Bytes)), keys[0].Offset, "the first key is the first fragment")

	return init, frags
}

// audioEntries are the sample entries of a lamina's audio tracks, in order.
func audioEntries(init *fmp4.Init) []string {
	var out []string
	for _, tr := range init.Audio() {
		out = append(out, tr.Entry)
	}

	return out
}
