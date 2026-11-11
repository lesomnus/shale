package exporthttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

type rangeTransport func(*http.Request) (*http.Response, error)

func (f rangeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func rangeResponse(r *http.Request, b []byte) (*http.Response, error) {
	var from, to int64
	if n, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &from, &to); err != nil || n != 2 || from < 0 || to < from || to >= int64(len(b)) || to-from >= maxRangeBytes {
		return nil, fmt.Errorf("invalid test range: %s", r.Header.Get("Range"))
	}
	return &http.Response{
		StatusCode: http.StatusPartialContent,
		Header:     http.Header{"Content-Range": []string{fmt.Sprintf("bytes %d-%d/%d", from, to, len(b))}},
		Body:       io.NopCloser(bytes.NewReader(b[from : to+1])),
		Request:    r,
	}, nil
}

func testRemoteReader(t *testing.T, ctx context.Context, client *http.Client, size int, path string) *remoteReader {
	t.Helper()
	r, err := newRemoteReader(ctx, client, Recording{Size: int64(size), URL: "https://storage.test/" + path}, false)
	require.NoError(t, err)
	return r
}

// A read is one exact range, however small: planning reads only a tail, an
// index and an init segment, and nothing around them.
func TestReadsAreExactRanges(t *testing.T) {
	b := make([]byte, 3<<20+37)
	for i := range b {
		b[i] = byte(i * 31)
	}
	var ranges []string
	client := &http.Client{Transport: rangeTransport(func(r *http.Request) (*http.Response, error) {
		ranges = append(ranges, r.Header.Get("Range"))
		return rangeResponse(r, b)
	})}
	r := testRemoteReader(t, t.Context(), client, len(b), "recording")
	for _, off := range []int{0, 8, 1<<20 - 4} {
		got := make([]byte, 8)
		n, err := r.ReadAt(got, int64(off))
		require.NoError(t, err)
		require.Equal(t, 8, n)
		require.Equal(t, b[off:off+8], got)
	}
	require.Equal(t, []string{"bytes=0-7", "bytes=8-15", fmt.Sprintf("bytes=%d-%d", 1<<20-4, 1<<20+3)}, ranges)
	got := make([]byte, 8)
	n, err := r.ReadAt(got, int64(len(b)-4))
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 4, n)
	require.Equal(t, b[len(b)-4:], got[:n])
	require.Equal(t, fmt.Sprintf("bytes=%d-%d", len(b)-4, len(b)-1), ranges[3])
	n, err = r.ReadAt(got, int64(len(b)))
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, n)
	_, err = r.ReadAt(got, -1)
	require.Error(t, err)
	n, err = r.ReadAt(nil, 0)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Len(t, ranges, 4)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = testRemoteReader(t, ctx, client, len(b), "recording").ReadAt(got, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, ranges, 4, "a canceled export asks Storage for nothing")
}

func TestLargeReadsUseDirectRangesOfOneMediaChunk(t *testing.T) {
	b := bytes.Repeat([]byte{0x5a}, 2*maxRangeBytes+100)
	var ranges []string
	client := &http.Client{Transport: rangeTransport(func(r *http.Request) (*http.Response, error) {
		ranges = append(ranges, r.Header.Get("Range"))
		return rangeResponse(r, b)
	})}
	r := testRemoteReader(t, t.Context(), client, len(b), "recording")
	got := make([]byte, 2*maxRangeBytes+19)
	n, err := r.ReadAt(got, 11)
	require.NoError(t, err)
	require.Equal(t, len(got), n)
	require.Equal(t, b[11:11+len(got)], got)
	require.Equal(t, []string{
		fmt.Sprintf("bytes=11-%d", 11+maxRangeBytes-1),
		fmt.Sprintf("bytes=%d-%d", 11+maxRangeBytes, 11+2*maxRangeBytes-1),
		fmt.Sprintf("bytes=%d-%d", 11+2*maxRangeBytes, 11+2*maxRangeBytes+18),
	}, ranges)
}
