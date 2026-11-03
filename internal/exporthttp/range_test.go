package exporthttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type rangeTransport func(*http.Request) (*http.Response, error)

func (f rangeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func rangeResponse(r *http.Request, b []byte) (*http.Response, error) {
	var from, to int64
	if n, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &from, &to); err != nil || n != 2 || from < 0 || to < from || to >= int64(len(b)) || to-from >= 1<<20 {
		return nil, fmt.Errorf("invalid test range: %s", r.Header.Get("Range"))
	}
	return &http.Response{
		StatusCode: http.StatusPartialContent,
		Header:     http.Header{"Content-Range": []string{fmt.Sprintf("bytes %d-%d/%d", from, to, len(b))}},
		Body:       io.NopCloser(bytes.NewReader(b[from : to+1])),
		Request:    r,
	}, nil
}

func testRemoteReader(t *testing.T, ctx context.Context, client *http.Client, size int, path string, cache *readAhead) *remoteReader {
	t.Helper()
	r, err := newRemoteReader(ctx, client, Recording{Size: int64(size), URL: "https://storage.test/" + path}, false, cache)
	require.NoError(t, err)
	return r
}

func TestReadAheadAdjacentBoundaryAndEOF(t *testing.T) {
	b := make([]byte, 2*readAheadBytes+37)
	for i := range b {
		b[i] = byte(i * 31)
	}
	var ranges []string
	client := &http.Client{Transport: rangeTransport(func(r *http.Request) (*http.Response, error) {
		ranges = append(ranges, r.Header.Get("Range"))
		return rangeResponse(r, b)
	})}
	cache := &readAhead{}
	r := testRemoteReader(t, t.Context(), client, len(b), "recording", cache)
	for _, off := range []int{0, 8, 16, readAheadBytes - 4} {
		got := make([]byte, 8)
		n, err := r.ReadAt(got, int64(off))
		require.NoError(t, err)
		require.Equal(t, 8, n)
		require.Equal(t, b[off:off+8], got)
	}
	require.Len(t, ranges, 2, "adjacent headers share a window; crossing its edge fetches one more")
	require.Equal(t, fmt.Sprintf("bytes=0-%d", readAheadBytes-1), ranges[0])
	require.Equal(t, fmt.Sprintf("bytes=%d-%d", readAheadBytes, 2*readAheadBytes-1), ranges[1])
	buffer := &cache.buf[0]
	got := make([]byte, 8)
	n, err := r.ReadAt(got, int64(len(b)-4))
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 4, n)
	require.Equal(t, b[len(b)-4:], got[:n])
	require.Same(t, buffer, &cache.buf[0], "window replacement reuses its byte array")
	require.Equal(t, fmt.Sprintf("bytes=%d-%d", 2*readAheadBytes, len(b)-1), ranges[2])
	count := len(ranges)
	n, err = r.ReadAt(got, int64(len(b)))
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, n)
	_, err = r.ReadAt(got, -1)
	require.Error(t, err)
	n, err = r.ReadAt(nil, 0)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Len(t, ranges, count)
}

func TestLargeReadsKeepDirectOneMiBRanges(t *testing.T) {
	b := bytes.Repeat([]byte{0x5a}, (3<<20)+100)
	var ranges []string
	client := &http.Client{Transport: rangeTransport(func(r *http.Request) (*http.Response, error) {
		ranges = append(ranges, r.Header.Get("Range"))
		return rangeResponse(r, b)
	})}
	cache := &readAhead{}
	r := testRemoteReader(t, t.Context(), client, len(b), "recording", cache)
	got := make([]byte, (2<<20)+19)
	n, err := r.ReadAt(got, 11)
	require.NoError(t, err)
	require.Equal(t, len(got), n)
	require.Equal(t, b[11:11+len(got)], got)
	require.Equal(t, []string{
		fmt.Sprintf("bytes=11-%d", 11+(1<<20)-1),
		fmt.Sprintf("bytes=%d-%d", 11+(1<<20), 11+(2<<20)-1),
		fmt.Sprintf("bytes=%d-%d", 11+(2<<20), 11+(2<<20)+18),
	}, ranges)
	require.Nil(t, cache.buf, "a large request's short final part must not trigger read-ahead")
}

func TestReadAheadFixedBudgetAndRecordingIdentity(t *testing.T) {
	b := bytes.Repeat([]byte{0x6d}, 4096)
	var requests atomic.Int32
	client := &http.Client{Transport: rangeTransport(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		return rangeResponse(r, b)
	})}
	cache := &readAhead{}
	var buffer *byte
	readers := make([]*remoteReader, 1000)
	for i := range readers {
		// Even identical URLs and offsets on different admitted recording
		// readers must miss: URLs are not recording identities.
		readers[i] = testRemoteReader(t, t.Context(), client, len(b), "same-url", cache)
		var got [8]byte
		_, err := readers[i].ReadAt(got[:], 0)
		require.NoError(t, err)
		require.Equal(t, b[:8], got[:])
		if buffer == nil {
			buffer = &cache.buf[0]
		}
		require.Same(t, buffer, &cache.buf[0])
		require.Equal(t, readAheadBytes, cap(cache.buf))
		require.Same(t, cache, readers[i].cache)
	}
	require.Equal(t, int32(len(readers)), requests.Load())
	// Another job gets another window and cannot spend the first job's
	// cached access, even at the same URL and offset.
	otherCache := &readAhead{}
	other := testRemoteReader(t, t.Context(), client, len(b), "same-url", otherCache)
	var got [8]byte
	_, err := other.ReadAt(got[:], 0)
	require.NoError(t, err)
	require.NotSame(t, buffer, &otherCache.buf[0])
	require.Equal(t, int32(len(readers)+1), requests.Load())
}

func TestReadAheadNeverPublishesFailedFill(t *testing.T) {
	for _, fault := range []string{"status", "range", "encoding", "short", "extra"} {
		t.Run(fault, func(t *testing.T) {
			var failing bool
			var requests int
			client := &http.Client{Transport: rangeTransport(func(r *http.Request) (*http.Response, error) {
				requests++
				b := bytes.Repeat([]byte{r.URL.Path[len(r.URL.Path)-1]}, 4096)
				resp, err := rangeResponse(r, b)
				if err != nil || !failing {
					return resp, err
				}
				switch fault {
				case "status":
					resp.StatusCode = http.StatusOK
				case "range":
					resp.Header.Set("Content-Range", "bytes 0-4094/4096")
				case "encoding":
					resp.Header.Set("Content-Encoding", "gzip")
				case "short":
					resp.Body = io.NopCloser(bytes.NewReader(b[:len(b)-1]))
				case "extra":
					resp.Body = io.NopCloser(bytes.NewReader(append(b, 1)))
				}
				return resp, nil
			})}
			cache := &readAhead{}
			a := testRemoteReader(t, t.Context(), client, 4096, "a", cache)
			b := testRemoteReader(t, t.Context(), client, 4096, "b", cache)
			got := make([]byte, 8)
			_, err := a.ReadAt(got, 0)
			require.NoError(t, err)
			buffer := &cache.buf[0]
			failing = true
			for range 2 {
				got = bytes.Repeat([]byte{0xff}, 8)
				n, err := b.ReadAt(got, 0)
				require.Error(t, err)
				require.Zero(t, n)
				require.Equal(t, bytes.Repeat([]byte{0xff}, 8), got)
				require.Nil(t, cache.owner)
				require.Zero(t, cache.n)
			}
			require.Equal(t, 3, requests, "the failed fill was retried rather than treated as a hit")
			failing = false
			_, err = a.ReadAt(got, 0)
			require.NoError(t, err)
			require.Equal(t, bytes.Repeat([]byte{'a'}, 8), got, "partially overwritten bytes cannot remain under the old identity")
			require.Equal(t, 4, requests)
			require.Same(t, buffer, &cache.buf[0])
		})
	}
}

func TestReadAheadCancellationStopsCacheHits(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var requests int
	client := &http.Client{Transport: rangeTransport(func(r *http.Request) (*http.Response, error) {
		requests++
		return rangeResponse(r, bytes.Repeat([]byte{1}, 4096))
	})}
	cache := &readAhead{}
	r := testRemoteReader(t, ctx, client, 4096, "recording", cache)
	var got [8]byte
	_, err := r.ReadAt(got[:], 0)
	require.NoError(t, err)
	cancel()
	n, err := r.ReadAt(got[:], 8)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, n)
	require.Equal(t, 1, requests)
}

type cancelOnEOF struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b cancelOnEOF) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		b.cancel()
	}
	return n, err
}

func TestReadAheadCanceledFillIsNotPublished(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := &http.Client{Transport: rangeTransport(func(r *http.Request) (*http.Response, error) {
		resp, err := rangeResponse(r, bytes.Repeat([]byte{1}, 4096))
		if err == nil {
			resp.Body = cancelOnEOF{ReadCloser: resp.Body, cancel: cancel}
		}
		return resp, err
	})}
	cache := &readAhead{}
	r := testRemoteReader(t, ctx, client, 4096, "recording", cache)
	got := bytes.Repeat([]byte{0xff}, 8)
	n, err := r.ReadAt(got, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, n)
	require.Nil(t, cache.owner)
	require.Zero(t, cache.n)
	require.Equal(t, bytes.Repeat([]byte{0xff}, 8), got)
}

func TestReadAheadConcurrentRecordingIsolation(t *testing.T) {
	client := &http.Client{Transport: rangeTransport(func(r *http.Request) (*http.Response, error) {
		id, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/"))
		if err != nil {
			return nil, err
		}
		return rangeResponse(r, bytes.Repeat([]byte{byte(id)}, 8192))
	})}
	cache := &readAhead{}
	var wg sync.WaitGroup
	errors := make(chan error, 32)
	for i := range 32 {
		r := testRemoteReader(t, t.Context(), client, 8192, strconv.Itoa(i), cache)
		wg.Go(func() {
			for off := range 32 {
				var got [8]byte
				if _, err := r.ReadAt(got[:], int64(off*8)); err != nil {
					errors <- err
					return
				}
				if !bytes.Equal(got[:], bytes.Repeat([]byte{byte(i)}, 8)) {
					errors <- fmt.Errorf("recording %d received another recording's window", i)
					return
				}
			}
		})
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}
