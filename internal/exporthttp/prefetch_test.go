package exporthttp

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/internal/mp4export"
)

type preparationCounts struct {
	requests int64
	tiny     int64
	bytes    int64
}

// Exercise the real parser, HTTP ranges and writer. The sources deliberately
// remain remote readers: a counting in-memory ReaderAt would not catch tiny
// parser reads turning into separate Storage requests.
// With windows, each recording gets its own, as the handler gives them.
func prepareOverHTTP(t *testing.T, b []byte, recordings int, windows bool) (preparationCounts, []byte) {
	t.Helper()
	var requests, tiny, received atomic.Int64
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var from, to int
		_, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &from, &to)
		if err != nil || from < 0 || to < from || to >= len(b) || to-from >= maxRangeBytes {
			http.Error(w, "invalid range", http.StatusBadRequest)
			return
		}
		requests.Add(1)
		if to-from+1 == 8 {
			tiny.Add(1)
		}
		received.Add(int64(to - from + 1))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, len(b)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(b[from : to+1])
	}))
	defer storage.Close()
	start := time.Date(2026, 11, 2, 5, 0, 0, 0, time.UTC)
	var sources []mp4export.Source
	for i := range recordings {
		var cache *readAhead
		if windows {
			cache = &readAhead{}
		}
		reader, err := newRemoteReader(t.Context(), storage.Client(), Recording{URL: storage.URL, Size: int64(len(b))}, true, cache)
		require.NoError(t, err)
		sources = append(sources, mp4export.Source{Reader: reader, Size: int64(len(b)), Start: start.Add(time.Duration(i) * 4 * time.Minute)})
	}
	// One second from one lamina is enough to expose the full-lamina metadata
	// scan. Multiple laminae model a 65-minute range with recording gaps.
	end := start.Add(time.Second)
	if recordings > 1 {
		end = start.Add(65 * time.Minute)
	}
	plan, err := mp4export.Prepare(t.Context(), sources, start, end)
	require.NoError(t, err)
	counts := preparationCounts{requests: requests.Load(), tiny: tiny.Load(), bytes: received.Load()}
	var out bytes.Buffer
	require.NoError(t, plan.WriteTo(t.Context(), &out))
	require.Equal(t, plan.Size(), int64(out.Len()))
	return counts, out.Bytes()
}

func TestPrepareReadAheadReducesHTTPRequests(t *testing.T) {
	for _, tc := range []struct {
		name       string
		recordings int
		baseline   int64
	}{
		{"sample", 1, 95},
		{"av", 1, 44},
		{"aac", 1, 32},
		{"av", 17, 748},
	} {
		t.Run(fmt.Sprintf("%s-%d", tc.name, tc.recordings), func(t *testing.T) {
			b, err := os.ReadFile("../fmp4/testdata/" + tc.name + ".mp4")
			require.NoError(t, err)
			baseline, original := prepareOverHTTP(t, b, tc.recordings, false)
			cached, actual := prepareOverHTTP(t, b, tc.recordings, true)
			require.Equal(t, tc.baseline, baseline.requests, "uncached fixture reproduces the original tiny-request count")
			require.Less(t, cached.requests, baseline.requests/4, "read-ahead must remove most metadata round trips")
			require.Zero(t, cached.tiny)
			require.Equal(t, original, actual, "read-ahead must not alter the selected MP4 payload or sample tables")
			// The explicit tradeoff is fewer Storage jobs at the cost of
			// fetching a bounded neighborhood, potentially containing media.
			require.Greater(t, cached.bytes, baseline.bytes)
			t.Logf("recordings=%d prepare HTTP requests %d -> %d; 8-byte requests %d -> %d; upstream bytes %d -> %d",
				tc.recordings, baseline.requests, cached.requests, baseline.tiny, cached.tiny, baseline.bytes, cached.bytes)
		})
	}
}
