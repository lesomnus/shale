package exporthttp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/lesomnus/shale/internal/mp4export"
)

// readAheadBytes is the window a scan reads container headers through.
// Container headers are tiny and adjacent, but every remote range otherwise
// incurs a request and a Storage read job. Windows may include media bytes.
// A recording fragments every 200 ms, so its headers are some tens of KiB
// apart: a scan reads nearly all of it, and a larger window is fewer requests.
const readAheadBytes = 1 << 20

// maxRangeBytes bounds one Storage request, as large as one media read.
const maxRangeBytes = mp4export.MediaChunk

// readAhead holds one window. A recording's reader gives its array back when
// its scan is done (mp4export.Scanned), so an export holds at most one per
// scan running at once, however many recordings it has. Fills are serialized
// so readers that share one stay correct.
type readAhead struct {
	mu    sync.Mutex
	buf   []byte
	owner *remoteReader
	off   int64
	n     int
}

func (c *readAhead) copy(r *remoteReader, p []byte, off int64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if c.owner != r || off < c.off || off >= c.off+int64(c.n) {
		// A failed fill must not expose partially overwritten bytes under
		// either the previous recording's identity or this one's.
		c.owner, c.n = nil, 0
		if c.buf == nil {
			c.buf = make([]byte, readAheadBytes)
		}
		start := off - off%readAheadBytes
		n := int(min(int64(len(c.buf)), r.recording.Size-start))
		if err := r.read(c.buf[:n], start); err != nil {
			return 0, err
		}
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		c.owner, c.off, c.n = r, start, n
	}
	at := int(off - c.off)
	return copy(p, c.buf[at:c.n]), nil
}

// remoteReader fetches only bounded byte ranges. It never retains an entire
// recording and renews only this admitted lamina after a token has expired.
type remoteReader struct {
	ctx       context.Context
	client    *http.Client
	recording Recording
	allowHTTP bool
	mu        sync.Mutex
	url       string
	cache     *readAhead
	// scanned is set once the scan is done: media reads, however short,
	// then go straight to Storage and never take a window again.
	scanned bool
}

func endpoint(raw string, allowHTTP bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "https" && !(allowHTTP && u.Scheme == "http")) {
		return &storageError{"invalid endpoint"}
	}
	return nil
}

func newRemoteReader(ctx context.Context, client *http.Client, recording Recording, allowHTTP bool, cache *readAhead) (*remoteReader, error) {
	if recording.Size <= 0 {
		return nil, &storageError{"invalid recording size"}
	}
	if err := endpoint(recording.URL, allowHTTP); err != nil {
		return nil, err
	}
	return &remoteReader{ctx: ctx, client: client, recording: recording, allowHTTP: allowHTTP, url: recording.URL, cache: cache}, nil
}

func (r *remoteReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, &storageError{"negative offset"}
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.recording.Size {
		return 0, io.EOF
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	small := r.cache != nil && !r.scanned && len(p) <= readAheadBytes
	n := 0
	for n < len(p) && off+int64(n) < r.recording.Size {
		if err := r.ctx.Err(); err != nil {
			return n, err
		}
		if small {
			got, err := r.cache.copy(r, p[n:], off+int64(n))
			n += got
			if err != nil {
				return n, err
			}
			continue
		}
		length := min(int64(len(p)-n), min(maxRangeBytes, r.recording.Size-off-int64(n)))
		if err := r.read(p[n:n+int(length)], off+int64(n)); err != nil {
			return n, err
		}
		n += int(length)
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *remoteReader) read(p []byte, off int64) error {
	for retry := 0; retry < 2; retry++ {
		ctx, cancel := context.WithTimeout(r.ctx, 30*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url, nil)
		if err != nil {
			cancel()
			return &storageError{"invalid request"}
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1))
		req.Header.Set("Accept-Encoding", "identity")
		resp, err := r.client.Do(req)
		if err != nil {
			cancel()
			return &storageError{"range request failed"}
		}
		if retry == 0 && (resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden) && r.recording.Refresh != nil {
			resp.Body.Close()
			cancel()
			fresh, err := r.recording.Refresh(r.ctx)
			if err != nil {
				return &storageError{"credential renewal failed"}
			}
			if err := endpoint(fresh, r.allowHTTP); err != nil {
				return err
			}
			r.url = fresh
			continue
		}
		want := fmt.Sprintf("bytes %d-%d/%d", off, off+int64(len(p))-1, r.recording.Size)
		if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != want || (resp.Header.Get("Content-Encoding") != "" && resp.Header.Get("Content-Encoding") != "identity") {
			resp.Body.Close()
			cancel()
			return &storageError{"invalid byte range response"}
		}
		_, err = io.ReadFull(resp.Body, p)
		if err == nil {
			var extra [1]byte
			n, e := resp.Body.Read(extra[:])
			if n != 0 || e != io.EOF {
				err = io.ErrUnexpectedEOF
			}
		}
		resp.Body.Close()
		cancel()
		if err != nil {
			return &storageError{"incomplete byte range"}
		}
		return nil
	}
	return &storageError{"cannot renew recording access"}
}

// Scanned gives back the reader's container window once its scan is done.
// Media reads are larger than a window and do not use it.
func (r *remoteReader) Scanned() {
	r.mu.Lock()
	r.scanned = true
	r.mu.Unlock()
	if r.cache == nil {
		return
	}
	r.cache.mu.Lock()
	defer r.cache.mu.Unlock()
	if r.cache.owner == r {
		r.cache.buf, r.cache.owner, r.cache.n = nil, nil, 0
	}
}
