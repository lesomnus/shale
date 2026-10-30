package exporthttp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// remoteReader fetches only bounded byte ranges. It never retains an entire
// recording and renews only this admitted lamina after a token has expired.
type remoteReader struct {
	ctx       context.Context
	client    *http.Client
	recording Recording
	allowHTTP bool
	mu        sync.Mutex
	url       string
}

func endpoint(raw string, allowHTTP bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "https" && !(allowHTTP && u.Scheme == "http")) {
		return &storageError{"invalid endpoint"}
	}
	return nil
}

func newRemoteReader(ctx context.Context, client *http.Client, recording Recording, allowHTTP bool) (*remoteReader, error) {
	if recording.Size <= 0 {
		return nil, &storageError{"invalid recording size"}
	}
	if err := endpoint(recording.URL, allowHTTP); err != nil {
		return nil, err
	}
	return &remoteReader{ctx: ctx, client: client, recording: recording, allowHTTP: allowHTTP, url: recording.URL}, nil
}

func (r *remoteReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, &storageError{"negative offset"}
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= r.recording.Size {
		return 0, io.EOF
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for n < len(p) && off+int64(n) < r.recording.Size {
		length := min(int64(len(p)-n), min(1<<20, r.recording.Size-off-int64(n)))
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
