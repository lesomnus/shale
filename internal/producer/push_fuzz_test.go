package producer

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The push endpoint takes requests from whatever is on the host (§38.9):
// any method, path, header and body must be answered, never panic, and
// never move the stream's offset past what was taken (#57).
func FuzzPushHandler(f *testing.F) {
	f.Add("PUT", "/sources/a/frames", "0", "?0", []byte("hello"))
	f.Add("PUT", "/sources/a/frames", "-1", "?1", []byte{})
	f.Add("HEAD", "/sources/a/frames", "", "", []byte{})
	f.Add("GET", "/sources/b/frames", "x", "yes", []byte("no"))
	f.Add("PUT", "/sources//frames", "0", "?0", []byte{0})
	f.Add("PUT", "/sources/a/frames/extra", "0", "?0", []byte{})
	f.Fuzz(func(t *testing.T, method, path, offset, complete string, body []byte) {
		p := &Push{Idle: time.Second}
		s := p.Add("a")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.Run(ctx, func(st *pushStream) { io.Copy(io.Discard, st) })
		}()
		req, err := http.NewRequest(method, "http://producer"+path, bytes.NewReader(body))
		if err != nil {
			// Not a request the net/http server would ever hand over.
			return
		}
		if offset != "" {
			req.Header.Set("Upload-Offset", offset)
		}
		if complete != "" {
			req.Header.Set("Upload-Complete", complete)
		}
		rec := httptest.NewRecorder()
		p.ServeHTTP(rec, req)
		switch rec.Code {
		case http.StatusNoContent, http.StatusConflict, http.StatusBadRequest, http.StatusNotFound, http.StatusMethodNotAllowed:
		default:
			t.Fatalf("an answer nobody defined: %d", rec.Code)
		}
		if off := s.Offset(); off < 0 || off > int64(len(body)) {
			t.Fatalf("offset %d after a body of %d", off, len(body))
		}
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the source's reader did not stop")
		}
	})
}
