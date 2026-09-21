package producer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Push is the producer's listener for push sources (§38.1, §38.9): a
// process on the host writes a source's stream to it instead of the
// producer running a capture. The contract is the node's upload (§12.2),
// one per source and never completing while the source runs:
//
//	PUT  /sources/<alias>/frames   Upload-Offset: o   Upload-Complete: ?0 | ?1
//	       → 204  Upload-Offset: cur   taken up to cur
//	       → 409  Upload-Offset: cur   o is not cur, or a request is already open
//	HEAD /sources/<alias>/frames   → Upload-Offset: cur, Upload-Complete: ?0 | ?1
//
// The offset counts the bytes of the stream the producer has taken, so a
// writer keeps only what is above the last 204 and resumes from HEAD after
// a disconnect. What the bytes are is the source's kind in the producer's
// configuration, TS or frames (§38.9); the request does not say.
type Push struct {
	// Addr is `unix:/path` or `tcp://host:port`.
	Addr string
	// Idle is how long a stream may carry nothing before its open segment
	// is closed as stopped, and how long an open request may be silent.
	Idle time.Duration
	Log  *slog.Logger

	mu      sync.Mutex
	sources map[string]*pushSource
	ln      net.Listener
}

// PushIdle is the default Push.Idle.
const PushIdle = 30 * time.Second

// Add makes the source known to the listener.
func (p *Push) Add(alias string) *pushSource {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sources == nil {
		p.sources = map[string]*pushSource{}
	}
	idle := p.Idle
	if idle <= 0 {
		idle = PushIdle
	}
	s := &pushSource{alias: alias, idle: idle, wake: make(chan struct{}, 1)}
	p.sources[alias] = s

	return s
}

// Listen opens the socket. A stale Unix socket file is replaced.
func (p *Push) Listen() error {
	network, addr, err := pushAddr(p.Addr)
	if err != nil {
		return err
	}
	if network == "unix" {
		if err := os.Remove(addr); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("push: %w", err)
		}
	}
	ln, err := net.Listen(network, addr)
	if err != nil {
		return fmt.Errorf("push: %w", err)
	}
	if network == "unix" {
		os.Chmod(addr, 0o660)
	}
	p.ln = ln

	return nil
}

// Serve answers on the listener until the context ends.
func (p *Push) Serve(ctx context.Context) error {
	if p.ln == nil {
		if err := p.Listen(); err != nil {
			return err
		}
	}
	srv := &http.Server{Handler: p, ReadHeaderTimeout: 10 * time.Second}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(p.ln) }()
	select {
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
		<-done

		return nil
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err
	}
}

// pushAddr reads `unix:/path` or `tcp://host:port`.
func pushAddr(v string) (network, addr string, err error) {
	switch {
	case strings.HasPrefix(v, "unix:"):
		return "unix", strings.TrimPrefix(v, "unix:"), nil
	case strings.HasPrefix(v, "tcp://"):
		return "tcp", strings.TrimPrefix(v, "tcp://"), nil
	}

	return "", "", fmt.Errorf("push: %q is neither unix:/path nor tcp://host:port", v)
}

// ServeHTTP routes /sources/<alias>/frames.
func (p *Push) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := strings.CutPrefix(r.URL.Path, "/sources/")
	alias, tail, _ := strings.Cut(rest, "/")
	if !ok || tail != "frames" || alias == "" {
		http.NotFound(w, r)

		return
	}
	p.mu.Lock()
	s := p.sources[alias]
	p.mu.Unlock()
	if s == nil {
		http.Error(w, "no such push source: "+alias, http.StatusNotFound)

		return
	}
	switch r.Method {
	case http.MethodHead:
		s.head(w)
	case http.MethodPut:
		s.put(w, r, p.log())
	default:
		w.Header().Set("Allow", "PUT, HEAD")
		http.Error(w, "PUT or HEAD", http.StatusMethodNotAllowed)
	}
}

func (p *Push) log() *slog.Logger {
	if p.Log != nil {
		return p.Log
	}

	return slog.Default()
}

// pushSource is one source's side of the listener: the stream being
// written, and where it is.
type pushSource struct {
	alias string
	idle  time.Duration

	mu     sync.Mutex
	offset int64
	stream *pushStream
	active bool
	conns  int64
	last   time.Time
	err    string
	// wake tells Run a stream has appeared.
	wake chan struct{}
}

// Run hands each stream to read, as Capture.Run hands a capture's stdout,
// until the context ends. A stream ends when a request completed it.
func (s *pushSource) Run(ctx context.Context, read func(st *pushStream)) error {
	for {
		st := s.await(ctx)
		if st == nil {
			return nil
		}
		read(st)
		s.mu.Lock()
		if s.stream == st {
			// The reader gave up on a stream nobody completed (the
			// context ended); the next request starts afresh.
			s.stream, s.offset = nil, 0
		}
		s.mu.Unlock()
	}
}

// await answers the open stream, waiting for a request to open one.
func (s *pushSource) await(ctx context.Context) *pushStream {
	for {
		s.mu.Lock()
		st := s.stream
		s.mu.Unlock()
		if st != nil {
			return st
		}
		select {
		case <-ctx.Done():
			return nil
		case <-s.wake:
		}
	}
}

// current answers the open stream, opening one when there is none.
func (s *pushSource) current() *pushStream {
	if s.stream == nil {
		s.stream = &pushStream{ch: make(chan []byte), idle: s.idle}
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}

	return s.stream
}

// Up says whether the source delivers: a request is open, or bytes came
// within the idle window.
func (s *pushSource) Up() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.active || (!s.last.IsZero() && time.Since(s.last) < s.idle)
}

// Restarts is how many requests the source has taken, the push side's
// capture restarts.
func (s *pushSource) Restarts() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.conns
}

// LastError is how the last request ended, when it did not end well.
func (s *pushSource) LastError() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.err
}

// Offset is where the stream is.
func (s *pushSource) Offset() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.offset
}

func (s *pushSource) head(w http.ResponseWriter) {
	s.mu.Lock()
	off, open := s.offset, s.stream != nil
	s.mu.Unlock()
	w.Header().Set("Upload-Offset", strconv.FormatInt(off, 10))
	if open {
		w.Header().Set("Upload-Complete", "?0")
	} else {
		w.Header().Set("Upload-Complete", "?1")
	}
	w.WriteHeader(http.StatusNoContent)
}

// put takes a request's bytes into the stream, one request at a time.
func (s *pushSource) put(w http.ResponseWriter, r *http.Request, log *slog.Logger) {
	off, err := strconv.ParseInt(r.Header.Get("Upload-Offset"), 10, 64)
	if err != nil || off < 0 {
		http.Error(w, "Upload-Offset: the offset the stream is at, from HEAD", http.StatusBadRequest)

		return
	}
	complete := r.Header.Get("Upload-Complete") == "?1"
	s.mu.Lock()
	if s.active || off != s.offset {
		cur := s.offset
		s.mu.Unlock()
		w.Header().Set("Upload-Offset", strconv.FormatInt(cur, 10))
		if s.Up() && off == cur {
			http.Error(w, "a request is open on this source", http.StatusConflict)
		} else {
			http.Error(w, "the stream is at "+strconv.FormatInt(cur, 10), http.StatusConflict)
		}

		return
	}
	st := s.current()
	s.active = true
	s.conns++
	s.err = ""
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.active = false
		s.mu.Unlock()
	}()

	rc := http.NewResponseController(w)
	buf := make([]byte, 256<<10)
	var rerr error
	for {
		rc.SetReadDeadline(time.Now().Add(s.idle))
		n, err := r.Body.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			select {
			case st.ch <- chunk:
			case <-r.Context().Done():
				rerr = r.Context().Err()
			}
			if rerr != nil {
				break
			}
			s.mu.Lock()
			s.offset += int64(n)
			s.last = time.Now()
			s.mu.Unlock()
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				rerr = err
			}

			break
		}
	}
	s.mu.Lock()
	cur := s.offset
	if rerr != nil {
		s.err = rerr.Error()
	}
	s.mu.Unlock()
	rc.SetReadDeadline(time.Time{})
	w.Header().Set("Upload-Offset", strconv.FormatInt(cur, 10))
	if rerr != nil {
		var ne net.Error
		if errors.As(rerr, &ne) && ne.Timeout() {
			// Nothing for a whole idle window: the request ends here, the
			// stream stays open for the next one.
			w.WriteHeader(http.StatusNoContent)
		} else {
			log.Debug("push: request ended", "source", s.alias, "offset", cur, "err", rerr.Error())
		}

		return
	}
	if complete {
		st.close()
		s.mu.Lock()
		s.stream, s.offset = nil, 0
		s.mu.Unlock()
	}
	w.WriteHeader(http.StatusNoContent)
}

// pushStream is one stream of a source as its reader sees it: the chunks
// requests hand over, in order, with the idle window applied between them.
type pushStream struct {
	ch   chan []byte
	rest []byte
	idle time.Duration
	// OnIdle is told, from Read, when a whole idle window passed with no
	// bytes: the reader closes its open segment and keeps waiting.
	OnIdle func()
	idled  bool
	once   sync.Once
}

func (s *pushStream) close() { s.once.Do(func() { close(s.ch) }) }

// Read blocks for the next bytes; it answers io.EOF once the stream was
// completed and drained.
func (s *pushStream) Read(p []byte) (int, error) {
	for len(s.rest) == 0 {
		timer := time.NewTimer(s.idle)
		select {
		case b, ok := <-s.ch:
			timer.Stop()
			if !ok {
				return 0, io.EOF
			}
			s.rest = b
			s.idled = false
		case <-timer.C:
			if !s.idled && s.OnIdle != nil {
				s.idled = true
				s.OnIdle()
			}
		}
	}
	n := copy(p, s.rest)
	s.rest = s.rest[n:]

	return n, nil
}
