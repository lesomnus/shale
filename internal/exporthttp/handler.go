// Package exporthttp serves bounded, authenticated MP4 attachment streams.
// Authorization supplies canonical recordings; HTTP callers never supply a URL.
package exporthttp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/internal/mp4export"
)

// Recording is immutable stored media admitted by the capability's resolver.
// Refresh must only renew credentials for this same recording, not widen scope.
type Recording struct {
	Start   time.Time
	Size    int64
	URL     string
	Refresh func(context.Context) (string, error)
}

// Request is the authenticated, canonical source and recording window.
type Request struct {
	Source     string
	Actor      string
	From, To   time.Time
	Recordings []Recording
}

// Handler streams a normal MP4 with its final moov at the tail. Configuration
// fields must not change after the first request. Limits are per CP process.
type Handler struct {
	Resolve       func(context.Context, string) (*Request, error)
	Client        *http.Client
	AllowHTTP     bool
	MaxConcurrent int
	PerActor      int
	MaxDuration   time.Duration

	mu     sync.Mutex
	active int
	actors map[string]int
}

func (h *Handler) global() (func(), bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	limit := h.MaxConcurrent
	if limit <= 0 {
		limit = 2
	}
	if h.active >= limit {
		return nil, false
	}
	h.active++
	return func() { h.mu.Lock(); h.active--; h.mu.Unlock() }, true
}

func (h *Handler) actor(id string) (func(), bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	limit := h.PerActor
	if limit <= 0 {
		limit = 1
	}
	if h.actors == nil {
		h.actors = make(map[string]int)
	}
	if h.actors[id] >= limit {
		return nil, false
	}
	h.actors[id]++
	return func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.actors[id]--
		if h.actors[id] == 0 {
			delete(h.actors, id)
		}
	}, true
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Accept-Ranges", "none")
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "exports accept GET only", http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("Range") != "" {
		http.Error(w, "export downloads do not support Range or resuming", http.StatusBadRequest)
		return
	}
	ticket := r.URL.Query().Get("token")
	if ticket == "" || len(ticket) > 16<<10 {
		http.Error(w, "a valid export capability is required", http.StatusUnauthorized)
		return
	}
	if h.Resolve == nil {
		http.Error(w, "exports are not configured", http.StatusServiceUnavailable)
		return
	}
	release, ok := h.global()
	if !ok {
		busy(w)
		return
	}
	defer release()
	duration := h.MaxDuration
	if duration <= 0 {
		duration = 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(r.Context(), duration)
	defer cancel()
	// A blocked socket write also needs a deadline: context alone cannot
	// interrupt ResponseWriter.Write when a browser stops reading.
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(duration))
	defer controller.SetWriteDeadline(time.Time{})
	request, err := h.Resolve(ctx, ticket)
	if err != nil {
		http.Error(w, "cannot authorize this export", authStatus(err))
		return
	}
	if request == nil || request.Actor == "" {
		http.Error(w, "invalid export authorization", http.StatusForbidden)
		return
	}
	releaseActor, ok := h.actor(request.Actor)
	if !ok {
		busy(w)
		return
	}
	defer releaseActor()
	// Clone the client configuration so credentials cannot follow a
	// redirect, including when a caller supplied the test transport.
	client := &http.Client{}
	if h.Client != nil {
		*client = *h.Client
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	sources := make([]mp4export.Source, 0, len(request.Recordings))
	for _, recording := range request.Recordings {
		reader, err := newRemoteReader(ctx, client, recording, h.AllowHTTP)
		if err != nil {
			http.Error(w, "invalid storage endpoint for export", http.StatusBadGateway)
			return
		}
		sources = append(sources, mp4export.Source{Reader: reader, Size: recording.Size, Start: recording.Start})
	}
	plan, err := mp4export.Prepare(ctx, sources, request.From, request.To)
	if err != nil {
		code := http.StatusUnprocessableEntity
		message := "cannot export this recording range: " + err.Error()
		var upstream *storageError
		if errors.As(err, &upstream) {
			code = http.StatusBadGateway
			message = "cannot read the stored recording"
		}
		if ctx.Err() != nil {
			code = http.StatusRequestTimeout
			message = "export preparation canceled or timed out"
		}
		http.Error(w, message, code)
		return
	}
	name := safeName(request.Source) + "-" + plan.Start().UTC().Format("20060102T150405Z") + ".mp4"
	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	w.Header().Set("Content-Length", strconv.FormatInt(plan.Size(), 10))
	if err := plan.WriteTo(ctx, w); err != nil {
		// Never include a token-bearing storage URL/error in logs or append
		// JSON to media. An aborted response must remain an incomplete download.
		slog.WarnContext(ctx, "MP4 export interrupted", "actor", request.Actor, "source", request.Source)
		panic(http.ErrAbortHandler)
	}
}

func busy(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "5")
	http.Error(w, "too many concurrent exports", http.StatusTooManyRequests)
}

func authStatus(err error) int {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.NotFound:
		return http.StatusNotFound
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unavailable, codes.FailedPrecondition:
		return http.StatusServiceUnavailable
	case codes.Canceled, codes.DeadlineExceeded:
		return http.StatusRequestTimeout
	default:
		return http.StatusInternalServerError
	}
}

func safeName(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || strings.ContainsRune("-_.", r) {
			return r
		}
		return '_'
	}, s)
	if len(s) > 120 {
		s = string([]rune(s)[:min(len([]rune(s)), 40)])
	}
	s = strings.Trim(s, ".")
	if s == "" {
		return "recording"
	}
	return s
}

type storageError struct{ why string }

func (e *storageError) Error() string { return fmt.Sprintf("storage: %s", e.why) }
