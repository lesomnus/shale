package exporthttp_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/internal/exporthttp"
)

func pending(t *testing.T, h http.Handler, ticket string) (context.CancelFunc, <-chan *httptest.ResponseRecorder) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r := httptest.NewRequest(http.MethodGet, "/exports/recording.mp4?token="+ticket, nil).WithContext(ctx)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		done <- w
	}()
	return cancel, done
}

func waitPending(t *testing.T, done <-chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	t.Helper()
	select {
	case w := <-done:
		return w
	case <-time.After(5 * time.Second):
		t.Fatal("export did not stop after cancellation")
		return nil
	}
}

func waitStarted(t *testing.T, started <-chan string) string {
	t.Helper()
	select {
	case actor := <-started:
		return actor
	case <-time.After(5 * time.Second):
		t.Fatal("export never reached storage")
		return ""
	}
}

func TestGlobalAdmissionPrecedesResolver(t *testing.T) {
	b := fixture(t)
	started := make(chan string, 10)
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- "storage"
		<-r.Context().Done()
	}))
	defer storage.Close()
	var calls atomic.Int32
	var deny atomic.Bool
	resolve := resolveRecording(b, storage.URL)
	h := &exporthttp.Handler{AllowHTTP: true, MaxConcurrent: 1, Resolve: func(ctx context.Context, ticket string) (*exporthttp.Request, error) {
		calls.Add(1)
		if deny.Load() {
			return nil, status.Error(codes.PermissionDenied, "denied")
		}
		return resolve(ctx, ticket)
	}}
	cancel, done := pending(t, h, "alice")
	waitStarted(t, started)
	w := request(h, http.MethodGet, "bob")
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.NotEmpty(t, w.Header().Get("Retry-After"))
	require.Equal(t, int32(1), calls.Load(), "busy exports do not run expensive resolver work")
	cancel()
	require.Equal(t, http.StatusRequestTimeout, waitPending(t, done).Code)
	// Authorization failure after a canceled request proves the global slot was
	// released without requiring another slow storage request.
	deny.Store(true)
	require.Equal(t, http.StatusForbidden, request(h, http.MethodGet, "next").Code)
}

func TestPerActorAdmissionAndCancellationRelease(t *testing.T) {
	b := fixture(t)
	started := make(chan string, 10)
	var complete atomic.Bool
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !complete.Load() {
			started <- r.URL.Query().Get("actor")
			<-r.Context().Done()
			return
		}
		from, to := requestedRange(t, r, len(b))
		serveRange(w, b, from, to)
	}))
	defer storage.Close()
	var calls atomic.Int32
	h := &exporthttp.Handler{AllowHTTP: true, MaxConcurrent: 3, Resolve: func(ctx context.Context, ticket string) (*exporthttp.Request, error) {
		calls.Add(1)
		return resolveRecording(b, storage.URL+"/?actor="+ticket)(ctx, ticket)
	}}
	cancelAlice, alice := pending(t, h, "alice")
	require.Equal(t, "alice", waitStarted(t, started))
	require.Equal(t, http.StatusTooManyRequests, request(h, http.MethodGet, "alice").Code)
	require.Equal(t, int32(2), calls.Load(), "actor identity is resolved before per-actor admission")
	cancelBob, bob := pending(t, h, "bob")
	require.Equal(t, "bob", waitStarted(t, started), "a different actor has an independent slot")
	cancelAlice()
	cancelBob()
	require.Equal(t, http.StatusRequestTimeout, waitPending(t, alice).Code)
	require.Equal(t, http.StatusRequestTimeout, waitPending(t, bob).Code)
	complete.Store(true)
	require.Equal(t, http.StatusOK, request(h, http.MethodGet, "alice").Code, "canceled exports release actor and global slots")
}

func TestPreparationTimeoutCancelsStorage(t *testing.T) {
	b := fixture(t)
	started := make(chan string, 1)
	stopped := make(chan struct{}, 1)
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- "storage"
		<-r.Context().Done()
		stopped <- struct{}{}
	}))
	defer storage.Close()
	h := &exporthttp.Handler{AllowHTTP: true, MaxDuration: 50 * time.Millisecond, Resolve: resolveRecording(b, storage.URL)}
	_, done := pending(t, h, "alice")
	waitStarted(t, started)
	require.Equal(t, http.StatusRequestTimeout, waitPending(t, done).Code)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("preparation timeout left the upstream storage request running")
	}
}

func TestAuthorizationFailureReleasesGlobalSlot(t *testing.T) {
	var calls atomic.Int32
	h := &exporthttp.Handler{MaxConcurrent: 1, Resolve: func(context.Context, string) (*exporthttp.Request, error) {
		calls.Add(1)
		return nil, status.Error(codes.PermissionDenied, "denied")
	}}
	for range 3 {
		require.Equal(t, http.StatusForbidden, request(h, http.MethodGet, "alice").Code)
	}
	require.Equal(t, int32(3), calls.Load())
}
