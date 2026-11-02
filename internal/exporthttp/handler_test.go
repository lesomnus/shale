package exporthttp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"mime"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/internal/exporthttp"
	"github.com/lesomnus/shale/internal/fmp4"
)

var epoch = time.Date(2026, 10, 30, 11, 0, 0, 0, time.UTC)

func fixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../fmp4/testdata/sample.mp4")
	require.NoError(t, err)
	return b
}

func requestedRange(t *testing.T, r *http.Request, size int) (int, int) {
	t.Helper()
	require.Equal(t, http.MethodGet, r.Method)
	require.Equal(t, "identity", r.Header.Get("Accept-Encoding"))
	var from, to int
	n, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &from, &to)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.GreaterOrEqual(t, from, 0)
	require.GreaterOrEqual(t, to, from)
	require.Less(t, to, size)
	require.LessOrEqual(t, to-from+1, 1<<20, "storage reads stay bounded")
	return from, to
}

func serveRange(w http.ResponseWriter, b []byte, from, to int) {
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", from, to, len(b)))
	w.Header().Set("Content-Length", strconv.Itoa(to-from+1))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(b[from : to+1])
}

func resolveRecording(b []byte, endpoint string) func(context.Context, string) (*exporthttp.Request, error) {
	return func(_ context.Context, token string) (*exporthttp.Request, error) {
		return &exporthttp.Request{
			Source: "camera", Actor: token, From: epoch, To: epoch.Add(time.Hour),
			Recordings: []exporthttp.Recording{{Start: epoch, Size: int64(len(b)), URL: endpoint}},
		}, nil
	}
}

func request(h http.Handler, method, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "/exports/recording.mp4?token="+url.QueryEscape(token), nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	previousOutput, previousFlags := log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previous)
		log.SetOutput(previousOutput)
		log.SetFlags(previousFlags)
	})
	return &logs
}

func checkAdmissionLog(t *testing.T, logs *bytes.Buffer, code codes.Code, httpStatus int) {
	t.Helper()
	var entry map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &entry))
	require.Len(t, entry, 5, "only time, level, event and the two status attributes are logged")
	require.Equal(t, "WARN", entry["level"])
	require.Equal(t, "MP4 export admission failed", entry["msg"])
	require.Equal(t, code.String(), entry["grpc_code"])
	require.Equal(t, float64(httpStatus), entry["http_status"])
}

// The endpoint emits a conventional attachment with one final moov, while its
// storage access consists only of bounded, exact range requests.
func TestDownload(t *testing.T) {
	b := fixture(t)
	var reads atomic.Int32
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, to := requestedRange(t, r, len(b))
		reads.Add(1)
		serveRange(w, b, from, to)
	}))
	defer storage.Close()
	resolve := resolveRecording(b, storage.URL+"/recording?access=private-storage-token")
	h := &exporthttp.Handler{AllowHTTP: true, Resolve: func(ctx context.Context, ticket string) (*exporthttp.Request, error) {
		r, err := resolve(ctx, ticket)
		r.Source = "../../camera\r\nX-Injected: yes/카메라"
		return r, err
	}}
	w := request(h, http.MethodGet, "viewer")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, "video/mp4", w.Header().Get("Content-Type"))
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	require.Equal(t, "no-referrer", w.Header().Get("Referrer-Policy"))
	require.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	require.Equal(t, "none", w.Header().Get("Accept-Ranges"))
	require.Equal(t, strconv.Itoa(w.Body.Len()), w.Header().Get("Content-Length"))
	kind, params, err := mime.ParseMediaType(w.Header().Get("Content-Disposition"))
	require.NoError(t, err)
	require.Equal(t, "attachment", kind)
	require.NotContains(t, params["filename"], "/")
	require.NotContains(t, params["filename"], "\r")
	require.NotContains(t, params["filename"], "\n")
	require.True(t, strings.HasSuffix(params["filename"], ".mp4"))
	boxes, err := fmp4.Children(w.Body.Bytes())
	require.NoError(t, err)
	require.Len(t, boxes, 3)
	require.Equal(t, "ftyp", boxes[0].Type)
	require.Equal(t, "mdat", boxes[1].Type)
	require.Equal(t, "moov", boxes[2].Type)
	require.Greater(t, reads.Load(), int32(3))
}

func TestRequestValidation(t *testing.T) {
	var calls atomic.Int32
	h := &exporthttp.Handler{Resolve: func(context.Context, string) (*exporthttp.Request, error) {
		calls.Add(1)
		return nil, errors.New("should not resolve")
	}}
	for _, method := range []string{http.MethodHead, http.MethodPost, http.MethodPut} {
		t.Run(method, func(t *testing.T) {
			w := request(h, method, "ticket")
			require.Equal(t, http.StatusMethodNotAllowed, w.Code)
			require.Equal(t, "GET", w.Header().Get("Allow"))
		})
	}
	for _, token := range []string{"", strings.Repeat("a", 16385)} {
		w := request(h, http.MethodGet, token)
		require.Equal(t, http.StatusUnauthorized, w.Code)
	}
	r := httptest.NewRequest(http.MethodGet, "/exports/recording.mp4?token=ticket", nil)
	r.Header.Set("Range", "bytes=0-100")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Zero(t, calls.Load())
	require.Equal(t, http.StatusServiceUnavailable, request(&exporthttp.Handler{}, http.MethodGet, "ticket").Code)
}

func TestAuthorizationErrorsDoNotLeak(t *testing.T) {
	const secret = "https://storage.example/secret?token=private-storage-token"
	const ticket = "private-export-ticket"
	for _, tc := range []struct {
		code    codes.Code
		want    int
		message string
	}{
		{codes.InvalidArgument, 400, "invalid export request"},
		{codes.Unauthenticated, 401, "this export link is invalid or expired; request a new export"},
		{codes.PermissionDenied, 403, "you are not permitted to download this export"},
		{codes.NotFound, 404, "no recording is available for this export range"},
		{codes.ResourceExhausted, 429, "export limits were exceeded; request a shorter range"},
		{codes.Unavailable, 503, "exports are temporarily unavailable; try again later"},
		{codes.FailedPrecondition, 503, "exports are temporarily unavailable; try again later"},
		{codes.Canceled, 408, "export preparation was canceled or timed out; try again"},
		{codes.DeadlineExceeded, 408, "export preparation was canceled or timed out; try again"},
		{codes.Unknown, 500, "cannot prepare this export; try again later"},
		{codes.AlreadyExists, 500, "cannot prepare this export; try again later"},
		{codes.Aborted, 500, "cannot prepare this export; try again later"},
		{codes.OutOfRange, 500, "cannot prepare this export; try again later"},
		{codes.Unimplemented, 500, "cannot prepare this export; try again later"},
		{codes.Internal, 500, "cannot prepare this export; try again later"},
		{codes.DataLoss, 500, "cannot prepare this export; try again later"},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			logs := captureLogs(t)
			h := &exporthttp.Handler{Resolve: func(context.Context, string) (*exporthttp.Request, error) {
				return nil, status.Error(tc.code, secret)
			}}
			w := request(h, http.MethodGet, ticket)
			require.Equal(t, tc.want, w.Code)
			require.Equal(t, tc.message+"\n", w.Body.String())
			require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			require.Equal(t, "no-referrer", w.Header().Get("Referrer-Policy"))
			require.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
			require.Equal(t, "none", w.Header().Get("Accept-Ranges"))
			require.Empty(t, w.Header().Get("Content-Disposition"))
			checkAdmissionLog(t, logs, tc.code, tc.want)
			for _, private := range []string{secret, "storage.example", "private-storage-token", ticket} {
				require.NotContains(t, w.Body.String(), private)
				require.NotContains(t, logs.String(), private)
			}
		})
	}
	for _, r := range []*exporthttp.Request{nil, {Actor: ""}} {
		h := &exporthttp.Handler{Resolve: func(context.Context, string) (*exporthttp.Request, error) { return r, nil }}
		require.Equal(t, http.StatusForbidden, request(h, http.MethodGet, "ticket").Code)
	}
}

func TestNonStatusAdmissionErrorsRemainInternal(t *testing.T) {
	// Preserve the existing HTTP classification for plain errors, including
	// cancellation that was not returned as a gRPC status by the resolver.
	for name, err := range map[string]error{
		"plain storage failure": errors.New("https://storage.example/?token=private-storage-token"),
		"canceled":              context.Canceled,
		"deadline exceeded":     context.DeadlineExceeded,
	} {
		t.Run(name, func(t *testing.T) {
			logs := captureLogs(t)
			h := &exporthttp.Handler{Resolve: func(context.Context, string) (*exporthttp.Request, error) { return nil, err }}
			w := request(h, http.MethodGet, "private-export-ticket")
			require.Equal(t, http.StatusInternalServerError, w.Code)
			require.Equal(t, "cannot prepare this export; try again later\n", w.Body.String())
			checkAdmissionLog(t, logs, codes.Unknown, http.StatusInternalServerError)
			for _, private := range []string{"storage.example", "private-storage-token", "private-export-ticket"} {
				require.NotContains(t, w.Body.String(), private)
				require.NotContains(t, logs.String(), private)
			}
		})
	}
}

func TestStorageResponseValidation(t *testing.T) {
	b := fixture(t)
	for _, fault := range []string{"full response", "wrong start", "wrong total", "short body", "extra body", "encoded body"} {
		t.Run(fault, func(t *testing.T) {
			storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				from, to := requestedRange(t, r, len(b))
				if fault == "full response" {
					_, _ = w.Write(b)
					return
				}
				contentRange := fmt.Sprintf("bytes %d-%d/%d", from, to, len(b))
				if fault == "wrong start" {
					contentRange = fmt.Sprintf("bytes %d-%d/%d", from+1, to+1, len(b))
				}
				if fault == "wrong total" {
					contentRange = fmt.Sprintf("bytes %d-%d/%d", from, to, len(b)+1)
				}
				if fault == "encoded body" {
					w.Header().Set("Content-Encoding", "gzip")
				}
				w.Header().Set("Content-Range", contentRange)
				w.WriteHeader(http.StatusPartialContent)
				payload := append([]byte(nil), b[from:to+1]...)
				if fault == "short body" {
					payload = payload[:len(payload)-1]
				}
				if fault == "extra body" {
					payload = append(payload, 1)
				}
				_, _ = w.Write(payload)
			}))
			defer storage.Close()
			h := &exporthttp.Handler{AllowHTTP: true, Resolve: resolveRecording(b, storage.URL+"/?token=private")}
			w := request(h, http.MethodGet, "ticket")
			require.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())
			require.NotContains(t, w.Body.String(), "private")
		})
	}
}

func TestStorageEndpointPolicy(t *testing.T) {
	b := fixture(t)
	var traps atomic.Int32
	trap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { traps.Add(1) }))
	defer trap.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, trap.URL+"/?token=private", http.StatusFound)
	}))
	defer redirect.Close()
	h := &exporthttp.Handler{AllowHTTP: true, Resolve: resolveRecording(b, redirect.URL+"/?token=private")}
	require.Equal(t, http.StatusBadGateway, request(h, http.MethodGet, "ticket").Code)
	require.Zero(t, traps.Load(), "credentials never follow redirects")
	for _, endpoint := range []string{trap.URL, "https://user:secret@example.com/file", "https://example.com/file#fragment", "file:///tmp/file.mp4"} {
		h := &exporthttp.Handler{Resolve: resolveRecording(b, endpoint)}
		require.Equal(t, http.StatusBadGateway, request(h, http.MethodGet, "ticket").Code)
	}
	require.Zero(t, traps.Load(), "HTTP is rejected before reaching storage unless enabled")
	secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, to := requestedRange(t, r, len(b))
		serveRange(w, b, from, to)
	}))
	defer secure.Close()
	h = &exporthttp.Handler{Client: secure.Client(), Resolve: resolveRecording(b, secure.URL)}
	require.Equal(t, http.StatusOK, request(h, http.MethodGet, "ticket").Code)
}

func TestExpiredStorageCredentialRenewal(t *testing.T) {
	b := fixture(t)
	var stale, fresh, renewals atomic.Int32
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("access") == "old" {
			stale.Add(1)
			http.Error(w, "expired", http.StatusForbidden)
			return
		}
		fresh.Add(1)
		from, to := requestedRange(t, r, len(b))
		serveRange(w, b, from, to)
	}))
	defer storage.Close()
	resolve := resolveRecording(b, storage.URL+"/?access=old")
	h := &exporthttp.Handler{AllowHTTP: true, Resolve: func(ctx context.Context, ticket string) (*exporthttp.Request, error) {
		r, err := resolve(ctx, ticket)
		r.Recordings[0].Refresh = func(context.Context) (string, error) {
			renewals.Add(1)
			return storage.URL + "/?access=new", nil
		}
		return r, err
	}}
	require.Equal(t, http.StatusOK, request(h, http.MethodGet, "ticket").Code)
	require.Equal(t, int32(1), stale.Load())
	require.Equal(t, int32(1), renewals.Load())
	require.Greater(t, fresh.Load(), int32(1))
}

func TestStorageCredentialRenewalIsBounded(t *testing.T) {
	b := fixture(t)
	for _, fault := range []string{"still expired", "renewal denied", "insecure renewal"} {
		t.Run(fault, func(t *testing.T) {
			var reads, renewals atomic.Int32
			storage := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads.Add(1)
				http.Error(w, "expired", http.StatusForbidden)
			}))
			defer storage.Close()
			resolve := resolveRecording(b, storage.URL)
			h := &exporthttp.Handler{Client: storage.Client(), Resolve: func(ctx context.Context, ticket string) (*exporthttp.Request, error) {
				r, err := resolve(ctx, ticket)
				r.Recordings[0].Refresh = func(context.Context) (string, error) {
					renewals.Add(1)
					if fault == "renewal denied" {
						return "", errors.New("private authorization revoked")
					}
					if fault == "insecure renewal" {
						return strings.Replace(storage.URL, "https:", "http:", 1), nil
					}
					return storage.URL, nil
				}
				return r, err
			}}
			w := request(h, http.MethodGet, "ticket")
			require.Equal(t, http.StatusBadGateway, w.Code)
			require.NotContains(t, w.Body.String(), "private")
			require.Equal(t, int32(1), renewals.Load())
			wantReads := int32(1)
			if fault == "still expired" {
				wantReads = 2
			}
			require.Equal(t, wantReads, reads.Load(), "failed renewal is not retried indefinitely")
		})
	}
}

func TestCodecChangeFailsBeforeMedia(t *testing.T) {
	b := fixture(t)
	changed := bytes.Clone(b)
	config := bytes.Index(changed, []byte("avcC"))
	require.Greater(t, config, 0)
	changed[config+5] ^= 1 // Same container dimensions, a different AVC profile.
	var mediaReads atomic.Int32
	boxes, err := fmp4.Children(b)
	require.NoError(t, err)
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, to := requestedRange(t, r, len(b))
		for _, box := range boxes {
			if box.Type == "mdat" && from >= box.Off+box.Hdr && from < box.End {
				mediaReads.Add(1)
			}
		}
		payload := b
		if r.URL.Path == "/changed" {
			payload = changed
		}
		serveRange(w, payload, from, to)
	}))
	defer storage.Close()
	h := &exporthttp.Handler{AllowHTTP: true, Resolve: func(context.Context, string) (*exporthttp.Request, error) {
		return &exporthttp.Request{Source: "cam", Actor: "alice", From: epoch, To: epoch.Add(time.Hour), Recordings: []exporthttp.Recording{
			{Start: epoch, Size: int64(len(b)), URL: storage.URL + "/original"},
			{Start: epoch.Add(time.Minute), Size: int64(len(changed)), URL: storage.URL + "/changed"},
		}}, nil
	}}
	w := request(h, http.MethodGet, "ticket")
	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	require.Contains(t, w.Body.String(), "configuration changed")
	require.Empty(t, w.Header().Get("Content-Disposition"))
	require.Zero(t, mediaReads.Load(), "all codec descriptions are validated before any media is streamed")
}

func TestInvalidContainerFailsBeforeMedia(t *testing.T) {
	b := []byte("invalid MP4 stored upstream")
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, to := requestedRange(t, r, len(b))
		serveRange(w, b, from, to)
	}))
	defer storage.Close()
	h := &exporthttp.Handler{AllowHTTP: true, Resolve: resolveRecording(b, storage.URL)}
	w := request(h, http.MethodGet, "ticket")
	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	require.Empty(t, w.Header().Get("Content-Disposition"))
	require.NotEqual(t, "video/mp4", w.Header().Get("Content-Type"))
}

// A failure after media bytes were sent is a truncated download, not a 200 MP4
// containing an error message or a successful short file.
func TestLateStorageFailureAbortsDownload(t *testing.T) {
	b := fixture(t)
	boxes, err := fmp4.Children(b)
	require.NoError(t, err)
	var mediaReads atomic.Int32
	storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		from, to := requestedRange(t, r, len(b))
		for _, box := range boxes {
			if box.Type == "mdat" && from >= box.Off+box.Hdr && from < box.End {
				if mediaReads.Add(1) > 1 {
					http.Error(w, "private upstream failure", http.StatusInternalServerError)
					return
				}
			}
		}
		serveRange(w, b, from, to)
	}))
	defer storage.Close()
	h := &exporthttp.Handler{AllowHTTP: true, Resolve: resolveRecording(b, storage.URL)}
	server := httptest.NewServer(h)
	defer server.Close()
	resp, err := http.Get(server.URL + "/exports/recording.mp4?token=ticket")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	payload, err := io.ReadAll(resp.Body)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.NotEmpty(t, payload)
	require.Less(t, int64(len(payload)), resp.ContentLength)
	require.False(t, bytes.Contains(payload, []byte("private upstream failure")))
	require.Greater(t, mediaReads.Load(), int32(1))
}
