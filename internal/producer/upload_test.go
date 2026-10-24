package producer

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/storage"
)

// noLaminae is a CP that cannot be reached: an upload that needs it to
// report or reallocate gets no answer.
type noLaminae struct{ api.LaminaServiceClient }

func (noLaminae) ReportAttempt(context.Context, *api.LaminaReportAttemptRequest, ...grpc.CallOption) (*api.Attempt, error) {
	return nil, status.Error(codes.Unavailable, "no CP")
}

func (noLaminae) Reallocate(context.Context, *api.LaminaReallocateRequest, ...grpc.CallOption) (*api.Allocation, error) {
	return nil, status.Error(codes.Unavailable, "no CP")
}

// allocationAt is an allocation of one candidate on a node at addr.
func allocationAt(t *testing.T, addr, key string) *api.Allocation {
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	n, err := strconv.Atoi(port)
	require.NoError(t, err)

	return api.Allocation_builder{
		LaminaId:  []byte(key),
		LaminaKey: key,
		Candidates: []*api.Candidate{api.Candidate_builder{
			AttemptId: []byte(key),
			Token:     "tok",
			LaminaKey: key,
			Endpoints: []*api.Endpoint{api.Endpoint_builder{Scheme: "http", Host: host, Port: int32(n)}.Build()},
		}.Build()},
	}.Build()
}

// A node that refuses a live upload it is being sent (§13), as one whose
// sink takes no writes does: 503 without reading the body. The answer
// comes while the transport is still sending the segment, which the
// capture goes on writing, and the transport reads on after Do has
// answered; what the request sent is counted as the transport reads it.
// The upload waits out the Retry-After and goes again to the same target,
// which takes it whole.
func TestUploadRefusedMidBody(t *testing.T) {
	var puts atomic.Int32
	var mu sync.Mutex
	var got []byte
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if puts.Add(1) == 1 {
			w.Header().Set(storage.HdrRetryAfter, "1")
			http.Error(w, "the sink takes no writes now", http.StatusServiceUnavailable)
			return
		}
		b, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		mu.Lock()
		got = b
		mu.Unlock()
		w.Header().Set(storage.HdrUploadComplete, "?1")
		w.WriteHeader(http.StatusCreated)
	}))
	defer node.Close()

	seg := NewSegment(time.Now(), []byte("tables"))
	chunk := bytes.Repeat([]byte{0x5a}, 16<<10)
	go func() {
		for range 40 {
			seg.Write(chunk)
			time.Sleep(30 * time.Millisecond)
		}
		seg.Close(time.Now())
	}()

	u := &Uploader{Cfg: UploadConfig{ResumeTimeout: 30 * time.Second}, Laminae: noLaminae{}, Log: slog.New(slog.DiscardHandler)}
	res := u.Upload(context.Background(), allocationAt(t, node.Listener.Addr().String(), "laminae/refused"), seg)
	require.True(t, res.Stored, "%v", res.Err)
	require.Equal(t, 1, res.Attempts, "again on the same target")
	require.Equal(t, int32(2), puts.Load(), "the refused request and the one after its Retry-After")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, seg.Bytes(), got)
}

// Every source's upload loop calls the producer's one uploader (§16), the
// first calls among them at once: what the configuration leaves out is
// filled in once, and every segment goes.
func TestUploadsAtOnce(t *testing.T) {
	var stored atomic.Int32
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		stored.Add(1)
		w.Header().Set(storage.HdrUploadComplete, "?1")
		w.WriteHeader(http.StatusCreated)
	}))
	defer node.Close()

	u := &Uploader{Laminae: noLaminae{}, Log: slog.New(slog.DiscardHandler)}
	const sources = 4
	var wg sync.WaitGroup
	results := make([]Result, sources)
	for i := range sources {
		seg := NewSegment(time.Now(), bytes.Repeat([]byte{byte(i)}, 64<<10))
		seg.Close(time.Now())
		al := allocationAt(t, node.Listener.Addr().String(), "laminae/at-once-"+strconv.Itoa(i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = u.Upload(context.Background(), al, seg)
		}()
	}
	wg.Wait()
	for i, res := range results {
		require.True(t, res.Stored, "source %d: %v", i, res.Err)
	}
	require.Equal(t, int32(sources), stored.Load())
}
