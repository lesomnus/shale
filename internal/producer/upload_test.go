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

// fakeNode keeps an upload's bytes as a node does (§12.2, §12.5): a
// request at another offset than its own is refused with 409 and the
// offset, and the first request with bytes is cut short after `cut` of
// them, answered 503 with what it kept, as a node that stops under it.
type fakeNode struct {
	mu   sync.Mutex
	have []byte
	cut  int
	cuts int
}

func (n *fakeNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if r.Method == http.MethodHead {
		w.Header().Set(storage.HdrUploadOffset, strconv.Itoa(len(n.have)))
		w.WriteHeader(http.StatusOK)
		return
	}
	offset, _ := strconv.Atoi(r.Header.Get(storage.HdrUploadOffset))
	if offset != len(n.have) {
		w.Header().Set(storage.HdrUploadOffset, strconv.Itoa(len(n.have)))
		w.WriteHeader(http.StatusConflict)
		return
	}
	if n.cut > 0 && n.cuts == 0 {
		n.cuts++
		b := make([]byte, n.cut)
		k, _ := io.ReadFull(r.Body, b)
		n.have = append(n.have, b[:k]...)
		w.Header().Set(storage.HdrUploadOffset, strconv.Itoa(len(n.have)))
		w.Header().Set(storage.HdrRetryAfter, "1")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	b, _ := io.ReadAll(r.Body)
	n.have = append(n.have, b...)
	w.Header().Set(storage.HdrUploadComplete, "?1")
	w.WriteHeader(http.StatusCreated)
}

// A node that cut an upload short has our bytes past the offset we last
// told it: the 409 that says so is ours to carry on from, and the segment
// is stored whole on the same target. Bytes past anything we sent are not
// ours, and end the attempt (§12.5).
func TestUploadResumesPastTheStatedOffset(t *testing.T) {
	seg := NewSegment(time.Now(), nil)
	body := bytes.Repeat([]byte("0123456789abcdef"), 4<<10)
	seg.Write(body)
	seg.Close(time.Now())
	u := &Uploader{Cfg: UploadConfig{ResumeTimeout: 10 * time.Second}, Laminae: noLaminae{}, Log: slog.New(slog.DiscardHandler)}

	n := &fakeNode{cut: 20_000}
	srv := httptest.NewServer(n)
	defer srv.Close()
	res := u.Upload(context.Background(), allocationAt(t, srv.Listener.Addr().String(), "laminae/cut"), seg)
	require.True(t, res.Stored, "%v", res.Err)
	require.Equal(t, 1, res.Attempts, "the same target")
	require.Equal(t, body, n.have)

	foreign := &fakeNode{have: []byte("an earlier incarnation's")}
	srv = httptest.NewServer(foreign)
	defer srv.Close()
	res = u.Upload(context.Background(), allocationAt(t, srv.Listener.Addr().String(), "laminae/foreign"), seg)
	require.False(t, res.Stored)
	require.Equal(t, "an earlier incarnation's", string(foreign.have), "not continued")
}
