package producer

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/server/core"
)

// burstCP allocates as the CP does while no commit has reached it
// (§12.1): every lamina it made is PENDING, and a request it takes for a
// lamina's segment is answered that lamina again, with fresh tokens. The
// lamina it takes a request for is the last of the slot's to begin no
// later than the request's start and a keyframe interval and an encoder
// burst, unless the request names it as `after` (§15); a round of the
// prefetch starts at the slot's start. A request it takes for none is
// answered a lamina of its own, dated by the request.
type burstCP struct {
	node string
	d    time.Duration
	// ahead says the prefetch is answered before any segment asked for
	// an allocation; otherwise the burst's first segment asks first.
	ahead bool
	// rounds counts the prefetch's calls. The prefetch asks again only
	// once it has filed the answer before, so a round has been answered
	// and filed when the one after it is asked.
	rounds atomic.Int64

	mu      sync.Mutex
	slot    time.Time
	source  []byte
	laminae []burstLamina
	asked   int
}

// burstLamina is a lamina as the CP's row has it while PENDING.
type burstLamina struct {
	id      []byte
	started time.Time
}

type (
	burstSets struct {
		api.UnimplementedSetServiceServer
		*burstCP
	}
	burstLaminae struct {
		api.UnimplementedLaminaServiceServer
		*burstCP
	}
)

func (f burstSets) Negotiate(_ context.Context, req *api.SetNegotiateRequest) (*api.SetNegotiateResponse, error) {
	var agreed []*api.SourceAgreed
	for _, sp := range req.GetSources() {
		agreed = append(agreed, api.SourceAgreed_builder{
			SourceId: sp.GetSource().GetId(),
			Profile:  api.SegmentProfile_builder{MaxBitrate: sp.GetProfile().GetMaxBitrate(), DurationSeconds: int64(f.d.Seconds()), KeyframeIntervalMs: 2000}.Build(),
		}.Build())
	}

	return api.SetNegotiateResponse_builder{
		ProfileVersion: 1,
		Link:           api.LinkProfile_builder{Mode: api.UploadMode_UPLOAD_MODE_LIVE, AllocationHorizonSeconds: 1}.Build(),
		Sources:        agreed,
	}.Build(), nil
}

func (f burstSets) Allocate(context.Context, *api.SetAllocateRequest) (*api.SetAllocateResponse, error) {
	defer f.rounds.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.slot.IsZero() || (!f.ahead && f.asked == 0) {
		return api.SetAllocateResponse_builder{ProfileVersion: 1}.Build(), nil
	}

	return api.SetAllocateResponse_builder{ProfileVersion: 1, Allocations: []*api.Allocation{f.allocation(f.slot, nil)}}.Build(), nil
}

func (f burstLaminae) Allocate(_ context.Context, req *api.LaminaAllocateRequest) (*api.Allocation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked++

	return f.allocation(req.GetDateStarted().AsTime(), req.GetAfter().GetId()), nil
}

// allocation answers a request that starts at `started` and names
// `after`, with the lamina's one candidate on the node. The lock is held.
func (f *burstCP) allocation(started time.Time, after []byte) *api.Allocation {
	slack := 2*time.Second + core.EncoderBurst
	var existing *burstLamina
	for i := range f.laminae {
		if o := &f.laminae[i]; !o.started.After(started.Add(slack)) && (existing == nil || !o.started.Before(existing.started)) {
			existing = o
		}
	}
	if existing != nil && bytes.Equal(existing.id, after) {
		existing = nil
	}
	if existing == nil {
		f.laminae = append(f.laminae, burstLamina{id: pdid.New(core.DomLamina).Bytes(), started: started})
		existing = &f.laminae[len(f.laminae)-1]
	}

	host, port, _ := net.SplitHostPort(f.node)
	n, _ := strconv.Atoi(port)
	key := "laminae/" + pdid.Id(existing.id).String()

	return api.Allocation_builder{
		LaminaId:       existing.id,
		SourceId:       f.source,
		DateStarted:    timestamppb.New(started),
		ProfileVersion: 1,
		DateExpires:    timestamppb.New(time.Now().Add(22 * time.Minute)),
		LaminaKey:      key,
		Candidates: []*api.Candidate{api.Candidate_builder{
			AttemptId: existing.id,
			Token:     "tok",
			LaminaKey: key,
			Endpoints: []*api.Endpoint{api.Endpoint_builder{Scheme: "http", Host: host, Port: int32(n)}.Build()},
		}.Build()},
	}.Build()
}

// burstNode keeps what is put at each key, as a node does (§12.2, §12.5):
// a complete key is answered 200 and complete without a byte read, since
// the producer may have missed its 201. It answers an upload only once
// the prefetch has asked twice since the upload began, so that a round of
// it is filed after the segment was handed its allocation and before the
// next segment asks for one.
type burstNode struct {
	cp *burstCP

	mu     sync.Mutex
	stored map[string][]byte
	puts   map[string]int
}

func (n *burstNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/")
	n.mu.Lock()
	b, complete := n.stored[key]
	n.puts[key]++
	n.mu.Unlock()
	if complete {
		w.Header().Set(storage.HdrUploadComplete, "?1")
		w.Header().Set(storage.HdrUploadOffset, strconv.Itoa(len(b)))
		w.WriteHeader(http.StatusOK)

		return
	}
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusNotFound)

		return
	}
	since := n.cp.rounds.Load()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}
	for n.cp.rounds.Load() < since+2 {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
	n.mu.Lock()
	n.stored[key] = b
	n.mu.Unlock()
	w.Header().Set(storage.HdrUploadComplete, "?1")
	w.WriteHeader(http.StatusCreated)
}

// A pushed source that bursts cuts several segments in one slot before the
// first one's commit reaches the CP, which answers that lamina again to
// every round of the prefetch while it is PENDING (§12.1, §15): a writer
// flushing what it kept offline, records a second apart in a slot of two
// minutes, each segment two records long by the ceiling. Every segment is
// uploaded under a lamina of its own, and the node holds every record
// once. A lamina handed to a second segment is answered 200 at its key,
// complete with the first one, which the producer counts as stored: the
// second segment is lost without a word.
//
// The slot's allocation is fetched ahead and in hand when the burst
// begins, or the burst's first segment asks for its own first, and the
// prefetch is then answered a lamina for the slot's start, which no
// segment of the burst began within a keyframe and a burst of.
func TestBurstInOneSlot(t *testing.T) {
	for _, c := range []struct {
		name  string
		ahead bool
	}{
		{"the slot's allocation in hand", true},
		{"the burst before the slot's allocation", false},
	} {
		t.Run(c.name, func(t *testing.T) { burstInOneSlot(t, c.ahead) })
	}
}

func burstInOneSlot(t *testing.T, ahead bool) {
	cp := &burstCP{d: 2 * time.Minute, ahead: ahead}
	node := &burstNode{cp: cp, stored: map[string][]byte{}, puts: map[string]int{}}
	srv := httptest.NewServer(node)
	defer srv.Close()
	cp.node = srv.Listener.Addr().String()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	gs := grpc.NewServer()
	api.RegisterSetServiceServer(gs, burstSets{burstCP: cp})
	api.RegisterLaminaServiceServer(gs, burstLaminae{burstCP: cp})
	go gs.Serve(lis)
	defer gs.Stop()
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	// A ceiling of 99,960 bytes in the slot: a record of 64 KiB is below
	// it and two are over, so a segment is cut at every second record
	// (§38.2, §38.9).
	const record = 64 << 10
	p, err := New(Config{
		StateDir: t.TempDir(),
		Push:     "unix:" + filepath.Join(t.TempDir(), "push.sock"),
		Sources:  []SourceConfig{{Alias: "robot", Input: InputPush, Kind: KindRaw, MaxBitrate: 6_664, ContentType: "application/x-mcap"}},
		Mode:     api.UploadMode_UPLOAD_MODE_LIVE,
		Log:      slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	// What Run has by the time the loops start: a connection, the set,
	// the source's row, the terms, and the uploader.
	p.conn.Store(conn)
	p.set = api.Set_builder{Id: pdid.New(core.DomSet).Bytes()}.Build()
	p.members = 1
	s := p.order[0]
	s.row = api.Source_builder{Id: pdid.New(core.DomSource).Bytes()}.Build()
	cp.source = s.row.GetId()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, p.negotiate(ctx))
	p.uploader = &Uploader{Laminae: api.NewLaminaServiceClient(p.cp()), Log: p.log, Mode: p.agreed().mode}
	require.NoError(t, p.push.Listen())

	s.mu.Lock()
	slot := s.sched.SlotOf(time.Now())
	s.mu.Unlock()
	cp.mu.Lock()
	cp.slot = slot
	cp.mu.Unlock()

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return p.push.Serve(gctx) })
	g.Go(func() error { return p.capture(gctx, s) })
	g.Go(func() error { return p.uploads(gctx, s) })
	g.Go(func() error { return p.allocations(gctx) })
	// A round of the prefetch is answered and filed before the burst.
	require.Eventually(t, func() bool { return cp.rounds.Load() >= 2 }, 10*time.Second, 10*time.Millisecond)

	// Eight records, each its own bytes, dated a second apart from ten
	// seconds into the slot, pushed at once: four segments.
	const records = 8
	var buf bytes.Buffer
	require.NoError(t, WriteFrame(&buf, &Frame{Kind: FramePrefix, Payload: []byte("HDR\n")}))
	t0 := slot.Add(10 * time.Second)
	for i := range records {
		require.NoError(t, WriteFrame(&buf, &Frame{Kind: FrameData, Time: t0.Add(time.Duration(i) * time.Second), Payload: bytes.Repeat([]byte{'a' + byte(i)}, record)}))
	}
	w := &Pusher{Addr: p.cfg.Push, Alias: "robot", Part: 256 << 10}
	_, err = w.Push(ctx, &buf, 0, true)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return p.Stats().Stored >= records/2 }, 30*time.Second, 10*time.Millisecond, "every segment stored, by the producer's count")
	require.Equal(t, int64(0), p.Stats().Lost)

	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, records/2, len(node.stored), "a lamina per segment; puts per key: %v", node.puts)
	seen := map[byte]int{}
	for key, b := range node.stored {
		require.True(t, bytes.HasPrefix(b, []byte("HDR\n")), "%s starts with the prefix", key)
		rest := b[4:]
		require.Zero(t, len(rest)%record, "%s holds whole records", key)
		for ; len(rest) > 0; rest = rest[record:] {
			seen[rest[0]]++
		}
	}
	for i := range records {
		require.Equal(t, 1, seen['a'+byte(i)], "record %d is in exactly one lamina; puts per key: %v", i, node.puts)
	}
	cancel()
	require.NoError(t, g.Wait())
}
