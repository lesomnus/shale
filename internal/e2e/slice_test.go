// Package e2e is the end-to-end harness (§15 of the plan): a synthetic
// producer and reader against `shale serve all --dev` in this process.
package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/payday/auth"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cli"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/internal/token"
)

// cluster is one `serve all --dev` in a temporary directory.
type cluster struct {
	t       *testing.T
	cfg     *cmd.Config
	running cli.Running
	cancel  context.CancelFunc
	done    chan error
}

func start(t *testing.T, opts ...func(*cmd.Config)) *cluster {
	t.Helper()
	dir := t.TempDir()
	c := &cmd.Config{}
	cli.ApplyDev(c, dir)
	c.Server.Addr = "127.0.0.1:0"
	c.Cluster.Addr = "127.0.0.1:0"
	c.Storage.Addr = "127.0.0.1:0"
	c.Storage.ControlAddr = "127.0.0.1:0"
	// A declared capacity: the sink is a directory on a shared filesystem,
	// whose own free space says nothing about the test (§22.2).
	c.Storage.Sinks = []cmd.SinkConfig{{Path: filepath.Join(dir, "sink"), Capacity: "4GiB"}}
	c.Storage.HeartbeatInterval = time.Second
	c.Control.DirectivesEvery = 500 * time.Millisecond
	c.Relay.IngestAddr = "127.0.0.1:0"
	c.Relay.WhepAddr = "127.0.0.1:0"
	c.Relay.IdleStop = time.Second
	for _, o := range opts {
		o(c)
	}
	// SHALE_E2E_DB_DSN runs the suite on PostgreSQL with the LISTEN/NOTIFY
	// broker and the leader lease (§34.2): the database is emptied first.
	if dsn := os.Getenv("SHALE_E2E_DB_DSN"); dsn != "" {
		c.Db.Driver, c.Db.Dsn, c.Watch.Broker = "pgx", dsn, "postgres"
		db, err := sql.Open("pgx", dsn)
		require.NoError(t, err)
		_, err = db.Exec("drop schema public cascade; create schema public")
		require.NoError(t, err)
		db.Close()
	}

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, cli.Init(ctx, c, "acme", "admin", "ops", io.Discard))

	ready := make(chan cli.Running, 1)
	done := make(chan error, 1)
	go func() { done <- cli.ServeAll(ctx, c, func(r cli.Running) { ready <- r }) }()

	var r cli.Running
	select {
	case r = <-ready:
	case err := <-done:
		cancel()
		t.Fatalf("serve all: %v", err)
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("serve all did not come up")
	}
	select {
	case <-r.Node.Ready:
	case err := <-done:
		cancel()
		t.Fatalf("serve all: %v", err)
	case <-time.After(30 * time.Second):
		cancel()
		t.Fatal("the node did not come up")
	}

	cl := &cluster{t: t, cfg: c, running: r, cancel: cancel, done: done}
	// The node is up once its first heartbeat registered its sink.
	ops := api.NewSinkServiceClient(cl.dialCluster("@cluster/ops"))
	require.Eventually(t, func() bool {
		vs, err := ops.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, s := range vs.GetItems() {
			if s.GetDateSeen() != nil && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED {
				return true
			}
		}

		return false
	}, 30*time.Second, 100*time.Millisecond, "the node's sink is registered")
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	})

	return cl
}

// dial is the tenant API as somebody, with the plain header of development
// mode.
func (c *cluster) dial(as string) *grpc.ClientConn {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	opts = append(opts, auth.Inject(auth.PlainProvider(as))...)
	conn, err := grpc.NewClient(c.running.TenantAddr, opts...)
	require.NoError(c.t, err)
	c.t.Cleanup(func() { conn.Close() })

	return conn
}

func setRef(alias string) *api.SetRef {
	return api.SetRef_builder{Slug: api.SetRefBySlug_builder{Alias: z.Ptr(alias), Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build()}.Build()}.Build()
}

// put uploads one buffered segment (§12.2) and answers the status.
func put(t *testing.T, al *api.Allocation, body []byte, ended time.Time) int {
	t.Helper()
	cand := al.GetCandidates()[0]
	ep := cand.GetEndpoints()[0]
	url := fmt.Sprintf("%s://%s:%d/%s", ep.GetScheme(), ep.GetHost(), ep.GetPort(), al.GetLaminaKey())
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
	req.Header.Set(storage.HdrUploadOffset, "0")
	req.Header.Set(storage.HdrUploadComplete, "?1")
	req.Header.Set(storage.HdrDateStarted, al.GetDateStarted().AsTime().Format(time.RFC3339Nano))
	req.Header.Set(storage.HdrDateEnded, ended.Format(time.RFC3339Nano))
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	return resp.StatusCode
}

// TestVerticalSlice is E1's acceptance: 100 segments from a synthetic
// producer, every one read back through Timeline, with correct gaps for the
// ones it skipped.
func TestVerticalSlice(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	conn := c.dial("@acme/admin")

	sets := api.NewSetServiceClient(conn)
	sources := api.NewSourceServiceClient(conn)
	laminae := api.NewLaminaServiceClient(conn)

	set, err := sets.Add(ctx, api.SetAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(),
		Alias:  "cam-set",
	}.Build())
	require.NoError(t, err)

	const members = 2
	var srcs []*api.Source
	for i := range members {
		s, err := sources.Add(ctx, api.SourceAddRequest_builder{
			Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(),
			Alias:  fmt.Sprintf("cam-%02d", i+1),
			Set:    api.SetRef_builder{Id: set.GetId()}.Build(),
		}.Build())
		require.NoError(t, err)
		require.Equal(t, int32(i), s.GetOrdinal(), "ordinals are assigned in order")
		srcs = append(srcs, s)
	}

	// Negotiate: 4 Mbps ceilings, buffered uploads.
	var proposals []*api.SourceProposal
	for _, s := range srcs {
		proposals = append(proposals, api.SourceProposal_builder{
			Source:  api.SourceRef_builder{Id: s.GetId()}.Build(),
			Profile: api.SegmentProfile_builder{MaxBitrate: 4_000_000}.Build(),
		}.Build())
	}
	neg, err := sets.Negotiate(ctx, api.SetNegotiateRequest_builder{
		Ref:     setRef("cam-set"),
		Link:    api.LinkProfile_builder{Mode: api.UploadMode_UPLOAD_MODE_BUFFERED}.Build(),
		Sources: proposals,
	}.Build())
	require.NoError(t, err)
	require.Len(t, neg.GetSources(), members)
	dur := time.Duration(neg.GetSources()[0].GetProfile().GetDurationSeconds()) * time.Second
	require.Greater(t, dur, time.Minute)

	// The synthetic producer: a backlog of 50 segments per source, uploaded
	// as buffered segments except every seventh, which it skips.
	const perSource = 50
	base := time.Now().Add(-time.Duration(perSource+2) * dur).Truncate(time.Second)
	type seg struct {
		al   *api.Allocation
		body []byte
		skip bool
		end  time.Time
	}
	segs := map[string][]seg{}
	uploaded := 0
	for _, s := range srcs {
		for i := range perSource {
			al, err := laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{
				Source:      api.SourceRef_builder{Id: s.GetId()}.Build(),
				DateStarted: timestamppb.New(base.Add(time.Duration(i) * dur)),
			}.Build())
			require.NoError(t, err)
			require.NotEmpty(t, al.GetCandidates())

			// Idempotent per slot: the same lamina again.
			again, err := laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{
				Source:      api.SourceRef_builder{Id: s.GetId()}.Build(),
				DateStarted: al.GetDateStarted(),
			}.Build())
			require.NoError(t, err)
			require.Equal(t, al.GetLaminaId(), again.GetLaminaId())

			body := make([]byte, 64<<10+i)
			rand.Read(body)
			sg := seg{al: al, body: body, skip: i%7 == 3, end: al.GetDateStarted().AsTime().Add(dur)}
			if !sg.skip {
				require.Equal(t, http.StatusCreated, put(t, al, body, sg.end))
				uploaded++
			}
			segs[string(s.GetId())] = append(segs[string(s.GetId())], sg)
		}
	}
	require.Equal(t, 100, members*perSource)

	// The commits reach the index within a moment.
	require.Eventually(t, func() bool {
		n := 0
		after := ""
		for {
			vs, err := laminae.List(ctx, api.LaminaListRequest_builder{
				Filters: []*api.LaminaFilter{api.LaminaFilter_builder{Set: api.SetRef_builder{Id: set.GetId()}.Build()}.Build()},
				Size:    1000, After: after,
			}.Build())
			if err != nil {
				return false
			}
			for _, o := range vs.GetItems() {
				if o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED {
					n++
				}
			}
			if vs.GetNext() == "" {
				break
			}
			after = vs.GetNext()
		}

		return n == uploaded
	}, 30*time.Second, 200*time.Millisecond, "every upload is committed")

	// Timeline over the whole range, paged.
	from, to := base.Add(-time.Minute), base.Add(time.Duration(perSource)*dur+time.Minute)
	got := map[string]*api.TimelineSource{}
	after := ""
	pages := 0
	for {
		tl, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{
			Set: api.SetRef_builder{Id: set.GetId()}.Build(), From: timestamppb.New(from), To: timestamppb.New(to), Size: 30, After: after,
		}.Build())
		require.NoError(t, err)
		pages++
		for _, ts := range tl.GetSources() {
			if g, ok := got[string(ts.GetSourceId())]; ok {
				g.SetLaminae(append(g.GetLaminae(), ts.GetLaminae()...))
				g.SetGaps(append(g.GetGaps(), ts.GetGaps()...))
			} else {
				got[string(ts.GetSourceId())] = ts
			}
		}
		if tl.GetNext() == "" {
			break
		}
		after = tl.GetNext()
	}
	require.Greater(t, pages, 1, "a page of 30 over 100 laminae is several pages")

	for _, s := range srcs {
		ts := got[string(s.GetId())]
		require.NotNil(t, ts)
		mine := segs[string(s.GetId())]
		var want []seg
		for _, sg := range mine {
			if !sg.skip {
				want = append(want, sg)
			}
		}
		require.Len(t, ts.GetLaminae(), len(want), "every uploaded segment is listed")
		for i, o := range ts.GetLaminae() {
			require.Equal(t, want[i].al.GetLaminaId(), o.GetLaminaId(), "in time order")
			require.Equal(t, api.ReadState_READ_STATE_AVAILABLE, o.GetState())
			require.Equal(t, int64(len(want[i].body)), o.GetSize())
			require.NotEmpty(t, o.GetUrl())
		}

		// Every skipped slot is a gap covering its span, IN_PROGRESS while
		// its attempt is open (§19).
		for _, sg := range mine {
			if !sg.skip {
				continue
			}
			start := sg.al.GetDateStarted().AsTime()
			found := false
			for _, g := range ts.GetGaps() {
				if !g.GetFrom().AsTime().After(start) && !g.GetTo().AsTime().Before(sg.end) {
					require.Equal(t, api.GapReason_GAP_REASON_IN_PROGRESS, g.GetReason())
					found = true
				}
			}
			require.True(t, found, "a gap for the skipped slot at %s", start)
		}
		// The window before the first segment, when there is one, is
		// NOT_RECEIVED; and no gap is a sliver, since every date is whole
		// milliseconds.
		if first := ts.GetLaminae()[0].GetDateStarted().AsTime(); first.After(from) {
			require.Equal(t, api.GapReason_GAP_REASON_NOT_RECEIVED, ts.GetGaps()[0].GetReason())
			require.True(t, ts.GetGaps()[0].GetFrom().AsTime().Equal(from))
			require.True(t, ts.GetGaps()[0].GetTo().AsTime().Equal(first))
		}
		for _, g := range ts.GetGaps() {
			require.GreaterOrEqual(t, g.GetTo().AsTime().Sub(g.GetFrom().AsTime()), time.Second, "no sliver gaps")
		}
	}

	// Read one back through its presigned URL, and a range of it.
	first := got[string(srcs[0].GetId())].GetLaminae()[0]
	resp, err := http.Get(first.GetUrl())
	require.NoError(t, err)
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var want []byte
	for _, sg := range segs[string(srcs[0].GetId())] {
		if sg.al.GetLaminaId() != nil && string(sg.al.GetLaminaId()) == string(first.GetLaminaId()) {
			want = sg.body
		}
	}
	require.True(t, bytes.Equal(want, b), "the bytes come back as uploaded")

	req, _ := http.NewRequest(http.MethodGet, first.GetUrl(), nil)
	req.Header.Set("Range", "bytes=10-19")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusPartialContent, resp.StatusCode)
	require.Equal(t, want[10:20], b)

	// A get token does not put, and a token for another key does not get.
	req, _ = http.NewRequest(http.MethodPut, first.GetUrl(), bytes.NewReader([]byte("x")))
	req.Header.Set(storage.HdrUploadOffset, "0")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// TestResumeAndIdempotence covers §12.2 and §12.5: an upload cut off is
// resumed from the offset HEAD reports, a wrong offset is refused with the
// current one, and a second complete PUT answers 200.
func TestResumeAndIdempotence(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	conn := c.dial("@acme/admin")
	sets := api.NewSetServiceClient(conn)
	sources := api.NewSourceServiceClient(conn)
	laminae := api.NewLaminaServiceClient(conn)

	set, err := sets.Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "one"}.Build())
	require.NoError(t, err)
	src, err := sources.Add(ctx, api.SourceAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "cam", Set: api.SetRef_builder{Id: set.GetId()}.Build(),
		Profile: api.SegmentProfile_builder{MaxBitrate: 2_000_000}.Build(),
	}.Build())
	require.NoError(t, err)

	al, err := laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(time.Now().Add(-time.Hour)),
	}.Build())
	require.NoError(t, err)
	cand := al.GetCandidates()[0]
	ep := cand.GetEndpoints()[0]
	url := fmt.Sprintf("%s://%s:%d/%s", ep.GetScheme(), ep.GetHost(), ep.GetPort(), al.GetLaminaKey())
	body := make([]byte, 200_000)
	rand.Read(body)

	do := func(method string, offset int, complete string, b []byte) *http.Response {
		req, err := http.NewRequest(method, url, bytes.NewReader(b))
		require.NoError(t, err)
		req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
		req.Header.Set(storage.HdrUploadOffset, fmt.Sprint(offset))
		if complete != "" {
			req.Header.Set(storage.HdrUploadComplete, complete)
		}
		if b != nil {
			req.ContentLength = int64(len(b))
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		return resp
	}

	// The first half, not complete: 204 with the offset on the device,
	// which is the 4 KiB-aligned prefix of what was sent (§12.2); the tail
	// is the producer's to send again.
	aligned := fmt.Sprint(100_000 &^ (storage.Align - 1))
	resp := do(http.MethodPut, 0, "?0", body[:100_000])
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.Equal(t, aligned, resp.Header.Get(storage.HdrUploadOffset))

	// HEAD says where to resume.
	resp = do(http.MethodHead, 0, "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, aligned, resp.Header.Get(storage.HdrUploadOffset))
	require.Equal(t, "?0", resp.Header.Get(storage.HdrUploadComplete))

	// A wrong offset is refused with the current one.
	resp = do(http.MethodPut, 50_000, "?1", body[50_000:])
	require.Equal(t, http.StatusConflict, resp.StatusCode)
	require.Equal(t, aligned, resp.Header.Get(storage.HdrUploadOffset))

	// The rest from the reported offset, complete: 201.
	from := 100_000 &^ (storage.Align - 1)
	resp = do(http.MethodPut, from, "?1", body[from:])
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Again: 200, already complete.
	resp = do(http.MethodPut, 0, "?1", body)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "?1", resp.Header.Get(storage.HdrUploadComplete))

	// Beyond max_length is 413 on a fresh key.
	al2, err := laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(time.Now().Add(-2 * time.Hour)),
	}.Build())
	require.NoError(t, err)
	cand2 := al2.GetCandidates()[0]
	url2 := fmt.Sprintf("%s://%s:%d/%s", ep.GetScheme(), ep.GetHost(), ep.GetPort(), al2.GetLaminaKey())
	req, _ := http.NewRequest(http.MethodPut, url2, bytes.NewReader([]byte("x")))
	req.Header.Set("Authorization", token.Scheme+" "+cand2.GetToken())
	req.Header.Set(storage.HdrUploadOffset, "0")
	req.Header.Set(storage.HdrUploadLength, fmt.Sprint(al2.GetMaxLength()+1))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
}
