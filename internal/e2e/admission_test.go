package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/internal/token"
)

// candURL is where a candidate takes its key.
func candURL(cand *api.Candidate) string {
	ep := cand.GetEndpoints()[0]

	return fmt.Sprintf("%s://%s:%d/%s", ep.GetScheme(), ep.GetHost(), ep.GetPort(), cand.GetLaminaKey())
}

// putAt uploads a whole body to one candidate of an allocation and answers
// the status with the headers, which is where a node that refuses says
// when to come back (§12.2).
func putAt(t *testing.T, al *api.Allocation, cand *api.Candidate, body []byte, complete string) (int, http.Header) {
	t.Helper()
	started := al.GetDateStarted().AsTime()
	req, err := http.NewRequest(http.MethodPut, candURL(cand), bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
	req.Header.Set(storage.HdrUploadOffset, "0")
	req.Header.Set(storage.HdrUploadComplete, complete)
	req.Header.Set(storage.HdrDateStarted, started.Format(time.RFC3339Nano))
	if complete == "?1" {
		req.Header.Set(storage.HdrDateEnded, started.Add(time.Minute).Format(time.RFC3339Nano))
	}
	req.ContentLength = int64(len(body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	return resp.StatusCode, resp.Header
}

// headAt asks a candidate's node what it holds at the key.
func headAt(t *testing.T, cand *api.Candidate) (int, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodHead, candURL(cand), nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	return resp.StatusCode, resp.Header
}

// heldUpload is an upload left open: its request runs until finish, and
// the node counts it against its admission limits meanwhile (§12.2).
type heldUpload struct {
	pw     *io.PipeWriter
	done   chan struct{}
	status int
}

// hold starts an upload and leaves it open, answering once the node has
// taken it: the file at the key is what says the node admitted it.
func hold(t *testing.T, cand *api.Candidate, first []byte, size int) *heldUpload {
	t.Helper()
	pr, pw := io.Pipe()
	h := &heldUpload{pw: pw, done: make(chan struct{})}
	go func() {
		defer close(h.done)
		req, err := http.NewRequest(http.MethodPut, candURL(cand), pr)
		if err != nil {
			return
		}
		req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
		req.Header.Set(storage.HdrUploadOffset, "0")
		req.Header.Set(storage.HdrUploadComplete, "?1")
		req.Header.Set(storage.HdrSizeHint, fmt.Sprint(size))
		req.ContentLength = -1
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		h.status = resp.StatusCode
	}()
	_, err := pw.Write(first)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		code, _ := headAt(t, cand)

		return code == http.StatusOK
	}, 10*time.Second, 100*time.Millisecond, "the node opened the upload")

	return h
}

// finish sends the rest and waits for the request to end, which is where
// the node lets the slot go.
func (h *heldUpload) finish(t *testing.T, rest []byte) {
	t.Helper()
	if len(rest) > 0 {
		_, err := h.pw.Write(rest)
		require.NoError(t, err)
	}
	require.NoError(t, h.pw.Close())
	select {
	case <-h.done:
	case <-time.After(30 * time.Second):
		t.Fatal("the held upload did not end")
	}
}

// camera is a set with one source, negotiated over the link profile given
// when there is one, and a way to allocate one slot at a time. Slots are
// asked for by how long ago they started, so two calls with different ages
// are two keys.
func camera(t *testing.T, ctx context.Context, conn *grpc.ClientConn, alias string, link *api.LinkProfile) (*api.Set, *api.Source, func(ago time.Duration) *api.Allocation) {
	t.Helper()
	sets := api.NewSetServiceClient(conn)
	sources := api.NewSourceServiceClient(conn)
	laminae := api.NewLaminaServiceClient(conn)

	set, err := sets.Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: alias}.Build())
	require.NoError(t, err)
	src, err := sources.Add(ctx, api.SourceAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "cam", Set: api.SetRef_builder{Id: set.GetId()}.Build(),
		Profile: api.SegmentProfile_builder{MaxBitrate: 2_000_000}.Build(),
	}.Build())
	require.NoError(t, err)
	if link != nil {
		neg, err := sets.Negotiate(ctx, api.SetNegotiateRequest_builder{
			Ref:  api.SetRef_builder{Id: set.GetId()}.Build(),
			Link: link,
			Sources: []*api.SourceProposal{api.SourceProposal_builder{
				Source: api.SourceRef_builder{Id: src.GetId()}.Build(), Profile: api.SegmentProfile_builder{MaxBitrate: 2_000_000}.Build(),
			}.Build()},
		}.Build())
		require.NoError(t, err)
		require.Equal(t, link.GetMode(), neg.GetLink().GetMode(), "the mode is the one asked for")
	}

	return set, src, func(ago time.Duration) *api.Allocation {
		al, err := laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{
			Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(time.Now().Add(-ago).Truncate(time.Second)),
		}.Build())
		require.NoError(t, err)
		require.NotEmpty(t, al.GetCandidates())

		return al
	}
}

// TestUploadLimitPerSink is a row of §12.2's admission table: a sink at
// `max_uploads` answers a new upload with 503 and a `Retry-After`, and
// takes it as soon as a slot is free. The limit is the node's own resource
// and is never negotiated (§12.6).
func TestUploadLimitPerSink(t *testing.T) {
	c := start(t, func(c *cmd.Config) { c.Storage.MaxUploads = 1 })
	ctx := context.Background()
	conn := c.dial("@acme/admin")
	_, _, allocate := camera(t, ctx, conn, "admit", nil)

	first, second := allocate(2*time.Hour), allocate(time.Hour)
	held, next := first.GetCandidates()[0], second.GetCandidates()[0]
	require.Equal(t, held.GetSinkId(), next.GetSinkId(), "both keys are on the one sink of this cluster")

	body := make([]byte, 128<<10)
	rand.Read(body)
	open := hold(t, held, body[:64<<10], len(body))

	// The sink's one slot is taken: the other upload is refused, and told
	// when to try again.
	code, hdr := putAt(t, second, next, body, "?1")
	require.Equal(t, http.StatusServiceUnavailable, code, "a sink at max_uploads refuses")
	require.Equal(t, "10", hdr.Get(storage.HdrRetryAfter), "and says when to come back")

	// Nothing was written for the refused key: it is not a failed upload,
	// it is one that never started.
	code, _ = headAt(t, next)
	require.Equal(t, http.StatusNotFound, code)

	// The slot frees when the held upload ends, and the same key is taken.
	open.finish(t, body[64<<10:])
	require.Equal(t, http.StatusCreated, open.status, "the held upload completed")
	require.Eventually(t, func() bool {
		code, _ := putAt(t, second, next, body, "?1")

		return code == http.StatusCreated
	}, 10*time.Second, 200*time.Millisecond, "the sink takes the upload once a slot is free")
}

// TestUploadLimitPerActor is the admission table's other counter: one
// caller may hold `uploads_per_actor` uploads on a node, whatever the
// sinks. The second upload here goes to the other sink, which is idle, so
// only the per-actor counter can refuse it.
func TestUploadLimitPerActor(t *testing.T) {
	c := start(t, func(c *cmd.Config) {
		c.Storage.UploadsPerActor = 1
		c.Storage.Sinks[0].Device = "disk-one"
		c.Storage.Sinks = append(c.Storage.Sinks, cmd.SinkConfig{
			Path:     filepath.Join(filepath.Dir(c.Storage.Sinks[0].Path), "sink-two"),
			Capacity: "4GiB",
			Device:   "disk-two",
		})
	})
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")
	sinks := api.NewSinkServiceClient(ops)
	require.Eventually(t, func() bool {
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		attached := 0
		for _, s := range vs.GetItems() {
			if s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED && s.GetDateSeen() != nil {
				attached++
			}
		}

		return attached == 2
	}, 30*time.Second, 200*time.Millisecond, "the node reported both of its sinks")

	conn := c.dial("@acme/admin")
	_, _, allocate := camera(t, ctx, conn, "actor", nil)
	first, second := allocate(2*time.Hour), allocate(time.Hour)
	held := first.GetCandidates()[0]
	var next *api.Candidate
	for _, cand := range second.GetCandidates() {
		if string(cand.GetSinkId()) != string(held.GetSinkId()) {
			next = cand
			break
		}
	}
	require.NotNil(t, next, "every sink is a candidate, so the other one is offered")

	body := make([]byte, 128<<10)
	rand.Read(body)
	open := hold(t, held, body[:64<<10], len(body))

	// One upload for this caller is one upload, wherever the second would
	// go: the idle sink refuses it too.
	code, hdr := putAt(t, second, next, body, "?1")
	require.Equal(t, http.StatusServiceUnavailable, code, "a caller at uploads_per_actor is refused on an idle sink")
	require.Equal(t, "10", hdr.Get(storage.HdrRetryAfter))

	open.finish(t, body[64<<10:])
	require.Equal(t, http.StatusCreated, open.status)
	require.Eventually(t, func() bool {
		code, _ := putAt(t, second, next, body, "?1")

		return code == http.StatusCreated
	}, 10*time.Second, 200*time.Millisecond, "the idle sink takes it once the caller is under the count")
}

// TestAbandonedBufferedUpload is the abandon rule's buffered half (§12.2,
// §15): an upload left open past its `abandon_timeout` is deleted rather
// than finalized, because a buffered upload's producer still holds the
// whole segment and can send it again; the node reports it, and the CP
// fails the attempt so the producer is given another.
func TestAbandonedBufferedUpload(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	ops := c.dialCluster("@cluster/ops")

	// The policy's floor is a minute, which is a long time to sit in a
	// test: a policy of its own brings it down to seconds (§12.6).
	policies := api.NewUploadPolicyServiceClient(ops)
	up, err := policies.Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias: "short", Version: 1,
		Bounds: api.UploadBounds_builder{
			IdleTimeoutDefault: 1, IdleTimeoutMin: 1,
			AbandonTimeoutDefault: 2, AbandonTimeoutMin: 2,
		}.Build(),
	}.Build())
	require.NoError(t, err)
	_, err = policies.Activate(ctx, api.UploadPolicyActivateRequest_builder{Ref: api.UploadPolicyRef_builder{Id: up.GetId()}.Build()}.Build())
	require.NoError(t, err)

	// A node that looks over its open uploads often, so the rule fires
	// within the test rather than on the half minute.
	sinkB := filepath.Join(t.TempDir(), "sink-b")
	nodeB, _ := c.startNodeAt("b", filepath.Join(t.TempDir(), "node-b"), sinkB, func(cfg *storage.Config) {
		cfg.AbandonEvery = 500 * time.Millisecond
	})
	sinks := api.NewSinkServiceClient(ops)
	require.Eventually(t, func() bool {
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, s := range vs.GetItems() {
			if string(s.GetNode().GetId()) == string(nodeB.Id().Bytes()) && s.GetAttachment() == api.SinkAttachment_SINK_ATTACHMENT_ATTACHED && s.GetDateSeen() != nil {
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "node B's sink is attached")

	conn := c.dial("@acme/admin")
	laminae := api.NewLaminaServiceClient(conn)
	attempts := api.NewAttemptServiceClient(conn)
	_, _, allocate := camera(t, ctx, conn, "abandon", api.LinkProfile_builder{
		Mode:                  api.UploadMode_UPLOAD_MODE_BUFFERED,
		IdleTimeoutSeconds:    1,
		AbandonTimeoutSeconds: 2,
	}.Build())

	al := allocate(time.Hour)
	var cand *api.Candidate
	for _, cd := range al.GetCandidates() {
		if string(cd.GetNodeId()) == string(nodeB.Id().Bytes()) {
			cand = cd
			break
		}
	}
	require.NotNil(t, cand, "node B is a candidate")

	// Half a segment, and then nothing: the producer went away.
	body := make([]byte, 128<<10)
	rand.Read(body)
	code, _ := putAt(t, al, cand, body, "?0")
	require.Equal(t, http.StatusNoContent, code)
	path := filepath.Join(sinkB, cand.GetLaminaKey())
	_, err = os.Stat(path)
	require.NoError(t, err, "the partial upload is on the sink")

	// Past the abandon timeout the node deletes it: a buffered upload is
	// never finalized incomplete, since its producer still has the bytes.
	require.Eventually(t, func() bool {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			return false
		}
		code, _ := headAt(t, cand)

		return code == http.StatusNotFound
	}, 30*time.Second, 200*time.Millisecond, "the abandoned buffered upload was deleted")

	// And it was reported: the attempt fails, naming the rule, so the CP
	// hands out a new one rather than waiting for bytes that never come.
	require.Eventually(t, func() bool {
		vs, err := attempts.List(ctx, api.AttemptListRequest_builder{
			Filters: []*api.AttemptFilter{api.AttemptFilter_builder{Lamina: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build()},
			Size:    10,
		}.Build())
		if err != nil {
			return false
		}
		for _, at := range vs.GetItems() {
			if at.GetState() == api.AttemptState_ATTEMPT_STATE_FAILED && strings.Contains(at.GetFailureReason(), "ABANDONED") {
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "the node's report failed the attempt")

	o, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())
	require.NoError(t, err)
	require.Equal(t, api.LaminaState_LAMINA_STATE_PENDING, o.GetState(), "nothing was stored, so the lamina is still due")
}

// storedBytes waits for a lamina to be committed and answers what reading
// it back gives, with its row: the proof an upload completed intact.
func storedBytes(t *testing.T, ctx context.Context, conn *grpc.ClientConn, set *api.Set, al *api.Allocation) ([]byte, *api.Lamina) {
	t.Helper()
	laminae := api.NewLaminaServiceClient(conn)
	var o *api.Lamina
	require.Eventually(t, func() bool {
		v, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())
		if err != nil || v.GetState() != api.LaminaState_LAMINA_STATE_COMMITTED {
			return false
		}
		o = v

		return true
	}, 30*time.Second, 100*time.Millisecond, "the lamina is committed")
	started := al.GetDateStarted().AsTime()
	tl, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{
		Set: api.SetRef_builder{Id: set.GetId()}.Build(), From: timestamppb.New(started.Add(-time.Minute)), To: timestamppb.New(started.Add(time.Hour)), Size: 100,
	}.Build())
	require.NoError(t, err)
	for _, s := range tl.GetSources() {
		for _, l := range s.GetLaminae() {
			if string(l.GetLaminaId()) == string(al.GetLaminaId()) {
				return fetch(t, l.GetUrl()), o
			}
		}
	}
	t.Fatal("the lamina is not on the timeline")

	return nil, nil
}

// countingBody is a request body that counts what the client handed to the
// connection: what a sender got rid of, as opposed to what the node read.
type countingBody struct {
	r io.Reader
	n atomic.Int64
}

func (b *countingBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n.Add(int64(n))

	return n, err
}

// TestPartBufferPoolBackpressure is the `part_buffer_pool` row of §12.2's
// admission table: when the uploads of a node hold the whole pool, the
// node stops reading sockets, and TCP flow control slows the senders. It
// refuses nothing: an upload that finds the pool spent is admitted and
// waits, and continues as soon as a part is released. The pool here is two
// 64 KiB parts, and three uploads of 1 MiB and more run at once.
func TestPartBufferPoolBackpressure(t *testing.T) {
	const part = 64 << 10
	c := start(t, func(c *cmd.Config) {
		c.Storage.PartSize = "64KiB"
		c.Storage.PartBufferPool = "128KiB"
	})
	ctx := context.Background()
	conn := c.dial("@acme/admin")
	set, _, allocate := camera(t, ctx, conn, "pool", nil)
	node := c.running.Node
	_, _, budget := node.PartBuffers()
	require.EqualValues(t, 2*part, budget, "the pool is what the configuration says")

	// Two uploads that hold a part each and then go quiet: the pool is
	// spent.
	held := []*api.Allocation{allocate(3 * time.Hour), allocate(2 * time.Hour)}
	bodies := make([][]byte, len(held))
	open := make([]*heldUpload, len(held))
	for i, al := range held {
		bodies[i] = make([]byte, 1<<20)
		rand.Read(bodies[i])
		open[i] = hold(t, al.GetCandidates()[0], bodies[i][:256<<10], len(bodies[i]))
	}
	require.Eventually(t, func() bool {
		used, _, _ := node.PartBuffers()

		return used == budget
	}, 10*time.Second, 50*time.Millisecond, "the held uploads hold the whole pool")

	// A third upload, much larger than what the sockets between the two
	// ends can buffer.
	late := allocate(time.Hour)
	cand := late.GetCandidates()[0]
	body := make([]byte, 32<<20)
	rand.Read(body)
	sent := &countingBody{r: bytes.NewReader(body)}
	req, err := http.NewRequest(http.MethodPut, candURL(cand), sent)
	require.NoError(t, err)
	req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
	req.Header.Set(storage.HdrUploadOffset, "0")
	req.Header.Set(storage.HdrUploadComplete, "?1")
	req.ContentLength = int64(len(body))
	type answer struct {
		code int
		err  error
		at   time.Time
	}
	done := make(chan answer, 1)
	began := time.Now()
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- answer{err: err, at: time.Now()}
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		done <- answer{code: resp.StatusCode, at: time.Now()}
	}()

	// It is admitted, not refused: the file is there, at offset 0, since
	// the node has no part to read it into.
	require.Eventually(t, func() bool {
		code, _ := headAt(t, cand)

		return code == http.StatusOK
	}, 10*time.Second, 50*time.Millisecond, "the node admitted the upload")

	// The sender stalls once the socket buffers are full: what it handed
	// over stops growing, well short of the body, and the request is still
	// open.
	var plateau int64
	stable := 0
	require.Eventually(t, func() bool {
		n := sent.n.Load()
		if n == plateau {
			stable++
		} else {
			plateau, stable = n, 0
		}

		return stable >= 8
	}, 30*time.Second, 125*time.Millisecond, "the sender stalls")
	select {
	case a := <-done:
		t.Fatalf("the upload ended while the pool was spent: %d %v", a.code, a.err)
	default:
	}
	require.Less(t, plateau, int64(len(body)), "the sender was held back")
	code, hdr := headAt(t, cand)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "0", hdr.Get(storage.HdrUploadOffset), "nothing of it reached the device")
	stalled := time.Since(began)
	t.Logf("the sender stalled at %d of %d bytes; %s after it began, still open", plateau, len(body), stalled.Round(time.Millisecond))

	// One held upload ends and releases its part: the stalled one goes on
	// in it, beside the other held upload, and completes.
	open[0].finish(t, bodies[0][256<<10:])
	require.Equal(t, http.StatusCreated, open[0].status)
	released := time.Now()
	var a answer
	select {
	case a = <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("the stalled upload did not go on once a part was free")
	}
	require.NoError(t, a.err)
	require.Equal(t, http.StatusCreated, a.code, "slowed, never refused")
	t.Logf("released a part; the rest (%d bytes) went in %s", int64(len(body))-plateau, a.at.Sub(released).Round(time.Millisecond))

	open[1].finish(t, bodies[1][256<<10:])
	require.Equal(t, http.StatusCreated, open[1].status)

	used, peak, _ := node.PartBuffers()
	require.LessOrEqual(t, peak, budget, "the uploads never held more than the pool")
	require.Zero(t, used, "every part went back")
	t.Logf("pool %d bytes, peak %d", budget, peak)

	// Everything arrived whole.
	for i, al := range held {
		got, _ := storedBytes(t, ctx, conn, set, al)
		require.Equal(t, bodies[i], got)
	}
	got, _ := storedBytes(t, ctx, conn, set, late)
	require.Equal(t, body, got)
}

// shortIdle is an UploadPolicy that lets a set negotiate an idle timeout of
// a second: the policy's floor is ten (§12.6).
func shortIdle(t *testing.T, ctx context.Context, c *cluster) {
	t.Helper()
	policies := api.NewUploadPolicyServiceClient(c.dialCluster("@cluster/ops"))
	up, err := policies.Add(ctx, api.UploadPolicyAddRequest_builder{
		Alias: "short-idle", Version: 1,
		Bounds: api.UploadBounds_builder{IdleTimeoutDefault: 1, IdleTimeoutMin: 1}.Build(),
	}.Build())
	require.NoError(t, err)
	_, err = policies.Activate(ctx, api.UploadPolicyActivateRequest_builder{Ref: api.UploadPolicyRef_builder{Id: up.GetId()}.Build()}.Build())
	require.NoError(t, err)
}

// TestIdleTimeoutClosesStalledUpload is the `idle_timeout` row of §12.2's
// table: a request that brings no bytes for that long is ended by the node,
// which keeps what it has; the upload stays open, and the producer resumes
// from the offset HEAD reports. The abandon timeout is left at its five
// minutes, so only the idle timeout can act here.
func TestIdleTimeoutClosesStalledUpload(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	shortIdle(t, ctx, c)
	conn := c.dial("@acme/admin")
	set, _, allocate := camera(t, ctx, conn, "idle", api.LinkProfile_builder{
		Mode:               api.UploadMode_UPLOAD_MODE_LIVE,
		IdleTimeoutSeconds: 1,
	}.Build())
	al := allocate(time.Hour)
	cand := al.GetCandidates()[0]
	claims, err := token.Parse(cand.GetToken())
	require.NoError(t, err)
	require.EqualValues(t, 1, claims.GetIdleTimeoutSeconds(), "the token carries the negotiated idle timeout")
	require.EqualValues(t, 300, claims.GetAbandonTimeoutSeconds(), "and the default abandon timeout")

	// A live upload that sends 200 kB and then nothing, its request still
	// open: a camera that stalled, or a link that did.
	body := make([]byte, 1<<20)
	rand.Read(body)
	const sent = 200_000
	pr, pw := io.Pipe()
	req, err := http.NewRequest(http.MethodPut, candURL(cand), pr)
	require.NoError(t, err)
	req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
	req.Header.Set(storage.HdrUploadOffset, "0")
	req.Header.Set(storage.HdrUploadComplete, "?1")
	req.Header.Set(storage.HdrSizeHint, fmt.Sprint(len(body)))
	req.Header.Set(storage.HdrDateStarted, al.GetDateStarted().AsTime().Format(time.RFC3339Nano))
	req.ContentLength = -1
	type answer struct {
		resp *http.Response
		err  error
	}
	done := make(chan answer, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		done <- answer{resp, err}
	}()
	_, err = pw.Write(body[:sent])
	require.NoError(t, err)
	quiet := time.Now()

	// The node ends the request about a second later, with the offset it
	// holds: the aligned prefix of what came, the rest to be sent again.
	var a answer
	select {
	case a = <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("the node kept an idle request open")
	}
	waited := time.Since(quiet)
	require.NoError(t, a.err)
	require.Equal(t, http.StatusServiceUnavailable, a.resp.StatusCode)
	aligned := int64(sent) &^ (storage.Align - 1)
	require.Equal(t, fmt.Sprint(aligned), a.resp.Header.Get(storage.HdrUploadOffset), "the answer says where to resume")
	require.Equal(t, "?0", a.resp.Header.Get(storage.HdrUploadComplete))
	require.GreaterOrEqual(t, waited, time.Second, "not before the idle timeout")
	require.Less(t, waited, 5*time.Second, "but soon after it")
	t.Logf("idle for %s, then closed at offset %d", waited.Round(time.Millisecond), aligned)

	// The connection is over too: what the sender still writes goes no
	// further than the socket buffers before the client finds it closed.
	var after int
	for deadline := time.Now().Add(5 * time.Second); ; {
		require.True(t, time.Now().Before(deadline), "the sender could still write %d bytes after the answer", after)
		if _, err := pw.Write(body[:4096]); err != nil {
			break
		}
		after += 4096
	}
	t.Logf("the sender's writes failed %d bytes after the answer", after)

	// The upload stays open and resumable: neither complete nor gone.
	code, hdr := headAt(t, cand)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, fmt.Sprint(aligned), hdr.Get(storage.HdrUploadOffset))
	require.Equal(t, "?0", hdr.Get(storage.HdrUploadComplete))

	// The producer resumes from there, and the lamina is whole.
	rest, err := http.NewRequest(http.MethodPut, candURL(cand), bytes.NewReader(body[aligned:]))
	require.NoError(t, err)
	rest.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
	rest.Header.Set(storage.HdrUploadOffset, fmt.Sprint(aligned))
	rest.Header.Set(storage.HdrUploadComplete, "?1")
	rest.Header.Set(storage.HdrDateEnded, al.GetDateStarted().AsTime().Add(time.Minute).Format(time.RFC3339Nano))
	rest.ContentLength = int64(len(body)) - aligned
	resp, err := http.DefaultClient.Do(rest)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	got, o := storedBytes(t, ctx, conn, set, al)
	require.Equal(t, body, got)
	require.False(t, o.GetIncomplete(), "the node did not finalize it on its own")
}

// renewCounter is the producer's LaminaService with its Renew calls
// counted.
type renewCounter struct {
	api.LaminaServiceClient
	n atomic.Int32
}

func (r *renewCounter) Renew(ctx context.Context, req *api.LaminaRenewRequest, opts ...grpc.CallOption) (*api.Allocation, error) {
	r.n.Add(1)

	return r.LaminaServiceClient.Renew(ctx, req, opts...)
}

// TestAllocationRenewed is the `allocation_ttl` row of §12.2's table: an
// attempt and its token expire; one still in progress is renewed before
// that with LaminaService.Renew, and stays on the same key; one that was
// not renewed in time is refused renewal and gets a new attempt of the
// same lamina. `allocation_ttl` is three seconds here, and the node allows
// no skew past a token's expiry.
func TestAllocationRenewed(t *testing.T) {
	const ttl = 3 * time.Second
	c := start(t, func(c *cmd.Config) {
		c.Control.AllocationTTL = ttl
		c.Control.JobsEvery = 250 * time.Millisecond
		c.Storage.TokenSkew = time.Millisecond
	})
	ctx := context.Background()
	conn := c.dial("@acme/admin")
	laminae := &renewCounter{LaminaServiceClient: api.NewLaminaServiceClient(conn)}
	attempts := api.NewAttemptServiceClient(conn)
	set, _, allocate := camera(t, ctx, conn, "renew", nil)
	attemptOf := func(id []byte) *api.Attempt {
		at, err := attempts.Get(ctx, api.AttemptGetRequest_builder{Ref: api.AttemptRef_builder{Id: id}.Build()}.Build())
		require.NoError(t, err)

		return at
	}

	// A live segment that grows for nine seconds, three TTLs, uploaded a
	// part at a time under `retain: written`: every part is a request of
	// its own, with the token the attempt has by then.
	al := allocate(time.Hour)
	cand := al.GetCandidates()[0]
	expires := attemptOf(cand.GetAttemptId()).GetDateExpires().AsTime()
	require.WithinDuration(t, time.Now().Add(ttl), expires, 2*time.Second, "the attempt expires after allocation_ttl")
	started := al.GetDateStarted().AsTime()
	seg := producer.NewSegment(started, nil)
	var whole []byte
	go func() {
		for range 90 {
			b := make([]byte, 32<<10)
			rand.Read(b)
			whole = append(whole, b...)
			seg.Write(b)
			time.Sleep(100 * time.Millisecond)
		}
		seg.Close(started.Add(9 * time.Second))
	}()
	up := &producer.Uploader{
		Cfg:     producer.UploadConfig{ResumeTimeout: 5 * time.Second, Part: 256 << 10},
		Laminae: laminae, Log: slog.Default(), Mode: api.UploadMode_UPLOAD_MODE_LIVE, Written: true,
	}
	began := time.Now()
	res := up.Upload(ctx, al, seg)
	took := time.Since(began)
	require.True(t, res.Stored, "stored: %v", res.Err)
	require.Equal(t, 1, res.Attempts, "on the first attempt")
	require.False(t, res.Cut)
	renewals := laminae.n.Load()
	require.GreaterOrEqual(t, renewals, int32(3), "renewed every two seconds")
	t.Logf("a %s upload under a %s TTL: %d renewals", took.Round(time.Millisecond), ttl, renewals)

	// Stored on the attempt it began with, at that attempt's key, and the
	// attempt's expiry moved with each renewal.
	got, o := storedBytes(t, ctx, conn, set, al)
	require.Equal(t, cand.GetLaminaKey(), o.GetLaminaKey(), "the same key, so the same attempt and target")
	require.Equal(t, string(cand.GetSinkId()), string(o.GetSink().GetId()))
	require.Equal(t, whole, got)
	at := attemptOf(cand.GetAttemptId())
	require.Equal(t, api.AttemptState_ATTEMPT_STATE_STORED, at.GetState())
	require.True(t, at.GetDateExpires().AsTime().After(expires.Add(ttl)), "renewed past its first expiry: %s, first %s", at.GetDateExpires().AsTime(), expires)

	// The token the upload started with is long gone: without renewal the
	// later parts would have been refused.
	code, _ := headAt(t, cand)
	require.Equal(t, http.StatusUnauthorized, code, "the first token expired during the upload")

	// An allocation nobody used within its TTL: the CP abandons the
	// attempt, and will not renew it.
	al = allocate(2 * time.Hour)
	cand = al.GetCandidates()[0]
	require.Eventually(t, func() bool {
		return attemptOf(cand.GetAttemptId()).GetState() == api.AttemptState_ATTEMPT_STATE_ABANDONED
	}, 10*time.Second, 100*time.Millisecond, "an attempt past its TTL is ABANDONED")
	seg = producer.NewSegment(al.GetDateStarted().AsTime(), nil)
	body := make([]byte, 300_000)
	rand.Read(body)
	seg.Write(body)
	seg.Close(al.GetDateStarted().AsTime().Add(time.Minute))
	up.Written = false
	res = up.Upload(ctx, al, seg)
	require.False(t, res.Stored)
	require.ErrorContains(t, res.Err, "expired")
	require.Equal(t, 1, res.Attempts, "the other candidates share the TTL: not tried, and nothing reallocated")
	code, _ = headAt(t, cand)
	require.Equal(t, http.StatusUnauthorized, code, "nothing was sent with the expired token")

	// Asking for the slot again, as the producer's queue does, answers the
	// same lamina with a new attempt, and the segment is stored there.
	again, err := laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{
		Source: api.SourceRef_builder{Id: al.GetSourceId()}.Build(), DateStarted: al.GetDateStarted(),
	}.Build())
	require.NoError(t, err)
	require.Equal(t, al.GetLaminaId(), again.GetLaminaId(), "the same lamina")
	require.NotEqual(t, cand.GetAttemptId(), again.GetCandidates()[0].GetAttemptId(), "a new attempt")
	res = up.Upload(ctx, again, seg)
	require.True(t, res.Stored, "stored: %v", res.Err)
	got, o = storedBytes(t, ctx, conn, set, al)
	require.Equal(t, again.GetCandidates()[0].GetLaminaKey(), o.GetLaminaKey())
	require.Equal(t, body, got)
}
