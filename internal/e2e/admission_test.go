package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/cmd"
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
func camera(t *testing.T, ctx context.Context, conn *grpc.ClientConn, alias string, link *api.LinkProfile) (*api.Set, func(ago time.Duration) *api.Allocation) {
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

	return set, func(ago time.Duration) *api.Allocation {
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
	_, allocate := camera(t, ctx, conn, "admit", nil)

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
	_, allocate := camera(t, ctx, conn, "actor", nil)
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
	_, allocate := camera(t, ctx, conn, "abandon", api.LinkProfile_builder{
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
