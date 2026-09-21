package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/internal/token"
)

// TestNodeDiesMidUpload is the first drill of §23's fault suite: a node
// stops in the middle of a live upload, comes back as itself on the same
// sink, adopts the open file from its record at the scan, and the
// producer resumes from the offset HEAD reports and completes the lamina,
// whose bytes then read back whole.
func TestNodeDiesMidUpload(t *testing.T) {
	c := start(t)
	ctx := context.Background()

	stateB := filepath.Join(t.TempDir(), "node-b")
	sinkB := filepath.Join(t.TempDir(), "sink-b")
	nodeB, stopB := c.startNodeAt("b", stateB, sinkB)

	ops := c.dialCluster("@cluster/ops")
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
	sets := api.NewSetServiceClient(conn)
	sources := api.NewSourceServiceClient(conn)
	laminae := api.NewLaminaServiceClient(conn)
	set, err := sets.Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "crash"}.Build())
	require.NoError(t, err)
	src, err := sources.Add(ctx, api.SourceAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "cam", Set: api.SetRef_builder{Id: set.GetId()}.Build(),
		Profile: api.SegmentProfile_builder{MaxBitrate: 2_000_000}.Build(),
	}.Build())
	require.NoError(t, err)

	// An allocation whose first candidate is node B's sink.
	var al *api.Allocation
	var cand *api.Candidate
	for i := range 100 {
		a, err := laminae.Allocate(ctx, api.LaminaAllocateRequest_builder{
			Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(time.Now().Add(-time.Duration(i+1) * time.Hour)),
		}.Build())
		require.NoError(t, err)
		for _, cd := range a.GetCandidates() {
			if string(cd.GetNodeId()) == string(nodeB.Id().Bytes()) {
				al, cand = a, cd
			}
		}
		if al != nil {
			break
		}
	}
	require.NotNil(t, al, "some slot lands on node B")
	ep := cand.GetEndpoints()[0]
	url := fmt.Sprintf("%s://%s:%d/%s", ep.GetScheme(), ep.GetHost(), ep.GetPort(), cand.GetLaminaKey())
	body := make([]byte, 400_000)
	rand.Read(body)

	// A live upload that stalls after 150 KB; then the node dies.
	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		req, _ := http.NewRequest(http.MethodPut, url, pr)
		req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
		req.Header.Set(storage.HdrUploadOffset, "0")
		req.Header.Set(storage.HdrUploadComplete, "?1")
		req.Header.Set(storage.HdrSizeHint, fmt.Sprint(len(body)))
		req.ContentLength = -1
		if resp, err := http.DefaultClient.Do(req); err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()
	_, err = pw.Write(body[:150_000])
	require.NoError(t, err)
	time.Sleep(300 * time.Millisecond)
	stopB()
	pw.Close()
	<-done

	// Back as itself, on the same sink: the scan adopts the open file.
	nodeB2, _ := c.startNodeAt("b", stateB, sinkB)
	require.Equal(t, nodeB.Id(), nodeB2.Id(), "the same node")
	require.Eventually(t, func() bool {
		vs, err := sinks.List(ctx, api.SinkListRequest_builder{}.Build())
		if err != nil {
			return false
		}
		for _, s := range vs.GetItems() {
			if string(s.GetNode().GetId()) == string(nodeB2.Id().Bytes()) && s.GetDateSeen() != nil && time.Since(s.GetDateSeen().AsTime()) < 3*time.Second {
				return true
			}
		}

		return false
	}, 30*time.Second, 200*time.Millisecond, "the node heartbeats again")

	// HEAD says what reached the device: an aligned prefix of what was
	// sent, and the upload still open.
	ep2 := fmt.Sprintf("%s://%s/%s", ep.GetScheme(), nodeB2.DataAddr, cand.GetLaminaKey())
	head, _ := http.NewRequest(http.MethodHead, ep2, nil)
	head.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
	resp, err := http.DefaultClient.Do(head)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "?0", resp.Header.Get(storage.HdrUploadComplete))
	var offset int
	fmt.Sscan(resp.Header.Get(storage.HdrUploadOffset), &offset)
	require.Greater(t, offset, 0)
	require.LessOrEqual(t, offset, 150_000)
	require.Zero(t, offset%storage.Align, "the offset on the device is aligned")

	// The producer resumes from there and completes.
	req, _ := http.NewRequest(http.MethodPut, ep2, bytes.NewReader(body[offset:]))
	req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
	req.Header.Set(storage.HdrUploadOffset, fmt.Sprint(offset))
	req.Header.Set(storage.HdrUploadComplete, "?1")
	req.Header.Set(storage.HdrDateEnded, time.Now().UTC().Format(time.RFC3339Nano))
	req.ContentLength = int64(len(body) - offset)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	require.Eventually(t, func() bool {
		o, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())

		return err == nil && o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED && o.GetSize() == int64(len(body))
	}, 15*time.Second, 200*time.Millisecond, "the lamina commits")
	tl, err := laminae.Timeline(ctx, api.LaminaTimelineRequest_builder{
		Set: api.SetRef_builder{Id: set.GetId()}.Build(), From: timestamppb.New(al.GetDateStarted().AsTime().Add(-time.Minute)), To: timestamppb.New(al.GetDateStarted().AsTime().Add(time.Hour)), Size: 10,
	}.Build())
	require.NoError(t, err)
	var got []byte
	for _, s := range tl.GetSources() {
		for _, o := range s.GetLaminae() {
			if string(o.GetLaminaId()) == string(al.GetLaminaId()) {
				got = fetch(t, o.GetUrl())
			}
		}
	}
	require.Equal(t, body, got, "the bytes are the bytes")
}
