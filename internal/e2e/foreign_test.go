package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/producer"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/internal/token"
)

// TestForeignBytes is a producer restarting in the middle of a slot
// (§12.5, §15): the node holds part of an upload the earlier incarnation
// sent for the same key. The new one must not continue that file with
// its own bytes; it gives the attempt up and the object gets another,
// which commits with the new segment whole.
func TestForeignBytes(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	conn := c.dial("@acme/admin")
	sets := api.NewSetServiceClient(conn)
	sources := api.NewSourceServiceClient(conn)
	objects := api.NewObjectServiceClient(conn)

	set, err := sets.Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "foreign"}.Build())
	require.NoError(t, err)
	src, err := sources.Add(ctx, api.SourceAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "cam", Set: api.SetRef_builder{Id: set.GetId()}.Build(),
		Profile: api.SegmentProfile_builder{MaxBitrate: 2_000_000}.Build(),
	}.Build())
	require.NoError(t, err)
	started := time.Now().Add(-time.Hour)
	al, err := objects.Allocate(ctx, api.ObjectAllocateRequest_builder{
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(started),
	}.Build())
	require.NoError(t, err)
	cand := al.GetCandidates()[0]
	ep := cand.GetEndpoints()[0]
	url := fmt.Sprintf("%s://%s:%d/%s", ep.GetScheme(), ep.GetHost(), ep.GetPort(), cand.GetObjectKey())

	// The earlier incarnation left 100 KB of its own at the key.
	old := make([]byte, 100_000)
	rand.Read(old)
	req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(old))
	req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
	req.Header.Set(storage.HdrUploadOffset, "0")
	req.Header.Set(storage.HdrUploadComplete, "?0")
	req.ContentLength = int64(len(old))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// The new incarnation, with the same allocation and its own segment.
	body := make([]byte, 200_000)
	rand.Read(body)
	seg := producer.NewSegment(started, nil)
	seg.Write(body)
	seg.Close(started.Add(time.Minute))
	up := &producer.Uploader{Cfg: producer.UploadConfig{ResumeTimeout: 5 * time.Second, PlacementRetries: 2}, Objects: objects, Log: slog.Default(), Mode: api.UploadMode_UPLOAD_MODE_BUFFERED}
	res := up.Upload(ctx, al, seg)
	require.True(t, res.Stored, "stored somewhere: %v", res.Err)
	require.Greater(t, res.Attempts, 1, "the first attempt was given up")

	require.Eventually(t, func() bool {
		o, err := objects.Get(ctx, api.ObjectGetRequest_builder{Ref: api.ObjectRef_builder{Id: al.GetObjectId()}.Build()}.Build())

		return err == nil && o.GetState() == api.ObjectState_OBJECT_STATE_COMMITTED && o.GetSize() == int64(len(body))
	}, 10*time.Second, 200*time.Millisecond)
	tl, err := objects.Timeline(ctx, api.ObjectTimelineRequest_builder{
		Set: api.SetRef_builder{Id: set.GetId()}.Build(), From: timestamppb.New(started.Add(-time.Minute)), To: timestamppb.New(started.Add(time.Hour)), Size: 10,
	}.Build())
	require.NoError(t, err)
	require.Len(t, tl.GetSources()[0].GetObjects(), 1)
	require.Equal(t, body, fetch(t, tl.GetSources()[0].GetObjects()[0].GetUrl()), "the new segment, whole")
}
