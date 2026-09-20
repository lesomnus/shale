package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/lesomnus/z"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/storage"
	"github.com/lesomnus/shale/internal/token"
)

// TestOneWriterPerKey is §12.2's rule: a new PUT on a key with a request
// still open ends the older one and takes its place; and a live upload
// that outgrows its size hint still completes and reads back whole.
func TestOneWriterPerKey(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	conn := c.dial("@acme/admin")
	sets := api.NewSetServiceClient(conn)
	sources := api.NewSourceServiceClient(conn)
	objects := api.NewObjectServiceClient(conn)

	set, err := sets.Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "writer"}.Build())
	require.NoError(t, err)
	src, err := sources.Add(ctx, api.SourceAddRequest_builder{
		Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "cam", Set: api.SetRef_builder{Id: set.GetId()}.Build(),
		Profile: api.SegmentProfile_builder{MaxBitrate: 2_000_000}.Build(),
	}.Build())
	require.NoError(t, err)
	al, err := objects.Allocate(ctx, api.ObjectAllocateRequest_builder{
		Source: api.SourceRef_builder{Id: src.GetId()}.Build(), DateStarted: timestamppb.New(time.Now().Add(-time.Hour)),
	}.Build())
	require.NoError(t, err)
	cand := al.GetCandidates()[0]
	ep := cand.GetEndpoints()[0]
	url := fmt.Sprintf("%s://%s:%d/%s", ep.GetScheme(), ep.GetHost(), ep.GetPort(), al.GetObjectKey())
	body := make([]byte, 300_000)
	rand.Read(body)

	// The older request: a chunked live body that stalls after 100 KB.
	pr, pw := io.Pipe()
	first := make(chan *http.Response, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPut, url, pr)
		req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
		req.Header.Set(storage.HdrUploadOffset, "0")
		req.Header.Set(storage.HdrUploadComplete, "?1")
		req.Header.Set(storage.HdrSizeHint, "65536") // far below what arrives: a second extent
		req.ContentLength = -1
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			first <- nil
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		first <- resp
	}()
	_, err = pw.Write(body[:100_000])
	require.NoError(t, err)
	time.Sleep(300 * time.Millisecond)

	// The newer request from the aligned offset the older one flushes.
	from := 100_000 &^ (storage.Align - 1)
	require.Eventually(t, func() bool {
		req, _ := http.NewRequest(http.MethodPut, url, bytes.NewReader(body[from:]))
		req.Header.Set("Authorization", token.Scheme+" "+cand.GetToken())
		req.Header.Set(storage.HdrUploadOffset, fmt.Sprint(from))
		req.Header.Set(storage.HdrUploadComplete, "?1")
		req.Header.Set(storage.HdrDateEnded, time.Now().UTC().Format(time.RFC3339Nano))
		req.ContentLength = int64(len(body) - from)
		r, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		io.Copy(io.Discard, r.Body)
		r.Body.Close()

		return r.StatusCode == http.StatusCreated
	}, 10*time.Second, 200*time.Millisecond, "the newer request wins the key")
	pw.Close()
	select {
	case r := <-first:
		if r != nil {
			require.NotEqual(t, http.StatusCreated, r.StatusCode, "the older request did not complete the object")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the older request never ended")
	}

	// Whole, and the bytes are the bytes.
	require.Eventually(t, func() bool {
		o, err := objects.Get(ctx, api.ObjectGetRequest_builder{Ref: api.ObjectRef_builder{Id: al.GetObjectId()}.Build()}.Build())

		return err == nil && o.GetState() == api.ObjectState_OBJECT_STATE_COMMITTED && o.GetSize() == int64(len(body))
	}, 10*time.Second, 200*time.Millisecond)
	tl, err := objects.Timeline(ctx, api.ObjectTimelineRequest_builder{
		Set: api.SetRef_builder{Id: set.GetId()}.Build(), From: timestamppb.New(time.Now().Add(-2 * time.Hour)), To: timestamppb.New(time.Now()), Size: 10,
	}.Build())
	require.NoError(t, err)
	require.Len(t, tl.GetSources()[0].GetObjects(), 1)
	require.Equal(t, body, fetch(t, tl.GetSources()[0].GetObjects()[0].GetUrl()))
}
