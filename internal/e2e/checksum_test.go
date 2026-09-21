package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"hash/crc32"
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

// TestChecksum is §30: a set with `checksum` on gets a CRC32C computed
// while the node receives, kept in the record and the row, and recomputed
// from the file when an upload resumes.
func TestChecksum(t *testing.T) {
	c := start(t)
	ctx := context.Background()
	conn := c.dial("@acme/admin")
	sets := api.NewSetServiceClient(conn)
	sources := api.NewSourceServiceClient(conn)
	laminae := api.NewLaminaServiceClient(conn)

	set, err := sets.Add(ctx, api.SetAddRequest_builder{Tenant: api.TenantRef_builder{Alias: z.Ptr("acme")}.Build(), Alias: "sums", Checksum: true}.Build())
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
	body := make([]byte, 250_000)
	rand.Read(body)
	want := crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli))

	do := func(method string, offset int, complete string, b []byte) *http.Response {
		req, _ := http.NewRequest(method, url, bytes.NewReader(b))
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
	// Two requests: the first ends at the flush point with a tail dropped,
	// so the second makes the node recompute over what is on the device.
	resp := do(http.MethodPut, 0, "?0", body[:100_000])
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	from := 100_000 &^ (storage.Align - 1)
	resp = do(http.MethodPut, from, "?1", body[from:])
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var sum [4]byte
	binary.BigEndian.PutUint32(sum[:], want)
	resp = do(http.MethodHead, 0, "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, fmt.Sprintf("crc32c=%08x", want), resp.Header.Get(storage.HdrChecksum))
	require.Eventually(t, func() bool {
		o, err := laminae.Get(ctx, api.LaminaGetRequest_builder{Ref: api.LaminaRef_builder{Id: al.GetLaminaId()}.Build()}.Build())

		return err == nil && o.GetState() == api.LaminaState_LAMINA_STATE_COMMITTED && bytes.Equal(o.GetChecksum(), sum[:])
	}, 10*time.Second, 200*time.Millisecond, "the row carries the checksum")
}
