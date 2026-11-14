package producer

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
)

// A managed catalog over producer.uplink waits for UPLINK, with what it needs
// (the ceilings × 2.2) and what it has, before the CP is asked (#242).
func TestUplinkRefusalSaysWhatItNeeds(t *testing.T) {
	p, v := catalogRuntime(t, Config{SourceProvider: testCatalog{}, Uplink: 1_000_000})
	s, err := p.sourceFor(p.configForSpec(v, v.Sources[0]))
	require.NoError(t, err)
	err = p.negotiateSources(context.Background(), []*source{s}, v, true)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	w := waitOf(err)
	require.Equal(t, api.AdmissionWaitReason_ADMISSION_WAIT_REASON_UPLINK, w.GetReason())
	require.Equal(t, int64(math.Ceil(float64(s.cfg.MaxBitrate)*2.2)), w.GetRequired())
	require.Equal(t, int64(1_000_000), w.GetAvailable())
}

// The CP's refusal says its reason as a detail, read through any wrapping;
// without one, the code is all that is used, never the message.
func TestWaitOfReadsTheCPsDetail(t *testing.T) {
	st, err := status.New(codes.ResourceExhausted, "anything").WithDetails(api.AdmissionWait_builder{
		Reason: api.AdmissionWaitReason_ADMISSION_WAIT_REASON_SET_BITRATE, Required: 3_000_000, Available: 2_000_000,
	}.Build())
	require.NoError(t, err)
	w := waitOf(fmt.Errorf("negotiate: %w", st.Err()))
	require.Equal(t, api.AdmissionWaitReason_ADMISSION_WAIT_REASON_SET_BITRATE, w.GetReason())
	require.Equal(t, int64(3_000_000), w.GetRequired())
	require.Equal(t, int64(2_000_000), w.GetAvailable())

	for code, want := range map[codes.Code]api.AdmissionWaitReason{
		codes.PermissionDenied: api.AdmissionWaitReason_ADMISSION_WAIT_REASON_AUTHORIZATION,
		codes.Unauthenticated:  api.AdmissionWaitReason_ADMISSION_WAIT_REASON_AUTHORIZATION,
		codes.Aborted:          api.AdmissionWaitReason_ADMISSION_WAIT_REASON_STALE,
		codes.Unavailable:      api.AdmissionWaitReason_ADMISSION_WAIT_REASON_OTHER,
	} {
		w := waitOf(fmt.Errorf("negotiate: %w", status.Error(code, "private detail")))
		require.Equal(t, want, w.GetReason(), code.String())
		require.Zero(t, w.GetRequired())
	}
}

// The wait names the cameras not yet recording as the catalog names them,
// and keeps its start while its reason and revision stay the same.
func TestAwaitAdmissionKeepsItsStart(t *testing.T) {
	p, v := catalogRuntime(t, Config{SourceProvider: testCatalog{}})
	t0 := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	uplink := refused(api.AdmissionWaitReason_ADMISSION_WAIT_REASON_UPLINK, 30, 20, status.Error(codes.ResourceExhausted, "x"))
	w := p.awaitAdmission(v, uplink, t0)
	id, err := pdid.Parse(v.Sources[0].SourceID)
	require.NoError(t, err)
	require.Equal(t, [][]byte{id.Bytes()}, w.GetSourceIds())
	require.Equal(t, v.Revision.Catalog, w.GetCatalogRevision())
	require.Equal(t, t0, w.GetDateSince().AsTime())

	w = p.awaitAdmission(v, uplink, t0.Add(time.Minute))
	require.Equal(t, t0, w.GetDateSince().AsTime(), "the same wait keeps its start")
	require.Same(t, w, p.admission.Load(), "and is what the heartbeat reports")

	buffer := refused(api.AdmissionWaitReason_ADMISSION_WAIT_REASON_BUFFER, 30, 20, status.Error(codes.ResourceExhausted, "x"))
	w = p.awaitAdmission(v, buffer, t0.Add(2*time.Minute))
	require.Equal(t, t0.Add(2*time.Minute), w.GetDateSince().AsTime(), "another reason starts again")
}
