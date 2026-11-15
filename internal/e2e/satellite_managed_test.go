package e2e_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lesomnus/shale/api"
)

// A managed producer (an edge's camera catalog) asks for its satellite in
// every negotiation, as any producer does (§41.2). The managed negotiation
// answered without it, and the producer, reading that as "none", uploaded
// directly from each negotiation until its next heartbeat named the
// satellite again.
func TestSatelliteManagedNegotiationKeepsTheAssignment(t *testing.T) {
	c := cameraControlCluster(t)
	ctx := t.Context()
	admin := c.dial("@acme/admin")
	set := sourceCatalogSet(t, admin, "satellite-managed")
	host := sourceCatalogAdopt(t, c, admin, set, "satellite-managed-edge")
	a := sourceCatalogAssignment(t, admin, cameraNetwork(t, admin, "satellite-managed-lan", nil), host)
	prepared := sourceCatalogPrepare(t, admin, a, "satellite-managed-cam", "rtsp://192.0.2.10/satellite-managed-cam")
	sourceCatalogPublish(t, admin, a, prepared)

	owner := c.dial(sourceCatalogID(t, a.GetProducer().GetId()))
	snapshot, err := api.NewCameraAssignmentServiceClient(owner).Snapshot(ctx, api.CameraAssignmentSnapshotRequest_builder{Ref: api.CameraAssignmentRef_builder{Id: a.GetId()}.Build()}.Build())
	require.NoError(t, err)
	var proposals []*api.SourceProposal
	for _, spec := range snapshot.GetSources() {
		proposals = append(proposals, api.SourceProposal_builder{Source: api.SourceRef_builder{Id: spec.GetSourceId()}.Build(), Profile: api.SegmentProfile_builder{MaxBitrate: 1_128_000, DurationSeconds: 30}.Build(), ContentType: "video/mp4", LiveCapable: true}.Build())
	}
	require.NotEmpty(t, proposals)
	negotiate := func(satellite, preview bool) *api.SetNegotiateResponse {
		t.Helper()
		resp, err := api.NewSetServiceClient(owner).Negotiate(ctx, api.SetNegotiateRequest_builder{
			Ref:       api.SetRef_builder{Id: a.GetSet().GetId()}.Build(),
			Catalog:   api.CameraApplyTarget_builder{AssignmentId: a.GetId(), AssignmentGeneration: a.GetGeneration(), CatalogRevision: snapshot.GetRevision()}.Build(),
			Sources:   proposals,
			Satellite: satellite,
			Preview:   preview,
		}.Build())
		require.NoError(t, err)
		return resp
	}

	// `serve all` runs a satellite of its own (§41.10); once it is adopted
	// and heard, a managed negotiation that asks is answered with it.
	var assigned []byte
	require.Eventually(t, func() bool {
		assigned = negotiate(true, false).GetSatellite().GetSatelliteId()
		return len(assigned) > 0
	}, 30*time.Second, 200*time.Millisecond, "a managed negotiation names the satellite")
	require.Equal(t, assigned, negotiate(true, false).GetSatellite().GetSatelliteId(), "and keeps naming the same one")
	require.Equal(t, assigned, negotiate(true, true).GetSatellite().GetSatelliteId(), "a preview as well")
	require.Nil(t, negotiate(false, false).GetSatellite(), "one that does not ask is given none")
}
