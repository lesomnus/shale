package cameratransport

import (
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	"github.com/stretchr/testify/require"
)

func endpoints(uris ...string) []*api.CameraObservedEndpoint {
	out := make([]*api.CameraObservedEndpoint, len(uris))
	for i, uri := range uris {
		out[i] = api.CameraObservedEndpoint_builder{Uri: uri}.Build()
	}
	return out
}

func TestRecoveredOnlyAtAcceptedEndpoints(t *testing.T) {
	moved := endpoints("http://192.0.2.20:2020/onvif", "rtsp://192.0.2.20:554/stream1")
	d := api.CameraDevice_builder{Endpoints: endpoints("rtsp://192.0.2.20:554/stream1", "http://192.0.2.20:2020/onvif")}.Build()
	require.False(t, Recovered(d))
	d.SetTransport(api.CameraTransportState_builder{Candidates: []*api.CameraTransportCandidate{
		api.CameraTransportCandidate_builder{State: StateOperator, Endpoints: moved}.Build(),
	}}.Build())
	require.False(t, Recovered(d), "an operator-required candidate moved nothing")
	d.GetTransport().GetCandidates()[0].SetState(StateAccepted)
	require.True(t, Recovered(d), "the same set in any order")
	d.SetEndpoints(endpoints("http://192.0.2.30:2020/onvif"))
	require.False(t, Recovered(d), "after another move the old acceptance says nothing")
}

func TestSameRecordedNeedsAuthenticatedSerialAndMAC(t *testing.T) {
	recorded := api.CameraIdentityEvidence_builder{Serial: "unit-1", Macs: []string{"02-00-00-00-00-01"}, Authenticated: true}.Build()
	ok := camera.DeviceHint{Serial: "unit-1", MAC: "02:00:00:00:00:01", Evidence: []camera.Evidence{{Authenticated: true}}}
	require.True(t, SameRecorded(recorded, ok))
	for name, edit := range map[string]func(*camera.DeviceHint){
		"unauthenticated": func(h *camera.DeviceHint) { h.Evidence = []camera.Evidence{{}} },
		"serial":          func(h *camera.DeviceHint) { h.Serial = "unit-2" },
		"mac":             func(h *camera.DeviceHint) { h.MAC = "02:00:00:00:00:02" },
		"no_mac":          func(h *camera.DeviceHint) { h.MAC = "" },
	} {
		h := ok
		h.Evidence = append([]camera.Evidence(nil), ok.Evidence...)
		edit(&h)
		require.False(t, SameRecorded(recorded, h), name)
	}
	require.False(t, SameRecorded(api.CameraIdentityEvidence_builder{Macs: []string{"02:00:00:00:00:01"}, Authenticated: true}.Build(), camera.DeviceHint{MAC: "02:00:00:00:00:01", Evidence: []camera.Evidence{{Authenticated: true}}}), "an empty recorded serial matches nothing")
	recorded.SetAuthenticated(false)
	require.False(t, SameRecorded(recorded, ok), "an unauthenticated record matches nothing")
}
