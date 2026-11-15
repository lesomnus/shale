package core

import (
	"strings"
	"testing"
	"time"

	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func transportEndpoints(uris ...string) []*api.CameraObservedEndpoint {
	out := make([]*api.CameraObservedEndpoint, len(uris))
	for i, uri := range uris {
		out[i] = api.CameraObservedEndpoint_builder{Uri: uri}.Build()
	}
	return out
}

func transportCandidate(id, uri, state string, now time.Time, expires time.Duration) *api.CameraTransportCandidate {
	return api.CameraTransportCandidate_builder{Id: id, Endpoints: transportEndpoints(uri), State: state, Code: cameraTransportRequired, AssignmentGeneration: 2, ObservationRevision: 5, DateObserved: timestamppb.New(now.Add(-time.Second)), DateExpires: timestamppb.New(now.Add(expires))}.Build()
}

func TestCameraTransportOriginIsOneHostOnThePinnedPort(t *testing.T) {
	origin, ok := cameraTransportOrigin(transportEndpoints("http://192.0.2.20:2020/onvif/device_service", "rtsp://192.0.2.20:554/stream1"), 443)
	require.True(t, ok)
	require.Equal(t, "https://192.0.2.20:443", origin)
	origin, ok = cameraTransportOrigin(transportEndpoints("http://[::ffff:192.0.2.20]:2020/onvif"), 8443)
	require.True(t, ok)
	require.Equal(t, "https://192.0.2.20:8443", origin, "an IPv4-mapped spelling is the same host")
	origin, ok = cameraTransportOrigin(transportEndpoints("http://[2001:db8::20]:2020/onvif"), 443)
	require.True(t, ok)
	require.Equal(t, "https://[2001:db8::20]:443", origin)
	for _, eps := range [][]*api.CameraObservedEndpoint{
		transportEndpoints("http://192.0.2.20/onvif", "http://192.0.2.21/onvif"),
		transportEndpoints("http://camera.example/onvif"),
		nil,
	} {
		_, ok := cameraTransportOrigin(eps, 443)
		require.False(t, ok)
	}
	_, ok = cameraTransportOrigin(transportEndpoints("http://192.0.2.20/onvif"), 0)
	require.False(t, ok, "no pinned HTTPS port, no origin")
}

func TestCameraTransportReasonOrder(t *testing.T) {
	eps := transportEndpoints("http://192.0.2.20:2020/onvif")
	trust := cameraTransportTrust{port: 443}
	code, origin := cameraTransportReason(false, false, trust, eps)
	require.Empty(t, code)
	require.Equal(t, "https://192.0.2.20:443", origin)
	code, _ = cameraTransportReason(true, true, cameraTransportTrust{code: cameraTransportMutation}, eps)
	require.Equal(t, cameraTransportEvidence, code, "contradicting hints come first")
	code, _ = cameraTransportReason(false, true, cameraTransportTrust{code: cameraTransportMutation}, eps)
	require.Equal(t, cameraTransportHostOwned, code)
	code, _ = cameraTransportReason(false, false, cameraTransportTrust{code: cameraTransportTrustMissing}, eps)
	require.Equal(t, cameraTransportTrustMissing, code)
	code, _ = cameraTransportReason(false, false, trust, transportEndpoints("http://192.0.2.20/a", "http://192.0.2.21/b"))
	require.Equal(t, cameraTransportAmbiguous, code)
}

func TestCameraTransportMaintainSettlesStaleCandidates(t *testing.T) {
	now := time.Now()
	a := api.CameraAssignment_builder{Generation: 2}.Build()
	d := api.CameraDevice_builder{ObservationRevision: 5}.Build()
	expired := transportCandidate("a", "http://192.0.2.20/onvif", cameraTransportPending, now, -time.Second)
	generation := transportCandidate("b", "http://192.0.2.21/onvif", cameraTransportPending, now, time.Minute)
	generation.SetAssignmentGeneration(1)
	moved := transportCandidate("c", "http://192.0.2.22/onvif", cameraTransportVerified, now, time.Minute)
	moved.SetObservationRevision(4)
	current := transportCandidate("d", "http://192.0.2.23/onvif", cameraTransportPending, now, time.Minute)
	settled := transportCandidate("e", "http://192.0.2.24/onvif", cameraTransportOperator, now, -time.Second)
	settled.SetCode(cameraTransportKeyMismatch)
	state := api.CameraTransportState_builder{Candidates: []*api.CameraTransportCandidate{expired, generation, moved, current, settled}}.Build()
	cameraTransportMaintain(state, a, d, now)
	got := map[string][2]string{}
	for _, c := range state.GetCandidates() {
		got[c.GetId()] = [2]string{c.GetState(), c.GetCode()}
	}
	require.Equal(t, map[string][2]string{
		"a": {cameraTransportOperator, cameraTransportExpired},
		"b": {cameraTransportOperator, cameraTransportGeneration},
		"c": {cameraTransportOperator, cameraTransportMoved},
		"d": {cameraTransportPending, cameraTransportRequired},
		"e": {cameraTransportOperator, cameraTransportKeyMismatch},
	}, got)
}

func TestCameraTransportAmbiguityAndBound(t *testing.T) {
	now := time.Now()
	one := transportCandidate("a", "http://192.0.2.20/onvif", cameraTransportPending, now, time.Minute)
	same := transportCandidate("b", "http://192.0.2.20/onvif", cameraTransportPending, now, time.Minute)
	state := api.CameraTransportState_builder{Candidates: []*api.CameraTransportCandidate{one, same}}.Build()
	cameraTransportAmbiguity(state, now)
	require.Equal(t, cameraTransportPending, one.GetState(), "one endpoint set is not ambiguous")
	impostor := transportCandidate("c", "http://192.0.2.21/onvif", cameraTransportOperator, now, time.Minute)
	impostor.SetCode(cameraTransportKeyMismatch)
	gone := transportCandidate("d", "http://192.0.2.22/onvif", cameraTransportPending, now, -time.Minute)
	state.SetCandidates([]*api.CameraTransportCandidate{one, impostor, gone})
	cameraTransportAmbiguity(state, now)
	require.Equal(t, cameraTransportAmbiguous, one.GetCode(), "a live impostor makes the real move ambiguous too")
	require.Equal(t, cameraTransportKeyMismatch, impostor.GetCode(), "a decided candidate keeps its own reason")
	require.Equal(t, cameraTransportPending, gone.GetState(), "an expired candidate is no evidence either way")

	var many []*api.CameraTransportCandidate
	for i := range 6 {
		expires := time.Minute
		if i%2 == 0 {
			expires = -time.Minute
		}
		many = append(many, transportCandidate(strings.Repeat(string(rune('a'+i)), 2), "http://192.0.2.30/onvif", cameraTransportPending, now, expires))
	}
	state.SetCandidates(many)
	cameraTransportBound(state, now)
	require.Len(t, state.GetCandidates(), cameraTransportLimit)
	var ids []string
	for _, c := range state.GetCandidates() {
		ids = append(ids, c.GetId())
	}
	require.Equal(t, []string{"bb", "dd", "ee", "ff"}, ids, "settled candidates go first, oldest first")
}

func TestCameraTransportFindNeedsTodaysGenerationAndRevision(t *testing.T) {
	now := time.Now()
	a := api.CameraAssignment_builder{Generation: 2}.Build()
	d := api.CameraDevice_builder{ObservationRevision: 5}.Build()
	c := transportCandidate("a", "http://192.0.2.20/onvif", cameraTransportPending, now, time.Minute)
	state := api.CameraTransportState_builder{Candidates: []*api.CameraTransportCandidate{c}}.Build()
	require.Same(t, c, cameraTransportFind(state, a, d, transportEndpoints("http://192.0.2.20/onvif"), now))
	require.Nil(t, cameraTransportFind(state, a, d, transportEndpoints("http://192.0.2.21/onvif"), now))
	require.Nil(t, cameraTransportFind(state, api.CameraAssignment_builder{Generation: 3}.Build(), d, transportEndpoints("http://192.0.2.20/onvif"), now))
	require.Nil(t, cameraTransportFind(state, a, api.CameraDevice_builder{ObservationRevision: 6}.Build(), transportEndpoints("http://192.0.2.20/onvif"), now))
	require.Nil(t, cameraTransportFind(state, a, d, transportEndpoints("http://192.0.2.20/onvif"), now.Add(2*time.Minute)), "expired evidence is never repeated")
}

func TestCameraTransportGrantSyntax(t *testing.T) {
	valid := api.CameraTransportGrant_builder{
		CandidateId: strings.Repeat("a", 32), AssignmentId: mustId(make([]byte, 16)).Bytes(), DeviceId: make([]byte, 16), NetworkId: make([]byte, 16), JobId: make([]byte, 16),
		Trust:      api.CameraDeviceTLSIdentity_builder{DeviceId: "019a0000-0000-7000-8000-000000000001", SpkiSha256: "sha256:" + strings.Repeat("0", 64)}.Build(),
		DateIssued: timestamppb.Now(), DateExpires: timestamppb.Now(),
	}.Build()
	require.Error(t, cameraTransportGrantValid(nil))
	g := api.CameraTransportGrant_builder{CandidateId: "short"}.Build()
	require.Error(t, cameraTransportGrantValid(g))
	g = api.CameraTransportGrant_builder{CandidateId: valid.GetCandidateId(), Trust: valid.GetTrust()}.Build()
	require.Error(t, cameraTransportGrantValid(g), "a grant names its times and rows")
}
