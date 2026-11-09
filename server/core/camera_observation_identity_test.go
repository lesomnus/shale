package core

import (
	"testing"

	"github.com/lesomnus/shale/api"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCameraInitialMACKeyReusesExplicitInventoryKey(t *testing.T) {
	const mac = "02:00:00:00:00:01"
	r := api.CameraPolicyObserveRequest_builder{DiscoveryId: "mac/" + mac, Identity: api.CameraIdentityEvidence_builder{Macs: []string{mac}}.Build()}.Build()
	key, keys, gotMAC, err := cameraObservationKeys(r)
	require.NoError(t, err)
	require.Equal(t, "mac/"+mac, key)
	require.Equal(t, mac, gotMAC)
	require.Contains(t, keys, legacyCameraDiscoveryKey("mac/"+mac), "old hashed MAC observations stay resolvable")
	r.GetIdentity().SetMacs([]string{"02:00:00:00:00:02"})
	_, _, _, err = cameraObservationKeys(r)
	require.Error(t, err)
}

func TestCameraObservationReconciliationNeverRedirectsAuthenticatedOrActiveDevice(t *testing.T) {
	const mac = "02:00:00:00:00:01"
	const epr = "urn:uuid:9c654a77-3a86-4466-b189-7bbdc38ad248"
	endpoint := func(uri string) []*api.CameraObservedEndpoint {
		return []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: uri}.Build()}
	}
	current := api.CameraDevice_builder{Identity: api.CameraIdentityEvidence_builder{Macs: []string{mac}, Authenticated: true}.Build(), Endpoints: endpoint("http://192.0.2.10/onvif")}.Build()
	r := api.CameraPolicyObserveRequest_builder{DiscoveryId: epr, Identity: api.CameraIdentityEvidence_builder{Macs: []string{mac}}.Build(), Endpoints: endpoint("http://192.0.2.10/onvif")}.Build()
	require.NoError(t, cameraObservationReconcile(current, r), "same transport can record the later EPR as an unauthenticated hint")
	current.SetEnrollment(api.CameraEnrollmentObservation_builder{DiscoveryId: epr}.Build())
	require.Equal(t, epr, cameraObservationDiscoveryID(current, "mac/"+mac), "a later initial-only observation cannot erase known EPR evidence")
	r.SetEndpoints(endpoint("http://192.0.2.20/onvif"))
	require.Equal(t, codes.Aborted, status.Code(cameraObservationReconcile(current, r)))
	current.GetIdentity().SetAuthenticated(false)
	require.NoError(t, cameraObservationReconcile(current, r), "unclaimed candidates may update address evidence before execution")
	current.SetActiveJobId([]byte{1})
	require.Equal(t, codes.Aborted, status.Code(cameraObservationReconcile(current, r)))
	current.SetActiveJobId(nil)
	r.SetEndpoints(current.GetEndpoints())
	r.GetIdentity().SetMacs([]string{"02:00:00:00:00:02"})
	require.Equal(t, codes.Aborted, status.Code(cameraObservationReconcile(current, r)))
	r.GetIdentity().SetMacs([]string{mac})
	r.GetIdentity().SetMacs([]string{mac, "02:00:00:00:00:02"})
	require.Equal(t, codes.Aborted, status.Code(cameraObservationReconcile(current, r)), "a second asserted interface cannot poison the selected MAC identity")
	r.GetIdentity().SetMacs([]string{mac})
	r.SetDiscoveryId("urn:uuid:60c1ccab-2b74-4229-84a6-446fb7bc1cc9")
	require.Equal(t, codes.Aborted, status.Code(cameraObservationReconcile(current, r)))
}
