package core

import (
	"testing"

	"github.com/lesomnus/payday/pdid"
	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
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

func TestCameraInitialFallbackFindsLegacyMACAndRejectsUnresolvedEndpointOwner(t *testing.T) {
	const mac = "a8:29:dc:c6:24:31"
	row := &ent.CameraDevice{Id: pdid.New(DomCameraDevice).Uuid(), DeviceKey: "operator-camera-a", Identity: api.CameraIdentityEvidence_builder{Macs: []string{"A8-29-DC-C6-24-31"}, Authenticated: true}.Build(), Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.10:2020/onvif"}.Build()}}
	r := api.CameraPolicyObserveRequest_builder{DiscoveryId: "mac/" + mac, Identity: api.CameraIdentityEvidence_builder{Macs: []string{mac}}.Build(), Endpoints: row.Endpoints}.Build()
	_, keys, _, err := cameraObservationKeys(r)
	require.NoError(t, err)
	rows, err := cameraSelectObservationInventory([]*ent.CameraDevice{row}, r, keys, mac)
	require.NoError(t, err)
	require.Equal(t, []*ent.CameraDevice{row}, rows)
	r.SetDiscoveryId("urn:uuid:9c654a77-3a86-4466-b189-7bbdc38ad248")
	r.GetIdentity().SetMacs(nil)
	for _, uri := range []string{"http://192.0.2.10:2020/onvif", "http://192.0.2.10:9999/other", "http://[::ffff:192.0.2.10]:2020/onvif"} {
		r.SetEndpoints([]*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: uri}.Build()})
		_, err = cameraSelectObservationInventory([]*ent.CameraDevice{row}, r, nil, "")
		require.Equal(t, codes.Aborted, status.Code(err), "a host collision is denied, never used as identity: %s", uri)
	}
	_, err = cameraSelectObservationInventory(make([]*ent.CameraDevice, cameraObservationLookupLimit+1), r, nil, "")
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	canonical := &ent.CameraDevice{Id: pdid.New(DomCameraDevice).Uuid(), DeviceKey: "mac/" + mac, Identity: api.CameraIdentityEvidence_builder{Macs: []string{mac}}.Build(), Endpoints: []*api.CameraObservedEndpoint{api.CameraObservedEndpoint_builder{Uri: "http://192.0.2.20/onvif"}.Build()}}
	r.SetDiscoveryId("mac/" + mac)
	r.GetIdentity().SetMacs([]string{mac})
	r.SetEndpoints(canonical.Endpoints)
	_, err = cameraSelectObservationInventory([]*ent.CameraDevice{canonical, row}, r, keys, mac)
	require.Equal(t, codes.Aborted, status.Code(err), "a canonical hit cannot hide a format-equivalent legacy MAC row")
	const another = "02:00:00:00:00:01"
	canonical.DeviceKey = "mac/" + another
	canonical.Identity.SetMacs([]string{another})
	r.SetDiscoveryId("mac/" + another)
	r.GetIdentity().SetMacs([]string{another})
	r.SetEndpoints(row.Endpoints)
	_, keys, _, err = cameraObservationKeys(r)
	require.NoError(t, err)
	_, err = cameraSelectObservationInventory([]*ent.CameraDevice{canonical, row}, r, keys, another)
	require.Equal(t, codes.Aborted, status.Code(err), "an indexed unclaimed candidate cannot move onto another owned host")
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
