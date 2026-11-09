package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"slices"
	"strings"

	"github.com/lesomnus/shale/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func cameraObservationMAC(raw string) string {
	mac, err := net.ParseMAC(raw)
	if err != nil || len(mac) != 6 || mac[0]&1 != 0 || bytes.Equal(mac, make([]byte, 6)) {
		return ""
	}
	return mac.String()
}

func cameraDiscoveryKey(id string) string {
	if strings.HasPrefix(id, "mac/") {
		if mac := cameraObservationMAC(strings.TrimPrefix(id, "mac/")); mac != "" {
			return "mac/" + mac
		}
	}
	return legacyCameraDiscoveryKey(id)
}

func legacyCameraDiscoveryKey(id string) string {
	digest := sha256.Sum256([]byte(id))
	return "discovery/" + hex.EncodeToString(digest[:])
}

// MAC and EPR are rediscovery evidence only. An explicit manual MAC inventory
// key stays stable when ONVIF starts advertising after activation.
func cameraObservationKeys(r *api.CameraPolicyObserveRequest) (string, []string, string, error) {
	discovery := r.GetDiscoveryId()
	mac := ""
	if len(r.GetIdentity().GetMacs()) == 1 {
		mac = cameraObservationMAC(r.GetIdentity().GetMacs()[0])
	}
	if strings.HasPrefix(discovery, "mac/") && (mac == "" || discovery != "mac/"+mac) {
		return "", nil, "", invalid("discovery_id", "a MAC observation requires matching canonical interface evidence")
	}
	key := cameraDiscoveryKey(discovery)
	keys := []string{key}
	if mac != "" {
		key = "mac/" + mac
		keys = append(keys, key, legacyCameraDiscoveryKey(key))
	}
	return key, slices.Compact(keys), mac, nil
}

func cameraObservationEndpointSet(eps []*api.CameraObservedEndpoint) []string {
	set := make([]string, len(eps))
	for i, ep := range eps {
		set[i] = ep.GetUri()
	}
	slices.Sort(set)
	return slices.Compact(set)
}

func cameraObservationDiscoveryID(d *api.CameraDevice, incoming string) string {
	known := d.GetEnrollment().GetDiscoveryId()
	if strings.HasPrefix(known, "urn:uuid:") && strings.HasPrefix(incoming, "mac/") {
		return known
	}
	return incoming
}

func cameraObservationReconcile(current *api.CameraDevice, r *api.CameraPolicyObserveRequest) error {
	known, incoming := current.GetEnrollment().GetDiscoveryId(), r.GetDiscoveryId()
	if strings.HasPrefix(known, "urn:uuid:") && strings.HasPrefix(incoming, "urn:uuid:") && known != incoming {
		return status.Error(codes.Aborted, "camera discovery identity requires operator reconciliation")
	}
	macs := current.GetIdentity().GetMacs()
	if len(macs) > 0 && len(r.GetIdentity().GetMacs()) > 0 {
		normalize := func(values []string) []string {
			out := make([]string, len(values))
			for i, raw := range values {
				out[i] = cameraObservationMAC(raw)
			}
			slices.Sort(out)
			return slices.Compact(out)
		}
		if !slices.Equal(normalize(macs), normalize(r.GetIdentity().GetMacs())) {
			return status.Error(codes.Aborted, "camera interface evidence requires operator reconciliation")
		}
	}
	if (current.GetIdentity().GetAuthenticated() || len(current.GetActiveJobId()) > 0) && !slices.Equal(cameraObservationEndpointSet(current.GetEndpoints()), cameraObservationEndpointSet(r.GetEndpoints())) {
		// A fresh MAC/EPR is not permission to redirect an existing camera's
		// credentials or active physical writer to another transport endpoint.
		return status.Error(codes.Aborted, "camera transport change requires operator reconciliation")
	}
	return nil
}
