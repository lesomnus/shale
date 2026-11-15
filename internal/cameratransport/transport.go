// Package cameratransport is what the CP and the Worker share about a camera
// whose endpoints were moved by a verified transport candidate
// (docs/camera-worker.md, "Transport candidates"). It approves nothing.
package cameratransport

import (
	"net"
	"slices"
	"strings"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
)

// The states of a transport candidate.
const (
	StatePending  = "pending"
	StateAccepted = "accepted"
	StateOperator = "operator_required"
)

// EndpointSet is the canonical form two endpoint lists are compared in.
func EndpointSet(eps []*api.CameraObservedEndpoint) []string {
	set := make([]string, len(eps))
	for i, ep := range eps {
		set[i] = ep.GetUri()
	}
	slices.Sort(set)
	return slices.Compact(set)
}

// Recovered says whether d's authorized endpoints are those of a candidate
// the CP accepted after verifying its key. Every job there must reinspect the
// recorded serial and MAC before it writes anything; an operator's
// CameraDevice.Observe to other endpoints clears the candidates.
func Recovered(d *api.CameraDevice) bool {
	set := EndpointSet(d.GetEndpoints())
	for _, c := range d.GetTransport().GetCandidates() {
		if c.GetState() == StateAccepted && slices.Equal(EndpointSet(c.GetEndpoints()), set) {
			return true
		}
	}
	return false
}

// SameRecorded says whether an authenticated inspection h reports the
// recorded identity's non-empty serial and one of its MACs.
func SameRecorded(recorded *api.CameraIdentityEvidence, h camera.DeviceHint) bool {
	authenticated := false
	for _, e := range h.Evidence {
		authenticated = authenticated || e.Authenticated
	}
	if !authenticated || !recorded.GetAuthenticated() || recorded.GetSerial() == "" || h.Serial != recorded.GetSerial() {
		return false
	}
	mac := canonicalMAC(h.MAC)
	return mac != "" && slices.ContainsFunc(recorded.GetMacs(), func(v string) bool { return canonicalMAC(v) == mac })
}

func canonicalMAC(raw string) string {
	mac, err := net.ParseMAC(strings.TrimSpace(raw))
	if err != nil || len(mac) != 6 {
		return ""
	}
	return mac.String()
}
