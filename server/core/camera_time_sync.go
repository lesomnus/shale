package core

import (
	"net/netip"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/camera"
	presetwire "github.com/lesomnus/shale/internal/camerapreset"
)

// cameraPolicyTimeServers refuses a policy whose preset names an explicit NTP
// server outside the policy network's CIDRs, in any rule that can apply on
// that network (docs/camera-policy.md, "Camera time sync"). It is the early
// form of what the worker and the job's checkpoints enforce on every camera:
// "edge" resolves only on the worker, so it is checked there.
func cameraPolicyTimeServers(network *api.CameraNetwork, preset *api.CameraPreset) error {
	var prefixes []netip.Prefix
	for _, raw := range network.GetCidrs() {
		if p, err := netip.ParsePrefix(raw); err == nil {
			prefixes = append(prefixes, p.Masked())
		}
	}
	id := mustId(network.GetId()).String()
	for _, rule := range preset.GetRules() {
		if scope := rule.GetScope().GetNetworkId(); scope != "" && scope != id {
			continue
		}
		for _, settings := range append([]*api.CameraPresetSettings{rule.GetSettings()}, rule.GetAlternatives().GetCandidates()...) {
			server, explicit := presetwire.TimeSync(settings.GetTimeSync()).Server()
			if explicit && !camera.TimeServerAllowed(server, prefixes) {
				return invalid("preset.time_sync", "a preset's NTP server must be inside the camera network")
			}
		}
	}
	return nil
}
