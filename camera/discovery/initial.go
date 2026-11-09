package discovery

import (
	"context"
	"slices"
	"strings"

	"github.com/lesomnus/shale/camera"
)

// WithInitial composes an injected observation source with normal discovery.
// It sends neither credentials nor writes. The caller must still validate every
// resulting endpoint against the current assignment before recording evidence.
type WithInitial struct {
	Discoverer camera.Discoverer
	Provider   camera.InitialObservationProvider
}

func (d WithInitial) Discover(ctx context.Context, scope camera.DiscoveryScope) ([]camera.Observation, error) {
	if d.Provider == nil {
		return nil, camera.ErrInvalid
	}
	initial, err := d.Provider.ObserveInitial(ctx, scope)
	if err != nil {
		return nil, err
	}
	var primary []camera.Observation
	if d.Discoverer != nil {
		primary, err = d.Discoverer.Discover(ctx, scope)
		if err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return combineInitial(scope, primary, initial)
}

func observationHost(o camera.Observation, scope string) (string, error) {
	if o.Scope != scope || len(o.Device.Endpoints) == 0 || len(o.Device.Endpoints) > 32 {
		return "", camera.ErrForbidden
	}
	host := ""
	for _, ep := range o.Device.Endpoints {
		current, err := seedHost(ep, scope)
		if err != nil {
			return "", err
		}
		if host != "" && host != current {
			return "", camera.ErrAmbiguousIdentity
		}
		host = current
	}
	return host, nil
}

func combineInitial(scope camera.DiscoveryScope, primary, initial []camera.Observation) ([]camera.Observation, error) {
	if len(primary)+len(initial) > 256 {
		return nil, camera.ErrInvalid
	}
	byHost, macHosts := map[string]camera.Observation{}, map[string]string{}
	for _, o := range initial {
		host, err := observationHost(o, scope.ID)
		mac := unicastMAC(o.Device.MAC)
		if err != nil {
			return nil, err
		}
		if o.Device.ID != "" || mac == "" || o.Device.DiscoveryID != "" {
			return nil, camera.ErrInvalid
		}
		if previous, ok := byHost[host]; ok && previous.Device.MAC != mac || macHosts[mac] != "" && macHosts[mac] != host {
			return nil, camera.ErrAmbiguousIdentity
		}
		o.Device.MAC = mac
		o.Device.Endpoints = append([]camera.Endpoint(nil), o.Device.Endpoints...)
		o.Device.Evidence = append([]camera.Evidence(nil), o.Device.Evidence...)
		for i := range o.Device.Evidence {
			o.Device.Evidence[i].Authenticated = false
		}
		byHost[host], macHosts[mac] = o, host
	}
	var out []camera.Observation
	merged := map[string]int{}
	for _, o := range primary {
		host, err := observationHost(o, scope.ID)
		if err != nil {
			return nil, err
		}
		hint, ok := byHost[host]
		if !ok {
			out = append(out, o)
			continue
		}
		if o.Device.MAC != "" && unicastMAC(o.Device.MAC) != hint.Device.MAC {
			return nil, camera.ErrAmbiguousIdentity
		}
		o.Device.MAC = hint.Device.MAC
		o.Device.Endpoints = append([]camera.Endpoint(nil), o.Device.Endpoints...)
		o.Device.Evidence = append(append([]camera.Evidence(nil), o.Device.Evidence...), hint.Device.Evidence...)
		if at, exists := merged[host]; exists {
			previous := out[at]
			if previous.Device.DiscoveryID != "" && o.Device.DiscoveryID != "" && !strings.EqualFold(previous.Device.DiscoveryID, o.Device.DiscoveryID) {
				return nil, camera.ErrAmbiguousIdentity
			}
			if previous.Device.DiscoveryID == "" && o.Device.DiscoveryID != "" {
				previous, o = o, previous
			}
			for _, ep := range o.Device.Endpoints {
				if !slices.Contains(previous.Device.Endpoints, ep) {
					previous.Device.Endpoints = append(previous.Device.Endpoints, ep)
				}
			}
			if len(previous.Device.Endpoints) > 32 {
				return nil, camera.ErrInvalid
			}
			out[at] = previous
			continue
		}
		merged[host] = len(out)
		out = append(out, o)
	}
	for host, o := range byHost {
		if _, exists := merged[host]; !exists {
			out = append(out, o)
		}
	}
	slices.SortFunc(out, func(a, b camera.Observation) int { return strings.Compare(a.Device.CandidateID, b.Device.CandidateID) })
	return out, nil
}
