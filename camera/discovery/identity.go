package discovery

import (
	"sort"
	"sync"

	"github.com/lesomnus/shale/camera"
)

// Inventory requires explicit physical identity claims. It never automatically
// merges observations by IP, advertised name, serial or discovery UUID. Persist
// Snapshot in the embedding application's store and Restore it after restart.
type Inventory struct {
	mu      sync.Mutex
	devices map[camera.DeviceID]camera.DeviceHint
}

func (i *Inventory) Claim(id camera.DeviceID, hint camera.DeviceHint) (camera.DeviceHint, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if id == "" || !authenticated(hint) || (hint.ID != "" && hint.ID != id) {
		return camera.DeviceHint{}, camera.ErrInvalid
	}
	if i.devices == nil {
		i.devices = map[camera.DeviceID]camera.DeviceHint{}
	}
	if prior, ok := i.devices[id]; ok && !sameIdentity(prior, hint) {
		return camera.DeviceHint{}, camera.ErrConflict
	}
	hint.ID = id
	i.devices[id] = cloneHint(hint)
	return cloneHint(hint), nil
}

// Reconcile updates endpoints only for an explicitly selected claimed device
// with matching authenticated identity evidence. Unknown candidates require a
// claim even if a vendor identifier happens to equal one already in inventory.
func (i *Inventory) Reconcile(hint camera.DeviceHint) (camera.DeviceHint, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if hint.ID == "" || !authenticated(hint) {
		return camera.DeviceHint{}, camera.ErrAmbiguousIdentity
	}
	prior, ok := i.devices[hint.ID]
	if !ok {
		return camera.DeviceHint{}, camera.ErrAmbiguousIdentity
	}
	if !sameIdentity(prior, hint) {
		return camera.DeviceHint{}, camera.ErrConflict
	}
	i.devices[hint.ID] = cloneHint(hint)
	return cloneHint(hint), nil
}

func (i *Inventory) Snapshot() []camera.DeviceHint {
	i.mu.Lock()
	defer i.mu.Unlock()
	var out []camera.DeviceHint
	for _, hint := range i.devices {
		out = append(out, cloneHint(hint))
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID < out[b].ID })
	return out
}

func (i *Inventory) Restore(devices []camera.DeviceHint) error {
	for _, hint := range devices {
		if _, err := i.Claim(hint.ID, hint); err != nil {
			return err
		}
	}
	return nil
}

func authenticated(hint camera.DeviceHint) bool {
	for _, e := range hint.Evidence {
		if e.Authenticated {
			return true
		}
	}
	return false
}

func sameIdentity(a, b camera.DeviceHint) bool {
	// An explicit authenticated claim alone may identify a device without a
	// useful serial. Endpoint-only rediscovery still cannot update such a record.
	if a.Manufacturer != b.Manufacturer || a.Model != b.Model || a.Serial != b.Serial || a.Hardware != b.Hardware {
		return false
	}
	if a.DiscoveryID != "" && a.DiscoveryID != b.DiscoveryID {
		return false
	}
	return (a.Serial != "" && b.Serial != "") || (a.DiscoveryID != "" && a.DiscoveryID == b.DiscoveryID)
}

func cloneHint(h camera.DeviceHint) camera.DeviceHint {
	h.Endpoints = append([]camera.Endpoint(nil), h.Endpoints...)
	h.Evidence = append([]camera.Evidence(nil), h.Evidence...)
	return h
}
