package core

import (
	"context"
	"net/netip"
	"net/url"
	"slices"
	"strings"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/cameradevice"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const cameraObservationLookupLimit = 256

func cameraObservedHost(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	ip, err := netip.ParseAddr(u.Hostname())
	if err != nil {
		return ""
	}
	return ip.Unmap().String()
}

// Resolve every discovery key against one bounded, locked network snapshot.
// Checking only a canonical index hit would hide legacy-format MAC duplicates
// or another device which already owns the proposed camera host.
func (s Core) cameraObservationInventory(ctx context.Context, n *api.CameraNetwork, r *api.CameraPolicyObserveRequest, keys []string, mac string) ([]*ent.CameraDevice, error) {
	rows, err := s.ent(ctx).CameraDevice.Query().Where(cameradevice.NetworkIdEQ(mustId(n.GetId()).Uuid()), cameradevice.DateErasedIsNil()).Select(cameradevice.FieldId, cameradevice.FieldDeviceKey, cameradevice.FieldIdentity, cameradevice.FieldEnrollment, cameradevice.FieldEndpoints).Limit(cameraObservationLookupLimit + 1).All(ctx)
	if err != nil {
		return nil, err
	}
	return cameraSelectObservationInventory(rows, r, keys, mac)
}

func cameraSelectObservationInventory(rows []*ent.CameraDevice, r *api.CameraPolicyObserveRequest, keys []string, mac string) ([]*ent.CameraDevice, error) {
	if len(rows) > cameraObservationLookupLimit {
		return nil, status.Error(codes.ResourceExhausted, "camera inventory lookup requires explicit operator enrollment or a smaller network")
	}
	var matched []*ent.CameraDevice
	endpointOwners := map[string]bool{}
	for _, row := range rows {
		match := slices.Contains(keys, row.DeviceKey) || strings.HasPrefix(r.GetDiscoveryId(), "urn:uuid:") && row.Enrollment.GetDiscoveryId() == r.GetDiscoveryId()
		if mac != "" {
			if strings.HasPrefix(row.DeviceKey, "mac/") && cameraObservationMAC(strings.TrimPrefix(row.DeviceKey, "mac/")) == mac {
				match = true
			}
			for _, old := range row.Identity.GetMacs() {
				match = match || cameraObservationMAC(old) == mac
			}
		}
		if match {
			matched = append(matched, row)
		}
		for _, old := range row.Endpoints {
			for _, incoming := range r.GetEndpoints() {
				if host := cameraObservedHost(old.GetUri()); host != "" && host == cameraObservedHost(incoming.GetUri()) {
					endpointOwners[row.Id.String()] = true
				}
			}
		}
	}
	if len(matched) > 1 {
		return nil, status.Error(codes.Aborted, "legacy camera interface evidence identifies conflicting inventory rows")
	}
	for id := range endpointOwners {
		if len(matched) == 0 || matched[0].Id.String() != id {
			// Endpoint equality is a collision signal, never an identity claim.
			// Do not create a second physical writer or merge records by address.
			return nil, status.Error(codes.Aborted, "camera endpoint already has unresolved inventory ownership")
		}
	}
	return matched, nil
}
