package core

import (
	"bytes"
	"context"
	"net"
	"net/netip"
	"strings"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/ent"
	"github.com/lesomnus/shale/internal/ent/cameraassignment"
	"github.com/lesomnus/shale/internal/ent/camerabinding"
	"github.com/lesomnus/shale/internal/ent/cameradevice"
	"github.com/lesomnus/z"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type coreCameraNetwork struct {
	Core
	api.CameraNetworkServiceServer
}
type coreCameraDevice struct {
	Core
	api.CameraDeviceServiceServer
}

func (s Core) CameraNetwork() api.CameraNetworkServiceServer {
	return coreCameraNetwork{s, s.Next().CameraNetwork()}
}
func (s Core) CameraDevice() api.CameraDeviceServiceServer {
	return coreCameraDevice{s, s.Next().CameraDevice()}
}
func (s coreCameraNetwork) Configure(ctx context.Context, r *api.CameraNetworkConfigureRequest) (*api.CameraNetwork, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	if _, err := cameraNetworks(r.GetCidrs()); err != nil {
		return nil, err
	}
	if r.GetInterfaceHint() != "" {
		if err := cameraToken("interface_hint", r.GetInterfaceHint()); err != nil {
			return nil, err
		}
	}
	if len(r.GetSeeds()) > 32 {
		return nil, invalid("seeds", "too many address seeds")
	}
	for _, v := range r.GetSeeds() {
		if err := cameraURI(v, r.GetCidrs(), false); err != nil {
			return nil, err
		}
	}
	var out *api.CameraNetwork
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) (result error) {
		// Return the database's persisted timestamp precision (PostgreSQL uses
		// microseconds), not the unsaved in-memory nanosecond mutation value.
		defer func() {
			if result == nil && out != nil {
				out, result = nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: out.GetId()}.Build()}.Build())
			}
		}()

		if r.GetRef() == nil {
			if r.GetTenant() == nil {
				return invalid("tenant", "a tenant is required")
			}
			t, err := nx.Tenant().Get(ctx, api.TenantGetRequest_builder{Ref: r.GetTenant()}.Build())
			if err != nil {
				return err
			}
			f, err := actor(ctx)
			if err != nil {
				return err
			}
			if !bytes.Equal(f.Tenant.Bytes(), t.GetId()) {
				return denied("configure network", "network belongs to another tenant")
			}
			var site *api.SiteRef
			if r.GetSite() != nil {
				v, err := nx.Site().Get(ctx, api.SiteGetRequest_builder{Ref: r.GetSite()}.Build())
				if err != nil {
					return err
				}
				if err := cameraSameScope(t.GetId(), nil, v.GetTenant().GetId(), nil); err != nil {
					return err
				}
				site = cameraSite(v)
			}
			out, err = nx.CameraNetwork().Add(ctx, api.CameraNetworkAddRequest_builder{Tenant: api.TenantRef_builder{Id: t.GetId()}.Build(), Site: site, Alias: r.GetAlias(), Name: r.GetName(), Cidrs: r.GetCidrs(), Multicast: r.GetMulticast(), Seeds: r.GetSeeds(), InterfaceHint: r.GetInterfaceHint(), Revision: 1}.Build())
			return err
		}
		v, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: r.GetRef()}.Build())
		if err != nil {
			return err
		}
		if err := New(nx, s.d).cameraLockNetwork(ctx, v.GetId()); err != nil {
			return err
		}
		v, err = nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: v.GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := cameraVersion(r.GetExpectedVersion(), v.GetDateUpdated()); err != nil {
			return err
		}
		if r.GetTenant() != nil || r.GetSite() != nil {
			return invalid("scope", "network tenant and site are immutable")
		}
		// Existing device endpoints must not silently leave the permitted network.
		devices, err := New(nx, s.d).ent(ctx).CameraDevice.Query().Where(cameradevice.NetworkIdEQ(mustId(v.GetId()).Uuid()), cameradevice.DateErasedIsNil()).All(ctx)
		if err != nil {
			return err
		}
		for _, d := range devices {
			if err := cameraEndpoints(d.Endpoints, r.GetCidrs()); err != nil {
				return invalid("cidrs", "existing camera endpoints would leave the network")
			}
		}
		bindings, err := New(nx, s.d).ent(ctx).CameraBinding.Query().Where(camerabinding.HasAssignmentWith(cameraassignment.NetworkIdEQ(mustId(v.GetId()).Uuid())), camerabinding.DateErasedIsNil()).All(ctx)
		if err != nil {
			return err
		}
		for _, binding := range bindings {
			if binding.Desired != nil {
				if err := cameraURI(binding.Desired.GetInput(), r.GetCidrs(), true); err != nil {
					return invalid("cidrs", "published capture input would leave the permitted network")
				}
			}
		}
		out, err = nx.CameraNetwork().Patch(ctx, api.CameraNetworkPatchRequest_builder{Ref: r.GetRef(), Alias: z.Ptr(r.GetAlias()), Name: z.Ptr(r.GetName()), Cidrs: r.GetCidrs(), Multicast: z.Ptr(r.GetMulticast()), Seeds: r.GetSeeds(), InterfaceHint: z.Ptr(r.GetInterfaceHint()), Revision: z.Ptr(v.GetRevision() + 1), DateUpdated: r.GetExpectedVersion()}.Build())
		return err
	})
	if err != nil {
		return nil, cameraConflict(err)
	}
	return out, nil
}
func (s coreCameraDevice) Claim(ctx context.Context, r *api.CameraDeviceClaimRequest) (*api.CameraDevice, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	if err := cameraToken("device_key", r.GetDeviceKey()); err != nil {
		return nil, err
	}
	if _, err := netip.ParseAddr(r.GetDeviceKey()); err == nil {
		return nil, invalid("device_key", "an IP address is not a physical inventory identity")
	}
	var out *api.CameraDevice
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) (result error) {
		// Return the database's persisted timestamp precision (PostgreSQL uses
		// microseconds), not the unsaved in-memory nanosecond mutation value.
		defer func() {
			if result == nil && out != nil {
				out, result = nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: out.GetId()}.Build()}.Build())
			}
		}()

		n, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: r.GetNetwork()}.Build())
		if err != nil {
			return err
		}
		if err := New(nx, s.d).cameraLockNetwork(ctx, n.GetId()); err != nil {
			return err
		}
		n, err = nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: n.GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := cameraEndpoints(r.GetEndpoints(), n.GetCidrs()); err != nil {
			return err
		}
		c := New(nx, s.d)
		v, err := c.ent(ctx).CameraDevice.Query().Where(cameradevice.NetworkIdEQ(mustId(n.GetId()).Uuid()), cameradevice.DeviceKeyEQ(r.GetDeviceKey()), cameradevice.DateErasedIsNil()).Only(ctx)
		if err == nil {
			out, err = nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: v.Id[:]}.Build()}.Build())
			return err
		}
		if !ent.IsNotFound(err) {
			return err
		}
		out, err = nx.CameraDevice().Add(ctx, api.CameraDeviceAddRequest_builder{Tenant: api.TenantRef_builder{Id: n.GetTenant().GetId()}.Build(), Site: cameraSite(n.GetSite()), Network: api.CameraNetworkRef_builder{Id: n.GetId()}.Build(), DeviceKey: r.GetDeviceKey(), Name: r.GetName(), Endpoints: r.GetEndpoints(), ObservationRevision: 1}.Build())
		return err
	})
	if err != nil {
		return nil, cameraConflict(err)
	}
	return out, nil
}
func (s coreCameraDevice) Observe(ctx context.Context, r *api.CameraDeviceObserveRequest) (*api.CameraDevice, error) {
	if err := cameraMessage(r); err != nil {
		return nil, err
	}
	if _, err := cameraOperator(ctx); err != nil {
		return nil, err
	}
	var out *api.CameraDevice
	err := s.tx(ctx, func(ctx context.Context, nx api.Server) (result error) {
		// Return the database's persisted timestamp precision (PostgreSQL uses
		// microseconds), not the unsaved in-memory nanosecond mutation value.
		defer func() {
			if result == nil && out != nil {
				out, result = nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: api.CameraDeviceRef_builder{Id: out.GetId()}.Build()}.Build())
			}
		}()

		v, err := nx.CameraDevice().Get(ctx, api.CameraDeviceGetRequest_builder{Ref: r.GetRef()}.Build())
		if err != nil {
			return err
		}
		if err := cameraVersion(r.GetExpectedVersion(), v.GetDateUpdated()); err != nil {
			return err
		}
		n, err := nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: v.GetNetwork().GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := New(nx, s.d).cameraLockNetwork(ctx, n.GetId()); err != nil {
			return err
		}
		n, err = nx.CameraNetwork().Get(ctx, api.CameraNetworkGetRequest_builder{Ref: api.CameraNetworkRef_builder{Id: n.GetId()}.Build()}.Build())
		if err != nil {
			return err
		}
		if err := cameraEndpoints(r.GetEndpoints(), n.GetCidrs()); err != nil {
			return err
		}
		identity := r.GetIdentity()
		if identity != nil {
			if len(identity.GetMacs()) > 16 {
				return invalid("identity", "too many network interfaces")
			}
			for _, mac := range identity.GetMacs() {
				if _, err := net.ParseMAC(mac); err != nil {
					return invalid("identity.macs", "invalid interface address")
				}
			}
			for _, text := range []string{identity.GetManufacturer(), identity.GetModel(), identity.GetHardware(), identity.GetFirmware(), identity.GetSerial()} {
				if len(text) > 256 || strings.ContainsAny(text, "\x00\r\n") {
					return invalid("identity", "invalid identity evidence")
				}
			}
			// Evidence records do not grant ownership or secret access.
		}
		out, err = nx.CameraDevice().Patch(ctx, api.CameraDevicePatchRequest_builder{Ref: r.GetRef(), Identity: identity, Endpoints: r.GetEndpoints(), ObservationRevision: z.Ptr(v.GetObservationRevision() + 1), DateUpdated: r.GetExpectedVersion()}.Build())
		return err
	})
	if status.Code(err) == codes.NotFound {
		return nil, err
	}
	if err != nil {
		return nil, cameraConflict(err)
	}
	return out, nil
}
