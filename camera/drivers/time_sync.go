package drivers

import (
	"context"
	"net"
	"net/netip"

	"github.com/lesomnus/shale/camera"
)

var _ camera.ClockObserver = (*Driver)(nil)

// timeServer resolves a time_sync NTP setting for the camera at h
// (docs/camera-worker.md, "Camera time sync"). camera.TimeSyncEdge is this
// host's own address toward the camera: the local address of a UDP socket
// connected to the camera's endpoint host, port 123, which the kernel picks
// by route without a packet being sent. An IPv4 literal is itself. Either must
// be an IPv4 address inside the camera network's prefixes, or it is
// camera.ErrTimeServerRefused; no name is resolved.
func (d *Driver) timeServer(ctx context.Context, h camera.DeviceHint, ntp string) (netip.Addr, error) {
	endpoint, err := d.endpoint(ctx, h)
	if err != nil {
		return netip.Addr{}, err
	}
	server, explicit := (camera.TimeSync{NTP: ntp}).Server()
	switch {
	case explicit:
	case ntp == camera.TimeSyncEdge:
		server, err = localAddressToward(ctx, endpoint.Hostname())
		if err != nil {
			return netip.Addr{}, err
		}
	default:
		return netip.Addr{}, camera.ErrInvalid
	}
	if !camera.TimeServerAllowed(server, d.prefixes) {
		return netip.Addr{}, camera.ErrTimeServerRefused
	}
	return server, nil
}

// localAddressToward is the source address this host reaches host from. A
// camera this host has no IPv4 route to has none.
func localAddressToward(ctx context.Context, host string) (netip.Addr, error) {
	target, err := netip.ParseAddr(host)
	if err != nil || !target.Unmap().Is4() {
		return netip.Addr{}, camera.ErrTimeServerRefused
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "udp4", netip.AddrPortFrom(target.Unmap(), 123).String())
	if err != nil {
		if ctx.Err() != nil {
			return netip.Addr{}, ctx.Err()
		}
		return netip.Addr{}, camera.ErrTimeServerRefused
	}
	defer conn.Close()
	local, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return netip.Addr{}, camera.ErrTimeServerRefused
	}
	return local.AddrPort().Addr().Unmap(), nil
}
