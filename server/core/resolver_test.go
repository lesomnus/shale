package core

import (
	"net"
	"testing"

	"github.com/lesomnus/shale/api"
)

// A caller on the CP's own host (loopback) gets loopback for the node that
// shares the host, and a routable address for every other node: the lab's
// CLI on the CP host was handed 127.0.0.1 for objects on other nodes
// (§34.10).
func TestPickAddressLoopbackCaller(t *testing.T) {
	saved := hostIPs
	hostIPs = func() []net.IP { return []net.IP{net.ParseIP("10.1.2.73"), net.ParseIP("127.0.0.1")} }
	defer func() { hostIPs = saved }()

	ifs := func(addrs ...string) []*api.HostInterface {
		return []*api.HostInterface{api.HostInterface_builder{Name: "eth0", Addresses: addrs}.Build()}
	}
	local := ifs("10.1.2.73/24", "127.0.0.1/8")
	remote := ifs("10.1.2.75/24", "127.0.0.1/8")

	if got := pickAddress(local, "127.0.0.1", nil); got != "127.0.0.1" {
		t.Fatalf("local node for a loopback caller: %s", got)
	}
	if got := pickAddress(remote, "127.0.0.1", nil); got != "10.1.2.75" {
		t.Fatalf("remote node for a loopback caller: %s", got)
	}
	if got := pickAddress(remote, "10.1.2.63", nil); got != "10.1.2.75" {
		t.Fatalf("remote node for a LAN caller: %s", got)
	}
}
