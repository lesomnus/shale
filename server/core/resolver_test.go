package core

import (
	"net"
	"testing"

	"github.com/lesomnus/payday/pdid"

	"github.com/lesomnus/shale/api"
	"github.com/lesomnus/shale/internal/placement"
)

// A caller on the CP's own host (loopback) gets loopback for the node that
// shares the host, and a routable address for every other node: the lab's
// CLI on the CP host was handed 127.0.0.1 for laminae on other nodes
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

// The forecast never leaves a set with nowhere to write: when it is the
// only reason every sink is out, those sinks come back (§11.1).
func TestReadmitForecast(t *testing.T) {
	a, b, c := pdid.New(14), pdid.New(14), pdid.New(14)
	sinks := []placement.Sink{{Id: a}, {Id: b}, {Id: c}}
	why := map[pdid.Id]string{a: forecastWhy, b: forecastWhy, c: "its node is down"}
	if !readmitForecast(sinks, why) {
		t.Fatal("nothing readmitted")
	}
	if !sinks[0].Eligible || !sinks[1].Eligible || sinks[2].Eligible {
		t.Fatalf("eligible: %v %v %v", sinks[0].Eligible, sinks[1].Eligible, sinks[2].Eligible)
	}
	if _, still := why[a]; still {
		t.Fatal("the reason stayed")
	}
	// One eligible sink: the forecast's exclusions stand.
	sinks = []placement.Sink{{Id: a, Eligible: true}, {Id: b}}
	why = map[pdid.Id]string{b: forecastWhy}
	if readmitForecast(sinks, why) || sinks[1].Eligible {
		t.Fatal("readmitted beside an eligible sink")
	}
}
