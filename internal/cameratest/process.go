package cameratest

import (
	"encoding/xml"
	"fmt"
	"net"
	"sync"

	"golang.org/x/net/ipv4"
)

// RequireCredentials prepares a known synthetic credential for both protocols.
// It does not decrypt the vendor activation payload or implement account reset.
func (c *Camera) RequireCredentials(username, password string) {
	c.ONVIF.RequireCredentials(username, password)
	c.Media.mu.Lock()
	c.Media.options.Username, c.Media.options.Password = username, password
	c.Media.mu.Unlock()
	c.Media.Disconnect()
}

// Active reports synthetic activation state without exposing its credential.
func (c *ONVIFCamera) Active() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active
}

// Close terminates both protocol listeners and all RTSP sessions.
func (c *Camera) Close() {
	c.ONVIF.close()
	c.Media.Close()
}

// ListenMulticastProbe joins WS-Discovery only on the explicitly assigned IPv4
// interface. It changes no interfaces or routes. Close must end this lifetime
// before rebinding to another address/interface or stopping the camera process.
func (c *ONVIFCamera) ListenMulticastProbe(interfaceName, address string) (func(), error) {
	ip := net.ParseIP(address).To4()
	iface, err := net.InterfaceByName(interfaceName)
	if err != nil || ip == nil {
		return nil, fmt.Errorf("invalid synthetic discovery interface")
	}
	addresses, err := iface.Addrs()
	if err != nil {
		return nil, fmt.Errorf("inspect synthetic discovery interface")
	}
	assigned := false
	for _, address := range addresses {
		local, _, _ := net.ParseCIDR(address.String())
		assigned = assigned || local.Equal(ip)
	}
	if !assigned || iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagMulticast == 0 {
		return nil, fmt.Errorf("synthetic discovery address must be assigned to an active multicast interface")
	}
	group := &net.UDPAddr{IP: net.IPv4(239, 255, 255, 250), Port: 3702}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: group.Port})
	if err != nil {
		return nil, fmt.Errorf("bind synthetic multicast discovery")
	}
	packet := ipv4.NewPacketConn(conn)
	if err = packet.JoinGroup(iface, group); err != nil {
		conn.Close()
		return nil, fmt.Errorf("join synthetic discovery group")
	}
	if err = packet.SetControlMessage(ipv4.FlagInterface|ipv4.FlagDst, true); err != nil {
		conn.Close()
		return nil, fmt.Errorf("scope synthetic multicast discovery")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buffer := make([]byte, 64<<10)
		for {
			n, control, from, err := packet.ReadFrom(buffer)
			if err != nil {
				return
			}
			if control == nil || control.IfIndex != iface.Index || !control.Dst.Equal(group.IP) {
				continue
			}
			var probe struct {
				Header struct {
					MessageID string `xml:"MessageID"`
				} `xml:"Header"`
				Body struct {
					Probe struct{ XMLName xml.Name } `xml:"Probe"`
				} `xml:"Body"`
			}
			if xml.Unmarshal(buffer[:n], &probe) != nil || probe.Body.Probe.XMLName.Local != "Probe" || len(probe.Header.MessageID) > 2048 {
				continue
			}
			_, _ = packet.WriteTo(c.ProbeResponse(probe.Header.MessageID), &ipv4.ControlMessage{Src: ip, IfIndex: iface.Index}, from)
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { conn.Close(); wg.Wait() }) }, nil
}
