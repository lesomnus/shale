package onvif

import (
	"net"
	"strings"
)

func interfaceMAC(response *node, host string) string {
	var all, matching []string
	for _, iface := range response.all("NetworkInterfaces") {
		if iface.text("Enabled") != "true" && iface.text("Enabled") != "1" {
			continue
		}
		mac, err := net.ParseMAC(iface.child("Info").text("HwAddress"))
		if err != nil || len(mac) != 6 {
			continue
		}
		value := mac.String()
		all = appendUnique(all, value)
		ip := net.ParseIP(host)
		if ip == nil {
			continue
		}
		if hasInterfaceIP(iface, ip) {
			matching = appendUnique(matching, value)
		}
	}
	if len(matching) == 1 {
		return matching[0]
	}
	if len(all) == 1 {
		return all[0]
	}
	return ""
}

func hasInterfaceIP(n *node, ip net.IP) bool {
	if n == nil {
		return false
	}
	if n.Name.Local == "Address" {
		if other := net.ParseIP(strings.TrimSpace(n.Text)); other != nil && ip.Equal(other) {
			return true
		}
	}
	for _, c := range n.Children {
		if hasInterfaceIP(c, ip) {
			return true
		}
	}
	return false
}

func appendUnique(values []string, value string) []string {
	for _, prior := range values {
		if prior == value {
			return values
		}
	}
	return append(values, value)
}
