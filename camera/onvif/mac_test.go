package onvif

import (
	"context"
	"testing"
)

func TestAuthenticatedMACInspection(t *testing.T) {
	f := newFixture(t, true)
	f.interfaces = `<t:NetworkInterfaces token="eth0"><tt:Enabled>true</tt:Enabled><tt:Info><tt:HwAddress>AA-BB-CC-DD-EE-01</tt:HwAddress></tt:Info><tt:IPv4><tt:Config><tt:FromDHCP><tt:Address>127.0.0.1</tt:Address></tt:FromDHCP></tt:Config></tt:IPv4></t:NetworkInterfaces>`
	i, err := (&Client{}).Inspect(context.Background(), f.device(), fixtureLease())
	if err != nil {
		t.Fatal(err)
	}
	if i.Device.MAC != "aa:bb:cc:dd:ee:01" {
		t.Fatalf("MAC = %q", i.Device.MAC)
	}
}

func TestUnverifiedHintMACIsNotPromotedByAuthenticatedDeviceInfo(t *testing.T) {
	f := newFixture(t, true)
	h := f.device()
	h.MAC = "aa:bb:cc:dd:ee:99"
	i, err := (&Client{}).Inspect(context.Background(), h, fixtureLease())
	if err != nil {
		t.Fatal(err)
	}
	if i.Device.MAC != "" {
		t.Fatalf("unsupported interface lookup promoted a caller hint: %q", i.Device.MAC)
	}
}

func TestInterfaceMACSelection(t *testing.T) {
	iface := func(enabled, mac, ip string) string {
		return `<NetworkInterfaces><Enabled>` + enabled + `</Enabled><Info><HwAddress>` + mac + `</HwAddress></Info><IPv4><Config><Manual><Address>` + ip + `</Address></Manual></Config></IPv4></NetworkInterfaces>`
	}
	for _, tc := range []struct{ name, body, host, want string }{
		{"matching interface", iface("true", "00:11:22:33:44:55", "192.0.2.10") + iface("true", "00:11:22:33:44:66", "192.0.2.11"), "192.0.2.11", "00:11:22:33:44:66"},
		{"ambiguous DNS", iface("true", "00:11:22:33:44:55", "192.0.2.10") + iface("true", "00:11:22:33:44:66", "192.0.2.11"), "camera.example", ""},
		{"disabled excluded", iface("false", "00:11:22:33:44:55", "192.0.2.10") + iface("true", "00:11:22:33:44:66", "192.0.2.11"), "camera.example", "00:11:22:33:44:66"},
		{"invalid ignored", iface("true", "invalid", "192.0.2.10"), "192.0.2.10", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := parseXML([]byte("<Response>" + tc.body + "</Response>"))
			if err != nil {
				t.Fatal(err)
			}
			if got := interfaceMAC(n, tc.host); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}
