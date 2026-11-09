import importlib.util
from pathlib import Path
import sys
import unittest

spec = importlib.util.spec_from_file_location("camera_lan", Path(__file__).with_name("camera_lan.py"))
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
Network, Invalid = module.Network, module.Invalid


class CameraLANTest(unittest.TestCase):
    def network(self, **values):
        defaults = dict(interface="eth0", uplink="wlan0", address="192.168.250.1/24", first="192.168.250.20", last="192.168.250.99")
        return Network(**(defaults | values))

    def host(self):
        return ([{"ifname": "eth0", "addr_info": []}, {"ifname": "wlan0", "addr_info": [{"family": "inet", "local": "10.1.12.75", "prefixlen": 24}]}],
                [{"dst": "default", "dev": "wlan0"}, {"dst": "10.1.12.0/24", "dev": "wlan0"}], {"wlan0"})

    def test_bad_pool_or_interface_cannot_render_dhcp(self):
        for values in ({"interface": "wlan0"}, {"interface": "eth*"}, {"interface": "lo"}, {"interface": "eth0\nport=53"},
                       {"first": "192.168.250.0"}, {"last": "192.168.250.255"}, {"first": "192.168.250.100"},
                       {"first": "192.168.250.1"}, {"last": "192.168.251.99"}, {"address": "8.8.8.1/24"},
                       {"address": "192.168.250.0/24"}, {"lease_seconds": 30}):
            with self.subTest(values=values), self.assertRaises(Invalid):
                self.network(**values).files()

    def test_real_host_must_have_distinct_wifi_default_and_no_overlap(self):
        n = self.network()
        n.check_host(*self.host())
        for mutation in ("camera-default", "overlap-route", "overlap-address", "bridge-member", "existing-camera-ip", "ipv6-only-uplink", "not-wifi"):
            addresses, routes, wireless = self.host()
            if mutation == "camera-default":
                routes.append({"dst": "default", "dev": "eth0"})
            elif mutation == "overlap-route":
                routes.append({"dst": "192.168.0.0/16", "dev": "tun0"})
            elif mutation == "overlap-address":
                addresses[1]["addr_info"][0] = {"family": "inet", "local": "192.168.250.8", "prefixlen": 24}
            elif mutation == "bridge-member":
                addresses[0]["master"] = "br0"
            elif mutation == "existing-camera-ip":
                addresses[0]["addr_info"] = [{"family": "inet", "local": "10.1.2.75", "prefixlen": 24}]
            elif mutation == "ipv6-only-uplink":
                addresses[1]["addr_info"] = []
                routes = [{"dst": "default", "dev": "wlan0", "gateway": "fe80::1", "family": "inet6"}]
            else:
                wireless.clear()
            with self.subTest(mutation=mutation), self.assertRaises(Invalid):
                n.check_host(addresses, routes, wireless)

    def test_render_limits_service_to_camera_lan_and_keeps_wifi_route(self):
        files = self.network().files()
        profile = files["shale-camera-lan.nmconnection"][1]
        dhcp = files["camera-dhcp.conf"][1]
        self.assertIn("never-default=true", profile)
        self.assertIn("method=manual", profile)
        self.assertNotIn("method=shared", profile)
        self.assertIn("interface=eth0\n", dhcp)
        self.assertIn("except-interface=wlan0\n", dhcp)
        self.assertIn("no-dhcp-interface=wlan0\n", dhcp)
        self.assertIn("port=0\n", dhcp)
        self.assertIn("dhcp-option=option:router\n", dhcp)
        self.assertIn("dhcp-option=option:dns-server\n", dhcp)
        service = files["shale-camera-dhcp.service"][1]
        self.assertNotIn("shale@", service)
        self.assertIn("StateDirectory=shale-camera-dhcp", service)
        self.assertNotIn("NET_ADMIN", profile)

    def test_lease_readers_have_no_dhcp_write_or_camera_credential_group(self):
        files = self.network().files()
        unit = files["shale-camera-dhcp.service"][1]
        for line in ("DynamicUser=no", "User=shale-camera-dhcp", "Group=shale-camera-discovery",
                     "StateDirectoryMode=0750", "UMask=0027",
                     "ExecStartPre=/usr/bin/chmod 0640 /var/lib/shale-camera-dhcp/leases"):
            self.assertIn(line + "\n", unit)
        self.assertNotIn("DynamicUser=yes", unit)
        self.assertNotIn("Group=shale\n", unit)
        users = files["shale-camera-dhcp.sysusers.conf"][1]
        self.assertIn("g shale-camera-discovery -\n", users)
        self.assertIn('u shale-camera-dhcp - "Shale camera DHCP" /nonexistent /usr/sbin/nologin\n', users)
        self.assertIn("m shale-camera-dhcp shale-camera-discovery\n", users)
        self.assertNotIn("m shale-camera-dhcp shale\n", users)
        for name in ("shale-edge-dhcp.conf", "shale-camera-worker-dhcp.conf"):
            dropin = files[name][1]
            self.assertEqual(dropin.splitlines()[1:], ["[Service]", "SupplementaryGroups=shale-camera-discovery"])
            self.assertNotIn("User=", dropin)
            self.assertNotIn("Capabilities=", dropin)


if __name__ == "__main__":
    unittest.main()
