import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
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

    def test_ntp_serves_exactly_the_camera_subnet_from_the_pi(self):
        for address, subnet, pool in (("192.168.250.1/24", "192.168.250.0/24", ("192.168.250.20", "192.168.250.99")),
                                      ("10.40.8.9/29", "10.40.8.8/29", ("10.40.8.10", "10.40.8.14")),
                                      ("172.20.0.1/16", "172.20.0.0/16", ("172.20.1.1", "172.20.1.9"))):
            with self.subTest(address=address):
                files = self.network(address=address, first=pool[0], last=pool[1]).files()
                chrony = files[module.CHRONY_FILE][1]
                # Exactly these: nothing wider, no `local`, no bindaddress/binddevice.
                directives = [line for line in chrony.splitlines() if line and not line.startswith("#")]
                self.assertEqual(directives, [f"allow {subnet}", "ratelimit interval 3 burst 8 leak 2"])
                self.assertEqual(files[module.CHRONY_FILE][0], 0o644)
                dhcp = files["camera-dhcp.conf"][1]
                self.assertIn(f"dhcp-option=option:ntp-server,{address.split('/')[0]}\n", dhcp)
                self.assertEqual(sum(line.startswith("dhcp-option=") for line in dhcp.splitlines()), 3)
                # Router and DNS stay suppressed beside it.
                self.assertIn("dhcp-option=option:router\n", dhcp)
                self.assertIn("dhcp-option=option:dns-server\n", dhcp)
        # chrony's confdir reads only *.conf.
        self.assertRegex(module.CHRONY_DROPIN, r"^/etc/chrony/conf\.d/[^/]+\.conf$")
        self.assertIn(module.CHRONY_DROPIN, self.network().files()[module.CHRONY_FILE][1])

    def test_no_ntp_renders_neither_the_dropin_nor_the_option(self):
        with_ntp = self.network().files()
        files = self.network(ntp=False).files()
        self.assertEqual(set(files), set(with_ntp) - {module.CHRONY_FILE})
        dhcp = files["camera-dhcp.conf"][1]
        self.assertNotIn("ntp-server", dhcp)
        self.assertNotIn("chrony", dhcp)
        self.assertIn("dhcp-option=option:router\n", dhcp)
        self.assertIn("dhcp-option=option:dns-server\n", dhcp)
        # Apart from comments, the option line is the whole difference.
        self.assertEqual([line for line in dhcp.splitlines() if not line.startswith("#")],
                         [line for line in with_ntp["camera-dhcp.conf"][1].splitlines()
                          if not line.startswith("#") and "ntp-server" not in line])

    def test_invalid_address_renders_no_ntp_scope(self):
        for address in ("192.168.250.1", "192.168.250.1/33", "192.168.250.1/8", "192.168.250.0/24",
                        "192.168.250.255/24", "8.8.8.1/24", "fd00::1/64", "192.168.250.1/24\nallow all",
                        "192.168.250.1/24 0.0.0.0/0", "camera", ""):
            with self.subTest(address=address), self.assertRaises(Invalid):
                self.network(address=address).files()

    def test_cli_prints_ntp_install_steps_and_no_ntp_omits_them(self):
        argv = ["--offline", "--interface", "eth0", "--uplink", "wlan0", "--address", "192.168.250.1/24",
                "--first", "192.168.250.20", "--last", "192.168.250.99"]
        with tempfile.TemporaryDirectory() as temp:
            for extra, expected in (([], True), (["--ntp"], True), (["--no-ntp"], False)):
                output = Path(temp) / f"out{len(extra)}{expected}"
                with self.subTest(extra=extra):
                    done = subprocess.run([sys.executable, str(Path(__file__).with_name("camera_lan.py")),
                                           *argv, *extra, "--output", str(output)],
                                          capture_output=True, text=True, check=True)
                    self.assertEqual((output / module.CHRONY_FILE).exists(), expected)
                    self.assertEqual("ntp-server" in (output / "camera-dhcp.conf").read_text(), expected)
                    self.assertEqual(module.CHRONY_DROPIN in done.stdout, expected)
                    self.assertEqual("systemctl restart chrony" in done.stdout, expected)
                    self.assertEqual(len(list(output.iterdir())), 8 if expected else 7)

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
