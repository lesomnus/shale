#!/usr/bin/env python3
"""Explicit isolated DHCP protocol test. Run in test.Dockerfile's container."""

import ipaddress
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

from camera_lan import Network


def run(*argv, **kw):
    return subprocess.run(argv, check=True, capture_output=True, text=True, **kw)


def main():
    if os.environ.get("SHALE_CAMERA_LAN_NETNS_TEST") != "1" or os.geteuid() != 0:
        raise SystemExit("run explicitly inside the isolated privileged test container")
    namespaces = {role: f"shale-test-{os.getpid()}-{role}" for role in ("server", "camera", "uplink")}
    processes = []
    created = []
    try:
        for ns in namespaces.values():
            run("ip", "netns", "add", ns)
            created.append(ns)
            run("ip", "-n", ns, "link", "set", "lo", "up")
        for index, role in enumerate(("camera", "uplink")):
            left, right = f"sl{os.getpid() % 100000}{index}", f"sr{os.getpid() % 100000}{index}"
            run("ip", "link", "add", left, "type", "veth", "peer", "name", right)
            run("ip", "link", "set", left, "netns", namespaces["server"])
            run("ip", "link", "set", right, "netns", namespaces[role])
            interface = "eth0" if role == "camera" else "wlan0"
            run("ip", "-n", namespaces["server"], "link", "set", left, "name", interface)
            run("ip", "-n", namespaces[role], "link", "set", right, "name", "eth0")
            run("ip", "-n", namespaces["server"], "link", "set", interface, "up")
            run("ip", "-n", namespaces[role], "link", "set", "eth0", "up")
        run("ip", "-n", namespaces["server"], "addr", "add", "192.168.250.1/24", "dev", "eth0")
        run("ip", "-n", namespaces["server"], "addr", "add", "10.23.0.2/24", "dev", "wlan0")
        run("ip", "-n", namespaces["uplink"], "addr", "add", "10.23.0.1/24", "dev", "eth0")
        run("ip", "-n", namespaces["server"], "route", "add", "default", "via", "10.23.0.1", "dev", "wlan0")

        def ns_run(role, *argv, **kw):
            return run("ip", "netns", "exec", namespaces[role], *argv, **kw)

        with tempfile.TemporaryDirectory(prefix="shale-camera-dhcp-") as temp:
            base = Path(temp)
            base.chmod(0o755)
            leases = base / "state"
            leases.mkdir(mode=0o700)
            os.chown(leases, 65534, 65534)
            conf = base / "dnsmasq.conf"
            generated = Network("eth0", "wlan0", "192.168.250.1/24", "192.168.250.20", "192.168.250.99").files()["camera-dhcp.conf"][1]
            conf.write_text(generated.replace("/var/lib/shale-camera-dhcp/leases", str(leases / "leases")))
            hook = base / "client-hook"
            hook.write_text("""#!/usr/bin/python3
import ipaddress, json, os, subprocess, sys
if sys.argv[1] in ('bound', 'renew'):
    values = {key: os.environ.get(key, '') for key in ('ip', 'subnet', 'router', 'dns', 'ntpsrv')}
    with open(os.environ['RESULT'], 'w') as f:
        json.dump(values, f)
    prefix = ipaddress.IPv4Network('0.0.0.0/' + values['subnet']).prefixlen
    subprocess.run(['ip', '-4', 'addr', 'replace', values['ip'] + '/' + str(prefix), 'dev', os.environ['interface']], check=True)
""")
            hook.chmod(0o755)
            log_path = base / "dnsmasq.log"

            def start_dhcp():
                log = log_path.open("a")
                # Match the unit's non-root identity and three ambient capabilities.
                caps = "+net_admin,+net_raw,+net_bind_service"
                p = subprocess.Popen(["ip", "netns", "exec", namespaces["server"], "setpriv",
                                      "--reuid=65534", "--regid=65534", "--clear-groups",
                                      "--inh-caps=" + caps, "--ambient-caps=" + caps,
                                      "dnsmasq", "--keep-in-foreground", "--conf-file=" + str(conf)],
                                     stdout=log, stderr=subprocess.STDOUT)
                log.close()
                processes.append(p)
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if p.poll() is not None:
                        raise AssertionError(log_path.read_text())
                    if "DHCP, IP range" in log_path.read_text():
                        return p
                    time.sleep(0.02)
                raise AssertionError("DHCP daemon did not become ready")

            def client(role, filename, expect=True):
                result = base / filename
                env = {**os.environ, "RESULT": str(result)}
                # Leave room for dnsmasq's initial ICMP duplicate-address check.
                attempts, interval = ("4", "2") if expect else ("2", "1")
                argv = ["ip", "netns", "exec", namespaces[role], "busybox", "udhcpc",
                        "-f", "-n", "-q", "-i", "eth0", "-t", attempts, "-T", interval, "-s", str(hook)]
                p = subprocess.run(argv, capture_output=True, text=True, env=env, timeout=15)
                if not expect:
                    assert p.returncode != 0 and not result.exists(), "DHCP escaped onto the uplink"
                    return
                assert p.returncode == 0, p.stdout + p.stderr + log_path.read_text()
                values = json.loads(result.read_text())
                assert ipaddress.IPv4Address(values["ip"]) in ipaddress.IPv4Network("192.168.250.0/24")
                assert not values["router"] and not values["dns"] and not values["ntpsrv"], values
                return values["ip"]

            daemon = start_dhcp()
            assigned = client("camera", "first.json")
            lease_file = leases / "leases"
            assert assigned in lease_file.read_text(), "lease was not persisted"
            client("uplink", "forbidden.json", expect=False)
            # A Wi-Fi flap leaves LAN DHCP and the local camera path alive.
            run("ip", "-n", namespaces["server"], "link", "set", "wlan0", "down")
            assert client("camera", "wifi-down.json") == assigned
            run("ip", "-n", namespaces["server"], "link", "set", "wlan0", "up")
            # Stand in for NetworkManager restoring the existing Wi-Fi route.
            run("ip", "-n", namespaces["server"], "route", "replace", "default", "via", "10.23.0.1", "dev", "wlan0")
            assert "default via 10.23.0.1 dev wlan0" in ns_run("server", "ip", "-4", "route").stdout
            daemon.terminate()
            daemon.wait(timeout=5)
            assert assigned in lease_file.read_text()
            # Startup must read the old lease; use a new log to wait for this startup.
            log_path.write_text("")
            daemon = start_dhcp()
            assert client("camera", "restart.json") == assigned
            client("uplink", "still-forbidden.json", expect=False)
            print("DHCP DORA/renewal, lease restart, Wi-Fi flap and uplink isolation passed")
    finally:
        for p in reversed(processes):
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait()
        for ns in reversed(created):
            subprocess.run(["ip", "netns", "del", ns], capture_output=True)


if __name__ == "__main__":
    main()
