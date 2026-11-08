#!/usr/bin/env python3
"""Render an opt-in camera LAN recipe. Never changes interfaces or services."""

import argparse
import ipaddress
import json
from pathlib import Path
import re
import subprocess
import sys
from dataclasses import dataclass


class Invalid(ValueError):
    pass


@dataclass(frozen=True)
class Network:
    interface: str
    uplink: str
    address: str
    first: str
    last: str
    lease_seconds: int = 86400

    def validate(self):
        for name in (self.interface, self.uplink):
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,14}", name) or name == "lo":
                raise Invalid("use explicit non-loopback interface names (no patterns)")
        if self.interface == self.uplink:
            raise Invalid("camera interface and uplink must differ")
        try:
            address = ipaddress.IPv4Interface(self.address)
            first, last = map(ipaddress.IPv4Address, (self.first, self.last))
        except ValueError as exc:
            raise Invalid("address and pool must be IPv4") from exc
        net = address.network
        private = tuple(map(ipaddress.IPv4Network, ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")))
        if not any(net.subnet_of(p) for p in private) or not 16 <= net.prefixlen <= 30:
            raise Invalid("choose an RFC1918 camera subnet with prefix /16 through /30")
        if not net.network_address < address.ip < net.broadcast_address:
            raise Invalid("camera interface needs a usable host address")
        if not net.network_address < first <= last < net.broadcast_address:
            raise Invalid("pool must be ordered usable addresses in the camera subnet")
        if first <= address.ip <= last:
            raise Invalid("DHCP pool must exclude the Pi address")
        if not 120 <= self.lease_seconds <= 604800:
            raise Invalid("lease duration must be 120 through 604800 seconds")
        return address, first, last

    def check_host(self, addresses, routes, wireless):
        """Validate a read-only `ip -j` inventory before manual installation."""
        address, _, _ = self.validate()
        links = {v["ifname"]: v for v in addresses}
        if self.interface not in links or self.uplink not in links:
            raise Invalid("both configured interfaces must exist")
        if self.interface in wireless or self.uplink not in wireless:
            raise Invalid("select a wired camera interface and a wireless uplink")
        camera = links[self.interface]
        if camera.get("master") or camera.get("linkinfo", {}).get("info_kind") in ("bridge", "bond"):
            raise Invalid("camera interface must not be a bridge/bond member")
        for v in camera.get("addr_info", []):
            if v.get("family") == "inet" and v.get("local") != str(address.ip):
                raise Invalid("camera interface already has another IPv4 address")
        if not any(v.get("dst") == "default" and v.get("dev") == self.uplink for v in routes):
            raise Invalid("wireless uplink must already have an IPv4 default route")
        for route in routes:
            dev, dst = route.get("dev"), route.get("dst")
            if dev == self.interface and dst == "default":
                raise Invalid("camera interface must not carry a default route")
            if not dst or dst == "default":
                continue
            try:
                other = ipaddress.IPv4Network(dst, strict=False)
            except ValueError:
                continue
            if other.overlaps(address.network) and dev != self.interface:
                raise Invalid("camera subnet overlaps an existing route on another interface")
        for name, link in links.items():
            if name == self.interface:
                continue
            for v in link.get("addr_info", []):
                if v.get("family") == "inet":
                    other = ipaddress.IPv4Interface(f"{v['local']}/{v['prefixlen']}")
                    if other.network.overlaps(address.network):
                        raise Invalid("camera subnet overlaps another interface address")

    def files(self):
        address, first, last = self.validate()
        profile = f"""[connection]
id=shale-camera-lan
type=ethernet
interface-name={self.interface}
autoconnect=true
autoconnect-priority=100

[ethernet]

[ipv4]
method=manual
address1={address}
never-default=true
ignore-auto-dns=true

[ipv6]
method=disabled
"""
        dhcp = f"""# Only the dedicated camera LAN. No DNS, router, NTP or forwarding service.
port=0
pid-file=
interface={self.interface}
except-interface=lo
except-interface={self.uplink}
no-dhcp-interface={self.uplink}
bind-dynamic
dhcp-authoritative
dhcp-range={first},{last},{address.netmask},{self.lease_seconds}s
dhcp-lease-max={int(last) - int(first) + 1}
dhcp-leasefile=/var/lib/shale-camera-dhcp/leases
dhcp-option=option:router
dhcp-option=option:dns-server
log-facility=-
"""
        service = """[Unit]
Description=DHCP for the isolated Shale camera LAN
After=NetworkManager.service network-online.target
Wants=network-online.target

[Service]
Type=simple
DynamicUser=yes
User=shale-camera-dhcp
StateDirectory=shale-camera-dhcp
StateDirectoryMode=0700
ExecStart=/usr/sbin/dnsmasq --keep-in-foreground --conf-file=/etc/shale/camera-dhcp.conf
Restart=on-failure
RestartSec=3
AmbientCapabilities=CAP_NET_BIND_SERVICE CAP_NET_RAW CAP_NET_ADMIN
CapabilityBoundingSet=CAP_NET_BIND_SERVICE CAP_NET_RAW CAP_NET_ADMIN
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ProtectKernelTunables=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6 AF_NETLINK AF_PACKET

[Install]
WantedBy=multi-user.target
"""
        manifest = json.dumps({
            "interface": self.interface, "uplink": self.uplink,
            "address": str(address), "cidr": str(address.network),
            "dhcp_first": str(first), "dhcp_last": str(last),
            "discovery_interface": self.interface,
            "multicast": True,
        }, indent=2) + "\n"
        return {
            "shale-camera-lan.nmconnection": (0o600, profile),
            "camera-dhcp.conf": (0o644, dhcp),
            "shale-camera-dhcp.service": (0o644, service),
            "camera-network.json": (0o644, manifest),
        }


def inventory():
    def ip(*args):
        return json.loads(subprocess.check_output(["ip", "-j", *args], text=True))
    addresses = ip("-d", "address", "show")
    routes = ip("-4", "route", "show", "table", "all") + ip("-6", "route", "show", "table", "all")
    wireless = {v["ifname"] for v in addresses if Path("/sys/class/net", v["ifname"], "wireless").exists()}
    return addresses, routes, wireless


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interface", required=True)
    parser.add_argument("--uplink", required=True)
    parser.add_argument("--address", required=True, help="Pi camera-LAN address/prefix")
    parser.add_argument("--first", required=True, help="first DHCP pool address")
    parser.add_argument("--last", required=True, help="last DHCP pool address")
    parser.add_argument("--lease-seconds", type=int, default=86400)
    parser.add_argument("--offline", action="store_true", help="render without host interface/route checks; validate again on the Pi")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--output", type=Path, help="new output directory; existing directories are refused")
    args = parser.parse_args()
    network = Network(args.interface, args.uplink, args.address, args.first, args.last, args.lease_seconds)
    try:
        files = network.files()
        if not args.offline:
            network.check_host(*inventory())
        if args.validate_only:
            if args.offline:
                raise Invalid("--validate-only requires host checks")
            print("Camera LAN parameters and current host routes validated; no changes made.")
            return
        if args.output is None:
            raise Invalid("--output is required when rendering")
        args.output.mkdir(mode=0o700, parents=False, exist_ok=False)
        for name, (mode, content) in files.items():
            path = args.output / name
            with path.open("x") as f:
                path.chmod(mode)
                f.write(content)
        print(f"Rendered {len(files)} files in {args.output}; review before manual installation.")
        if args.offline:
            print("Host checks skipped: run --validate-only on the Pi before installing.")
    except (Invalid, OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        parser.exit(2, f"camera LAN: {exc}\n")


if __name__ == "__main__":
    main()
