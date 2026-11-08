#!/usr/bin/env python3
"""Run a coordinator against an isolated DHCP camera/edge/control-plane topology."""

import argparse
import ctypes
import ipaddress
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

from camera_lan import Network


class TopologyInterrupted(RuntimeError):
    """Keep a requested runner shutdown distinct from an operation failure."""


def run(*argv, **kw):
    return subprocess.run(argv, check=True, capture_output=True, text=True, **kw)


class Topology:
    def __init__(self, artifacts):
        self.artifacts = artifacts
        self.namespaces = {role: f"shale-e2e-{os.getpid()}-{role}"
                           for role in ("edge", "camera", "cp")}
        self.created = []
        self.links = []
        self.processes = []

    def ns(self, role, *argv):
        return ["ip", "netns", "exec", self.namespaces[role], *argv]

    def app(self, role):
        # This prefix is executed by the root coordinator, never by a Go setns.
        return self.ns(role, "setpriv", "--reuid=65534", "--regid=65534", "--clear-groups",
                       "--bounding-set=-all", "--inh-caps=-all", "--ambient-caps=-all",
                       "--no-new-privs")

    def start(self, argv, log, **kw):
        with log.open("w") as output:
            process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True, **kw)
        self.processes.append(process)
        return process

    def setup(self):
        for namespace in self.namespaces.values():
            run("ip", "netns", "add", namespace)
            self.created.append(namespace)
            run("ip", "-n", namespace, "link", "set", "lo", "up")
        for index, role in enumerate(("camera", "cp")):
            left, right = f"sl{os.getpid() % 100000}{index}", f"sr{os.getpid() % 100000}{index}"
            run("ip", "link", "add", left, "type", "veth", "peer", "name", right)
            self.links.extend((left, right))
            run("ip", "link", "set", left, "netns", self.namespaces["edge"])
            run("ip", "link", "set", right, "netns", self.namespaces[role])
            interface = "eth0" if role == "camera" else "wlan0"
            run("ip", "-n", self.namespaces["edge"], "link", "set", left, "name", interface)
            run("ip", "-n", self.namespaces[role], "link", "set", right, "name", "eth0")
            run("ip", "-n", self.namespaces["edge"], "link", "set", interface, "up")
            run("ip", "-n", self.namespaces[role], "link", "set", "eth0", "up")
        run("ip", "-n", self.namespaces["edge"], "addr", "add", "192.168.250.1/24", "dev", "eth0")
        run("ip", "-n", self.namespaces["edge"], "addr", "add", "10.23.0.2/24", "dev", "wlan0")
        run("ip", "-n", self.namespaces["cp"], "addr", "add", "10.23.0.1/24", "dev", "eth0")
        run("ip", "-n", self.namespaces["edge"], "route", "add", "default", "via", "10.23.0.1", "dev", "wlan0")
        # WS-Discovery multicast must use the camera LAN, not the Wi-Fi default.
        run("ip", "-n", self.namespaces["edge"], "route", "add", "239.255.255.250/32", "dev", "eth0")
        run("ip", "-n", self.namespaces["camera"], "route", "add", "239.255.255.250/32", "dev", "eth0")
        # There is no CP-to-camera route or forwarding/NAT service.
        run(*self.ns("edge", "python3", "-c",
                     "from pathlib import Path; Path('/proc/sys/net/ipv4/ip_forward').write_text('0')"))
        leases = self.artifacts / "dhcp"
        leases.mkdir(mode=0o700)
        os.chown(leases, 65534, 65534)
        conf = self.artifacts / "dnsmasq.conf"
        generated = Network("eth0", "wlan0", "192.168.250.1/24",
                            "192.168.250.20", "192.168.250.99").files()["camera-dhcp.conf"][1]
        conf.write_text(generated.replace("/var/lib/shale-camera-dhcp/leases", str(leases / "leases")))
        caps = "+net_admin,+net_raw,+net_bind_service"
        self.daemon = self.start(self.ns("edge", "setpriv", "--reuid=65534", "--regid=65534",
                                        "--clear-groups", "--inh-caps=" + caps,
                                        "--ambient-caps=" + caps, "dnsmasq",
                                        "--keep-in-foreground", "--conf-file=" + str(conf)),
                                 self.artifacts / "dnsmasq.log")
        self.wait_for(lambda: "DHCP, IP range" in (self.artifacts / "dnsmasq.log").read_text(),
                      self.daemon, 5, "DHCP server startup")
        hook = self.artifacts / "dhcp-hook"
        hook.write_text("""#!/usr/bin/python3
import ipaddress, json, os, subprocess, sys
if sys.argv[1] in ('bound', 'renew'):
    lease = {k: os.environ.get(k, '') for k in ('ip', 'subnet', 'router', 'dns', 'ntpsrv')}
    lease['event'] = sys.argv[1]
    prefix = ipaddress.IPv4Network('0.0.0.0/' + lease['subnet']).prefixlen
    subprocess.run(['ip', '-4', 'addr', 'replace', lease['ip'] + '/' + str(prefix),
                    'dev', os.environ['interface']], check=True)
    with open(os.environ['SHALE_DHCP_LEASE'] + '.tmp', 'w') as output:
        json.dump(lease, output)
    os.replace(os.environ['SHALE_DHCP_LEASE'] + '.tmp', os.environ['SHALE_DHCP_LEASE'])
""")
        hook.chmod(0o755)
        lease_path = self.artifacts / "lease.json"
        self.client = self.start(self.ns("camera", "busybox", "udhcpc", "-f", "-n", "-i", "eth0",
                                        "-t", "4", "-T", "2", "-s", str(hook)),
                                 self.artifacts / "dhcp-client.log",
                                 env={**os.environ, "SHALE_DHCP_LEASE": str(lease_path)})
        self.wait_for(lease_path.exists, self.client, 15, "camera DHCP lease")
        lease = json.loads(lease_path.read_text())
        if lease["event"] != "bound" or any(lease[k] for k in ("router", "dns", "ntpsrv")):
            raise RuntimeError("unexpected DHCP options: " + repr(lease))
        address = ipaddress.IPv4Address(lease["ip"])
        if not ipaddress.IPv4Address("192.168.250.20") <= address <= ipaddress.IPv4Address("192.168.250.99"):
            raise RuntimeError("camera received an address outside the generated DHCP pool")
        server_log = (self.artifacts / "dnsmasq.log").read_text()
        if not all(event in server_log for event in ("DHCPDISCOVER", "DHCPOFFER", "DHCPREQUEST", "DHCPACK")):
            raise RuntimeError("missing DHCP DORA server evidence")
        if lease["ip"] not in (leases / "leases").read_text():
            raise RuntimeError("camera lease was not persisted")
        manifest = {
            "version": 1,
            "application_uid": 65534,
            "application_gid": 65534,
            "roles": {
                "edge": {"camera_interface": "eth0", "camera_address": "192.168.250.1",
                         "camera_cidr": "192.168.250.0/24", "uplink_interface": "wlan0",
                         "uplink_address": "10.23.0.2", "exec_prefix": self.app("edge")},
                "camera": {"interface": "eth0", "address": lease["ip"],
                           "exec_prefix": self.app("camera")},
                "cp": {"interface": "eth0", "address": "10.23.0.1", "exec_prefix": self.app("cp")},
            },
            "discovery": {"address": "239.255.255.250", "port": 3702},
            "dhcp": {"lease": lease, "client_log": str(self.artifacts / "dhcp-client.log"),
                     "server_log": str(self.artifacts / "dnsmasq.log"),
                     "lease_file": str(leases / "leases")},
        }
        self.manifest = self.artifacts / "topology.json"
        self.manifest.write_text(json.dumps(manifest, indent=2) + "\n")

    @staticmethod
    def wait_for(condition, process, seconds, description):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(description + " process exited")
            if condition():
                return
            time.sleep(0.02)
        raise RuntimeError("timed out waiting for " + description)

    def coordinate(self, command, timeout):
        env = {**os.environ, "SHALE_TOPOLOGY_FILE": str(self.manifest)}
        coordinator = subprocess.Popen(self.ns("cp", *command), env=env, start_new_session=True)
        self.processes.append(coordinator)
        deadline = time.monotonic() + timeout
        while coordinator.poll() is None:
            if self.daemon.poll() is not None or self.client.poll() is not None:
                raise RuntimeError("DHCP service/client exited while coordinator was running")
            if time.monotonic() >= deadline:
                raise RuntimeError("coordinator timed out")
            time.sleep(0.02)
        return coordinator.returncode

    def stop_processes(self):
        for process in reversed(self.processes):
            # A coordinator can exit before its process group does.
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        # Also catch role children which created their own sessions/groups.
        pids = set()
        for namespace in self.created:
            pids.update(int(pid) for pid in run("ip", "netns", "pids", namespace).stdout.split())
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and any(p.poll() is None for p in self.processes):
            time.sleep(0.02)
        for pid in pids:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        for process in self.processes:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        for process in self.processes:
            process.wait(timeout=3)
        # Reap adopted grandchildren (including detached role services).
        deadline = time.monotonic() + 3
        while True:
            try:
                child, _ = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                break
            if child == 0:
                if time.monotonic() >= deadline:
                    raise RuntimeError("topology descendants did not exit")
                time.sleep(0.02)
    def cleanup(self):
        errors = []
        try:
            self.stop_processes()
        except Exception as error:
            errors.append(str(error))
        # Attempt every owned resource even if stopping one process failed.
        for namespace in reversed(self.created):
            try:
                run("ip", "netns", "del", namespace)
            except subprocess.CalledProcessError as error:
                errors.append(str(error))
        for link in self.links:
            # Only the two newly created veth pairs can still be in this namespace.
            subprocess.run(["ip", "link", "del", link], capture_output=True)
        remaining = {row[0] for row in (line.split() for line in run("ip", "netns", "list").stdout.splitlines())}
        links = {link["ifname"] for link in json.loads(run("ip", "-j", "link").stdout)}
        if remaining.intersection(self.created) or links.intersection(self.links):
            errors.append("topology namespace/veth cleanup was incomplete")
        if errors:
            raise RuntimeError("; ".join(errors))


def require_isolation():
    if os.environ.get("SHALE_CAMERA_LAN_NETNS_TEST") != "1" or os.geteuid() != 0:
        raise RuntimeError("run explicitly as root in test.Dockerfile's isolated privileged container")
    if {link["ifname"] for link in json.loads(run("ip", "-j", "link").stdout)} != {"lo"}:
        raise RuntimeError("outer namespace must contain only loopback; use docker --network=none")


def main(factory=Topology):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifacts", type=Path, help="create and retain this fresh artifact directory")
    parser.add_argument("--timeout", type=float, default=120, help="coordinator timeout in seconds")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="-- coordinator command and arguments")
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("a coordinator command and a positive timeout are required")
    require_isolation()
    # Linux PR_SET_CHILD_SUBREAPER makes detached grandchildren ours to reap.
    if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "could not become topology child subreaper")
    temporary = None
    if args.artifacts:
        artifacts = args.artifacts.resolve()
        artifacts.mkdir(mode=0o755)
        artifacts.chmod(0o755)
    else:
        temporary = tempfile.TemporaryDirectory(prefix="shale-topology-")
        artifacts = Path(temporary.name)
        artifacts.chmod(0o755)
    topology = factory(artifacts)

    def interrupted(signum, _frame):
        raise TopologyInterrupted("topology interrupted by signal " + str(signum))

    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, interrupted)
    status = {"version": 1, "coordinator_exit_code": None, "cleanup_complete": False}
    try:
        topology.setup()
        status["coordinator_exit_code"] = topology.coordinate(command, args.timeout)
        return status["coordinator_exit_code"]
    except Exception as error:
        status["error"] = str(error)
        raise
    finally:
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, signal.SIG_IGN)
        try:
            topology.cleanup()
            status["cleanup_complete"] = True
        finally:
            (artifacts / "result.json").write_text(json.dumps(status, indent=2) + "\n")
            if temporary:
                temporary.cleanup()


if __name__ == "__main__":
    sys.exit(main())
