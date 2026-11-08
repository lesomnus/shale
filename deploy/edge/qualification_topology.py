#!/usr/bin/env python3
"""Opt-in two-camera DHCP topology and private, bounded coordinator controls.

Only run through topology.main's explicit disposable --network=none container
gate. This module never accepts a command, interface, PID or path from control
requests. Application processes remain UID/GID 65534 with zero capabilities.
"""

from collections import OrderedDict
import ipaddress
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import sys
import time
import uuid

from camera_lan import Network
from topology import Topology, TopologyInterrupted, main, require_isolation, run


CAMERAS = {"camera": "02:00:00:25:00:10", "camera2": "02:00:00:25:00:11"}
POOL = (ipaddress.IPv4Address("192.168.250.20"), ipaddress.IPv4Address("192.168.250.99"))
OPERATIONS = {
    "move_camera": {"role", "address"}, "renew_camera": {"role"},
    "restart_dhcp": set(), "uplink_down": set(), "uplink_up": set(), "status": set(),
}


class ControlError(Exception):
    pass


def validate_request(request):
    if not isinstance(request, dict) or type(request.get("version")) is not int or request["version"] != 1:
        raise ControlError("invalid_request")
    if not isinstance(request.get("id"), str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", request["id"]):
        raise ControlError("invalid_request")
    operation = request.get("op")
    if not isinstance(operation, str) or operation not in OPERATIONS:
        raise ControlError("invalid_request")
    if set(request) != {"version", "id", "op"} | OPERATIONS[operation]:
        raise ControlError("invalid_request")
    if "role" in request and (not isinstance(request["role"], str) or request["role"] not in CAMERAS):
        raise ControlError("invalid_request")
    if operation == "move_camera":
        try:
            address = ipaddress.IPv4Address(request["address"])
        except (ipaddress.AddressValueError, TypeError):
            raise ControlError("invalid_request") from None
        if str(address) != request["address"] or not POOL[0] <= address <= POOL[1]:
            raise ControlError("invalid_request")
    return request


def atomic_json(path, value, mode=0o600):
    temporary = path.with_suffix(".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "w") as output:
            json.dump(value, output, sort_keys=True)
            output.write("\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def control_request(manifest, operation, *, role=None, address=None, request_id=None, timeout=30):
    """Root coordinator helper. One request in flight; no shell or namespaces.

    The outer runner consumes the fixed private files. Helpers launched with an
    application exec_prefix cannot traverse their directory. IDs may be reused
    only to retry the exact same request after an uncertain response.
    """
    request = {"version": 1, "id": request_id or uuid.uuid4().hex, "op": operation}
    if role is not None:
        request["role"] = role
    if address is not None:
        request["address"] = address
    validate_request(request)
    if type(timeout) not in (int, float) or not 0 < timeout <= 30:
        raise ControlError("invalid_timeout")
    directory = Path(manifest["control"]["directory"])
    # A retry can otherwise match an old reply before the server consumes it.
    (directory / "response.json").unlink(missing_ok=True)
    atomic_json(directory / "request.json", request)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = directory / "response.json"
        if response.exists():
            with response.open("rb") as source:
                raw = source.read(65537)
            if len(raw) > 65536:
                raise ControlError("invalid_response")
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ControlError("invalid_response")
            if value.get("version") == 1 and value.get("id") == request["id"]:
                return value
        time.sleep(0.02)
    raise ControlError("timeout")


class QualificationTopology(Topology):
    def __init__(self, artifacts):
        super().__init__(artifacts)
        self.namespaces["camera2"] = f"shale-e2e-{os.getpid()}-camera2"
        self.addresses = {"camera": "192.168.250.20", "camera2": "192.168.250.21"}
        self.clients = {}
        self.responses = OrderedDict()
        self.uplink_up = True
        self.control = artifacts / "control"
        self.operation_deadline = None
        self.coordinator_deadline = None

    def command(self, *arguments):
        timeout = 3
        if self.operation_deadline is not None:
            timeout = min(timeout, self.operation_deadline - time.monotonic())
            if timeout <= 0:
                raise ControlError("timeout")
        return run(*arguments, timeout=timeout)

    def wait_for(self, condition, process, seconds, description):
        deadline = time.monotonic() + seconds
        if self.operation_deadline is not None:
            deadline = min(deadline, self.operation_deadline)
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise ControlError("process_exited")
            if condition():
                return
            time.sleep(0.02)
        raise ControlError("timeout")

    def setup(self):
        require_isolation()
        self.operation_deadline = time.monotonic() + 30
        for namespace in self.namespaces.values():
            self.command("ip", "netns", "add", namespace)
            self.created.append(namespace)
            self.command("ip", "-n", namespace, "link", "set", "lo", "up")
        edge = self.namespaces["edge"]
        self.command("ip", "-n", edge, "link", "add", "eth0", "type", "bridge", "mcast_snooping", "0")
        self.command("ip", "-n", edge, "link", "set", "eth0", "up")
        for index, role in enumerate((*CAMERAS, "cp")):
            left, right = f"ql{os.getpid() % 100000}{index}", f"qr{os.getpid() % 100000}{index}"
            self.command("ip", "link", "add", left, "type", "veth", "peer", "name", right)
            self.links.extend((left, right))
            self.command("ip", "link", "set", left, "netns", edge)
            self.command("ip", "link", "set", right, "netns", self.namespaces[role])
            interface = f"cam{index}" if role in CAMERAS else "wlan0"
            self.command("ip", "-n", edge, "link", "set", left, "name", interface)
            self.command("ip", "-n", self.namespaces[role], "link", "set", right, "name", "eth0")
            if role in CAMERAS:
                self.command("ip", "-n", edge, "link", "set", interface, "master", "eth0")
                self.command("ip", "-n", self.namespaces[role], "link", "set", "eth0", "address", CAMERAS[role])
            self.command("ip", "-n", edge, "link", "set", interface, "up")
            self.command("ip", "-n", self.namespaces[role], "link", "set", "eth0", "up")
        self.command("ip", "-n", edge, "addr", "add", "192.168.250.1/24", "dev", "eth0")
        self.command("ip", "-n", edge, "addr", "add", "10.23.0.2/24", "dev", "wlan0")
        self.command("ip", "-n", self.namespaces["cp"], "addr", "add", "10.23.0.1/24", "dev", "eth0")
        self.command("ip", "-n", edge, "route", "add", "default", "via", "10.23.0.1", "dev", "wlan0")
        for role in ("edge", *CAMERAS):
            self.command("ip", "-n", self.namespaces[role], "route", "add", "239.255.255.250/32", "dev", "eth0")
        self.command(*self.ns("edge", "python3", "-c", "from pathlib import Path; Path('/proc/sys/net/ipv4/ip_forward').write_text('0')"))
        self.control.mkdir(mode=0o700)
        leases = self.artifacts / "dhcp"
        leases.mkdir(mode=0o700)
        os.chown(leases, 65534, 65534)
        self.lease_file = leases / "leases"
        self.hosts_file = self.artifacts / "dhcp-hosts"
        self.write_reservations()
        generated = Network("eth0", "wlan0", "192.168.250.1/24", str(POOL[0]), str(POOL[1])).files()["camera-dhcp.conf"][1]
        self.conf = self.artifacts / "dnsmasq.conf"
        self.conf.write_text(generated.replace("/var/lib/shale-camera-dhcp/leases", str(self.lease_file)) +
                             f"\ndhcp-hostsfile={self.hosts_file}\n")
        self.conf.chmod(0o644)
        self.start_daemon()
        self.hook = self.artifacts / "dhcp-hook"
        self.hook.write_text(DHCP_HOOK)
        self.hook.chmod(0o755)
        for role in CAMERAS:
            lease = self.artifacts / f"{role}-lease.json"
            self.clients[role] = self.start(self.ns(role, "busybox", "udhcpc", "-f", "-n", "-i", "eth0",
                                                   "-t", "4", "-T", "2", "-s", str(self.hook)),
                                            self.artifacts / f"{role}-dhcp.log",
                                            env={**os.environ, "SHALE_DHCP_LEASE": str(lease)})
            self.wait_for(lambda: self.lease(role).get("ip") == self.addresses[role],
                          self.clients[role], 15, role + " DHCP lease")
        self.write_manifest()
        self.operation_deadline = None

    def write_reservations(self):
        temporary = self.hosts_file.with_suffix(".tmp")
        temporary.write_text("".join(f"{CAMERAS[role]},{address}\n" for role, address in self.addresses.items()))
        temporary.chmod(0o644)
        os.replace(temporary, self.hosts_file)

    def start_daemon(self):
        caps = "+net_admin,+net_raw,+net_bind_service"
        with (self.artifacts / "dnsmasq.log").open("a") as output:
            self.daemon = subprocess.Popen(self.ns("edge", "setpriv", "--reuid=65534", "--regid=65534", "--clear-groups",
                                                   "--inh-caps=" + caps, "--ambient-caps=" + caps, "dnsmasq",
                                                   "--keep-in-foreground", "--conf-file=" + str(self.conf)),
                                           stdout=output, stderr=subprocess.STDOUT, start_new_session=True)
        self.processes.append(self.daemon)
        # A new PID must own an actual bound DHCP socket before a control reply.
        self.wait_for(lambda: any(row.split()[1].endswith(":0043") for row in
                                 self.command(*self.ns("edge", "cat", f"/proc/{self.daemon.pid}/net/udp")).stdout.splitlines()[1:]),
                      self.daemon, 5, "DHCP server startup")

    def lease(self, role):
        path = self.artifacts / f"{role}-lease.json"
        return json.loads(path.read_text()) if path.exists() else {}

    def camera_state(self, role):
        # ip's -4 address view omits link-layer fields; use the complete view
        # and select only inet entries so the MAC is observed, not assumed.
        rows = json.loads(self.command("ip", "-n", self.namespaces[role], "-j", "address", "show", "dev", "eth0").stdout)
        return {"mac": rows[0]["address"], "addresses": [info["local"] for row in rows for info in row.get("addr_info", []) if info.get("family") == "inet"]}

    def camera_ips(self, role):
        return self.camera_state(role)["addresses"]

    def status(self):
        leases = []
        if self.lease_file.exists():
            for line in self.lease_file.read_text().splitlines():
                fields = line.split()
                if len(fields) >= 3 and fields[1] in CAMERAS.values():
                    leases.append({"expires": int(fields[0]), "mac": fields[1], "address": fields[2]})
        return {"cameras": {role: {**self.camera_state(role), "lease": self.lease(role),
                                    "client_pid": self.clients[role].pid, "client_alive": self.clients[role].poll() is None}
                            for role in CAMERAS}, "leases": leases, "dhcp_pid": self.daemon.pid,
                "dhcp_alive": self.daemon.poll() is None, "uplink_up": self.uplink_up}

    def write_manifest(self):
        roles = {"edge": {"camera_interface": "eth0", "camera_address": "192.168.250.1", "camera_cidr": "192.168.250.0/24",
                           "uplink_interface": "wlan0", "uplink_address": "10.23.0.2", "exec_prefix": self.app("edge")},
                 "cp": {"interface": "eth0", "address": "10.23.0.1", "exec_prefix": self.app("cp")}}
        for role in CAMERAS:
            roles[role] = {"interface": "eth0", "address": self.addresses[role], "mac": CAMERAS[role],
                           "logical_identity": role + "-fixture", "exec_prefix": self.app(role)}
        value = {"version": 1, "application_uid": 65534, "application_gid": 65534, "roles": roles,
                 "discovery": {"address": "239.255.255.250", "port": 3702},
                 "dhcp": {"lease": self.lease("camera"), "client_log": str(self.artifacts / "camera-dhcp.log"),
                          "leases": {role: self.lease(role) for role in CAMERAS}, "lease_file": str(self.lease_file),
                          "server_log": str(self.artifacts / "dnsmasq.log")},
                 "control": {"version": 1, "directory": str(self.control), "request_file": str(self.control / "request.json"),
                             "response_file": str(self.control / "response.json"), "max_request_bytes": 4096,
                             "max_response_bytes": 65536, "operation_timeout_seconds": 20,
                             "operations": sorted(OPERATIONS)}}
        self.manifest = self.artifacts / "topology.json"
        atomic_json(self.manifest, value, 0o644)

    def dispatch(self, request):
        operation, role = request["op"], request.get("role")
        if operation == "move_camera":
            target = request["address"]
            if any(target == address for other, address in self.addresses.items() if other != role):
                raise ControlError("address_conflict")
            old = self.addresses[role]
            if target != old:
                client = self.clients[role]
                client.send_signal(signal.SIGUSR2)
                self.wait_for(lambda: self.lease(role).get("event") == "deconfig" and old not in self.camera_ips(role),
                              client, 5, "old address release")
                self.addresses[role] = target
                self.write_reservations()
                self.daemon.send_signal(signal.SIGHUP)
                client.send_signal(signal.SIGUSR1)
                self.wait_for(lambda: self.lease(role).get("ip") == target and self.camera_ips(role) == [target],
                              client, 15, "camera address move")
                self.write_manifest()
        elif operation == "renew_camera":
            previous = self.lease(role).get("sequence", 0)
            self.clients[role].send_signal(signal.SIGUSR1)
            self.wait_for(lambda: self.lease(role).get("sequence", 0) > previous and
                          self.lease(role).get("ip") == self.addresses[role], self.clients[role], 15, "camera DHCP renewal")
        elif operation == "restart_dhcp":
            previous = self.daemon
            # A stopped known single-process daemon must not leave a stale PID
            # in the cleanup list. An interrupted stop is still caught by the
            # owned namespace's PID sweep and child-subreaper cleanup.
            self.processes.remove(previous)
            previous.terminate()
            try:
                previous.wait(timeout=3)
            except subprocess.TimeoutExpired:
                previous.kill()
                previous.wait(timeout=3)
            self.start_daemon()
        elif operation in ("uplink_down", "uplink_up"):
            up = operation == "uplink_up"
            self.command("ip", "-n", self.namespaces["edge"], "link", "set", "wlan0", "up" if up else "down")
            if up:
                # Link-down removes the gateway route. Model the route restore
                # a Wi-Fi manager performs, without touching the camera bridge.
                self.command("ip", "-n", self.namespaces["edge"], "route", "replace", "default", "via", "10.23.0.1", "dev", "wlan0")
            self.uplink_up = up

    def handle_control(self):
        path = self.control / "request.json"
        if not path.exists() and not path.is_symlink():
            return
        request_id, canonical = None, None
        try:
            if path.is_symlink():
                raise ControlError("invalid_request")
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(descriptor, "rb") as source:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077 or info.st_size > 4096:
                    raise ControlError("invalid_request")
                decoded = json.loads(source.read(4097))
                if isinstance(decoded, dict) and isinstance(decoded.get("id"), str) and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", decoded["id"]):
                    request_id = decoded["id"]
                request = validate_request(decoded)
            canonical = json.dumps(request, sort_keys=True)
            if request_id in self.responses:
                prior, response = self.responses[request_id]
                if prior != canonical:
                    raise ControlError("id_conflict")
            else:
                self.operation_deadline = time.monotonic() + 20
                if self.coordinator_deadline is not None:
                    self.operation_deadline = min(self.operation_deadline, self.coordinator_deadline)
                self.dispatch(request)
                response = {"version": 1, "id": request_id, "ok": True, "status": self.status()}
        except TopologyInterrupted:
            raise
        except Exception as error:
            code = str(error) if isinstance(error, ControlError) else "operation_failed"
            response = {"version": 1, "id": request_id, "ok": False, "code": code}
        finally:
            self.operation_deadline = None
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        if canonical is not None and response.get("code") != "id_conflict":
            self.responses[request_id] = (canonical, response)
            while len(self.responses) > 128:
                self.responses.popitem(last=False)
        atomic_json(self.control / "response.json", response)

    def coordinate(self, command, timeout):
        env = {**os.environ, "SHALE_TOPOLOGY_FILE": str(self.manifest)}
        coordinator = subprocess.Popen(self.ns("cp", *command), env=env, start_new_session=True)
        self.processes.append(coordinator)
        deadline = time.monotonic() + timeout
        self.coordinator_deadline = deadline
        while coordinator.poll() is None:
            self.handle_control()
            if self.daemon.poll() is not None or any(client.poll() is not None for client in self.clients.values()):
                raise RuntimeError("DHCP service/client exited while coordinator was running")
            if time.monotonic() >= deadline:
                raise RuntimeError("coordinator timed out")
            time.sleep(0.02)
        return coordinator.returncode


DHCP_HOOK = r'''#!/usr/bin/python3
import ipaddress, json, os, pathlib, subprocess, sys
path = pathlib.Path(os.environ['SHALE_DHCP_LEASE'])
old = json.loads(path.read_text()) if path.exists() else {}
event = sys.argv[1]
if os.environ.get('interface') != 'eth0' or event not in ('deconfig', 'bound', 'renew'):
    raise SystemExit(0)
lease = {'event': event, 'ip': '', 'sequence': old.get('sequence', 0) + 1}
if event in ('bound', 'renew'):
    lease.update({k: os.environ.get(k, '') for k in ('ip', 'subnet', 'router', 'dns', 'ntpsrv')})
    address = ipaddress.IPv4Address(lease['ip'])
    assert ipaddress.IPv4Address('192.168.250.20') <= address <= ipaddress.IPv4Address('192.168.250.99')
    assert lease['subnet'] == '255.255.255.0' and not any(lease[k] for k in ('router', 'dns', 'ntpsrv'))
if old.get('ip') and old['ip'] != lease['ip']:
    subprocess.run(['ip', '-4', 'addr', 'del', old['ip'] + '/24', 'dev', 'eth0'], check=True)
if lease['ip']:
    subprocess.run(['ip', '-4', 'addr', 'replace', lease['ip'] + '/24', 'dev', 'eth0'], check=True)
with path.with_suffix('.tmp').open('w') as output:
    json.dump(lease, output)
os.replace(path.with_suffix('.tmp'), path)
'''


if __name__ == "__main__":
    sys.exit(main(QualificationTopology))
