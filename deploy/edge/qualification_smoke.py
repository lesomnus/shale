#!/usr/bin/env python3
"""Two-camera DHCP/control/socket qualification; no media or device claims."""

import json
import errno
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

from qualification_topology import CAMERAS, control_request
from topology_smoke import identity, manifest


def camera(role, ready):
    topology = manifest()
    proof = identity()
    # Application identities can read the public manifest, never control files.
    try:
        list(Path(topology["control"]["directory"]).iterdir())
    except PermissionError:
        proof["control_denied"] = True
    else:
        raise AssertionError("unprivileged camera can access private control")
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(("0.0.0.0", 3702))
    udp.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                   socket.inet_aton("239.255.255.250") + socket.inet_aton(topology["roles"][role]["address"]))
    tcp = socket.socket()
    tcp.bind(("0.0.0.0", 8554))
    tcp.listen()
    def connections():
        while True:
            with tcp.accept()[0] as connection:
                assert connection.recv(16) == b"camera?"
                connection.sendall(role.encode())
    threading.Thread(target=connections, daemon=True).start()
    Path(ready).write_text(json.dumps(proof))
    while True:
        request, peer = udp.recvfrom(4096)
        assert request == b"Probe"
        udp.sendto(role.encode(), peer)


def edge_probe():
    topology = manifest()
    proof = identity()
    assert Path("/proc/sys/net/ipv4/ip_forward").read_text().strip() == "0"
    routes = subprocess.check_output(["ip", "-4", "route"], text=True)
    link = json.loads(subprocess.check_output(["ip", "-j", "link", "show", "dev", "wlan0"], text=True))[0]
    if "UP" in link["flags"]:
        assert "default via 10.23.0.1 dev wlan0" in routes, routes
    assert "239.255.255.250 dev eth0" in routes, routes
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.settimeout(3)
    udp.bind(("192.168.250.1", 0))
    udp.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton("192.168.250.1"))
    udp.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("b", 1))
    udp.sendto(b"Probe", ("239.255.255.250", 3702))
    replies = {}
    while len(replies) < 2:
        body, peer = udp.recvfrom(256)
        role = body.decode()
        assert role in CAMERAS and peer[0] == topology["roles"][role]["address"]
        replies[role] = peer[0]
    for role, address in replies.items():
        with socket.create_connection((address, 8554), timeout=3) as connection:
            connection.sendall(b"camera?")
            assert connection.recv(256).decode() == role
    proof["camera_sockets"] = replies
    print(json.dumps(proof))


def coordinator():
    topology = manifest()
    assert os.getuid() == 0
    routes = subprocess.check_output(["ip", "-4", "route"], text=True)
    assert "192.168.250" not in routes, routes
    def assert_cp_isolated():
        for role in CAMERAS:
            try:
                socket.create_connection((topology["roles"][role]["address"], 8554), timeout=0.2)
            except OSError as error:
                assert error.errno in (errno.ENETUNREACH, errno.EHOSTUNREACH), error
            else:
                raise AssertionError("CP namespace reached camera LAN")
    assert_cp_isolated()
    def request(operation, **kwargs):
        response = control_request(topology, operation, **kwargs)
        assert response["ok"], response
        return response["status"]
    initial = request("status")
    for role, camera_status in initial["cameras"].items():
        assert camera_status["lease"]["event"] == "bound"
        assert camera_status["mac"] == topology["roles"][role]["mac"]
        assert camera_status["addresses"] == [topology["roles"][role]["address"]]
    log = Path(topology["dhcp"]["server_log"]).read_text()
    for event in ("DHCPDISCOVER", "DHCPOFFER", "DHCPREQUEST", "DHCPACK"):
        assert event in log, event
    for mac in CAMERAS.values():
        assert mac in log
    with tempfile.TemporaryDirectory(prefix="shale-two-camera-smoke-") as temp:
        os.chown(temp, 65534, 65534)
        Path(temp).chmod(0o755)
        children = {}
        identities = {}
        for role in CAMERAS:
            ready = Path(temp) / (role + ".json")
            children[role] = subprocess.Popen([*topology["roles"][role]["exec_prefix"], sys.executable,
                                               __file__, "camera", role, str(ready)], start_new_session=True)
            deadline = time.monotonic() + 5
            while not ready.exists():
                assert children[role].poll() is None
                assert time.monotonic() < deadline
                time.sleep(0.02)
            identities[role] = json.loads(ready.read_text())
        def probe():
            result = subprocess.run([*topology["roles"]["edge"]["exec_prefix"], sys.executable, __file__, "probe"],
                                    capture_output=True, text=True, timeout=5)
            assert result.returncode == 0, (result.stdout, result.stderr)
            return json.loads(result.stdout)
        assert_cp_isolated()  # Cameras now listen; failure is actual routing isolation.
        probe()
        for role in CAMERAS:
            renewed = request("renew_camera", role=role)
            assert renewed["cameras"][role]["lease"]["sequence"] > initial["cameras"][role]["lease"]["sequence"]
        before = request("status")
        moved = request("move_camera", role="camera", address="192.168.250.30")
        assert moved["dhcp_pid"] == before["dhcp_pid"]
        assert moved["cameras"]["camera"]["addresses"] == ["192.168.250.30"]
        assert moved["cameras"]["camera"]["mac"] == before["cameras"]["camera"]["mac"]
        assert moved["cameras"]["camera"]["client_pid"] == before["cameras"]["camera"]["client_pid"]
        assert moved["cameras"]["camera2"] == before["cameras"]["camera2"], "moving one camera changed the other lease/client"
        after_move = probe()
        assert after_move["camera_sockets"]["camera"] == "192.168.250.30"
        # A duplicate response must not perform a second address transition.
        repeated = control_request(topology, "move_camera", role="camera", address="192.168.250.30", request_id="repeat")
        assert repeated == control_request(topology, "move_camera", role="camera", address="192.168.250.30", request_id="repeat")
        assert control_request(topology, "move_camera", role="camera", address="192.168.250.21")["code"] == "address_conflict"
        down = request("uplink_down")
        assert not down["uplink_up"]
        probe()
        request("renew_camera", role="camera2")
        up = request("uplink_up")
        assert up["uplink_up"]
        before_restart = request("status")
        restarted = request("restart_dhcp")
        assert restarted["dhcp_pid"] != before_restart["dhcp_pid"]
        for role in CAMERAS:
            assert restarted["cameras"][role]["client_pid"] == before_restart["cameras"][role]["client_pid"]
            assert restarted["cameras"][role]["addresses"] == before_restart["cameras"][role]["addresses"]
            request("renew_camera", role=role)
            assert children[role].poll() is None
        final = request("status")
        assert {lease["address"] for lease in final["leases"]} == {"192.168.250.30", "192.168.250.21"}
        probe()
        # Restore actual uplink reachability, not just its interface flag.
        listener = socket.socket()
        listener.bind(("10.23.0.1", 8555))
        listener.listen()
        listener.settimeout(3)
        sender = subprocess.Popen([*topology["roles"]["edge"]["exec_prefix"], sys.executable, "-c",
                                   "import socket; s=socket.create_connection(('10.23.0.1',8555),timeout=3); s.sendall(b'edge-uplink'); s.close()"])
        with listener.accept()[0] as connection:
            assert connection.recv(128) == b"edge-uplink"
        assert sender.wait(timeout=3) == 0
        proof = {"identities": identities, "initial": initial, "after_move": moved, "final": final,
                 "camera_pids": {role: child.pid for role, child in children.items()},
                 "two_multicast_socket_peers": True, "same_mac_address_move": True,
                 "uplink_flap_camera_continuity": True, "lease_persistence": True}
        (Path(os.environ["SHALE_TOPOLOGY_FILE"]).parent / "qualification.json").write_text(json.dumps(proof, indent=2) + "\n")
        print("two-camera DHCP/DORA/renew/same-MAC move, isolated controls, persistence and uplink recovery passed")
        # Children intentionally outlive the coordinator; the wrapper owns cleanup.


if __name__ == "__main__":
    if sys.argv[1:2] == ["camera"]:
        camera(sys.argv[2], sys.argv[3])
    elif sys.argv[1:2] == ["probe"]:
        edge_probe()
    else:
        coordinator()
