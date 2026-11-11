#!/usr/bin/env python3
"""Exercise the topology contract with small socket peers, without media claims."""

import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time


def manifest():
    return json.loads(Path(os.environ["SHALE_TOPOLOGY_FILE"]).read_text())


def identity():
    status = dict(line.split(":", 1) for line in Path("/proc/self/status").read_text().splitlines() if ":" in line)
    assert os.getuid() == 65534 and os.getgid() == 65534
    for key in ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"):
        assert int(status[key].strip(), 16) == 0, (key, status[key])
    assert status["NoNewPrivs"].strip() == "1"
    return {"uid": os.getuid(), "gid": os.getgid(), "capabilities": 0}


def camera(ready):
    topology = manifest()
    address = topology["roles"]["camera"]["address"]
    proof = identity()
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp.bind(("0.0.0.0", topology["discovery"]["port"]))
    group = socket.inet_aton(topology["discovery"]["address"]) + socket.inet_aton(address)
    udp.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, group)
    tcp = socket.socket()
    tcp.bind((address, 8554))
    tcp.listen()

    def rtsp():
        with tcp.accept()[0] as connection:
            request = connection.recv(4096)
            assert request.startswith(b"OPTIONS rtsp://")
            connection.sendall(b"RTSP/1.0 200 OK\r\nCSeq: 1\r\nPublic: OPTIONS\r\n\r\n")

    thread = threading.Thread(target=rtsp, daemon=True)
    thread.start()
    Path(ready).write_text(json.dumps(proof))
    request, peer = udp.recvfrom(4096)
    assert b"Probe" in request, request
    udp.sendto(b'<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">'
               b'<s:Body><ProbeMatches/></s:Body></s:Envelope>', peer)
    thread.join(timeout=5)
    assert not thread.is_alive(), "RTSP peer was not reached"
    # Deliberately outlive the coordinator in a separate session: runner owns cleanup.
    time.sleep(300)


def edge():
    topology = manifest()
    proof = identity()
    role = topology["roles"]["edge"]
    routes = subprocess.check_output(["ip", "-4", "route"], text=True)
    assert "default via 10.23.0.1 dev wlan0" in routes, routes
    assert "239.255.255.250 dev eth0" in routes, routes
    assert Path("/proc/sys/net/ipv4/ip_forward").read_text().strip() == "0"
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.settimeout(5)
    udp.bind((role["camera_address"], 0))
    udp.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(role["camera_address"]))
    udp.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("b", 1))
    udp.sendto(b'<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">'
               b'<s:Body><Probe/></s:Body></s:Envelope>',
               (topology["discovery"]["address"], topology["discovery"]["port"]))
    response, peer = udp.recvfrom(4096)
    assert b"ProbeMatches" in response and peer[0] == topology["roles"]["camera"]["address"]
    with socket.create_connection((peer[0], 8554), timeout=5) as connection:
        connection.sendall(f"OPTIONS rtsp://{peer[0]}:8554/test RTSP/1.0\r\nCSeq: 1\r\n\r\n".encode())
        assert connection.recv(4096).startswith(b"RTSP/1.0 200 OK")
    with socket.create_connection((topology["roles"]["cp"]["address"], 8555), timeout=5) as connection:
        assert connection.getsockname()[0] == role["uplink_address"]
        connection.sendall(b"edge-uplink")
    proof.update(multicast_probe_reply=True, rtsp_options=True, cp_uplink=True)
    print(json.dumps(proof))


def coordinator():
    topology = manifest()
    assert os.geteuid() == 0, "coordinator needs test-only namespace launch privileges"
    addresses = subprocess.check_output(["ip", "-4", "address"], text=True)
    assert topology["roles"]["cp"]["address"] in addresses, addresses
    assert topology["roles"]["camera"]["address"] not in addresses, addresses
    assert topology["dhcp"]["lease"]["event"] == "bound"
    listener = socket.socket()
    listener.bind((topology["roles"]["cp"]["address"], 8555))
    listener.listen()
    listener.settimeout(10)
    with tempfile.TemporaryDirectory(prefix="shale-topology-smoke-") as state:
        os.chown(state, 65534, 65534)
        ready = Path(state) / "camera-ready.json"
        camera_process = subprocess.Popen([*topology["roles"]["camera"]["exec_prefix"],
                                           sys.executable, __file__, "camera", str(ready)],
                                          start_new_session=True)
        deadline = time.monotonic() + 5
        while not ready.exists():
            assert camera_process.poll() is None, "camera helper exited"
            assert time.monotonic() < deadline, "camera helper did not become ready"
            time.sleep(0.02)
        camera_proof = json.loads(ready.read_text())
        result = subprocess.run([*topology["roles"]["edge"]["exec_prefix"],
                                 sys.executable, __file__, "edge"],
                                check=True, capture_output=True, text=True, timeout=10)
        with listener.accept()[0] as connection:
            assert connection.recv(128) == b"edge-uplink"
        proof = {"camera": camera_proof, "edge": json.loads(result.stdout),
                 "coordinator_in_cp_namespace": True, "detached_camera_pid": camera_process.pid}
        (Path(os.environ["SHALE_TOPOLOGY_FILE"]).parent / "smoke.json").write_text(json.dumps(proof, indent=2) + "\n")
        print("isolated topology: real DHCP lease, unprivileged multicast/RTSP socket routes and CP uplink passed")


if __name__ == "__main__":
    if sys.argv[1:] and sys.argv[1] == "camera":
        camera(sys.argv[2])
    elif sys.argv[1:] and sys.argv[1] == "edge":
        edge()
    else:
        coordinator()
