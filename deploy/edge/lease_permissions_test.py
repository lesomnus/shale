#!/usr/bin/env python3
"""Actual Unix DAC fixture, only in the explicitly isolated root test container."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile

from camera_lan import Network


def main():
    if os.environ.get("SHALE_CAMERA_LAN_NETNS_TEST") != "1" or os.geteuid() != 0:
        raise SystemExit("run explicitly inside the isolated test container")
    unit = Network("eth0", "wlan0", "192.168.250.1/24", "192.168.250.20", "192.168.250.99").files()["shale-camera-dhcp.service"][1]
    settings = dict(line.split("=", 1) for line in unit.splitlines() if "=" in line)
    owner, worker, readers, outside = 65533, 65534, 65532, 65531
    with tempfile.TemporaryDirectory(prefix="shale-lease-permissions-") as temp:
        base = Path(temp)
        base.chmod(0o755)
        state = base / "state"
        state.mkdir(mode=int(settings["StateDirectoryMode"], 8))
        os.chown(state, owner, readers)
        lease = state / "leases"
        old_umask = os.umask(int(settings["UMask"], 8))
        try:
            lease.write_text("0 02:00:00:00:00:01 192.168.250.20 * *\n")
        finally:
            os.umask(old_umask)
        os.chown(lease, owner, readers)
        assert lease.stat().st_mode & 0o777 == 0o640
        private = base / "producer-private"
        private.mkdir(mode=0o700)
        os.chown(private, worker, worker)
        key = private / "machine-key"
        key.write_bytes(b"synthetic-private-key-canary")
        key.chmod(0o600)
        os.chown(key, worker, worker)
        script = r'''
import os, pathlib, sys
role, directory, private = sys.argv[1:]
lease = pathlib.Path(directory) / "leases"
def denied(call):
    try: call()
    except PermissionError: return
    raise AssertionError("permission boundary was bypassed")
if role == "worker":
    assert lease.read_text().startswith("0 02:")
    denied(lambda: lease.open("a"))
    denied(lambda: (pathlib.Path(directory) / "new").write_bytes(b"x"))
    assert pathlib.Path(private).read_bytes() == b"synthetic-private-key-canary"
elif role == "dhcp":
    assert lease.read_text().startswith("0 02:")
    with lease.open("a") as f: f.write("\n")
    denied(lambda: pathlib.Path(private).read_bytes())
else:
    denied(lambda: lease.read_bytes())
'''
        for role, uid, gid, groups in (("worker", worker, worker, [readers]),
                                      ("dhcp", owner, readers, []),
                                      ("outside", outside, outside, [])):
            def identity(uid=uid, gid=gid, groups=groups):
                os.setgroups(groups)
                os.setgid(gid)
                os.setuid(uid)
            subprocess.run([sys.executable, "-c", script, role, str(state), str(key)],
                           preexec_fn=identity, check=True, timeout=5,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    print("Lease reader/owner/outsider and machine-key DAC boundaries passed")


if __name__ == "__main__":
    main()
