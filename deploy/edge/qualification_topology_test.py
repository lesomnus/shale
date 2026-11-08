#!/usr/bin/env python3
"""Container-only two-camera topology success/failure/timeout/signal checks."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

from topology import require_isolation
from topology_test import snapshot


def main():
    require_isolation()
    before = snapshot()
    runner = [sys.executable, "qualification_topology.py"]
    sleeper = """import json, os, pathlib, subprocess, time
t = json.loads(pathlib.Path(os.environ['SHALE_TOPOLOGY_FILE']).read_text())
p = subprocess.Popen(t['roles']['camera2']['exec_prefix'] + ['sleep', '300'], start_new_session=True)
pathlib.Path(os.environ['SHALE_TOPOLOGY_FILE']).with_name('child-pid').write_text(str(p.pid))
"""
    with tempfile.TemporaryDirectory(prefix="shale-multicamera-check-") as temp:
        Path(temp).chmod(0o755)
        scenarios = [
            ("success", 60, [sys.executable, "qualification_smoke.py"], 0),
            ("failure", 5, [sys.executable, "-c", sleeper + "raise SystemExit(7)"], 7),
            ("timeout", 0.3, [sys.executable, "-c", sleeper + "time.sleep(300)"], 1),
        ]
        for name, timeout, command, expected in scenarios:
            artifacts = Path(temp) / name
            result = subprocess.run([*runner, "--artifacts", str(artifacts), "--timeout", str(timeout), "--", *command],
                                    capture_output=True, text=True, timeout=100)
            status = json.loads((artifacts / "result.json").read_text())
            assert status["cleanup_complete"], status
            assert snapshot() == before, name + " leaked a namespace or interface"
            assert result.returncode == expected, (name, status, result.stdout, result.stderr)
            if name == "success":
                proof = json.loads((artifacts / "qualification.json").read_text())
                pids = proof["camera_pids"].values()
                print(json.dumps({"before": proof["initial"]["cameras"]["camera"]["addresses"],
                                  "after": proof["after_move"]["cameras"]["camera"]["addresses"],
                                  "mac": proof["after_move"]["cameras"]["camera"]["mac"],
                                  "other_camera": proof["after_move"]["cameras"]["camera2"]["addresses"],
                                  "same_dhcp_pid": proof["initial"]["dhcp_pid"] == proof["after_move"]["dhcp_pid"],
                                  "same_client_pid": proof["initial"]["cameras"]["camera"]["client_pid"] ==
                                  proof["after_move"]["cameras"]["camera"]["client_pid"],
                                  "old_ip_removed": proof["initial"]["cameras"]["camera"]["addresses"][0] not in
                                  proof["after_move"]["cameras"]["camera"]["addresses"]}))
            else:
                pids = [int((artifacts / "child-pid").read_text())]
            assert all(not Path(f"/proc/{pid}").exists() for pid in pids), (name, pids)
        artifacts = Path(temp) / "signal"
        process = subprocess.Popen([*runner, "--artifacts", str(artifacts), "--", sys.executable, "-c", sleeper + "time.sleep(300)"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        deadline = time.monotonic() + 35
        while not (artifacts / "child-pid").exists():
            assert process.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.02)
        process.send_signal(signal.SIGTERM)
        output = process.communicate(timeout=10)
        assert process.returncode != 0, output
        assert json.loads((artifacts / "result.json").read_text())["cleanup_complete"]
        assert snapshot() == before
        pid = int((artifacts / "child-pid").read_text())
        assert not Path(f"/proc/{pid}").exists()
    print("two-camera success/failure/timeout/SIGTERM cleanup passed; outer namespace unchanged")


if __name__ == "__main__":
    main()
