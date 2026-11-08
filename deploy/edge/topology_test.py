#!/usr/bin/env python3
"""Explicit container-only topology integration checks; no Go/FFmpeg workload."""

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

from topology import require_isolation, run


def snapshot():
    return (json.loads(run("ip", "-j", "link").stdout), run("ip", "netns", "list").stdout)


def main():
    require_isolation()
    before = snapshot()
    runner = [sys.executable, "topology.py"]
    sleeper = """import json, os, pathlib, subprocess, time
t = json.loads(pathlib.Path(os.environ['SHALE_TOPOLOGY_FILE']).read_text())
p = subprocess.Popen(t['roles']['edge']['exec_prefix'] + ['sleep', '300'], start_new_session=True)
pathlib.Path(os.environ['SHALE_TOPOLOGY_FILE']).with_name('child-pid').write_text(str(p.pid))
"""
    with tempfile.TemporaryDirectory(prefix="shale-topology-check-") as temp:
        # The unprivileged helpers must traverse the retained artifacts' parent.
        Path(temp).chmod(0o755)
        scenarios = [
            ("success", 30, [sys.executable, "topology_smoke.py"], 0),
            ("failure", 30, [sys.executable, "-c", sleeper + "raise SystemExit(7)"], 7),
            ("timeout", 0.3, [sys.executable, "-c", sleeper + "time.sleep(300)"], 1),
        ]
        for name, timeout, command, expected in scenarios:
            artifacts = Path(temp) / name
            result = subprocess.run([*runner, "--artifacts", str(artifacts), "--timeout", str(timeout),
                                     "--", *command], capture_output=True, text=True, timeout=40)
            assert result.returncode == expected, (name, result.stdout, result.stderr)
            status = json.loads((artifacts / "result.json").read_text())
            assert status["cleanup_complete"], (name, status)
            assert snapshot() == before, name + " leaked a namespace or outer interface"
            if name == "success":
                pid = json.loads((artifacts / "smoke.json").read_text())["detached_camera_pid"]
            else:
                pid = int((artifacts / "child-pid").read_text())
            assert not Path(f"/proc/{pid}").exists(), (name, "descendant survived", pid)

        artifacts = Path(temp) / "signal"
        process = subprocess.Popen([*runner, "--artifacts", str(artifacts), "--", sys.executable,
                                    "-c", sleeper + "time.sleep(300)"],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        deadline = time.monotonic() + 20
        while not (artifacts / "child-pid").exists():
            assert process.poll() is None, "runner exited before signal check"
            assert time.monotonic() < deadline, "signal check did not start"
            time.sleep(0.02)
        process.send_signal(signal.SIGTERM)
        output = process.communicate(timeout=10)
        assert process.returncode != 0, output
        assert json.loads((artifacts / "result.json").read_text())["cleanup_complete"]
        assert snapshot() == before, "signal cleanup leaked a namespace or outer interface"
        pid = int((artifacts / "child-pid").read_text())
        assert not Path(f"/proc/{pid}").exists(), ("signal descendant survived", pid)
    print("topology coordinator success/failure/timeout/SIGTERM and detached-child cleanup passed")


if __name__ == "__main__":
    main()
