#!/usr/bin/env python3
"""Build/run the opt-in isolated synthetic media qualification and clean it up."""

import argparse
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-binary", type=Path, required=True,
                        help="current Linux internal/e2e test binary (go test -c)")
    parser.add_argument("--topology-module", choices=("topology.py", "qualification_topology.py"),
                        default="qualification_topology.py")
    parser.add_argument("--timeout", type=float, default=1000)
    parser.add_argument("--case", choices=("all", "combined", "independent"), default="all")
    args = parser.parse_args()
    if not args.test_binary.is_file() or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("an existing test binary and positive finite timeout are required")
    listed = subprocess.run([
        str(args.test_binary.resolve()), "-test.list",
        "^Test(EdgeIsolatedCentralWorkflow|EdgeCameraProcess|EdgeRuntimeProcess|EdgeControlProcess)$",
    ], check=True, capture_output=True, text=True, timeout=15,
        env={key: value for key, value in os.environ.items() if not key.startswith("SHALE_")})
    required = {"TestEdgeIsolatedCentralWorkflow", "TestEdgeCameraProcess",
                "TestEdgeRuntimeProcess", "TestEdgeControlProcess"}
    if not required.issubset(set(listed.stdout.splitlines())):
        parser.error("the binary is missing required qualification tests")
    source = Path(__file__).resolve().parent
    if not (source / args.topology_module).is_file():
        parser.error("the selected topology module is missing")
    owned = "shale-edge-qualification-" + uuid.uuid4().hex[:12]
    case = "^TestEdgeIsolatedCentralWorkflow$"
    if args.case != "all":
        case += "/^" + args.case + "$"
    image_created = False
    try:
        with tempfile.TemporaryDirectory(prefix="shale-qualification-build-") as temporary:
            context = Path(temporary)
            # No repository, credential/configuration directory or host socket
            # is mounted in the container. Its only inputs are these fixtures.
            for module in source.glob("*.py"):
                shutil.copy2(module, context / module.name)
            shutil.copy2(source / "qualification.Dockerfile", context / "Dockerfile")
            shutil.copy2(args.test_binary, context / "e2e.test")
            (context / "e2e.test").chmod(0o755)
            subprocess.run(["docker", "build", "-t", owned, str(context)], check=True,
                           timeout=300)
            image_created = True
            completed = subprocess.run([
                "docker", "run", "--rm", "--name", owned, "--network=none",
                "--privileged", "--cpus=2", "--memory=4g", "--pids-limit=512",
                owned, "python3", args.topology_module, "--timeout", str(args.timeout),
                "--", "/test/e2e.test", "-test.run", case, "-test.v", "-test.timeout=18m",
            ], check=False, timeout=args.timeout + 15, capture_output=True, text=True)
            print(completed.stdout, end="")
            print(completed.stderr, end="", file=sys.stderr)
            completed.check_returncode()
            modes = ("combined", "independent") if args.case == "all" else (args.case,)
            for mode in modes:
                if "SHQA_RESULT_V1 " + mode not in completed.stdout.splitlines():
                    raise RuntimeError("qualification completion witness is missing")
    finally:
        subprocess.run(["docker", "rm", "-f", owned], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, timeout=15)
        if image_created:
            subprocess.run(["docker", "image", "rm", owned], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=30)


if __name__ == "__main__":
    main()
