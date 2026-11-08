#!/usr/bin/env python3
"""Give a disposable replacement actor a distinct OS identity before cap drop.

Run only inside the qualification container and a new private mount namespace.
The production host-id reader still reads its ordinary OS paths. No identity is
injected into the application and no credential/key is copied from the old edge.
"""

import os
from pathlib import Path
import subprocess
import sys


def main():
    if (os.geteuid() != 0 or os.environ.get("SHALE_REPLACEMENT_IDENTITY") != "1"
            or not Path("/.dockerenv").is_file()
            or os.readlink("/proc/self/ns/mnt") == os.readlink("/proc/1/ns/mnt")):
        raise RuntimeError("explicit disposable replacement fixture required")
    paths = ("/sys/class/dmi/id/product_uuid", "/sys/firmware/devicetree/base/serial-number", "/etc/machine-id")
    target = next((Path(path) for path in paths if Path(path).is_file()), None)
    if target is None or len(sys.argv) < 2:
        raise RuntimeError("replacement fixture needs an OS identity path and actor")
    # This file exists only in the disposable container. The caller has made /
    # recursively private, so the bind cannot propagate to another actor/host.
    source = Path("/test/replacement-identity")
    source.write_text("2df28685-59de-45b5-a3d6-56ef75f4e202\n")
    source.chmod(0o644)
    subprocess.run(["mount", "--bind", str(source), str(target)], check=True, timeout=5)
    os.environ.pop("SHALE_REPLACEMENT_IDENTITY", None)
    os.execvp(sys.argv[1], sys.argv[1:])


if __name__ == "__main__":
    main()
