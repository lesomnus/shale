"""Check the real Debian assembly with a stub binary, without Go/console builds.

This qualifies installed files and the offline renderer, not the Shale binary or
network/service installation. Run from a checkout with python3 and dpkg-deb.
"""

import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(shutil.which("dpkg-deb"), "dpkg-deb is required")
class PackageContentTest(unittest.TestCase):
    def command(self, *args, **kwargs):
        return subprocess.run(
            args, check=True, cwd=ROOT, capture_output=True, text=True,
            timeout=30, **kwargs,
        )

    def test_installed_edge_files_and_offline_render(self):
        with tempfile.TemporaryDirectory(prefix="shale-package-content-") as temp:
            directory = Path(temp)
            tools = directory / "tools"
            tools.mkdir()
            fake_go = tools / "go"
            fake_go.write_text(
                f"#!{sys.executable}\n"
                "import os, pathlib, sys\n"
                "args = sys.argv[1:]\n"
                "assert args[0] == 'build' and args[-1] == './cmd/shale'\n"
                "assert os.environ['CGO_ENABLED'] == '0'\n"
                "assert os.environ['GOOS'] == 'linux'\n"
                "out = pathlib.Path(args[args.index('-o') + 1])\n"
                "out.write_text('package-content-test stub; not a Shale binary\\n')\n"
                "out.chmod(0o755)\n"
            )
            fake_go.chmod(0o755)
            environment = dict(os.environ, PATH=f"{tools}{os.pathsep}{os.environ['PATH']}",
                               SHALE_NO_CONSOLE="1", OUT=str(directory / "output with spaces"))

            for architecture in ("amd64", "arm64"):
                with self.subTest(architecture=architecture):
                    result = self.command("sh", "deploy/deb/build.sh", "0.0.0+contenttest",
                                          architecture, env=environment)
                    package = Path(result.stdout.strip())
                    self.assertTrue(package.is_file())
                    extracted = directory / architecture
                    control = directory / f"{architecture}-control"
                    self.command("dpkg-deb", "--extract", str(package), str(extracted))
                    self.command("dpkg-deb", "--control", str(package), str(control))
                    self.assertEqual(self.command("dpkg-deb", "--field", str(package),
                                                  "Architecture").stdout.strip(), architecture)

                    files = {
                        "etc/shale/shale.yaml.example": "deploy/deb/shale.yaml.example",
                        "etc/shale/shale-edge.yaml.example": "deploy/deb/shale-edge.yaml.example",
                        "lib/systemd/system/shale@.service": "deploy/systemd/shale@.service",
                        "usr/share/shale/edge/camera_lan.py": "deploy/edge/camera_lan.py",
                        "usr/share/doc/shale/README.systemd.md": "deploy/systemd/README.md",
                        "usr/share/doc/shale/docs/managed-edge.md": "docs/managed-edge.md",
                        "usr/share/doc/shale/deploy/edge/README.md": "deploy/edge/README.md",
                    }
                    for guide in ("camera-policy", "camera-credentials", "camera-worker-drivers"):
                        files[f"usr/share/doc/shale/docs/{guide}.md"] = f"docs/{guide}.md"
                    for installed, source in files.items():
                        with self.subTest(installed=installed):
                            path = extracted / installed
                            self.assertFalse(path.is_symlink())
                            self.assertEqual(path.read_bytes(), (ROOT / source).read_bytes())
                            expected_mode = 0o755 if installed.endswith(".py") else 0o644
                            self.assertEqual(stat.S_IMODE(path.stat().st_mode), expected_mode)
                    self.assertFalse((extracted / "etc/shale/shale.yaml").exists())
                    self.assertFalse((extracted / "etc/shale/camera-dhcp.conf").exists())
                    self.assertFalse((extracted / "etc/NetworkManager").exists())
                    self.assertFalse((extracted / "etc/systemd/system").exists())
                    self.assertEqual(set((control / "conffiles").read_text().splitlines()), {
                        "/etc/shale/shale.yaml.example", "/etc/shale/shale-edge.yaml.example",
                    })
                    for script in ("postinst", "prerm"):
                        self.assertEqual((control / script).read_bytes(),
                                         (ROOT / "deploy/deb" / script).read_bytes())
                    self.check_guide_links(extracted / "usr/share/doc/shale")

                    output = directory / f"rendered-{architecture}"
                    self.command(sys.executable, str(extracted / "usr/share/shale/edge/camera_lan.py"),
                                 "--offline", "--interface", "eth0", "--uplink", "wlan0",
                                 "--address", "192.168.250.1/24", "--first", "192.168.250.20",
                                 "--last", "192.168.250.99", "--output", str(output))
                    self.assertEqual({p.name for p in output.iterdir()}, {
                        "shale-camera-lan.nmconnection", "camera-dhcp.conf",
                        "shale-camera-dhcp.service", "camera-network.json",
                    })
                    self.assertEqual(stat.S_IMODE((output / "shale-camera-lan.nmconnection").stat().st_mode),
                                     0o600)
                    # A packaged renderer must retain the camera/uplink isolation.
                    dhcp = (output / "camera-dhcp.conf").read_text()
                    self.assertIn("interface=eth0\n", dhcp)
                    self.assertIn("no-dhcp-interface=wlan0\n", dhcp)
                    self.assertIn("dhcp-option=option:router\n", dhcp)
                    self.assertIn("dhcp-option=option:dns-server\n", dhcp)

    def check_guide_links(self, documents):
        count = 0
        guides = ("docs/managed-edge.md", "docs/camera-policy.md", "docs/camera-credentials.md",
                  "docs/camera-worker-drivers.md", "deploy/edge/README.md")
        for relative in guides:
            guide = documents / relative
            for link in re.findall(r"\[[^\]]+\]\(([^)]+)\)", guide.read_text()):
                if "://" in link or link.startswith("#"):
                    continue
                target = (guide.parent / link.split("#", 1)[0]).resolve()
                self.assertTrue(target.is_relative_to(documents.resolve()))
                self.assertTrue(target.is_file(), f"broken installed guide link: {relative}: {link}")
                count += 1
        self.assertGreaterEqual(count, 2)


if __name__ == "__main__":
    unittest.main()
