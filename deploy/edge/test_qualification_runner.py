import contextlib
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import qualification_test


class RunnerContract(unittest.TestCase):
    def binary(self, root):
        path = Path(root) / "e2e.test"
        path.write_bytes(b"synthetic binary placeholder")
        return str(path)

    def test_missing_test_cannot_start_docker(self):
        calls = []

        def run(command, **_):
            calls.append(command)
            return subprocess.CompletedProcess(command, 0, "TestUnrelated\n", "")

        with tempfile.TemporaryDirectory() as root, patch.object(
                sys, "argv", ["qualification_test.py", "--test-binary", self.binary(root)]), \
                patch.object(qualification_test.subprocess, "run", side_effect=run), \
                contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                qualification_test.main()
        self.assertEqual(1, len(calls))
        self.assertNotEqual("docker", calls[0][0])

    def test_successful_exit_still_requires_each_mode_witness(self):
        required = "\n".join(("TestEdgeIsolatedCentralWorkflow", "TestEdgeCameraProcess",
                              "TestEdgeRuntimeProcess", "TestEdgeControlProcess"))
        calls = []

        def run(command, **_):
            calls.append(command)
            if "-test.list" in command:
                return subprocess.CompletedProcess(command, 0, required, "")
            if command[:2] == ["docker", "run"]:
                return subprocess.CompletedProcess(command, 0, "PASS (no tests to run)\n", "")
            return subprocess.CompletedProcess(command, 0, "", "")

        with tempfile.TemporaryDirectory() as root, patch.object(
                sys, "argv", ["qualification_test.py", "--test-binary", self.binary(root)]), \
                patch.object(qualification_test.subprocess, "run", side_effect=run), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(RuntimeError, "completion witness"):
                qualification_test.main()
        self.assertTrue(any(command[:3] == ["docker", "rm", "-f"] for command in calls))
        self.assertTrue(any(command[:3] == ["docker", "image", "rm"] for command in calls))


if __name__ == "__main__":
    unittest.main()
