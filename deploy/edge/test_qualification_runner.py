import contextlib
import io
import os
from pathlib import Path
import subprocess
import signal
import sys
import tempfile
import unittest
from unittest.mock import patch

import qualification_test


class RunnerContract(unittest.TestCase):
    required = "\n".join(("TestEdgeIsolatedCentralWorkflow", "TestEdgeCameraProcess",
                          "TestEdgeRuntimeProcess", "TestEdgeControlProcess",
                          "TestEdgeIsolatedRecovery", "TestEdgeIsolatedOperations"))

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
        calls = []

        def run(command, **_):
            calls.append(command)
            if "-test.list" in command:
                return subprocess.CompletedProcess(command, 0, self.required, "")
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

    def test_sigterm_runs_both_cleanup_steps_and_restores_handlers(self):
        calls = []
        original = signal.getsignal(signal.SIGTERM)
        sentinel = lambda *_: None
        signal.signal(signal.SIGTERM, sentinel)
        def run(command, **_):
            calls.append(command)
            if "-test.list" in command:
                return subprocess.CompletedProcess(command, 0, self.required, "")
            if command[:2] == ["docker", "run"]:
                os.kill(os.getpid(), signal.SIGTERM)
                self.fail("termination did not interrupt the runner")
            return subprocess.CompletedProcess(command, 0, "", "")
        try:
            with tempfile.TemporaryDirectory() as root, patch.object(
                    sys, "argv", ["qualification_test.py", "--test-binary", self.binary(root)]), \
                    patch.object(qualification_test.subprocess, "run", side_effect=run):
                with self.assertRaisesRegex(qualification_test.QualificationInterrupted, "^qualification interrupted$"):
                    qualification_test.main()
            self.assertIs(sentinel, signal.getsignal(signal.SIGTERM))
        finally:
            signal.signal(signal.SIGTERM, original)
        self.assertTrue(any(command[:3] == ["docker", "rm", "-f"] for command in calls))
        self.assertTrue(any(command[:3] == ["docker", "image", "rm"] for command in calls))

    def test_image_cleanup_still_runs_when_container_cleanup_times_out(self):
        calls = []
        def run(command, **_):
            calls.append(command)
            if "-test.list" in command:
                return subprocess.CompletedProcess(command, 0, self.required, "")
            if command[:3] == ["docker", "rm", "-f"]:
                raise subprocess.TimeoutExpired(command, 15)
            return subprocess.CompletedProcess(command, 0, "", "")
        original = signal.getsignal(signal.SIGTERM)
        with tempfile.TemporaryDirectory() as root, patch.object(
                sys, "argv", ["qualification_test.py", "--test-binary", self.binary(root)]), \
                patch.object(qualification_test.subprocess, "run", side_effect=run), \
                contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(subprocess.TimeoutExpired):
                qualification_test.main()
        self.assertIs(original, signal.getsignal(signal.SIGTERM))
        self.assertTrue(any(command[:3] == ["docker", "image", "rm"] for command in calls))

    def test_all_and_individual_selectors_require_full_group_witnesses(self):
        selectors = {
            "all": "^TestEdgeIsolated(CentralWorkflow|Recovery|Operations)$",
            "combined": "^TestEdgeIsolatedCentralWorkflow$/^combined$",
            "independent": "^TestEdgeIsolatedCentralWorkflow$/^independent$",
            "recovery": "^TestEdgeIsolatedRecovery$",
            "operations": "^TestEdgeIsolatedOperations$",
        }
        for selected, selector in selectors.items():
            with self.subTest(case=selected):
                modes = ("combined", "independent", "recovery", "operations") if selected == "all" else (selected,)
                def run(command, **_):
                    if "-test.list" in command:
                        return subprocess.CompletedProcess(command, 0, self.required, "")
                    if command[:2] == ["docker", "run"]:
                        self.assertEqual(selector, command[command.index("-test.run") + 1])
                        self.assertIn("--network=none", command)
                        self.assertIn("--privileged", command)
                        self.assertEqual("2", command[command.index("--cpus=2")].split("=", 1)[1])
                        self.assertIn("--memory=4g", command)
                        self.assertFalse(any(value in command for value in ("--mount", "-v", "--network=host")))
                        return subprocess.CompletedProcess(command, 0, "\n".join("SHQA_RESULT_V1 " + mode for mode in modes), "")
                    return subprocess.CompletedProcess(command, 0, "", "")
                with tempfile.TemporaryDirectory() as root, patch.object(
                        sys, "argv", ["qualification_test.py", "--test-binary", self.binary(root), "--case", selected]), \
                        patch.object(qualification_test.subprocess, "run", side_effect=run), \
                        contextlib.redirect_stdout(io.StringIO()):
                    qualification_test.main()


if __name__ == "__main__":
    unittest.main()
