"""No-root protocol/validation checks; actual DHCP is a separate opt-in test."""

import inspect
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from qualification_topology import (
    ControlError, QualificationTopology, atomic_json, control_request, validate_request,
)
from topology import Topology, TopologyInterrupted, main


class QualificationControlTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="shale-control-unit-")
        self.addCleanup(self.temporary.cleanup)
        self.topology = QualificationTopology(Path(self.temporary.name))
        self.topology.control.mkdir(mode=0o700)
        self.calls = []
        self.topology.dispatch = lambda request: self.calls.append(request)
        self.topology.status = lambda: {"dhcp_alive": True}

    def submit(self, request):
        atomic_json(self.topology.control / "request.json", request)
        self.topology.handle_control()
        return json.loads((self.topology.control / "response.json").read_text())

    def test_only_fixed_operations_and_pool_addresses_are_accepted(self):
        for operation in ("status", "restart_dhcp", "uplink_down", "uplink_up"):
            validate_request({"version": 1, "id": "request", "op": operation})
        for role in ("camera", "camera2"):
            validate_request({"version": 1, "id": "request", "op": "renew_camera", "role": role})
            validate_request({"version": 1, "id": "request", "op": "move_camera", "role": role,
                              "address": "192.168.250.99"})
        base = {"version": 1, "id": "request", "op": "move_camera", "role": "camera", "address": "192.168.250.30"}
        invalid = [dict(base, version=True), dict(base, version=2), dict(base, id="secret\ncommand"),
                   dict(base, id="x" * 65), dict(base, op="exec"), dict(base, op=[]),
                   dict(base, role="edge"), dict(base, role=[]), dict(base, command="secret"),
                   dict(base, interface="wlan0"), dict(base, password="secret")]
        for address in ("192.168.250.1", "192.168.250.100", "10.23.0.1", "192.168.250.30/24",
                        "192.168.250.30\n", "127.0.0.1", 20, None):
            invalid.append(dict(base, address=address))
        for request in invalid:
            with self.subTest(request=request), self.assertRaisesRegex(ControlError, "^invalid_request$"):
                validate_request(request)
        self.assertEqual([], self.calls)

    def test_correlated_retry_is_idempotent_and_changed_payload_is_denied(self):
        request = {"version": 1, "id": "retry", "op": "status"}
        response = self.submit(request)
        self.assertTrue(response["ok"])
        self.assertEqual("retry", response["id"])
        self.assertEqual(response, self.submit(request))
        self.assertEqual(1, len(self.calls))
        denied = self.submit(dict(request, op="uplink_down"))
        self.assertEqual("id_conflict", denied["code"])
        self.assertEqual(1, len(self.calls))
        self.assertEqual(response, self.submit(request))

    def test_cached_reply_does_not_delete_the_next_request(self):
        cached = {"version": 1, "id": "cached", "op": "status"}
        self.submit(cached)
        next_request = {"version": 1, "id": "next", "op": "status"}
        atomic_json(self.topology.control / "request.json", cached)
        def respond_then_submit(path, value, mode=0o600):
            atomic_json(path, value, mode)
            if path.name == "response.json":
                atomic_json(self.topology.control / "request.json", next_request)
        with patch("qualification_topology.atomic_json", side_effect=respond_then_submit):
            self.topology.handle_control()
        self.assertTrue((self.topology.control / "request.json").exists(),
                        "a caller may submit its next request immediately after seeing a cached reply")
        self.assertEqual(1, len(self.calls))

    def test_invalid_fields_and_sensitive_failures_never_cross_response(self):
        denied = self.submit({"version": 1, "id": "unknown", "op": "status", "password": "fixture-secret"})
        self.assertEqual("unknown", denied["id"])
        self.assertEqual("invalid_request", denied["code"])
        self.assertEqual([], self.calls)
        def fail(_):
            raise RuntimeError("rtsp://operator:fixture-secret@camera.invalid/live")
        self.topology.dispatch = fail
        failed = self.submit({"version": 1, "id": "failure", "op": "status"})
        self.assertEqual("operation_failed", failed["code"])
        self.assertNotIn("fixture-secret", json.dumps(failed))
        self.assertNotIn("rtsp", json.dumps(failed))

    def test_symlinks_oversize_and_shared_files_are_refused(self):
        private = self.topology.artifacts / "private.txt"
        private.write_text("fixture-secret")
        request = self.topology.control / "request.json"
        request.symlink_to(private)
        self.topology.handle_control()
        self.assertEqual("fixture-secret", private.read_text())
        self.assertFalse(request.exists())
        self.assertEqual("invalid_request", json.loads((self.topology.control / "response.json").read_text())["code"])
        for size, mode in ((4097, 0o600), (100, 0o644)):
            request.write_text("x" * size)
            request.chmod(mode)
            self.topology.handle_control()
            self.assertFalse(request.exists())
            self.assertEqual("invalid_request", json.loads((self.topology.control / "response.json").read_text())["code"])
        self.assertEqual([], self.calls)

    def test_response_cache_is_bounded_and_does_not_change_original_factory(self):
        for index in range(140):
            self.submit({"version": 1, "id": str(index), "op": "status"})
        self.assertEqual(128, len(self.topology.responses))
        self.assertIs(Topology, inspect.signature(main).parameters["factory"].default)
        self.assertEqual({"edge", "camera", "cp"}, set(Topology(self.topology.artifacts).namespaces))
        self.assertEqual({"edge", "camera", "camera2", "cp"}, set(self.topology.namespaces))

    def test_public_manifest_and_private_controls_have_explicit_modes(self):
        previous = os.umask(0o077)
        try:
            public = self.topology.artifacts / "public.json"
            atomic_json(public, {"version": 1}, 0o644)
            response = self.submit({"version": 1, "id": "modes", "op": "status"})
        finally:
            os.umask(previous)
        self.assertTrue(response["ok"])
        self.assertEqual(0o644, public.stat().st_mode & 0o777)
        self.assertEqual(0o700, self.topology.control.stat().st_mode & 0o777)
        self.assertEqual(0o600, (self.topology.control / "response.json").stat().st_mode & 0o777)

    def test_signal_interrupt_is_not_converted_into_recoverable_control_error(self):
        def interrupted(_):
            raise TopologyInterrupted("signal")
        self.topology.dispatch = interrupted
        atomic_json(self.topology.control / "request.json", {"version": 1, "id": "signal", "op": "status"})
        with self.assertRaises(TopologyInterrupted):
            self.topology.handle_control()
        self.assertIsNone(self.topology.operation_deadline)
        self.assertFalse((self.topology.control / "request.json").exists())

    def test_client_ignores_stale_response_and_has_bounded_wait(self):
        atomic_json(self.topology.control / "response.json", {"version": 1, "id": "old", "ok": True})
        manifest = {"control": {"directory": str(self.topology.control)}}
        def serve():
            deadline = time.monotonic() + 2
            while not (self.topology.control / "request.json").exists():
                if time.monotonic() >= deadline:
                    return
                time.sleep(0.001)
            self.topology.handle_control()
        server = threading.Thread(target=serve)
        server.start()
        try:
            response = control_request(manifest, "status", request_id="new", timeout=2)
            self.assertEqual("new", response["id"])
        finally:
            server.join(timeout=2)
        self.assertFalse(server.is_alive())
        before = time.monotonic()
        with self.assertRaisesRegex(ControlError, "^timeout$"):
            control_request(manifest, "status", request_id="timeout", timeout=0.05)
        self.assertLess(time.monotonic() - before, 0.5)


if __name__ == "__main__":
    unittest.main()
