#!/usr/bin/env python3
"""Exercise the actual downloader over local HTTPS, including integrity failure."""
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import os
from pathlib import Path
import ssl
import subprocess
import tempfile
import threading
import unittest


class FetchToolTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="shale-ci-download-")
        cls.base = Path(cls.temp.name)
        cert, key = cls.base / "cert.pem", cls.base / "key.pem"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
                        "-keyout", str(key), "-out", str(cert)], check=True, capture_output=True)
        cls.counts = {}
        cls.payload = b"#!/bin/sh\nexit 0\n"

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                cls.counts[self.path] = cls.counts.get(self.path, 0) + 1
                if self.path == "/missing":
                    self.send_error(404)
                elif self.path == "/retry" and cls.counts[self.path] == 1:
                    self.send_error(504)
                else:
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(cls.payload)))
                    self.end_headers()
                    self.wfile.write(cls.payload)

        cls.server = HTTPServer(("127.0.0.1", 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        cls.server.socket = context.wrap_socket(cls.server.socket, server_side=True)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.env = {**os.environ, "CURL_CA_BUNDLE": str(cert)}
        cls.script = Path(__file__).with_name("fetch-tool.sh")

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()
        cls.temp.cleanup()

    def fetch(self, path, digest, destination):
        return subprocess.run(["sh", str(self.script), f"https://localhost:{self.server.server_port}{path}", digest, str(destination)],
                              env=self.env, capture_output=True, text=True, timeout=20)

    def test_504_retries_and_verified_cache_avoids_download(self):
        destination = self.base / "retry tool"
        digest = hashlib.sha256(self.payload).hexdigest()
        result = self.fetch("/retry", digest, destination)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.counts["/retry"], 2)
        self.assertEqual(destination.read_bytes(), self.payload)
        self.assertEqual(destination.stat().st_mode & 0o777, 0o755)
        self.assertEqual(self.fetch("/retry", digest, destination).returncode, 0)
        self.assertEqual(self.counts["/retry"], 2)
        self.assertFalse(list(self.base.glob("*.download.*")))

    def test_404_is_not_retried_and_preserves_old_destination(self):
        destination = self.base / "old-tool"
        destination.write_bytes(b"previous executable")
        result = self.fetch("/missing", hashlib.sha256(self.payload).hexdigest(), destination)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.counts["/missing"], 1)
        self.assertEqual(destination.read_bytes(), b"previous executable")
        self.assertFalse(list(self.base.glob("*.download.*")))

    def test_wrong_hash_is_never_installed_or_retried(self):
        destination = self.base / "must-not-exist"
        result = self.fetch("/wrong-hash", "0" * 64, destination)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.counts["/wrong-hash"], 1)
        self.assertFalse(destination.exists())
        self.assertFalse(list(self.base.glob("*.download.*")))


if __name__ == "__main__":
    unittest.main()
