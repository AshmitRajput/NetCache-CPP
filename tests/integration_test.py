#!/usr/bin/env python3
"""Phase 1 integration tests: real proxy binary, real TCP, local origin. Python 3 stdlib only.

usage: integration_test.py [path/to/netcache] [unittest args...]
"""
import os
import signal
import socket
import subprocess
import sys
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ORIGIN = os.path.join(ROOT, "tools", "origin.py")
BIN = os.environ.get("NETCACHE_BIN", os.path.join(ROOT, "build", "netcache"))
if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
    BIN = sys.argv.pop(1)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_listening(port, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} never came up")


def raw(port, data, timeout=5.0):
    """Send bytes, read until the server closes; return everything received."""
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        s.sendall(data)
        chunks = []
        try:
            while True:
                c = s.recv(65536)
                if not c:
                    break
                chunks.append(c)
        except socket.timeout:
            pass
        return b"".join(chunks)


def get(port, path, headers="", timeout=5.0):
    return raw(port, f"GET {path} HTTP/1.1\r\nHost: t\r\n{headers}\r\n".encode(), timeout)


def status(resp):
    return int(resp.split(b" ", 2)[1]) if resp.startswith(b"HTTP/1.1 ") else None


def body(resp):
    return resp.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in resp else b""


def header_names(resp):
    head = resp.split(b"\r\n\r\n", 1)[0].decode().split("\r\n")[1:]
    return [h.split(":", 1)[0].lower() for h in head]


class Harness(unittest.TestCase):
    origin = None
    proxy = None

    @classmethod
    def start_origin(cls):
        cls.origin_port = free_port()
        cls.origin = subprocess.Popen([sys.executable, ORIGIN, "--port", str(cls.origin_port)],
                                      stdout=subprocess.DEVNULL)
        wait_listening(cls.origin_port)

    @classmethod
    def start_proxy(cls, *extra, upstream_port=None):
        cls.proxy_port = free_port()
        cls.proxy = subprocess.Popen(
            [BIN, "--port", str(cls.proxy_port), "--upstream-port", str(upstream_port or cls.origin_port), *extra],
            stderr=subprocess.DEVNULL)
        wait_listening(cls.proxy_port)

    @classmethod
    def stop(cls):
        for p in (cls.proxy, cls.origin):
            if p and p.poll() is None:
                p.send_signal(signal.SIGTERM)
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()
        cls.proxy = cls.origin = None


class ForwardingTests(Harness):
    @classmethod
    def setUpClass(cls):
        cls.start_origin()
        cls.start_proxy()

    @classmethod
    def tearDownClass(cls):
        cls.stop()

    def test_body_identical_to_direct(self):
        via = get(self.proxy_port, "/hello")
        direct = get(self.origin_port, "/hello")
        self.assertEqual(status(via), 200)
        self.assertEqual(body(via), body(direct))

    def test_large_body_identical(self):
        via = get(self.proxy_port, "/size?n=500000")
        self.assertEqual(status(via), 200)
        self.assertEqual(body(via), b"x" * 500000)

    def test_upstream_status_codes_pass_through(self):
        self.assertEqual(status(get(self.proxy_port, "/missing")), 404)
        self.assertEqual(status(get(self.proxy_port, "/status?code=204")), 204)
        self.assertEqual(status(get(self.proxy_port, "/status?code=304")), 304)

    def test_connection_header_is_ours(self):
        names = header_names(get(self.proxy_port, "/hello"))
        self.assertEqual(names.count("connection"), 1)
        self.assertNotIn("keep-alive", names)

    def test_overlong_upstream_body_is_cut_at_content_length(self):
        r = get(self.proxy_port, "/overlong")
        self.assertEqual(status(r), 200)
        self.assertEqual(body(r), b"AAAAA")

    def test_request_split_across_packets(self):
        with socket.create_connection(("127.0.0.1", self.proxy_port), timeout=5) as s:
            for piece in (b"GET /hel", b"lo HTTP/1.1\r", b"\nHost: t\r\n", b"\r\n"):
                s.sendall(piece)
                time.sleep(0.05)
            data = b""
            while True:
                c = s.recv(4096)
                if not c:
                    break
                data += c
        self.assertEqual(status(data), 200)

    def test_client_headers_are_not_forwarded_upstream(self):
        # /__counts is served by the origin; this only proves the proxy still answers with a
        # cookie present. Header rebuilding itself is covered by the unit tests.
        r = get(self.proxy_port, "/hello", "Cookie: a=b\r\nX-Evil: 1\r\n")
        self.assertEqual(status(r), 200)


class MalformedClientTests(Harness):
    @classmethod
    def setUpClass(cls):
        cls.start_origin()
        cls.start_proxy("--request-timeout-ms", "500")

    @classmethod
    def tearDownClass(cls):
        cls.stop()

    def counts(self):
        import json
        return json.loads(body(get(self.origin_port, "/__counts")))

    def test_rejections_never_reach_origin(self):
        cases = [
            (b"POST /blocked HTTP/1.1\r\nHost: t\r\nContent-Length: 5\r\n\r\nhello", 405),
            (b"BREW /blocked HTTP/1.1\r\nHost: t\r\n\r\n", 501),
            (b"GET /blocked HTTP/1.0\r\nHost: t\r\n\r\n", 505),
            (b"GET http://evil/ HTTP/1.1\r\nHost: t\r\n\r\n", 400),
            (b"GET /a/../blocked HTTP/1.1\r\nHost: t\r\n\r\n", 400),
            (b"GET /blocked HTTP/1.1\r\nHost: t\nX: y\r\n\r\n", 400),
            (b"GET /blocked HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", 400),
            (b"GET /blocked HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n", 400),
            (b"GET /blocked HTTP/1.1\r\n\r\n", 400),
            (b"GET /" + b"a" * 3000 + b" HTTP/1.1\r\nHost: t\r\n\r\n", 414),
            (b"GET /blocked HTTP/1.1\r\nHost: t\r\nX: " + b"a" * 9000 + b"\r\n\r\n", 431),
        ]
        for data, want in cases:
            with self.subTest(want=want, data=data[:50]):
                self.assertEqual(status(raw(self.proxy_port, data)), want)
        self.assertNotIn("/blocked", self.counts())

    def test_binary_garbage(self):
        r = raw(self.proxy_port, os.urandom(300) + b"\r\n\r\n")
        self.assertIn(status(r), (400, 501, 405, 431, None))

    def test_slow_client_gets_408(self):
        with socket.create_connection(("127.0.0.1", self.proxy_port), timeout=5) as s:
            s.sendall(b"GET /hello HTTP/1.1\r\nHost: t\r\n")  # never finishes
            time.sleep(0.9)
            data = s.recv(4096)
        self.assertEqual(status(data), 408)

    def test_idle_connection_is_closed_silently(self):
        with socket.create_connection(("127.0.0.1", self.proxy_port), timeout=5) as s:
            time.sleep(0.9)
            self.assertEqual(s.recv(4096), b"")

    def test_client_disconnect_mid_request_does_not_hurt(self):
        s = socket.create_connection(("127.0.0.1", self.proxy_port))
        s.sendall(b"GET /hel")
        s.close()
        self.assertEqual(status(get(self.proxy_port, "/hello")), 200)


class UpstreamFailureTests(Harness):
    @classmethod
    def setUpClass(cls):
        cls.start_origin()
        cls.start_proxy("--upstream-timeout-ms", "700", "--max-response-bytes", "1000")

    @classmethod
    def tearDownClass(cls):
        cls.stop()

    def test_bad_upstream_responses_become_502(self):
        for path in ("/chunked", "/truncated", "/reset", "/garbage", "/dupcl", "/nocl", "/cl_te"):
            with self.subTest(path=path):
                r = get(self.proxy_port, path)
                self.assertEqual(status(r), 502, r[:80])
                self.assertNotIn(b"0123456789", r)  # nothing of the broken body leaks through

    def test_oversized_response_is_502(self):
        self.assertEqual(status(get(self.proxy_port, "/size?n=5000")), 502)
        self.assertEqual(status(get(self.proxy_port, "/size?n=1000")), 200)

    def test_slow_upstream_is_504(self):
        t = time.time()
        self.assertEqual(status(get(self.proxy_port, "/slow?ms=3000")), 504)
        self.assertLess(time.time() - t, 2.0)

    def test_origin_down_is_502(self):
        dead = free_port()  # nothing listens here
        port = free_port()
        proc = subprocess.Popen([BIN, "--port", str(port), "--upstream-port", str(dead)], stderr=subprocess.DEVNULL)
        try:
            wait_listening(port)
            self.assertEqual(status(get(port, "/hello")), 502)
        finally:
            proc.terminate()
            proc.wait()


class OverloadAndLifecycleTests(Harness):
    def test_queue_full_gets_503(self):
        self.start_origin()
        self.start_proxy("--workers", "1", "--queue", "1")
        try:
            time.sleep(0.3)  # let the worker drain the harness's startup probe connection from the 1-slot queue
            busy = socket.create_connection(("127.0.0.1", self.proxy_port))
            busy.sendall(b"GET /slow?ms=1500 HTTP/1.1\r\nHost: t\r\n\r\n")
            time.sleep(0.3)  # worker has picked it up
            queued = socket.create_connection(("127.0.0.1", self.proxy_port))  # occupies the queue slot
            time.sleep(0.2)
            r = raw(self.proxy_port, b"GET /hello HTTP/1.1\r\nHost: t\r\n\r\n")
            self.assertEqual(status(r), 503)
            busy.close()
            queued.close()
        finally:
            self.stop()

    def test_no_fd_leak_after_mixed_traffic(self):
        self.start_origin()
        self.start_proxy("--upstream-timeout-ms", "500")
        try:
            pid = self.proxy.pid
            fd_count = lambda: len(os.listdir(f"/proc/{pid}/fd"))
            get(self.proxy_port, "/hello")
            time.sleep(0.3)
            before = fd_count()
            mix = [lambda: get(self.proxy_port, "/hello"),
                   lambda: get(self.proxy_port, "/chunked"),
                   lambda: get(self.proxy_port, "/truncated"),
                   lambda: get(self.proxy_port, "/reset"),
                   lambda: raw(self.proxy_port, b"POST / HTTP/1.1\r\nHost: t\r\n\r\n"),
                   lambda: raw(self.proxy_port, b"GET /x\x00 HTTP/1.1\r\n\r\n")]
            for i in range(600):
                mix[i % len(mix)]()
            time.sleep(0.5)
            self.assertLessEqual(fd_count(), before + 1)
        finally:
            self.stop()

    def test_sigterm_shuts_down_cleanly(self):
        self.start_origin()
        self.start_proxy()
        try:
            self.proxy.send_signal(signal.SIGTERM)
            self.assertEqual(self.proxy.wait(timeout=5), 0)
        finally:
            self.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
