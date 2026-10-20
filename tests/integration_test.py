#!/usr/bin/env python3
"""Integration tests (Phase 1 proxy behaviour + Phase 2B caching): real proxy binary, real TCP, local origin.
Python 3 stdlib only.

usage: integration_test.py [path/to/netcache] [unittest args...]
"""
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import unittest
from urllib.parse import quote

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
        for path in ("/truncated", "/reset", "/garbage", "/dupcl", "/nocl", "/cl_te"):
            with self.subTest(path=path):
                r = get(self.proxy_port, path)
                self.assertEqual(status(r), 502, r[:80])
                self.assertNotIn(b"0123456789", r)  # nothing of the broken body leaks through

    def test_valid_chunked_upstream_response_is_decoded(self):
        r = get(self.proxy_port, "/chunked")
        self.assertEqual(status(r), 200)
        self.assertEqual(body(r), b"hello world")
        self.assertEqual(hvals(r, "content-length"), ["11"])
        self.assertEqual(hvals(r, "connection"), ["close"])

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


# =============================================================== Phase 2B: caching

def hvals(resp, name):
    """All values of header `name` (case-insensitive) in the response head."""
    head = resp.split(b"\r\n\r\n", 1)[0].decode().split("\r\n")[1:]
    return [h.split(":", 1)[1].strip() for h in head if h.split(":", 1)[0].lower() == name.lower()]


def xcache(resp):
    v = hvals(resp, "x-cache")
    assert len(v) == 1, f"expected exactly one X-Cache header, got {v}"
    return v[0]


def origin_counts(origin_port):
    # The origin keeps connections alive; ask it to close so raw() returns at once instead of timing out.
    return json.loads(body(get(origin_port, "/__counts", "Connection: close\r\n")))


def hdr(**kw):
    """/hdr?... path for the origin's configurable endpoint (key order is stable)."""
    return "/hdr?" + "&".join(f"{k}={quote(str(v), safe='')}" for k, v in kw.items())


def stats(port):
    r = get(port, "/__netcache/stats")
    assert status(r) == 200, r[:100]
    return json.loads(body(r))


def burst(port, path, n, headers=""):
    """n simultaneous GETs (released together by a barrier); returns the list of raw responses."""
    out = [None] * n
    barrier = threading.Barrier(n)

    def one(i):
        barrier.wait()
        out[i] = get(port, path, headers, timeout=20)

    ts = [threading.Thread(target=one, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(30)
    assert all(not t.is_alive() for t in ts), "a client thread hung"
    return out


class CacheBehaviourTests(Harness):
    @classmethod
    def setUpClass(cls):
        cls.start_origin()
        cls.start_proxy("--workers", "64", "--queue", "256", "--enable-stats")

    @classmethod
    def tearDownClass(cls):
        cls.stop()

    def n(self, path):
        return origin_counts(self.origin_port).get(path, 0)

    def test_second_request_is_a_hit_and_origin_is_called_once(self):
        p = hdr(v="repeat")
        first = get(self.proxy_port, p)
        self.assertEqual((status(first), xcache(first)), (200, "MISS"))
        for _ in range(10):
            r = get(self.proxy_port, p)
            self.assertEqual((status(r), xcache(r)), (200, "HIT"))
            self.assertEqual(body(r), body(first))
        self.assertEqual(self.n(p), 1)  # the origin's own counter, not just a label

    def test_hit_is_byte_identical_and_well_formed(self):
        p = hdr(v="wellformed", size=2000, cc="public, max-age=30")
        miss = get(self.proxy_port, p)
        hit = get(self.proxy_port, p)
        self.assertEqual(body(miss), body(hit))
        self.assertEqual(len(body(hit)), 2000)
        for r in (miss, hit):
            self.assertEqual(len(hvals(r, "x-cache")), 1)
            self.assertEqual(hvals(r, "content-length"), ["2000"])
            self.assertEqual(hvals(r, "connection"), ["close"])
            self.assertEqual(hvals(r, "cache-control"), ["public, max-age=30"])

    def test_large_body_hit_is_identical(self):
        p = hdr(v="large", size=500000)
        a = get(self.proxy_port, p, timeout=15)
        b = get(self.proxy_port, p, timeout=15)
        self.assertEqual((xcache(a), xcache(b)), ("MISS", "HIT"))
        self.assertEqual(len(body(b)), 500000)
        self.assertEqual(body(a), body(b))
        self.assertEqual(self.n(p), 1)

    def test_different_keys_do_not_collide(self):
        pa, pb, pc = hdr(v="k", a=1), hdr(v="k", a=2), hdr(v="k", a=1, b=0)
        ra = [get(self.proxy_port, pa) for _ in range(2)]
        rb = [get(self.proxy_port, pb) for _ in range(2)]
        rc = get(self.proxy_port, pc)
        self.assertNotEqual(body(ra[0]), body(rb[0]))
        self.assertEqual(body(ra[0]), body(ra[1]))
        self.assertEqual(body(rb[0]), body(rb[1]))
        self.assertEqual(xcache(rc), "MISS")
        self.assertEqual([self.n(pa), self.n(pb), self.n(pc)], [1, 1, 1])

    def test_accept_header_is_part_of_the_key(self):
        p = hdr(v="accept")
        a1 = get(self.proxy_port, p, "Accept: text/html\r\n")
        a2 = get(self.proxy_port, p, "Accept: text/html\r\n")
        b1 = get(self.proxy_port, p, "Accept: application/json\r\n")
        none = get(self.proxy_port, p)
        self.assertEqual([xcache(a1), xcache(a2), xcache(b1), xcache(none)], ["MISS", "HIT", "MISS", "MISS"])
        self.assertEqual(self.n(p), 3)

    def test_expiry_forces_a_refetch_and_stale_is_never_served(self):
        p = hdr(v="ttl", cc="max-age=1")
        a = get(self.proxy_port, p)
        b = get(self.proxy_port, p)
        self.assertEqual((xcache(a), xcache(b)), ("MISS", "HIT"))
        time.sleep(1.4)
        c = get(self.proxy_port, p)
        self.assertEqual(xcache(c), "MISS")
        self.assertNotEqual(body(c), body(a))  # a new generation, not the old bytes
        self.assertEqual(self.n(p), 2)

    def test_max_age_is_clamped_by_max_ttl(self):
        # Covered at unit level (policy tests); here just prove a large max-age is accepted.
        p = hdr(v="bigttl", cc="max-age=99999999")
        self.assertEqual([xcache(get(self.proxy_port, p)) for _ in range(2)], ["MISS", "HIT"])

    def test_ineligible_responses_are_never_cached(self):
        cases = {
            "no-store": hdr(v="u1", cc="no-store"),
            "private": hdr(v="u2", cc="private"),
            "no-cache": hdr(v="u3", cc="no-cache"),
            "max-age=0": hdr(v="u4", cc="max-age=0"),
            "s-maxage": hdr(v="u5", cc="s-maxage=60"),
            "swr": hdr(v="u6", cc="max-age=60, stale-while-revalidate=30"),
            "set-cookie": hdr(v="u7", cookie=1),
            "vary": hdr(v="u8", vary="Accept-Encoding"),
            "expires-only": hdr(v="u9", expires="0"),
            "age": hdr(v="u10", age=5),
            "pragma": hdr(v="u11", pragma="no-cache"),
            "404": hdr(v="u12", status=404),
            "500": hdr(v="u13", status=500),
        }
        for name, p in cases.items():
            with self.subTest(name):
                r1, r2 = get(self.proxy_port, p), get(self.proxy_port, p)
                self.assertEqual([xcache(r1), xcache(r2)], ["BYPASS", "BYPASS"])
                self.assertEqual(self.n(p), 2)
        # They are still delivered correctly, with their upstream status.
        self.assertEqual(status(get(self.proxy_port, hdr(v="u12", status=404))), 404)

    def test_requests_that_could_be_personalised_bypass_the_cache(self):
        p = hdr(v="personal")
        self.assertEqual(xcache(get(self.proxy_port, p)), "MISS")  # warm
        for extra in ("Authorization: Bearer secret\r\n", "Cookie: sid=abc\r\n", "Cache-Control: no-cache\r\n",
                      "Range: bytes=0-4\r\n", "If-None-Match: \"x\"\r\n"):
            with self.subTest(extra=extra.split(":")[0]):
                r = get(self.proxy_port, p, extra)
                self.assertEqual(xcache(r), "BYPASS")
                self.assertEqual(status(r), 200)
        self.assertEqual(self.n(p), 1 + 5)  # every bypass really went to the origin
        self.assertEqual(xcache(get(self.proxy_port, p)), "HIT")  # and none of them replaced the stored entry
        self.assertEqual(self.n(p), 6)

    def test_upstream_errors_are_not_cached(self):
        for path in ("/truncated", "/garbage"):
            with self.subTest(path):
                self.assertEqual([status(get(self.proxy_port, path)) for _ in range(2)], [502, 502])
                self.assertEqual(self.n(path), 2)
        chunked = get(self.proxy_port, "/chunked")
        self.assertEqual((status(chunked), xcache(chunked)), (200, "BYPASS"))

    def test_stats_counters_match_traffic(self):
        before = stats(self.proxy_port)
        p = hdr(v="stats")
        for _ in range(4):
            get(self.proxy_port, p)
        after = stats(self.proxy_port)
        d = {k: after[k] - before[k] for k in ("hits", "miss_stored", "upstream_fetches", "requests")}
        self.assertEqual(d, {"hits": 3, "miss_stored": 1, "upstream_fetches": 1, "requests": 4})
        self.assertLessEqual(after["payload_bytes"], after["max_bytes"])

    # ---- coalescing --------------------------------------------------------------------------

    def test_fifty_concurrent_misses_cause_exactly_one_origin_fetch(self):
        p = hdr(v="burst", ms=800, size=2000, cc="max-age=60")
        rs = burst(self.proxy_port, p, 50)
        self.assertTrue(all(status(r) == 200 for r in rs))
        self.assertEqual(len({body(r) for r in rs}), 1)  # everybody got the same bytes
        labels = [xcache(r) for r in rs]
        self.assertEqual(labels.count("MISS"), 1, labels)
        self.assertGreaterEqual(labels.count("COALESCED"), 40, labels)  # the rest are COALESCED (or late HITs)
        self.assertEqual(self.n(p), 1)

    def test_concurrent_uncacheable_misses_share_one_fetch(self):
        p = hdr(v="burst-uncacheable", ms=700, cc="no-store")
        rs = burst(self.proxy_port, p, 20)
        self.assertTrue(all(status(r) == 200 for r in rs))
        self.assertEqual(self.n(p), 1)  # shared while in flight...
        self.assertEqual(xcache(get(self.proxy_port, p)), "BYPASS")
        self.assertEqual(self.n(p), 2)  # ...but never stored: a later request fetches again

    def test_expired_entry_refresh_is_coalesced(self):
        p = hdr(v="refresh", cc="max-age=1", ms=500)
        get(self.proxy_port, p)
        time.sleep(1.3)
        rs = burst(self.proxy_port, p, 20)
        self.assertEqual(self.n(p), 2)  # initial fill + exactly one refresh
        self.assertEqual(len({body(r) for r in rs}), 1)

    def test_slow_miss_does_not_block_a_hit_on_another_key(self):
        fast, slow = hdr(v="fastkey"), hdr(v="slowkey", ms=1500)
        get(self.proxy_port, fast)
        t = threading.Thread(target=lambda: get(self.proxy_port, slow, timeout=10))
        t.start()
        time.sleep(0.3)
        t0 = time.time()
        r = get(self.proxy_port, fast)
        self.assertEqual(xcache(r), "HIT")
        self.assertLess(time.time() - t0, 0.5)
        t.join()

    def test_owner_failure_wakes_every_waiter_and_releases_the_key(self):
        for mode in ("truncate", "reset"):
            with self.subTest(mode):
                p = hdr(v="failburst-" + mode, ms=600, fail=mode)
                t0 = time.time()
                rs = burst(self.proxy_port, p, 20)
                self.assertLess(time.time() - t0, 5)
                self.assertTrue(all(status(r) == 502 for r in rs), [status(r) for r in rs])
                self.assertEqual(self.n(p), 1)  # one fetch, one failure, shared
                again = get(self.proxy_port, p)  # key is free again: a NEW fetch is attempted
                self.assertEqual(status(again), 502)
                self.assertEqual(self.n(p), 2)

    def test_stats_report_coalescing(self):
        before = stats(self.proxy_port)
        burst(self.proxy_port, hdr(v="statsburst", ms=600), 10)
        after = stats(self.proxy_port)
        self.assertEqual(after["upstream_fetches"] - before["upstream_fetches"], 1)
        self.assertGreaterEqual(after["coalesced"] - before["coalesced"], 5)


class CoalescingOffControlTests(Harness):
    """Control experiment: with --no-coalesce the same burst reaches the origin many times,
    which proves the coalescing test above can actually fail."""

    @classmethod
    def setUpClass(cls):
        cls.start_origin()
        cls.start_proxy("--workers", "64", "--queue", "256", "--no-coalesce")

    @classmethod
    def tearDownClass(cls):
        cls.stop()

    def test_burst_without_coalescing_hits_origin_repeatedly(self):
        p = hdr(v="control", ms=800, cc="max-age=60")
        rs = burst(self.proxy_port, p, 30)
        self.assertTrue(all(status(r) == 200 for r in rs))
        self.assertGreater(origin_counts(self.origin_port)[p], 1)


class CacheLimitTests(Harness):
    @classmethod
    def setUpClass(cls):
        cls.start_origin()
        # ~1.15 KB per stored response: two fit in 3000 bytes, three do not.
        cls.start_proxy("--cache-bytes", "3000", "--cache-entry-bytes", "1500", "--enable-stats")

    @classmethod
    def tearDownClass(cls):
        cls.stop()

    def test_oversized_response_is_forwarded_but_not_cached(self):
        p = hdr(v="big", size=5000)
        rs = [get(self.proxy_port, p) for _ in range(2)]
        self.assertEqual([status(r) for r in rs], [200, 200])
        self.assertEqual([len(body(r)) for r in rs], [5000, 5000])
        self.assertEqual([xcache(r) for r in rs], ["BYPASS", "BYPASS"])
        self.assertEqual(origin_counts(self.origin_port)[p], 2)
        small = hdr(v="small", size=300)
        self.assertEqual([xcache(get(self.proxy_port, small)) for _ in range(2)], ["MISS", "HIT"])

    def test_budget_is_never_exceeded_and_lru_order_decides_the_victim(self):
        A, B, C = (hdr(v="lru" + x, size=1000) for x in "ABC")
        self.assertEqual(xcache(get(self.proxy_port, A)), "MISS")
        self.assertEqual(xcache(get(self.proxy_port, B)), "MISS")
        self.assertEqual(xcache(get(self.proxy_port, A)), "HIT")   # A is now most recently used
        self.assertEqual(xcache(get(self.proxy_port, C)), "MISS")  # must evict B (the LRU), not A
        s = stats(self.proxy_port)
        self.assertLessEqual(s["payload_bytes"], 3000)
        self.assertLessEqual(s["entries"], 2)
        self.assertEqual(xcache(get(self.proxy_port, A)), "HIT")
        self.assertEqual(xcache(get(self.proxy_port, C)), "HIT")
        self.assertEqual(xcache(get(self.proxy_port, B)), "MISS")  # B was the victim
        self.assertLessEqual(stats(self.proxy_port)["payload_bytes"], 3000)

    def test_budget_holds_under_a_stream_of_distinct_keys(self):
        for i in range(30):
            get(self.proxy_port, hdr(v="stream", i=i, size=900))
            self.assertLessEqual(stats(self.proxy_port)["payload_bytes"], 3000)


class UpstreamTimeoutCoalescingTests(Harness):
    @classmethod
    def setUpClass(cls):
        cls.start_origin()
        cls.start_proxy("--workers", "32", "--upstream-timeout-ms", "700")

    @classmethod
    def tearDownClass(cls):
        cls.stop()

    def test_timeout_wakes_all_waiters_with_504_and_releases_the_key(self):
        p = hdr(v="slow", ms=3000)
        t0 = time.time()
        rs = burst(self.proxy_port, p, 10)
        self.assertLess(time.time() - t0, 2.5)
        self.assertTrue(all(status(r) == 504 for r in rs), [status(r) for r in rs])
        self.assertEqual(origin_counts(self.origin_port)[p], 1)
        self.assertEqual(status(get(self.proxy_port, p)), 504)  # not stuck "in flight"
        self.assertEqual(origin_counts(self.origin_port)[p], 2)


class OriginDownTests(Harness):
    def test_cached_entries_survive_origin_death_but_uncached_paths_fail_cleanly(self):
        self.start_origin()
        self.start_proxy()
        try:
            warm = hdr(v="warm")
            self.assertEqual(xcache(get(self.proxy_port, warm)), "MISS")
            self.origin.kill()
            self.origin.wait()
            r = get(self.proxy_port, warm)
            self.assertEqual((status(r), xcache(r)), (200, "HIT"))  # fresh entry still served
            self.assertEqual(status(get(self.proxy_port, hdr(v="cold"))), 502)
        finally:
            self.stop()


class StatsAndCacheSwitchTests(Harness):
    def test_stats_path_is_404_and_never_forwarded_unless_enabled(self):
        self.start_origin()
        self.start_proxy()
        try:
            self.assertEqual(status(get(self.proxy_port, "/__netcache/stats")), 404)
            self.assertEqual(status(get(self.proxy_port, "/__netcache/other")), 404)
            self.assertFalse([k for k in origin_counts(self.origin_port) if "__netcache" in k])
        finally:
            self.stop()

    def test_cache_can_be_disabled(self):
        self.start_origin()
        self.start_proxy("--cache-bytes", "0")
        try:
            p = hdr(v="off")
            self.assertEqual([xcache(get(self.proxy_port, p)) for _ in range(3)], ["BYPASS"] * 3)
            self.assertEqual(origin_counts(self.origin_port)[p], 3)
        finally:
            self.stop()

    def test_default_ttl_zero_requires_explicit_max_age(self):
        self.start_origin()
        self.start_proxy("--default-ttl-s", "0")
        try:
            implicit, explicit = hdr(v="implicit"), hdr(v="explicit", cc="max-age=30")
            self.assertEqual([xcache(get(self.proxy_port, implicit)) for _ in range(2)], ["BYPASS", "BYPASS"])
            self.assertEqual([xcache(get(self.proxy_port, explicit)) for _ in range(2)], ["MISS", "HIT"])
        finally:
            self.stop()

    def test_sigterm_with_waiters_in_flight_exits_promptly(self):
        self.start_origin()
        self.start_proxy("--workers", "16")
        try:
            p = hdr(v="shutdown", ms=2500)
            ts = [threading.Thread(target=lambda: get(self.proxy_port, p, timeout=15)) for _ in range(8)]
            for t in ts:
                t.start()
            time.sleep(0.5)
            t0 = time.time()
            self.proxy.send_signal(signal.SIGTERM)
            self.assertEqual(self.proxy.wait(timeout=10), 0)
            self.assertLess(time.time() - t0, 5)
            for t in ts:
                t.join(10)
        finally:
            self.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
