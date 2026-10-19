#!/usr/bin/env python3
"""Test origin for NetCache. Python 3 stdlib only.

Well-behaved endpoints:
  /hello              small fixed body
  /size?n=N           N bytes of 'x'
  /slow?ms=N          sleeps N ms, then replies
  /status?code=N      204 / 304 / 404 / 500 with the right framing
  /__counts           JSON: how many times each path was served (not itself counted)
  /__reset            clears the counters

Deliberately broken endpoints (used by the integration tests):
  /chunked            valid chunked response
  /truncated          Content-Length: 100 but only 10 bytes, then close
  /overlong           Content-Length: 5 but 20 bytes sent
  /reset              Content-Length: 100, 10 bytes, then TCP RST
  /garbage            not HTTP at all
  /dupcl              two Content-Length headers
  /nocl               body with no framing headers (close-delimited)
  /cl_te              Content-Length and Transfer-Encoding together
"""
import argparse
import json
import socket
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

COUNTS = {}
LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):  # keep test output quiet
        pass

    def _send(self, status, body=b"", content_type="text/plain"):
        self.send_response(status)
        if status not in (204, 304):
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if status not in (204, 304):
            self.wfile.write(body)

    def _raw(self, data):
        self.wfile.write(data)
        self.wfile.flush()
        self.close_connection = True

    def do_GET(self):
        url = urlsplit(self.path)
        q = parse_qs(url.query)
        path = url.path

        if path == "/__counts":
            with LOCK:
                return self._send(200, json.dumps(COUNTS).encode(), "application/json")
        if path == "/__reset":
            with LOCK:
                COUNTS.clear()
            return self._send(200, b"ok")

        with LOCK:
            COUNTS[path] = COUNTS.get(path, 0) + 1

        if path == "/hello":
            return self._send(200, b"hello from origin\n")
        if path == "/size":
            return self._send(200, b"x" * int(q.get("n", ["1024"])[0]))
        if path == "/slow":
            time.sleep(int(q.get("ms", ["1000"])[0]) / 1000)
            return self._send(200, b"slow reply\n")
        if path == "/status":
            return self._send(int(q.get("code", ["204"])[0]), b"")
        if path == "/chunked":
            return self._raw(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                             b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n")
        if path == "/truncated":
            return self._raw(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n0123456789")
        if path == "/overlong":
            return self._raw(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n" + b"A" * 20)
        if path == "/reset":
            self.wfile.write(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n0123456789")
            self.wfile.flush()
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            self.connection.close()  # RST
            self.close_connection = True
            return
        if path == "/garbage":
            return self._raw(b"THIS IS NOT HTTP\r\n\r\n")
        if path == "/dupcl":
            return self._raw(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello")
        if path == "/nocl":
            return self._raw(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nno framing here")
        if path == "/cl_te":
            return self._raw(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n"
                             b"5\r\nhello\r\n0\r\n\r\n")
        return self._send(404, b"not found\n")


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    args = ap.parse_args()
    srv = Server((args.host, args.port), Handler)
    print(f"origin listening on {args.host}:{args.port}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
