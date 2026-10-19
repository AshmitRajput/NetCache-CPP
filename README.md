# NetCache

A small HTTP/1.1 caching proxy in C++17 with a deliberately narrow scope: `GET` only, one fixed
upstream, `Content-Length` framing, bounded memory. The goal is correctness you can explain, not
feature count.

**Status: Phase 1 of 3 (forwarding proxy, no cache yet).**

| Phase | Delivers | State |
|---|---|---|
| 1 | Hardened TCP/HTTP forwarding proxy, worker pool, load shedding, strict parser | done |
| 2 | Bounded LRU cache, injectable clock, request coalescing, stats | planned |
| 3 | Chunked forwarding (uncached), cost-aware eviction, reproducible benchmarks | planned |

## Build and test

```sh
cmake -S . -B build && cmake --build build -j
ctest --test-dir build --output-on-failure        # unit + integration

# Sanitizers
cmake -S . -B build-asan -DNETCACHE_SANITIZE=address,undefined && cmake --build build-asan -j
cmake -S . -B build-tsan -DNETCACHE_SANITIZE=thread           && cmake --build build-tsan -j
```

## Run

```sh
python3 tools/origin.py --port 9000 &
./build/netcache --port 8080 --upstream-port 9000
curl -i http://127.0.0.1:8080/hello
```

`./build/netcache --help` lists every option (workers, queue depth, timeouts, size limits).

## Architecture

```
accept (main thread) -> bounded queue -> N workers
worker: read head (deadline) -> parse/validate -> fetch upstream (deadline) -> validate -> relay
```

* Fixed worker pool, blocking-style code over non-blocking sockets; every wait is `poll()` against
  an absolute deadline, so no peer can hold a worker longer than the configured timeouts.
* Full queue -> immediate `503` (load is shed, never queued without bound).
* The upstream response is fully read and validated **before** a byte is sent to the client, so a
  truncated or malformed upstream response can never be relayed as a success.
* `src/http.*` is pure (no I/O) and unit-tested byte-at-a-time; `src/main.cpp` owns sockets.

## HTTP contract (Phase 1)

Principle: reject ambiguity before forwarding; never repair framing.

**Requests:** `GET` + `HTTP/1.1` + origin-form target only. Rejected: dot segments (also
`%2e`), `%00`, fragments, backslashes, `//` prefix, bare CR/LF, obs-fold, whitespace before the
colon, control characters, missing/duplicate `Host`, any `Transfer-Encoding`, any non-zero or
duplicate `Content-Length`, `Expect`, `Upgrade`. Limits: 8 KiB head, 64 headers, 2048-byte target.
Statuses: 400/405/408/414/417/431/501/505.

**Upstream request** is rebuilt from scratch: fixed `Host`, `Connection: close`, and at most one
client `Accept`. Cookies, `Authorization`, `Accept-Encoding`, etc. are never forwarded.

**Upstream response:** exactly one valid `Content-Length` (or 204/304), within
`--max-response-bytes`. Anything else, including `Transfer-Encoding`, 1xx, duplicate or conflicting
framing, truncated bodies, and garbage status lines, yields `502` (`504` on timeout). Hop-by-hop
headers are stripped; `Content-Length` is never rewritten. Extra bytes after the body are dropped.

## Known limitations (honest list)

* IPv4 listener only; Linux only (`accept4`, `MSG_NOSIGNAL`).
* `Connection: close` on both sides, so no keep-alive, and `HTTP/1.0` clients are rejected.
* Responses are buffered whole: worst-case in-flight memory is `workers x max-response-bytes`.
* A worker is occupied for the whole upstream fetch (and up to 200 ms of lingering close), so
  slow upstreams reduce capacity. Size `--workers` accordingly. This is the main cost of the
  thread-pool model and the motivation for an `epoll` variant.
* Graceful shutdown drains queued connections; it can take up to the request timeout if a client
  holds a connection open without sending anything.
* Duplicate `Content-Length` headers are rejected even when identical (stricter than the RFC).
* Under overload, a shed client may see a connection reset instead of the `503` (best effort).
* `%2f` in paths is not rejected; since the proxy forwards the target verbatim, this is a
  question for the origin, not a proxy-side ambiguity.
