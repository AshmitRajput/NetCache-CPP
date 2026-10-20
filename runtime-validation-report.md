# Runtime Validation Report

**Generated:** 2026-10-20  
**Target:** `NetCacheC++`

## Summary

| Step | Status | Exit code | Details |
|---|---|---:|---|
| Linux CMake build | PASS | 0 | GCC 13.2.1; RelWithDebInfo build |
| Unit and integration tests | PASS | 0 | 6/6 CTest targets passed; 34.43 seconds |
| Benchmark harness | PASS | 0 | 4 configurations, 3 seeds each, 600 requests/configuration |

## Test evidence

The complete CTest run passed:

- `http_unit`
- `cache_unit`
- `eviction_unit`
- `coalescing_unit`
- `cache_policy_unit`
- `integration`

The integration suite exercised forwarding, request validation, malformed upstream framing,
valid chunked decoding, cache hits/misses, TTL expiry, bypass rules, request coalescing,
coalescing failure paths, cache limits, queue overload, file-descriptor behavior, and shutdown.

## Benchmark evidence

Command shape:

```sh
python3 tools/bench.py --port 18080 --requests 200 --keys 20 --seed <seed>
```

Each configuration used seeds `20261021`, `20261022`, and `20261023`, with 200 requests per seed.
The proxy used 64 workers, a 256-request queue, a 5,000-byte cache budget, and a 1,000-byte
per-entry limit.

| Configuration | Mean latency | p95 range | Serial-equivalent req/s* | Hit rate | Errors |
|---|---:|---:|---:|---:|---:|
| LRU | 0.522 ms | 1.219–1.846 ms | ~1,916 | 88.5% | 0 |
| Cost-aware | 0.448 ms | 1.008–1.701 ms | ~2,232 | 89.7% | 0 |
| LRU without coalescing | 0.557 ms | 1.487–1.972 ms | ~1,795 | 88.5% | 0 |
| Cache disabled | 1.267 ms | 1.640–1.927 ms | ~789 | 0% | 0 |

\* Reciprocal of mean latency; not a concurrent throughput ceiling.

The benchmark is a local comparative workload and should not be presented as a production capacity
claim. The cost-aware policy was not tested with sufficiently varied fetch costs to establish a
general advantage over LRU.

## Environment and gaps

- Docker CLI was installed, but the Docker daemon was unavailable.
- Docker Desktop's Linux WSL environment was available and used as the validation host.
- Linux build: CMake with GCC 13.2.1.
- Python: 3.12 in the Linux validation environment; Windows host Python 3.14.5 also passed AST syntax checks.
- Node.js was available, but browser E2E is not applicable because NetCache is an HTTP proxy with no rendered UI.
- IPv6 and non-Python origin implementations remain untested.

## Verdict

**Overall: PASS** — the Linux build, all configured unit/integration tests, and reproducible
benchmark runs completed successfully with exit code 0.
