#pragma once
// Phase 2B: which requests may use the cache and which responses may be stored.
//
// Pure functions over the already-validated Phase 1 types (no I/O, no locks), so every rule is
// table-testable. Principle: cache only what is EXPLICITLY eligible; anything unrecognised,
// malformed or ambiguous is uncacheable (fail safe). This is a deliberately small subset of
// RFC 9111, not an implementation of it.

#include <chrono>
#include <cstddef>
#include <string>

#include "http.hpp"

namespace netcache {

struct CachePolicyConfig {
    std::chrono::seconds default_ttl{60};  // used when the response has no Cache-Control; 0 = require max-age
    std::chrono::seconds max_ttl{3600};    // upper clamp for max-age and default_ttl
    std::size_t max_entry_bytes = 2 * 1024 * 1024;  // limit on the stored (serialized) response
};

// True when the request must skip the cache AND request coalescing entirely (it is still
// forwarded, as `X-Cache: BYPASS`). Triggers: Authorization, Proxy-Authorization, Cookie, Range,
// If-* conditionals, Cache-Control, Pragma. `reason` gets a short log string.
bool requestBypassesCache(const http::Request& request, std::string* reason = nullptr);

struct CacheDecision {
    bool cacheable = false;
    std::chrono::seconds ttl{0};  // valid only when cacheable
    std::string reason;           // why not (or "ok"), for logs and tests
};

// `storedBytes` is the size of what would actually be stored (the serialized response).
// Cacheable only if ALL hold: status 200; Content-Length framing; no Set-Cookie/Set-Cookie2, Vary,
// WWW-Authenticate, Age, Pragma, Content-Range; at most one Cache-Control header whose directives
// are a subset of {public, max-age=N, must-revalidate} (no duplicates; no-store, no-cache,
// private, s-maxage, stale-*, immutable, unknown directives, or malformed values => uncacheable);
// Expires is only tolerated alongside max-age (it is otherwise unparsed => uncacheable);
// max-age > 0 (or, with no Cache-Control, default_ttl > 0); storedBytes <= max_entry_bytes.
// TTL = max-age (else default_ttl), clamped to max_ttl.
CacheDecision evaluateResponse(const http::ResponseHead& head, std::size_t storedBytes, const CachePolicyConfig& cfg);

}  // namespace netcache
