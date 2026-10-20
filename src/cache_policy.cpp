#include "cache_policy.hpp"

#include <algorithm>
#include <cstdint>
#include <string_view>
#include <vector>

namespace netcache {
namespace {

std::string lower(std::string_view s) {
    std::string out(s);
    for (char& c : out)
        if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
    return out;
}

std::string_view trim(std::string_view s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.remove_prefix(1);
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.remove_suffix(1);
    return s;
}

size_t countHeader(const std::vector<http::Header>& hs, std::string_view lname) {
    return static_cast<size_t>(
        std::count_if(hs.begin(), hs.end(), [&](const http::Header& h) { return lower(h.name) == lname; }));
}

const http::Header* findHeader(const std::vector<http::Header>& hs, std::string_view lname) {
    for (const auto& h : hs)
        if (lower(h.name) == lname) return &h;
    return nullptr;
}

CacheDecision no(std::string why) {
    CacheDecision d;
    d.cacheable = false;
    d.reason = std::move(why);
    return d;
}

// Digits only, no sign, no overflow past `cap` (saturates at cap + 1 to signal "too big").
bool parseSeconds(std::string_view s, std::uint64_t& out) {
    if (s.empty() || s.size() > 18) return false;  // 18 digits cannot overflow uint64_t
    std::uint64_t v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return false;
        v = v * 10 + static_cast<std::uint64_t>(c - '0');
    }
    out = v;
    return true;
}

}  // namespace

bool requestBypassesCache(const http::Request& request, std::string* reason) {
    static constexpr std::string_view kBypass[] = {"authorization", "proxy-authorization", "cookie", "range",
                                                   "if-match",      "if-none-match",       "if-modified-since",
                                                   "if-unmodified-since", "if-range",      "cache-control", "pragma"};
    for (const auto& h : request.headers) {
        std::string n = lower(h.name);
        if (std::find(std::begin(kBypass), std::end(kBypass), n) != std::end(kBypass)) {
            if (reason) *reason = "request has " + n;
            return true;
        }
    }
    return false;
}

CacheDecision evaluateResponse(const http::ResponseHead& head, std::size_t storedBytes, const CachePolicyConfig& cfg) {
    if (head.status != 200) return no("status " + std::to_string(head.status) + " is not cacheable");
    if (head.framing != http::Framing::ContentLength) return no("unsupported framing");

    for (const char* n : {"set-cookie", "set-cookie2", "vary", "www-authenticate", "age", "pragma", "content-range"}) {
        if (countHeader(head.headers, n) > 0) return no(std::string("response has ") + n);
    }

    const bool hasCacheControl = [&] {
        const http::Header* cc = findHeader(head.headers, "cache-control");
        return cc != nullptr;
    }();
    if (countHeader(head.headers, "cache-control") > 1) return no("multiple Cache-Control headers");

    std::chrono::seconds ttl = cfg.default_ttl;
    bool sawMaxAge = false;
    if (hasCacheControl) {
        const http::Header* cc = findHeader(head.headers, "cache-control");
        std::string_view v = cc->value;
        size_t pos = 0;
        bool sawPublic = false, sawRevalidate = false;
        while (true) {
            size_t comma = v.find(',', pos);
            std::string_view tok = trim(v.substr(pos, comma == std::string_view::npos ? v.size() - pos : comma - pos));
            std::string lt = lower(tok);
            if (lt == "public") {
                if (sawPublic) return no("duplicate Cache-Control directive");
                sawPublic = true;
            } else if (lt == "must-revalidate") {
                if (sawRevalidate) return no("duplicate Cache-Control directive");
                sawRevalidate = true;  // we never serve stale, so this is trivially satisfied
            } else if (lt.rfind("max-age=", 0) == 0) {
                if (sawMaxAge) return no("duplicate max-age");
                std::uint64_t secs = 0;
                if (!parseSeconds(lt.substr(8), secs)) return no("malformed max-age");  // also rejects quoted values
                sawMaxAge = true;
                ttl = std::chrono::seconds(static_cast<std::chrono::seconds::rep>(std::min<std::uint64_t>(secs, 1ULL << 40)));
            } else {
                return no("unsupported Cache-Control directive '" + lt + "'");  // includes empty tokens
            }
            if (comma == std::string_view::npos) break;
            pos = comma + 1;
        }
    }

    if (findHeader(head.headers, "expires") != nullptr && !sawMaxAge) return no("Expires without max-age is unsupported");
    if (countHeader(head.headers, "expires") > 1) return no("multiple Expires headers");

    if (ttl <= std::chrono::seconds::zero()) return no(sawMaxAge ? "max-age=0" : "no explicit freshness and default TTL is 0");
    if (ttl > cfg.max_ttl) ttl = cfg.max_ttl;
    if (ttl <= std::chrono::seconds::zero()) return no("max_ttl is 0");
    if (storedBytes > cfg.max_entry_bytes) return no("response larger than max entry size");

    CacheDecision d;
    d.cacheable = true;
    d.ttl = ttl;
    d.reason = "ok";
    return d;
}

}  // namespace netcache
