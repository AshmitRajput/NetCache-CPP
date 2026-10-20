// Socket-free table tests for cache eligibility (src/cache_policy.cpp).

#include "cache_policy.hpp"
#include "http.hpp"
#include "test_harness.hpp"

#include <string>
#include <vector>

using namespace netcache;

namespace {

http::ResponseHead respHead(const std::string& statusLine, const std::string& headers) {
    auto r = http::parseResponseHead(statusLine + "\r\n" + headers + "\r\n");
    if (r.state != http::ParseState::Complete) throw th::Failure{"test setup: response head did not parse: " + statusLine};
    return r.head;
}

http::Request reqWith(const std::string& headers) {
    auto r = http::parseRequest("GET /x HTTP/1.1\r\nHost: a\r\n" + headers + "\r\n");
    if (r.state != http::ParseState::Complete) throw th::Failure{"test setup: request did not parse: " + headers};
    return r.request;
}

CacheDecision decide(const std::string& headers, const std::string& status = "HTTP/1.1 200 OK",
                     const CachePolicyConfig& cfg = {}, size_t stored = 100) {
    return evaluateResponse(respHead(status, "Content-Length: 5\r\n" + headers), stored, cfg);
}

}  // namespace

TEST(plain_200_uses_default_ttl) {
    auto d = decide("");
    CHECK(d.cacheable);
    CHECK_EQ(d.ttl.count(), 60);
}

TEST(max_age_sets_ttl_and_is_clamped) {
    CHECK_EQ(decide("Cache-Control: max-age=30\r\n").ttl.count(), 30);
    CHECK_EQ(decide("Cache-Control: public, max-age=5\r\n").ttl.count(), 5);
    CHECK_EQ(decide("Cache-Control: MAX-AGE=7, Must-Revalidate\r\n").ttl.count(), 7);  // case-insensitive
    CHECK_EQ(decide("Cache-Control: max-age=999999\r\n").ttl.count(), 3600);           // clamped to max_ttl
    CHECK_EQ(decide("Cache-Control: max-age=99999999999999999\r\n").ttl.count(), 3600); // huge, no overflow
}

TEST(max_age_wins_over_expires_but_expires_alone_is_unsupported) {
    CHECK(decide("Cache-Control: max-age=10\r\nExpires: Thu, 01 Jan 2037 00:00:00 GMT\r\n").cacheable);
    CHECK(!decide("Expires: Thu, 01 Jan 2037 00:00:00 GMT\r\n").cacheable);
    CHECK(!decide("Expires: 0\r\n").cacheable);
}

TEST(default_ttl_zero_requires_explicit_max_age) {
    CachePolicyConfig cfg;
    cfg.default_ttl = std::chrono::seconds(0);
    CHECK(!decide("", "HTTP/1.1 200 OK", cfg).cacheable);
    CHECK(decide("Cache-Control: max-age=10\r\n", "HTTP/1.1 200 OK", cfg).cacheable);
}

TEST(uncacheable_responses_table) {
    struct Case {
        const char* name;
        std::string status;
        std::string headers;
    };
    const std::vector<Case> cases = {
        {"no-store", "HTTP/1.1 200 OK", "Cache-Control: no-store\r\n"},
        {"private", "HTTP/1.1 200 OK", "Cache-Control: private\r\n"},
        {"no-cache", "HTTP/1.1 200 OK", "Cache-Control: no-cache\r\n"},
        {"max-age=0", "HTTP/1.1 200 OK", "Cache-Control: max-age=0\r\n"},
        {"no-store mixed with max-age", "HTTP/1.1 200 OK", "Cache-Control: max-age=60, no-store\r\n"},
        {"s-maxage unsupported", "HTTP/1.1 200 OK", "Cache-Control: s-maxage=60\r\n"},
        {"stale-while-revalidate unsupported", "HTTP/1.1 200 OK", "Cache-Control: max-age=60, stale-while-revalidate=30\r\n"},
        {"immutable unsupported", "HTTP/1.1 200 OK", "Cache-Control: max-age=60, immutable\r\n"},
        {"unknown directive", "HTTP/1.1 200 OK", "Cache-Control: max-age=60, banana\r\n"},
        {"empty Cache-Control", "HTTP/1.1 200 OK", "Cache-Control:\r\n"},
        {"empty token", "HTTP/1.1 200 OK", "Cache-Control: public,\r\n"},
        {"quoted max-age", "HTTP/1.1 200 OK", "Cache-Control: max-age=\"60\"\r\n"},
        {"negative max-age", "HTTP/1.1 200 OK", "Cache-Control: max-age=-1\r\n"},
        {"non-numeric max-age", "HTTP/1.1 200 OK", "Cache-Control: max-age=abc\r\n"},
        {"empty max-age", "HTTP/1.1 200 OK", "Cache-Control: max-age=\r\n"},
        {"duplicate max-age", "HTTP/1.1 200 OK", "Cache-Control: max-age=5, max-age=5\r\n"},
        {"two Cache-Control headers", "HTTP/1.1 200 OK", "Cache-Control: max-age=5\r\nCache-Control: public\r\n"},
        {"Set-Cookie", "HTTP/1.1 200 OK", "Set-Cookie: a=b\r\n"},
        {"Set-Cookie even with max-age", "HTTP/1.1 200 OK", "Cache-Control: max-age=60\r\nSet-Cookie: a=b\r\n"},
        {"Vary", "HTTP/1.1 200 OK", "Vary: Accept-Encoding\r\n"},
        {"Vary star", "HTTP/1.1 200 OK", "Vary: *\r\n"},
        {"Age header", "HTTP/1.1 200 OK", "Age: 10\r\n"},
        {"Pragma", "HTTP/1.1 200 OK", "Pragma: no-cache\r\n"},
        {"WWW-Authenticate", "HTTP/1.1 200 OK", "WWW-Authenticate: Basic\r\n"},
        {"404", "HTTP/1.1 404 Not Found", ""},
        {"500", "HTTP/1.1 500 Internal Server Error", "Cache-Control: max-age=60\r\n"},
        {"301", "HTTP/1.1 301 Moved", ""},
        {"206", "HTTP/1.1 206 Partial Content", "Content-Range: bytes 0-4/10\r\n"},
    };
    for (const auto& c : cases) {
        auto d = decide(c.headers, c.status);
        if (d.cacheable) throw th::Failure{std::string("case \"") + c.name + "\" was judged cacheable"};
        CHECK(!d.reason.empty());
    }
}

TEST(no_body_responses_are_not_cacheable) {
    auto h = respHead("HTTP/1.1 204 No Content", "Date: x\r\n");
    CHECK(!evaluateResponse(h, 50, {}).cacheable);
    auto h2 = respHead("HTTP/1.1 200 OK", "Content-Length: 0\r\n");
    CHECK(evaluateResponse(h2, 50, {}).cacheable);  // an empty 200 is a normal, cacheable response
}

TEST(entry_size_limit_is_inclusive) {
    CachePolicyConfig cfg;
    cfg.max_entry_bytes = 100;
    CHECK(decide("", "HTTP/1.1 200 OK", cfg, 100).cacheable);
    CHECK(!decide("", "HTTP/1.1 200 OK", cfg, 101).cacheable);
}

TEST(request_bypass_table) {
    for (const char* h : {"Authorization: Bearer x\r\n", "authorization: x\r\n", "Cookie: a=b\r\n", "Proxy-Authorization: x\r\n",
                          "Range: bytes=0-1\r\n", "If-None-Match: \"x\"\r\n", "If-Modified-Since: Thu, 01 Jan 2037 00:00:00 GMT\r\n",
                          "If-Match: *\r\n", "If-Range: x\r\n", "If-Unmodified-Since: x\r\n", "Cache-Control: no-cache\r\n",
                          "Cache-Control: max-age=0\r\n", "Pragma: no-cache\r\n"}) {
        std::string why;
        if (!requestBypassesCache(reqWith(h), &why)) throw th::Failure{std::string("not bypassed: ") + h};
        CHECK(!why.empty());
    }
    CHECK(!requestBypassesCache(reqWith("")));
    CHECK(!requestBypassesCache(reqWith("Accept: text/html\r\nX-Anything: 1\r\n")));
}

int main() { return th::runAll(); }
