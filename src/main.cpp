// NetCache, Phase 2B: hardened forwarding proxy + bounded LRU cache + per-key request coalescing.
//
// Threading model: one acceptor (main thread) -> bounded queue -> fixed pool of workers.
// Each worker owns one connection end to end. All sockets are non-blocking and every wait
// goes through poll() against an absolute deadline, so a slow peer can never hold a worker
// longer than the configured timeouts (slowloris mitigation).
// Linux only (accept4, MSG_NOSIGNAL).

#include "cache.hpp"
#include "cache_policy.hpp"
#include "coalescing_cache.hpp"
#include "http.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cctype>
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
using Ms = std::chrono::milliseconds;

volatile std::sig_atomic_t g_stop = 0;
void onSignal(int) { g_stop = 1; }

// ------------------------------------------------------------------ config

struct Config {
    std::string bindAddress = "127.0.0.1";  // IPv4 literal; loopback by default (safe default)
    uint16_t listenPort = 8080;
    std::string upstreamHost = "127.0.0.1";
    uint16_t upstreamPort = 9000;
    size_t workers = 8;
    size_t queueCapacity = 64;
    unsigned requestTimeoutMs = 5000;    // whole request head must arrive within this
    unsigned upstreamTimeoutMs = 10000;  // connect + send + full response
    unsigned writeTimeoutMs = 10000;     // sending the response to the client
    size_t maxResponseBytes = 1024 * 1024;
    // Phase 2B
    size_t cacheBytes = 16 * 1024 * 1024;       // payload budget; 0 disables the cache entirely
    size_t cacheEntryBytes = 2 * 1024 * 1024;   // largest stored (serialized) response
    unsigned defaultTtlS = 60;                  // when the response has no Cache-Control; 0 = require max-age
    unsigned maxTtlS = 3600;
    unsigned coalesceWaitMs = 0;                // 0 = upstream timeout + 2000
    bool enableStats = false;
    bool coalesce = true;
    netcache::EvictionKind eviction = netcache::EvictionKind::Lru;
};

void printUsage(const char* prog) {
    std::fprintf(stderr,
                 "usage: %s [options]\n"
                 "  --bind ADDR              IPv4 address to listen on (default 127.0.0.1)\n"
                 "  --port N                 listen port (default 8080)\n"
                 "  --upstream-host HOST     (default 127.0.0.1)\n"
                 "  --upstream-port N        (default 9000)\n"
                 "  --workers N              worker threads, 1-256 (default 8)\n"
                 "  --queue N                pending-connection queue, 1-65536 (default 64)\n"
                 "  --request-timeout-ms N   (default 5000)\n"
                 "  --upstream-timeout-ms N  (default 10000)\n"
                 "  --write-timeout-ms N     (default 10000)\n"
                 "  --max-response-bytes N   (default 1048576)\n"
                 "  --cache-bytes N          cache payload budget in bytes; 0 disables caching (default 16777216)\n"
                 "  --cache-entry-bytes N    largest cacheable stored response (default 2097152)\n"
                 "  --default-ttl-s N        TTL when no Cache-Control; 0 = only cache explicit max-age (default 60)\n"
                 "  --max-ttl-s N            clamp for any TTL (default 3600)\n"
                 "  --coalesce-wait-ms N     max time a request waits for another's fetch (default upstream timeout + 2000)\n"
                 "  --no-coalesce            disable request coalescing (for experiments)\n"
                 "  --eviction KIND         lru or cost-aware (default lru)\n"
                 "  --enable-stats           serve JSON counters at /__netcache/stats (off by default)\n",
                 prog);
}

bool parseUint(const char* s, unsigned long long lo, unsigned long long hi, unsigned long long& out) {
    if (s == nullptr || *s < '0' || *s > '9') return false;
    char* end = nullptr;
    errno = 0;
    unsigned long long v = std::strtoull(s, &end, 10);
    if (errno != 0 || *end != '\0' || v < lo || v > hi) return false;
    out = v;
    return true;
}

bool parseArgs(int argc, char** argv, Config& cfg) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> const char* { return i + 1 < argc ? argv[++i] : nullptr; };
        unsigned long long v = 0;
        bool ok = true;
        if (a == "--help" || a == "-h") {
            return false;
        } else if (a == "--bind") {
            const char* s = next();
            ok = s != nullptr;
            if (ok) cfg.bindAddress = s;
        } else if (a == "--upstream-host") {
            const char* s = next();
            ok = s != nullptr && *s != '\0';
            if (ok) cfg.upstreamHost = s;
        } else if (a == "--port") {
            ok = parseUint(next(), 1, 65535, v);
            cfg.listenPort = static_cast<uint16_t>(v);
        } else if (a == "--upstream-port") {
            ok = parseUint(next(), 1, 65535, v);
            cfg.upstreamPort = static_cast<uint16_t>(v);
        } else if (a == "--workers") {
            ok = parseUint(next(), 1, 256, v);
            cfg.workers = static_cast<size_t>(v);
        } else if (a == "--queue") {
            ok = parseUint(next(), 1, 65536, v);
            cfg.queueCapacity = static_cast<size_t>(v);
        } else if (a == "--request-timeout-ms") {
            ok = parseUint(next(), 50, 600000, v);
            cfg.requestTimeoutMs = static_cast<unsigned>(v);
        } else if (a == "--upstream-timeout-ms") {
            ok = parseUint(next(), 50, 600000, v);
            cfg.upstreamTimeoutMs = static_cast<unsigned>(v);
        } else if (a == "--write-timeout-ms") {
            ok = parseUint(next(), 50, 600000, v);
            cfg.writeTimeoutMs = static_cast<unsigned>(v);
        } else if (a == "--max-response-bytes") {
            ok = parseUint(next(), 1, 1ULL << 30, v);
            cfg.maxResponseBytes = static_cast<size_t>(v);
        } else if (a == "--cache-bytes") {
            ok = parseUint(next(), 0, 1ULL << 36, v);
            cfg.cacheBytes = static_cast<size_t>(v);
        } else if (a == "--cache-entry-bytes") {
            ok = parseUint(next(), 1, 1ULL << 30, v);
            cfg.cacheEntryBytes = static_cast<size_t>(v);
        } else if (a == "--default-ttl-s") {
            ok = parseUint(next(), 0, 86400 * 365, v);
            cfg.defaultTtlS = static_cast<unsigned>(v);
        } else if (a == "--max-ttl-s") {
            ok = parseUint(next(), 1, 86400 * 365, v);
            cfg.maxTtlS = static_cast<unsigned>(v);
        } else if (a == "--coalesce-wait-ms") {
            ok = parseUint(next(), 50, 3600000, v);
            cfg.coalesceWaitMs = static_cast<unsigned>(v);
        } else if (a == "--no-coalesce") {
            cfg.coalesce = false;
        } else if (a == "--eviction") {
            const char* s = next();
            ok = s != nullptr;
            if (ok && std::string(s) == "lru") cfg.eviction = netcache::EvictionKind::Lru;
            else if (ok && (std::string(s) == "cost" || std::string(s) == "cost-aware"))
                cfg.eviction = netcache::EvictionKind::CostAware;
            else ok = false;
        } else if (a == "--enable-stats") {
            cfg.enableStats = true;
        } else {
            std::fprintf(stderr, "unknown option: %s\n", a.c_str());
            return false;
        }
        if (!ok) {
            std::fprintf(stderr, "invalid or missing value for %s\n", a.c_str());
            return false;
        }
    }
    return true;
}

// -------------------------------------------------------------- fd helpers

class FdGuard {
public:
    explicit FdGuard(int fd = -1) : fd_(fd) {}
    ~FdGuard() {
        if (fd_ >= 0) ::close(fd_);
    }
    FdGuard(const FdGuard&) = delete;
    FdGuard& operator=(const FdGuard&) = delete;
    int get() const { return fd_; }
    int release() {
        int f = fd_;
        fd_ = -1;
        return f;
    }

private:
    int fd_;
};

enum class Wait { Ready, Timeout, Error };

// POLLERR/POLLHUP count as Ready so the following recv/send reports the real error.
Wait waitFd(int fd, short events, Clock::time_point deadline) {
    for (;;) {
        auto now = Clock::now();
        if (now >= deadline) return Wait::Timeout;
        auto remaining = std::chrono::duration_cast<Ms>(deadline - now).count() + 1;
        pollfd p{fd, events, 0};
        int r = ::poll(&p, 1, static_cast<int>(std::min<long long>(remaining, INT_MAX)));
        if (r > 0) return (p.revents & POLLNVAL) ? Wait::Error : Wait::Ready;
        if (r < 0 && errno != EINTR) return Wait::Error;
    }
}

enum class Io { Ok, Closed, Timeout, Error };

Io recvSome(int fd, char* buf, size_t cap, Clock::time_point deadline, size_t& n) {
    for (;;) {
        Wait w = waitFd(fd, POLLIN, deadline);
        if (w == Wait::Timeout) return Io::Timeout;
        if (w == Wait::Error) return Io::Error;
        ssize_t r = ::recv(fd, buf, cap, 0);
        if (r > 0) {
            n = static_cast<size_t>(r);
            return Io::Ok;
        }
        if (r == 0) return Io::Closed;
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
        return Io::Error;
    }
}

bool sendAll(int fd, std::string_view data, Clock::time_point deadline) {
    size_t sent = 0;
    while (sent < data.size()) {
        if (waitFd(fd, POLLOUT, deadline) != Wait::Ready) return false;
        ssize_t r = ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (r > 0) {
            sent += static_cast<size_t>(r);
        } else if (r < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            return false;
        }
    }
    return true;
}

// Close without destroying an in-flight response: closing a socket that still has unread
// input makes the kernel send RST, which can discard data the peer has not read yet.
// So: send FIN, then briefly drain whatever the peer is still sending.
void lingeringClose(FdGuard& fd) {
    ::shutdown(fd.get(), SHUT_WR);
    auto deadline = Clock::now() + Ms(200);
    char sink[4096];
    size_t total = 0;
    size_t n = 0;
    while (total < 64 * 1024 && recvSome(fd.get(), sink, sizeof sink, deadline, n) == Io::Ok) total += n;
}

// ---------------------------------------------------------------- upstream

struct ResolvedAddr {
    int family = AF_UNSPEC;
    sockaddr_storage storage{};
    socklen_t len = 0;
};

std::vector<ResolvedAddr> resolveUpstream(const std::string& host, uint16_t port, std::string& err) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    int rc = ::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res);
    if (rc != 0) {
        err = ::gai_strerror(rc);
        return {};
    }
    std::vector<ResolvedAddr> out;
    for (addrinfo* p = res; p != nullptr; p = p->ai_next) {
        ResolvedAddr a;
        a.family = p->ai_family;
        a.len = p->ai_addrlen;
        std::memcpy(&a.storage, p->ai_addr, p->ai_addrlen);
        out.push_back(a);
    }
    ::freeaddrinfo(res);
    return out;
}

// Returns a connected non-blocking fd, or -1 (timedOut tells deadline expiry from other failures).
int connectUpstream(const std::vector<ResolvedAddr>& addrs, Clock::time_point deadline, bool& timedOut,
                    std::string& detail) {
    timedOut = false;
    for (const auto& a : addrs) {
        FdGuard fd(::socket(a.family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
        if (fd.get() < 0) {
            detail = std::string("socket: ") + std::strerror(errno);
            continue;
        }
        int rc = ::connect(fd.get(), reinterpret_cast<const sockaddr*>(&a.storage), a.len);
        if (rc != 0 && errno != EINPROGRESS) {
            detail = std::string("connect: ") + std::strerror(errno);
            continue;
        }
        if (rc != 0) {
            Wait w = waitFd(fd.get(), POLLOUT, deadline);
            if (w == Wait::Timeout) {
                timedOut = true;
                detail = "connect timed out";
                return -1;
            }
            if (w == Wait::Error) {
                detail = "poll failed during connect";
                continue;
            }
            int soErr = 0;
            socklen_t len = sizeof soErr;
            if (::getsockopt(fd.get(), SOL_SOCKET, SO_ERROR, &soErr, &len) != 0 || soErr != 0) {
                detail = std::string("connect: ") + std::strerror(soErr != 0 ? soErr : errno);
                continue;
            }
        }
        return fd.release();
    }
    if (detail.empty()) detail = "no usable upstream address";
    return -1;
}

enum class FetchStatus { Ok, Timeout, Failure };

struct FetchResult {
    FetchStatus status = FetchStatus::Failure;
    std::string detail;  // logs only
    netcache::http::ResponseHead head;
    std::string body;  // complete: exactly head.contentLength bytes when status == Ok
};

struct UpstreamContext {
    Config cfg;
    std::vector<ResolvedAddr> addrs;
    std::string authority;  // value of the Host header sent upstream
    // Phase 2B. cache/fetcher are null when --cache-bytes 0 (every request is then a BYPASS).
    netcache::CachePolicyConfig policy;
    std::unique_ptr<netcache::Cache> cache;
    std::unique_ptr<netcache::CoalescingCache> fetcher;
    std::chrono::milliseconds coalesceWait{12000};
    std::atomic<std::uint64_t> bypasses{0};
    std::atomic<std::uint64_t> requests{0};
};

// The whole response is buffered and validated BEFORE anything is sent to the client, so a
// truncated or malformed upstream response can never be relayed as if it were a success.
FetchResult fetchUpstream(const UpstreamContext& up, const netcache::http::Request& request) {
    namespace http = netcache::http;
    FetchResult res;
    auto deadline = Clock::now() + Ms(up.cfg.upstreamTimeoutMs);
    auto timedOutResult = [&res](const char* what) {
        res.status = FetchStatus::Timeout;
        res.detail = what;
        return res;
    };
    auto failure = [&res](std::string what) {
        res.status = FetchStatus::Failure;
        res.detail = std::move(what);
        return res;
    };

    bool connectTimedOut = false;
    std::string detail;
    FdGuard fd(connectUpstream(up.addrs, deadline, connectTimedOut, detail));
    if (fd.get() < 0) return connectTimedOut ? timedOutResult("upstream connect timed out") : failure(detail);

    if (!sendAll(fd.get(), http::buildUpstreamRequest(request, up.authority), deadline)) {
        return Clock::now() >= deadline ? timedOutResult("upstream send timed out") : failure("upstream send failed");
    }

    http::ResponseLimits limits;
    limits.maxBodyBytes = up.cfg.maxResponseBytes;

    std::string buf;
    char tmp[8192];
    http::ResponseParseResult pr;
    for (;;) {
        pr = http::parseResponseHead(buf, limits);
        if (pr.state == http::ParseState::Error) return failure(pr.errorDetail);
        if (pr.state == http::ParseState::Complete) break;
        size_t n = 0;
        switch (recvSome(fd.get(), tmp, sizeof tmp, deadline, n)) {
            case Io::Ok: buf.append(tmp, n); break;
            case Io::Timeout: return timedOutResult("upstream timed out waiting for headers");
            case Io::Closed: return failure("upstream closed before sending complete headers");
            case Io::Error: return failure("upstream read error while reading headers");
        }
    }

    res.head = std::move(pr.head);
    res.body = buf.substr(pr.consumed);
    if (res.head.framing == http::Framing::Chunked) {
        for (;;) {
            http::ChunkedDecodeResult decoded = http::decodeChunkedBody(res.body, up.cfg.maxResponseBytes);
            if (decoded.state == http::ParseState::Error) return failure(decoded.errorDetail);
            if (decoded.state == http::ParseState::Complete) {
                res.body = std::move(decoded.body);
                break;
            }
            size_t n = 0;
            switch (recvSome(fd.get(), tmp, sizeof tmp, deadline, n)) {
                case Io::Ok: res.body.append(tmp, n); break;
                case Io::Timeout: return timedOutResult("upstream timed out waiting for chunked body");
                case Io::Closed: return failure("upstream closed before complete chunked body");
                case Io::Error: return failure("upstream read error while reading chunked body");
            }
        }
    } else {
        if (res.body.size() > res.head.contentLength) res.body.resize(res.head.contentLength);
    }
    while (res.head.framing == http::Framing::ContentLength && res.body.size() < res.head.contentLength) {
        size_t n = 0;
        switch (recvSome(fd.get(), tmp, std::min(sizeof tmp, res.head.contentLength - res.body.size()), deadline, n)) {
            case Io::Ok: res.body.append(tmp, n); break;
            case Io::Timeout: return timedOutResult("upstream timed out waiting for body");
            case Io::Closed:
                return failure("truncated upstream body: " + std::to_string(res.body.size()) + " of " +
                               std::to_string(res.head.contentLength) + " bytes");
            case Io::Error: return failure("upstream read error while reading body");
        }
    }
    res.status = FetchStatus::Ok;
    return res;
}

// ------------------------------------------------------------ worker pool

class BoundedQueue {
public:
    explicit BoundedQueue(size_t capacity) : capacity_(capacity) {}

    bool tryPush(int fd) {
        std::lock_guard<std::mutex> lock(mu_);
        if (closed_ || items_.size() >= capacity_) return false;
        items_.push_back(fd);
        cv_.notify_one();
        return true;
    }

    // Blocks. After close(), remaining items are still handed out (drain), then nullopt.
    std::optional<int> pop() {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return closed_ || !items_.empty(); });
        if (items_.empty()) return std::nullopt;
        int fd = items_.front();
        items_.pop_front();
        return fd;
    }

    void close() {
        std::lock_guard<std::mutex> lock(mu_);
        closed_ = true;
        cv_.notify_all();
    }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<int> items_;
    size_t capacity_;
    bool closed_ = false;
};

void logLine(const std::string& target, int status, long long ms, const std::string& detail) {
    std::fprintf(stderr, "GET %s -> %d %lldms%s%s\n", target.empty() ? "-" : target.c_str(), status, ms,
                 detail.empty() ? "" : " | ", detail.c_str());
}

// Cache key: everything that selects the response. The fixed upstream authority is included for
// clarity; the target is used exactly as received (never decoded or normalised); the Accept value
// is part of the key because it is the one client header we forward, and an origin may vary on it
// without saying so. '|' can appear inside a target, so the target is placed last and Accept is
// length-prefixed; that keeps the key unambiguous.
std::string cacheKey(const UpstreamContext& up, const netcache::http::Request& r) {
    std::string accept;
    int count = 0;
    for (const auto& h : r.headers) {
        std::string n = h.name;
        for (char& c : n) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        if (n == "accept") {
            accept = h.value;
            ++count;
        }
    }
    if (count != 1) accept.clear();  // duplicate Accept is not forwarded upstream, so it cannot select anything
    return "GET|" + up.authority + "|" + std::to_string(accept.size()) + ":" + accept + "|" + r.target;
}

int statusOfSerialized(const std::string& response) {  // "HTTP/1.1 NNN ..."
    if (response.size() < 12) return 0;
    return std::atoi(response.substr(9, 3).c_str());
}

std::string statsResponse(const UpstreamContext& up) {
    netcache::Cache::Stats cs{};
    netcache::CoalescerCounters c{};
    if (up.cache) cs = up.cache->stats();
    if (up.fetcher) c = up.fetcher->counters();
    auto n = [](std::uint64_t v) { return std::to_string(v); };
    std::string body = "{\"requests\":" + n(up.requests) + ",\"bypassed_requests\":" + n(up.bypasses) +
                       ",\"cache_enabled\":" + (up.cache ? "true" : "false") +
                       ",\"coalescing\":" + (up.cfg.coalesce ? "true" : "false") +
                       ",\"hits\":" + n(c.hits) + ",\"miss_stored\":" + n(c.miss_stored) +
                       ",\"miss_uncacheable\":" + n(c.miss_uncacheable) + ",\"coalesced\":" + n(c.coalesced) +
                       ",\"upstream_failures\":" + n(c.upstream_failures) +
                       ",\"upstream_timeouts\":" + n(c.upstream_timeouts) + ",\"wait_timeouts\":" + n(c.wait_timeouts) +
                       ",\"cancelled\":" + n(c.cancelled) + ",\"upstream_fetches\":" + n(c.fetches_started) +
                       ",\"entries\":" + n(cs.entries) + ",\"payload_bytes\":" + n(cs.payload_bytes) +
                       ",\"max_bytes\":" + n(cs.max_bytes) + ",\"max_entry_bytes\":" + n(cs.max_entry_bytes) +
                       ",\"evictions\":" + n(cs.evictions) + ",\"expirations\":" + n(cs.expirations) +
                       ",\"rejected_puts\":" + n(cs.rejected_puts) + "}\n";
    return "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\nContent-Length: " +
           std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;
}

void handleConnection(int rawFd, UpstreamContext& up) {
    namespace http = netcache::http;
    FdGuard client(rawFd);
    const Config& cfg = up.cfg;
    auto start = Clock::now();
    auto elapsedMs = [&] { return std::chrono::duration_cast<Ms>(Clock::now() - start).count(); };
    auto reply = [&](const std::string& bytes) {
        sendAll(client.get(), bytes, Clock::now() + Ms(cfg.writeTimeoutMs));
        lingeringClose(client);
    };

    // 1. Read and validate the request head, bounded by a single overall deadline.
    std::string buf;
    char tmp[4096];
    http::RequestParseResult pr;
    auto deadline = start + Ms(cfg.requestTimeoutMs);
    for (;;) {
        pr = http::parseRequest(buf, {});
        if (pr.state != http::ParseState::NeedMore) break;
        size_t n = 0;
        Io io = recvSome(client.get(), tmp, sizeof tmp, deadline, n);
        if (io == Io::Ok) {
            buf.append(tmp, n);
            continue;
        }
        if (io == Io::Timeout && !buf.empty()) {
            reply(http::buildErrorResponse(408));
            logLine("", 408, elapsedMs(), "request head timed out");
        }
        return;  // idle timeout, peer closed, or read error: nothing useful to answer
    }
    if (pr.state == http::ParseState::Error) {
        reply(http::buildErrorResponse(pr.errorStatus));
        logLine("", pr.errorStatus, elapsedMs(), pr.errorDetail);
        return;
    }

    // 2. Local endpoints. Anything under /__netcache/ is never forwarded.
    constexpr std::string_view kLocalPrefix = "/__netcache/";
    if (std::string_view(pr.request.target).substr(0, kLocalPrefix.size()) == kLocalPrefix) {
        if (cfg.enableStats && pr.request.target == "/__netcache/stats") {
            reply(statsResponse(up));
            logLine(pr.request.target, 200, elapsedMs(), "stats");
        } else {
            reply(http::buildErrorResponse(404));
            logLine(pr.request.target, 404, elapsedMs(), "local path");
        }
        return;
    }
    ++up.requests;

    // 3. Forward / cache. One fetch callback serves every path: it fetches, serializes the client
    //    response once, and decides cacheability with the pure policy function.
    auto fetchFn = [&]() -> netcache::Fetched {
        auto fetchStart = Clock::now();
        FetchResult fr = fetchUpstream(up, pr.request);
        netcache::Fetched f;
        f.fetch_cost = std::chrono::duration_cast<netcache::Cache::Duration>(Clock::now() - fetchStart);
        f.detail = fr.detail;
        switch (fr.status) {
            case FetchStatus::Ok: {
                auto payload = std::make_shared<const std::string>(http::buildClientResponse(fr.head, fr.body));
                netcache::CacheDecision d = netcache::evaluateResponse(fr.head, payload->size(), up.policy);
                f.status = netcache::Fetched::Status::Ok;
                f.payload = std::move(payload);
                f.cacheable = d.cacheable;
                f.ttl = std::chrono::duration_cast<netcache::Cache::Duration>(d.ttl);
                f.detail = d.reason;
                break;
            }
            case FetchStatus::Timeout: f.status = netcache::Fetched::Status::Timeout; break;
            case FetchStatus::Failure: f.status = netcache::Fetched::Status::UpstreamError; break;
        }
        return f;
    };

    netcache::LookupResult lr;
    std::string label;  // X-Cache value
    std::string bypassReason;
    if (!up.fetcher || netcache::requestBypassesCache(pr.request, &bypassReason)) {
        ++up.bypasses;
        netcache::Fetched f = fetchFn();
        switch (f.status) {
            case netcache::Fetched::Status::Ok: lr.outcome = netcache::Outcome::MissUncacheable; lr.payload = f.payload; break;
            case netcache::Fetched::Status::Timeout: lr.outcome = netcache::Outcome::UpstreamTimeout; break;
            case netcache::Fetched::Status::UpstreamError: lr.outcome = netcache::Outcome::UpstreamFailure; break;
        }
        lr.detail = f.detail;
        label = "BYPASS";
        if (!bypassReason.empty()) lr.detail = bypassReason;
    } else {
        lr = up.fetcher->getOrFetch(cacheKey(up, pr.request), fetchFn, up.coalesceWait);
        switch (lr.outcome) {
            case netcache::Outcome::Hit: label = "HIT"; break;
            case netcache::Outcome::MissStored: label = "MISS"; break;
            case netcache::Outcome::MissUncacheable: label = "BYPASS"; break;
            case netcache::Outcome::Coalesced: label = "COALESCED"; break;
            default: break;
        }
    }

    // 4. Map every outcome explicitly (no default branch: the compiler flags a missed enumerator).
    switch (lr.outcome) {
        case netcache::Outcome::Hit:
        case netcache::Outcome::MissStored:
        case netcache::Outcome::MissUncacheable:
        case netcache::Outcome::Coalesced:
            reply(http::setResponseHeader(*lr.payload, "X-Cache", label));
            logLine(pr.request.target, statusOfSerialized(*lr.payload), elapsedMs(),
                    label + (lr.detail.empty() ? "" : " " + lr.detail));
            break;
        case netcache::Outcome::UpstreamTimeout:
        case netcache::Outcome::WaitTimeout:
            reply(http::buildErrorResponse(504));
            logLine(pr.request.target, 504, elapsedMs(), std::string(netcache::outcomeName(lr.outcome)) + " " + lr.detail);
            break;
        case netcache::Outcome::UpstreamFailure:
            reply(http::buildErrorResponse(502));
            logLine(pr.request.target, 502, elapsedMs(), lr.detail);
            break;
        case netcache::Outcome::Cancelled:
            reply(http::buildErrorResponse(503));
            logLine(pr.request.target, 503, elapsedMs(), "shutting down");
            break;
    }
}

void workerLoop(BoundedQueue& queue, UpstreamContext& up) {
    while (auto fd = queue.pop()) {
        try {
            handleConnection(*fd, up);
        } catch (const std::exception& e) {  // a worker must never die; fd is closed by FdGuard
            std::fprintf(stderr, "worker error: %s\n", e.what());
        } catch (...) {
            std::fprintf(stderr, "worker error: unknown exception\n");
        }
    }
}

// ----------------------------------------------------------------- listener

int makeListener(const Config& cfg) {
    in_addr ia{};
    if (::inet_pton(AF_INET, cfg.bindAddress.c_str(), &ia) != 1) {
        std::fprintf(stderr, "invalid --bind address (IPv4 literal required): %s\n", cfg.bindAddress.c_str());
        return -1;
    }
    FdGuard fd(::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (fd.get() < 0) {
        std::perror("socket");
        return -1;
    }
    int one = 1;
    ::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(cfg.listenPort);
    sa.sin_addr = ia;
    if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&sa), sizeof sa) != 0) {
        std::perror("bind");
        return -1;
    }
    if (::listen(fd.get(), 128) != 0) {
        std::perror("listen");
        return -1;
    }
    return fd.release();
}

// Runs on the acceptor thread, so it must never block. Best effort: send 503, FIN, drain what
// has already arrived (a bounded number of reads) so close() is less likely to turn into an RST
// that destroys the 503. Under overload a client may still see a reset; both mean "retry later".
void shedLoad(int fd) {
    std::string resp = netcache::http::buildErrorResponse(503);
    ::send(fd, resp.data(), resp.size(), MSG_NOSIGNAL | MSG_DONTWAIT);
    ::shutdown(fd, SHUT_WR);
    char sink[4096];
    for (int i = 0; i < 16 && ::recv(fd, sink, sizeof sink, MSG_DONTWAIT) > 0; ++i) {
    }
    ::close(fd);
}

std::string formatAuthority(const std::string& host, uint16_t port) {
    bool v6 = host.find(':') != std::string::npos;
    return (v6 ? "[" + host + "]" : host) + ":" + std::to_string(port);
}

}  // namespace

int main(int argc, char** argv) {
    Config cfg;
    if (!parseArgs(argc, argv, cfg)) {
        printUsage(argv[0]);
        return 2;
    }

    UpstreamContext up;
    up.cfg = cfg;
    std::string err;
    up.addrs = resolveUpstream(cfg.upstreamHost, cfg.upstreamPort, err);
    if (up.addrs.empty()) {
        std::fprintf(stderr, "cannot resolve upstream %s: %s\n", cfg.upstreamHost.c_str(), err.c_str());
        return 1;
    }
    up.authority = formatAuthority(cfg.upstreamHost, cfg.upstreamPort);
    up.policy.default_ttl = std::chrono::seconds(cfg.defaultTtlS);
    up.policy.max_ttl = std::chrono::seconds(cfg.maxTtlS);
    up.policy.max_entry_bytes = std::min(cfg.cacheEntryBytes, cfg.cacheBytes);
    up.coalesceWait = Ms(cfg.coalesceWaitMs != 0 ? cfg.coalesceWaitMs : cfg.upstreamTimeoutMs + 2000);
    if (cfg.cacheBytes > 0) {
        netcache::Cache::Config cc;
        cc.max_bytes = cfg.cacheBytes;
        cc.max_entry_bytes = cfg.cacheEntryBytes;
        cc.eviction = cfg.eviction;
        up.cache = std::make_unique<netcache::Cache>(cc);
        netcache::CoalescingCache::Options co;
        co.coalesce = cfg.coalesce;
        up.fetcher = std::make_unique<netcache::CoalescingCache>(*up.cache, co);
    }

    // No SA_RESTART: signals must interrupt poll() so shutdown is prompt.
    struct sigaction sa{};
    sa.sa_handler = onSignal;
    sigemptyset(&sa.sa_mask);
    ::sigaction(SIGINT, &sa, nullptr);
    ::sigaction(SIGTERM, &sa, nullptr);
    std::signal(SIGPIPE, SIG_IGN);

    FdGuard listener(makeListener(cfg));
    if (listener.get() < 0) return 1;

    BoundedQueue queue(cfg.queueCapacity);
    std::vector<std::thread> workers;
    workers.reserve(cfg.workers);
    for (size_t i = 0; i < cfg.workers; ++i) workers.emplace_back(workerLoop, std::ref(queue), std::ref(up));

    std::fprintf(stderr, "netcache listening on %s:%u -> %s (%zu workers, queue %zu, cache %zu B, coalescing %s)\n",
                 cfg.bindAddress.c_str(), static_cast<unsigned>(cfg.listenPort), up.authority.c_str(), cfg.workers,
                 cfg.queueCapacity, cfg.cacheBytes, cfg.coalesce ? "on" : "off");

    while (!g_stop) {
        pollfd p{listener.get(), POLLIN, 0};
        int r = ::poll(&p, 1, 200);
        if (r < 0) {
            if (errno == EINTR) continue;
            std::perror("poll");
            break;
        }
        if (r == 0) continue;

        int fd = ::accept4(listener.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EMFILE || errno == ENFILE) std::this_thread::sleep_for(Ms(10));  // avoid a hot loop
            continue;
        }
        if (!queue.tryPush(fd)) {  // overload: shed load immediately rather than queue without bound
            shedLoad(fd);
            std::fprintf(stderr, "GET - -> 503 0ms | queue full\n");
        }
    }

    std::fprintf(stderr, "shutting down: draining queued connections\n");
    queue.close();
    if (up.fetcher) up.fetcher->shutdown();  // wake coalesced waiters; no new upstream fetches while draining
    for (auto& t : workers) t.join();
    return 0;
}
