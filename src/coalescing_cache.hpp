#pragma once
// Phase 2B: single-flight ("request coalescing") in front of the Phase 2A Cache.
//
// getOrFetch(key, fetch): fresh hit -> returned immediately. Otherwise exactly ONE caller per key
// (the owner) runs `fetch` while concurrent callers for the same key (waiters) block and share its
// result. Different keys never wait on each other. Expired entries are misses, so a refresh is
// coalesced the same way; stale data is never served.
//
// Knows nothing about sockets or HTTP: the fetch callback returns an opaque payload plus a
// cacheability verdict, so everything here is testable with fake fetchers and a fake clock.
//
// Locking (documented, tested under TSan):
//   * mu_ guards ONLY the in-flight table and flight state. The Cache has its own mutex.
//   * Lock order is always mu_ -> Cache::mutex_ (Cache never calls back out), never the reverse.
//   * `fetch` (network I/O) runs with NO lock held. The Cache mutex is never held across it.
//   * The owner stores into the Cache BEFORE it removes its in-flight record. A thread that finds
//     no in-flight record therefore re-checks the Cache under mu_, which closes the window where a
//     finished fetch would otherwise be repeated.
//
// Every owner exit path (success, error, timeout, exception of any type, shutdown) runs one RAII
// guard that (a) publishes a result, (b) erases the in-flight record and (c) notify_all()s, so a
// waiter can never hang on a dead owner. Waiters also have their own deadline.
//
// Failure policy: if the owner fails, ALL its waiters receive the same failure (no retry storm);
// nothing is cached; the next request starts a fresh fetch.
//
// Sharing an uncacheable result (e.g. a 404 or a `no-store` response) with concurrent waiters is
// safe ONLY because callers have already bypassed requests that could be personalised
// (Authorization/Cookie/...) and the cache key covers everything that selects the response.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "cache.hpp"

namespace netcache {

struct Fetched {
    enum class Status { Ok, UpstreamError, Timeout };
    Status status = Status::UpstreamError;
    std::shared_ptr<const std::string> payload;  // required when status == Ok
    bool cacheable = false;                      // meaningful when status == Ok
    Cache::Duration ttl{};                       // meaningful when cacheable
    Cache::Duration fetch_cost{};                // measured upstream fetch duration
    std::string detail;                          // logs only
};

enum class Outcome {
    Hit,              // fresh entry served from the cache
    MissStored,       // this caller fetched, response was cacheable and is now stored
    MissUncacheable,  // this caller fetched; valid response, deliberately not stored
    Coalesced,        // waited on another caller's fetch and shares its (valid) response
    UpstreamFailure,  // the fetch this caller owned or waited on failed
    UpstreamTimeout,  // ... timed out
    WaitTimeout,      // this waiter gave up waiting for the owner
    Cancelled         // shutdown() was called (waiting, or a miss that needed a new fetch)
};

const char* outcomeName(Outcome o);

struct LookupResult {
    Outcome outcome = Outcome::Cancelled;
    std::shared_ptr<const std::string> payload;  // non-null exactly for Hit/MissStored/MissUncacheable/Coalesced
    std::string detail;
};

struct CoalescerCounters {
    std::uint64_t hits = 0, miss_stored = 0, miss_uncacheable = 0, coalesced = 0, upstream_failures = 0,
                  upstream_timeouts = 0, wait_timeouts = 0, cancelled = 0, fetches_started = 0;
};

class CoalescingCache {
public:
    using FetchFn = std::function<Fetched()>;

    struct Options {
        bool coalesce = true;  // false: every miss fetches (control experiment / ablation)
        // TEST SEAM, never set in production: called (with no lock held) after the first lookup
        // missed and before the in-flight table is consulted. Lets a test force the exact
        // interleaving "another owner finishes in this window" deterministically.
        std::function<void(const std::string& key)> after_first_miss;
    };

    CoalescingCache(Cache& cache, Options options);
    explicit CoalescingCache(Cache& cache) : CoalescingCache(cache, Options{}) {}
    CoalescingCache(const CoalescingCache&) = delete;
    CoalescingCache& operator=(const CoalescingCache&) = delete;

    // `waitTimeout` bounds only the time spent WAITING for another caller's fetch.
    // Never throws for fetch failures (they become outcomes).
    LookupResult getOrFetch(const std::string& key, const FetchFn& fetch, std::chrono::milliseconds waitTimeout);

    // Wakes every waiter with Cancelled and refuses to start new fetches (hits are still served).
    // Fetches already running finish normally.
    void shutdown();

    CoalescerCounters counters() const;
    std::size_t inflightCount() const;                 // diagnostic
    std::size_t waitersFor(const std::string& key) const;  // diagnostic (tests): waiters currently blocked on key

private:
    struct Flight {
        std::condition_variable cv;
        bool done = false;
        LookupResult result;  // what waiters will adopt (outcome rewritten for them)
        std::size_t waiters = 0;
    };
    class OwnerGuard;

    LookupResult runFetch(const std::string& key, const FetchFn& fetch, OwnerGuard* guard);

    Cache& cache_;
    Options options_;

    mutable std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<Flight>> inflight_;
    bool shutdown_ = false;

    std::atomic<std::uint64_t> hits_{0}, miss_stored_{0}, miss_uncacheable_{0}, coalesced_{0}, failures_{0},
        timeouts_{0}, wait_timeouts_{0}, cancelled_{0}, fetches_{0};
};

}  // namespace netcache
