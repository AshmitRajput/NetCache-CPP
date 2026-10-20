// Socket-free tests for single-flight coalescing (src/coalescing_cache.cpp).
// Fake fetchers + fake clock; overlap is forced with explicit gates, never with sleeps-as-synchronisation.

#include "coalescing_cache.hpp"
#include "test_harness.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
using namespace netcache;

namespace {

Cache::Config cfg(std::size_t total, std::size_t entry) {
    Cache::Config c;
    c.max_bytes = total;
    c.max_entry_bytes = entry;
    return c;
}

Fetched ok(const std::string& body, bool cacheable = true, std::chrono::seconds ttl = 60s) {
    Fetched f;
    f.status = Fetched::Status::Ok;
    f.payload = std::make_shared<const std::string>(body);
    f.cacheable = cacheable;
    f.ttl = std::chrono::duration_cast<Cache::Duration>(ttl);
    return f;
}

Fetched failed(Fetched::Status st, const std::string& why = "boom") {
    Fetched f;
    f.status = st;
    f.detail = why;
    return f;
}

// Poll a condition with a hard deadline so a bug fails the test instead of hanging it.
bool waitUntil(const std::function<bool()>& cond, std::chrono::milliseconds limit = 10s) {
    auto end = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < end) {
        if (cond()) return true;
        std::this_thread::sleep_for(1ms);
    }
    return cond();
}

// Joins threads, or kills the process if they do not finish: a hang is a test failure.
struct Group {
    std::vector<std::thread> ts;
    std::atomic<int> finished{0};
    template <class F>
    void spawn(F f) {
        ts.emplace_back([this, f] {
            f();
            ++finished;
        });
    }
    // If a CHECK throws while threads are still running, join them instead of std::terminate().
    ~Group() {
        for (auto& t : ts)
            if (t.joinable()) t.join();
    }
    void joinAll(std::chrono::milliseconds limit = 15s) {
        if (!waitUntil([&] { return finished.load() == static_cast<int>(ts.size()); }, limit)) {
            std::fprintf(stderr, "DEADLOCK: %d of %zu threads finished\n", finished.load(), ts.size());
            std::_Exit(3);
        }
        for (auto& t : ts) t.join();
        ts.clear();
    }
};

// Blocks fetchers until opened.
struct Gate {
    std::mutex m;
    std::condition_variable cv;
    bool open = false;
    void wait() {
        std::unique_lock<std::mutex> l(m);
        cv.wait_for(l, 15s, [&] { return open; });
    }
    void release() {
        std::lock_guard<std::mutex> l(m);
        open = true;
        cv.notify_all();
    }
};

}  // namespace

TEST(miss_then_hit_fetches_once) {
    Cache cache(cfg(1000, 1000));
    CoalescingCache cc(cache);
    int calls = 0;
    auto fetch = [&] {
        ++calls;
        return ok("body");
    };
    auto a = cc.getOrFetch("k", fetch, 1s);
    CHECK(a.outcome == Outcome::MissStored);
    CHECK_EQ(*a.payload, std::string("body"));
    auto b = cc.getOrFetch("k", fetch, 1s);
    CHECK(b.outcome == Outcome::Hit);
    CHECK_EQ(*b.payload, std::string("body"));
    CHECK_EQ(calls, 1);
    CHECK_EQ(cc.inflightCount(), size_t(0));
}

TEST(uncacheable_result_is_returned_but_never_stored) {
    Cache cache(cfg(1000, 1000));
    CoalescingCache cc(cache);
    int calls = 0;
    auto fetch = [&] {
        ++calls;
        return ok("private-ish", /*cacheable=*/false);
    };
    CHECK(cc.getOrFetch("k", fetch, 1s).outcome == Outcome::MissUncacheable);
    CHECK(cc.getOrFetch("k", fetch, 1s).outcome == Outcome::MissUncacheable);
    CHECK_EQ(calls, 2);
    CHECK_EQ(cache.stats().entries, size_t(0));
}

TEST(cache_rejection_of_oversized_payload_is_uncacheable_not_error) {
    Cache cache(cfg(1000, 10));  // entry cap 10
    CoalescingCache cc(cache);
    auto r = cc.getOrFetch("k", [] { return ok(std::string(50, 'x'), true); }, 1s);
    CHECK(r.outcome == Outcome::MissUncacheable);
    CHECK_EQ(r.payload->size(), size_t(50));
    CHECK_EQ(cache.stats().entries, size_t(0));
}

TEST(failures_are_outcomes_with_null_payload_and_are_never_cached) {
    Cache cache(cfg(1000, 1000));
    CoalescingCache cc(cache);
    auto e = cc.getOrFetch("e", [] { return failed(Fetched::Status::UpstreamError); }, 1s);
    CHECK(e.outcome == Outcome::UpstreamFailure);
    CHECK(!e.payload);
    auto t = cc.getOrFetch("t", [] { return failed(Fetched::Status::Timeout); }, 1s);
    CHECK(t.outcome == Outcome::UpstreamTimeout);
    CHECK(!t.payload);
    auto n = cc.getOrFetch("n", [] {
        Fetched f;
        f.status = Fetched::Status::Ok;  // contract violation: Ok without payload
        return f;
    }, 1s);
    CHECK(n.outcome == Outcome::UpstreamFailure);
    CHECK(!n.payload);
    CHECK_EQ(cache.stats().entries, size_t(0));
    CHECK_EQ(cc.inflightCount(), size_t(0));
}

TEST(fifty_concurrent_misses_trigger_one_fetch) {
    Cache cache(cfg(1000, 1000));
    CoalescingCache cc(cache);
    constexpr int N = 50;
    std::atomic<int> calls{0};
    auto fetch = [&] {
        ++calls;
        // Hold the fetch open until every other thread is provably waiting on this key.
        bool all = waitUntil([&] { return cc.waitersFor("k") == N - 1; });
        return all ? ok("shared") : failed(Fetched::Status::UpstreamError, "waiters never arrived");
    };
    std::vector<LookupResult> res(N);
    Group g;
    for (int i = 0; i < N; ++i) g.spawn([&, i] { res[i] = cc.getOrFetch("k", fetch, 15s); });
    g.joinAll();

    CHECK_EQ(calls.load(), 1);
    int stored = 0, coalesced = 0;
    for (auto& r : res) {
        CHECK(r.payload != nullptr);
        CHECK_EQ(*r.payload, std::string("shared"));
        if (r.outcome == Outcome::MissStored) ++stored;
        if (r.outcome == Outcome::Coalesced) ++coalesced;
    }
    CHECK_EQ(stored, 1);
    CHECK_EQ(coalesced, N - 1);
    CHECK_EQ(cc.inflightCount(), size_t(0));
    CHECK(cc.getOrFetch("k", fetch, 1s).outcome == Outcome::Hit);
    CHECK_EQ(calls.load(), 1);
}

TEST(different_keys_fetch_independently_and_concurrently) {
    Cache cache(cfg(1000, 1000));
    CoalescingCache cc(cache);
    std::atomic<int> inFetch{0};
    auto fetch = [&](const std::string& body) {
        return [&, body] {
            ++inFetch;
            // Each fetch can only finish once BOTH are running: serialised fetching would time out here.
            bool both = waitUntil([&] { return inFetch.load() == 2; }, 5s);
            return both ? ok(body) : failed(Fetched::Status::UpstreamError, "fetches were serialised");
        };
    };
    LookupResult a, b;
    Group g;
    g.spawn([&] { a = cc.getOrFetch("A", fetch("alpha"), 10s); });
    g.spawn([&] { b = cc.getOrFetch("B", fetch("beta"), 10s); });
    g.joinAll();
    CHECK(a.outcome == Outcome::MissStored);
    CHECK(b.outcome == Outcome::MissStored);
    CHECK_EQ(*a.payload, std::string("alpha"));
    CHECK_EQ(*b.payload, std::string("beta"));
}

TEST(slow_fetch_does_not_block_hits_on_other_keys) {
    Cache cache(cfg(1000, 1000));
    CoalescingCache cc(cache);
    CHECK(cc.getOrFetch("fast", [] { return ok("f"); }, 1s).outcome == Outcome::MissStored);
    Gate gate;
    LookupResult slow;
    Group g;
    std::atomic<bool> started{false};
    g.spawn([&] {
        slow = cc.getOrFetch("slow", [&] {
            started = true;
            gate.wait();
            return ok("s");
        }, 20s);
    });
    CHECK(waitUntil([&] { return started.load(); }));
    // The owner is blocked mid-fetch; a hit must return immediately regardless.
    auto t0 = std::chrono::steady_clock::now();
    CHECK(cc.getOrFetch("fast", [] { return failed(Fetched::Status::UpstreamError); }, 1s).outcome == Outcome::Hit);
    CHECK(std::chrono::steady_clock::now() - t0 < 1s);
    gate.release();
    g.joinAll();
    CHECK(slow.outcome == Outcome::MissStored);
}

// One test body, four failure exit paths: every waiter must wake, the key must be released,
// nothing may be cached, and the next request must be able to start a fresh fetch.
namespace {
void failureWakesAllWaiters(const std::function<Fetched()>& failing, Outcome expected) {
    Cache cache(cfg(1000, 1000));
    CoalescingCache cc(cache);
    constexpr int N = 20;
    std::atomic<int> calls{0};
    auto fetch = [&]() -> Fetched {
        ++calls;
        waitUntil([&] { return cc.waitersFor("k") == N - 1; });
        return failing();
    };
    std::vector<LookupResult> res(N);
    Group g;
    for (int i = 0; i < N; ++i) g.spawn([&, i] { res[i] = cc.getOrFetch("k", fetch, 15s); });
    g.joinAll();

    CHECK_EQ(calls.load(), 1);
    for (auto& r : res) {
        CHECK(r.outcome == expected);
        CHECK(!r.payload);
    }
    CHECK_EQ(cc.inflightCount(), size_t(0));       // in-flight state cleared
    CHECK_EQ(cache.stats().entries, size_t(0));    // nothing cached
    auto again = cc.getOrFetch("k", [] { return ok("recovered"); }, 1s);  // key is usable again
    CHECK(again.outcome == Outcome::MissStored);
    CHECK_EQ(*again.payload, std::string("recovered"));
}
}  // namespace

TEST(upstream_error_wakes_all_waiters) {
    failureWakesAllWaiters([] { return failed(Fetched::Status::UpstreamError); }, Outcome::UpstreamFailure);
}
TEST(upstream_timeout_wakes_all_waiters) {
    failureWakesAllWaiters([] { return failed(Fetched::Status::Timeout); }, Outcome::UpstreamTimeout);
}
TEST(fetch_exception_wakes_all_waiters) {
    failureWakesAllWaiters([]() -> Fetched { throw std::runtime_error("kaboom"); }, Outcome::UpstreamFailure);
}
TEST(non_std_exception_wakes_all_waiters) {
    failureWakesAllWaiters([]() -> Fetched { throw 42; }, Outcome::UpstreamFailure);
}

TEST(exception_detail_is_reported) {
    Cache cache(cfg(1000, 1000));
    CoalescingCache cc(cache);
    auto r = cc.getOrFetch("k", []() -> Fetched { throw std::runtime_error("kaboom"); }, 1s);
    CHECK(r.outcome == Outcome::UpstreamFailure);
    CHECK(r.detail.find("kaboom") != std::string::npos);
}

TEST(waiter_gives_up_after_its_own_timeout_without_disturbing_the_owner) {
    Cache cache(cfg(1000, 1000));
    CoalescingCache cc(cache);
    Gate gate;
    std::atomic<bool> started{false};
    LookupResult owner;
    Group g;
    g.spawn([&] {
        owner = cc.getOrFetch("k", [&] {
            started = true;
            gate.wait();
            return ok("late");
        }, 20s);
    });
    CHECK(waitUntil([&] { return started.load(); }));
    auto w = cc.getOrFetch("k", [] { return ok("never called"); }, 50ms);
    CHECK(w.outcome == Outcome::WaitTimeout);
    CHECK(!w.payload);
    CHECK_EQ(cc.waitersFor("k"), size_t(0));  // the timed-out waiter deregistered itself
    gate.release();
    g.joinAll();
    CHECK(owner.outcome == Outcome::MissStored);  // owner unaffected
    CHECK(cc.getOrFetch("k", [] { return failed(Fetched::Status::UpstreamError); }, 1s).outcome == Outcome::Hit);
}

TEST(shutdown_cancels_waiters_refuses_new_fetches_but_still_serves_hits) {
    Cache cache(cfg(1000, 1000));
    CoalescingCache cc(cache);
    CHECK(cc.getOrFetch("warm", [] { return ok("w"); }, 1s).outcome == Outcome::MissStored);

    Gate gate;
    constexpr int W = 5;
    std::atomic<bool> started{false};
    LookupResult owner;
    std::vector<LookupResult> waiters(W);
    Group g;
    g.spawn([&] {
        owner = cc.getOrFetch("k", [&] {
            started = true;
            gate.wait();
            return ok("v");
        }, 20s);
    });
    CHECK(waitUntil([&] { return started.load(); }));
    for (int i = 0; i < W; ++i) g.spawn([&, i] { waiters[i] = cc.getOrFetch("k", [] { return ok("x"); }, 20s); });
    CHECK(waitUntil([&] { return cc.waitersFor("k") == W; }));

    cc.shutdown();
    // Waiters are released without the owner finishing.
    CHECK(waitUntil([&] { return cc.waitersFor("k") == 0; }));
    CHECK(cc.getOrFetch("brand-new", [] { return ok("n"); }, 1s).outcome == Outcome::Cancelled);
    CHECK(cc.getOrFetch("warm", [] { return ok("n"); }, 1s).outcome == Outcome::Hit);

    gate.release();
    g.joinAll();
    for (auto& w : waiters) {
        CHECK(w.outcome == Outcome::Cancelled);
        CHECK(!w.payload);
    }
    CHECK(owner.outcome == Outcome::MissStored);  // the running fetch finished normally
    CHECK_EQ(cc.inflightCount(), size_t(0));
}

TEST(expiry_triggers_one_coalesced_refresh_and_stale_is_never_served) {
    std::atomic<std::int64_t> ms{0};
    Cache cache(cfg(1000, 1000), [&ms] { return Cache::TimePoint{std::chrono::milliseconds(ms.load())}; });
    CoalescingCache cc(cache);
    std::atomic<int> calls{0};
    auto fetch = [&] {
        int n = ++calls;
        waitUntil([&] { return n == 1 || cc.waitersFor("k") == 9; });
        return ok("gen" + std::to_string(n), true, 5s);
    };
    CHECK_EQ(*cc.getOrFetch("k", fetch, 1s).payload, std::string("gen1"));
    ms += 4999;
    CHECK_EQ(*cc.getOrFetch("k", fetch, 1s).payload, std::string("gen1"));  // still fresh
    CHECK_EQ(calls.load(), 1);
    ms += 1;  // exactly at expiry

    std::vector<LookupResult> res(10);
    Group g;
    for (int i = 0; i < 10; ++i) g.spawn([&, i] { res[i] = cc.getOrFetch("k", fetch, 15s); });
    g.joinAll();
    CHECK_EQ(calls.load(), 2);  // ten requests, one refresh
    for (auto& r : res) CHECK_EQ(*r.payload, std::string("gen2"));  // nobody saw the stale gen1
}

TEST(coalescing_can_be_disabled_for_experiments) {
    Cache cache(cfg(1000, 1000));
    CoalescingCache::Options o;
    o.coalesce = false;
    CoalescingCache cc(cache, o);
    constexpr int N = 8;
    std::atomic<int> inFetch{0};
    auto fetch = [&] {
        ++inFetch;
        waitUntil([&] { return inFetch.load() == N; }, 5s);  // all N must be fetching at the same time
        return ok("v");
    };
    Group g;
    for (int i = 0; i < N; ++i) g.spawn([&] { cc.getOrFetch("k", fetch, 15s); });
    g.joinAll();
    CHECK_EQ(cc.counters().fetches_started, uint64_t(N));
}

TEST(owner_finishing_between_first_lookup_and_registration_is_not_refetched) {
    // Deterministic version of the race the "re-check before becoming owner" step exists for:
    // thread A misses, then (while parked in the seam) B fetches, stores and unregisters.
    // A must then find B's entry instead of starting a second fetch.
    Cache cache(cfg(1000, 1000));
    Gate parked, release;
    std::atomic<bool> armed{true};
    CoalescingCache::Options o;
    o.after_first_miss = [&](const std::string&) {
        if (!armed.exchange(false)) return;  // only the first caller (A) parks
        parked.release();
        release.wait();
    };
    CoalescingCache cc(cache, o);
    std::atomic<int> calls{0};
    auto fetch = [&] {
        ++calls;
        return ok("once");
    };
    LookupResult a;
    Group g;
    g.spawn([&] { a = cc.getOrFetch("k", fetch, 5s); });
    parked.wait();                                    // A has missed and is parked
    auto b = cc.getOrFetch("k", fetch, 5s);           // B runs a complete fetch
    CHECK(b.outcome == Outcome::MissStored);
    release.release();
    g.joinAll();
    CHECK(a.outcome == Outcome::Hit);
    CHECK_EQ(calls.load(), 1);
}

TEST(stress_each_key_is_fetched_exactly_once_under_contention) {
    // Long TTL, big cache, fake (frozen) clock: any second fetch for a key means the
    // "re-check the cache before becoming owner" step is broken.
    Cache cache(cfg(1 << 20, 1 << 20), [] { return Cache::TimePoint{}; });
    CoalescingCache cc(cache);
    constexpr int kKeys = 12, kThreads = 8, kOps = 3000;
    std::map<int, std::atomic<int>> perKey;
    for (int k = 0; k < kKeys; ++k) perKey[k].store(0);
    std::atomic<int> bad{0};
    Group g;
    for (int t = 0; t < kThreads; ++t) {
        g.spawn([&, t] {
            std::mt19937 rng(100 + t);
            for (int i = 0; i < kOps; ++i) {
                int k = static_cast<int>(rng() % kKeys);
                auto r = cc.getOrFetch("key" + std::to_string(k), [&, k] {
                    ++perKey[k];
                    std::this_thread::sleep_for(std::chrono::microseconds(200));  // widen the race window
                    return ok("v" + std::to_string(k), true, 1000s);
                }, 15s);
                if (!r.payload || *r.payload != "v" + std::to_string(k)) ++bad;
            }
        });
    }
    g.joinAll(60s);
    CHECK_EQ(bad.load(), 0);
    for (int k = 0; k < kKeys; ++k) CHECK_EQ(perKey[k].load(), 1);
    CHECK_EQ(cc.inflightCount(), size_t(0));
    CHECK(cache.check_invariants());
}

TEST(counters_add_up) {
    Cache cache(cfg(1000, 1000));
    CoalescingCache cc(cache);
    cc.getOrFetch("a", [] { return ok("1"); }, 1s);                                  // miss_stored
    cc.getOrFetch("a", [] { return ok("1"); }, 1s);                                  // hit
    cc.getOrFetch("b", [] { return ok("2", false); }, 1s);                           // uncacheable
    cc.getOrFetch("c", [] { return failed(Fetched::Status::UpstreamError); }, 1s);   // failure
    cc.getOrFetch("d", [] { return failed(Fetched::Status::Timeout); }, 1s);        // timeout
    auto c = cc.counters();
    CHECK_EQ(c.hits, uint64_t(1));
    CHECK_EQ(c.miss_stored, uint64_t(1));
    CHECK_EQ(c.miss_uncacheable, uint64_t(1));
    CHECK_EQ(c.upstream_failures, uint64_t(1));
    CHECK_EQ(c.upstream_timeouts, uint64_t(1));
    CHECK_EQ(c.fetches_started, uint64_t(4));
}

int main() { return th::runAll(); }
