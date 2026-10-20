#include "cache.hpp"
#include "test_harness.hpp"

#include <chrono>
#include <string>

using namespace netcache;
using namespace std::chrono_literals;

namespace {

struct FakeClock {
    Cache::TimePoint now = Cache::TimePoint{};
    Cache::TimePoint operator()() const { return now; }
};

}  // namespace

TEST(cache_stores_hits_and_copies_payload) {
    FakeClock clock;
    Cache::Config cfg;
    cfg.max_bytes = 100;
    cfg.max_entry_bytes = 100;
    Cache cache(cfg, [&clock] { return clock.now; });
    CHECK(cache.put("key", "payload", 10s));
    auto value = cache.get("key");
    CHECK(value.has_value());
    CHECK_EQ(*value, std::string("payload"));
    CHECK(cache.check_invariants());
}

TEST(cache_expires_lazily_and_rejects_invalid_puts) {
    FakeClock clock;
    Cache::Config cfg;
    cfg.max_bytes = 10;
    cfg.max_entry_bytes = 10;
    Cache cache(cfg, [&clock] { return clock.now; });
    CHECK(!cache.put("bad", "x", 0s));
    CHECK(cache.put("key", "1234567890", 1s));
    clock.now += 1s;
    CHECK(!cache.get("key").has_value());
    CHECK_EQ(cache.stats().expirations, std::uint64_t(1));
}

TEST(cache_lru_eviction_preserves_recent_entry) {
    Cache::Config cfg;
    cfg.max_bytes = 5;
    cfg.max_entry_bytes = 5;
    Cache cache(cfg);
    CHECK(cache.put("a", "aa", 1h));
    CHECK(cache.put("b", "bb", 1h));
    CHECK(cache.get("a").has_value());
    CHECK(cache.put("c", "cc", 1h));
    CHECK(cache.get("a").has_value());
    CHECK(!cache.get("b").has_value());
    CHECK(cache.get("c").has_value());
    CHECK_EQ(cache.stats().evictions, std::uint64_t(1));
    CHECK(cache.check_invariants());
}

int main() { return th::runAll(); }
