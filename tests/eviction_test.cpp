#include "eviction.hpp"
#include "test_harness.hpp"

#include <chrono>
#include <cmath>
#include <stdexcept>

using namespace netcache;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

namespace {
const Clock::time_point T0 = Clock::time_point{} + 1000s;

EntryMeta makeMeta(const char* key, std::size_t bytes, std::chrono::milliseconds cost,
                   Clock::time_point lastAccess) {
    EntryMeta meta;
    meta.key = key;
    meta.bytes = bytes;
    meta.fetch_cost = cost;
    meta.inserted_at = T0;
    meta.last_access = lastAccess;
    return meta;
}
}

TEST(lru_tracks_hits_and_replacements) {
    LruPolicy policy;
    policy.onInsert(makeMeta("a", 1, 0ms, T0));
    policy.onInsert(makeMeta("b", 1, 0ms, T0));
    CHECK_EQ(policy.selectVictim(T0), std::string("a"));
    policy.onHit(makeMeta("a", 1, 0ms, T0), T0);
    CHECK_EQ(policy.selectVictim(T0), std::string("b"));
    policy.onInsert(makeMeta("b", 2, 0ms, T0));
    CHECK_EQ(policy.size(), std::size_t(2));
}

TEST(cost_policy_is_deterministic_and_validates_weights) {
    CostAwarePolicy policy(CostAwareParams{1.0, 1.0});
    policy.onInsert(makeMeta("large", 100, 1ms, T0));
    policy.onInsert(makeMeta("old", 10, 100ms, T0 - 10s));
    CHECK(std::isfinite(policy.scoreOf("old", T0)));
    CHECK_EQ(policy.selectVictim(T0), std::string("large"));
    bool threw = false;
    try {
        CostAwarePolicy invalid(CostAwareParams{0.0, 0.0});
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    CHECK(threw);
}

int main() { return th::runAll(); }
