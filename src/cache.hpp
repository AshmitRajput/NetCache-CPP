#pragma once

#include "eviction.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace netcache {

class Cache {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = std::chrono::nanoseconds;
    using NowFunction = std::function<TimePoint()>;

    struct Config {
        std::size_t max_bytes = 16 * 1024 * 1024;
        std::size_t max_entry_bytes = 2 * 1024 * 1024;
        EvictionKind eviction = EvictionKind::Lru;
        CostAwareParams cost;
    };

    struct Stats {
        std::size_t entries = 0;
        std::size_t payload_bytes = 0;
        std::size_t max_bytes = 0;
        std::size_t max_entry_bytes = 0;
        std::uint64_t evictions = 0;
        std::uint64_t expirations = 0;
        std::uint64_t rejected_puts = 0;
    };

    static TimePoint steady_now();
    Cache();
    explicit Cache(Config config, NowFunction now = &Cache::steady_now,
                   std::unique_ptr<EvictionPolicy> policy = nullptr);
    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;

    std::optional<std::string> get(const std::string& key);
    bool put(const std::string& key, std::string payload, Duration ttl,
             Duration fetch_cost = Duration::zero());
    bool erase(const std::string& key);
    void clear();
    Stats stats() const;
    bool check_invariants() const;

private:
    struct Entry {
        std::string payload;
        TimePoint expires_at;
        EntryMeta meta;
    };
    using Map = std::unordered_map<std::string, Entry>;

    void removeLocked(Map::iterator it);
    void reapExpiredLocked(TimePoint now);

    Config config_;
    NowFunction now_;
    std::unique_ptr<EvictionPolicy> policy_;
    mutable std::mutex mu_;
    Map index_;
    std::size_t payload_bytes_ = 0;
    std::uint64_t evictions_ = 0;
    std::uint64_t expirations_ = 0;
    std::uint64_t rejected_puts_ = 0;
};

}  // namespace netcache
