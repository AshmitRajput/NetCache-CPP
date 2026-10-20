#include "cache.hpp"

#include <algorithm>
#include <limits>

namespace netcache {

Cache::TimePoint Cache::steady_now() {
    return Clock::now();
}

Cache::Cache() : Cache(Config{}) {}

Cache::Cache(Config config, NowFunction now, std::unique_ptr<EvictionPolicy> policy)
    : config_(config),
      now_(now ? std::move(now) : NowFunction(&Cache::steady_now)),
      policy_(policy ? std::move(policy) : makeEvictionPolicy(config.eviction, config.cost)) {
    config_.max_entry_bytes = std::min(config_.max_entry_bytes, config_.max_bytes);
}

void Cache::removeLocked(Map::iterator it) {
    payload_bytes_ -= it->second.meta.bytes;
    policy_->onRemove(it->first);
    index_.erase(it);
}

void Cache::reapExpiredLocked(TimePoint now) {
    for (auto it = index_.begin(); it != index_.end();) {
        if (now >= it->second.expires_at) {
            payload_bytes_ -= it->second.meta.bytes;
            policy_->onRemove(it->first);
            it = index_.erase(it);
            ++expirations_;
        } else {
            ++it;
        }
    }
}

std::optional<std::string> Cache::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = index_.find(key);
    if (it == index_.end()) return std::nullopt;
    TimePoint now = now_();
    if (now >= it->second.expires_at) {
        removeLocked(it);
        ++expirations_;
        return std::nullopt;
    }
    ++it->second.meta.hits;
    it->second.meta.last_access = now;
    policy_->onHit(it->second.meta, now);
    return it->second.payload;
}

bool Cache::put(const std::string& key, std::string payload, Duration ttl, Duration fetch_cost) {
    std::lock_guard<std::mutex> lock(mu_);
    auto old = index_.find(key);
    if (old != index_.end()) removeLocked(old);

    const std::size_t bytes = payload.size();
    if (ttl <= Duration::zero() || bytes > config_.max_entry_bytes || bytes > config_.max_bytes) {
        ++rejected_puts_;
        return false;
    }

    TimePoint now = now_();
    reapExpiredLocked(now);
    while (payload_bytes_ > config_.max_bytes - bytes) {
        if (index_.empty()) {
            ++rejected_puts_;
            return false;
        }
        std::string victim = policy_->selectVictim(now);
        auto it = index_.find(victim);
        if (it == index_.end()) {
            policy_->onRemove(victim);
            continue;
        }
        removeLocked(it);
        ++evictions_;
    }

    Entry entry;
    entry.payload = std::move(payload);
    Duration headroom = TimePoint::max() - now;
    entry.expires_at = ttl >= headroom ? TimePoint::max() : now + ttl;
    entry.meta.key = key;
    entry.meta.bytes = bytes;
    entry.meta.fetch_cost = std::max(fetch_cost, Duration::zero());
    entry.meta.inserted_at = now;
    entry.meta.last_access = now;

    auto [it, inserted] = index_.emplace(key, std::move(entry));
    if (!inserted) {
        ++rejected_puts_;
        return false;
    }
    try {
        policy_->onInsert(it->second.meta);
    } catch (...) {
        policy_->onRemove(key);
        index_.erase(it);
        throw;
    }
    payload_bytes_ += bytes;
    return true;
}

bool Cache::erase(const std::string& key) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = index_.find(key);
    if (it == index_.end()) return false;
    removeLocked(it);
    return true;
}

void Cache::clear() {
    std::lock_guard<std::mutex> lock(mu_);
    index_.clear();
    policy_->clear();
    payload_bytes_ = 0;
}

Cache::Stats Cache::stats() const {
    std::lock_guard<std::mutex> lock(mu_);
    return {index_.size(), payload_bytes_, config_.max_bytes, config_.max_entry_bytes,
            evictions_, expirations_, rejected_puts_};
}

bool Cache::check_invariants() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::size_t sum = 0;
    for (const auto& pair : index_) {
        if (pair.second.meta.key != pair.first || pair.second.meta.bytes != pair.second.payload.size() ||
            pair.second.meta.bytes > config_.max_entry_bytes ||
            sum > std::numeric_limits<std::size_t>::max() - pair.second.meta.bytes) {
            return false;
        }
        sum += pair.second.meta.bytes;
    }
    return sum == payload_bytes_ && payload_bytes_ <= config_.max_bytes &&
           policy_->size() == index_.size();
}

}  // namespace netcache
