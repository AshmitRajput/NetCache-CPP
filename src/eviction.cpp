#include "eviction.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <vector>

namespace netcache {

void LruPolicy::onInsert(const EntryMeta& meta) {
    onRemove(meta.key);
    order_.push_front(meta.key);
    try {
        positions_.emplace(meta.key, order_.begin());
    } catch (...) {
        order_.pop_front();
        throw;
    }
}

void LruPolicy::onHit(const EntryMeta& meta, TimePoint) {
    auto it = positions_.find(meta.key);
    if (it != positions_.end()) order_.splice(order_.begin(), order_, it->second);
}

void LruPolicy::onRemove(const std::string& key) {
    auto it = positions_.find(key);
    if (it == positions_.end()) return;
    order_.erase(it->second);
    positions_.erase(it);
}

std::string LruPolicy::selectVictim(TimePoint) {
    return order_.empty() ? std::string{} : order_.back();
}

void LruPolicy::clear() {
    order_.clear();
    positions_.clear();
}

namespace {

struct Scored {
    const EntryMeta* meta;
    double score;
};

std::vector<Scored> scores(const std::unordered_map<std::string, EntryMeta>& entries,
                           const CostAwareParams& params, EvictionPolicy::TimePoint now) {
    struct Raw {
        const EntryMeta* meta;
        double recency;
        double cost;
        double bytes;
    };
    std::vector<Raw> raw;
    raw.reserve(entries.size());
    double maxRecency = 0.0;
    double maxCost = 0.0;
    double maxBytes = 0.0;
    for (const auto& pair : entries) {
        const EntryMeta& meta = pair.second;
        double idle = std::max(0.0, std::chrono::duration<double>(now - meta.last_access).count());
        double recency = 1.0 / (1.0 + idle);
        double cost = std::max(0.0, std::chrono::duration<double>(meta.fetch_cost).count());
        double bytes = static_cast<double>(std::max<std::size_t>(meta.bytes, 1));
        raw.push_back({&meta, recency, cost, bytes});
        maxRecency = std::max(maxRecency, recency);
        maxCost = std::max(maxCost, cost);
        maxBytes = std::max(maxBytes, bytes);
    }
    std::vector<Scored> result;
    result.reserve(raw.size());
    for (const Raw& item : raw) {
        double normalizedCost = maxCost > 0.0 ? item.cost / maxCost : 0.0;
        result.push_back({item.meta, (params.alpha * item.recency / maxRecency + params.beta * normalizedCost) /
                                         (item.bytes / maxBytes)});
    }
    return result;
}

}  // namespace

CostAwarePolicy::CostAwarePolicy(CostAwareParams params) : params_(params) {
    if (!std::isfinite(params.alpha) || !std::isfinite(params.beta) || params.alpha < 0.0 || params.beta < 0.0 ||
        (params.alpha == 0.0 && params.beta == 0.0)) {
        throw std::invalid_argument("cost-aware weights must be finite, non-negative and not both zero");
    }
}

void CostAwarePolicy::onInsert(const EntryMeta& meta) {
    entries_[meta.key] = meta;
}

void CostAwarePolicy::onHit(const EntryMeta& meta, TimePoint) {
    auto it = entries_.find(meta.key);
    if (it != entries_.end()) it->second = meta;
}

void CostAwarePolicy::onRemove(const std::string& key) {
    entries_.erase(key);
}

std::string CostAwarePolicy::selectVictim(TimePoint now) {
    auto scored = scores(entries_, params_, now);
    if (scored.empty()) return {};
    const Scored* victim = &scored.front();
    for (const Scored& candidate : scored) {
        if (candidate.score < victim->score ||
            (candidate.score == victim->score &&
             (candidate.meta->last_access < victim->meta->last_access ||
              (candidate.meta->last_access == victim->meta->last_access &&
               candidate.meta->key < victim->meta->key)))) {
            victim = &candidate;
        }
    }
    return victim->meta->key;
}

double CostAwarePolicy::scoreOf(const std::string& key, TimePoint now) const {
    for (const Scored& item : scores(entries_, params_, now)) {
        if (item.meta->key == key) return item.score;
    }
    return -1.0;
}

std::unique_ptr<EvictionPolicy> makeEvictionPolicy(EvictionKind kind, const CostAwareParams& params) {
    switch (kind) {
        case EvictionKind::Lru: return std::make_unique<LruPolicy>();
        case EvictionKind::CostAware: return std::make_unique<CostAwarePolicy>(params);
    }
    throw std::invalid_argument("unknown eviction kind");
}

}  // namespace netcache
