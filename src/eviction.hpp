#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>

namespace netcache {

struct EntryMeta {
    std::string key;
    std::size_t bytes = 0;
    std::chrono::nanoseconds fetch_cost{0};
    std::chrono::steady_clock::time_point inserted_at{};
    std::chrono::steady_clock::time_point last_access{};
    std::uint64_t hits = 0;
};

class EvictionPolicy {
public:
    using TimePoint = std::chrono::steady_clock::time_point;
    virtual ~EvictionPolicy() = default;
    virtual void onInsert(const EntryMeta& meta) = 0;
    virtual void onHit(const EntryMeta& meta, TimePoint now) = 0;
    virtual void onRemove(const std::string& key) = 0;
    virtual std::string selectVictim(TimePoint now) = 0;
    virtual std::size_t size() const = 0;
    virtual void clear() = 0;
};

class LruPolicy final : public EvictionPolicy {
public:
    void onInsert(const EntryMeta& meta) override;
    void onHit(const EntryMeta& meta, TimePoint now) override;
    void onRemove(const std::string& key) override;
    std::string selectVictim(TimePoint now) override;
    std::size_t size() const override { return positions_.size(); }
    void clear() override;

private:
    std::list<std::string> order_;
    std::unordered_map<std::string, std::list<std::string>::iterator> positions_;
};

struct CostAwareParams {
    double alpha = 1.0;
    double beta = 1.0;
};

class CostAwarePolicy final : public EvictionPolicy {
public:
    explicit CostAwarePolicy(CostAwareParams params);
    void onInsert(const EntryMeta& meta) override;
    void onHit(const EntryMeta& meta, TimePoint now) override;
    void onRemove(const std::string& key) override;
    std::string selectVictim(TimePoint now) override;
    std::size_t size() const override { return entries_.size(); }
    void clear() override { entries_.clear(); }
    double scoreOf(const std::string& key, TimePoint now) const;

private:
    CostAwareParams params_;
    std::unordered_map<std::string, EntryMeta> entries_;
};

enum class EvictionKind { Lru, CostAware };

std::unique_ptr<EvictionPolicy> makeEvictionPolicy(EvictionKind kind, const CostAwareParams& params);

}  // namespace netcache
