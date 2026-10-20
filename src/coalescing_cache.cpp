#include "coalescing_cache.hpp"

#include <exception>
#include <utility>

namespace netcache {

const char* outcomeName(Outcome o) {
    switch (o) {
        case Outcome::Hit: return "Hit";
        case Outcome::MissStored: return "MissStored";
        case Outcome::MissUncacheable: return "MissUncacheable";
        case Outcome::Coalesced: return "Coalesced";
        case Outcome::UpstreamFailure: return "UpstreamFailure";
        case Outcome::UpstreamTimeout: return "UpstreamTimeout";
        case Outcome::WaitTimeout: return "WaitTimeout";
        case Outcome::Cancelled: return "Cancelled";
    }
    return "?";
}

// Completes the owner's flight on EVERY exit path. Created before the fetch runs; if the owner
// never calls complete() (exception escaping, early return) the destructor publishes a failure.
class CoalescingCache::OwnerGuard {
public:
    OwnerGuard(CoalescingCache& self, std::string key, std::shared_ptr<Flight> flight)
        : self_(self), key_(std::move(key)), flight_(std::move(flight)) {
        result_.outcome = Outcome::UpstreamFailure;
        result_.detail = "fetch aborted before producing a result";
    }
    OwnerGuard(const OwnerGuard&) = delete;
    OwnerGuard& operator=(const OwnerGuard&) = delete;

    void setResult(LookupResult r) { result_ = std::move(r); }

    ~OwnerGuard() {
        std::lock_guard<std::mutex> lock(self_.mu_);
        // Only erase our own record (a later flight for the same key cannot exist while we are
        // registered, but be defensive about identity).
        auto it = self_.inflight_.find(key_);
        if (it != self_.inflight_.end() && it->second == flight_) self_.inflight_.erase(it);
        flight_->result = std::move(result_);
        flight_->done = true;
        flight_->cv.notify_all();
    }

private:
    CoalescingCache& self_;
    std::string key_;
    std::shared_ptr<Flight> flight_;
    LookupResult result_;
};

CoalescingCache::CoalescingCache(Cache& cache, Options options) : cache_(cache), options_(options) {}

LookupResult CoalescingCache::runFetch(const std::string& key, const FetchFn& fetch, OwnerGuard* guard) {
    ++fetches_;
    LookupResult out;
    Fetched f;
    try {
        f = fetch();  // NO lock held here
    } catch (const std::exception& e) {
        f = Fetched{};
        f.status = Fetched::Status::UpstreamError;
        f.detail = std::string("fetch threw: ") + e.what();
    } catch (...) {
        f = Fetched{};
        f.status = Fetched::Status::UpstreamError;
        f.detail = "fetch threw a non-standard exception";
    }

    switch (f.status) {
        case Fetched::Status::Ok: {
            if (!f.payload) {  // contract violation by the callback: never hand out a null "success"
                out.outcome = Outcome::UpstreamFailure;
                out.detail = "fetch reported Ok without a payload";
                ++failures_;
                break;
            }
            out.payload = f.payload;
            out.detail = f.detail;
            out.outcome = Outcome::MissUncacheable;
            if (f.cacheable) {
                try {
                    if (cache_.put(key, std::string(*f.payload), f.ttl)) out.outcome = Outcome::MissStored;
                } catch (...) {  // e.g. bad_alloc while storing: still a valid response, just not cached
                    out.detail = "cache insert failed";
                }
            }
            if (out.outcome == Outcome::MissStored) ++miss_stored_; else ++miss_uncacheable_;
            break;
        }
        case Fetched::Status::Timeout:
            out.outcome = Outcome::UpstreamTimeout;
            out.detail = f.detail;
            ++timeouts_;
            break;
        case Fetched::Status::UpstreamError:
            out.outcome = Outcome::UpstreamFailure;
            out.detail = f.detail;
            ++failures_;
            break;
    }
    if (guard) guard->setResult(out);  // published to waiters when the guard is destroyed (after the cache put)
    return out;
}

LookupResult CoalescingCache::getOrFetch(const std::string& key, const FetchFn& fetch,
                                         std::chrono::milliseconds waitTimeout) {
    // 1. Fast path: fresh hit, no coalescer lock at all.
    if (auto hit = cache_.get(key)) {
        ++hits_;
        return LookupResult{Outcome::Hit, std::make_shared<const std::string>(std::move(*hit)), {}};
    }

    if (options_.after_first_miss) options_.after_first_miss(key);

    if (!options_.coalesce) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (shutdown_) {
                ++cancelled_;
                return LookupResult{Outcome::Cancelled, nullptr, "shutting down"};
            }
        }
        return runFetch(key, fetch, nullptr);
    }

    std::unique_lock<std::mutex> lock(mu_);
    if (shutdown_) {
        // Still serve a hit that appeared meanwhile; otherwise no new fetches.
        if (auto hit = cache_.get(key)) {
            ++hits_;
            return LookupResult{Outcome::Hit, std::make_shared<const std::string>(std::move(*hit)), {}};
        }
        ++cancelled_;
        return LookupResult{Outcome::Cancelled, nullptr, "shutting down"};
    }

    auto it = inflight_.find(key);
    if (it == inflight_.end()) {
        // Nobody is fetching. The previous owner may have finished between step 1 and now; it
        // stores before it unregisters, so a re-check here cannot miss its result.
        if (auto hit = cache_.get(key)) {
            ++hits_;
            return LookupResult{Outcome::Hit, std::make_shared<const std::string>(std::move(*hit)), {}};
        }
        // 2. Become the owner.
        auto flight = std::make_shared<Flight>();
        inflight_.emplace(key, flight);
        lock.unlock();
        OwnerGuard guard(*this, key, flight);  // from here on every exit path completes the flight
        return runFetch(key, fetch, &guard);
    }

    // 3. Wait for the owner.
    std::shared_ptr<Flight> flight = it->second;
    ++flight->waiters;
    const bool finished =
        flight->cv.wait_for(lock, waitTimeout, [&] { return flight->done || shutdown_; });
    --flight->waiters;

    if (flight->done) {  // a finished result wins over a simultaneous shutdown
        LookupResult r = flight->result;
        if (r.outcome == Outcome::MissStored || r.outcome == Outcome::MissUncacheable) r.outcome = Outcome::Coalesced;
        if (r.outcome == Outcome::Coalesced) ++coalesced_;
        // Failure outcomes are shared as-is; they were already counted once, by the owner.
        return r;
    }
    if (finished) {  // woken by shutdown
        ++cancelled_;
        return LookupResult{Outcome::Cancelled, nullptr, "shutdown while waiting"};
    }
    ++wait_timeouts_;
    return LookupResult{Outcome::WaitTimeout, nullptr, "timed out waiting for the in-flight fetch"};
}

void CoalescingCache::shutdown() {
    std::lock_guard<std::mutex> lock(mu_);
    shutdown_ = true;
    for (auto& kv : inflight_) kv.second->cv.notify_all();
}

CoalescerCounters CoalescingCache::counters() const {
    CoalescerCounters c;
    c.hits = hits_;
    c.miss_stored = miss_stored_;
    c.miss_uncacheable = miss_uncacheable_;
    c.coalesced = coalesced_;
    c.upstream_failures = failures_;
    c.upstream_timeouts = timeouts_;
    c.wait_timeouts = wait_timeouts_;
    c.cancelled = cancelled_;
    c.fetches_started = fetches_;
    return c;
}

std::size_t CoalescingCache::inflightCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return inflight_.size();
}

std::size_t CoalescingCache::waitersFor(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = inflight_.find(key);
    return it == inflight_.end() ? 0 : it->second->waiters;
}

}  // namespace netcache
