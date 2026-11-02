#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <string>

namespace exchange::runtime {

// Lock-free latency histogram: one writer thread, any number of readers.
// Buckets are powers of two split into 8 linear sub-buckets, so a reported
// percentile is within 12.5% of the true value.
class LatencyHistogram {
public:
  static constexpr std::size_t BUCKETS = 8U + (61U * 8U);

  struct Snapshot {
    std::array<std::uint64_t, BUCKETS> counts{};
    std::uint64_t count{0};
    std::uint64_t max{0};

    [[nodiscard]] Snapshot since(const Snapshot &earlier) const noexcept {
      Snapshot delta;
      for (std::size_t index = 0; index < BUCKETS; ++index) {
        delta.counts[index] = counts[index] - earlier.counts[index];
      }
      delta.count = count - earlier.count;
      delta.max = max; // the maximum is cumulative
      return delta;
    }

    // Upper bound of the bucket holding the given percentile (0-100).
    [[nodiscard]] std::uint64_t percentile(double percent) const noexcept {
      if (count == 0U) {
        return 0;
      }
      const auto rank = static_cast<std::uint64_t>(
          (percent / 100.0) * static_cast<double>(count - 1U));
      std::uint64_t seen = 0;
      for (std::size_t index = 0; index < BUCKETS; ++index) {
        seen += counts[index];
        if (seen > rank) {
          return bucket_upper_bound(index);
        }
      }
      return max;
    }
  };

  void record(std::uint64_t value) noexcept {
    buckets_[bucket_index(value)].fetch_add(1, std::memory_order_relaxed);
    count_.fetch_add(1, std::memory_order_relaxed);
    if (value > max_.load(std::memory_order_relaxed)) {
      max_.store(value, std::memory_order_relaxed);
    }
  }

  [[nodiscard]] Snapshot snapshot() const noexcept {
    Snapshot snapshot;
    for (std::size_t index = 0; index < BUCKETS; ++index) {
      snapshot.counts[index] = buckets_[index].load(std::memory_order_relaxed);
    }
    snapshot.count = count_.load(std::memory_order_relaxed);
    snapshot.max = max_.load(std::memory_order_relaxed);
    return snapshot;
  }

  [[nodiscard]] static std::size_t bucket_index(std::uint64_t value) noexcept {
    if (value < 8U) {
      return static_cast<std::size_t>(value);
    }
    const unsigned msb = 63U - static_cast<unsigned>(std::countl_zero(value));
    const std::uint64_t sub = (value >> (msb - 3U)) & 7U;
    return (static_cast<std::size_t>(msb - 2U) * 8U) +
           static_cast<std::size_t>(sub);
  }

  [[nodiscard]] static std::uint64_t
  bucket_upper_bound(std::size_t index) noexcept {
    if (index < 8U) {
      return index;
    }
    const unsigned msb = static_cast<unsigned>(index / 8U) + 2U;
    const std::uint64_t sub = index % 8U;
    const std::uint64_t width = std::uint64_t{1} << (msb - 3U);
    return ((8U + sub) * width) + width - 1U;
  }

private:
  std::array<std::atomic<std::uint64_t>, BUCKETS> buckets_{};
  std::atomic<std::uint64_t> count_{0};
  std::atomic<std::uint64_t> max_{0};
};

struct EngineMetrics {
  LatencyHistogram engine_latency; // matching time per request, ns
  std::atomic<std::uint64_t> requests{0};
  std::atomic<std::uint64_t> events{0};
  std::atomic<std::uint64_t> trades{0};
};

// Written by the gateway thread only.
struct GatewayMetrics {
  LatencyHistogram round_trip; // request received -> results processed, ns
  std::uint64_t messages_in{0};
  std::uint64_t gateway_rejects{0};
  std::uint64_t sessions_opened{0};
  std::uint64_t slow_consumer_disconnects{0};
  std::size_t max_inbound_depth{0};
  std::size_t max_output_depth{0};
};

[[nodiscard]] inline std::string format_ns(std::uint64_t ns) {
  if (ns < 10'000U) {
    return std::to_string(ns) + "ns";
  }
  if (ns < 10'000'000U) {
    return std::to_string(ns / 1'000U) + "us";
  }
  return std::to_string(ns / 1'000'000U) + "ms";
}

} // namespace exchange::runtime
