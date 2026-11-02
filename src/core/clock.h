#pragma once

#include <chrono>

#include "core/types.h"

namespace exchange::core {

// Steady-clock nanoseconds, for latency measurements.
[[nodiscard]] inline Timestamp now_ns() noexcept {
  const auto now = std::chrono::steady_clock::now().time_since_epoch();
  return static_cast<Timestamp>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

} // namespace exchange::core
