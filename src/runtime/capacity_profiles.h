#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <type_traits>

#include "matching/matching_engine.h"

namespace exchange::runtime {

// Engine capacities are compile-time template parameters, so deployments pick
// one of these named profiles. The journal records the profile because
// capacity limits affect results (pool-exhausted rejections), and a replay must
// use the same limits as the live run.
// Sizes are the memory each symbol's book reserves; pages are only touched as
// orders use them.
enum class CapacityProfile : std::uint8_t {
  SMALL,  // ~26 MB per symbol: 65k live orders. Tests, demos, laptops.
  MEDIUM, // ~105 MB per symbol: 262k live orders.
  LARGE,  // ~335 MB per symbol: 1M live orders, the engine's own defaults.
};

using SmallEngine = matching::MatchingEngine<65'536, 4'096, 65'536, 4'096>;
using MediumEngine =
    matching::MatchingEngine<262'144, 8'192, 262'144, 16'384>;
using LargeEngine = matching::MatchingEngine<>;

[[nodiscard]] inline std::string_view
profile_name(CapacityProfile profile) noexcept {
  switch (profile) {
  case CapacityProfile::SMALL:
    return "small";
  case CapacityProfile::MEDIUM:
    return "medium";
  case CapacityProfile::LARGE:
    return "large";
  }
  return "unknown";
}

[[nodiscard]] inline std::optional<CapacityProfile>
parse_profile(std::string_view text) noexcept {
  for (const CapacityProfile profile :
       {CapacityProfile::SMALL, CapacityProfile::MEDIUM,
        CapacityProfile::LARGE}) {
    if (profile_name(profile) == text) {
      return profile;
    }
  }
  return std::nullopt;
}

// Calls `function(std::type_identity<Engine>{})` with the engine type for the
// profile.
template <typename Function>
decltype(auto) with_engine_type(CapacityProfile profile, Function &&function) {
  switch (profile) {
  case CapacityProfile::MEDIUM:
    return function(std::type_identity<MediumEngine>{});
  case CapacityProfile::LARGE:
    return function(std::type_identity<LargeEngine>{});
  case CapacityProfile::SMALL:
    break;
  }
  return function(std::type_identity<SmallEngine>{});
}

} // namespace exchange::runtime
