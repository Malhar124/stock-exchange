#pragma once

#include <sys/stat.h>

#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

#include "core/types.h"
#include "gateway/gateway_core.h"
#include "runtime/capacity_profiles.h"
#include "runtime/journal.h"
#include "text/order_text.h"

namespace exchange::server {

struct ServerConfig {
  std::string listen_address{"127.0.0.1"};
  std::uint16_t port{9000};
  std::vector<core::Symbol> symbols{core::make_symbol("AAPL"),
                                    core::make_symbol("MSFT"),
                                    core::make_symbol("GOOG")};
  runtime::CapacityProfile capacity_profile{runtime::CapacityProfile::SMALL};
  std::string journal_path{"data/exchange.journal"}; // empty disables it
  runtime::JournalSync journal_sync{runtime::JournalSync::BATCH};
  gateway::RiskLimits limits{.max_order_qty = 10'000,
                             .price_band_percent = 10,
                             .tick_size = core::PRICE_SCALE / 100};
  std::string auth_file; // empty: no authentication (loopback only)
  std::uint32_t metrics_interval_seconds{10}; // 0 disables periodic metrics
};

namespace detail {

[[nodiscard]] inline std::string_view trim(std::string_view text) {
  while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
    text.remove_prefix(1);
  }
  while (!text.empty() && (text.back() == ' ' || text.back() == '\t' ||
                           text.back() == '\r')) {
    text.remove_suffix(1);
  }
  return text;
}

[[nodiscard]] inline bool parse_bool(std::string_view text, bool &out) {
  if (text == "true" || text == "yes" || text == "1") {
    out = true;
    return true;
  }
  if (text == "false" || text == "no" || text == "0") {
    out = false;
    return true;
  }
  return false;
}

template <typename Integer>
[[nodiscard]] bool parse_number(std::string_view text, Integer &out) {
  const auto value = text::parse_integer<Integer>(text);
  if (!value) {
    return false;
  }
  out = *value;
  return true;
}

} // namespace detail

// Applies one `key = value` setting. Used for config files and --set.
[[nodiscard]] inline bool apply_setting(ServerConfig &config,
                                        std::string_view key,
                                        std::string_view value,
                                        std::string &error) {
  bool ok = true;
  if (key == "listen_address") {
    config.listen_address = std::string(value);
  } else if (key == "port") {
    ok = detail::parse_number(value, config.port);
  } else if (key == "symbols") {
    config.symbols.clear();
    std::size_t start = 0;
    while (start <= value.size()) {
      const std::size_t comma = value.find(',', start);
      const std::string_view symbol = detail::trim(value.substr(
          start, comma == std::string_view::npos ? std::string_view::npos
                                                 : comma - start));
      if (symbol.empty() || symbol.size() > core::Symbol{}.size()) {
        ok = false;
        break;
      }
      config.symbols.push_back(core::make_symbol(symbol));
      if (comma == std::string_view::npos) {
        break;
      }
      start = comma + 1;
    }
    ok = ok && !config.symbols.empty();
  } else if (key == "capacity_profile") {
    const auto profile = runtime::parse_profile(value);
    ok = profile.has_value();
    if (ok) {
      config.capacity_profile = *profile;
    }
  } else if (key == "journal_path") {
    config.journal_path = std::string(value);
  } else if (key == "journal_fsync") {
    if (value == "batch") {
      config.journal_sync = runtime::JournalSync::BATCH;
    } else if (value == "none") {
      config.journal_sync = runtime::JournalSync::NONE;
    } else {
      ok = false;
    }
  } else if (key == "max_order_qty") {
    ok = detail::parse_number(value, config.limits.max_order_qty);
  } else if (key == "max_notional") {
    ok = detail::parse_number(value, config.limits.max_notional);
  } else if (key == "price_band_percent") {
    ok = detail::parse_number(value, config.limits.price_band_percent);
  } else if (key == "max_open_orders_per_participant") {
    ok = detail::parse_number(value, config.limits.max_open_orders);
  } else if (key == "cancel_on_disconnect") {
    ok = detail::parse_bool(value, config.limits.cancel_on_disconnect);
  } else if (key == "tick_size") {
    const auto tick = text::parse_price(value);
    ok = tick.has_value() && *tick > 0;
    if (ok) {
      config.limits.tick_size = *tick;
    }
  } else if (key == "auth_file") {
    config.auth_file = std::string(value);
  } else if (key == "metrics_interval_seconds") {
    ok = detail::parse_number(value, config.metrics_interval_seconds);
  } else {
    error = "unknown setting: " + std::string(key);
    return false;
  }
  if (!ok) {
    error = "invalid value for " + std::string(key) + ": " + std::string(value);
  }
  return ok;
}

[[nodiscard]] inline bool load_config(const std::string &path,
                                      ServerConfig &config,
                                      std::string &error) {
  std::ifstream input(path);
  if (!input) {
    error = path + ": cannot open";
    return false;
  }
  std::string line;
  std::size_t line_number = 0;
  while (std::getline(input, line)) {
    ++line_number;
    std::string_view content = line;
    if (const std::size_t hash = content.find('#');
        hash != std::string_view::npos) {
      content = content.substr(0, hash);
    }
    content = detail::trim(content);
    if (content.empty()) {
      continue;
    }
    const std::size_t equals = content.find('=');
    if (equals == std::string_view::npos) {
      error = path + ':' + std::to_string(line_number) +
              ": expected 'key = value'";
      return false;
    }
    std::string setting_error;
    if (!apply_setting(config, detail::trim(content.substr(0, equals)),
                       detail::trim(content.substr(equals + 1)),
                       setting_error)) {
      error = path + ':' + std::to_string(line_number) + ": " + setting_error;
      return false;
    }
  }
  return true;
}

// Reads `<participant> <secret>` lines ('#' starts a comment). The file must
// not be readable by group or others, and each secret must be 16 to 32
// characters without spaces.
[[nodiscard]] inline bool load_secrets(const std::string &path,
                                       gateway::Secrets &secrets,
                                       std::string &error) {
  secrets.clear();
  struct stat info {};
  if (::stat(path.c_str(), &info) != 0) {
    error = path + ": cannot open";
    return false;
  }
  if ((info.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
    error = path + ": readable by group or others; run chmod 600 " + path;
    return false;
  }
  std::ifstream input(path);
  if (!input) {
    error = path + ": cannot open";
    return false;
  }
  std::string line;
  std::size_t line_number = 0;
  while (std::getline(input, line)) {
    ++line_number;
    std::string_view content = line;
    if (const std::size_t hash = content.find('#');
        hash != std::string_view::npos) {
      content = content.substr(0, hash);
    }
    const auto words = text::split_words(content);
    if (words.empty()) {
      continue;
    }
    const auto participant =
        words.size() == 2U ? text::parse_integer<core::ParticipantId>(words[0])
                           : std::nullopt;
    const bool valid = participant.has_value() && *participant != 0 &&
                       words[1].size() >= 16U &&
                       words[1].size() <= protocol::Secret{}.size();
    if (!valid || !secrets
                       .emplace(*participant, protocol::make_secret(words[1]))
                       .second) {
      error = path + ':' + std::to_string(line_number) +
              ": expected '<participant> <secret>', a non-zero participant "
              "listed once, and a secret of 16 to 32 characters";
      return false;
    }
  }
  if (secrets.empty()) {
    error = path + ": no participants";
    return false;
  }
  return true;
}

} // namespace exchange::server
