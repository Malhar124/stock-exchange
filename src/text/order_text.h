#pragma once

#include <charconv>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/types.h"
#include "matching/events.h"
#include "matching/matching_engine.h"

// Human-readable text forms of requests and events. Used by the demo, the
// replay runner, scenario tests, and journal dumps, so every tool prints the
// same thing.
namespace exchange::text {

[[nodiscard]] inline std::string format_price(core::Price price) {
  const bool negative = price < 0;
  const std::uint64_t magnitude =
      negative ? static_cast<std::uint64_t>(-(price + 1)) + 1U
               : static_cast<std::uint64_t>(price);
  const std::uint64_t scale = static_cast<std::uint64_t>(core::PRICE_SCALE);
  std::string fractional = std::to_string(magnitude % scale);
  fractional.insert(0, 4U - fractional.size(), '0');
  return (negative ? "-" : "") + std::to_string(magnitude / scale) + '.' +
         fractional;
}

// Accepts "101", "101.5", "-0.25" and up to four fractional digits.
[[nodiscard]] inline std::optional<core::Price>
parse_price(std::string_view text) noexcept {
  bool negative = false;
  if (!text.empty() && text.front() == '-') {
    negative = true;
    text.remove_prefix(1);
  }
  // from_chars below would accept a second sign, so "--5" would become 5.
  if (text.empty() || text.front() < '0' || text.front() > '9') {
    return std::nullopt;
  }

  const std::size_t dot = text.find('.');
  const std::string_view whole_text = text.substr(0, dot);
  const std::string_view fraction_text =
      dot == std::string_view::npos ? std::string_view{} : text.substr(dot + 1);
  if (whole_text.empty() || fraction_text.size() > 4U ||
      (dot != std::string_view::npos && fraction_text.empty())) {
    return std::nullopt;
  }

  std::int64_t whole = 0;
  const auto [whole_end, whole_error] = std::from_chars(
      whole_text.data(), whole_text.data() + whole_text.size(), whole);
  if (whole_error != std::errc{} ||
      whole_end != whole_text.data() + whole_text.size() ||
      whole > std::numeric_limits<core::Price>::max() / core::PRICE_SCALE) {
    return std::nullopt;
  }

  std::int64_t fraction = 0;
  for (const char digit : fraction_text) {
    if (digit < '0' || digit > '9') {
      return std::nullopt;
    }
    fraction = (fraction * 10) + (digit - '0');
  }
  for (std::size_t index = fraction_text.size(); index < 4U; ++index) {
    fraction *= 10;
  }
  constexpr core::Price MAX = std::numeric_limits<core::Price>::max();
  if (whole == MAX / core::PRICE_SCALE && fraction > MAX % core::PRICE_SCALE) {
    return std::nullopt;
  }

  const core::Price value = (whole * core::PRICE_SCALE) + fraction;
  return negative ? -value : value;
}

[[nodiscard]] inline std::string_view side_name(core::Side side) noexcept {
  return side == core::Side::BUY ? "BUY" : "SELL";
}

[[nodiscard]] inline std::optional<core::Side>
parse_side(std::string_view text) noexcept {
  if (text == "BUY") {
    return core::Side::BUY;
  }
  if (text == "SELL") {
    return core::Side::SELL;
  }
  return std::nullopt;
}

inline constexpr core::OrderType ALL_ORDER_TYPES[] = {
    core::OrderType::LIMIT,      core::OrderType::MARKET,
    core::OrderType::IOC,        core::OrderType::FOK,
    core::OrderType::GTC,        core::OrderType::STOP,
    core::OrderType::STOP_LIMIT, core::OrderType::ICEBERG,
    core::OrderType::POST_ONLY,
};

[[nodiscard]] inline std::string_view
order_type_name(core::OrderType type) noexcept {
  switch (type) {
  case core::OrderType::LIMIT:
    return "LIMIT";
  case core::OrderType::MARKET:
    return "MARKET";
  case core::OrderType::IOC:
    return "IOC";
  case core::OrderType::FOK:
    return "FOK";
  case core::OrderType::GTC:
    return "GTC";
  case core::OrderType::STOP:
    return "STOP";
  case core::OrderType::STOP_LIMIT:
    return "STOP_LIMIT";
  case core::OrderType::ICEBERG:
    return "ICEBERG";
  case core::OrderType::POST_ONLY:
    return "POST_ONLY";
  }
  return "UNKNOWN";
}

[[nodiscard]] inline std::optional<core::OrderType>
parse_order_type(std::string_view text) noexcept {
  for (const core::OrderType type : ALL_ORDER_TYPES) {
    if (order_type_name(type) == text) {
      return type;
    }
  }
  return std::nullopt;
}

inline constexpr core::SelfTradePrevention ALL_STP_MODES[] = {
    core::SelfTradePrevention::NONE,
    core::SelfTradePrevention::CANCEL_RESTING,
    core::SelfTradePrevention::CANCEL_AGGRESSOR,
    core::SelfTradePrevention::CANCEL_BOTH,
};

[[nodiscard]] inline std::string_view
stp_name(core::SelfTradePrevention stp) noexcept {
  switch (stp) {
  case core::SelfTradePrevention::NONE:
    return "NONE";
  case core::SelfTradePrevention::CANCEL_RESTING:
    return "CANCEL_RESTING";
  case core::SelfTradePrevention::CANCEL_AGGRESSOR:
    return "CANCEL_AGGRESSOR";
  case core::SelfTradePrevention::CANCEL_BOTH:
    return "CANCEL_BOTH";
  }
  return "UNKNOWN";
}

[[nodiscard]] inline std::optional<core::SelfTradePrevention>
parse_stp(std::string_view text) noexcept {
  for (const core::SelfTradePrevention stp : ALL_STP_MODES) {
    if (stp_name(stp) == text) {
      return stp;
    }
  }
  return std::nullopt;
}

[[nodiscard]] inline std::string_view
reason_name(matching::ReasonCode reason) noexcept {
  using matching::ReasonCode;
  switch (reason) {
  case ReasonCode::NONE:
    return "NONE";
  case ReasonCode::ZERO_QUANTITY:
    return "ZERO_QUANTITY";
  case ReasonCode::NEGATIVE_PRICE:
    return "NEGATIVE_PRICE";
  case ReasonCode::BOOK_EMPTY:
    return "BOOK_EMPTY";
  case ReasonCode::DUPLICATE_ORDER_ID:
    return "DUPLICATE_ORDER_ID";
  case ReasonCode::ORDER_NOT_FOUND:
    return "ORDER_NOT_FOUND";
  case ReasonCode::ORDER_NOT_ACTIVE:
    return "ORDER_NOT_ACTIVE";
  case ReasonCode::FOK_INSUFFICIENT_LIQUIDITY:
    return "FOK_INSUFFICIENT_LIQUIDITY";
  case ReasonCode::POST_ONLY_WOULD_CROSS:
    return "POST_ONLY_WOULD_CROSS";
  case ReasonCode::INVALID_MODIFICATION:
    return "INVALID_MODIFICATION";
  case ReasonCode::INVALID_ICEBERG_DISPLAY:
    return "INVALID_ICEBERG_DISPLAY";
  case ReasonCode::ORDER_POOL_EXHAUSTED:
    return "ORDER_POOL_EXHAUSTED";
  case ReasonCode::LEVEL_CAPACITY_EXHAUSTED:
    return "LEVEL_CAPACITY_EXHAUSTED";
  case ReasonCode::STOP_CAPACITY_EXHAUSTED:
    return "STOP_CAPACITY_EXHAUSTED";
  case ReasonCode::SELF_TRADE_PREVENTED:
    return "SELF_TRADE_PREVENTED";
  }
  return "UNKNOWN";
}

[[nodiscard]] inline std::string format_event(const matching::Event &event) {
  using matching::EventType;
  std::string out;
  switch (event.type) {
  case EventType::ORDER_ACCEPTED: {
    const auto &value = event.payload.order_accepted;
    out = "ACCEPTED order=" + std::to_string(value.order_id) +
          " side=" + std::string(side_name(value.side)) +
          " price=" + format_price(value.price) +
          " qty=" + std::to_string(value.qty) +
          " type=" + std::string(order_type_name(value.order_type));
    break;
  }
  case EventType::ORDER_RESTED: {
    const auto &value = event.payload.order_rested;
    out = "RESTED order=" + std::to_string(value.order_id) +
          " side=" + std::string(side_name(value.side)) +
          " price=" + format_price(value.price) +
          " qty=" + std::to_string(value.qty);
    break;
  }
  case EventType::ORDER_REJECTED: {
    const auto &value = event.payload.order_rejected;
    out = "REJECTED order=" + std::to_string(value.order_id) +
          " reason=" + std::string(reason_name(value.reason_code));
    break;
  }
  case EventType::ORDER_REDUCED: {
    const auto &value = event.payload.order_reduced;
    out = "REDUCED order=" + std::to_string(value.order_id) +
          " new_qty=" + std::to_string(value.new_qty);
    break;
  }
  case EventType::ORDER_PARTIALLY_FILLED: {
    const auto &value = event.payload.order_partially_filled;
    out = "PARTIALLY_FILLED order=" + std::to_string(value.order_id) +
          " filled=" + std::to_string(value.filled_qty) +
          " remaining=" + std::to_string(value.remaining_qty) +
          " price=" + format_price(value.price);
    break;
  }
  case EventType::ORDER_FILLED: {
    const auto &value = event.payload.order_filled;
    out = "FILLED order=" + std::to_string(value.order_id) +
          " qty=" + std::to_string(value.filled_qty) +
          " price=" + format_price(value.price);
    break;
  }
  case EventType::ORDER_CANCELED: {
    const auto &value = event.payload.order_canceled;
    out = "CANCELED order=" + std::to_string(value.order_id) +
          " qty=" + std::to_string(value.canceled_qty);
    if (value.reason != matching::ReasonCode::NONE) {
      out += " reason=" + std::string(reason_name(value.reason));
    }
    break;
  }
  case EventType::TRADE: {
    const auto &value = event.payload.trade;
    out = "TRADE match=" + std::to_string(value.match_id) +
          " buy_order=" + std::to_string(value.buy_order_id) +
          " sell_order=" + std::to_string(value.sell_order_id) +
          " price=" + format_price(value.price) +
          " qty=" + std::to_string(value.qty);
    break;
  }
  }
  return out;
}

[[nodiscard]] inline std::vector<std::string_view>
split_words(std::string_view line) {
  std::vector<std::string_view> words;
  std::size_t index = 0;
  while (index < line.size()) {
    while (index < line.size() && (line[index] == ' ' || line[index] == '\t')) {
      ++index;
    }
    const std::size_t start = index;
    while (index < line.size() && line[index] != ' ' && line[index] != '\t') {
      ++index;
    }
    if (index > start) {
      words.push_back(line.substr(start, index - start));
    }
  }
  return words;
}

template <typename Integer>
[[nodiscard]] std::optional<Integer> parse_integer(std::string_view text) {
  Integer value{};
  const auto [end, error] =
      std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size()) {
    return std::nullopt;
  }
  return value;
}

struct ParsedCommand {
  matching::InboundOrder order{};
  bool has_timestamp{false};
};

// Command grammar (one request per line):
//   ADD <symbol> <id> <BUY|SELL> <type> <qty> [<price>] [key=value...]
//   CANCEL <symbol> <id> [ts=<n>]
//   MODIFY <symbol> <id> <qty> <price> [ts=<n>]
// ADD keys: trigger=<price> display=<qty> participant=<id> stp=<mode> ts=<n>
[[nodiscard]] inline bool parse_command(std::string_view line,
                                        ParsedCommand &out,
                                        std::string &error) {
  out = ParsedCommand{};
  const std::vector<std::string_view> words = split_words(line);
  if (words.size() < 3U) {
    error = "expected '<ACTION> <symbol> <id> ...'";
    return false;
  }

  matching::InboundOrder &order = out.order;
  const std::string_view action = words[0];
  if (words[1].size() > core::Symbol{}.size()) {
    error = "symbol longer than 8 characters: " + std::string(words[1]);
    return false;
  }
  order.symbol = core::make_symbol(words[1]);

  const auto order_id = parse_integer<core::OrderId>(words[2]);
  if (!order_id) {
    error = "invalid order id: " + std::string(words[2]);
    return false;
  }
  order.order_id = *order_id;

  std::size_t next = 3;
  if (action == "ADD") {
    order.action = matching::RequestAction::ADD;
    if (words.size() < 6U) {
      error = "ADD needs <symbol> <id> <side> <type> <qty>";
      return false;
    }
    const auto side = parse_side(words[3]);
    const auto type = parse_order_type(words[4]);
    const auto qty = parse_integer<core::Quantity>(words[5]);
    if (!side || !type || !qty) {
      error = "invalid side, type, or quantity";
      return false;
    }
    order.side = *side;
    order.order_type = *type;
    order.qty = *qty;
    next = 6;
    if (next < words.size() &&
        words[next].find('=') == std::string_view::npos) {
      const auto price = parse_price(words[next]);
      if (!price) {
        error = "invalid price: " + std::string(words[next]);
        return false;
      }
      order.price = *price;
      ++next;
    }
  } else if (action == "CANCEL") {
    order.action = matching::RequestAction::CANCEL;
  } else if (action == "MODIFY") {
    order.action = matching::RequestAction::MODIFY;
    if (words.size() < 5U) {
      error = "MODIFY needs <symbol> <id> <qty> <price>";
      return false;
    }
    const auto qty = parse_integer<core::Quantity>(words[3]);
    const auto price = parse_price(words[4]);
    if (!qty || !price) {
      error = "invalid quantity or price";
      return false;
    }
    order.qty = *qty;
    order.price = *price;
    next = 5;
  } else {
    error = "unknown action: " + std::string(action);
    return false;
  }

  for (; next < words.size(); ++next) {
    const std::string_view word = words[next];
    const std::size_t equals = word.find('=');
    if (equals == std::string_view::npos) {
      error = "unexpected argument: " + std::string(word);
      return false;
    }
    const std::string_view key = word.substr(0, equals);
    const std::string_view value = word.substr(equals + 1);
    const bool is_add = order.action == matching::RequestAction::ADD;
    bool ok = false;
    if (key == "ts") {
      const auto timestamp = parse_integer<core::Timestamp>(value);
      ok = timestamp.has_value();
      if (ok) {
        order.timestamp = *timestamp;
        out.has_timestamp = true;
      }
    } else if (is_add && key == "trigger") {
      const auto price = parse_price(value);
      ok = price.has_value();
      if (ok) {
        order.trigger_price = *price;
      }
    } else if (is_add && key == "display") {
      const auto qty = parse_integer<core::Quantity>(value);
      ok = qty.has_value();
      if (ok) {
        order.display_qty = *qty;
      }
    } else if (is_add && key == "participant") {
      const auto participant = parse_integer<core::ParticipantId>(value);
      ok = participant.has_value();
      if (ok) {
        order.participant_id = *participant;
      }
    } else if (is_add && key == "stp") {
      const auto stp = parse_stp(value);
      ok = stp.has_value();
      if (ok) {
        order.stp = *stp;
      }
    }
    if (!ok) {
      error = "invalid argument: " + std::string(word);
      return false;
    }
  }

  return true;
}

[[nodiscard]] inline std::string
format_command(const matching::InboundOrder &order, bool include_timestamp) {
  const std::string symbol(core::symbol_view(order.symbol));
  std::string out;
  switch (order.action) {
  case matching::RequestAction::ADD: {
    out = "ADD " + symbol + ' ' + std::to_string(order.order_id) + ' ' +
          std::string(side_name(order.side)) + ' ' +
          std::string(order_type_name(order.order_type)) + ' ' +
          std::to_string(order.qty);
    const bool priceless = order.order_type == core::OrderType::MARKET ||
                           order.order_type == core::OrderType::STOP;
    if (order.price != 0 || !priceless) {
      out += ' ' + format_price(order.price);
    }
    if (order.trigger_price != 0) {
      out += " trigger=" + format_price(order.trigger_price);
    }
    if (order.display_qty != 0) {
      out += " display=" + std::to_string(order.display_qty);
    }
    if (order.participant_id != 0) {
      out += " participant=" + std::to_string(order.participant_id);
    }
    if (order.stp != core::SelfTradePrevention::NONE) {
      out += " stp=" + std::string(stp_name(order.stp));
    }
    break;
  }
  case matching::RequestAction::CANCEL:
    out = "CANCEL " + symbol + ' ' + std::to_string(order.order_id);
    break;
  case matching::RequestAction::MODIFY:
    out = "MODIFY " + symbol + ' ' + std::to_string(order.order_id) + ' ' +
          std::to_string(order.qty) + ' ' + format_price(order.price);
    break;
  }
  if (include_timestamp) {
    out += " ts=" + std::to_string(order.timestamp);
  }
  return out;
}

} // namespace exchange::text
