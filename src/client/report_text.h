#pragma once

#include <string>
#include <string_view>

#include "matching/events.h"
#include "protocol/messages.h"
#include "text/order_text.h"

namespace exchange::client {

[[nodiscard]] inline std::string_view
gateway_reason_name(protocol::GatewayReason reason) noexcept {
  using protocol::GatewayReason;
  switch (reason) {
  case GatewayReason::NONE:
    return "NONE";
  case GatewayReason::NOT_LOGGED_IN:
    return "NOT_LOGGED_IN";
  case GatewayReason::UNKNOWN_SYMBOL:
    return "UNKNOWN_SYMBOL";
  case GatewayReason::DUPLICATE_CLIENT_ORDER_ID:
    return "DUPLICATE_CLIENT_ORDER_ID";
  case GatewayReason::UNKNOWN_CLIENT_ORDER_ID:
    return "UNKNOWN_CLIENT_ORDER_ID";
  case GatewayReason::MAX_ORDER_QTY:
    return "MAX_ORDER_QTY";
  case GatewayReason::MAX_NOTIONAL:
    return "MAX_NOTIONAL";
  case GatewayReason::PRICE_BAND:
    return "PRICE_BAND";
  case GatewayReason::MAX_OPEN_ORDERS:
    return "MAX_OPEN_ORDERS";
  case GatewayReason::ENGINE_BUSY:
    return "ENGINE_BUSY";
  case GatewayReason::INVALID_REQUEST:
    return "INVALID_REQUEST";
  case GatewayReason::SHUTTING_DOWN:
    return "SHUTTING_DOWN";
  case GatewayReason::PRICE_NOT_ON_TICK:
    return "PRICE_NOT_ON_TICK";
  }
  return "UNKNOWN";
}

[[nodiscard]] inline std::string_view
exec_kind_name(protocol::ExecKind kind) noexcept {
  using protocol::ExecKind;
  switch (kind) {
  case ExecKind::ACCEPTED:
    return "ACCEPTED";
  case ExecKind::RESTED:
    return "RESTED";
  case ExecKind::REJECTED:
    return "REJECTED";
  case ExecKind::REQUEST_REJECTED:
    return "REQUEST_REJECTED";
  case ExecKind::REDUCED:
    return "REDUCED";
  case ExecKind::REPLACED:
    return "REPLACED";
  case ExecKind::TRADE:
    return "TRADE";
  case ExecKind::PARTIALLY_FILLED:
    return "PARTIALLY_FILLED";
  case ExecKind::FILLED:
    return "FILLED";
  case ExecKind::CANCELED:
    return "CANCELED";
  case ExecKind::STATUS:
    return "OPEN";
  }
  return "UNKNOWN";
}

[[nodiscard]] inline std::string
reason_text(const protocol::ExecutionReport &report) {
  if (report.reject_source == protocol::RejectSource::GATEWAY) {
    return std::string(gateway_reason_name(
        static_cast<protocol::GatewayReason>(report.reason)));
  }
  return std::string(
      text::reason_name(static_cast<matching::ReasonCode>(report.reason)));
}

// e.g. "TRADE id=3 AAPL BUY 50 @ 101.0000 match=7 open=50"
[[nodiscard]] inline std::string
format_report(const protocol::ExecutionReport &report) {
  using protocol::ExecKind;
  std::string out = std::string(exec_kind_name(report.kind)) +
                    " id=" + std::to_string(report.client_order_id);
  if (!core::symbol_view(report.symbol).empty()) {
    out += ' ' + std::string(core::symbol_view(report.symbol)) + ' ' +
           std::string(text::side_name(report.side));
  }
  switch (report.kind) {
  case ExecKind::REJECTED:
  case ExecKind::REQUEST_REJECTED:
    out += " reason=" + reason_text(report);
    return out;
  case ExecKind::ACCEPTED:
    out += ' ' + std::to_string(report.qty) + ' ' +
           std::string(text::order_type_name(report.order_type));
    if (report.price != 0) {
      out += " @ " + text::format_price(report.price);
    }
    break;
  case ExecKind::TRADE:
    out += ' ' + std::to_string(report.qty) + " @ " +
           text::format_price(report.price) +
           " match=" + std::to_string(report.match_id);
    break;
  case ExecKind::CANCELED:
    out += ' ' + std::to_string(report.qty);
    if (report.reason != 0) {
      out += " reason=" + reason_text(report);
    }
    break;
  default:
    out += ' ' + std::to_string(report.qty) + " @ " +
           text::format_price(report.price);
    break;
  }
  out += " open=" + std::to_string(report.leaves_qty);
  return out;
}

} // namespace exchange::client
