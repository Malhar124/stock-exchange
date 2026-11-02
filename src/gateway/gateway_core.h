#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "core/clock.h"
#include "core/types.h"
#include "matching/events.h"
#include "matching/matching_engine.h"
#include "protocol/messages.h"
#include "runtime/engine_stage.h"
#include "runtime/journal.h"
#include "runtime/market_data.h"
#include "runtime/metrics.h"

namespace exchange::gateway {

using SessionId = std::uint32_t;
using Secrets = std::unordered_map<core::ParticipantId, protocol::Secret>;

struct RiskLimits {
  core::Quantity max_order_qty{1'000'000};
  std::uint64_t max_notional{0};       // currency units; 0 disables the check
  std::uint32_t price_band_percent{0}; // vs last trade; 0 disables the check
  std::uint32_t max_open_orders{10'000}; // per participant
  bool cancel_on_disconnect{false};
  core::Price tick_size{1}; // prices must be a multiple; 1 allows any price
};

// Session handling, order ownership, risk checks, execution reports and
// market data, independent of sockets. Single-threaded: the I/O thread owns
// it. The gateway assigns exchange order IDs, so client order IDs only need
// to be unique per participant, and a participant can only cancel or modify
// its own orders.
class GatewayCore {
public:
  // Hands a request to the engine thread; false if the queue is full.
  using Submit = std::function<bool(const runtime::GatewayRequest &)>;

  GatewayCore(const std::vector<core::Symbol> &symbols, RiskLimits limits,
              Submit submit, runtime::GatewayMetrics *metrics = nullptr)
      : symbols_(symbols.begin(), symbols.end()), limits_(limits),
        submit_(std::move(submit)), metrics_(metrics) {}

  [[nodiscard]] SessionId open_session() {
    const SessionId id = next_session_id_++;
    sessions_.emplace(id, Session{});
    if (metrics_ != nullptr) {
      ++metrics_->sessions_opened;
    }
    return id;
  }

  void close_session(SessionId session_id) {
    const auto it = sessions_.find(session_id);
    if (it == sessions_.end()) {
      return;
    }
    const core::ParticipantId participant = it->second.participant;
    sessions_.erase(it);
    if (participant == 0) {
      return;
    }
    session_by_participant_.erase(participant);
    const auto owned = participant_orders_.find(participant);
    if (limits_.cancel_on_disconnect && accepting_ &&
        owned != participant_orders_.end()) {
      for (const core::OrderId order_id : owned->second) {
        const OrderInfo &info = orders_.at(order_id);
        if (info.live && !submit_cancel(order_id, info)) {
          pending_cancels_.push_back(order_id);
        }
      }
    }
  }

  // Sends the cancel-on-disconnect requests the engine queue refused.
  void retry_pending_cancels() {
    std::erase_if(pending_cancels_, [&](core::OrderId order_id) {
      const OrderInfo *info = find_order(order_id);
      return info == nullptr || !info->live || submit_cancel(order_id, *info);
    });
  }

  // Returns false on a protocol violation; the caller should disconnect.
  [[nodiscard]] bool on_message(SessionId session_id,
                                const protocol::Frame &frame) {
    const auto it = sessions_.find(session_id);
    if (it == sessions_.end()) {
      return false;
    }
    Session &session = it->second;
    if (metrics_ != nullptr) {
      ++metrics_->messages_in;
    }

    switch (frame.type) {
    case protocol::MessageType::LOGIN: {
      protocol::Login login;
      return protocol::decode(frame, login) &&
             handle_login(session_id, session, login);
    }
    case protocol::MessageType::NEW_ORDER: {
      protocol::NewOrder message;
      return protocol::decode(frame, message) &&
             (handle_new_order(session, message), true);
    }
    case protocol::MessageType::CANCEL_ORDER: {
      protocol::CancelOrder message;
      return protocol::decode(frame, message) &&
             (handle_cancel(session, message), true);
    }
    case protocol::MessageType::MODIFY_ORDER: {
      protocol::ModifyOrder message;
      return protocol::decode(frame, message) &&
             (handle_modify(session, message), true);
    }
    case protocol::MessageType::SUBSCRIBE: {
      protocol::Subscribe message;
      return protocol::decode(frame, message) &&
             handle_subscribe(session, message);
    }
    case protocol::MessageType::ORDER_STATUS_REQUEST: {
      protocol::OrderStatusRequest message;
      return protocol::decode(frame, message) &&
             (handle_status_request(session), true);
    }
    default:
      return false;
    }
  }

  void on_engine_output(const runtime::EngineOutput &output) {
    switch (output.kind) {
    case runtime::EngineOutput::Kind::BEGIN:
      current_ = output.request;
      replaced_in_batch_ = false;
      touched_.clear();
      break;
    case runtime::EngineOutput::Kind::EVENT:
      handle_event(output.event);
      market_data_.apply(output.event);
      break;
    case runtime::EngineOutput::Kind::END:
      finish_batch(output.request);
      break;
    }
  }

  // Recovery: registers the order a journaled ADD created, before its
  // replayed outputs arrive through on_engine_output().
  void restore_submission(const runtime::JournalRecord &record) {
    const matching::InboundOrder &order = record.order;
    if (order.action != matching::RequestAction::ADD) {
      return;
    }
    register_order(order.order_id,
                   OrderInfo{.participant = order.participant_id,
                             .client_order_id = record.client_order_id,
                             .symbol = order.symbol,
                             .side = order.side,
                             .type = order.order_type,
                             .price = order.price,
                             .leaves_qty = order.qty});
    next_exchange_order_id_ =
        std::max(next_exchange_order_id_, order.order_id + 1U);
  }

  // While shutting down, new requests are refused.
  void set_accepting(bool accepting) noexcept { accepting_ = accepting; }

  // With secrets, a login must carry its participant's secret. Without, any
  // participant ID is trusted, which is only safe on a loopback address.
  void set_secrets(Secrets secrets) { secrets_ = std::move(secrets); }

  [[nodiscard]] std::vector<std::byte> *outbound(SessionId session_id) {
    const auto it = sessions_.find(session_id);
    return it == sessions_.end() ? nullptr : &it->second.out;
  }

  [[nodiscard]] std::size_t open_orders() const noexcept {
    return orders_.size();
  }

  [[nodiscard]] std::size_t session_count() const noexcept {
    return sessions_.size();
  }

  [[nodiscard]] const runtime::MarketDataBook &market_data() const noexcept {
    return market_data_;
  }

private:
  struct Session {
    core::ParticipantId participant{0};
    bool subscribed_all{false};
    std::set<core::Symbol> subscriptions;
    std::vector<std::byte> out;
  };

  struct OrderInfo {
    core::ParticipantId participant{0};
    std::uint64_t client_order_id{0};
    core::Symbol symbol{};
    core::Side side{core::Side::BUY};
    core::OrderType type{core::OrderType::LIMIT};
    core::Price price{0};
    core::Quantity leaves_qty{0};
    bool live{true}; // false once filled, canceled, or rejected
  };

  struct ClientKey {
    core::ParticipantId participant;
    std::uint64_t client_order_id;
    bool operator==(const ClientKey &) const = default;
  };

  struct ClientKeyHash {
    std::size_t operator()(const ClientKey &key) const noexcept {
      return std::hash<std::uint64_t>{}(key.client_order_id *
                                            0x9E3779B97F4A7C15ULL ^
                                        key.participant);
    }
  };

  // Returns false after a failed authentication, so the caller disconnects
  // and every guess costs a new connection.
  [[nodiscard]] bool handle_login(SessionId session_id, Session &session,
                                  const protocol::Login &login) {
    protocol::LoginResponse response{.participant_id = login.participant_id};
    if (session.participant != 0) {
      response.result = protocol::LoginResult::SESSION_ALREADY_BOUND;
    } else if (login.participant_id == 0) {
      response.result = protocol::LoginResult::INVALID_PARTICIPANT;
    } else if (!authenticated(login)) {
      response.result = protocol::LoginResult::AUTHENTICATION_FAILED;
    } else if (session_by_participant_.contains(login.participant_id)) {
      response.result = protocol::LoginResult::ALREADY_LOGGED_IN;
    } else {
      session.participant = login.participant_id;
      session_by_participant_[login.participant_id] = session_id;
    }
    protocol::append_message(session.out, response);
    return response.result != protocol::LoginResult::AUTHENTICATION_FAILED;
  }

  [[nodiscard]] bool authenticated(const protocol::Login &login) const {
    if (secrets_.empty()) {
      return true;
    }
    const auto found = secrets_.find(login.participant_id);
    if (found == secrets_.end()) {
      return false;
    }
    // Compares every byte, so the time taken does not reveal a prefix match.
    unsigned char difference = 0;
    for (std::size_t index = 0; index < found->second.size(); ++index) {
      difference |= static_cast<unsigned char>(found->second[index] ^
                                               login.secret[index]);
    }
    return difference == 0;
  }

  void handle_new_order(Session &session, const protocol::NewOrder &message) {
    protocol::ExecutionReport reject{.client_order_id = message.client_order_id,
                                     .symbol = message.symbol,
                                     .kind = protocol::ExecKind::REJECTED,
                                     .side = message.side,
                                     .order_type = message.type};
    const protocol::NewOrder order = with_market_protection(message);
    const auto reason = validate_new_order(session, order);
    if (reason != protocol::GatewayReason::NONE) {
      send_gateway_reject(session, reject, reason);
      return;
    }

    const core::OrderId order_id = next_exchange_order_id_++;
    const matching::InboundOrder inbound{
        .action = matching::RequestAction::ADD,
        .symbol = order.symbol,
        .order_id = order_id,
        .side = order.side,
        .price = order.price,
        .qty = order.qty,
        .order_type = order.type,
        .timestamp = 0,
        .participant_id = session.participant,
        .trigger_price = order.trigger_price,
        .display_qty = order.display_qty,
        .stp = order.stp,
    };
    if (!submit_(runtime::GatewayRequest{
            .client_order_id = message.client_order_id,
            .received_ns = core::now_ns(),
            .order = inbound})) {
      send_gateway_reject(session, reject, protocol::GatewayReason::ENGINE_BUSY);
      return;
    }
    register_order(order_id,
                   OrderInfo{.participant = session.participant,
                             .client_order_id = message.client_order_id,
                             .symbol = message.symbol,
                             .side = message.side,
                             .type = message.type,
                             .price = order.price,
                             .leaves_qty = message.qty});
  }

  // With a price band, a market order becomes an IOC limited to the band
  // edge (rounded toward the last trade), so it cannot sweep the book at any
  // price. Reports still show it as a market order.
  [[nodiscard]] protocol::NewOrder
  with_market_protection(protocol::NewOrder order) const {
    const core::Price last = market_data_.last_trade_price(order.symbol);
    if (order.type != core::OrderType::MARKET ||
        limits_.price_band_percent == 0U || last <= 0) {
      return order;
    }
    const auto band = static_cast<core::Price>(
        static_cast<double>(last) *
        static_cast<double>(limits_.price_band_percent) / 100.0);
    const core::Price tick = limits_.tick_size;
    order.type = core::OrderType::IOC;
    if (order.side == core::Side::BUY) {
      order.price = (last + band) / tick * tick;
    } else {
      order.price = std::max(tick, (last - band + tick - 1) / tick * tick);
    }
    return order;
  }

  [[nodiscard]] protocol::GatewayReason
  validate_new_order(const Session &session,
                     const protocol::NewOrder &message) const {
    using protocol::GatewayReason;
    if (session.participant == 0) {
      return GatewayReason::NOT_LOGGED_IN;
    }
    if (!accepting_) {
      return GatewayReason::SHUTTING_DOWN;
    }
    if (static_cast<std::uint8_t>(message.side) > 1U ||
        static_cast<std::uint8_t>(message.type) >
            static_cast<std::uint8_t>(core::OrderType::POST_ONLY) ||
        static_cast<std::uint8_t>(message.stp) >
            static_cast<std::uint8_t>(core::SelfTradePrevention::CANCEL_BOTH)) {
      return GatewayReason::INVALID_REQUEST;
    }
    if (!symbols_.contains(message.symbol)) {
      return GatewayReason::UNKNOWN_SYMBOL;
    }
    if (by_client_id_.contains(
            ClientKey{session.participant, message.client_order_id})) {
      return GatewayReason::DUPLICATE_CLIENT_ORDER_ID;
    }
    const auto open = participant_orders_.find(session.participant);
    if (open != participant_orders_.end() &&
        open->second.size() >= limits_.max_open_orders) {
      return GatewayReason::MAX_OPEN_ORDERS;
    }
    const bool has_limit_price = message.type != core::OrderType::MARKET &&
                                 message.type != core::OrderType::STOP;
    return check_price_and_size(message.symbol, message.qty,
                                has_limit_price ? message.price : 0,
                                message.trigger_price);
  }

  [[nodiscard]] protocol::GatewayReason
  check_price_and_size(const core::Symbol &symbol, core::Quantity qty,
                       core::Price limit_price,
                       core::Price trigger_price) const {
    using protocol::GatewayReason;
    if (qty > limits_.max_order_qty) {
      return GatewayReason::MAX_ORDER_QTY;
    }
    if (limit_price % limits_.tick_size != 0 ||
        trigger_price % limits_.tick_size != 0) {
      return GatewayReason::PRICE_NOT_ON_TICK;
    }
    const core::Price last = market_data_.last_trade_price(symbol);
    if (limits_.max_notional > 0U) {
      const core::Price reference =
          limit_price > 0 ? limit_price : (trigger_price > 0 ? trigger_price
                                                             : last);
      if (reference <= 0) {
        return GatewayReason::MAX_NOTIONAL; // a market order with no reference
      }
      const double notional = (static_cast<double>(reference) /
                               static_cast<double>(core::PRICE_SCALE)) *
                              static_cast<double>(qty);
      if (notional > static_cast<double>(limits_.max_notional)) {
        return GatewayReason::MAX_NOTIONAL;
      }
    }
    if (limits_.price_band_percent > 0U && limit_price > 0 && last > 0) {
      const double distance =
          std::fabs(static_cast<double>(limit_price - last)) * 100.0;
      if (distance > static_cast<double>(limits_.price_band_percent) *
                         static_cast<double>(last)) {
        return GatewayReason::PRICE_BAND;
      }
    }
    return GatewayReason::NONE;
  }

  void handle_cancel(Session &session, const protocol::CancelOrder &message) {
    protocol::ExecutionReport reject{
        .client_order_id = message.client_order_id,
        .kind = protocol::ExecKind::REQUEST_REJECTED};
    if (session.participant == 0) {
      send_gateway_reject(session, reject,
                          protocol::GatewayReason::NOT_LOGGED_IN);
      return;
    }
    // The engine may already have stopped, so a queued cancel could be lost.
    if (!accepting_) {
      send_gateway_reject(session, reject,
                          protocol::GatewayReason::SHUTTING_DOWN);
      return;
    }
    const auto found = by_client_id_.find(
        ClientKey{session.participant, message.client_order_id});
    if (found == by_client_id_.end()) {
      send_gateway_reject(session, reject,
                          protocol::GatewayReason::UNKNOWN_CLIENT_ORDER_ID);
      return;
    }
    const OrderInfo &info = orders_.at(found->second);
    fill_order_fields(reject, found->second, info);
    if (!submit_cancel(found->second, info)) {
      send_gateway_reject(session, reject, protocol::GatewayReason::ENGINE_BUSY);
    }
  }

  void handle_modify(Session &session, const protocol::ModifyOrder &message) {
    protocol::ExecutionReport reject{
        .client_order_id = message.client_order_id,
        .kind = protocol::ExecKind::REQUEST_REJECTED};
    if (session.participant == 0) {
      send_gateway_reject(session, reject,
                          protocol::GatewayReason::NOT_LOGGED_IN);
      return;
    }
    if (!accepting_) {
      send_gateway_reject(session, reject,
                          protocol::GatewayReason::SHUTTING_DOWN);
      return;
    }
    const auto found = by_client_id_.find(
        ClientKey{session.participant, message.client_order_id});
    if (found == by_client_id_.end()) {
      send_gateway_reject(session, reject,
                          protocol::GatewayReason::UNKNOWN_CLIENT_ORDER_ID);
      return;
    }
    const OrderInfo &info = orders_.at(found->second);
    fill_order_fields(reject, found->second, info);
    const auto reason =
        check_price_and_size(info.symbol, message.qty, message.price, 0);
    if (reason != protocol::GatewayReason::NONE) {
      send_gateway_reject(session, reject, reason);
      return;
    }
    const matching::InboundOrder order{
        .action = matching::RequestAction::MODIFY,
        .symbol = info.symbol,
        .order_id = found->second,
        .price = message.price,
        .qty = message.qty,
        .participant_id = session.participant,
    };
    if (!submit_(runtime::GatewayRequest{
            .client_order_id = message.client_order_id,
            .received_ns = core::now_ns(),
            .order = order})) {
      send_gateway_reject(session, reject, protocol::GatewayReason::ENGINE_BUSY);
    }
  }

  [[nodiscard]] bool handle_subscribe(Session &session,
                                      const protocol::Subscribe &message) {
    const bool all = core::symbol_view(message.symbol).empty();
    if (!all && !symbols_.contains(message.symbol)) {
      return true; // nothing to send; unknown symbols are ignored
    }
    for (const core::Symbol &symbol : symbols_) {
      if (!all && symbol != message.symbol) {
        continue;
      }
      if (session.subscribed_all || session.subscriptions.contains(symbol)) {
        continue;
      }
      send_snapshot(session, symbol);
      session.subscriptions.insert(symbol);
    }
    session.subscribed_all = session.subscribed_all || all;
    return true;
  }

  void send_snapshot(Session &session, const core::Symbol &symbol) {
    protocol::append_message(session.out,
                             protocol::BookSnapshotBegin{.symbol = symbol});
    const runtime::SymbolBook *book = market_data_.book(symbol);
    if (book != nullptr) {
      for (const auto &[price, qty] : book->bids) {
        protocol::append_message(session.out,
                                 protocol::BookUpdate{.symbol = symbol,
                                                      .price = price,
                                                      .qty = qty,
                                                      .side = core::Side::BUY});
      }
      for (const auto &[price, qty] : book->asks) {
        protocol::append_message(
            session.out, protocol::BookUpdate{.symbol = symbol,
                                              .price = price,
                                              .qty = qty,
                                              .side = core::Side::SELL});
      }
    }
    protocol::append_message(
        session.out,
        protocol::BookSnapshotEnd{
            .symbol = symbol,
            .last_trade_price = market_data_.last_trade_price(symbol)});
  }

  void handle_status_request(Session &session) {
    std::uint32_t count = 0;
    const auto owned = participant_orders_.find(session.participant);
    if (owned != participant_orders_.end()) {
      for (const core::OrderId order_id : owned->second) {
        const OrderInfo &info = orders_.at(order_id);
        if (!info.live) {
          continue;
        }
        protocol::ExecutionReport report{.kind = protocol::ExecKind::STATUS};
        fill_order_fields(report, order_id, info);
        report.qty = info.leaves_qty;
        report.leaves_qty = info.leaves_qty;
        report.price = info.price;
        protocol::append_message(session.out, report);
        ++count;
      }
    }
    protocol::append_message(session.out,
                             protocol::OrderStatusEnd{.open_orders = count});
  }

  void handle_event(const matching::Event &event) {
    using matching::EventType;
    using protocol::ExecKind;
    switch (event.type) {
    case EventType::ORDER_ACCEPTED: {
      const auto &value = event.payload.order_accepted;
      if (OrderInfo *info = find_order(value.order_id)) {
        info->live = true;
        info->leaves_qty = value.qty;
        info->price = value.price;
        report(value.order_id, *info, ExecKind::ACCEPTED, value.timestamp,
               value.qty, value.price);
      }
      break;
    }
    case EventType::ORDER_RESTED: {
      const auto &value = event.payload.order_rested;
      if (OrderInfo *info = find_order(value.order_id)) {
        info->price = value.price;
        report(value.order_id, *info, ExecKind::RESTED, value.timestamp,
               value.qty, value.price);
      }
      break;
    }
    case EventType::ORDER_REJECTED: {
      const auto &value = event.payload.order_rejected;
      const bool rejected_new_order =
          current_.order.action == matching::RequestAction::ADD &&
          current_.order.order_id == value.order_id;
      OrderInfo *info = find_order(value.order_id);
      if (rejected_new_order && info != nullptr) {
        info->live = false;
        info->leaves_qty = 0;
        touched_.push_back(value.order_id);
        report(value.order_id, *info, ExecKind::REJECTED, value.timestamp, 0,
               info->price, protocol::RejectSource::ENGINE,
               static_cast<std::uint16_t>(value.reason_code));
        break;
      }
      // A cancel or modify was refused: tell whoever sent it.
      protocol::ExecutionReport report{
          .client_order_id = current_.client_order_id,
          .exchange_order_id = value.order_id,
          .symbol = current_.order.symbol,
          .timestamp = value.timestamp,
          .kind = ExecKind::REQUEST_REJECTED,
          .reject_source = protocol::RejectSource::ENGINE,
          .reason = static_cast<std::uint16_t>(value.reason_code)};
      if (info != nullptr) {
        fill_order_fields(report, value.order_id, *info);
      }
      send_to_participant(current_.order.participant_id, report);
      break;
    }
    case EventType::ORDER_REDUCED: {
      const auto &value = event.payload.order_reduced;
      if (OrderInfo *info = find_order(value.order_id)) {
        const bool from_modify =
            current_.order.action == matching::RequestAction::MODIFY &&
            current_.order.order_id == value.order_id;
        info->leaves_qty = from_modify ? current_.order.qty : value.new_qty;
        report(value.order_id, *info, ExecKind::REDUCED, value.timestamp,
               value.new_qty, info->price);
      }
      break;
    }
    case EventType::ORDER_PARTIALLY_FILLED: {
      const auto &value = event.payload.order_partially_filled;
      if (OrderInfo *info = find_order(value.order_id)) {
        info->leaves_qty = value.remaining_qty;
        report(value.order_id, *info, ExecKind::PARTIALLY_FILLED,
               value.timestamp, value.filled_qty, value.price);
      }
      break;
    }
    case EventType::ORDER_FILLED: {
      const auto &value = event.payload.order_filled;
      if (OrderInfo *info = find_order(value.order_id)) {
        info->leaves_qty = 0;
        info->live = false;
        touched_.push_back(value.order_id);
        report(value.order_id, *info, ExecKind::FILLED, value.timestamp,
               value.filled_qty, value.price);
      }
      break;
    }
    case EventType::ORDER_CANCELED: {
      const auto &value = event.payload.order_canceled;
      OrderInfo *info = find_order(value.order_id);
      if (info == nullptr) {
        break;
      }
      const bool is_replace =
          current_.order.action == matching::RequestAction::MODIFY &&
          current_.order.order_id == value.order_id && !replaced_in_batch_ &&
          value.reason == matching::ReasonCode::NONE;
      if (is_replace) {
        // The replacement's ACCEPTED follows in this batch.
        replaced_in_batch_ = true;
        report(value.order_id, *info, ExecKind::REPLACED, value.timestamp,
               value.canceled_qty, info->price);
        break;
      }
      info->leaves_qty = 0;
      info->live = false;
      touched_.push_back(value.order_id);
      report(value.order_id, *info, ExecKind::CANCELED, value.timestamp,
             value.canceled_qty, info->price, protocol::RejectSource::NONE,
             static_cast<std::uint16_t>(value.reason));
      break;
    }
    case EventType::TRADE: {
      const auto &value = event.payload.trade;
      for (const core::OrderId order_id :
           {value.buy_order_id, value.sell_order_id}) {
        if (OrderInfo *info = find_order(order_id)) {
          info->leaves_qty -= std::min(info->leaves_qty, value.qty);
          protocol::ExecutionReport trade{.match_id = value.match_id};
          fill_order_fields(trade, order_id, *info);
          trade.kind = ExecKind::TRADE;
          trade.timestamp = value.timestamp;
          trade.qty = value.qty;
          trade.price = value.price;
          send_to_participant(info->participant, trade);
        }
      }
      publish(value.symbol, protocol::TradePrint{.symbol = value.symbol,
                                                 .price = value.price,
                                                 .qty = value.qty,
                                                 .match_id = value.match_id,
                                                 .timestamp = value.timestamp});
      break;
    }
    }
  }

  void finish_batch(const runtime::RequestContext &request) {
    for (const core::OrderId order_id : touched_) {
      const auto it = orders_.find(order_id);
      if (it == orders_.end() || it->second.live) {
        continue;
      }
      by_client_id_.erase(
          ClientKey{it->second.participant, it->second.client_order_id});
      const auto owned = participant_orders_.find(it->second.participant);
      if (owned != participant_orders_.end() &&
          owned->second.erase(order_id) == 1U && owned->second.empty()) {
        participant_orders_.erase(owned);
      }
      orders_.erase(it);
    }
    touched_.clear();

    market_data_.drain([&](const runtime::LevelChange &change) {
      publish(change.symbol, protocol::BookUpdate{.symbol = change.symbol,
                                                  .price = change.price,
                                                  .qty = change.qty,
                                                  .side = change.side});
    });

    if (metrics_ != nullptr && request.received_ns != 0U) {
      metrics_->round_trip.record(core::now_ns() - request.received_ns);
    }
  }

  template <typename Message>
  void publish(const core::Symbol &symbol, const Message &message) {
    for (auto &[session_id, session] : sessions_) {
      if (session.subscribed_all || session.subscriptions.contains(symbol)) {
        protocol::append_message(session.out, message);
      }
    }
  }

  void register_order(core::OrderId order_id, const OrderInfo &info) {
    orders_[order_id] = info;
    by_client_id_[ClientKey{info.participant, info.client_order_id}] =
        order_id;
    participant_orders_[info.participant].insert(order_id);
  }

  [[nodiscard]] bool submit_cancel(core::OrderId order_id,
                                   const OrderInfo &info) {
    return submit_(runtime::GatewayRequest{
        .client_order_id = info.client_order_id,
        .received_ns = core::now_ns(),
        .order = matching::InboundOrder{
            .action = matching::RequestAction::CANCEL,
            .symbol = info.symbol,
            .order_id = order_id,
            .participant_id = info.participant,
        }});
  }

  [[nodiscard]] OrderInfo *find_order(core::OrderId order_id) {
    const auto it = orders_.find(order_id);
    return it == orders_.end() ? nullptr : &it->second;
  }

  static void fill_order_fields(protocol::ExecutionReport &report,
                                core::OrderId order_id, const OrderInfo &info) {
    report.client_order_id = info.client_order_id;
    report.exchange_order_id = order_id;
    report.symbol = info.symbol;
    report.side = info.side;
    report.order_type = info.type;
    report.leaves_qty = info.leaves_qty;
  }

  void report(core::OrderId order_id, const OrderInfo &info,
              protocol::ExecKind kind, core::Timestamp timestamp,
              core::Quantity qty, core::Price price,
              protocol::RejectSource source = protocol::RejectSource::NONE,
              std::uint16_t reason = 0) {
    protocol::ExecutionReport report{};
    fill_order_fields(report, order_id, info);
    report.kind = kind;
    report.timestamp = timestamp;
    report.qty = qty;
    report.price = price;
    report.reject_source = source;
    report.reason = reason;
    send_to_participant(info.participant, report);
  }

  void send_to_participant(core::ParticipantId participant,
                           const protocol::ExecutionReport &report) {
    const auto session = session_by_participant_.find(participant);
    if (session == session_by_participant_.end()) {
      return; // offline; the order state is still tracked
    }
    protocol::append_message(sessions_.at(session->second).out, report);
  }

  void send_gateway_reject(Session &session, protocol::ExecutionReport report,
                           protocol::GatewayReason reason) {
    report.reject_source = protocol::RejectSource::GATEWAY;
    report.reason = static_cast<std::uint16_t>(reason);
    protocol::append_message(session.out, report);
    if (metrics_ != nullptr) {
      ++metrics_->gateway_rejects;
    }
  }

  std::set<core::Symbol> symbols_;
  RiskLimits limits_;
  Secrets secrets_;
  Submit submit_;
  runtime::GatewayMetrics *metrics_{nullptr};
  bool accepting_{true};

  std::unordered_map<SessionId, Session> sessions_;
  std::unordered_map<core::ParticipantId, SessionId> session_by_participant_;
  SessionId next_session_id_{1};

  std::unordered_map<core::OrderId, OrderInfo> orders_;
  std::unordered_map<ClientKey, core::OrderId, ClientKeyHash> by_client_id_;
  // Open orders per participant, in exchange order ID (arrival) order.
  std::unordered_map<core::ParticipantId, std::set<core::OrderId>>
      participant_orders_;
  std::vector<core::OrderId> pending_cancels_; // see retry_pending_cancels()
  core::OrderId next_exchange_order_id_{1};

  runtime::RequestContext current_{};
  bool replaced_in_batch_{false};
  std::vector<core::OrderId> touched_;

  runtime::MarketDataBook market_data_;
};

} // namespace exchange::gateway
