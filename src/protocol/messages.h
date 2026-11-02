#pragma once

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

#include "core/types.h"

// Client <-> gateway wire protocol. Every message is a 4-byte header followed
// by a fixed-size body, sent in the host's (little-endian) byte order.
namespace exchange::protocol {

static_assert(std::endian::native == std::endian::little,
              "The wire protocol assumes a little-endian host.");

enum class MessageType : std::uint8_t {
  // Client to gateway.
  LOGIN = 1,
  NEW_ORDER = 2,
  CANCEL_ORDER = 3,
  MODIFY_ORDER = 4,
  SUBSCRIBE = 5,
  ORDER_STATUS_REQUEST = 6,
  // Gateway to client.
  LOGIN_RESPONSE = 101,
  EXECUTION_REPORT = 102,
  BOOK_SNAPSHOT_BEGIN = 103,
  BOOK_UPDATE = 104,
  BOOK_SNAPSHOT_END = 105,
  TRADE_PRINT = 106,
  ORDER_STATUS_END = 107,
};

struct MessageHeader {
  std::uint16_t body_length{0};
  MessageType type{MessageType::LOGIN};
  std::uint8_t reserved{0};
};

// A participant's shared secret, zero-padded. The gateway checks it only
// when it has an auth file.
using Secret = std::array<char, 32>;

// Copies at most 32 characters; callers reject longer secrets first.
[[nodiscard]] inline Secret make_secret(std::string_view text) noexcept {
  Secret secret{};
  for (std::size_t index = 0; index < secret.size() && index < text.size();
       ++index) {
    secret[index] = text[index];
  }
  return secret;
}

struct Login {
  core::ParticipantId participant_id{0};
  std::uint32_t reserved{0};
  Secret secret{};
};

enum class LoginResult : std::uint8_t {
  ACCEPTED,
  ALREADY_LOGGED_IN,       // another session holds this participant
  INVALID_PARTICIPANT,     // participant 0 is reserved
  SESSION_ALREADY_BOUND,   // this connection already logged in
  AUTHENTICATION_FAILED,   // unknown participant or wrong secret
};

struct LoginResponse {
  core::ParticipantId participant_id{0};
  LoginResult result{LoginResult::ACCEPTED};
  std::array<std::uint8_t, 3> reserved{};
};

struct NewOrder {
  std::uint64_t client_order_id{0};
  core::Symbol symbol{};
  core::Side side{core::Side::BUY};
  core::OrderType type{core::OrderType::LIMIT};
  core::SelfTradePrevention stp{core::SelfTradePrevention::NONE};
  std::uint8_t reserved{0};
  core::Quantity qty{0};
  core::Price price{0};
  core::Price trigger_price{0};
  core::Quantity display_qty{0};
  std::uint32_t reserved2{0};
};

struct CancelOrder {
  std::uint64_t client_order_id{0};
};

struct ModifyOrder {
  std::uint64_t client_order_id{0};
  core::Price price{0};
  core::Quantity qty{0};
  std::uint32_t reserved{0};
};

// An empty symbol subscribes to every symbol.
struct Subscribe {
  core::Symbol symbol{};
};

struct OrderStatusRequest {
  std::uint32_t reserved{0};
};

enum class ExecKind : std::uint8_t {
  ACCEPTED,
  RESTED,           // qty = visible quantity now resting
  REJECTED,         // a new order was refused; it never existed
  REQUEST_REJECTED, // a cancel or modify was refused; the order is unchanged
  REDUCED,          // qty = new visible quantity
  REPLACED,         // a modify replaced the order; ACCEPTED follows
  TRADE,            // one fill: qty and price of this trade, match_id set
  PARTIALLY_FILLED, // qty = quantity just filled
  FILLED,           // qty = quantity just filled; nothing left
  CANCELED,         // qty = quantity canceled
  STATUS,           // reply to ORDER_STATUS_REQUEST; qty = open quantity
};

enum class RejectSource : std::uint8_t { NONE, ENGINE, GATEWAY };

enum class GatewayReason : std::uint16_t {
  NONE,
  NOT_LOGGED_IN,
  UNKNOWN_SYMBOL,
  DUPLICATE_CLIENT_ORDER_ID,
  UNKNOWN_CLIENT_ORDER_ID,
  MAX_ORDER_QTY,
  MAX_NOTIONAL,
  PRICE_BAND,
  MAX_OPEN_ORDERS,
  ENGINE_BUSY,
  INVALID_REQUEST,
  SHUTTING_DOWN,
  PRICE_NOT_ON_TICK,
};

struct ExecutionReport {
  std::uint64_t client_order_id{0};
  core::OrderId exchange_order_id{0};
  core::Symbol symbol{};
  core::Timestamp timestamp{0};
  core::MatchId match_id{0};
  core::Price price{0};
  core::Quantity qty{0};
  core::Quantity leaves_qty{0}; // open quantity after this report
  ExecKind kind{ExecKind::ACCEPTED};
  core::Side side{core::Side::BUY};
  core::OrderType order_type{core::OrderType::LIMIT};
  RejectSource reject_source{RejectSource::NONE};
  std::uint16_t reason{0}; // matching::ReasonCode or GatewayReason
  std::uint16_t reserved{0};
};

// The client should clear its book for the symbol; levels and END follow.
struct BookSnapshotBegin {
  core::Symbol symbol{};
};

struct BookUpdate {
  core::Symbol symbol{};
  core::Price price{0};
  core::Quantity qty{0}; // 0 removes the level
  core::Side side{core::Side::BUY};
  std::array<std::uint8_t, 3> reserved{};
};

struct BookSnapshotEnd {
  core::Symbol symbol{};
  core::Price last_trade_price{0};
};

struct TradePrint {
  core::Symbol symbol{};
  core::Price price{0};
  core::Quantity qty{0};
  std::uint32_t reserved{0};
  core::MatchId match_id{0};
  core::Timestamp timestamp{0};
};

struct OrderStatusEnd {
  std::uint32_t open_orders{0};
};

template <typename Body> struct MessageTraits;

#define EXCHANGE_PROTOCOL_MESSAGE(Body, Type)                                  \
  template <> struct MessageTraits<Body> {                                     \
    static constexpr MessageType type = MessageType::Type;                     \
  };                                                                           \
  static_assert(std::is_trivially_copyable_v<Body>);                           \
  static_assert(sizeof(Body) < 1024U)

EXCHANGE_PROTOCOL_MESSAGE(Login, LOGIN);
EXCHANGE_PROTOCOL_MESSAGE(NewOrder, NEW_ORDER);
EXCHANGE_PROTOCOL_MESSAGE(CancelOrder, CANCEL_ORDER);
EXCHANGE_PROTOCOL_MESSAGE(ModifyOrder, MODIFY_ORDER);
EXCHANGE_PROTOCOL_MESSAGE(Subscribe, SUBSCRIBE);
EXCHANGE_PROTOCOL_MESSAGE(OrderStatusRequest, ORDER_STATUS_REQUEST);
EXCHANGE_PROTOCOL_MESSAGE(LoginResponse, LOGIN_RESPONSE);
EXCHANGE_PROTOCOL_MESSAGE(ExecutionReport, EXECUTION_REPORT);
EXCHANGE_PROTOCOL_MESSAGE(BookSnapshotBegin, BOOK_SNAPSHOT_BEGIN);
EXCHANGE_PROTOCOL_MESSAGE(BookUpdate, BOOK_UPDATE);
EXCHANGE_PROTOCOL_MESSAGE(BookSnapshotEnd, BOOK_SNAPSHOT_END);
EXCHANGE_PROTOCOL_MESSAGE(TradePrint, TRADE_PRINT);
EXCHANGE_PROTOCOL_MESSAGE(OrderStatusEnd, ORDER_STATUS_END);

#undef EXCHANGE_PROTOCOL_MESSAGE

inline constexpr std::size_t MAX_BODY_BYTES = 1024;

template <typename Body>
void append_message(std::vector<std::byte> &out, const Body &body) {
  const MessageHeader header{
      .body_length = static_cast<std::uint16_t>(sizeof(Body)),
      .type = MessageTraits<Body>::type};
  const std::size_t offset = out.size();
  out.resize(offset + sizeof(header) + sizeof(Body));
  std::memcpy(out.data() + offset, &header, sizeof(header));
  std::memcpy(out.data() + offset + sizeof(header), &body, sizeof(Body));
}

struct Frame {
  MessageType type{MessageType::LOGIN};
  std::span<const std::byte> body;
};

enum class FrameStatus { COMPLETE, INCOMPLETE, MALFORMED };

// Reads one frame from the front of `buffer`. On COMPLETE, `consumed` is the
// frame's total size.
[[nodiscard]] inline FrameStatus next_frame(std::span<const std::byte> buffer,
                                            Frame &frame,
                                            std::size_t &consumed) {
  if (buffer.size() < sizeof(MessageHeader)) {
    return FrameStatus::INCOMPLETE;
  }
  MessageHeader header;
  std::memcpy(&header, buffer.data(), sizeof(header));
  if (header.body_length > MAX_BODY_BYTES) {
    return FrameStatus::MALFORMED;
  }
  const std::size_t total = sizeof(header) + header.body_length;
  if (buffer.size() < total) {
    return FrameStatus::INCOMPLETE;
  }
  frame.type = header.type;
  frame.body = buffer.subspan(sizeof(header), header.body_length);
  consumed = total;
  return FrameStatus::COMPLETE;
}

template <typename Body>
[[nodiscard]] bool decode(const Frame &frame, Body &out) {
  if (frame.type != MessageTraits<Body>::type ||
      frame.body.size() != sizeof(Body)) {
    return false;
  }
  std::memcpy(&out, frame.body.data(), sizeof(Body));
  return true;
}

} // namespace exchange::protocol
