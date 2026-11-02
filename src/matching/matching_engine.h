#pragma once

#include <map>
#include <memory>
#include <span>

#include "core/types.h"
#include "matching/order_book.h"

namespace exchange::matching {

// Bump when the same sequence of requests can produce different events, so a
// journal recorded by an older engine is not silently replayed differently.
// 1: original engine. 2: stops trigger after the inbound order completes,
// already-triggered stops activate immediately, self-trade prevention.
// 3: a post-only modify cannot cross, sell stops activate highest trigger
// first, every reduction is reported, a no-change modify is rejected.
// 4: an FOK counts hidden iceberg quantity.
inline constexpr std::uint32_t ENGINE_BEHAVIOR_VERSION = 4;

enum class RequestAction : std::uint8_t {
  ADD,
  CANCEL,
  MODIFY,
};

struct InboundOrder {
  RequestAction action{RequestAction::ADD};
  core::Symbol symbol{};
  core::OrderId order_id{0};
  core::Side side{core::Side::BUY};
  core::Price price{0};
  core::Quantity qty{0};
  core::OrderType order_type{core::OrderType::LIMIT};
  core::Timestamp timestamp{0};
  core::ParticipantId participant_id{0};
  core::Price trigger_price{0};
  core::Quantity display_qty{0};
  core::SelfTradePrevention stp{core::SelfTradePrevention::NONE};
};

static_assert(std::is_trivially_copyable_v<RequestAction>);
static_assert(std::is_trivially_copyable_v<InboundOrder>);

template <
    std::size_t OrderCapacity = 1'000'000, std::size_t LevelCapacity = 10'000,
    std::size_t EventCapacity = 262'144, std::size_t StopCapacity = 65'536>
class MatchingEngine {
  using Book =
      OrderBook<OrderCapacity, LevelCapacity, EventCapacity, StopCapacity>;

public:
  using EventOverflow = typename Book::EventOverflow;

  void prepare_symbols(std::span<const core::Symbol> symbols) {
    for (const core::Symbol &symbol : symbols) {
      (void)book_for(symbol);
    }
  }

  // With `overflow`, events beyond the book's reserve go to it in chunks
  // while the request runs (see OrderBook::set_event_overflow).
  std::span<const Event> process_order(const InboundOrder &order,
                                       const EventOverflow *overflow = nullptr) {
    Book &book = book_for(order.symbol);
    book.set_event_overflow(overflow);
    std::span<const Event> events;
    switch (order.action) {
    case RequestAction::ADD:
      events = book.add_order(order.order_id, order.side, order.price,
                              order.qty, order.order_type, order.timestamp,
                              order.participant_id, order.trigger_price,
                              order.display_qty, order.stp);
      break;
    case RequestAction::CANCEL:
      events = book.cancel_order(order.order_id, order.timestamp);
      break;
    case RequestAction::MODIFY:
      events = book.modify_order(order.order_id, order.qty, order.price,
                                 order.timestamp);
      break;
    }
    book.set_event_overflow(nullptr);
    return events;
  }

private:
  Book &book_for(const core::Symbol &symbol) {
    auto [book_it, inserted] = books_.try_emplace(symbol, nullptr);
    if (inserted) {
      book_it->second = std::make_unique<Book>(symbol);
    }
    return *book_it->second;
  }

  std::map<core::Symbol, std::unique_ptr<Book>, core::SymbolLess> books_;
};

} // namespace exchange::matching
