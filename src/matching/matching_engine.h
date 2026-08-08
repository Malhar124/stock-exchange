#pragma once

#include <map>
#include <memory>
#include <span>

#include "core/types.h"
#include "matching/order_book.h"

namespace exchange::matching {

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
  void prepare_symbols(std::span<const core::Symbol> symbols) {
    for (const core::Symbol &symbol : symbols) {
      (void)book_for(symbol);
    }
  }

  std::span<const Event> process_order(const InboundOrder &order) {
    Book &book = book_for(order.symbol);
    switch (order.action) {
    case RequestAction::ADD:
      return book.add_order(order.order_id, order.side, order.price, order.qty,
                            order.order_type, order.timestamp,
                            order.participant_id, order.trigger_price,
                            order.display_qty);
    case RequestAction::CANCEL:
      return book.cancel_order(order.order_id, order.timestamp);
    case RequestAction::MODIFY:
      return book.modify_order(order.order_id, order.qty, order.price,
                               order.timestamp);
    }

    return {};
  }

  [[nodiscard]] const Book *
  find_book(const core::Symbol &symbol) const noexcept {
    const auto book_it = books_.find(symbol);
    if (book_it == books_.end()) {
      return nullptr;
    }
    return book_it->second.get();
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
