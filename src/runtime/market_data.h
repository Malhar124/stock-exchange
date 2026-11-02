#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <tuple>
#include <unordered_map>

#include "core/types.h"
#include "matching/events.h"

namespace exchange::runtime {

struct LevelChange {
  core::Symbol symbol{};
  core::Side side{core::Side::BUY};
  core::Price price{0};
  core::Quantity qty{0}; // 0 means the level is gone
};

struct SymbolBook {
  std::map<core::Price, core::Quantity, std::greater<>> bids;
  std::map<core::Price, core::Quantity, std::less<>> asks;
  core::Price last_trade_price{0};
};

// Aggregated price-level (L2) book built only from engine events, so the
// matching engine needs no read API. Only visible quantity is shown: iceberg
// hidden quantity appears when a slice is refilled. Order IDs must be unique
// across symbols, which the gateway guarantees by assigning them.
class MarketDataBook {
public:
  void apply(const matching::Event &event) {
    using matching::EventType;
    switch (event.type) {
    case EventType::ORDER_RESTED: {
      const auto &value = event.payload.order_rested;
      auto [it, inserted] = orders_.try_emplace(
          value.order_id, Resting{value.symbol, value.side, value.price, 0});
      Resting &order = it->second;
      if (!inserted && order.price != value.price) {
        set_visible(order, 0);
        order.price = value.price;
      }
      set_visible(order, value.qty);
      break;
    }
    case EventType::ORDER_PARTIALLY_FILLED: {
      const auto &value = event.payload.order_partially_filled;
      if (Resting *order = find(value.order_id)) {
        const core::Quantity filled = std::min(value.filled_qty, order->visible);
        set_visible(*order, order->visible - filled);
      }
      break;
    }
    case EventType::ORDER_FILLED:
      remove(event.payload.order_filled.order_id);
      break;
    case EventType::ORDER_CANCELED:
      remove(event.payload.order_canceled.order_id);
      break;
    case EventType::ORDER_REDUCED: {
      const auto &value = event.payload.order_reduced;
      if (Resting *order = find(value.order_id)) {
        set_visible(*order, value.new_qty);
      }
      break;
    }
    case EventType::TRADE: {
      const auto &value = event.payload.trade;
      books_[value.symbol].last_trade_price = value.price;
      break;
    }
    case EventType::ORDER_ACCEPTED:
    case EventType::ORDER_REJECTED:
      break;
    }
  }

  // Reports the final quantity of every level that changed since the last
  // call, then forgets them.
  template <typename OnLevel> void drain(OnLevel &&on_level) {
    for (const auto &[symbol, side, price] : changed_) {
      on_level(LevelChange{.symbol = symbol,
                           .side = side,
                           .price = price,
                           .qty = level_qty(symbol, side, price)});
    }
    changed_.clear();
  }

  [[nodiscard]] const SymbolBook *book(const core::Symbol &symbol) const {
    const auto it = books_.find(symbol);
    return it == books_.end() ? nullptr : &it->second;
  }

  [[nodiscard]] core::Price last_trade_price(const core::Symbol &symbol) const {
    const SymbolBook *found = book(symbol);
    return found == nullptr ? 0 : found->last_trade_price;
  }

private:
  struct Resting {
    core::Symbol symbol;
    core::Side side;
    core::Price price;
    core::Quantity visible;
  };

  [[nodiscard]] Resting *find(core::OrderId order_id) {
    const auto it = orders_.find(order_id);
    return it == orders_.end() ? nullptr : &it->second;
  }

  void remove(core::OrderId order_id) {
    const auto it = orders_.find(order_id);
    if (it == orders_.end()) {
      return;
    }
    set_visible(it->second, 0);
    orders_.erase(it);
  }

  void set_visible(Resting &order, core::Quantity visible) {
    if (visible == order.visible) {
      return;
    }
    SymbolBook &symbol_book = books_[order.symbol];
    const auto adjust = [&](auto &levels) {
      core::Quantity &level = levels[order.price];
      level = level - order.visible + visible;
      if (level == 0U) {
        levels.erase(order.price);
      }
    };
    if (order.side == core::Side::BUY) {
      adjust(symbol_book.bids);
    } else {
      adjust(symbol_book.asks);
    }
    order.visible = visible;
    changed_.insert({order.symbol, order.side, order.price});
  }

  [[nodiscard]] core::Quantity level_qty(const core::Symbol &symbol,
                                         core::Side side,
                                         core::Price price) const {
    const SymbolBook *symbol_book = book(symbol);
    if (symbol_book == nullptr) {
      return 0;
    }
    if (side == core::Side::BUY) {
      const auto it = symbol_book->bids.find(price);
      return it == symbol_book->bids.end() ? 0 : it->second;
    }
    const auto it = symbol_book->asks.find(price);
    return it == symbol_book->asks.end() ? 0 : it->second;
  }

  std::unordered_map<core::OrderId, Resting> orders_;
  std::map<core::Symbol, SymbolBook, core::SymbolLess> books_;
  std::set<std::tuple<core::Symbol, core::Side, core::Price>> changed_;
};

} // namespace exchange::runtime
