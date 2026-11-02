#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>

#include "core/fixed_block_resource.h"
#include "core/memory_pool.h"
#include "core/types.h"
#include "matching/events.h"
#include "matching/level.h"

namespace exchange::matching {

template <
    std::size_t OrderCapacity = 1'000'000, std::size_t LevelCapacity = 10'000,
    std::size_t EventCapacity = 262'144, std::size_t StopCapacity = 65'536>
class OrderBook {
  using BidMap = std::pmr::map<core::Price, Level *, std::greater<core::Price>>;
  using AskMap = std::pmr::map<core::Price, Level *, std::less<core::Price>>;
  // Sell stops sort highest first, so both sides activate stops in the order
  // the price crossed their triggers.
  using BuyStopMap =
      std::pmr::multimap<core::Price, Order *, std::less<core::Price>>;
  using SellStopMap =
      std::pmr::multimap<core::Price, Order *, std::greater<core::Price>>;
  // A retired order keeps its ID here with a null pointer.
  using OrderMap = std::pmr::unordered_map<core::OrderId, Order *>;

  // The order index holds live orders plus up to OrderCapacity recently
  // retired IDs (for ORDER_NOT_ACTIVE and duplicate detection), so it never
  // holds more than 2 * OrderCapacity entries. Each entry needs one node
  // (<= 64 bytes) and one bucket pointer.
  static constexpr std::size_t ORDER_INDEX_ENTRIES = OrderCapacity * 2U;
  static constexpr std::size_t ORDER_INDEX_BYTES =
      (ORDER_INDEX_ENTRIES * (64U + 16U)) + 4096U;
  // Level maps only hold live levels (at most LevelCapacity per side) and
  // stop maps only hold parked stops (at most StopCapacity in total).
  static constexpr std::size_t LEVEL_INDEX_BYTES = LevelCapacity * 128U;
  static constexpr std::size_t STOP_INDEX_BYTES = StopCapacity * 128U;

public:
  explicit OrderBook(core::Symbol symbol)
      : symbol_(symbol), order_index_buffer_(ORDER_INDEX_BYTES),
        bid_index_buffer_(LEVEL_INDEX_BYTES),
        ask_index_buffer_(LEVEL_INDEX_BYTES),
        stop_buy_index_buffer_(STOP_INDEX_BYTES),
        stop_sell_index_buffer_(STOP_INDEX_BYTES),
        bids_(std::greater<core::Price>{}, &bid_index_buffer_),
        asks_(std::less<core::Price>{}, &ask_index_buffer_),
        stop_buys_(&stop_buy_index_buffer_),
        stop_sells_(&stop_sell_index_buffer_),
        orders_(&order_index_buffer_),
        retired_ids_(std::make_unique<core::OrderId[]>(OrderCapacity)) {
    orders_.reserve(ORDER_INDEX_ENTRIES);
    events_.reserve(EventCapacity);
  }

  OrderBook(const OrderBook &) = delete;
  OrderBook &operator=(const OrderBook &) = delete;
  OrderBook(OrderBook &&) = delete;
  OrderBook &operator=(OrderBook &&) = delete;

  using EventOverflow = std::function<void(std::span<const Event>)>;

  // While set, full chunks of EventCapacity events go to `overflow` as a
  // request runs, and the request returns only the rest. Without it, a
  // request with more events grows the buffer.
  void set_event_overflow(const EventOverflow *overflow) noexcept {
    overflow_ = overflow;
  }

  std::span<const Event>
  add_order(core::OrderId order_id, core::Side side, core::Price price,
            core::Quantity qty, core::OrderType order_type,
            core::Timestamp timestamp, core::ParticipantId participant_id,
            core::Price trigger_price = 0, core::Quantity display_qty = 0,
            core::SelfTradePrevention stp = core::SelfTradePrevention::NONE) {
    reset_events();

    if (const auto reason =
            validate_new_order(order_id, side, price, qty, order_type,
                               trigger_price, display_qty, participant_id,
                               stp)) {
      emit_rejected(timestamp, order_id, *reason);
      return events();
    }

    Order *order = order_pool_.allocate();
    if (order == nullptr) [[unlikely]] {
      emit_rejected(timestamp, order_id, ReasonCode::ORDER_POOL_EXHAUSTED);
      return events();
    }

    const core::Quantity peak_qty =
        order_type == core::OrderType::ICEBERG ? display_qty : 0U;
    order = std::construct_at(order, Order{
                                         .id = order_id,
                                         .side = side,
                                         .price = price,
                                         .qty = qty,
                                         .display_qty = qty,
                                         .hidden_qty = 0,
                                         .type = order_type,
                                         .stp = stp,
                                         .participant_id = participant_id,
                                         .trigger_price = trigger_price,
                                         .peak_qty = peak_qty,
                                     });

    prepare_visible_slice(*order);
    orders_.insert_or_assign(order_id, order);

    process_inbound_order(*order, timestamp);
    return events();
  }

  std::span<const Event> cancel_order(core::OrderId order_id,
                                      core::Timestamp timestamp = 0) {
    reset_events();

    auto *order = find_active_order(order_id);
    if (order == nullptr) {
      emit_rejected(timestamp, order_id, lookup_reject_reason(order_id));
      return events();
    }

    emit_canceled(timestamp, order_id, order->qty);
    retire_order(*order);
    return events();
  }

  std::span<const Event> modify_order(core::OrderId order_id,
                                      core::Quantity new_qty,
                                      core::Price new_price,
                                      core::Timestamp timestamp = 0) {
    reset_events();

    Order *order = find_active_order(order_id);
    if (order == nullptr) {
      emit_rejected(timestamp, order_id, lookup_reject_reason(order_id));
      return events();
    }

    if (order->is_stop_order()) {
      emit_rejected(timestamp, order_id, ReasonCode::INVALID_MODIFICATION);
      return events();
    }

    if (new_qty == 0) {
      emit_rejected(timestamp, order_id, ReasonCode::ZERO_QUANTITY);
      return events();
    }

    if (new_price < 0) {
      emit_rejected(timestamp, order_id, ReasonCode::NEGATIVE_PRICE);
      return events();
    }

    if (new_price == order->price && new_qty < order->qty) {
      reduce_order_quantity(*order, new_qty, timestamp);
      return events();
    }

    if (new_price == order->price && new_qty == order->qty) {
      emit_rejected(timestamp, order_id, ReasonCode::INVALID_MODIFICATION);
      return events();
    }

    if (order->type == core::OrderType::POST_ONLY &&
        would_cross(order->side, new_price)) {
      emit_rejected(timestamp, order_id, ReasonCode::POST_ONLY_WOULD_CROSS);
      return events();
    }

    const core::Side side = order->side;
    const core::OrderType type = order->type;
    const core::ParticipantId participant_id = order->participant_id;
    const core::SelfTradePrevention stp = order->stp;
    const core::Price trigger_price = order->trigger_price;
    const core::Quantity peak_qty = order->peak_qty;

    emit_canceled(timestamp, order_id, order->qty);
    retire_order(*order);

    Order *replacement = order_pool_.allocate();
    if (replacement == nullptr) [[unlikely]] {
      emit_rejected(timestamp, order_id, ReasonCode::ORDER_POOL_EXHAUSTED);
      return events();
    }

    replacement =
        std::construct_at(replacement, Order{
                                           .id = order_id,
                                           .side = side,
                                           .price = new_price,
                                           .qty = new_qty,
                                           .display_qty = new_qty,
                                           .hidden_qty = 0,
                                           .type = type,
                                           .stp = stp,
                                           .participant_id = participant_id,
                                           .trigger_price = trigger_price,
                                           .peak_qty = peak_qty,
                                       });

    prepare_visible_slice(*replacement);
    orders_[order_id] = replacement;

    process_inbound_order(*replacement, timestamp);
    return events();
  }

private:
  void reset_events() noexcept { events_.clear(); }

  [[nodiscard]] std::span<const Event> events() const {
    return std::span<const Event>(events_);
  }

  [[nodiscard]] const Level *
  opposite_best_level(core::Side side) const noexcept {
    return side == core::Side::BUY ? best_ask_ : best_bid_;
  }

  [[nodiscard]] static bool allows_resting(core::OrderType type) noexcept {
    return type == core::OrderType::LIMIT || type == core::OrderType::GTC ||
           type == core::OrderType::ICEBERG ||
           type == core::OrderType::POST_ONLY ||
           type == core::OrderType::STOP_LIMIT;
  }

  [[nodiscard]] bool would_cross(core::Side side,
                                 core::Price price) const noexcept {
    const Level *best_opposite = opposite_best_level(side);
    if (best_opposite == nullptr)
      return false;
    if (side == core::Side::BUY)
      return best_opposite->price <= price;
    return best_opposite->price >= price;
  }

  [[nodiscard]] static bool
  stp_applies(core::ParticipantId participant_id,
              core::SelfTradePrevention stp) noexcept {
    return stp != core::SelfTradePrevention::NONE && participant_id != 0;
  }

  [[nodiscard]] static bool is_self_trade(const Order &aggressor,
                                          const Order &passive) noexcept {
    return stp_applies(aggressor.participant_id, aggressor.stp) &&
           passive.participant_id == aggressor.participant_id;
  }

  // An FOK must fill completely; hidden iceberg quantity counts, because the
  // iceberg refills at the same price. With self-trade prevention on, the
  // participant's own resting orders provide no liquidity: they are either
  // canceled as the FOK reaches them, or they stop the FOK, so it is
  // rejected if one comes before the full quantity is available.
  [[nodiscard]] std::optional<ReasonCode>
  fok_reject_reason(core::Side side, core::Price limit_price,
                    core::Quantity qty, core::ParticipantId participant_id,
                    core::SelfTradePrevention stp) const noexcept {
    const bool prevents_self_trade = stp_applies(participant_id, stp);
    std::uint64_t available = 0;
    std::optional<ReasonCode> reason = ReasonCode::FOK_INSUFFICIENT_LIQUIDITY;
    const auto walk = [&](const auto &levels, auto crosses) {
      for (const auto &[price, level] : levels) {
        if (!crosses(price)) {
          return;
        }
        for (const Order *order = level->head; order != nullptr;
             order = order->next) {
          if (prevents_self_trade && order->participant_id == participant_id) {
            if (stp == core::SelfTradePrevention::CANCEL_RESTING) {
              continue;
            }
            reason = ReasonCode::SELF_TRADE_PREVENTED;
            return;
          }
          available += order->qty;
          if (available >= qty) {
            reason = std::nullopt;
            return;
          }
        }
      }
    };

    if (side == core::Side::BUY) {
      walk(asks_, [&](core::Price price) { return price <= limit_price; });
    } else {
      walk(bids_, [&](core::Price price) { return price >= limit_price; });
    }
    return reason;
  }

  [[nodiscard]] Order *find_active_order(core::OrderId order_id) noexcept {
    const auto it = orders_.find(order_id);
    return it == orders_.end() ? nullptr : it->second;
  }

  [[nodiscard]] ReasonCode
  lookup_reject_reason(core::OrderId order_id) const noexcept {
    const auto it = orders_.find(order_id);
    if (it == orders_.end()) {
      return ReasonCode::ORDER_NOT_FOUND;
    }
    return ReasonCode::ORDER_NOT_ACTIVE;
  }

  [[nodiscard]] std::optional<ReasonCode>
  validate_new_order(core::OrderId order_id, core::Side side, core::Price price,
                     core::Quantity qty, core::OrderType order_type,
                     core::Price trigger_price, core::Quantity display_qty,
                     core::ParticipantId participant_id,
                     core::SelfTradePrevention stp) const noexcept {
    if (qty == 0) {
      return ReasonCode::ZERO_QUANTITY;
    }

    if (orders_.contains(order_id)) {
      return ReasonCode::DUPLICATE_ORDER_ID;
    }

    const bool requires_limit_price =
        order_type == core::OrderType::LIMIT ||
        order_type == core::OrderType::IOC ||
        order_type == core::OrderType::FOK ||
        order_type == core::OrderType::GTC ||
        order_type == core::OrderType::STOP_LIMIT ||
        order_type == core::OrderType::ICEBERG ||
        order_type == core::OrderType::POST_ONLY;

    if (requires_limit_price && price <= 0) {
      return ReasonCode::NEGATIVE_PRICE;
    }

    if ((order_type == core::OrderType::STOP ||
         order_type == core::OrderType::STOP_LIMIT) &&
        trigger_price <= 0) {
      return ReasonCode::NEGATIVE_PRICE;
    }

    if (order_type == core::OrderType::ICEBERG &&
        (display_qty == 0 || display_qty > qty)) {
      return ReasonCode::INVALID_ICEBERG_DISPLAY;
    }

    if (order_type == core::OrderType::MARKET &&
        opposite_best_level(side) == nullptr) {
      return ReasonCode::BOOK_EMPTY;
    }

    if (order_type == core::OrderType::POST_ONLY && would_cross(side, price)) {
      return ReasonCode::POST_ONLY_WOULD_CROSS;
    }

    if (order_type == core::OrderType::FOK) {
      if (const auto reason =
              fok_reject_reason(side, price, qty, participant_id, stp)) {
        return reason;
      }
    }

    if ((order_type == core::OrderType::STOP ||
         order_type == core::OrderType::STOP_LIMIT) &&
        !stop_already_triggered(side, trigger_price) &&
        parked_stop_count_ >= StopCapacity) {
      return ReasonCode::STOP_CAPACITY_EXHAUSTED;
    }

    return std::nullopt;
  }

  void replenish_iceberg(Order &order, core::Timestamp event_timestamp) {
    Level *level = order.parent_level;
    if (level == nullptr) {
      return;
    }

    level->remove_order(&order);
    const core::Quantity replenished =
        std::min(order.hidden_qty, order.peak_qty);
    order.hidden_qty -= replenished;
    order.display_qty = replenished;
    level->add_order(&order);
    emit_rested(event_timestamp, order);
  }

  // Matches the aggressor against one level until the aggressor is done or
  // the level is retired. `level` must not be used after this returns, since
  // removing its last order frees it. Returns true if self-trade prevention
  // canceled the aggressor.
  bool match_fifo_at_level(Order &aggressor, Level &level,
                           core::Timestamp event_timestamp,
                           core::Price &last_fill_price) {
    while (aggressor.qty > 0) {
      Order *passive = level.front();
      if (passive == nullptr) {
        return false;
      }

      if (is_self_trade(aggressor, *passive)) {
        const core::SelfTradePrevention mode = aggressor.stp;
        bool level_retired = false;
        if (mode != core::SelfTradePrevention::CANCEL_AGGRESSOR) {
          emit_canceled(event_timestamp, passive->id, passive->qty,
                        ReasonCode::SELF_TRADE_PREVENTED);
          level_retired = retire_order(*passive, false);
        }
        if (mode != core::SelfTradePrevention::CANCEL_RESTING) {
          return true;
        }
        if (level_retired) {
          return false;
        }
        continue;
      }

      const core::Quantity trade_qty =
          std::min(aggressor.qty, passive->display_qty);
      if (execute_trade_slice(aggressor, *passive, trade_qty, event_timestamp,
                              last_fill_price)) {
        return false;
      }
    }
    return false;
  }

  void refresh_best_level(core::Side side) noexcept {
    if (side == core::Side::BUY) {
      best_bid_ = first_active_level(bids_);
    } else {
      best_ask_ = first_active_level(asks_);
    }
  }

  template <typename LevelMap>
  [[nodiscard]] static Level *
  first_active_level(const LevelMap &levels) noexcept {
    return levels.empty() ? nullptr : levels.begin()->second;
  }

  // Returns false when a new price level is needed and the level pool is
  // exhausted.
  [[nodiscard]] bool insert_order(Order *order) {
    Level *level = find_or_create_level(order->side, order->price);
    if (level == nullptr) [[unlikely]] {
      return false;
    }

    level->add_order(order);
    if (order->side == core::Side::BUY) {
      if (best_bid_ == nullptr || level->price > best_bid_->price) {
        best_bid_ = level;
      }
    } else if (best_ask_ == nullptr || level->price < best_ask_->price) {
      best_ask_ = level;
    }
    return true;
  }

  [[nodiscard]] Level *find_or_create_level(core::Side side,
                                            core::Price price) {
    if (side == core::Side::BUY) {
      return find_or_create_level_impl(bids_, price);
    }
    return find_or_create_level_impl(asks_, price);
  }

  template <typename LevelMap>
  [[nodiscard]] Level *find_or_create_level_impl(LevelMap &levels,
                                                 core::Price price) {
    const auto level_it = levels.find(price);
    if (level_it != levels.end()) {
      return level_it->second;
    }

    Level *level = allocate_level(price);
    if (level == nullptr) {
      return nullptr;
    }

    levels.emplace(price, level);
    return level;
  }

  [[nodiscard]] Level *allocate_level(core::Price price) noexcept {
    Level *level = level_pool_.allocate();
    if (level == nullptr) {
      return nullptr;
    }

    return std::construct_at(level, Level{
                                        .price = price,
                                        .total_qty = 0,
                                        .order_count = 0,
                                        .head = nullptr,
                                        .tail = nullptr,
                                    });
  }

  // Returns true if retiring the order emptied and freed its price level.
  bool retire_order(Order &order, bool refresh_best = true) {
    bool removed_level = false;
    bool removed_best_level = false;
    const core::Side side = order.side;

    if (order.parent_level != nullptr) {
      Level *level = order.parent_level;
      level->remove_order(&order);
      if (level->is_empty()) {
        removed_level = true;
        removed_best_level = side == core::Side::BUY ? best_bid_ == level
                                                     : best_ask_ == level;
        deactivate_level(side, level->price, level);
      }
    } else if (order.is_stop_order()) {
      erase_stop_reference(order);
    }

    orders_[order.id] = nullptr;
    remember_retired(order.id);

    order_pool_.deallocate(&order);
    if (refresh_best && removed_best_level) {
      refresh_best_level(side);
    }
    return removed_level;
  }

  void remember_retired(core::OrderId order_id) {
    if (retired_count_ < OrderCapacity) {
      retired_ids_[(retired_head_ + retired_count_) % OrderCapacity] =
          order_id;
      ++retired_count_;
      return;
    }

    // A modify re-uses its order ID, so the evicted ID may be live again.
    const auto evicted = orders_.find(retired_ids_[retired_head_]);
    if (evicted != orders_.end() && evicted->second == nullptr) {
      orders_.erase(evicted);
    }
    retired_ids_[retired_head_] = order_id;
    retired_head_ = (retired_head_ + 1U) % OrderCapacity;
  }

  void deactivate_level(core::Side side, core::Price price,
                        Level *level) noexcept {
    if (side == core::Side::BUY) {
      bids_.erase(price);
    } else {
      asks_.erase(price);
    }
    level_pool_.deallocate(level);
  }

  // Returns true if the passive order's price level was retired.
  bool execute_trade_slice(Order &aggressor, Order &passive,
                           core::Quantity trade_qty,
                           core::Timestamp event_timestamp,
                           core::Price &last_fill_price) {
    const core::Price trade_price = passive.price;
    last_fill_price = trade_price;
    last_trade_price_ = trade_price;

    aggressor.qty -= trade_qty;
    passive.qty -= trade_qty;
    passive.display_qty -= trade_qty;
    passive.parent_level->total_qty -= trade_qty;

    emit_trade(event_timestamp, aggressor, passive, trade_price, trade_qty);
    emit_passive_fill_state(event_timestamp, passive, trade_qty, trade_price);

    if (passive.qty == 0) {
      return retire_order(passive, false);
    }
    if (passive.display_qty == 0) {
      replenish_iceberg(passive, event_timestamp);
    }
    return false;
  }

  [[nodiscard]] static bool is_marketable(const Order &aggressor,
                                          core::Price resting_price) noexcept {
    if (aggressor.type == core::OrderType::MARKET) {
      return true;
    }

    if (aggressor.side == core::Side::BUY) {
      return aggressor.price >= resting_price;
    }
    return aggressor.price <= resting_price;
  }

  void trigger_stop_orders(core::Timestamp event_timestamp) {
    bool triggered_any = false;

    do {
      triggered_any = false;

      while (!stop_buys_.empty()) {
        auto stop_it = stop_buys_.begin();
        if (stop_it->first > last_trade_price_) {
          break;
        }

        Order *order = stop_it->second;
        stop_buys_.erase(stop_it);
        --parked_stop_count_;
        activate_stop_order(*order, event_timestamp);
        triggered_any = true;
      }

      while (!stop_sells_.empty()) {
        auto stop_it = stop_sells_.begin();
        if (stop_it->first < last_trade_price_) {
          break;
        }

        Order *order = stop_it->second;
        stop_sells_.erase(stop_it);
        --parked_stop_count_;
        activate_stop_order(*order, event_timestamp);
        triggered_any = true;
      }
    } while (triggered_any);
  }

  void activate_stop_order(Order &order, core::Timestamp event_timestamp) {
    if (order.type == core::OrderType::STOP) {
      order.type = core::OrderType::MARKET;
      order.price = 0;
    } else {
      order.type = core::OrderType::LIMIT;
    }

    execute_inbound_order(order, event_timestamp, false);
  }

  // Walks the opposite side from its best level. Emptied levels are erased
  // from the map, so the best level is always begin(). Returns true if
  // self-trade prevention canceled the aggressor.
  template <typename LevelMap>
  bool match_against_levels(Order &aggressor, LevelMap &levels,
                            core::Side resting_side,
                            core::Timestamp event_timestamp,
                            core::Price &last_fill_price) {
    bool aggressor_canceled = false;
    while (!aggressor_canceled && aggressor.qty > 0 && !levels.empty()) {
      Level *level = levels.begin()->second;
      if (!is_marketable(aggressor, level->price)) {
        break;
      }
      aggressor_canceled = match_fifo_at_level(aggressor, *level,
                                               event_timestamp,
                                               last_fill_price);
    }

    refresh_best_level(resting_side);
    return aggressor_canceled;
  }

  bool match(Order &aggressor, core::Timestamp event_timestamp,
             core::Price &last_fill_price) {
    const Level *best_opposite = opposite_best_level(aggressor.side);
    if (best_opposite == nullptr ||
        !is_marketable(aggressor, best_opposite->price)) {
      return false;
    }

    if (aggressor.side == core::Side::BUY) {
      return match_against_levels(aggressor, asks_, core::Side::SELL,
                                  event_timestamp, last_fill_price);
    }
    return match_against_levels(aggressor, bids_, core::Side::BUY,
                                event_timestamp, last_fill_price);
  }

  // Runs a new or replacement order to completion, then activates any stop
  // orders its trades triggered. Triggering only after the order has rested
  // or been canceled means activated stops see the book as it stands.
  void process_inbound_order(Order &order, core::Timestamp event_timestamp) {
    const core::MatchId first_match_id = next_match_id_;
    execute_inbound_order(order, event_timestamp, true);
    if (next_match_id_ != first_match_id) {
      trigger_stop_orders(event_timestamp);
    }
  }

  [[nodiscard]] bool stop_already_triggered(core::Side side,
                                            core::Price trigger_price) const
      noexcept {
    if (last_trade_price_ == 0) {
      return false;
    }
    return side == core::Side::BUY ? trigger_price <= last_trade_price_
                                   : trigger_price >= last_trade_price_;
  }

  void execute_inbound_order(Order &order, core::Timestamp event_timestamp,
                             bool emit_accept_event) {
    if (emit_accept_event) {
      emit_accepted(event_timestamp, order);
    }

    if (order.is_stop_order()) {
      if (stop_already_triggered(order.side, order.trigger_price)) {
        activate_stop_order(order, event_timestamp);
        return;
      }
      park_stop_order(order);
      return;
    }

    const core::Quantity original_qty = order.qty;
    core::Price last_fill_price = 0;
    const bool canceled_by_stp = match(order, event_timestamp, last_fill_price);

    const core::Quantity total_filled = original_qty - order.qty;
    if (total_filled > 0) {
      emit_aggressor_fill_state(event_timestamp, order, total_filled,
                                last_fill_price);
    }

    if (order.qty == 0) {
      retire_order(order);
      return;
    }

    if (!canceled_by_stp && allows_resting(order.type)) {
      prepare_visible_slice(order);
      if (!insert_order(&order)) [[unlikely]] {
        emit_canceled(event_timestamp, order.id, order.qty,
                      ReasonCode::LEVEL_CAPACITY_EXHAUSTED);
        retire_order(order);
        return;
      }
      emit_rested(event_timestamp, order);
      return;
    }

    emit_canceled(event_timestamp, order.id, order.qty,
                  canceled_by_stp ? ReasonCode::SELF_TRADE_PREVENTED
                                  : ReasonCode::NONE);
    retire_order(order);
  }

  void prepare_visible_slice(Order &order) noexcept {
    if (order.type != core::OrderType::ICEBERG) {
      order.display_qty = order.qty;
      order.hidden_qty = 0;
      return;
    }

    order.display_qty = std::min(order.qty, order.peak_qty);
    order.hidden_qty = order.qty - order.display_qty;
  }

  void erase_stop_reference(const Order &order) {
    const auto erase_from = [&](auto &stops) {
      const auto [first, last] = stops.equal_range(order.trigger_price);
      for (auto it = first; it != last; ++it) {
        if (it->second == &order) {
          stops.erase(it);
          --parked_stop_count_;
          return;
        }
      }
    };
    if (order.side == core::Side::BUY) {
      erase_from(stop_buys_);
    } else {
      erase_from(stop_sells_);
    }
  }

  void park_stop_order(Order &order) {
    if (order.side == core::Side::BUY) {
      stop_buys_.emplace(order.trigger_price, &order);
    } else {
      stop_sells_.emplace(order.trigger_price, &order);
    }
    ++parked_stop_count_;
  }

  void reduce_order_quantity(Order &order, core::Quantity new_qty,
                             core::Timestamp event_timestamp) {
    const core::Quantity reduction = order.qty - new_qty;
    order.qty = new_qty;

    if (order.type == core::OrderType::ICEBERG) {
      core::Quantity remaining_reduction = reduction;
      const core::Quantity hidden_reduction =
          std::min(remaining_reduction, order.hidden_qty);
      order.hidden_qty -= hidden_reduction;
      remaining_reduction -= hidden_reduction;

      if (remaining_reduction > 0) {
        order.display_qty -= remaining_reduction;
        if (order.parent_level != nullptr) {
          order.parent_level->total_qty -= remaining_reduction;
        }
      }
    } else {
      order.display_qty = new_qty;
      if (order.parent_level != nullptr) {
        order.parent_level->total_qty -= reduction;
      }
    }

    if (order.parent_level != nullptr) {
      emit_reduced(event_timestamp, order.id, order.display_qty);
    }
  }

  void push_event(const Event &event) {
    if (overflow_ != nullptr && events_.size() == EventCapacity) {
      (*overflow_)(events_);
      events_.clear();
    }
    events_.push_back(event);
  }

  void emit_reduced(core::Timestamp timestamp, core::OrderId order_id,
                    core::Quantity new_qty) {
    push_event(Event::make(OrderReduced{
        .timestamp = timestamp,
        .order_id = order_id,
        .new_qty = new_qty,
    }));
  }

  void emit_rested(core::Timestamp timestamp, const Order &order) {
    push_event(Event::make(OrderRested{
        .timestamp = timestamp,
        .order_id = order.id,
        .symbol = symbol_,
        .side = order.side,
        .price = order.price,
        .qty = order.display_qty,
    }));
  }

  void emit_trade(core::Timestamp timestamp, const Order &aggressor,
                  const Order &passive, core::Price price, core::Quantity qty) {
    const core::OrderId buy_order_id =
        aggressor.side == core::Side::BUY ? aggressor.id : passive.id;
    const core::OrderId sell_order_id =
        aggressor.side == core::Side::SELL ? aggressor.id : passive.id;

    push_event(Event::make(Trade{
        .timestamp = timestamp,
        .match_id = next_match_id_++,
        .symbol = symbol_,
        .buy_order_id = buy_order_id,
        .sell_order_id = sell_order_id,
        .price = price,
        .qty = qty,
    }));
  }

  void emit_passive_fill_state(core::Timestamp timestamp, const Order &passive,
                               core::Quantity filled_qty, core::Price price) {
    if (passive.qty == 0) {
      push_event(Event::make(OrderFilled{
          .timestamp = timestamp,
          .order_id = passive.id,
          .filled_qty = filled_qty,
          .price = price,
      }));
      return;
    }

    push_event(Event::make(OrderPartiallyFilled{
        .timestamp = timestamp,
        .order_id = passive.id,
        .filled_qty = filled_qty,
        .remaining_qty = passive.qty,
        .price = price,
    }));
  }

  void emit_canceled(core::Timestamp timestamp, core::OrderId order_id,
                     core::Quantity canceled_qty,
                     ReasonCode reason = ReasonCode::NONE) {
    push_event(Event::make(OrderCanceled{
        .timestamp = timestamp,
        .order_id = order_id,
        .canceled_qty = canceled_qty,
        .reason = reason,
    }));
  }

  void emit_accepted(core::Timestamp timestamp, const Order &order) {
    push_event(Event::make(OrderAccepted{
        .timestamp = timestamp,
        .order_id = order.id,
        .symbol = symbol_,
        .side = order.side,
        .price = order.type == core::OrderType::STOP ? order.trigger_price
                                                     : order.price,
        .qty = order.qty,
        .order_type = order.type,
    }));
  }

  void emit_aggressor_fill_state(core::Timestamp timestamp,
                                 const Order &aggressor,
                                 core::Quantity total_filled,
                                 core::Price last_fill_price) {
    if (aggressor.qty == 0) {
      push_event(Event::make(OrderFilled{
          .timestamp = timestamp,
          .order_id = aggressor.id,
          .filled_qty = total_filled,
          .price = last_fill_price,
      }));
      return;
    }

    push_event(Event::make(OrderPartiallyFilled{
        .timestamp = timestamp,
        .order_id = aggressor.id,
        .filled_qty = total_filled,
        .remaining_qty = aggressor.qty,
        .price = last_fill_price,
    }));
  }

  void emit_rejected(core::Timestamp timestamp, core::OrderId order_id,
                     ReasonCode reason) {
    push_event(Event::make(OrderRejected{
        .timestamp = timestamp,
        .order_id = order_id,
        .reason_code = reason,
    }));
  }

  core::Symbol symbol_{};

  core::FixedBlockResource order_index_buffer_;
  core::FixedBlockResource bid_index_buffer_;
  core::FixedBlockResource ask_index_buffer_;
  core::FixedBlockResource stop_buy_index_buffer_;
  core::FixedBlockResource stop_sell_index_buffer_;

  BidMap bids_;
  AskMap asks_;
  BuyStopMap stop_buys_;
  SellStopMap stop_sells_;
  OrderMap orders_;

  // FIFO of retired order IDs still kept in orders_; the oldest is evicted
  // once OrderCapacity are remembered.
  std::unique_ptr<core::OrderId[]> retired_ids_;
  std::size_t retired_head_{0};
  std::size_t retired_count_{0};

  std::size_t parked_stop_count_{0};

  Level *best_bid_{nullptr};
  Level *best_ask_{nullptr};

  core::MemoryPool<Order, OrderCapacity> order_pool_{};
  core::MemoryPool<Level, LevelCapacity> level_pool_{};

  std::vector<Event> events_; // reserved for EventCapacity
  const EventOverflow *overflow_{nullptr};

  core::MatchId next_match_id_{1};
  core::Price last_trade_price_{0};
};

} // namespace exchange::matching
