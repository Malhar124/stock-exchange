#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "matching/order_book.h"
#include "test_support.h"
#include "text/order_text.h"

namespace {

using namespace exchange;
using core::OrderType;
using core::Side;
using matching::EventType;

// Tiny capacities so lifetime limits would show up within a few thousand
// operations.
using SmallBook = matching::OrderBook<1'000, 100, 4'096, 64>;

std::vector<std::string> lines(std::span<const matching::Event> events) {
  std::vector<std::string> out;
  for (const auto &event : events) {
    out.push_back(text::format_event(event));
  }
  return out;
}

std::unique_ptr<SmallBook> make_book() {
  return std::make_unique<SmallBook>(core::make_symbol("TEST"));
}

} // namespace

TEST_CASE(order_index_survives_many_lifetime_orders) {
  auto book = make_book();
  for (core::OrderId id = 1; id <= 50'000; ++id) {
    const auto added = book->add_order(id, Side::BUY, 100, 1, OrderType::LIMIT,
                                       id, 1);
    REQUIRE(added.size() == 2U);
    const auto canceled = book->cancel_order(id, id);
    REQUIRE(canceled.size() == 1U);
    REQUIRE(canceled[0].type == EventType::ORDER_CANCELED);
  }
}

TEST_CASE(level_maps_survive_many_lifetime_prices) {
  auto book = make_book();
  for (core::OrderId id = 1; id <= 50'000; ++id) {
    const auto price = static_cast<core::Price>(id);
    REQUIRE(book->add_order(id, Side::SELL, price, 1, OrderType::LIMIT, id, 1)
                .size() == 2U);
    REQUIRE(book->cancel_order(id, id).size() == 1U);
  }
}

TEST_CASE(stop_maps_survive_many_lifetime_stops) {
  auto book = make_book();
  for (core::OrderId id = 1; id <= 50'000; ++id) {
    const auto trigger = static_cast<core::Price>(1'000 + (id % 500));
    REQUIRE(book->add_order(id, Side::BUY, 0, 1, OrderType::STOP, id, 1,
                            trigger)
                .size() == 1U);
    REQUIRE(book->cancel_order(id, id).size() == 1U);
  }
}

TEST_CASE(matching_survives_many_lifetime_fills) {
  auto book = make_book();
  core::OrderId id = 1;
  for (int round = 0; round < 20'000; ++round) {
    const auto price = static_cast<core::Price>(100 + (round % 300));
    const core::OrderId seller = id++;
    const core::OrderId buyer = id++;
    REQUIRE(book->add_order(seller, Side::SELL, price, 5, OrderType::LIMIT,
                            seller, 1)
                .size() == 2U);
    const auto fills =
        book->add_order(buyer, Side::BUY, price, 5, OrderType::LIMIT, buyer, 2);
    REQUIRE(fills.size() == 4U);
    REQUIRE(fills[1].type == EventType::TRADE);
  }
}

TEST_CASE(recently_retired_ids_still_report_not_active_and_duplicate) {
  auto book = make_book();
  REQUIRE(book->add_order(7, Side::BUY, 100, 1, OrderType::LIMIT, 1, 1)
              .size() == 2U);
  REQUIRE(book->cancel_order(7, 2).size() == 1U);

  CHECK_EQ(lines(book->cancel_order(7, 3)),
           std::vector<std::string>{"REJECTED order=7 reason=ORDER_NOT_ACTIVE"});
  CHECK_EQ(lines(book->add_order(7, Side::BUY, 100, 1, OrderType::LIMIT, 4, 1)),
           std::vector<std::string>{
               "REJECTED order=7 reason=DUPLICATE_ORDER_ID"});
  CHECK_EQ(lines(book->cancel_order(8, 5)),
           std::vector<std::string>{"REJECTED order=8 reason=ORDER_NOT_FOUND"});
}

TEST_CASE(price_text_round_trips) {
  CHECK_EQ(text::parse_price("101"), std::optional<core::Price>{1'010'000});
  CHECK_EQ(text::parse_price("101.5"), std::optional<core::Price>{1'015'000});
  CHECK_EQ(text::parse_price("-0.0001"), std::optional<core::Price>{-1});
  CHECK(!text::parse_price("1.23456").has_value());
  CHECK(!text::parse_price("abc").has_value());
  CHECK(!text::parse_price("1.").has_value());
  CHECK(!text::parse_price("--5").has_value());
  CHECK(!text::parse_price("-+5").has_value());
  CHECK_EQ(text::parse_price("922337203685477.5807"),
           std::optional<core::Price>{std::numeric_limits<core::Price>::max()});
  CHECK(!text::parse_price("922337203685477.5808").has_value());
  CHECK_EQ(text::format_price(1'010'000), std::string("101.0000"));
  CHECK_EQ(text::format_price(-1), std::string("-0.0001"));
}

TEST_CASE(command_text_round_trips) {
  const std::vector<std::string> commands = {
      "ADD AAPL 1 BUY LIMIT 100 101.5000 participant=7",
      "ADD AAPL 2 SELL MARKET 10",
      "ADD AAPL 3 BUY STOP 10 trigger=99.0000",
      "ADD AAPL 4 SELL ICEBERG 100 101.0000 display=10",
      "CANCEL AAPL 1",
      "MODIFY AAPL 4 50 100.2500",
  };
  for (const std::string &command : commands) {
    text::ParsedCommand parsed;
    std::string error;
    REQUIRE(text::parse_command(command, parsed, error));
    CHECK_EQ(text::format_command(parsed.order, false), command);
  }

  text::ParsedCommand parsed;
  std::string error;
  CHECK(!text::parse_command("ADD AAPL 1 BUY LIMIT", parsed, error));
  CHECK(!text::parse_command("ADD AAPL 1 UP LIMIT 5 1", parsed, error));
  CHECK(!text::parse_command("FLY AAPL 1", parsed, error));
  CHECK(!text::parse_command("ADD TOOLONGSYMBOL 1 BUY LIMIT 5 1", parsed,
                             error));
}

TEST_CASE(too_many_live_price_levels_cancels_instead_of_aborting) {
  auto book = make_book();
  for (core::OrderId id = 1; id <= 100; ++id) {
    REQUIRE(book->add_order(id, Side::BUY, static_cast<core::Price>(id), 1,
                            OrderType::LIMIT, id, 1)
                .size() == 2U);
  }
  CHECK_EQ(lines(book->add_order(101, Side::BUY, 101, 1, OrderType::LIMIT, 101,
                                 1)),
           (std::vector<std::string>{
               "ACCEPTED order=101 side=BUY price=0.0101 qty=1 type=LIMIT",
               "CANCELED order=101 qty=1 reason=LEVEL_CAPACITY_EXHAUSTED"}));

  // Joining an existing level needs no new level.
  CHECK_EQ(book->add_order(102, Side::BUY, 50, 1, OrderType::LIMIT, 102, 1)
               .size(),
           2U);
  // Freeing a level makes room again.
  REQUIRE(book->cancel_order(1, 103).size() == 1U);
  CHECK_EQ(book->add_order(103, Side::BUY, 101, 1, OrderType::LIMIT, 104, 1)
               .size(),
           2U);
}

TEST_CASE(too_many_parked_stops_is_rejected) {
  auto book = make_book();
  for (core::OrderId id = 1; id <= 64; ++id) {
    REQUIRE(book->add_order(id, Side::BUY, 0, 1, OrderType::STOP, id, 1, 500)
                .size() == 1U);
  }
  CHECK_EQ(lines(book->add_order(65, Side::BUY, 0, 1, OrderType::STOP, 65, 1,
                                 500)),
           std::vector<std::string>{
               "REJECTED order=65 reason=STOP_CAPACITY_EXHAUSTED"});
  REQUIRE(book->cancel_order(1, 66).size() == 1U);
  CHECK_EQ(book->add_order(66, Side::BUY, 0, 1, OrderType::STOP, 67, 1, 500)
               .size(),
           1U);
}

TEST_CASE(too_many_live_orders_is_rejected) {
  auto book = make_book();
  for (core::OrderId id = 1; id <= 1'000; ++id) {
    REQUIRE(book->add_order(id, Side::BUY, 100, 1, OrderType::LIMIT, id, 1)
                .size() == 2U);
  }
  CHECK_EQ(lines(book->add_order(1'001, Side::BUY, 100, 1, OrderType::LIMIT,
                                 1'001, 1)),
           std::vector<std::string>{
               "REJECTED order=1001 reason=ORDER_POOL_EXHAUSTED"});
}

// A journaled request that aborted the engine would abort recovery too, so a
// request with more events than the reserved buffer holds must still finish.
TEST_CASE(a_request_with_more_events_than_reserved_does_not_abort) {
  auto book = make_book();
  REQUIRE(book->add_order(1, Side::SELL, 100, 3'000, OrderType::ICEBERG, 1, 1,
                          0, 1)
              .size() == 2U);
  const auto sweep =
      book->add_order(2, Side::BUY, 100, 2'000, OrderType::LIMIT, 2, 2);
  // ACCEPTED, 2000 x (TRADE + PARTIALLY_FILLED + RESTED), FILLED.
  CHECK_EQ(sweep.size(), 6'002U);
  CHECK(sweep.back().type == EventType::ORDER_FILLED);
}

// With an overflow sink, the book never holds more than the reserved events:
// full chunks go to the sink while the request runs, the call returns the rest.
TEST_CASE(a_request_streams_events_beyond_the_reserve_to_the_overflow) {
  auto book = make_book();
  REQUIRE(book->add_order(1, Side::SELL, 100, 3'000, OrderType::ICEBERG, 1, 1,
                          0, 1)
              .size() == 2U);
  std::vector<std::size_t> chunks;
  const SmallBook::EventOverflow overflow =
      [&](std::span<const matching::Event> events) {
        chunks.push_back(events.size());
      };
  book->set_event_overflow(&overflow);
  const auto rest =
      book->add_order(2, Side::BUY, 100, 2'000, OrderType::LIMIT, 2, 2);
  CHECK((chunks == std::vector<std::size_t>{4'096U}));
  CHECK_EQ(rest.size(), 6'002U - 4'096U);
  CHECK(rest.back().type == EventType::ORDER_FILLED);
}

// Built with EXCHANGE_POISON_FREED: reading a freed price level crashes.
TEST_CASE(sweeping_several_levels_does_not_read_freed_levels) {
  auto book = make_book();
  for (core::OrderId id = 1; id <= 20; ++id) {
    REQUIRE(book->add_order(id, Side::SELL, static_cast<core::Price>(100 + id),
                            1, OrderType::LIMIT, id, 1)
                .size() == 2U);
  }
  const auto sweep =
      book->add_order(100, Side::BUY, 200, 25, OrderType::LIMIT, 100, 2);
  // ACCEPTED, 20 x (TRADE + FILLED), PARTIALLY_FILLED, RESTED.
  CHECK_EQ(sweep.size(), 43U);
  CHECK_EQ(lines(book->add_order(101, Side::SELL, 200, 5, OrderType::LIMIT,
                                 101, 3))
               .size(),
           4U);
}
