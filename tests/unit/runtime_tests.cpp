#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <span>
#include <string>
#include <system_error>
#include <vector>

#include "gateway/gateway_core.h"
#include "protocol/messages.h"
#include "runtime/capacity_profiles.h"
#include "runtime/engine_stage.h"
#include "runtime/journal.h"
#include "runtime/market_data.h"
#include "server/config.h"
#include "test_support.h"

namespace {

using namespace exchange;
using protocol::ExecKind;
using protocol::GatewayReason;

constexpr core::Price PX = core::PRICE_SCALE; // one currency unit

std::string temp_path(const std::string &name) {
  std::error_code ignored;
  return (std::filesystem::temp_directory_path(ignored) /
          ("exchange_test_" + std::to_string(::getpid()) + '_' + name))
      .string();
}

matching::InboundOrder limit(core::OrderId id, core::Side side,
                             core::Price price, core::Quantity qty) {
  return matching::InboundOrder{.action = matching::RequestAction::ADD,
                                .symbol = core::make_symbol("AAPL"),
                                .order_id = id,
                                .side = side,
                                .price = price,
                                .qty = qty,
                                .participant_id = 1};
}

std::vector<protocol::Frame> frames_of(const std::vector<std::byte> &bytes) {
  std::vector<protocol::Frame> frames;
  std::size_t offset = 0;
  protocol::Frame frame;
  std::size_t consumed = 0;
  while (protocol::next_frame(std::span(bytes).subspan(offset), frame,
                              consumed) == protocol::FrameStatus::COMPLETE) {
    frames.push_back(frame);
    offset += consumed;
  }
  return frames;
}

// A gateway wired synchronously to a real engine stage.
class Harness {
public:
  explicit Harness(gateway::RiskLimits limits = {},
                   runtime::JournalWriter *journal = nullptr)
      : core_(symbols_, limits,
              [this](const runtime::GatewayRequest &request) {
                if (busy) {
                  return false;
                }
                pending_.push_back(request);
                return true;
              }),
        stage_(symbols_, journal, nullptr) {}

  gateway::SessionId login(core::ParticipantId participant) {
    const gateway::SessionId session = core_.open_session();
    send(session, protocol::Login{.participant_id = participant});
    take(session);
    return session;
  }

  template <typename Message>
  void send(gateway::SessionId session, const Message &message) {
    std::vector<std::byte> bytes;
    protocol::append_message(bytes, message);
    for (const protocol::Frame &frame : frames_of(bytes)) {
      REQUIRE(core_.on_message(session, frame));
    }
    run_engine();
  }

  void new_order(gateway::SessionId session, std::uint64_t id,
                 core::Side side, core::Price price, core::Quantity qty,
                 core::OrderType type = core::OrderType::LIMIT) {
    send(session, protocol::NewOrder{.client_order_id = id,
                                     .symbol = core::make_symbol("AAPL"),
                                     .side = side,
                                     .type = type,
                                     .qty = qty,
                                     .price = price});
  }

  // Returns and clears the execution reports sent to a session.
  std::vector<protocol::ExecutionReport> reports(gateway::SessionId session) {
    std::vector<protocol::ExecutionReport> out;
    for (const protocol::Frame &frame : frames_of(*core_.outbound(session))) {
      protocol::ExecutionReport report;
      if (protocol::decode(frame, report)) {
        out.push_back(report);
      }
    }
    take(session);
    return out;
  }

  std::vector<std::byte> take(gateway::SessionId session) {
    std::vector<std::byte> bytes = std::move(*core_.outbound(session));
    core_.outbound(session)->clear();
    return bytes;
  }

  gateway::GatewayCore &core() { return core_; }

  void retry_pending_cancels() {
    core_.retry_pending_cancels();
    run_engine();
  }

  bool busy{false};

private:
  void run_engine() {
    const std::vector<runtime::GatewayRequest> batch = std::move(pending_);
    pending_.clear();
    REQUIRE(stage_.process_batch(batch, [&](const runtime::EngineOutput &out) {
      core_.on_engine_output(out);
    }));
  }

  std::vector<core::Symbol> symbols_{core::make_symbol("AAPL")};
  std::vector<runtime::GatewayRequest> pending_;
  gateway::GatewayCore core_;
  runtime::EngineStage<runtime::SmallEngine> stage_;
};

std::vector<ExecKind> kinds(const std::vector<protocol::ExecutionReport> &all) {
  std::vector<ExecKind> out;
  for (const auto &report : all) {
    out.push_back(report.kind);
  }
  return out;
}

} // namespace

TEST_CASE(journal_round_trips_records) {
  const std::string path = temp_path("roundtrip.journal");
  {
    runtime::JournalWriter writer;
    std::string error;
    REQUIRE(writer.open(path, runtime::CapacityProfile::SMALL,
                        runtime::JournalSync::NONE, nullptr, error));
    for (core::SequenceNumber seq = 1; seq <= 100; ++seq) {
      writer.append(runtime::make_record(
          seq, seq * 10U, limit(seq, core::Side::BUY, 100 * PX, 1)));
    }
    REQUIRE(writer.commit());
  }
  runtime::JournalReadResult result;
  std::string error;
  std::vector<runtime::JournalRecord> records;
  REQUIRE(runtime::read_journal(path, result, error,
                                [&](const runtime::JournalRecord &record) {
                                  records.push_back(record);
                                }));
  CHECK_EQ(records.size(), 100U);
  CHECK_EQ(result.last_sequence, 100U);
  CHECK(!result.damaged_tail);
  CHECK_EQ(records[41].client_order_id, 420U);
  CHECK_EQ(records[41].order.order_id, 42U);
  std::filesystem::remove(path);
}

// Two servers on one journal would overwrite each other's records.
TEST_CASE(journal_refuses_a_second_writer) {
  const std::string path = temp_path("locked.journal");
  runtime::JournalWriter first;
  std::string error;
  REQUIRE(first.open(path, runtime::CapacityProfile::SMALL,
                     runtime::JournalSync::NONE, nullptr, error));
  first.append(
      runtime::make_record(1, 1, limit(1, core::Side::BUY, 100 * PX, 1)));
  REQUIRE(first.commit());
  const auto size = std::filesystem::file_size(path);

  runtime::JournalWriter second;
  CHECK(!second.open(path, runtime::CapacityProfile::SMALL,
                     runtime::JournalSync::NONE, nullptr, error));
  CHECK(error.find("in use") != std::string::npos);
  CHECK_EQ(std::filesystem::file_size(path), size);

  // Once the first writer closes, the journal can be opened again.
  first.close();
  runtime::JournalReadResult result;
  REQUIRE(runtime::read_journal(path, result, error, [](const auto &) {}));
  CHECK(second.open(path, runtime::CapacityProfile::SMALL,
                    runtime::JournalSync::NONE, &result, error));
  std::filesystem::remove(path);
}

TEST_CASE(journal_drops_torn_tail_but_rejects_damage_elsewhere) {
  const std::string path = temp_path("damage.journal");
  {
    runtime::JournalWriter writer;
    std::string error;
    REQUIRE(writer.open(path, runtime::CapacityProfile::SMALL,
                        runtime::JournalSync::NONE, nullptr, error));
    for (core::SequenceNumber seq = 1; seq <= 10; ++seq) {
      writer.append(runtime::make_record(
          seq, seq, limit(seq, core::Side::BUY, 100 * PX, 1)));
    }
    REQUIRE(writer.commit());
  }
  const auto full_size = std::filesystem::file_size(path);

  std::filesystem::resize_file(path, full_size - 7U); // torn final write
  runtime::JournalReadResult result;
  std::string error;
  std::uint64_t seen = 0;
  REQUIRE(runtime::read_journal(path, result, error,
                                [&](const auto &) { ++seen; }));
  CHECK_EQ(seen, 9U);
  CHECK(result.damaged_tail);
  CHECK_EQ(result.valid_bytes, sizeof(runtime::JournalHeader) +
                                   (9U * sizeof(runtime::JournalRecord)));

  // Reopening for append truncates the torn bytes.
  {
    runtime::JournalWriter writer;
    REQUIRE(writer.open(path, runtime::CapacityProfile::SMALL,
                        runtime::JournalSync::NONE, &result, error));
  }
  CHECK_EQ(std::filesystem::file_size(path), result.valid_bytes);

  // A corrupt record that is not the last one is an error.
  {
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    file.seekp(static_cast<std::streamoff>(sizeof(runtime::JournalHeader) +
                                           sizeof(runtime::JournalRecord) +
                                           20U));
    file.put('\x7f');
  }
  CHECK(!runtime::read_journal(path, result, error, [](const auto &) {}));
  std::filesystem::remove(path);
}

TEST_CASE(market_data_tracks_visible_quantity) {
  runtime::SmallEngine engine;
  runtime::MarketDataBook book;
  const auto apply = [&](const matching::InboundOrder &order) {
    for (const auto &event : engine.process_order(order)) {
      book.apply(event);
    }
  };
  const auto symbol = core::make_symbol("AAPL");
  auto iceberg = limit(1, core::Side::SELL, 101 * PX, 30);
  iceberg.order_type = core::OrderType::ICEBERG;
  iceberg.display_qty = 10;
  apply(iceberg);
  apply(limit(2, core::Side::SELL, 101 * PX, 5));
  apply(limit(3, core::Side::BUY, 99 * PX, 7));
  CHECK_EQ(book.book(symbol)->asks.at(101 * PX), 15U); // hidden 20 not shown
  CHECK_EQ(book.book(symbol)->bids.at(99 * PX), 7U);

  apply(limit(4, core::Side::BUY, 101 * PX, 12)); // 10 from 1 (refilled), 2
  CHECK_EQ(book.book(symbol)->asks.at(101 * PX), 13U);
  CHECK_EQ(book.last_trade_price(symbol), 101 * PX);

  apply(matching::InboundOrder{.action = matching::RequestAction::MODIFY,
                               .symbol = symbol,
                               .order_id = 3,
                               .price = 99 * PX,
                               .qty = 2});
  CHECK_EQ(book.book(symbol)->bids.at(99 * PX), 2U);
  apply(matching::InboundOrder{.action = matching::RequestAction::CANCEL,
                               .symbol = symbol,
                               .order_id = 3});
  CHECK(book.book(symbol)->bids.empty());

  std::vector<runtime::LevelChange> changes;
  book.drain([&](const auto &change) { changes.push_back(change); });
  CHECK(!changes.empty());
}

TEST_CASE(gateway_login_rules) {
  Harness harness;
  auto &core = harness.core();
  const auto first = core.open_session();
  harness.send(first, protocol::Login{.participant_id = 0});
  auto frames = harness.take(first);
  protocol::LoginResponse response;
  REQUIRE(protocol::decode(frames_of(frames).at(0), response));
  CHECK(response.result == protocol::LoginResult::INVALID_PARTICIPANT);

  harness.send(first, protocol::Login{.participant_id = 5});
  frames = harness.take(first);
  REQUIRE(protocol::decode(frames_of(frames).at(0), response));
  CHECK(response.result == protocol::LoginResult::ACCEPTED);

  const auto second = core.open_session();
  harness.send(second, protocol::Login{.participant_id = 5});
  frames = harness.take(second);
  REQUIRE(protocol::decode(frames_of(frames).at(0), response));
  CHECK(response.result == protocol::LoginResult::ALREADY_LOGGED_IN);

  // Orders before login are refused.
  harness.new_order(second, 1, core::Side::BUY, 100 * PX, 1);
  const auto refused = harness.reports(second);
  REQUIRE(refused.size() == 1U);
  CHECK(refused[0].kind == ExecKind::REJECTED);
  CHECK_EQ(refused[0].reason,
           static_cast<std::uint16_t>(GatewayReason::NOT_LOGGED_IN));
}

TEST_CASE(gateway_checks_secrets_when_configured) {
  Harness harness;
  auto &core = harness.core();
  core.set_secrets({{4, protocol::make_secret("correct-horse-battery")}});
  // Returns the login result, and whether the gateway keeps the connection.
  const auto login = [&](core::ParticipantId participant,
                         std::string_view secret) {
    const auto session = core.open_session();
    std::vector<std::byte> bytes;
    protocol::append_message(
        bytes, protocol::Login{.participant_id = participant,
                               .secret = protocol::make_secret(secret)});
    const bool kept = core.on_message(session, frames_of(bytes).at(0));
    const auto out = harness.take(session);
    protocol::LoginResponse response;
    CHECK(protocol::decode(frames_of(out).at(0), response));
    return std::pair{response.result, kept};
  };

  CHECK((login(4, "wrong-horse-battery") ==
         std::pair{protocol::LoginResult::AUTHENTICATION_FAILED, false}));
  CHECK((login(5, "correct-horse-battery") ==
         std::pair{protocol::LoginResult::AUTHENTICATION_FAILED, false}));
  CHECK((login(4, "correct-horse-battery") ==
         std::pair{protocol::LoginResult::ACCEPTED, true}));
}

TEST_CASE(secrets_file_must_be_private_and_well_formed) {
  const std::string path = temp_path("participants.secrets");
  const auto write = [&](const std::string &content) {
    std::ofstream(path, std::ios::trunc) << content;
    std::filesystem::permissions(path, std::filesystem::perms::owner_read |
                                           std::filesystem::perms::owner_write);
  };
  gateway::Secrets secrets;
  std::string error;

  write("# participant secret\n4 correct-horse-battery\n\n7 zebra-staple-orbit-9\n");
  REQUIRE(server::load_secrets(path, secrets, error));
  CHECK_EQ(secrets.size(), 2U);
  CHECK(secrets.at(7) == protocol::make_secret("zebra-staple-orbit-9"));

  write("4 too-short\n");
  CHECK(!server::load_secrets(path, secrets, error));
  write("4 correct-horse-battery\n4 correct-horse-battery\n");
  CHECK(!server::load_secrets(path, secrets, error));
  write("0 correct-horse-battery\n");
  CHECK(!server::load_secrets(path, secrets, error));

  write("4 correct-horse-battery\n");
  std::filesystem::permissions(path, std::filesystem::perms::group_read,
                               std::filesystem::perm_options::add);
  CHECK(!server::load_secrets(path, secrets, error));
  CHECK(error.find("chmod 600") != std::string::npos);
  std::filesystem::remove(path);
}

TEST_CASE(gateway_routes_reports_and_enforces_ownership) {
  Harness harness;
  const auto seller = harness.login(1);
  const auto buyer = harness.login(2);

  harness.new_order(seller, 77, core::Side::SELL, 101 * PX, 10);
  CHECK((kinds(harness.reports(seller)) ==
         std::vector<ExecKind>{ExecKind::ACCEPTED, ExecKind::RESTED}));

  // Both participants may use the same client order ID.
  harness.new_order(buyer, 77, core::Side::BUY, 101 * PX, 4);
  const auto buyer_reports = harness.reports(buyer);
  CHECK((kinds(buyer_reports) == std::vector<ExecKind>{ExecKind::ACCEPTED,
                                                       ExecKind::TRADE,
                                                       ExecKind::FILLED}));
  const auto seller_reports = harness.reports(seller);
  REQUIRE(seller_reports.size() == 2U);
  CHECK(seller_reports[0].kind == ExecKind::TRADE);
  CHECK(seller_reports[0].side == core::Side::SELL);
  CHECK_EQ(seller_reports[0].leaves_qty, 6U);
  CHECK(seller_reports[1].kind == ExecKind::PARTIALLY_FILLED);
  CHECK_EQ(seller_reports[1].client_order_id, 77U);

  // The buyer cannot cancel the seller's order: client IDs are per owner,
  // and the buyer's own order 77 is already filled.
  harness.send(buyer, protocol::CancelOrder{.client_order_id = 77});
  const auto denied = harness.reports(buyer);
  REQUIRE(denied.size() == 1U);
  CHECK(denied[0].kind == ExecKind::REQUEST_REJECTED);
  CHECK_EQ(denied[0].reason, static_cast<std::uint16_t>(
                                 GatewayReason::UNKNOWN_CLIENT_ORDER_ID));
  CHECK(harness.reports(seller).empty());

  // A live client order ID cannot be reused.
  harness.new_order(seller, 77, core::Side::SELL, 102 * PX, 1);
  const auto duplicate = harness.reports(seller);
  REQUIRE(duplicate.size() == 1U);
  CHECK_EQ(duplicate[0].reason, static_cast<std::uint16_t>(
                                    GatewayReason::DUPLICATE_CLIENT_ORDER_ID));

  harness.send(seller, protocol::CancelOrder{.client_order_id = 77});
  const auto canceled = harness.reports(seller);
  REQUIRE(canceled.size() == 1U);
  CHECK(canceled[0].kind == ExecKind::CANCELED);
  CHECK_EQ(canceled[0].qty, 6U);
  CHECK_EQ(harness.core().open_orders(), 0U);
}

TEST_CASE(gateway_modify_reports_replace_and_reduce) {
  Harness harness;
  const auto session = harness.login(3);
  harness.new_order(session, 1, core::Side::BUY, 100 * PX, 10);
  harness.reports(session);

  harness.send(session, protocol::ModifyOrder{
                            .client_order_id = 1, .price = 100 * PX, .qty = 4});
  auto reports = harness.reports(session);
  REQUIRE(reports.size() == 1U);
  CHECK(reports[0].kind == ExecKind::REDUCED);
  CHECK_EQ(reports[0].leaves_qty, 4U);

  harness.send(session, protocol::ModifyOrder{
                            .client_order_id = 1, .price = 99 * PX, .qty = 4});
  CHECK((kinds(harness.reports(session)) ==
         std::vector<ExecKind>{ExecKind::REPLACED, ExecKind::ACCEPTED,
                               ExecKind::RESTED}));
  CHECK_EQ(harness.core().open_orders(), 1U);
}

// Shrinking only the hidden part of an iceberg must still be acknowledged,
// with the new open quantity.
TEST_CASE(gateway_reports_iceberg_hidden_reduction) {
  Harness harness;
  const auto session = harness.login(3);
  harness.send(session, protocol::NewOrder{.client_order_id = 1,
                                           .symbol = core::make_symbol("AAPL"),
                                           .side = core::Side::SELL,
                                           .type = core::OrderType::ICEBERG,
                                           .qty = 100,
                                           .price = 100 * PX,
                                           .display_qty = 10});
  harness.reports(session);

  harness.send(session, protocol::ModifyOrder{
                            .client_order_id = 1, .price = 100 * PX, .qty = 50});
  const auto reports = harness.reports(session);
  REQUIRE(reports.size() == 1U);
  CHECK(reports[0].kind == ExecKind::REDUCED);
  CHECK_EQ(reports[0].qty, 10U);
  CHECK_EQ(reports[0].leaves_qty, 50U);
}

TEST_CASE(gateway_risk_checks) {
  Harness harness(gateway::RiskLimits{.max_order_qty = 100,
                                      .max_notional = 50'000,
                                      .price_band_percent = 10,
                                      .max_open_orders = 2});
  const auto session = harness.login(4);
  const auto reason_of = [&](std::uint64_t id, core::Price price,
                             core::Quantity qty) {
    harness.new_order(session, id, core::Side::BUY, price, qty);
    const auto reports = harness.reports(session);
    return reports.empty() ? std::uint16_t{999} : reports[0].reason;
  };

  CHECK_EQ(reason_of(1, 100 * PX, 101),
           static_cast<std::uint16_t>(GatewayReason::MAX_ORDER_QTY));
  CHECK_EQ(reason_of(2, 1'000 * PX, 51),
           static_cast<std::uint16_t>(GatewayReason::MAX_NOTIONAL));

  // Establish a last trade at 100 so the price band applies.
  const auto other = harness.login(5);
  harness.new_order(other, 1, core::Side::SELL, 100 * PX, 1);
  harness.new_order(session, 3, core::Side::BUY, 100 * PX, 1);
  harness.reports(session);
  CHECK_EQ(reason_of(4, 111 * PX, 1),
           static_cast<std::uint16_t>(GatewayReason::PRICE_BAND));
  CHECK_EQ(reason_of(5, 95 * PX, 1), 0U);
  CHECK_EQ(reason_of(6, 96 * PX, 1), 0U);
  CHECK_EQ(reason_of(7, 97 * PX, 1),
           static_cast<std::uint16_t>(GatewayReason::MAX_OPEN_ORDERS));

  harness.busy = true;
  harness.send(session, protocol::CancelOrder{.client_order_id = 5});
  const auto busy = harness.reports(session);
  REQUIRE(busy.size() == 1U);
  CHECK_EQ(busy[0].reason,
           static_cast<std::uint16_t>(GatewayReason::ENGINE_BUSY));
}

TEST_CASE(gateway_retries_cancel_on_disconnect_when_the_queue_is_full) {
  Harness harness(gateway::RiskLimits{.cancel_on_disconnect = true});
  const auto session = harness.login(4);
  harness.new_order(session, 1, core::Side::BUY, 99 * PX, 1);
  harness.new_order(session, 2, core::Side::BUY, 98 * PX, 1);
  REQUIRE(harness.core().open_orders() == 2U);

  harness.busy = true;
  harness.core().close_session(session);
  harness.retry_pending_cancels();
  CHECK_EQ(harness.core().open_orders(), 2U); // still queued for retry

  harness.busy = false;
  harness.retry_pending_cancels();
  CHECK_EQ(harness.core().open_orders(), 0U);
}

TEST_CASE(gateway_refuses_cancels_while_shutting_down) {
  Harness harness;
  const auto session = harness.login(4);
  harness.new_order(session, 1, core::Side::BUY, 99 * PX, 1);
  harness.reports(session);
  harness.core().set_accepting(false);
  harness.send(session, protocol::CancelOrder{.client_order_id = 1});
  const auto reports = harness.reports(session);
  REQUIRE(reports.size() == 1U);
  CHECK(reports[0].kind == ExecKind::REQUEST_REJECTED);
  CHECK_EQ(reports[0].reason,
           static_cast<std::uint16_t>(GatewayReason::SHUTTING_DOWN));
}

TEST_CASE(gateway_rejects_prices_off_the_tick) {
  Harness harness(gateway::RiskLimits{.tick_size = PX / 100});
  const auto session = harness.login(4);
  harness.new_order(session, 1, core::Side::BUY, (99 * PX) + 50, 1);
  auto reports = harness.reports(session);
  REQUIRE(reports.size() == 1U);
  CHECK_EQ(reports[0].reason,
           static_cast<std::uint16_t>(GatewayReason::PRICE_NOT_ON_TICK));

  harness.new_order(session, 2, core::Side::BUY, (99 * PX) + 100, 1);
  CHECK((kinds(harness.reports(session)) ==
         std::vector<ExecKind>{ExecKind::ACCEPTED, ExecKind::RESTED}));
  harness.send(session, protocol::ModifyOrder{
                            .client_order_id = 2, .price = 99 * PX + 1, .qty = 1});
  reports = harness.reports(session);
  REQUIRE(reports.size() == 1U);
  CHECK_EQ(reports[0].reason,
           static_cast<std::uint16_t>(GatewayReason::PRICE_NOT_ON_TICK));
}

TEST_CASE(gateway_limits_market_orders) {
  Harness harness(gateway::RiskLimits{.max_notional = 1'000'000,
                                      .price_band_percent = 10});
  const auto buyer = harness.login(4);
  const auto seller = harness.login(5);

  // No last trade yet: nothing to value a market order against.
  harness.new_order(seller, 1, core::Side::SELL, 105 * PX, 1);
  harness.new_order(buyer, 1, core::Side::BUY, 0, 1, core::OrderType::MARKET);
  auto reports = harness.reports(buyer);
  REQUIRE(reports.size() == 1U);
  CHECK_EQ(reports[0].reason,
           static_cast<std::uint16_t>(GatewayReason::MAX_NOTIONAL));

  // After a trade at 100, a market buy only takes asks up to 110.
  harness.new_order(seller, 2, core::Side::SELL, 100 * PX, 1);
  harness.new_order(buyer, 2, core::Side::BUY, 100 * PX, 1);
  harness.new_order(seller, 3, core::Side::SELL, 115 * PX, 1);
  harness.reports(buyer);
  harness.new_order(buyer, 3, core::Side::BUY, 0, 2, core::OrderType::MARKET);
  reports = harness.reports(buyer);
  CHECK((kinds(reports) ==
         std::vector<ExecKind>{ExecKind::ACCEPTED, ExecKind::TRADE,
                               ExecKind::PARTIALLY_FILLED,
                               ExecKind::CANCELED}));
  REQUIRE(reports.size() == 4U);
  CHECK_EQ(reports[1].price, 105 * PX);
  CHECK(reports[0].order_type == core::OrderType::MARKET);
}

TEST_CASE(gateway_limits_stop_orders) {
  Harness harness(gateway::RiskLimits{.price_band_percent = 10});
  const auto buyer = harness.login(4);
  const auto seller = harness.login(5);

  // A bid rests at 91 while trades move the last price up to 110.
  harness.new_order(buyer, 1, core::Side::BUY, 91 * PX, 1);
  harness.new_order(seller, 1, core::Side::SELL, 100 * PX, 1);
  harness.new_order(buyer, 2, core::Side::BUY, 100 * PX, 1);
  harness.new_order(seller, 2, core::Side::SELL, 110 * PX, 1);
  harness.new_order(buyer, 3, core::Side::BUY, 110 * PX, 1);
  harness.reports(seller);

  // A triggered sell stop only sells down to 99, its trigger less 10%, so
  // it rests there instead of hitting the bid at 91.
  harness.send(seller, protocol::NewOrder{.client_order_id = 3,
                                          .symbol = core::make_symbol("AAPL"),
                                          .side = core::Side::SELL,
                                          .type = core::OrderType::STOP,
                                          .qty = 1,
                                          .trigger_price = 110 * PX});
  const auto reports = harness.reports(seller);
  CHECK((kinds(reports) ==
         std::vector<ExecKind>{ExecKind::ACCEPTED, ExecKind::RESTED}));
  REQUIRE(reports.size() == 2U);
  CHECK_EQ(reports[1].price, 99 * PX);
  CHECK(reports[0].order_type == core::OrderType::STOP);
}

TEST_CASE(gateway_values_orders_by_trigger_price_only_for_stops) {
  Harness harness(gateway::RiskLimits{.max_notional = 1'000});
  const auto buyer = harness.login(4);
  const auto seller = harness.login(5);
  harness.new_order(seller, 1, core::Side::SELL, 100 * PX, 5);
  harness.new_order(buyer, 1, core::Side::BUY, 100 * PX, 1); // last trade 100
  REQUIRE(harness.core().market_data().last_trade_price(
              core::make_symbol("AAPL")) == 100 * PX);
  harness.reports(buyer);
  const auto reason_of = [&](std::uint64_t id, core::OrderType type) {
    harness.send(buyer, protocol::NewOrder{.client_order_id = id,
                                           .symbol = core::make_symbol("AAPL"),
                                           .side = core::Side::BUY,
                                           .type = type,
                                           .qty = 18,
                                           .trigger_price = PX / 100});
    const auto reports = harness.reports(buyer);
    return reports.empty() ? std::uint16_t{999} : reports[0].reason;
  };

  // 18 at 100 is over the limit; a trigger of 0.01 must not hide that.
  CHECK_EQ(reason_of(2, core::OrderType::MARKET),
           static_cast<std::uint16_t>(GatewayReason::INVALID_REQUEST));
  // A buy stop below the last trade triggers at once, so it is valued at
  // the last trade.
  CHECK_EQ(reason_of(3, core::OrderType::STOP),
           static_cast<std::uint16_t>(GatewayReason::MAX_NOTIONAL));
}

TEST_CASE(config_rejects_a_price_band_over_100_percent) {
  server::ServerConfig config;
  std::string error;
  CHECK(server::apply_setting(config, "price_band_percent", "100", error));
  CHECK(!server::apply_setting(config, "price_band_percent", "101", error));
}

TEST_CASE(engine_stage_delivers_every_event_of_a_huge_request) {
  runtime::EngineStage<runtime::SmallEngine> stage(
      std::vector<core::Symbol>{core::make_symbol("AAPL")}, nullptr, nullptr);
  auto iceberg = limit(1, core::Side::SELL, 100 * PX, 30'000);
  iceberg.order_type = core::OrderType::ICEBERG;
  iceberg.display_qty = 1;
  const std::vector<runtime::GatewayRequest> batch{
      {.order = iceberg}, {.order = limit(2, core::Side::BUY, 100 * PX, 22'000)}};
  std::uint64_t events = 0;
  REQUIRE(stage.process_batch(batch, [&](const runtime::EngineOutput &output) {
    events += output.kind == runtime::EngineOutput::Kind::EVENT ? 1U : 0U;
  }));
  // ACCEPTED + RESTED, then ACCEPTED, 22000 x 3, FILLED: past the 65536 reserve.
  CHECK_EQ(events, 2U + 66'002U);
}

TEST_CASE(gateway_market_data_snapshot_then_updates) {
  Harness harness;
  const auto trader = harness.login(6);
  harness.new_order(trader, 1, core::Side::SELL, 101 * PX, 5);
  harness.new_order(trader, 2, core::Side::BUY, 99 * PX, 3);

  const auto watcher = harness.login(7);
  harness.send(watcher, protocol::Subscribe{.symbol = core::make_symbol("AAPL")});
  // Frames point into `bytes`, so it must outlive them.
  auto bytes = harness.take(watcher);
  auto frames = frames_of(bytes);
  REQUIRE(frames.size() == 4U); // begin, bid, ask, end
  CHECK(frames[0].type == protocol::MessageType::BOOK_SNAPSHOT_BEGIN);
  protocol::BookUpdate level;
  REQUIRE(protocol::decode(frames[1], level));
  CHECK(level.side == core::Side::BUY);
  CHECK_EQ(level.qty, 3U);
  CHECK(frames[3].type == protocol::MessageType::BOOK_SNAPSHOT_END);

  harness.new_order(trader, 3, core::Side::BUY, 101 * PX, 2,
                    core::OrderType::IOC); // trades 2 against its own ask
  bytes = harness.take(watcher);
  frames = frames_of(bytes);
  REQUIRE(frames.size() == 2U);
  protocol::TradePrint trade;
  REQUIRE(protocol::decode(frames[0], trade));
  CHECK_EQ(trade.qty, 2U);
  REQUIRE(protocol::decode(frames[1], level));
  CHECK_EQ(level.price, 101 * PX);
  CHECK_EQ(level.qty, 3U);
}

TEST_CASE(recovery_rebuilds_the_same_gateway_state) {
  const std::string path = temp_path("recovery.journal");
  std::string error;
  std::vector<std::uint32_t> live_status;
  {
    runtime::JournalWriter writer;
    REQUIRE(writer.open(path, runtime::CapacityProfile::SMALL,
                        runtime::JournalSync::NONE, nullptr, error));
    Harness live({}, &writer);
    const auto a = live.login(1);
    const auto b = live.login(2);
    live.new_order(a, 10, core::Side::SELL, 101 * PX, 10);
    live.new_order(a, 11, core::Side::SELL, 102 * PX, 10);
    live.new_order(b, 10, core::Side::BUY, 101 * PX, 4);
    live.send(a, protocol::ModifyOrder{
                     .client_order_id = 11, .price = 102 * PX, .qty = 3});
    live.new_order(b, 12, core::Side::BUY, 98 * PX, 5);
    live.send(b, protocol::CancelOrder{.client_order_id = 12});
    live.new_order(b, 13, core::Side::BUY, 97 * PX, 6);
    CHECK_EQ(live.core().open_orders(), 3U);
  }

  runtime::JournalReadResult result;
  Harness recovered;
  runtime::EngineStage<runtime::SmallEngine> stage(
      std::vector<core::Symbol>{core::make_symbol("AAPL")}, nullptr, nullptr);
  REQUIRE(runtime::read_journal(
      path, result, error, [&](const runtime::JournalRecord &record) {
        recovered.core().restore_submission(record);
        stage.replay(record, [&](const runtime::EngineOutput &output) {
          recovered.core().on_engine_output(output);
        });
      }));
  CHECK_EQ(result.records, 7U);
  CHECK_EQ(recovered.core().open_orders(), 3U);

  // Participant 1 sees its two orders with the right open quantities.
  const auto a = recovered.login(1);
  recovered.send(a, protocol::OrderStatusRequest{});
  const auto status = recovered.reports(a);
  REQUIRE(status.size() == 2U);
  std::uint32_t open_total = 0;
  for (const auto &report : status) {
    CHECK(report.kind == ExecKind::STATUS);
    open_total += report.leaves_qty;
  }
  CHECK_EQ(open_total, 6U + 3U);

  // New exchange order IDs continue after the recovered ones.
  recovered.new_order(a, 20, core::Side::SELL, 105 * PX, 1);
  const auto accepted = recovered.reports(a);
  REQUIRE(!accepted.empty());
  CHECK_EQ(accepted[0].exchange_order_id, 6U); // five ADDs so far
  std::filesystem::remove(path);
}
