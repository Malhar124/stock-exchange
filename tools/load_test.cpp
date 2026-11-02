// Load generator for exchange_server. Sends crossing buy/sell pairs (so every
// order trades) with a bounded number in flight, then reports throughput and
// client-observed latency from send to first response.
//
//   load_test --port P [--orders 100000] [--window 256] [--participant 800]
//             [--symbol AAPL] [--host 127.0.0.1]

#include <algorithm>
#include <chrono>
#include <csignal>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "client/client_connection.h"
#include "core/clock.h"
#include "protocol/messages.h"
#include "runtime/metrics.h"
#include "text/order_text.h"

int main(int argc, char **argv) {
  using namespace exchange;
  std::string host = "127.0.0.1";
  std::uint16_t port = 0;
  std::uint64_t orders = 100'000;
  std::uint64_t window = 256;
  core::ParticipantId participant = 800;
  core::Symbol symbol = core::make_symbol("AAPL");
  for (int index = 1; index + 1 < argc; index += 2) {
    const std::string_view arg = argv[index];
    const std::string_view value = argv[index + 1];
    if (arg == "--host") {
      host = value;
    } else if (arg == "--port") {
      port = text::parse_integer<std::uint16_t>(value).value_or(0);
    } else if (arg == "--orders") {
      orders = text::parse_integer<std::uint64_t>(value).value_or(0);
    } else if (arg == "--window") {
      window = text::parse_integer<std::uint64_t>(value).value_or(1);
    } else if (arg == "--participant") {
      participant = text::parse_integer<core::ParticipantId>(value).value_or(0);
    } else if (arg == "--symbol") {
      symbol = core::make_symbol(value);
    }
  }
  if (port == 0 || orders == 0 || participant == 0 || window == 0) {
    std::cerr << "Usage: load_test --port P [--orders N] [--window W] "
                 "[--participant ID] [--symbol S] [--host H]\n";
    return 2;
  }
  std::signal(SIGPIPE, SIG_IGN);

  client::ClientConnection connection;
  std::string error;
  if (!connection.connect(host, port, error) ||
      !connection.login(participant, error, [](const protocol::Frame &) {})) {
    std::cerr << error << '\n';
    return 1;
  }

  std::unordered_map<std::uint64_t, core::Timestamp> sent_at;
  runtime::LatencyHistogram latency;
  std::uint64_t sent = 0;
  std::uint64_t answered = 0;
  std::uint64_t done = 0; // filled, canceled, or rejected
  std::uint64_t rejected = 0;
  std::uint64_t trades = 0;

  const auto on_frame = [&](const protocol::Frame &frame) {
    protocol::ExecutionReport report;
    if (!protocol::decode(frame, report)) {
      return;
    }
    const auto it = sent_at.find(report.client_order_id);
    if (it != sent_at.end()) {
      latency.record(core::now_ns() - it->second);
      sent_at.erase(it);
      ++answered;
    }
    switch (report.kind) {
    case protocol::ExecKind::TRADE:
      ++trades;
      break;
    case protocol::ExecKind::REJECTED:
      ++rejected;
      ++done;
      break;
    case protocol::ExecKind::FILLED:
    case protocol::ExecKind::CANCELED:
      ++done;
      break;
    default:
      break;
    }
  };

  const auto started = std::chrono::steady_clock::now();
  std::vector<std::byte> batch;
  while (done < orders) {
    batch.clear();
    while (sent < orders && sent - answered < window) {
      const std::uint64_t id = ++sent;
      protocol::append_message(
          batch, protocol::NewOrder{
                     .client_order_id = id,
                     .symbol = symbol,
                     .side = (id % 2U) == 1U ? core::Side::SELL : core::Side::BUY,
                     .type = core::OrderType::LIMIT,
                     .qty = 1,
                     .price = 100 * core::PRICE_SCALE});
      sent_at[id] = core::now_ns();
    }
    if (!batch.empty() && !connection.send_encoded(batch)) {
      std::cerr << "send failed\n";
      return 1;
    }
    if (!connection.poll(10, on_frame)) {
      std::cerr << "disconnected\n";
      return 1;
    }
    if (std::chrono::steady_clock::now() - started > std::chrono::minutes(5)) {
      std::cerr << "timed out with " << done << '/' << orders << " done\n";
      return 1;
    }
  }
  const double seconds = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - started)
                             .count();

  const auto snapshot = latency.snapshot();
  std::cout << "orders=" << orders << " trades_reported=" << trades
            << " rejected=" << rejected << " seconds=" << seconds
            << " orders/s=" << static_cast<std::uint64_t>(orders / seconds)
            << "\nlatency send->first response: p50="
            << runtime::format_ns(snapshot.percentile(50))
            << " p90=" << runtime::format_ns(snapshot.percentile(90))
            << " p99=" << runtime::format_ns(snapshot.percentile(99))
            << " max=" << runtime::format_ns(snapshot.max) << '\n';
  return rejected == 0 ? 0 : 1;
}
