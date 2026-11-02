// Market-making client for exchange_server: keeps a ladder of POST_ONLY bids
// and asks around the last trade price (or --mid before any trade), moving
// its quotes as the market moves.
//
//   liquidity_bot [--participant 900] [--symbol AAPL] [--mid 100] [--spread 0.10]
//                 [--step 0.05] [--levels 3] [--size 100] [--interval-ms 500]
//                 [--duration-s 0] [--host 127.0.0.1] [--port 9000]

#include <algorithm>
#include <chrono>
#include <cmath>
#include <csignal>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <utility>

#include "client/client_connection.h"
#include "client/report_text.h"
#include "protocol/messages.h"
#include "text/order_text.h"

namespace {

using namespace exchange;

volatile std::sig_atomic_t g_stop_requested = 0;
void request_stop(int) { g_stop_requested = 1; }

struct Options {
  std::string host{"127.0.0.1"};
  std::uint16_t port{9000};
  core::ParticipantId participant{900};
  core::Symbol symbol{core::make_symbol("AAPL")};
  core::Price mid{100 * core::PRICE_SCALE};
  core::Price spread{core::PRICE_SCALE / 10};
  core::Price step{core::PRICE_SCALE / 20};
  core::Price tick{core::PRICE_SCALE / 100};
  int levels{3};
  core::Quantity size{100};
  int interval_ms{500};
  int duration_s{0};
};

struct Quote {
  core::Side side;
  core::Price price;
  core::Quantity open;
  bool cancel_sent{false};
};

class Bot {
public:
  Bot(const Options &options, client::ClientConnection &connection)
      : options_(options), connection_(connection) {}

  void on_frame(const protocol::Frame &frame) {
    protocol::ExecutionReport report;
    protocol::TradePrint trade;
    protocol::BookSnapshotEnd snapshot_end;
    if (protocol::decode(frame, report)) {
      on_report(report);
    } else if (protocol::decode(frame, trade)) {
      last_trade_ = trade.price;
    } else if (protocol::decode(frame, snapshot_end)) {
      last_trade_ = snapshot_end.last_trade_price;
    }
  }

  // Cancels quotes away from the target prices, then adds missing ones.
  void requote() {
    const core::Price reference = last_trade_ != 0 ? last_trade_ : options_.mid;
    std::set<std::pair<core::Side, core::Price>> wanted;
    for (int level = 0; level < options_.levels; ++level) {
      const core::Price offset = (options_.spread / 2) + (level * options_.step);
      wanted.insert({core::Side::BUY, round_down(reference - offset)});
      wanted.insert({core::Side::SELL, round_up(reference + offset)});
    }

    std::set<std::pair<core::Side, core::Price>> held;
    for (auto &[id, quote] : quotes_) {
      const std::pair key{quote.side, quote.price};
      if (!wanted.contains(key) && !quote.cancel_sent) {
        connection_.send(protocol::CancelOrder{.client_order_id = id});
        quote.cancel_sent = true;
      }
      if (!quote.cancel_sent) {
        held.insert(key);
      }
    }
    for (const auto &[side, price] : wanted) {
      if (price <= 0 || held.contains({side, price})) {
        continue;
      }
      const std::uint64_t id = next_id_++;
      quotes_[id] = Quote{.side = side, .price = price, .open = options_.size};
      connection_.send(protocol::NewOrder{.client_order_id = id,
                                          .symbol = options_.symbol,
                                          .side = side,
                                          .type = core::OrderType::POST_ONLY,
                                          .qty = options_.size,
                                          .price = price});
    }
  }

  void cancel_all() {
    for (auto &[id, quote] : quotes_) {
      if (!quote.cancel_sent) {
        connection_.send(protocol::CancelOrder{.client_order_id = id});
        quote.cancel_sent = true;
      }
    }
  }

  void print_status() const {
    std::string bids;
    std::string asks;
    for (const auto &[id, quote] : quotes_) {
      if (quote.cancel_sent) {
        continue;
      }
      std::string &side = quote.side == core::Side::BUY ? bids : asks;
      side += ' ' + text::format_price(quote.price) + 'x' +
              std::to_string(quote.open);
    }
    std::cout << "[bot] last="
              << (last_trade_ == 0 ? std::string("-")
                                   : text::format_price(last_trade_))
              << " bids:" << (bids.empty() ? " -" : bids)
              << " asks:" << (asks.empty() ? " -" : asks)
              << " fills=" << fills_ << " position=" << position_ << std::endl;
  }

private:
  void on_report(const protocol::ExecutionReport &report) {
    using protocol::ExecKind;
    switch (report.kind) {
    case ExecKind::TRADE: {
      ++fills_;
      position_ += report.side == core::Side::BUY
                       ? static_cast<std::int64_t>(report.qty)
                       : -static_cast<std::int64_t>(report.qty);
      std::cout << "[bot] " << client::format_report(report) << std::endl;
      const auto it = quotes_.find(report.client_order_id);
      if (it != quotes_.end()) {
        it->second.open = report.leaves_qty;
      }
      break;
    }
    case ExecKind::FILLED:
    case ExecKind::CANCELED:
    case ExecKind::REJECTED:
      quotes_.erase(report.client_order_id);
      break;
    case ExecKind::REQUEST_REJECTED:
      // The cancel lost a race with a fill; the order is already gone.
      if (report.reject_source == protocol::RejectSource::ENGINE) {
        quotes_.erase(report.client_order_id);
      }
      break;
    default:
      break;
    }
  }

  [[nodiscard]] core::Price round_down(core::Price price) const {
    return price - (price % options_.tick);
  }
  [[nodiscard]] core::Price round_up(core::Price price) const {
    const core::Price remainder = price % options_.tick;
    return remainder == 0 ? price : price + options_.tick - remainder;
  }

  const Options &options_;
  client::ClientConnection &connection_;
  std::map<std::uint64_t, Quote> quotes_;
  std::uint64_t next_id_{1};
  core::Price last_trade_{0};
  std::uint64_t fills_{0};
  std::int64_t position_{0};
};

bool parse_options(int argc, char **argv, Options &options) {
  for (int index = 1; index < argc; ++index) {
    const std::string_view arg = argv[index];
    if (index + 1 >= argc) {
      return false;
    }
    const std::string_view value = argv[++index];
    const auto price = [&](core::Price &out) {
      const auto parsed = text::parse_price(value);
      if (parsed && *parsed > 0) {
        out = *parsed;
        return true;
      }
      return false;
    };
    const auto number = [&](auto &out) {
      const auto parsed =
          text::parse_integer<std::remove_reference_t<decltype(out)>>(value);
      if (parsed) {
        out = *parsed;
      }
      return parsed.has_value();
    };
    bool ok = true;
    if (arg == "--host") {
      options.host = value;
    } else if (arg == "--port") {
      ok = number(options.port);
    } else if (arg == "--participant") {
      ok = number(options.participant);
    } else if (arg == "--symbol") {
      ok = value.size() <= core::Symbol{}.size();
      options.symbol = core::make_symbol(value);
    } else if (arg == "--mid") {
      ok = price(options.mid);
    } else if (arg == "--spread") {
      ok = price(options.spread);
    } else if (arg == "--step") {
      ok = price(options.step);
    } else if (arg == "--levels") {
      ok = number(options.levels);
    } else if (arg == "--size") {
      ok = number(options.size);
    } else if (arg == "--interval-ms") {
      ok = number(options.interval_ms);
    } else if (arg == "--duration-s") {
      ok = number(options.duration_s);
    } else {
      ok = false;
    }
    if (!ok) {
      return false;
    }
  }
  return options.participant != 0 && options.levels > 0 &&
         options.interval_ms > 0;
}

} // namespace

int main(int argc, char **argv) {
  Options options;
  if (!parse_options(argc, argv, options)) {
    std::cerr << "Usage: liquidity_bot [--participant N] [--symbol S] "
                 "[--mid P] [--spread P] [--step P] [--levels N] [--size Q] "
                 "[--interval-ms MS] [--duration-s S] [--host H] [--port P]\n";
    return 2;
  }
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGINT, request_stop);
  std::signal(SIGTERM, request_stop);

  client::ClientConnection connection;
  std::string error;
  if (!connection.connect(options.host, options.port, error)) {
    std::cerr << error << '\n';
    return 1;
  }
  Bot bot(options, connection);
  const auto on_frame = [&](const protocol::Frame &frame) {
    bot.on_frame(frame);
  };
  if (!connection.login(options.participant, error, on_frame)) {
    std::cerr << error << '\n';
    return 1;
  }
  connection.send(protocol::Subscribe{.symbol = options.symbol});
  std::cout << "[bot] quoting " << core::symbol_view(options.symbol)
            << " as participant " << options.participant << std::endl;

  const auto started = std::chrono::steady_clock::now();
  auto next_quote = started;
  auto next_status = started + std::chrono::seconds(5);
  while (g_stop_requested == 0) {
    const auto now = std::chrono::steady_clock::now();
    if (options.duration_s > 0 &&
        now - started >= std::chrono::seconds(options.duration_s)) {
      break;
    }
    if (now >= next_quote) {
      bot.requote();
      next_quote = now + std::chrono::milliseconds(options.interval_ms);
    }
    if (now >= next_status) {
      bot.print_status();
      next_status = now + std::chrono::seconds(5);
    }
    if (!connection.poll(20, on_frame)) {
      std::cerr << "[bot] disconnected\n";
      return 1;
    }
  }

  bot.cancel_all();
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
  while (std::chrono::steady_clock::now() < deadline &&
         connection.poll(50, on_frame)) {
  }
  bot.print_status();
  return 0;
}
