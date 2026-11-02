// Command-line trading client for exchange_server.
//
//   trader_terminal --participant <id> [--host 127.0.0.1] [--port 9000]
//
// Reads commands from stdin (type "help"), so it also works with scripts:
//   printf 'buy AAPL 10 101\nwait 200\n' | trader_terminal --participant 7

#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "client/client_connection.h"
#include "client/report_text.h"
#include "protocol/messages.h"
#include "text/order_text.h"

namespace {

using namespace exchange;

struct LocalBook {
  std::map<core::Price, core::Quantity, std::greater<>> bids;
  std::map<core::Price, core::Quantity> asks;
  core::Price last_trade_price{0};
  bool snapshot_complete{false};
};

class Terminal {
public:
  explicit Terminal(client::ClientConnection &connection)
      : connection_(connection) {}

  void on_frame(const protocol::Frame &frame) {
    protocol::ExecutionReport report;
    protocol::BookUpdate update;
    protocol::BookSnapshotBegin begin;
    protocol::BookSnapshotEnd end;
    protocol::TradePrint trade;
    protocol::OrderStatusEnd status_end;
    if (protocol::decode(frame, report)) {
      next_client_order_id_ =
          std::max(next_client_order_id_, report.client_order_id + 1U);
      if (report.kind == protocol::ExecKind::STATUS) {
        if (!quiet_status_) {
          print("  " + client::format_report(report));
        }
      } else {
        print("< " + client::format_report(report));
      }
    } else if (protocol::decode(frame, begin)) {
      books_[begin.symbol] = LocalBook{};
    } else if (protocol::decode(frame, update)) {
      LocalBook &book = books_[update.symbol];
      if (update.side == core::Side::BUY) {
        apply_level(book.bids, update.price, update.qty);
      } else {
        apply_level(book.asks, update.price, update.qty);
      }
    } else if (protocol::decode(frame, end)) {
      LocalBook &book = books_[end.symbol];
      book.last_trade_price = end.last_trade_price;
      book.snapshot_complete = true;
    } else if (protocol::decode(frame, trade)) {
      books_[trade.symbol].last_trade_price = trade.price;
      print("* TRADE " + std::string(core::symbol_view(trade.symbol)) + ' ' +
            std::to_string(trade.qty) + " @ " + text::format_price(trade.price));
    } else if (protocol::decode(frame, status_end)) {
      status_pending_ = false;
      if (!quiet_status_) {
        print("  " + std::to_string(status_end.open_orders) + " open orders");
      }
    }
  }

  // Asks for open orders so new client order IDs never collide with them.
  void sync_order_ids() {
    quiet_status_ = true;
    request_status_and_wait();
    quiet_status_ = false;
  }

  // Returns false to quit.
  bool execute(std::string_view line) {
    const std::vector<std::string_view> words = text::split_words(line);
    if (words.empty() || words[0].front() == '#') {
      return true;
    }
    const std::string_view command = words[0];
    if (command == "quit" || command == "exit") {
      return false;
    }
    if (command == "help") {
      print_help();
    } else if (command == "buy" || command == "sell") {
      send_new_order(words, command == "buy" ? core::Side::BUY
                                             : core::Side::SELL);
    } else if (command == "cancel" && words.size() == 2U) {
      const auto id = text::parse_integer<std::uint64_t>(words[1]);
      if (!id) {
        print("! invalid order id");
      } else {
        connection_.send(protocol::CancelOrder{.client_order_id = *id});
      }
    } else if (command == "modify" && words.size() == 4U) {
      const auto id = text::parse_integer<std::uint64_t>(words[1]);
      const auto qty = text::parse_integer<core::Quantity>(words[2]);
      const auto price = text::parse_price(words[3]);
      if (!id || !qty || !price) {
        print("! usage: modify <id> <qty> <price>");
      } else {
        connection_.send(protocol::ModifyOrder{
            .client_order_id = *id, .price = *price, .qty = *qty});
      }
    } else if (command == "book" && words.size() == 2U) {
      show_book(core::make_symbol(words[1]));
    } else if (command == "orders") {
      request_status_and_wait();
    } else if (command == "wait" && words.size() == 2U) {
      const auto ms = text::parse_integer<int>(words[1]);
      pump_for(ms.value_or(0));
    } else {
      print("! unknown command; type 'help'");
    }
    return true;
  }

  // Processes server messages for up to `ms` milliseconds.
  bool pump_for(int ms) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline) {
      const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
          deadline - std::chrono::steady_clock::now());
      if (!connection_.poll(static_cast<int>(std::max<long long>(
                                1, left.count())),
                            [&](const protocol::Frame &frame) {
                              on_frame(frame);
                            })) {
        return false;
      }
    }
    return true;
  }

  void print(const std::string &line) { std::cout << line << std::endl; }

private:
  static void apply_level(auto &levels, core::Price price, core::Quantity qty) {
    if (qty == 0U) {
      levels.erase(price);
    } else {
      levels[price] = qty;
    }
  }

  void send_new_order(const std::vector<std::string_view> &words,
                      core::Side side) {
    if (words.size() < 3U || words[1].size() > core::Symbol{}.size()) {
      print("! usage: buy|sell <symbol> <qty> [price] [type=..] [trigger=..] "
            "[display=..] [stp=..]");
      return;
    }
    protocol::NewOrder order{.client_order_id = next_client_order_id_,
                             .symbol = core::make_symbol(words[1]),
                             .side = side};
    const auto qty = text::parse_integer<core::Quantity>(words[2]);
    if (!qty) {
      print("! invalid quantity");
      return;
    }
    order.qty = *qty;

    bool has_price = false;
    bool has_type = false;
    for (std::size_t index = 3; index < words.size(); ++index) {
      const std::string_view word = words[index];
      const std::size_t equals = word.find('=');
      if (equals == std::string_view::npos) {
        const auto price = text::parse_price(word);
        if (!price || has_price) {
          print("! invalid price: " + std::string(word));
          return;
        }
        order.price = *price;
        has_price = true;
        continue;
      }
      const std::string_view key = word.substr(0, equals);
      const std::string_view value = word.substr(equals + 1);
      bool ok = false;
      if (key == "type") {
        const auto type = text::parse_order_type(value);
        ok = type.has_value();
        if (ok) {
          order.type = *type;
          has_type = true;
        }
      } else if (key == "trigger") {
        const auto price = text::parse_price(value);
        ok = price.has_value();
        if (ok) {
          order.trigger_price = *price;
        }
      } else if (key == "display") {
        const auto display = text::parse_integer<core::Quantity>(value);
        ok = display.has_value();
        if (ok) {
          order.display_qty = *display;
        }
      } else if (key == "stp") {
        const auto stp = text::parse_stp(value);
        ok = stp.has_value();
        if (ok) {
          order.stp = *stp;
        }
      }
      if (!ok) {
        print("! invalid argument: " + std::string(word));
        return;
      }
    }
    if (!has_type) {
      order.type = has_price ? core::OrderType::LIMIT : core::OrderType::MARKET;
    }

    print("> order id=" + std::to_string(order.client_order_id));
    ++next_client_order_id_;
    connection_.send(order);
  }

  void show_book(const core::Symbol &symbol) {
    LocalBook &book = books_[symbol];
    if (!book.snapshot_complete) {
      connection_.send(protocol::Subscribe{.symbol = symbol});
      const auto deadline =
          std::chrono::steady_clock::now() + std::chrono::seconds(2);
      while (!books_[symbol].snapshot_complete &&
             std::chrono::steady_clock::now() < deadline) {
        if (!pump_for(20)) {
          return;
        }
      }
    }
    const LocalBook &current = books_[symbol];
    if (!current.snapshot_complete) {
      print("! no book for " + std::string(core::symbol_view(symbol)));
      return;
    }
    print("  " + std::string(core::symbol_view(symbol)) + "  last=" +
          (current.last_trade_price == 0
               ? std::string("-")
               : text::format_price(current.last_trade_price)));
    std::vector<std::pair<core::Price, core::Quantity>> asks(
        current.asks.begin(), current.asks.end());
    const std::size_t shown_asks = std::min<std::size_t>(asks.size(), 5U);
    for (std::size_t index = shown_asks; index-- > 0U;) {
      print("    ask " + text::format_price(asks[index].first) + "  " +
            std::to_string(asks[index].second));
    }
    std::size_t shown_bids = 0;
    for (const auto &[price, qty] : current.bids) {
      if (shown_bids++ == 5U) {
        break;
      }
      print("    bid " + text::format_price(price) + "  " +
            std::to_string(qty));
    }
    if (current.bids.empty() && current.asks.empty()) {
      print("    (empty)");
    }
  }

  void request_status_and_wait() {
    status_pending_ = true;
    connection_.send(protocol::OrderStatusRequest{});
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (status_pending_ && std::chrono::steady_clock::now() < deadline) {
      if (!pump_for(20)) {
        return;
      }
    }
  }

  void print_help() {
    print("  buy|sell <symbol> <qty> [price] [type=LIMIT|MARKET|IOC|FOK|GTC|"
          "STOP|STOP_LIMIT|ICEBERG|POST_ONLY]");
    print("           [trigger=<price>] [display=<qty>] "
          "[stp=CANCEL_RESTING|CANCEL_AGGRESSOR|CANCEL_BOTH]");
    print("  cancel <id>                 cancel one of your orders");
    print("  modify <id> <qty> <price>   change quantity and/or price");
    print("  book <symbol>               subscribe to and show the book");
    print("  orders                      list your open orders");
    print("  wait <ms>                   process messages for a while");
    print("  quit");
  }

  client::ClientConnection &connection_;
  std::map<core::Symbol, LocalBook> books_;
  std::uint64_t next_client_order_id_{1};
  bool status_pending_{false};
  bool quiet_status_{false};
};

} // namespace

int main(int argc, char **argv) {
  std::string host = "127.0.0.1";
  std::uint16_t port = 9000;
  core::ParticipantId participant = 0;
  for (int index = 1; index + 1 < argc; index += 2) {
    const std::string_view arg = argv[index];
    if (arg == "--host") {
      host = argv[index + 1];
    } else if (arg == "--port") {
      port = text::parse_integer<std::uint16_t>(argv[index + 1]).value_or(0);
    } else if (arg == "--participant") {
      participant =
          text::parse_integer<core::ParticipantId>(argv[index + 1]).value_or(0);
    }
  }
  if (participant == 0 || port == 0) {
    std::cerr << "Usage: trader_terminal --participant <id> [--host H] "
                 "[--port P]\n";
    return 2;
  }
  std::signal(SIGPIPE, SIG_IGN);

  client::ClientConnection connection;
  std::string error;
  if (!connection.connect(host, port, error)) {
    std::cerr << error << '\n';
    return 1;
  }
  Terminal terminal(connection);
  if (!connection.login(participant, error, [&](const protocol::Frame &frame) {
        terminal.on_frame(frame);
      })) {
    std::cerr << error << '\n';
    return 1;
  }
  terminal.sync_order_ids();

  const bool interactive = ::isatty(STDIN_FILENO) != 0;
  if (interactive) {
    terminal.print("connected as participant " + std::to_string(participant) +
                   "; type 'help'");
    std::cout << "> " << std::flush;
  }

  std::string pending;
  bool stdin_open = true;
  while (stdin_open) {
    bool stdin_ready = false;
    if (!connection.poll(
            200, [&](const protocol::Frame &frame) { terminal.on_frame(frame); },
            STDIN_FILENO, &stdin_ready)) {
      std::cerr << "disconnected\n";
      return 1;
    }
    if (!stdin_ready) {
      continue;
    }
    char chunk[4096];
    const ssize_t got = ::read(STDIN_FILENO, chunk, sizeof(chunk));
    if (got <= 0) {
      stdin_open = false;
      break;
    }
    pending.append(chunk, static_cast<std::size_t>(got));
    std::size_t newline;
    while ((newline = pending.find('\n')) != std::string::npos) {
      const std::string line = pending.substr(0, newline);
      pending.erase(0, newline + 1);
      if (!terminal.execute(line)) {
        return 0;
      }
      if (interactive) {
        std::cout << "> " << std::flush;
      }
    }
  }
  // Let replies to the last commands arrive before exiting.
  (void)terminal.pump_for(300);
  return 0;
}
