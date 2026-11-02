#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "core/spsc_queue.h"
#include "gateway/gateway_core.h"
#include "gateway/gateway_server.h"
#include "net/socket.h"
#include "protocol/messages.h"
#include "test_support.h"

namespace {

using namespace exchange;

// A gateway server on a free loopback port, with no engine behind it.
class ServerHarness {
public:
  explicit ServerHarness(std::size_t max_outbound_bytes)
      : core_(std::vector<core::Symbol>{core::make_symbol("AAPL")}, {},
              [](const runtime::GatewayRequest &) { return true; },
              &metrics_) {
    std::string error;
    listen_fd_ = net::listen_tcp("127.0.0.1", 0, error);
    REQUIRE(listen_fd_ >= 0);
    REQUIRE(::pipe(wake_) == 0);
    net::set_nonblocking(wake_[0]);
    server_ = std::make_unique<gateway::GatewayServer>(
        core_, listen_fd_, wake_[0],
        [](runtime::EngineOutput &) { return false; }, &metrics_,
        max_outbound_bytes);
  }

  ~ServerHarness() {
    server_.reset();
    ::close(wake_[0]);
    ::close(wake_[1]);
  }

  // A client socket with a small receive buffer, so its window fills fast.
  [[nodiscard]] int connect_client() const {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int small = 4096;
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &small, sizeof(small));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(net::local_port(listen_fd_));
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, reinterpret_cast<const sockaddr *>(&addr),
                  sizeof(addr)) != 0) {
      ::close(fd);
      return -1;
    }
    net::set_nonblocking(fd);
    return fd;
  }

  // Polls until `done` or about five seconds pass.
  template <typename Done> bool poll_until(Done &&done) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!done()) {
      if (std::chrono::steady_clock::now() > deadline) {
        return false;
      }
      server_->poll_once(1);
    }
    return true;
  }

  gateway::GatewayServer &server() { return *server_; }
  gateway::GatewayCore &core() { return core_; }
  runtime::GatewayMetrics &metrics() { return metrics_; }

private:
  runtime::GatewayMetrics metrics_;
  gateway::GatewayCore core_;
  int listen_fd_{-1};
  int wake_[2]{-1, -1};
  std::unique_ptr<gateway::GatewayServer> server_;
};

} // namespace

TEST_CASE(spsc_queue_keeps_order_and_payload_across_threads) {
  struct Item {
    std::uint64_t value;
    std::uint64_t check; // must match value, so a torn read shows up
  };
  auto queue = std::make_unique<core::SPSCQueue<Item, 1024>>();
  constexpr std::uint64_t COUNT = 2'000'000;
  std::thread producer([&] {
    for (std::uint64_t value = 1; value <= COUNT; ++value) {
      while (!queue->push(Item{value, (value * 3U) + 1U})) {
        std::this_thread::yield();
      }
    }
  });
  bool intact = true;
  Item item{};
  for (std::uint64_t expected = 1; expected <= COUNT;) {
    if (queue->pop(item)) {
      intact = intact && item.value == expected &&
               item.check == (expected * 3U) + 1U;
      ++expected;
    }
  }
  producer.join();
  CHECK(intact);
  CHECK(queue->empty());
}

TEST_CASE(gateway_server_drops_a_client_that_sends_a_malformed_frame) {
  ServerHarness harness(64U << 20U);
  const int client = harness.connect_client();
  REQUIRE(client >= 0);
  REQUIRE(harness.poll_until([&] { return harness.core().session_count() == 1U; }));

  // A body longer than any message is a protocol violation.
  const protocol::MessageHeader header{.body_length = 2'000,
                                       .type = protocol::MessageType::LOGIN};
  REQUIRE(::send(client, &header, sizeof(header), 0) ==
          static_cast<ssize_t>(sizeof(header)));
  CHECK(harness.poll_until([&] { return harness.core().session_count() == 0U; }));
  ::close(client);
}

TEST_CASE(gateway_server_drops_a_client_that_stops_reading) {
  ServerHarness harness(64U * 1024U);
  const int client = harness.connect_client();
  REQUIRE(client >= 0);
  REQUIRE(harness.poll_until([&] { return harness.core().session_count() == 1U; }));

  // Each status request is answered, but the client never reads the answers.
  std::vector<std::byte> requests;
  for (int index = 0; index < 4'096; ++index) {
    protocol::append_message(requests, protocol::OrderStatusRequest{});
  }
  std::size_t offset = 0;
  const bool dropped = harness.poll_until([&] {
    if (offset == requests.size()) {
      offset = 0;
    }
    const ssize_t sent = ::send(client, requests.data() + offset,
                                requests.size() - offset, 0);
    if (sent > 0) {
      offset += static_cast<std::size_t>(sent);
    }
    return harness.metrics().slow_consumer_disconnects == 1U;
  });
  CHECK(dropped);
  CHECK_EQ(harness.core().session_count(), 0U);
  ::close(client);
}

TEST_CASE(accepted_sockets_detect_dead_peers) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  REQUIRE(fd >= 0);
  net::set_keepalive(fd);
  int enabled = 0;
  socklen_t length = sizeof(enabled);
  REQUIRE(::getsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enabled, &length) == 0);
  CHECK(enabled != 0);
  ::close(fd);
}
