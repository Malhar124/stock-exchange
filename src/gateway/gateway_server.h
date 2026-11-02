#pragma once

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <functional>
#include <span>
#include <unordered_map>
#include <vector>

#include "gateway/gateway_core.h"
#include "net/socket.h"
#include "protocol/messages.h"
#include "runtime/engine_stage.h"

namespace exchange::gateway {

// The gateway's I/O loop: accepts connections, decodes client messages into
// GatewayCore, drains engine outputs, and writes replies. It never blocks on
// the engine: a full request queue is reported to the client as ENGINE_BUSY.
class GatewayServer {
public:
  using PopOutput = std::function<bool(runtime::EngineOutput &)>;

  GatewayServer(GatewayCore &core, int listen_fd, int wake_fd,
                PopOutput pop_output, runtime::GatewayMetrics *metrics,
                std::size_t max_outbound_bytes = 64U << 20U)
      : core_(core), listen_fd_(listen_fd), wake_fd_(wake_fd),
        pop_output_(std::move(pop_output)), metrics_(metrics),
        max_outbound_bytes_(max_outbound_bytes) {}

  GatewayServer(const GatewayServer &) = delete;
  GatewayServer &operator=(const GatewayServer &) = delete;
  ~GatewayServer() { close_all(); }

  void poll_once(int timeout_ms) {
    std::vector<pollfd> fds;
    std::vector<SessionId> fd_sessions;
    if (listen_fd_ >= 0 && !accept_paused_) {
      fds.push_back(pollfd{.fd = listen_fd_, .events = POLLIN, .revents = 0});
      fd_sessions.push_back(0);
    }
    fds.push_back(pollfd{.fd = wake_fd_, .events = POLLIN, .revents = 0});
    fd_sessions.push_back(0);
    for (const auto &[session_id, connection] : connections_) {
      const std::vector<std::byte> *out = core_.outbound(session_id);
      const short events =
          static_cast<short>(POLLIN | (out != nullptr && !out->empty()
                                           ? POLLOUT
                                           : 0));
      fds.push_back(pollfd{.fd = connection.fd, .events = events, .revents = 0});
      fd_sessions.push_back(session_id);
    }

    const int ready =
        ::poll(fds.data(), fds.size(), output_backlog_ ? 0 : timeout_ms);
    if (ready > 0) {
      for (std::size_t index = 0; index < fds.size(); ++index) {
        const pollfd &entry = fds[index];
        if (entry.revents == 0) {
          continue;
        }
        if (entry.fd == listen_fd_) {
          accept_connections();
        } else if (entry.fd == wake_fd_) {
          char drain[256];
          while (::read(wake_fd_, drain, sizeof(drain)) > 0) {
          }
        } else if ((entry.revents & (POLLIN | POLLHUP | POLLERR)) != 0) {
          read_session(fd_sessions[index]);
        }
      }
    }

    output_backlog_ = drain_engine_outputs();
    core_.retry_pending_cancels();
    flush_all();
  }

  void close_listener() {
    if (listen_fd_ >= 0) {
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
  }

  // True while engine outputs or replies are still waiting to go out.
  [[nodiscard]] bool has_pending_writes() {
    if (output_backlog_) {
      return true;
    }
    for (const auto &[session_id, connection] : connections_) {
      const std::vector<std::byte> *out = core_.outbound(session_id);
      if (out != nullptr && !out->empty()) {
        return true;
      }
    }
    return false;
  }

  void close_all() {
    while (!connections_.empty()) {
      close_session(connections_.begin()->first);
    }
    close_listener();
  }

private:
  // Outputs handled per poll before replies are written, so one huge request
  // cannot grow a client's buffer far past the slow-consumer limit.
  static constexpr std::size_t MAX_OUTPUTS_PER_POLL = 65'536;

  struct Connection {
    int fd{-1};
    std::vector<std::byte> in;
    std::size_t sent{0}; // bytes of the session's outbound buffer written
  };

  void accept_connections() {
    while (true) {
      const int fd = ::accept(listen_fd_, nullptr, nullptr);
      if (fd < 0) {
        // Out of descriptors, the listener stays readable: stop polling it
        // until a connection closes, instead of spinning.
        accept_paused_ = errno == EMFILE || errno == ENFILE;
        return;
      }
      net::set_nonblocking(fd);
      net::set_nodelay(fd);
      net::set_keepalive(fd);
#ifdef SO_NOSIGPIPE
      int enabled = 1;
      (void)::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled,
                         sizeof(enabled));
#endif
      connections_.emplace(core_.open_session(), Connection{.fd = fd});
    }
  }

  void read_session(SessionId session_id) {
    const auto it = connections_.find(session_id);
    if (it == connections_.end()) {
      return;
    }
    Connection &connection = it->second;
    const bool open = net::read_available(connection.fd, connection.in);

    std::size_t offset = 0;
    while (true) {
      protocol::Frame frame;
      std::size_t consumed = 0;
      const auto status = protocol::next_frame(
          std::span<const std::byte>(connection.in).subspan(offset), frame,
          consumed);
      if (status == protocol::FrameStatus::INCOMPLETE) {
        break;
      }
      if (status == protocol::FrameStatus::MALFORMED ||
          !core_.on_message(session_id, frame)) {
        (void)flush_session(session_id); // e.g. the refused login's reply
        close_session(session_id);
        return;
      }
      offset += consumed;
    }
    connection.in.erase(connection.in.begin(),
                        connection.in.begin() +
                            static_cast<std::ptrdiff_t>(offset));

    if (!open) {
      (void)flush_session(session_id); // best effort before closing
      close_session(session_id);
    }
  }

  // Returns true if it stopped at the limit, so more may be waiting.
  bool drain_engine_outputs() {
    runtime::EngineOutput output;
    for (std::size_t count = 0; count < MAX_OUTPUTS_PER_POLL; ++count) {
      if (!pop_output_(output)) {
        return false;
      }
      core_.on_engine_output(output);
    }
    return true;
  }

  void flush_all() {
    std::vector<SessionId> failed;
    for (const auto &[session_id, connection] : connections_) {
      if (!flush_session(session_id)) {
        failed.push_back(session_id);
      }
    }
    for (const SessionId session_id : failed) {
      close_session(session_id);
    }
  }

  [[nodiscard]] bool flush_session(SessionId session_id) {
    std::vector<std::byte> *out = core_.outbound(session_id);
    const auto it = connections_.find(session_id);
    if (out == nullptr || it == connections_.end() || out->empty()) {
      return true;
    }
    std::size_t &sent = it->second.sent;
    if (!net::write_available(it->second.fd, *out, sent)) {
      return false;
    }
    if (sent == out->size()) {
      out->clear();
      sent = 0;
    } else if (sent > out->size() / 2U) {
      // Dropping written bytes only once they are most of the buffer moves
      // each unsent byte a bounded number of times.
      out->erase(out->begin(), out->begin() + static_cast<std::ptrdiff_t>(sent));
      sent = 0;
    }
    if (out->size() - sent > max_outbound_bytes_) {
      // The client is not reading; drop it rather than buffer without bound.
      if (metrics_ != nullptr) {
        ++metrics_->slow_consumer_disconnects;
      }
      return false;
    }
    return true;
  }

  void close_session(SessionId session_id) {
    const auto it = connections_.find(session_id);
    if (it == connections_.end()) {
      return;
    }
    ::close(it->second.fd);
    connections_.erase(it);
    core_.close_session(session_id);
    accept_paused_ = false; // a descriptor is free again
  }

  GatewayCore &core_;
  int listen_fd_{-1};
  int wake_fd_{-1};
  PopOutput pop_output_;
  runtime::GatewayMetrics *metrics_{nullptr};
  std::size_t max_outbound_bytes_{0};
  std::unordered_map<SessionId, Connection> connections_;
  bool output_backlog_{false};
  bool accept_paused_{false};
};

} // namespace exchange::gateway
