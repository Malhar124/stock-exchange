#pragma once

#include <poll.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "net/socket.h"
#include "protocol/messages.h"

namespace exchange::client {

// A blocking-send, polled-receive connection to exchange_server.
class ClientConnection {
public:
  ClientConnection() = default;
  ClientConnection(const ClientConnection &) = delete;
  ClientConnection &operator=(const ClientConnection &) = delete;
  ~ClientConnection() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  [[nodiscard]] bool connect(const std::string &host, std::uint16_t port,
                             std::string &error) {
    fd_ = net::connect_tcp(host, port, error);
    return fd_ >= 0;
  }

  // Logs in with the secret in EXCHANGE_SECRET (if the server needs one)
  // and waits for the response. Frames that arrive first go to on_frame.
  template <typename OnFrame>
  [[nodiscard]] bool login(core::ParticipantId participant, std::string &error,
                           OnFrame &&on_frame) {
    const char *env = std::getenv("EXCHANGE_SECRET");
    const std::string_view secret = env == nullptr ? "" : env;
    if (secret.size() > protocol::Secret{}.size()) {
      error = "EXCHANGE_SECRET is longer than 32 characters";
      return false;
    }
    send(protocol::Login{.participant_id = participant,
                         .secret = protocol::make_secret(secret)});
    bool answered = false;
    protocol::LoginResponse response;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!answered && std::chrono::steady_clock::now() < deadline) {
      const bool open = poll(100, [&](const protocol::Frame &frame) {
        if (protocol::decode(frame, response)) {
          answered = true;
        } else {
          on_frame(frame);
        }
      });
      // A refused login is answered, then the server closes the connection.
      if (!open && !answered) {
        error = "connection closed during login";
        return false;
      }
    }
    if (!answered) {
      error = "no login response";
      return false;
    }
    switch (response.result) {
    case protocol::LoginResult::ACCEPTED:
      return true;
    case protocol::LoginResult::ALREADY_LOGGED_IN:
      error = "participant is already logged in elsewhere";
      break;
    case protocol::LoginResult::AUTHENTICATION_FAILED:
      error = "authentication failed; check EXCHANGE_SECRET";
      break;
    default:
      error = "login refused";
      break;
    }
    return false;
  }

  template <typename Message> bool send(const Message &message) {
    std::vector<std::byte> out;
    protocol::append_message(out, message);
    return net::write_all(fd_, out);
  }

  // Sends pre-encoded messages; `bytes` is empty afterwards.
  bool send_encoded(std::vector<std::byte> &bytes) {
    return net::write_all(fd_, bytes);
  }

  // Waits up to timeout_ms for data (or for extra_fd to become readable),
  // then delivers every complete frame. Returns false once disconnected.
  template <typename OnFrame>
  [[nodiscard]] bool poll(int timeout_ms, OnFrame &&on_frame,
                          int extra_fd = -1, bool *extra_ready = nullptr) {
    pollfd fds[2] = {{.fd = fd_, .events = POLLIN, .revents = 0},
                     {.fd = extra_fd, .events = POLLIN, .revents = 0}};
    const nfds_t count = extra_fd >= 0 ? 2 : 1;
    if (extra_ready != nullptr) {
      *extra_ready = false;
    }
    const int ready = ::poll(fds, count, timeout_ms);
    if (ready <= 0) {
      return true;
    }
    if (count == 2 && extra_ready != nullptr &&
        (fds[1].revents & (POLLIN | POLLHUP)) != 0) {
      *extra_ready = true;
    }
    if ((fds[0].revents & (POLLIN | POLLHUP | POLLERR)) == 0) {
      return true;
    }
    const bool open = net::read_available(fd_, in_);
    std::size_t offset = 0;
    while (true) {
      protocol::Frame frame;
      std::size_t consumed = 0;
      const auto status = protocol::next_frame(
          std::span<const std::byte>(in_).subspan(offset), frame, consumed);
      if (status != protocol::FrameStatus::COMPLETE) {
        break;
      }
      on_frame(frame);
      offset += consumed;
    }
    in_.erase(in_.begin(), in_.begin() + static_cast<std::ptrdiff_t>(offset));
    return open;
  }

private:
  int fd_{-1};
  std::vector<std::byte> in_;
};

} // namespace exchange::client
