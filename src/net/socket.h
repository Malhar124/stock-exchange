#pragma once

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <vector>

// Thin POSIX TCP helpers. Callers should ignore SIGPIPE.
namespace exchange::net {

inline bool set_nonblocking(int fd) {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  return flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

inline void set_nodelay(int fd) {
  int enabled = 1;
  (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
}

// Probes an idle connection after 10 s, then every 5 s, and fails it after 3
// unanswered probes, so a peer that vanished without closing is noticed.
inline void set_keepalive(int fd) {
  int enabled = 1;
  int idle = 10;
  int interval = 5;
  int probes = 3;
  (void)::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &enabled, sizeof(enabled));
#ifdef TCP_KEEPALIVE
  (void)::setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &idle, sizeof(idle));
#else
  (void)::setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
#endif
  (void)::setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &interval,
                     sizeof(interval));
  (void)::setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &probes, sizeof(probes));
}

// Returns a non-blocking listening socket, or -1. Port 0 picks a free port.
inline int listen_tcp(const std::string &address, std::uint16_t port,
                      std::string &error) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  if (::inet_pton(AF_INET, address.c_str(), &addr.sin_addr) != 1) {
    error = "invalid IPv4 listen address: " + address;
    return -1;
  }

  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    error = std::string("socket: ") + std::strerror(errno);
    return -1;
  }
  int reuse = 1;
  (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  if (::bind(fd, reinterpret_cast<const sockaddr *>(&addr), sizeof(addr)) !=
          0 ||
      ::listen(fd, 64) != 0 || !set_nonblocking(fd)) {
    error = "cannot listen on " + address + ':' + std::to_string(port) +
            ": " + std::strerror(errno);
    ::close(fd);
    return -1;
  }
  return fd;
}

[[nodiscard]] inline std::uint16_t local_port(int fd) {
  sockaddr_in addr{};
  socklen_t length = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &length) != 0) {
    return 0;
  }
  return ntohs(addr.sin_port);
}

// Returns a connected (blocking) socket, or -1.
inline int connect_tcp(const std::string &host, std::uint16_t port,
                       std::string &error) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo *results = nullptr;
  const std::string service = std::to_string(port);
  if (const int status =
          ::getaddrinfo(host.c_str(), service.c_str(), &hints, &results);
      status != 0) {
    error = host + ": " + ::gai_strerror(status);
    return -1;
  }

  int fd = -1;
  for (const addrinfo *candidate = results; candidate != nullptr;
       candidate = candidate->ai_next) {
    fd = ::socket(candidate->ai_family, candidate->ai_socktype,
                  candidate->ai_protocol);
    if (fd < 0) {
      continue;
    }
    if (::connect(fd, candidate->ai_addr, candidate->ai_addrlen) == 0) {
      break;
    }
    ::close(fd);
    fd = -1;
  }
  ::freeaddrinfo(results);

  if (fd < 0) {
    error = "cannot connect to " + host + ':' + service + ": " +
            std::strerror(errno);
    return -1;
  }
  set_nodelay(fd);
  return fd;
}

// Appends what is readable without blocking, up to about 1 MB per call so one
// fast sender cannot hold the caller's loop. Returns false once the peer has
// closed the connection or on error.
inline bool read_available(int fd, std::vector<std::byte> &buffer) {
  constexpr std::size_t MAX_BYTES_PER_CALL = 1U << 20U;
  std::byte chunk[16 * 1024];
  for (std::size_t total = 0; total < MAX_BYTES_PER_CALL;) {
    const ssize_t got = ::recv(fd, chunk, sizeof(chunk), MSG_DONTWAIT);
    if (got > 0) {
      buffer.insert(buffer.end(), chunk, chunk + got);
      total += static_cast<std::size_t>(got);
      continue;
    }
    if (got == 0) {
      return false;
    }
    if (errno == EINTR) {
      continue;
    }
    return errno == EAGAIN || errno == EWOULDBLOCK;
  }
  return true; // more may be waiting; poll reports it again
}

// Writes as much of `buffer` after `done` as the socket accepts and advances
// `done`; the caller drops written bytes when it suits it, instead of moving
// the rest of a large buffer after every write. Returns false on error.
inline bool write_available(int fd, std::span<const std::byte> buffer,
                            std::size_t &done) {
  while (done < buffer.size()) {
#ifdef MSG_NOSIGNAL
    const int flags = MSG_DONTWAIT | MSG_NOSIGNAL;
#else
    const int flags = MSG_DONTWAIT;
#endif
    const ssize_t wrote =
        ::send(fd, buffer.data() + done, buffer.size() - done, flags);
    if (wrote > 0) {
      done += static_cast<std::size_t>(wrote);
      continue;
    }
    if (wrote < 0 && errno == EINTR) {
      continue;
    }
    if (wrote < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      break;
    }
    return false;
  }
  return true;
}

// Writes all of `buffer`, waiting for the socket as needed, then clears it.
inline bool write_all(int fd, std::vector<std::byte> &buffer) {
  std::size_t done = 0;
  while (done < buffer.size()) {
    if (!write_available(fd, buffer, done)) {
      return false;
    }
    if (done < buffer.size()) {
      ::usleep(200);
    }
  }
  buffer.clear();
  return true;
}

} // namespace exchange::net
