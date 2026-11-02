#pragma once

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "core/types.h"
#include "matching/matching_engine.h"
#include "runtime/capacity_profiles.h"

// Append-only journal of sequenced requests. The engine is deterministic, so
// replaying the journal through a fresh engine rebuilds the exact book state.
namespace exchange::runtime {

inline constexpr std::array<char, 8> JOURNAL_MAGIC = {'E', 'X', 'J', 'R',
                                                      'N', 'L', '0', '1'};
inline constexpr std::uint32_t JOURNAL_FORMAT_VERSION = 1;

struct JournalHeader {
  std::array<char, 8> magic{};
  std::uint32_t format_version{0};
  std::uint32_t engine_behavior_version{0};
  CapacityProfile capacity_profile{CapacityProfile::SMALL};
  std::array<std::uint8_t, 15> reserved{};
};

struct JournalRecord {
  core::SequenceNumber sequence{0};
  std::uint64_t client_order_id{0};
  matching::InboundOrder order{}; // timestamp and participant filled in
  std::uint32_t checksum{0};      // FNV-1a over the bytes before it
  std::uint32_t reserved{0};
};

static_assert(std::is_trivially_copyable_v<JournalHeader>);
static_assert(std::is_trivially_copyable_v<JournalRecord>);

[[nodiscard]] inline std::uint32_t record_checksum(const JournalRecord &record) {
  const auto *bytes = reinterpret_cast<const unsigned char *>(&record);
  std::uint32_t hash = 2'166'136'261U;
  for (std::size_t index = 0; index < offsetof(JournalRecord, checksum);
       ++index) {
    hash = (hash ^ bytes[index]) * 16'777'619U;
  }
  return hash;
}

// Builds a record with zeroed padding so its checksum is reproducible.
[[nodiscard]] inline JournalRecord
make_record(core::SequenceNumber sequence, std::uint64_t client_order_id,
            const matching::InboundOrder &order) {
  JournalRecord record;
  std::memset(static_cast<void *>(&record), 0, sizeof(record));
  record.sequence = sequence;
  record.client_order_id = client_order_id;
  std::memcpy(static_cast<void *>(&record.order), &order, sizeof(order));
  record.checksum = record_checksum(record);
  return record;
}

[[nodiscard]] inline JournalHeader make_header(CapacityProfile profile) {
  JournalHeader header;
  std::memset(static_cast<void *>(&header), 0, sizeof(header));
  header.magic = JOURNAL_MAGIC;
  header.format_version = JOURNAL_FORMAT_VERSION;
  header.engine_behavior_version = matching::ENGINE_BEHAVIOR_VERSION;
  header.capacity_profile = profile;
  return header;
}

enum class JournalSync : std::uint8_t {
  NONE,  // write() per batch: survives a process crash, not power loss
  BATCH, // fsync per batch before matching: acknowledged work is durable
};

struct JournalReadResult {
  JournalHeader header{};
  std::uint64_t records{0};
  core::SequenceNumber last_sequence{0};
  std::uint64_t valid_bytes{0}; // header plus intact records
  bool damaged_tail{false};     // a torn or corrupt final record was dropped
};

// Reads every intact record and calls on_record for each. A damaged final
// record is treated as a torn write and dropped; damage anywhere earlier, a
// sequence gap, or an incompatible header is an error.
template <typename OnRecord>
[[nodiscard]] bool read_journal(const std::string &path,
                                JournalReadResult &result, std::string &error,
                                OnRecord &&on_record) {
  result = JournalReadResult{};
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    error = path + ": " + std::strerror(errno);
    return false;
  }

  struct stat info {};
  if (::fstat(fd, &info) != 0) {
    error = path + ": " + std::strerror(errno);
    ::close(fd);
    return false;
  }
  const auto file_size = static_cast<std::uint64_t>(info.st_size);

  const auto read_exact = [&](void *out, std::size_t bytes) {
    std::size_t done = 0;
    while (done < bytes) {
      const ssize_t got =
          ::read(fd, static_cast<char *>(out) + done, bytes - done);
      if (got < 0 && errno == EINTR) {
        continue;
      }
      if (got <= 0) {
        return false;
      }
      done += static_cast<std::size_t>(got);
    }
    return true;
  };

  if (file_size < sizeof(JournalHeader) ||
      !read_exact(&result.header, sizeof(JournalHeader))) {
    error = path + ": too short to hold a journal header";
    ::close(fd);
    return false;
  }
  if (result.header.magic != JOURNAL_MAGIC ||
      result.header.format_version != JOURNAL_FORMAT_VERSION) {
    error = path + ": not a journal, or an unsupported journal format";
    ::close(fd);
    return false;
  }
  if (result.header.engine_behavior_version !=
      matching::ENGINE_BEHAVIOR_VERSION) {
    error = path + ": written by engine behavior version " +
            std::to_string(result.header.engine_behavior_version) +
            ", this build is version " +
            std::to_string(matching::ENGINE_BEHAVIOR_VERSION) +
            "; replaying it could produce different results";
    ::close(fd);
    return false;
  }

  result.valid_bytes = sizeof(JournalHeader);
  const std::uint64_t record_bytes = file_size - sizeof(JournalHeader);
  const std::uint64_t whole_records = record_bytes / sizeof(JournalRecord);
  const bool partial_tail = (record_bytes % sizeof(JournalRecord)) != 0U;

  JournalRecord record;
  for (std::uint64_t index = 0; index < whole_records; ++index) {
    if (!read_exact(&record, sizeof(record))) {
      error = path + ": read failed";
      ::close(fd);
      return false;
    }
    const bool intact = record.checksum == record_checksum(record);
    const bool in_sequence = record.sequence == result.last_sequence + 1U;
    if (!intact || !in_sequence) {
      const bool is_last = index + 1U == whole_records && !partial_tail;
      if (!intact && is_last) {
        result.damaged_tail = true;
        break;
      }
      error = path + ": record " + std::to_string(index + 1U) +
              (intact ? " is out of sequence" : " is corrupt") +
              " and is not the final record";
      ::close(fd);
      return false;
    }
    on_record(record);
    ++result.records;
    result.last_sequence = record.sequence;
    result.valid_bytes += sizeof(JournalRecord);
  }
  if (partial_tail) {
    result.damaged_tail = true;
  }

  ::close(fd);
  return true;
}

class JournalWriter {
public:
  JournalWriter() = default;
  JournalWriter(const JournalWriter &) = delete;
  JournalWriter &operator=(const JournalWriter &) = delete;
  ~JournalWriter() { close(); }

  // Creates the journal, or opens an existing one that read_journal() has
  // validated, truncating any damaged tail it reported.
  [[nodiscard]] bool open(const std::string &path, CapacityProfile profile,
                          JournalSync sync, const JournalReadResult *existing,
                          std::string &error) {
    sync_ = sync;
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd_ < 0) {
      error = path + ": " + std::strerror(errno);
      return false;
    }
    // A second writer would overwrite records at the same offsets.
    if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
      error = path + (errno == EWOULDBLOCK
                          ? std::string(": in use by another process")
                          : ": " + std::string(std::strerror(errno)));
      close();
      return false;
    }

    if (existing != nullptr) {
      if (existing->header.capacity_profile != profile) {
        error = path + ": recorded with capacity profile '" +
                std::string(profile_name(existing->header.capacity_profile)) +
                "', but the server is configured for '" +
                std::string(profile_name(profile)) + "'";
        close();
        return false;
      }
      if (::ftruncate(fd_, static_cast<off_t>(existing->valid_bytes)) != 0 ||
          ::lseek(fd_, 0, SEEK_END) < 0) {
        error = path + ": " + std::strerror(errno);
        close();
        return false;
      }
      return true;
    }

    if (::ftruncate(fd_, 0) != 0) {
      error = path + ": " + std::strerror(errno);
      close();
      return false;
    }
    const JournalHeader header = make_header(profile);
    buffer_.resize(sizeof(header));
    std::memcpy(buffer_.data(), &header, sizeof(header));
    if (!commit()) {
      error = path + ": cannot write header: " + std::strerror(errno);
      close();
      return false;
    }
    // A new file survives a power loss only once its directory entry does.
    if (!sync_full() || !sync_directory(path)) {
      error = path + ": cannot sync: " + std::strerror(errno);
      close();
      return false;
    }
    return true;
  }

  void append(const JournalRecord &record) {
    const std::size_t offset = buffer_.size();
    buffer_.resize(offset + sizeof(record));
    std::memcpy(buffer_.data() + offset, &record, sizeof(record));
  }

  // Writes everything appended so far, and syncs it under BATCH. Returns
  // false on I/O failure, after which the caller must stop processing.
  [[nodiscard]] bool commit() {
    std::size_t done = 0;
    while (done < buffer_.size()) {
      const ssize_t wrote =
          ::write(fd_, buffer_.data() + done, buffer_.size() - done);
      if (wrote < 0 && errno == EINTR) {
        continue;
      }
      if (wrote <= 0) {
        return false;
      }
      done += static_cast<std::size_t>(wrote);
    }
    buffer_.clear();
    return sync_ == JournalSync::BATCH ? sync_full() : true;
  }

  void close() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

private:
  [[nodiscard]] bool sync_full() {
#ifdef F_FULLFSYNC
    // On macOS, fsync() does not flush the drive's cache.
    if (::fcntl(fd_, F_FULLFSYNC) == 0) {
      return true;
    }
#endif
    return ::fsync(fd_) == 0;
  }

  [[nodiscard]] static bool sync_directory(const std::string &path) {
    const std::string directory =
        std::filesystem::path(path).parent_path().string();
    const int fd = ::open(directory.empty() ? "." : directory.c_str(), O_RDONLY);
    if (fd < 0) {
      return false;
    }
    const bool synced = ::fsync(fd) == 0;
    ::close(fd);
    return synced;
  }

  int fd_{-1};
  JournalSync sync_{JournalSync::BATCH};
  std::vector<char> buffer_;
};

} // namespace exchange::runtime
