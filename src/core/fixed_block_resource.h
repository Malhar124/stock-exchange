#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <memory_resource>

namespace exchange::core {

// Bounded memory resource for node-based containers. It carves blocks out of
// one buffer allocated up front and recycles freed small blocks through
// per-size free lists, so long-running insert/erase churn reuses memory
// instead of exhausting the buffer. Blocks larger than MAX_RECYCLED_BYTES (such
// as hash-table bucket arrays, allocated once by reserve()) are not recycled.
class FixedBlockResource final : public std::pmr::memory_resource {
public:
  explicit FixedBlockResource(std::size_t bytes)
      : storage_(new std::byte[bytes]), capacity_(bytes) {}

  FixedBlockResource(const FixedBlockResource &) = delete;
  FixedBlockResource &operator=(const FixedBlockResource &) = delete;
  FixedBlockResource(FixedBlockResource &&) = delete;
  FixedBlockResource &operator=(FixedBlockResource &&) = delete;

private:
  static constexpr std::size_t GRANULE = 16;
  static constexpr std::size_t MAX_RECYCLED_BYTES = 256;

  struct FreeBlock {
    FreeBlock *next;
  };

  [[nodiscard]] static std::size_t block_size(std::size_t bytes) noexcept {
    const std::size_t size = bytes < GRANULE ? GRANULE : bytes;
    return (size + GRANULE - 1U) & ~(GRANULE - 1U);
  }

  [[nodiscard]] static bool recyclable(std::size_t size,
                                       std::size_t alignment) noexcept {
    return size <= MAX_RECYCLED_BYTES && alignment <= GRANULE;
  }

  void *do_allocate(std::size_t bytes, std::size_t alignment) override {
    const std::size_t size = block_size(bytes);
    if (recyclable(size, alignment)) {
      FreeBlock *&head = free_lists_[(size / GRANULE) - 1U];
      if (head != nullptr) {
        FreeBlock *block = head;
        head = block->next;
        return block;
      }
    }

    void *cursor = storage_.get() + used_;
    std::size_t space = capacity_ - used_;
    if (std::align(alignment < GRANULE ? GRANULE : alignment, size, cursor,
                   space) == nullptr) [[unlikely]] {
      // Capacity checks in the order book keep live entries within budget,
      // so reaching this is a sizing bug, not a load condition.
      std::fputs("FixedBlockResource: buffer exhausted\n", stderr);
      std::abort();
    }
    used_ = static_cast<std::size_t>(static_cast<std::byte *>(cursor) -
                                     storage_.get()) +
            size;
    return cursor;
  }

  void do_deallocate(void *pointer, std::size_t bytes,
                     std::size_t alignment) override {
    const std::size_t size = block_size(bytes);
    if (!recyclable(size, alignment)) {
      return;
    }
    auto *block = static_cast<FreeBlock *>(pointer);
    FreeBlock *&head = free_lists_[(size / GRANULE) - 1U];
    block->next = head;
    head = block;
  }

  [[nodiscard]] bool
  do_is_equal(const std::pmr::memory_resource &other) const noexcept override {
    return this == &other;
  }

  std::unique_ptr<std::byte[]> storage_;
  std::size_t capacity_{0};
  std::size_t used_{0};
  std::array<FreeBlock *, MAX_RECYCLED_BYTES / GRANULE> free_lists_{};
};

} // namespace exchange::core
