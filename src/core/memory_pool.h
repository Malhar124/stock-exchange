#pragma once

#include <array>
#include <cstddef>
#include <cstring>
#include <memory>

namespace exchange::core {

template <typename T, std::size_t N>

class MemoryPool {
  static_assert(N > 0, "MemoryPool capacity must be positive.");
  static_assert(sizeof(T) >= sizeof(void *),
                "Object storage must fit an intrusive free-list pointer.");

  struct alignas((alignof(T) > 64U) ? alignof(T) : 64U) Slot {
    std::array<std::byte, sizeof(T)> storage{};
  };

  struct FreeNode {
    FreeNode *next;
  };

public:
  MemoryPool() : slots_(std::make_unique<Slot[]>(N)) { initialize_free_list(); }

  MemoryPool(const MemoryPool &) = delete;
  MemoryPool &operator=(const MemoryPool &) = delete;
  MemoryPool(MemoryPool &&) = delete;
  MemoryPool &operator=(MemoryPool &&) = delete;

  T *allocate() noexcept {
    if (free_list_head_ == nullptr) [[unlikely]] {
      return nullptr;
    }

    FreeNode *node = free_list_head_;
    free_list_head_ = free_list_head_->next;
    return reinterpret_cast<T *>(node);
  }

  void deallocate(T *object) noexcept {
    if (object == nullptr) [[unlikely]] {
      return;
    }

    std::destroy_at(object);
#ifdef EXCHANGE_POISON_FREED
    // Debug builds: make any read of a freed object return garbage.
    std::memset(static_cast<void *>(object), 0xDD, sizeof(T));
#endif
    auto *node = reinterpret_cast<FreeNode *>(object);
    node->next = free_list_head_;
    free_list_head_ = node;
  }

private:
  void initialize_free_list() noexcept {
    free_list_head_ = nullptr;
    for (std::size_t index = 0; index < N; ++index) {
      auto *node = reinterpret_cast<FreeNode *>(slots_[index].storage.data());
      node->next = free_list_head_;
      free_list_head_ = node;
    }
  }

  std::unique_ptr<Slot[]> slots_;

  FreeNode *free_list_head_{nullptr};
};

} // namespace exchange::core
