#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "core/clock.h"
#include "core/types.h"
#include "matching/matching_engine.h"
#include "runtime/journal.h"
#include "runtime/metrics.h"

namespace exchange::runtime {

// A request from the gateway, before it is sequenced.
struct GatewayRequest {
  std::uint64_t client_order_id{0};
  core::Timestamp received_ns{0}; // steady clock, for latency metrics
  matching::InboundOrder order{};
};

// The sequenced request an engine output belongs to.
struct RequestContext {
  core::SequenceNumber sequence{0};
  std::uint64_t client_order_id{0};
  core::Timestamp received_ns{0};
  matching::InboundOrder order{};
};

// The engine thread emits BEGIN, the request's events, then END, so the
// gateway knows which request produced each event and when a batch is
// complete.
struct EngineOutput {
  enum class Kind : std::uint8_t { BEGIN, EVENT, END };
  Kind kind{Kind::EVENT};
  RequestContext request{};
  matching::Event event{};
};

static_assert(std::is_trivially_copyable_v<GatewayRequest>);
static_assert(std::is_trivially_copyable_v<EngineOutput>);

[[nodiscard]] inline core::Timestamp wall_clock_ns() noexcept {
  return static_cast<core::Timestamp>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

// Sequencer, journal, and matching engine. Owned by the engine thread.
template <typename Engine> class EngineStage {
public:
  EngineStage(std::span<const core::Symbol> symbols, JournalWriter *journal,
              EngineMetrics *metrics)
      : engine_(std::make_unique<Engine>()), journal_(journal),
        metrics_(metrics) {
    engine_->prepare_symbols(symbols);
  }

  // Recovery: re-runs a journaled request exactly as it ran live.
  template <typename Sink>
  void replay(const JournalRecord &record, Sink &&sink) {
    next_sequence_ = record.sequence + 1U;
    last_timestamp_ = std::max(last_timestamp_, record.order.timestamp);
    run(RequestContext{.sequence = record.sequence,
                       .client_order_id = record.client_order_id,
                       .received_ns = 0,
                       .order = record.order},
        sink);
  }

  // Sequences and journals the whole batch (one write, and one fsync under
  // JournalSync::BATCH) before matching any of it, so nothing is reported
  // before it is recoverable. Returns false if the journal write failed.
  template <typename Sink>
  [[nodiscard]] bool process_batch(std::span<const GatewayRequest> requests,
                                   Sink &&sink) {
    batch_.clear();
    for (const GatewayRequest &request : requests) {
      RequestContext context{.sequence = next_sequence_++,
                             .client_order_id = request.client_order_id,
                             .received_ns = request.received_ns,
                             .order = request.order};
      last_timestamp_ = std::max(wall_clock_ns(), last_timestamp_ + 1U);
      context.order.timestamp = last_timestamp_;
      if (journal_ != nullptr) {
        journal_->append(make_record(context.sequence,
                                     context.client_order_id, context.order));
      }
      batch_.push_back(context);
    }

    if (journal_ != nullptr && !journal_->commit()) {
      return false;
    }

    for (const RequestContext &context : batch_) {
      const core::Timestamp started = core::now_ns();
      run(context, sink);
      if (metrics_ != nullptr) {
        metrics_->engine_latency.record(core::now_ns() - started);
        metrics_->requests.fetch_add(1, std::memory_order_relaxed);
      }
    }
    return true;
  }

  [[nodiscard]] core::SequenceNumber next_sequence() const noexcept {
    return next_sequence_;
  }

  [[nodiscard]] const Engine &engine() const noexcept { return *engine_; }

private:
  template <typename Sink> void run(const RequestContext &context, Sink &sink) {
    sink(EngineOutput{.kind = EngineOutput::Kind::BEGIN, .request = context});
    std::uint64_t events = 0;
    std::uint64_t trades = 0;
    // The engine hands over full chunks while it runs, then returns the rest,
    // so one request never holds more than the reserved events.
    const typename Engine::EventOverflow deliver =
        [&](std::span<const matching::Event> chunk) {
          for (const matching::Event &event : chunk) {
            trades += event.type == matching::EventType::TRADE ? 1U : 0U;
            sink(EngineOutput{.kind = EngineOutput::Kind::EVENT,
                              .event = event});
          }
          events += chunk.size();
        };
    deliver(engine_->process_order(context.order, &deliver));
    sink(EngineOutput{.kind = EngineOutput::Kind::END, .request = context});
    if (metrics_ != nullptr) {
      metrics_->events.fetch_add(events, std::memory_order_relaxed);
      metrics_->trades.fetch_add(trades, std::memory_order_relaxed);
    }
  }

  std::unique_ptr<Engine> engine_;
  JournalWriter *journal_{nullptr};
  EngineMetrics *metrics_{nullptr};
  core::SequenceNumber next_sequence_{1};
  core::Timestamp last_timestamp_{0};
  std::vector<RequestContext> batch_;
};

} // namespace exchange::runtime
