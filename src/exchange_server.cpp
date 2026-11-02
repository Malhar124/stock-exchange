// The exchange process: TCP gateway on the main thread, sequencer + journal +
// matching engine on a dedicated engine thread, connected by SPSC queues.
//
//   exchange_server [--config FILE] [--set key=value]...
//
// Settings are documented in config/exchange.conf.

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "core/spsc_queue.h"
#include "gateway/gateway_core.h"
#include "gateway/gateway_server.h"
#include "net/socket.h"
#include "runtime/capacity_profiles.h"
#include "runtime/engine_stage.h"
#include "runtime/journal.h"
#include "runtime/metrics.h"
#include "server/config.h"

namespace {

using namespace exchange;

volatile std::sig_atomic_t g_stop_requested = 0;

void request_stop(int) { g_stop_requested = 1; }

constexpr std::size_t QUEUE_CAPACITY = 65'536;
constexpr std::size_t MAX_BATCH = 256;

using RequestQueue = core::SPSCQueue<runtime::GatewayRequest, QUEUE_CAPACITY>;
using OutputQueue = core::SPSCQueue<runtime::EngineOutput, QUEUE_CAPACITY>;

// Spins briefly when idle, then sleeps, so an idle exchange does not burn a
// core while a busy one still sees microsecond wake-ups.
class IdleBackoff {
public:
  void wait() {
    if (++idle_polls_ < 2'000U) {
      std::this_thread::yield();
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  }
  void reset() noexcept { idle_polls_ = 0; }

private:
  std::uint32_t idle_polls_{0};
};

void wake(int fd) {
  const char byte = 1;
  (void)!::write(fd, &byte, 1); // a full pipe already means "wake up"
}

void print_symbols(const std::vector<core::Symbol> &symbols) {
  for (std::size_t index = 0; index < symbols.size(); ++index) {
    std::cout << (index == 0 ? "" : ",") << core::symbol_view(symbols[index]);
  }
}

class MetricsReporter {
public:
  MetricsReporter(const runtime::EngineMetrics &engine,
                  const runtime::GatewayMetrics &gateway)
      : engine_(engine), gateway_(gateway),
        engine_previous_(engine.engine_latency.snapshot()),
        round_trip_previous_(gateway.round_trip.snapshot()) {}

  // Reports the interval since the last report, or everything since startup
  // when `cumulative` is set.
  void report(const char *label, std::size_t sessions,
              std::size_t open_orders, bool cumulative) {
    const auto engine_now = engine_.engine_latency.snapshot();
    const auto round_trip_now = gateway_.round_trip.snapshot();
    const auto engine_delta =
        cumulative ? engine_now : engine_now.since(engine_previous_);
    const auto round_trip_delta =
        cumulative ? round_trip_now : round_trip_now.since(round_trip_previous_);
    const std::uint64_t requests =
        engine_.requests.load(std::memory_order_relaxed);
    const std::uint64_t trades = engine_.trades.load(std::memory_order_relaxed);
    if (!cumulative && engine_delta.count == 0U) {
      return;
    }
    std::cout << '[' << label << "] requests=" << requests;
    if (!cumulative) {
      std::cout << " (+" << requests - requests_previous_ << ')';
    }
    std::cout << " trades=" << trades;
    if (!cumulative) {
      std::cout << " (+" << trades - trades_previous_ << ')';
    }
    std::cout
              << " | engine p50="
              << runtime::format_ns(engine_delta.percentile(50))
              << " p99=" << runtime::format_ns(engine_delta.percentile(99))
              << " max=" << runtime::format_ns(engine_now.max)
              << " | round-trip p50="
              << runtime::format_ns(round_trip_delta.percentile(50))
              << " p99=" << runtime::format_ns(round_trip_delta.percentile(99))
              << " | sessions=" << sessions << " open_orders=" << open_orders
              << " gateway_rejects=" << gateway_.gateway_rejects
              << " max_queue_depth=" << gateway_.max_inbound_depth << '/'
              << gateway_.max_output_depth << std::endl;
    engine_previous_ = engine_now;
    round_trip_previous_ = round_trip_now;
    requests_previous_ = requests;
    trades_previous_ = trades;
  }

private:
  const runtime::EngineMetrics &engine_;
  const runtime::GatewayMetrics &gateway_;
  runtime::LatencyHistogram::Snapshot engine_previous_;
  runtime::LatencyHistogram::Snapshot round_trip_previous_;
  std::uint64_t requests_previous_{0};
  std::uint64_t trades_previous_{0};
};

template <typename Engine>
int run_server(const server::ServerConfig &config, gateway::Secrets secrets) {
  runtime::EngineMetrics engine_metrics;
  runtime::GatewayMetrics gateway_metrics;
  auto requests = std::make_unique<RequestQueue>();
  auto outputs = std::make_unique<OutputQueue>();

  gateway::GatewayCore core(
      config.symbols, config.limits,
      [&](const runtime::GatewayRequest &request) {
        return requests->push(request);
      },
      &gateway_metrics);
  const bool authenticating = !secrets.empty();
  core.set_secrets(std::move(secrets));

  runtime::JournalWriter journal;
  const bool journaling = !config.journal_path.empty();
  runtime::EngineStage<Engine> stage(config.symbols,
                                     journaling ? &journal : nullptr,
                                     &engine_metrics);

  // Recovery: replay the journal through the engine and the gateway's order
  // registry and market data, exactly as the requests ran live.
  std::string error;
  if (journaling) {
    std::error_code ignored;
    const std::filesystem::path journal_path(config.journal_path);
    if (journal_path.has_parent_path()) {
      std::filesystem::create_directories(journal_path.parent_path(), ignored);
    }
    struct stat info {};
    const bool exists = ::stat(config.journal_path.c_str(), &info) == 0;
    runtime::JournalReadResult recovered;
    if (exists) {
      const auto started = std::chrono::steady_clock::now();
      const bool ok = runtime::read_journal(
          config.journal_path, recovered, error,
          [&](const runtime::JournalRecord &record) {
            core.restore_submission(record);
            stage.replay(record, [&](const runtime::EngineOutput &output) {
              core.on_engine_output(output);
            });
          });
      if (!ok) {
        std::cerr << "recovery failed: " << error << '\n';
        return 1;
      }
      const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - started);
      std::cout << "recovered " << recovered.records << " requests from "
                << config.journal_path << " in " << elapsed.count()
                << " ms; " << core.open_orders() << " orders open\n";
      if (recovered.damaged_tail) {
        std::cout << "warning: dropped a torn record at the end of the "
                     "journal (crash during a write)\n";
      }
    }
    if (!journal.open(config.journal_path, config.capacity_profile,
                      config.journal_sync, exists ? &recovered : nullptr,
                      error)) {
      std::cerr << error << '\n';
      return 1;
    }
  }
  core.finish_recovery();

  int wake_pipe[2];
  if (::pipe(wake_pipe) != 0) {
    std::perror("pipe");
    return 1;
  }
  net::set_nonblocking(wake_pipe[0]);
  net::set_nonblocking(wake_pipe[1]);

  const int listen_fd =
      net::listen_tcp(config.listen_address, config.port, error);
  if (listen_fd < 0) {
    std::cerr << error << '\n';
    return 1;
  }

  std::cout << "exchange_server listening on " << config.listen_address << ':'
            << net::local_port(listen_fd) << " | symbols=";
  print_symbols(config.symbols);
  std::cout << " | profile="
            << runtime::profile_name(config.capacity_profile) << " | journal="
            << (journaling ? config.journal_path : std::string("off"));
  if (journaling) {
    std::cout << " (fsync="
              << (config.journal_sync == runtime::JournalSync::BATCH ? "batch"
                                                                     : "none")
              << ')';
  }
  std::cout << " | auth=" << (authenticating ? "on" : "off") << std::endl;

  std::atomic<bool> engine_stop{false};
  std::atomic<bool> engine_done{false};
  std::atomic<bool> engine_failed{false};

  std::thread engine_thread([&] {
    std::vector<runtime::GatewayRequest> batch;
    batch.reserve(MAX_BATCH);
    IdleBackoff idle;
    while (true) {
      batch.clear();
      runtime::GatewayRequest request;
      while (batch.size() < MAX_BATCH && requests->pop(request)) {
        batch.push_back(request);
      }
      if (batch.empty()) {
        // The gateway stops submitting before it sets engine_stop, so an
        // empty queue observed after the flag means nothing is left.
        if (engine_stop.load(std::memory_order_acquire) && requests->empty()) {
          break;
        }
        idle.wait();
        continue;
      }
      idle.reset();
      const bool ok = stage.process_batch(
          batch, [&](const runtime::EngineOutput &output) {
            while (!outputs->push(output)) {
              wake(wake_pipe[1]);
              std::this_thread::yield();
            }
          });
      wake(wake_pipe[1]);
      if (!ok) {
        std::cerr << "journal write failed; stopping the engine\n";
        engine_failed.store(true, std::memory_order_release);
        break;
      }
    }
    engine_done.store(true, std::memory_order_release);
    wake(wake_pipe[1]);
  });

  gateway::GatewayServer gateway_server(
      core, listen_fd, wake_pipe[0],
      [&](runtime::EngineOutput &output) { return outputs->pop(output); },
      &gateway_metrics);

  MetricsReporter reporter(engine_metrics, gateway_metrics);
  auto next_report = std::chrono::steady_clock::now() +
                     std::chrono::seconds(config.metrics_interval_seconds);

  while (g_stop_requested == 0 &&
         !engine_failed.load(std::memory_order_acquire)) {
    gateway_server.poll_once(100);
    gateway_metrics.max_inbound_depth =
        std::max(gateway_metrics.max_inbound_depth, requests->size_approx());
    gateway_metrics.max_output_depth =
        std::max(gateway_metrics.max_output_depth, outputs->size_approx());
    if (config.metrics_interval_seconds > 0U &&
        std::chrono::steady_clock::now() >= next_report) {
      reporter.report("metrics", core.session_count(), core.open_orders(),
                      false);
      next_report += std::chrono::seconds(config.metrics_interval_seconds);
    }
  }

  // Clean shutdown: refuse new work, let the engine finish what is queued,
  // deliver its results, then close everything.
  std::cout << "shutting down" << std::endl;
  core.set_accepting(false);
  gateway_server.close_listener();
  engine_stop.store(true, std::memory_order_release);
  while (!engine_done.load(std::memory_order_acquire)) {
    gateway_server.poll_once(10);
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  do {
    gateway_server.poll_once(10);
  } while (gateway_server.has_pending_writes() &&
           std::chrono::steady_clock::now() < deadline);
  engine_thread.join();
  gateway_server.close_all();
  journal.close();
  ::close(wake_pipe[0]);
  ::close(wake_pipe[1]);

  reporter.report("final", 0, core.open_orders(), true);
  return engine_failed.load() ? 1 : 0;
}

void usage() {
  std::cerr << "Usage: exchange_server [--config FILE] [--set key=value]...\n"
               "See config/exchange.conf for the settings.\n";
}

} // namespace

int main(int argc, char **argv) {
  server::ServerConfig config;
  std::vector<std::string> overrides;
  std::string error;

  for (int index = 1; index < argc; ++index) {
    const std::string_view arg = argv[index];
    if (arg == "--config" && index + 1 < argc) {
      if (!server::load_config(argv[++index], config, error)) {
        std::cerr << error << '\n';
        return 2;
      }
    } else if (arg == "--set" && index + 1 < argc) {
      overrides.emplace_back(argv[++index]);
    } else if (arg == "-h" || arg == "--help") {
      usage();
      return 0;
    } else {
      usage();
      return 2;
    }
  }
  for (const std::string &setting : overrides) {
    const std::size_t equals = setting.find('=');
    if (equals == std::string::npos ||
        !server::apply_setting(config, setting.substr(0, equals),
                               setting.substr(equals + 1), error)) {
      std::cerr << (error.empty() ? "expected key=value: " + setting : error)
                << '\n';
      return 2;
    }
  }

  // Without secrets anyone who reaches the port can act as any participant.
  gateway::Secrets secrets;
  if (!config.auth_file.empty()) {
    if (!server::load_secrets(config.auth_file, secrets, error)) {
      std::cerr << error << '\n';
      return 2;
    }
  } else if (!config.listen_address.starts_with("127.")) {
    std::cerr << "listen_address " << config.listen_address
              << " is not a loopback address; set auth_file so that logins "
                 "need a secret\n";
    return 2;
  }

  std::signal(SIGPIPE, SIG_IGN);
  struct sigaction action {};
  action.sa_handler = request_stop;
  sigemptyset(&action.sa_mask);
  sigaction(SIGINT, &action, nullptr);
  sigaction(SIGTERM, &action, nullptr);

  return runtime::with_engine_type(config.capacity_profile, [&](auto engine) {
    return run_server<typename decltype(engine)::type>(config,
                                                       std::move(secrets));
  });
}
