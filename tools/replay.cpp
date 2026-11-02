// Replays order scenarios through the matching engine.
//
//   replay <scenario>                print the transcript
//   replay --check <scenario>...     verify each command's expected events
//   replay --update <scenario>...    rewrite expected events in place
//   replay --journal <file>          print an exchange_server journal as a
//                                    transcript (itself a valid scenario)
//
// A scenario is a text file of commands (see text/order_text.h). The events a
// command is expected to produce are listed below it, indented. Comment lines
// start with '#'. Timestamps default to 1, 2, 3, ... unless a command sets ts=.

#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "runtime/capacity_profiles.h"
#include "runtime/journal.h"
#include "text/order_text.h"

namespace {

using namespace exchange;

struct Entry {
  enum class Kind { VERBATIM, COMMAND } kind{Kind::VERBATIM};
  std::size_t line_number{0};
  std::string text;
  text::ParsedCommand command{};
  std::vector<std::string> expected;
  std::vector<std::string> actual;
};

[[nodiscard]] std::string_view trim(std::string_view text) {
  while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
    text.remove_prefix(1);
  }
  while (!text.empty() && (text.back() == ' ' || text.back() == '\t' ||
                           text.back() == '\r')) {
    text.remove_suffix(1);
  }
  return text;
}

[[nodiscard]] bool load_scenario(const std::string &path,
                                 std::vector<Entry> &entries) {
  std::ifstream input(path);
  if (!input) {
    std::cerr << path << ": cannot open\n";
    return false;
  }

  std::string line;
  std::size_t line_number = 0;
  while (std::getline(input, line)) {
    ++line_number;
    const std::string_view trimmed = trim(line);
    const bool indented =
        !line.empty() && (line.front() == ' ' || line.front() == '\t');

    if (trimmed.empty() || trimmed.front() == '#') {
      entries.push_back(Entry{.kind = Entry::Kind::VERBATIM,
                              .line_number = line_number,
                              .text = std::string(trim(line))});
      continue;
    }

    if (indented) {
      if (entries.empty() || entries.back().kind != Entry::Kind::COMMAND) {
        std::cerr << path << ':' << line_number
                  << ": expected event without a command above it\n";
        return false;
      }
      entries.back().expected.emplace_back(trimmed);
      continue;
    }

    Entry entry{.kind = Entry::Kind::COMMAND,
                .line_number = line_number,
                .text = std::string(trimmed)};
    std::string error;
    if (!text::parse_command(trimmed, entry.command, error)) {
      std::cerr << path << ':' << line_number << ": " << error << '\n';
      return false;
    }
    entries.push_back(std::move(entry));
  }
  return true;
}

template <typename Engine> void run_entries(std::vector<Entry> &entries) {
  auto engine = std::make_unique<Engine>();
  core::Timestamp clock = 0;
  for (Entry &entry : entries) {
    if (entry.kind != Entry::Kind::COMMAND) {
      continue;
    }
    matching::InboundOrder order = entry.command.order;
    order.timestamp = entry.command.has_timestamp ? order.timestamp : ++clock;
    if (entry.command.has_timestamp) {
      clock = order.timestamp;
    }
    for (const matching::Event &event : engine->process_order(order)) {
      entry.actual.push_back(text::format_event(event));
    }
  }
}

[[nodiscard]] std::string transcript(const std::vector<Entry> &entries) {
  std::ostringstream out;
  for (const Entry &entry : entries) {
    out << entry.text << '\n';
    for (const std::string &event : entry.actual) {
      out << "  " << event << '\n';
    }
  }
  return out.str();
}

[[nodiscard]] bool check(const std::string &path,
                         const std::vector<Entry> &entries) {
  bool ok = true;
  for (const Entry &entry : entries) {
    if (entry.kind != Entry::Kind::COMMAND || entry.expected == entry.actual) {
      continue;
    }
    ok = false;
    std::cerr << path << ':' << entry.line_number << ": " << entry.text
              << "\n  expected:\n";
    for (const std::string &event : entry.expected) {
      std::cerr << "    " << event << '\n';
    }
    std::cerr << "  actual:\n";
    for (const std::string &event : entry.actual) {
      std::cerr << "    " << event << '\n';
    }
  }
  return ok;
}

int dump_journal(const std::string &path) {
  runtime::JournalReadResult result;
  std::string error;
  std::vector<runtime::JournalRecord> records;
  if (!runtime::read_journal(path, result, error,
                             [&](const runtime::JournalRecord &record) {
                               records.push_back(record);
                             })) {
    std::cerr << error << '\n';
    return 1;
  }
  std::cout << "# " << path << ": " << records.size()
            << " requests, capacity profile "
            << runtime::profile_name(result.header.capacity_profile)
            << (result.damaged_tail ? ", torn final record ignored" : "")
            << '\n';
  runtime::with_engine_type(
      result.header.capacity_profile, [&](auto engine_type) {
        auto engine =
            std::make_unique<typename decltype(engine_type)::type>();
        for (const runtime::JournalRecord &record : records) {
          std::cout << text::format_command(record.order, true) << '\n';
          for (const matching::Event &event :
               engine->process_order(record.order)) {
            std::cout << "  " << text::format_event(event) << '\n';
          }
        }
      });
  return 0;
}

void usage() {
  std::cerr
      << "Usage: replay [--profile small|medium|large] <scenario>\n"
         "       replay [--profile ...] --check <scenario>...\n"
         "       replay [--profile ...] --update <scenario>...\n"
         "       replay --journal <journal-file>\n";
}

} // namespace

int main(int argc, char **argv) {
  enum class Mode { PRINT, CHECK, UPDATE } mode = Mode::PRINT;
  runtime::CapacityProfile profile = runtime::CapacityProfile::SMALL;
  std::vector<std::string> paths;

  for (int index = 1; index < argc; ++index) {
    const std::string_view arg = argv[index];
    if (arg == "--check") {
      mode = Mode::CHECK;
    } else if (arg == "--update") {
      mode = Mode::UPDATE;
    } else if (arg == "--journal" && index + 1 < argc) {
      return dump_journal(argv[++index]);
    } else if (arg == "--profile" && index + 1 < argc) {
      const auto parsed = runtime::parse_profile(argv[++index]);
      if (!parsed) {
        usage();
        return 2;
      }
      profile = *parsed;
    } else if (arg == "-h" || arg == "--help") {
      usage();
      return 0;
    } else {
      paths.emplace_back(arg);
    }
  }
  if (paths.empty() || (mode == Mode::PRINT && paths.size() != 1U)) {
    usage();
    return 2;
  }

  int failures = 0;
  for (const std::string &path : paths) {
    std::vector<Entry> entries;
    if (!load_scenario(path, entries)) {
      return 2;
    }
    runtime::with_engine_type(profile, [&](auto engine_type) {
      run_entries<typename decltype(engine_type)::type>(entries);
    });

    switch (mode) {
    case Mode::PRINT:
      std::cout << transcript(entries);
      break;
    case Mode::CHECK:
      if (!check(path, entries)) {
        ++failures;
      } else {
        std::cout << path << ": ok\n";
      }
      break;
    case Mode::UPDATE: {
      std::ofstream output(path, std::ios::trunc);
      output << transcript(entries);
      std::cout << path << ": updated\n";
      break;
    }
    }
  }
  return failures == 0 ? 0 : 1;
}
