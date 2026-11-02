#pragma once

#include <cstdio>
#include <functional>
#include <string>
#include <vector>

// Minimal test harness: the build disables exceptions, and the project has no
// third-party dependencies, so tests use plain functions and CHECK macros.
namespace exchange::testing {

struct TestCase {
  const char *name;
  void (*function)();
};

inline std::vector<TestCase> &registry() {
  static std::vector<TestCase> tests;
  return tests;
}

inline int &failure_count() {
  static int failures = 0;
  return failures;
}

inline bool register_test(const char *name, void (*function)()) {
  registry().push_back(TestCase{name, function});
  return true;
}

inline void report_failure(const char *file, int line,
                           const std::string &message) {
  ++failure_count();
  std::fprintf(stderr, "  %s:%d: %s\n", file, line, message.c_str());
}

} // namespace exchange::testing

#define TEST_CASE(name)                                                        \
  static void name();                                                          \
  [[maybe_unused]] static const bool name##_registered =                       \
      ::exchange::testing::register_test(#name, name);                         \
  static void name()

#define CHECK(condition)                                                       \
  do {                                                                         \
    if (!(condition)) {                                                        \
      ::exchange::testing::report_failure(__FILE__, __LINE__,                  \
                                          "CHECK(" #condition ") failed");     \
    }                                                                          \
  } while (false)

#define CHECK_EQ(actual, expected)                                             \
  do {                                                                         \
    const auto &check_actual_ = (actual);                                      \
    const auto &check_expected_ = (expected);                                  \
    if (!(check_actual_ == check_expected_)) {                                 \
      ::exchange::testing::report_failure(                                     \
          __FILE__, __LINE__, "CHECK_EQ(" #actual ", " #expected ") failed");  \
    }                                                                          \
  } while (false)

#define REQUIRE(condition)                                                     \
  do {                                                                         \
    if (!(condition)) {                                                        \
      ::exchange::testing::report_failure(__FILE__, __LINE__,                  \
                                          "REQUIRE(" #condition ") failed");   \
      return;                                                                  \
    }                                                                          \
  } while (false)
