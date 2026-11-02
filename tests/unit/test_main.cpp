#include <cstdio>
#include <cstring>

#include "test_support.h"

// Runs every registered test, or only those whose name contains argv[1].
int main(int argc, char **argv) {
  const char *filter = argc > 1 ? argv[1] : nullptr;
  int run = 0;
  int failed_tests = 0;
  for (const auto &test : exchange::testing::registry()) {
    if (filter != nullptr && std::strstr(test.name, filter) == nullptr) {
      continue;
    }
    const int failures_before = exchange::testing::failure_count();
    std::printf("[ RUN  ] %s\n", test.name);
    test.function();
    ++run;
    if (exchange::testing::failure_count() != failures_before) {
      ++failed_tests;
      std::printf("[ FAIL ] %s\n", test.name);
    }
  }
  std::printf("%d tests, %d failed\n", run, failed_tests);
  return failed_tests == 0 ? 0 : 1;
}
