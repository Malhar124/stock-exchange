cmake_minimum_required(VERSION 3.24)

project(exchange LANGUAGES CXX)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(exchange_project INTERFACE)

target_include_directories(
    exchange_project
    INTERFACE
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
)

target_compile_features(exchange_project INTERFACE cxx_std_20)

target_compile_options(
    exchange_project
    INTERFACE
        -Wall
        -Wextra
        -Wpedantic
        -fno-exceptions
)


add_executable(
    exchange
    src/exchange_main.cpp
)

target_link_libraries(
    exchange
    PRIVATE
        exchange_project
)

set_target_properties(
    exchange
    PROPERTIES
        OUTPUT_NAME "exchange"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/src"
)

find_package(Threads REQUIRED)

add_executable(
    exchange_server
    src/exchange_server.cpp
)

target_link_libraries(
    exchange_server
    PRIVATE
        exchange_project
        Threads::Threads
)

set_target_properties(
    exchange_server
    PROPERTIES
        OUTPUT_NAME "exchange_server"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/src"
)

add_executable(
    trader_terminal
    tools/trader_terminal.cpp
)

target_link_libraries(
    trader_terminal
    PRIVATE
        exchange_project
)

set_target_properties(
    trader_terminal
    PROPERTIES
        OUTPUT_NAME "trader_terminal"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tools"
)


add_executable(
    liquidity_bot
    tools/liquidity_bot.cpp
)

target_link_libraries(
    liquidity_bot
    PRIVATE
        exchange_project
)

set_target_properties(
    liquidity_bot
    PROPERTIES
        OUTPUT_NAME "liquidity_bot"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tools"
)

add_executable(
    load_test
    tools/load_test.cpp
)

target_link_libraries(
    load_test
    PRIVATE
        exchange_project
)

set_target_properties(
    load_test
    PROPERTIES
        OUTPUT_NAME "load_test"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tools"
)

add_executable(
    replay
    tools/replay.cpp
)

target_link_libraries(
    replay
    PRIVATE
        exchange_project
)

set_target_properties(
    replay
    PROPERTIES
        OUTPUT_NAME "replay"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tools"
)

enable_testing()

add_executable(
    unit_tests
    tests/unit/test_main.cpp
    tests/unit/engine_tests.cpp
    tests/unit/runtime_tests.cpp
    tests/unit/io_tests.cpp
)

target_link_libraries(
    unit_tests
    PRIVATE
        exchange_project
        Threads::Threads
)

# Overwrite freed pool objects so any use-after-free in matching fails loudly.
target_compile_definitions(unit_tests PRIVATE EXCHANGE_POISON_FREED)

set_target_properties(
    unit_tests
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests"
)

add_test(NAME unit_tests COMMAND unit_tests)

# Scenario tests use a replay build that also poisons freed pool objects.
add_executable(
    replay_checked
    tools/replay.cpp
)

target_link_libraries(
    replay_checked
    PRIVATE
        exchange_project
)

target_compile_definitions(replay_checked PRIVATE EXCHANGE_POISON_FREED)

set_target_properties(
    replay_checked
    PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests"
)

file(GLOB SCENARIO_FILES CONFIGURE_DEPENDS
    "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/*.scenario")
foreach(scenario IN LISTS SCENARIO_FILES)
    get_filename_component(scenario_name "${scenario}" NAME_WE)
    add_test(NAME "scenario.${scenario_name}"
             COMMAND replay_checked --check "${scenario}")
endforeach()

add_test(NAME integration.server_smoke
         COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration/server_smoke.sh"
                 "${CMAKE_BINARY_DIR}")
