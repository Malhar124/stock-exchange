cmake_minimum_required(VERSION 3.24)

project(exchange LANGUAGES CXX)

include(FetchContent)

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