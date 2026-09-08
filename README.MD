# Stock Exchange

A compact, deterministic stock-exchange core written from scratch in modern C++.
The project focuses on the parts of an exchange that are interesting to reason
about and performance-sensitive to implement: typed order data, price-time
matching, bounded memory, cache-aware structures, and an event-based interface.

The matching engine is implemented as a header-only library under `src/`, with
CMake targets available for the exchange process and two tool entry points.

> **Current status:** The matching engine and supporting data structures are the
> main implementation surface. The `exchange` executable runs a deterministic
> matching demonstration; the two tool entry points remain integration
> placeholders rather than complete terminals or live exchange processes.

## Highlights

- **Price-time priority:** bids are traversed from highest to lowest price, asks
	from lowest to highest, and orders at one price level are matched FIFO.
- **Deterministic processing:** inbound requests are processed synchronously and
	produce an ordered span of typed events.
- **Multiple symbols:** `MatchingEngine` owns one `OrderBook` per symbol and
	routes add, cancel, and modify requests to the correct book.
- **Broad order support:** limit, market, IOC, FOK, GTC, stop, stop-limit,
	iceberg, and post-only order types are represented and handled by the book.
- **Order lifecycle management:** accepts, rests, fills, partial fills,
	reductions, cancellations, and rejections are represented explicitly.
- **Stop-order activation:** stop and stop-limit orders are parked until the
	last trade price reaches their trigger, then activated as market or limit
	orders respectively.
- **Iceberg replenishment:** visible slices are replenished from hidden quantity
	and re-enter the back of their price level, preserving queue semantics.
- **Predictable memory behavior:** orders and price levels use fixed-capacity
	intrusive free-list pools; event storage is allocated up front.
- **Allocation-free hot paths:** the order book uses pre-sized polymorphic memory
	resources for indexes and avoids general-purpose allocation during normal
	matching operations.
- **Low-level data layout:** order and level records are cache-aligned, compact,
	trivially copyable value types are used for request/event payloads, and SPSC
	queue cursors are separated onto cache lines.
- **Strong compile-time constraints:** C++20, strict warnings, no compiler
	extensions, and `-fno-exceptions` are enabled by the build.
- **Useful failure reporting:** validation returns typed reason codes such as
	duplicate order ID, empty book, insufficient FOK liquidity, post-only crossing,
	invalid iceberg display quantity, and exhausted order pool.

## Architecture

```text
InboundOrder
		 |
		 v
MatchingEngine  -- routes by Symbol -->  OrderBook
																						 |
												 +-------------------+-------------------+
												 |                   |                   |
										Bid/ask maps       Stop-order maps      Order index
										(price levels)     (trigger prices)    (order IDs)
												 |                   |                   |
												 +-----------> Event span <-----------+
```

### Core types

`src/core/types.h` defines the exchange vocabulary:

- `Price` is a signed 64-bit integer. Prices are represented in fixed-point
	units using `PRICE_SCALE = 10'000`, avoiding floating-point comparisons.
- `Quantity`, IDs, sequence numbers, timestamps, and participant IDs have
	explicit fixed-width integer types.
- Symbols are fixed-size eight-character arrays, making request and event
	payloads predictable and trivially copyable.
- `Side`, `OrderType`, and `OrderStatus` are scoped enums.

### Matching engine

`src/matching/matching_engine.h` is the multi-symbol facade. Call
`prepare_symbols()` to initialize books ahead of time, then submit an
`InboundOrder` through `process_order()`. The request action can be:

- `ADD` to validate, match, rest, park, or reject an order
- `CANCEL` to remove an active order
- `MODIFY` to reduce quantity in place or replace an order with new price or
	quantity

Each operation returns `std::span<const Event>`. The span refers to storage owned
by the selected book and remains valid until the next operation on that book.

### Order book

`src/matching/order_book.h` stores the best bid and ask directly while keeping
ordered maps for all active price levels. Each `Level` is a doubly linked FIFO
list of `Order` objects. This gives the implementation clear best-price
traversal and constant-time insertion/removal once a level is known.

Matching occurs at the resting order's price. A marketable inbound order walks
the opposing side until its quantity is exhausted or its limit price no longer
crosses. Trades and order-state changes are emitted as typed events in the order
they occur.

### Memory and concurrency primitives

- `MemoryPool<T, N>` provides bounded, reusable object storage with an intrusive
	free list and no per-object heap allocation.
- `SPSCQueue<T, N>` is a bounded single-producer/single-consumer ring buffer.
	Its capacity must be a power of two and its payload must be trivially
	copyable; acquire/release ordering protects publication and consumption.
- `LogicalClock` provides deterministic monotonic timestamps for simulations,
	while `now_ns()` exposes a steady-clock nanosecond timestamp helper.

## Supported order behavior

| Order type | Behavior |
| --- | --- |
| `LIMIT` | Matches at or better than the limit, then rests remaining quantity. |
| `MARKET` | Consumes available opposing liquidity; rejected when the book is empty. |
| `IOC` | Matches immediately and cancels any unfilled remainder. |
| `FOK` | Rejected unless the full requested quantity is available at the limit. |
| `GTC` | Matches and rests remaining quantity. |
| `STOP` | Parks at a trigger price and activates as a market order. |
| `STOP_LIMIT` | Parks at a trigger price and activates as a limit order. |
| `ICEBERG` | Shows a validated display slice and replenishes hidden quantity. |
| `POST_ONLY` | Rejected if it would immediately cross; otherwise rests. |

Orders can be canceled while active. A same-price quantity reduction preserves
the order's queue position; a price change or quantity increase replaces the
old order and therefore re-enters the book.

## Events

The book emits a bounded sequence of `Event` values. The event union includes:

- `ORDER_ACCEPTED`
- `ORDER_RESTED`
- `ORDER_REJECTED`
- `ORDER_REDUCED`
- `ORDER_PARTIALLY_FILLED`
- `ORDER_FILLED`
- `ORDER_CANCELED`
- `TRADE`

Events include the relevant order IDs, symbol, side, price, quantity, timestamp,
match ID, or rejection reason. The representation is trivially copyable and
does not require virtual dispatch.

## Capacity defaults

The templated `OrderBook` and `MatchingEngine` defaults are intentionally
explicit and can be changed at compile time:

| Resource | Default capacity |
| --- | ---: |
| Orders | 1,000,000 |
| Price levels | 10,000 |
| Events per operation | 262,144 |
| Stop-order index entries | 65,536 |

Capacity exhaustion is handled as a typed order rejection where the API can
report it. Event storage overflow is treated as a programming/configuration
failure and aborts, so deployment capacity should be sized for the largest
expected operation.

## Requirements

- CMake 3.24 or newer
- A C++20 compiler
- macOS, Linux, or another platform supported by the selected compiler

The project currently has no third-party runtime dependencies and uses only the
C++ standard library.

## Build

From the repository root:

```sh
cmake -S . -B build
cmake --build build --parallel
```

The build enables:

- C++20 with extensions disabled
- `-Wall`, `-Wextra`, and `-Wpedantic`
- `-fno-exceptions`
- `compile_commands.json` generation

Build artifacts are placed in predictable subdirectories:

```text
build/src/exchange
build/tools/trader_terminal
build/tools/liquidity_bot
```

## Execute the current targets

The targets can be run after building:

```sh
./build/src/exchange
./build/tools/trader_terminal
./build/tools/liquidity_bot
```

The `exchange` executable runs a deterministic AAPL scenario that prints
accepted, rested, trade, partial-fill, filled, and canceled events. The two tool
executables currently print:

```text
Hello World
```

The reusable matching implementation is exercised by including its headers in a
C++20 program. A minimal request flow looks like this:

```cpp
#include "matching/matching_engine.h"

using namespace exchange;

int main() {
	matching::MatchingEngine<> engine;

	matching::InboundOrder order{
			.action = matching::RequestAction::ADD,
			.symbol = core::make_symbol("AAPL"),
			.order_id = 1,
			.side = core::Side::BUY,
			.price = 1'750'000, // 175.0000 at PRICE_SCALE 10,000
			.qty = 100,
			.order_type = core::OrderType::LIMIT,
			.timestamp = 1,
			.participant_id = 7,
	};

	const auto events = engine.process_order(order);
	(void)events;
}
```

Because the library is header-based, add `src/` to the compiler include path
and compile with C++20. The CMake target `exchange_project` already exposes
that include directory and the required compile features.

## Project layout

```text
.
├── CMakeLists.txt
├── README.MD
├── src/
│   ├── exchange_main.cpp       # Current exchange executable placeholder
│   ├── core/
│   │   ├── clock.h             # Logical and steady-clock timestamps
│   │   ├── memory_pool.h       # Fixed-capacity object pool
│   │   ├── spsc_queue.h        # Bounded SPSC ring buffer
│   │   └── types.h             # Fixed-width domain types
│   └── matching/
│       ├── events.h            # Request outcomes and trade events
│       ├── level.h             # FIFO price-level list
│       ├── matching_engine.h   # Multi-symbol routing facade
│       ├── order.h             # Cache-aligned order record
│       └── order_book.h        # Matching and lifecycle logic
└── tools/
		├── liquidity_bot.cpp       # Current tool placeholder
		└── trader_terminal.cpp     # Current tool placeholder
```

## Development notes

This codebase is deliberately close to the engine mechanics: data structures
are visible in headers, capacities are explicit template parameters, and event
outputs are easy to inspect. That makes it useful for learning exchange design,
benchmarking focused changes, and building a future transport or simulation
layer around a deterministic matching core.

There is currently no test target or benchmark target in `CMakeLists.txt`. Before
using the project as a production exchange, add focused coverage for matching
priority, all order-type edge cases, stop activation, iceberg replenishment,
capacity exhaustion, event-buffer sizing, and concurrent transport integration.
