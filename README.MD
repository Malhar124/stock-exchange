# Stock Exchange

A compact, deterministic stock exchange written from scratch in modern C++: a
price-time matching engine with bounded memory, wrapped in a runnable exchange
process with a TCP gateway, a durable journal with crash recovery, market data,
command-line clients, and a test suite.

The matching engine is a header-only library under `src/matching`. The exchange
process (`exchange_server`) runs it on a dedicated thread behind a gateway that
handles sessions, order ownership, and risk checks.

## Quick start

```sh
./run.sh test    # build everything and run all tests
./run.sh live    # start the server and a market-making bot, then trade
```

`./run.sh live` opens a trader terminal as participant 1 against a liquidity
bot quoting AAPL around 100. Try `book AAPL`, `buy AAPL 10 100.10`, `orders`,
`help`. Quitting the terminal stops the server and the bot; the journal in
`data/` means the next `./run.sh live` resumes where you left off.

## Highlights

- **Price-time priority:** bids are traversed from highest to lowest price, asks
	from lowest to highest, and orders at one price level are matched FIFO.
- **Deterministic processing:** the same sequence of requests always produces
	the same events, which is what makes journal replay and scenario tests exact.
- **Broad order support:** limit, market, IOC, FOK, GTC, stop, stop-limit,
	iceberg, and post-only orders, plus opt-in self-trade prevention.
- **Bounded memory, no crashes on long runs:** orders, levels, and index nodes
	come from fixed-capacity pools that recycle freed memory, so capacity limits
	apply to live orders, not to orders seen over the process lifetime.
- **Typed failure reporting:** every capacity limit and validation failure is
	a reason code (duplicate order ID, empty book, FOK liquidity, post-only
	crossing, pool, level, or stop capacity exhausted, self-trade prevented, ...).
- **Engine isolated from I/O:** a single engine thread sequences, journals, and
	matches; the gateway thread does all networking. They communicate through
	lock-free SPSC queues, and the gateway never blocks on the engine.
- **Durable and recoverable:** with `journal_fsync = batch`, every request is
	on disk before it is matched (group commit, one fsync per batch). Restarting
	replays the journal and restores the books, open orders, and ownership.
- **Strong compile-time constraints:** C++20, strict warnings, no compiler
	extensions, `-fno-exceptions`, and no third-party dependencies.

## Architecture

```text
trader_terminal, liquidity_bot, load_test, ...
        |  TCP, length-prefixed binary messages (src/protocol/messages.h)
        v
+-- Gateway thread (main) -----------------------------------------------+
| sessions and login, client order ID -> exchange order ID, owner checks, |
| risk limits, execution reports, market data (L2 book built from events) |
+--------------+-------------------------------------------^--------------+
               | SPSCQueue<GatewayRequest>                 | SPSCQueue<EngineOutput>
               v                                           |
+-- Engine thread ----------------------------------------+--------------+
| sequencer (global sequence + timestamp) -> journal (append, fsync) ->  |
| MatchingEngine::process_order() -> BEGIN, events, END per request      |
+------------------------------------------------------------------------+
```

The engine itself:

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
                         +-----------> Event span <--------------+
```

### Core types

`src/core/types.h` defines the exchange vocabulary:

- `Price` is a signed 64-bit integer. Prices are represented in fixed-point
	units using `PRICE_SCALE = 10'000`, avoiding floating-point comparisons.
- `Quantity`, IDs, sequence numbers, timestamps, and participant IDs have
	explicit fixed-width integer types.
- Symbols are fixed-size eight-character arrays, making request and event
	payloads predictable and trivially copyable.
- `Side`, `OrderType`, and `SelfTradePrevention` are scoped enums.

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

`ENGINE_BEHAVIOR_VERSION` is bumped whenever the same requests could produce
different events; journals record it so an old journal is never silently
replayed with new semantics.

### Order book

`src/matching/order_book.h` stores the best bid and ask directly while keeping
ordered maps of the live price levels. Each `Level` is a doubly linked FIFO
list of `Order` objects. Emptied levels are removed from the maps, so the best
level is always the first entry.

Matching occurs at the resting order's price. A marketable inbound order walks
the opposing side until its quantity is exhausted or its limit price no longer
crosses. Trades and order-state changes are emitted as typed events in the order
they occur.

Stop orders are triggered only after the order that caused the triggering trade
has finished (rested, filled, or been canceled), so an activated stop sees the
book as it actually stands. A stop whose trigger has already been reached when
it arrives activates immediately. When one trade triggers several stops, they
activate in the order the price crossed their triggers: lowest buy stop first,
highest sell stop first.

### Memory and concurrency primitives

- `MemoryPool<T, N>` provides bounded, reusable object storage with an intrusive
	free list and no per-object heap allocation.
- `FixedBlockResource` is a `std::pmr::memory_resource` over one fixed buffer
	that recycles freed blocks, used for the order index, level maps, and stop maps.
- `SPSCQueue<T, N>` is a bounded single-producer/single-consumer ring buffer.
	Its capacity must be a power of two and its payload must be trivially
	copyable; acquire/release ordering protects publication and consumption.
- `now_ns()` returns steady-clock nanoseconds for latency measurements.

## Supported order behavior

| Order type | Behavior |
| --- | --- |
| `LIMIT` | Matches at or better than the limit, then rests remaining quantity. |
| `MARKET` | Consumes available opposing liquidity; rejected when the book is empty. |
| `IOC` | Matches immediately and cancels any unfilled remainder. |
| `FOK` | Rejected unless the full requested quantity, hidden iceberg quantity included, is available at the limit. |
| `GTC` | Matches and rests remaining quantity. |
| `STOP` | Parks at a trigger price and activates as a market order. |
| `STOP_LIMIT` | Parks at a trigger price and activates as a limit order. |
| `ICEBERG` | Shows a validated display slice and replenishes hidden quantity. |
| `POST_ONLY` | Rejected if it would immediately cross, on entry or on modify; otherwise rests. |

Orders can be canceled while active. A same-price quantity reduction preserves
the order's queue position; a price change or quantity increase replaces the
old order and therefore re-enters the book. A modify that changes nothing is
rejected with `INVALID_MODIFICATION`.

### Self-trade prevention

Each order can set what happens when it would trade against a resting order
from the same participant. The incoming order's setting decides; participant 0
is anonymous and never matches itself.

| Mode | Effect |
| --- | --- |
| `NONE` (default) | Trade normally. |
| `CANCEL_RESTING` | Cancel the resting order and keep matching. |
| `CANCEL_AGGRESSOR` | Cancel the rest of the incoming order. |
| `CANCEL_BOTH` | Cancel both. |

Prevented trades produce `ORDER_CANCELED` with reason `SELF_TRADE_PREVENTED`.
An FOK does not count the participant's own orders as liquidity.

## Events

The book emits a bounded sequence of `Event` values:

- `ORDER_ACCEPTED`
- `ORDER_RESTED`
- `ORDER_REJECTED`
- `ORDER_REDUCED`
- `ORDER_PARTIALLY_FILLED`
- `ORDER_FILLED`
- `ORDER_CANCELED` (with a reason: `NONE` for requested cancels and unfilled
	IOC/market remainders, or `SELF_TRADE_PREVENTED`, `LEVEL_CAPACITY_EXHAUSTED`)
- `TRADE`

Events include the relevant order IDs, symbol, side, price, quantity, timestamp,
match ID, or rejection reason. They carry no sequence number of their own: the
gateway knows each event's request, and the request has the global sequence.
 The representation is trivially copyable and
does not require virtual dispatch.

## Capacity

Capacities are compile-time template parameters. The server selects one of
three profiles (`src/runtime/capacity_profiles.h`); the journal records which:

| Profile | Live orders | Price levels | Event chunk | Parked stops | Memory per symbol |
| --- | ---: | ---: | ---: | ---: | ---: |
| `small` | 65,536 | 4,096 | 65,536 | 4,096 | ~26 MB |
| `medium` | 262,144 | 8,192 | 262,144 | 16,384 | ~105 MB |
| `large` (engine defaults) | 1,000,000 | 10,000 | 262,144 | 65,536 | ~335 MB |

Limits apply to live state only. Exceeding one is reported, not fatal:

- too many live orders: `REJECTED` with `ORDER_POOL_EXHAUSTED`
- too many parked stops: `REJECTED` with `STOP_CAPACITY_EXHAUSTED`
- a resting order needing a new price level when all are in use:
	`CANCELED` with `LEVEL_CAPACITY_EXHAUSTED`

The event chunk is not a limit. The server receives a request's events in
chunks of this size while the request runs, so a stop cascade or a sweep
through many small orders never needs more memory. Used as a library without an
overflow sink (`OrderBook::set_event_overflow`), the buffer grows instead.

## Running the exchange

### Server

```sh
./run.sh exchange_server --config config/exchange.conf
./run.sh exchange_server --config config/exchange.conf --set port=9100 --set journal_fsync=none
```

All settings are documented in [`config/exchange.conf`](config/exchange.conf):
listen address and port, symbols, capacity profile, journal path and fsync
policy, risk limits (maximum order quantity, notional, price band around the
last trade, tick size, open orders per participant), cancel-on-disconnect, the
auth file, and the metrics interval. `SIGINT`/`SIGTERM` shut down cleanly: new requests are refused, queued
ones are matched and journaled, results are delivered, then the process exits
and prints cumulative metrics.

While running, the server prints latency and throughput metrics, e.g.:

```text
[metrics] requests=100000 (+100000) trades=50000 (+50000) | engine p50=127ns p99=351ns max=7417ns | round-trip p50=73us p99=212us | sessions=1 open_orders=0 gateway_rejects=0 max_queue_depth=256/300
```

"engine" is matching time per request; "round-trip" is from the gateway
receiving a request to the gateway finishing its results, including the journal.

### Gateway rules

- A connection must log in with a participant ID (non-zero; one session per
	participant at a time). With `auth_file` set, the login must also carry
	that participant's secret, or the gateway answers `AUTHENTICATION_FAILED`
	and closes the connection. Without `auth_file`, the server only listens
	on a loopback address (`127.x.x.x`) and trusts the participant ID.
- The auth file has one `<participant> <secret>` line per participant, with
	secrets of 16 to 32 characters. It must not be readable by group or
	others (`chmod 600`), and `*.secrets` files are ignored by git. Clients
	send the secret in `EXCHANGE_SECRET`, so it does not show in `ps`.
- Clients choose their own client order IDs, unique among their live orders.
	The gateway assigns exchange order IDs, so participants cannot collide, and
	a participant can only cancel or modify its own orders.
- Risk checks run before an order reaches the engine. Rejections carry a
	gateway reason (`MAX_ORDER_QTY`, `PRICE_BAND`, `PRICE_NOT_ON_TICK`,
	`ENGINE_BUSY`, ...). Only stop and stop-limit orders may carry a trigger
	price (`INVALID_REQUEST` otherwise); the notional limit values a stop
	order at the higher of its trigger price and the last trade.
- With a price band, a market order is sent to the engine as an IOC at the
	band edge, so it cannot sweep the book at any price. A stop order is sent
	as a stop-limit at the band edge around its trigger price, so once
	triggered it also trades only within the band; an unfilled rest stays at
	that limit. With a notional limit, a market order before the symbol's
	first trade is rejected: there is no price to value it at.
- While the server shuts down, new orders, modifies, and cancels are refused
	with `SHUTTING_DOWN`.
- Execution reports go to the order's owner, including fills on resting orders.
	Market data (trades and level updates) goes to subscribers; subscribing sends
	a snapshot first.
- Order state survives disconnects; `orders` after reconnecting lists what is
	still open. `cancel_on_disconnect = true` cancels instead; cancels the
	full engine queue refuses are sent again until it takes them.
- TCP keepalive drops a connection whose peer vanished (about 25 seconds),
	so the participant can log in again.

### Trader terminal

```sh
./run.sh trader_terminal --participant 7 [--host 127.0.0.1] [--port 9000]
```

```text
buy|sell <symbol> <qty> [price] [type=...] [trigger=<price>] [display=<qty>] [stp=<mode>]
cancel <id>
modify <id> <qty> <price>
book <symbol>
orders
wait <ms>
```

Without a price, an order is a market order. The terminal reads stdin, so it
can be scripted: `printf 'buy AAPL 10 101\nwait 200\n' | trader_terminal ...`.
When the server has an auth file, put the secret in the environment:
`EXCHANGE_SECRET='<secret>' ./run.sh trader_terminal --participant 7`. The bot
and the load test read it the same way.

### Liquidity bot

```sh
./run.sh liquidity_bot --symbol AAPL --mid 100 --spread 0.10 --step 0.05 --levels 3 --size 100
```

Keeps a ladder of `POST_ONLY` quotes around the last trade price (or `--mid`
before the first trade), moves them as the market moves, and cancels them on
exit.

### Load test

```sh
./run.sh load_test --port 9000 --orders 100000 --window 256
```

Sends crossing buy/sell pairs with a bounded number in flight and reports
throughput and client-observed latency.

## Journal and recovery

The journal (`src/runtime/journal.h`) is an append-only file: a header with the
format version, engine behavior version, and capacity profile, followed by one
fixed-size record per sequenced request (sequence number, client order ID, the
request with its assigned timestamp, and a checksum).

- `journal_fsync = batch`: each batch of requests is written and fsynced (with
	`F_FULLFSYNC` on macOS) before it is matched, so nothing is ever acknowledged
	that a crash could lose.
- `journal_fsync = none`: written before matching but not fsynced; survives a
	process crash, not a power loss.

On startup the server replays the journal through a fresh engine and through
the gateway's order registry and market data, so books, open orders, ownership,
and the last trade price are exactly as before. A torn final record (a crash
mid-write) is dropped and truncated; corruption anywhere else, a sequence gap,
a different engine behavior version, or a different capacity profile stops the
server instead of guessing. Only one server can write a journal at a time; a
second one stops with "in use by another process".

`replay --journal <file>` prints a journal as a transcript of commands and
events, which is itself a valid scenario file.

## Testing

```sh
./run.sh test
```

runs three kinds of tests through `ctest`:

- **Scenario tests** (`tests/scenarios/*.scenario`): commands with the events
	each must produce, checked by the replay runner. They cover price-time
	priority, every order type, validation, modify/cancel, stops, self-trade
	prevention, and regression cases for fixed bugs.
- **Unit tests** (`tests/unit`): lifetime capacity, capacity rejections, text
	formats, journal integrity and locking, market data, gateway rules, risk
	checks and authentication, recovery equivalence, the SPSC queue across two
	threads, and the gateway server's handling of malformed frames and clients
	that stop reading.
- **Integration test** (`tests/integration/server_smoke.sh`): trades through a
	real server over TCP, restarts it, and checks recovery.

Unit and scenario tests are built with `EXCHANGE_POISON_FREED`, which overwrites
freed pool objects so a use-after-free in matching fails loudly.

A scenario is plain text; indented lines are the expected events:

```text
ADD AAPL 1 SELL LIMIT 10 101 participant=2
  ACCEPTED order=1 side=SELL price=101.0000 qty=10 type=LIMIT
  RESTED order=1 side=SELL price=101.0000 qty=10
```

To add one, write the commands, run `./build/tools/replay --update <file>`,
review the generated events, and commit. `replay <file>` prints the transcript,
`replay --check <file>...` verifies.

## Performance

Measured on an Apple Silicon Mac (macOS, Release with debug info), with the
load test against a local server:

| Journal mode | Throughput | Client latency p50 | p99 | Engine matching p50 |
| --- | ---: | ---: | ---: | ---: |
| `none` | ~1.8M orders/s | 147 µs | 294 µs | 127 ns |
| `batch` | ~38k orders/s | 6.3 ms | 16 ms | 223 ns |

With `batch`, latency is dominated by the drive flush (`F_FULLFSYNC`, several
milliseconds on macOS); group commit amortizes it across up to 256 requests.
Recovering 100,000 requests takes about 90 ms.

## Requirements

- CMake 3.24 or newer
- A C++20 compiler
- macOS (tested) or Linux (written for POSIX, not yet tested there)

The project has no third-party dependencies and uses only the C++ standard
library and POSIX.

## Build

From the repository root:

```sh
cmake -S . -B build
cmake --build build --parallel
```

The build defaults to `RelWithDebInfo` and enables:

- C++20 with extensions disabled
- `-Wall`, `-Wextra`, and `-Wpedantic`
- `-fno-exceptions`
- `compile_commands.json` generation

`run.sh` configures on first use, builds the requested target, and runs it:

```sh
./run.sh                  # the engine demonstration
./run.sh exchange_server  # or trader_terminal, liquidity_bot, replay, load_test
./run.sh test             # all tests
./run.sh live             # server + bot + terminal
./run.sh --help
```

## Engine demonstration

The `exchange` executable runs a deterministic AAPL scenario directly against
the engine:

1. Adds sell order `1001` at `1'010'000` for quantity `100` and rests it.
2. Adds non-crossing buy order `2001` at `1'000'000` for quantity `40`.
3. Adds buy order `2002` at `1'010'000` for quantity `60`, matching order `1001`.
4. Cancels the remaining quantity of order `2001`.

Prices are stored as fixed-point values, so `1'010'000` represents `101.0000`
with `PRICE_SCALE = 10'000`.

```text
1. Add resting ask (2 events)
  ACCEPTED order=1001 side=SELL price=101.0000 qty=100 type=LIMIT
  RESTED order=1001 side=SELL price=101.0000 qty=100
2. Add non-crossing bid (2 events)
  ACCEPTED order=2001 side=BUY price=100.0000 qty=40 type=LIMIT
  RESTED order=2001 side=BUY price=100.0000 qty=40
3. Add crossing bid (4 events)
  ACCEPTED order=2002 side=BUY price=101.0000 qty=60 type=LIMIT
  TRADE match=1 buy_order=2002 sell_order=1001 price=101.0000 qty=60
  PARTIALLY_FILLED order=1001 filled=60 remaining=40 price=101.0000
  FILLED order=2002 qty=60 price=101.0000
4. Cancel remaining bid (1 events)
  CANCELED order=2001 qty=40
```

The same scenario is in `tests/scenarios/demo.scenario`.

Using the engine as a library:

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

Add `src/` to the include path and compile with C++20; the CMake target
`exchange_project` provides both.

## Project layout

```text
.
├── CMakeLists.txt
├── README.MD
├── run.sh                      # Build-and-run helper
├── config/exchange.conf        # Server settings, documented
├── src/
│   ├── exchange_main.cpp       # Engine demonstration
│   ├── exchange_server.cpp     # The exchange process
│   ├── core/                   # Types, clock, pools, SPSC queue, memory resource
│   ├── matching/               # Matching engine (header-only)
│   ├── text/order_text.h       # Text forms of requests and events
│   ├── runtime/                # Engine stage, journal, market data, metrics, profiles
│   ├── protocol/messages.h     # Client <-> gateway wire format
│   ├── gateway/                # Gateway logic and its I/O loop
│   ├── net/socket.h            # POSIX TCP helpers
│   ├── client/                 # Client connection and report formatting
│   └── server/config.h         # Config file parsing
├── tools/
│   ├── trader_terminal.cpp     # Command-line trading client
│   ├── liquidity_bot.cpp       # Market maker
│   ├── load_test.cpp           # Throughput/latency test
│   └── replay.cpp              # Scenario and journal replay
└── tests/
    ├── scenarios/              # Expected-event scenario files
    ├── unit/                   # Unit tests
    └── integration/            # End-to-end server test
```

## Known limitations

- **No encryption.** The protocol is plain TCP, so secrets and orders can be
	read on the network. Across a network you do not trust, tunnel the port
	(SSH or WireGuard).
- **Little-endian hosts only.** The wire protocol and journal use the host's
	byte order.
- **One engine thread.** All symbols share it. Splitting symbols across several
	engine threads would scale further.
- **Market data shares the order connection.** There is no separate multicast
	feed, and a client that stops reading is disconnected once 64 MB is queued.
- **Self-trade prevention matches on participant ID.** There is no separate
	firm-level group ID.
- **No journal snapshots.** Recovery replays the whole journal; it is fast
	(about 1 µs per request) but grows with history.
