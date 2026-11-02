#!/usr/bin/env bash
# Configure (first run only), build, and run the project's programs.

set -euo pipefail

usage() {
	cat <<EOF
Usage: ./run.sh [target] [args...]

Targets:
  exchange         Deterministic matching-engine demonstration (default)
  exchange_server  The exchange: TCP gateway, engine thread, journal
  trader_terminal  Command-line trading client (needs --participant N)
  liquidity_bot    Market-making client
  replay           Replay scenarios or a journal through the engine
  load_test        Throughput and latency test against a running server
  test             Build everything and run all tests
  live             Start the server and a liquidity bot, then open a
                   trader terminal as participant 1 (args go to the terminal)

Any remaining arguments are passed to the executable.
Set BUILD_DIR to use a different build directory (default: ./build).
EOF
}

root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
build_dir="${BUILD_DIR:-$root/build}"
target="${1:-exchange}"
if (($# > 0)); then
	shift
fi

configure() {
	if [[ ! -f "$build_dir/CMakeCache.txt" ]]; then
		cmake -S "$root" -B "$build_dir"
	fi
}

case "$target" in
exchange | exchange_server)
	binary="$build_dir/src/$target"
	;;
trader_terminal | liquidity_bot | replay | load_test)
	binary="$build_dir/tools/$target"
	;;
test)
	configure
	cmake --build "$build_dir" --parallel
	exec ctest --test-dir "$build_dir" --output-on-failure
	;;
live)
	configure
	cmake --build "$build_dir" --parallel \
		--target exchange_server liquidity_bot trader_terminal
	log_dir="$build_dir/live"
	mkdir -p "$log_dir"
	"$build_dir/src/exchange_server" --config "$root/config/exchange.conf" \
		--set "journal_path=$root/data/exchange.journal" \
		>"$log_dir/server.log" 2>&1 &
	server_pid=$!
	bot_pid=""
	stop_all() {
		[[ -n "$bot_pid" ]] && kill "$bot_pid" 2>/dev/null && wait "$bot_pid" 2>/dev/null
		kill "$server_pid" 2>/dev/null && wait "$server_pid" 2>/dev/null
		echo "server and bot stopped; logs in $log_dir"
	}
	trap stop_all EXIT
	port=""
	for _ in $(seq 1 50); do
		port="$(grep -o 'listening on [0-9.]*:[0-9]*' "$log_dir/server.log" |
			sed 's/.*://' || true)"
		[[ -n "$port" ]] && break
		if ! kill -0 "$server_pid" 2>/dev/null; then
			cat "$log_dir/server.log" >&2
			exit 1
		fi
		sleep 0.1
	done
	[[ -n "$port" ]] || { cat "$log_dir/server.log" >&2; exit 1; }
	head -n 2 "$log_dir/server.log"
	"$build_dir/tools/liquidity_bot" --port "$port" --symbol AAPL --mid 100 \
		>"$log_dir/bot.log" 2>&1 &
	bot_pid=$!
	echo "liquidity bot quoting AAPL around 100 (log: $log_dir/bot.log)"
	"$build_dir/tools/trader_terminal" --port "$port" --participant 1 "$@"
	exit 0
	;;
-h | --help)
	usage
	exit 0
	;;
*)
	echo "Unknown target: $target" >&2
	usage >&2
	exit 2
	;;
esac

configure
cmake --build "$build_dir" --parallel --target "$target"

exec "$binary" "$@"
