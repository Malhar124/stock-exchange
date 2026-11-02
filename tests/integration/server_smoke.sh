#!/usr/bin/env bash
# End-to-end test: trade through exchange_server over TCP, restart it, and
# check that recovery from the journal restores the open orders and book.
#
# Usage: server_smoke.sh <build-dir>

set -euo pipefail

build_dir="$(cd "$1" && pwd)"
server="$build_dir/src/exchange_server"
terminal="$build_dir/tools/trader_terminal"
replay="$build_dir/tools/replay"

work_dir="$(mktemp -d)"
server_pid=""
cleanup() {
	if [[ -n "$server_pid" ]]; then
		kill "$server_pid" 2>/dev/null || true
		wait "$server_pid" 2>/dev/null || true
	fi
	rm -rf "$work_dir"
}
trap cleanup EXIT

fail() {
	echo "FAIL: $*" >&2
	echo "--- server log" >&2
	cat "$work_dir"/server*.log >&2 || true
	exit 1
}

start_server() {
	local log="$work_dir/server$1.log"
	"$server" --set port=0 --set "journal_path=$work_dir/exchange.journal" \
		--set journal_fsync=none --set metrics_interval_seconds=0 >"$log" 2>&1 &
	server_pid=$!
	for _ in $(seq 1 50); do
		port="$(grep -o '127.0.0.1:[0-9]*' "$log" | cut -d: -f2 || true)"
		[[ -n "$port" ]] && return 0
		sleep 0.1
	done
	fail "server did not start"
}

stop_server() {
	kill -TERM "$server_pid"
	wait "$server_pid" || fail "server exited with an error"
	server_pid=""
}

# expect <output> <pattern>...: every pattern must appear in the output.
expect() {
	local output="$1"
	shift
	for pattern in "$@"; do
		grep -qF -- "$pattern" <<<"$output" ||
			fail "expected '$pattern' in:"$'\n'"$output"
	done
}

start_server 1

out="$(printf 'sell AAPL 100 101.5\nsell AAPL 50 102\nwait 200\n' |
	"$terminal" --participant 1 --port "$port")"
expect "$out" \
	"RESTED id=1 AAPL SELL 100 @ 101.5000 open=100" \
	"RESTED id=2 AAPL SELL 50 @ 102.0000 open=50"

out="$(printf 'buy AAPL 120 102\nwait 200\ncancel 2\nwait 200\nbook AAPL\n' |
	"$terminal" --participant 2 --port "$port")"
expect "$out" \
	"TRADE id=1 AAPL BUY 100 @ 101.5000" \
	"TRADE id=1 AAPL BUY 20 @ 102.0000" \
	"FILLED id=1 AAPL BUY 120 @ 102.0000 open=0" \
	"REQUEST_REJECTED id=2 reason=UNKNOWN_CLIENT_ORDER_ID" \
	"ask 102.0000  30"

out="$(printf 'buy AAPL 5 99 stp=CANCEL_BOTH\nsell AAPL 5 99 stp=CANCEL_BOTH\nwait 200\n' |
	"$terminal" --participant 3 --port "$port")"
expect "$out" \
	"CANCELED id=1 AAPL BUY 5 reason=SELF_TRADE_PREVENTED" \
	"CANCELED id=2 AAPL SELL 5 reason=SELF_TRADE_PREVENTED"

# Five requests reached the engine; the rejected cancel never left the gateway.
stop_server
start_server 2
expect "$(cat "$work_dir/server2.log")" "recovered 5 requests" "1 orders open"

out="$(printf 'orders\nbook AAPL\nmodify 2 10 102\nwait 200\n' |
	"$terminal" --participant 1 --port "$port")"
expect "$out" \
	"OPEN id=2 AAPL SELL 30 @ 102.0000 open=30" \
	"last=102.0000" \
	"REDUCED id=2 AAPL SELL 10 @ 102.0000 open=10"

stop_server

out="$("$replay" --journal "$work_dir/exchange.journal")"
expect "$out" "6 requests" "MODIFY AAPL 2 10 102.0000" "REDUCED order=2 new_qty=10"

echo "server smoke test passed"
