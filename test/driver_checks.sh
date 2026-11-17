#!/bin/sh
# Runs every case of the Driver's tests, built in BUILD, under a
# per-case alarm, so a hang is a failure. Exits nonzero if any case fails. Each waiter case
# explores up to WAITER_LIMIT schedules (1,000,000; threads: at most 300 rounds).
#   test/driver_checks.sh BUILD
build="$(cd "$1" && pwd)"; shift
status=0
bounded() { perl -e 'alarm shift; exec @ARGV' "$@"; }
out="$build/driver_checks.out"
run() {
	echo "== $*"; bounded "$@" >"$out" 2>&1; rc=$?
	grep -m 12 FAIL "$out"; tail -n 6 "$out"
	[ "$rc" -eq 0 ] || { echo "exit $rc"; status=1; }
}
for c in lone_voter rows checkpoints close fence; do run 120 "$build/consensus_driver_test" "$c"; done
for c in lone_voter stalled needs_peer staged_then_cancelled cross_thread_cancel ownership; do run 120 "$build/consensus_blocking_entry_test" "$c"; done
for c in rows-propose rows-committed rows-read rows-propose-pair close close-read close-open close-late teardown teardown-close threads; do run 900 "$build/consensus_waiter_test" "$c" "${WAITER_LIMIT:-1000000}"; done
run 300 "$build/consensus_driver_idle" 1000
SIM_DRIVER_SEEDS="${SIM_DRIVER_SEEDS:-8}" run 600 "$build/consensus_simulation_test" 0 3000 1
exit $status
