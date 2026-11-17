#!/bin/sh
# Builds the Driver's tests in BUILD (configured by the caller) and runs every case under a
# per-case alarm, so a hang is a failure. Exits nonzero if any case fails.
#   tools/driver_checks.sh BUILD [targets...]
build="$1"; shift
status=0
bounded() { perl -e 'alarm shift; exec @ARGV' "$@"; }
run() { echo "== $*"; bounded "$@" 2>&1 | tail -n 25; rc=$?; [ "$rc" -eq 0 ] || { echo "exit $rc"; status=1; }; }
for c in lone_voter rows checkpoints close fence; do run 120 "$build/consensus_driver_test" "$c"; done
for c in lone_voter stalled needs_peer staged_then_cancelled cross_thread_cancel ownership; do run 120 "$build/consensus_blocking_entry_test" "$c"; done
for c in rows close close-read close-open close-late teardown threads; do run 600 "$build/consensus_waiter_test" "$c"; done
(cd "$build" && run 300 ./consensus_driver_idle 1000) || status=1
SIM_DRIVER_SEEDS="${SIM_DRIVER_SEEDS:-8}" run 600 "$build/consensus_simulation_test" 0 3000 1
exit $status
