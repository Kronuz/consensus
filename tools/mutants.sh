#!/usr/bin/env bash
# Seeds a known fault into a copy of the core and runs the simulation harness against it:
# every mutant must be caught. Prints the seeds the harness needed for each.
#   tools/mutants.sh BUILD_DIR [seeds] [steps]
# BUILD_DIR is a configured build of this repository (for the dependencies' include paths).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
build="${1:?usage: mutants.sh BUILD_DIR [seeds] [steps]}"
seeds="${2:-2000}"
steps="${3:-2000}"
work="$build/mutants"
mkdir -p "$work"
# The harnesses' compile commands without their dependency and output flags: the
# randomized simulation, and the unit suite whose scripts force the rare schedules.
compile=$(ninja -C "$build" -t commands consensus_simulation_test | grep -- '-c .*simulation_test.cc' | sed 's/ -MD .*//')
compile_unit=$(ninja -C "$build" -t commands consensus_consensus_test | grep -- '-c .*consensus_test.cc' | sed 's/ -MD .*//')
compile_membership=$(ninja -C "$build" -t commands consensus_membership_test | grep -- '-c .*membership_test.cc' | sed 's/ -MD .*//')
# name, search, replace (tab-separated), applied to include/consensus/core.h. An unclamped
# follower commit is not among them: every append that reaches a divergent suffix replaces
# it in the same RPC, so the fault cannot be observed until coalesced heartbeats (Track C,
# D12) carry a commit without entries; it joins this list with them.
mutants=(
	$'sends before durability\tif (batch.hard || batch.log) { persist(std::move(batch), std::move(deferred), output); }\tif (batch.hard || batch.log) { for (auto& a : deferred) { output.push_back(a); } persist(std::move(batch), {}, output); }'
	$'votes and pre-votes without the log check\treturn term > log_term(last_index()) || (term == log_term(last_index()) && index >= last_index());\treturn true;'
	$'repair with a delivery outstanding\tif (!started_ || busy() || delivered_through_) { reject(RejectReason::Busy); return; }\tif (!started_ || busy()) { reject(RejectReason::Busy); return; }'
	$'membership change before the leader commits in its term\tif (durable_.commit_index == 0 || log_term(durable_.commit_index) != hard_.term ||\tif (durable_.commit_index == 0 ||'
	$'no membership rollback on a replaced suffix\tvoid note_truncate(Index from) { configs_.erase(configs_.lower_bound(from), configs_.end()); }\tvoid note_truncate(Index) {}'
	$'a removed leader counts itself\t\t\tif (is_voter(configuration_.local)) { matches.push_back(last_index()); }\t\t\tmatches.push_back(last_index());'
	$'a follower checks entries by its node limits, not by position\tif (!fits(entry, running)) {\tif (!fits(entry, limits_)) {'
	$'a leader grants pre-votes\treturn role_ == Role::Leader || (role_ == Role::Follower\treturn (role_ == Role::Follower'
	$'learners refuse votes\tbool grant = request.term == hard_.term && (!hard_.voted_for\tbool grant = request.term == hard_.term && is_voter(configuration_.local) && (!hard_.voted_for'
	$'repair below base_index\tif (b.index < base_index_ || b.index > durable_.commit_index ||\tif (b.index > durable_.commit_index ||'
	$'a leader ignores members\' node limits\t\t\tif (auto found = peer_limits_.find(member); found != peer_limits_.end()) { cap(found->second); }\t\t\t(void)member;'
	$'commit of an earlier term by counting\tif (candidate <= durable_.commit_index || log_term(candidate) != hard_.term) { return; }\tif (candidate <= durable_.commit_index) { return; }'
)
status=0
for m in "${mutants[@]}"; do
	IFS=$'\t' read -r name search replace <<<"$m"
	dir="$work/$(echo "$name" | tr ' ' '-')"
	rm -rf "$dir" && mkdir -p "$dir" && cp -R "$root/include" "$dir/include"
	python3 - "$dir/include/consensus/core.h" "$search" "$replace" <<'PY'
import sys
p, search, replace = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(p).read()
if search not in s: sys.exit("mutation point not found: " + search)
open(p, "w").write(s.replace(search, replace, 1))
PY
	# shellcheck disable=SC2086
	${compile/ -I/ -I$dir/include -I} "$root/test/simulation_test.cc" -o "$dir/simulation" 2>"$dir/build.log" || { echo "$name: build failed"; status=1; continue; }
	${compile_unit/ -I/ -I$dir/include -I} "$root/test/consensus_test.cc" -o "$dir/unit" 2>>"$dir/build.log" || { echo "$name: unit build failed"; status=1; continue; }
	${compile_membership/ -I/ -I$dir/include -I} "$root/test/membership_test.cc" -o "$dir/membership" 2>>"$dir/build.log" || { echo "$name: membership build failed"; status=1; continue; }
	# Each suite on its own, so the report shows whether the randomized harness catches the
	# fault, not only the first suite that does.
	unit=no; membership=no; simulation=no
	"$dir/unit" >"$dir/unit.log" 2>&1 || unit=yes
	"$dir/membership" >"$dir/membership.log" 2>&1 || membership=yes
	"$dir/simulation" "$seeds" "$steps" >"$dir/simulation.log" 2>&1 || simulation=yes
	first=""
	[[ $simulation == yes ]] && first=", first at $(grep -m1 -o '^seed [0-9]*' "$dir/simulation.log" || echo 'the heal')"
	echo "$name: unit $unit, membership $membership, simulation $simulation ($seeds seeds$first)"
	if [[ $unit == no && $membership == no && $simulation == no ]]; then status=1; fi
done
exit $status
