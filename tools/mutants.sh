#!/usr/bin/env bash
# Seeds a known fault into a copy of the core and runs the simulation harness against it:
# every mutant must be caught. Prints the seeds the harness needed for each.
#   tools/mutants.sh BUILD_DIR [seeds] [steps] [name]
#   tools/mutants.sh --list
#   tools/mutants.sh --check DIR
# With a name (the mutant's lower-case, dash-separated slug) it runs that mutant alone;
# --list prints the slugs; --check applies every mutant into DIR and fails when a pattern does
# not match exactly once (a mutant that no longer applies proves nothing).
# BUILD_DIR is a configured build of this repository (for the dependencies' include paths).
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
slug() { echo "$1" | tr 'A-Z' 'a-z' | sed 's/[^a-z0-9][^a-z0-9]*/-/g; s/^-//; s/-$//'; }
if [[ ${1:-} != --list && ${1:-} != --check ]]; then
	build="${1:?usage: mutants.sh BUILD_DIR [seeds] [steps] [name]}"
	seeds="${2:-2000}"
	steps="${3:-2000}"
	only="${4:-}"
	work="$build/mutants"
	mkdir -p "$work"
	# The harnesses' compile commands without their dependency and output flags: the
	# randomized simulation, and the unit suite whose scripts force the rare schedules.
	compile=$(ninja -C "$build" -t commands consensus_simulation_test | grep -- '-c .*simulation_test.cc' | sed 's/ -MD .*//')
	compile_unit=$(ninja -C "$build" -t commands consensus_consensus_test | grep -- '-c .*consensus_test.cc' | sed 's/ -MD .*//')
	compile_membership=$(ninja -C "$build" -t commands consensus_membership_test | grep -- '-c .*membership_test.cc' | sed 's/ -MD .*//')
fi
# name, search, replace (tab-separated), applied to include/consensus/core.h. An unclamped
# follower commit is not among them: every append that reaches a divergent suffix replaces
# it in the same RPC, so the fault cannot be observed until coalesced heartbeats (Track C,
# D12) carry a commit without entries; it joins this list with them.
mutants=(
	$'recovered delivery byte accounting underflows\tbytes > limits_.rpc_bytes || \t'
	$'sends before durability\tif (batch.hard || batch.log) { persist(std::move(batch), std::move(deferred), output); }\tif (batch.hard || batch.log) { for (auto& a : deferred) { output.push_back(a); } persist(std::move(batch), {}, output); }'
	$'votes and pre-votes without the log check\treturn term > log_term(last_index()) || (term == log_term(last_index()) && index >= last_index());\treturn true;'
	$'repair with a delivery outstanding\tif (!started_ || busy() || delivered_through_) { reject(RejectReason::Busy); return; }\tif (!started_ || busy()) { reject(RejectReason::Busy); return; }'
	$'membership change before the leader commits in its term\tif (durable_.commit_index == 0 || log_term(durable_.commit_index) != hard_.term ||\tif (durable_.commit_index == 0 ||'
	$'no membership rollback on a replaced suffix\tvoid note_truncate(Index from) { configs_.erase(configs_.lower_bound(from), configs_.end()); ++membership_version_; }\tvoid note_truncate(Index) { ++membership_version_; }'
	$'a removed leader counts itself\t\t\tif (is_voter(configuration_.local)) { matches.push_back(last_index()); }\t\t\tmatches.push_back(last_index());'
	$'a follower checks entries by its node limits, not by position\tif (!fits(entry, running)) {\tif (!fits(entry, limits_)) {'
	$'a leader grants pre-votes\treturn role_ == Role::Leader || (role_ == Role::Follower\treturn (role_ == Role::Follower'
	$'learners refuse votes\tbool grant = request.term == hard_.term && (!hard_.voted_for\tbool grant = request.term == hard_.term && is_voter(configuration_.local) && (!hard_.voted_for'
	$'repair ignores the term at the base boundary\t(b.index == base_index_ ? b.term != base_term_ : log_term(b.index) != b.term)\t(b.index == base_index_ ? false : log_term(b.index) != b.term)'
	$'repair above the commit index\tif (b.index < base_index_ || b.index > durable_.commit_index ||\tif (b.index < base_index_ ||'
	$'repair below base_index\tif (b.index < base_index_ || b.index > durable_.commit_index ||\tif (b.index > durable_.commit_index ||'
	$'a leader ignores members\' node limits\t\t\tif (auto found = peer_limits_.find(member); found != peer_limits_.end()) { cap(found->second); }\t\t\t(void)member;'
	$'commit of an earlier term by counting\tif (candidate <= durable_.commit_index || log_term(candidate) != hard_.term) { return; }\tif (candidate <= durable_.commit_index) { return; }'
	$'a follower ignores LeaderGone\t\tfollower(0, output); leader_contact_ = 0;\t\t(void)output;'
	$'a read counts every acknowledgement\treturn std::size_t(std::count_if(read.acknowledgements.begin(), read.acknowledgements.end(), [&](NodeId id) { return is_voter(id); }));\treturn read.acknowledgements.size();'
)
# Applies one mutant to core.h of the include copy in $1: the search must match exactly once.
apply_mutant() {
	python3 - "$1/include/consensus/core.h" "$2" "$3" <<'PY'
import sys
p, search, replace = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(p).read()
if s.count(search) != 1: sys.exit("mutation point not found once: " + search)
open(p, "w").write(s.replace(search, replace, 1))
PY
}
# Runs a suite for at most $1 seconds; a mutant that hangs a suite fails it.
bounded() {
	local limit=$1; shift
	"$@" & local pid=$!
	(for ((i = 0; i < limit; ++i)); do kill -0 "$pid" 2>/dev/null || exit 0; sleep 1; done; kill -9 "$pid") &
	local rc=0; wait "$pid" || rc=$?
	wait
	return "$rc"
}
if [[ ${1:-} == --list ]]; then
	for m in "${mutants[@]}"; do slug "${m%%$'\t'*}"; done
	exit 0
fi
if [[ ${1:-} == --check ]]; then
	dir="${2:?usage: mutants.sh --check DIR}"
	failed=0
	for m in "${mutants[@]}"; do
		IFS=$'\t' read -r name search replace <<<"$m"
		rm -rf "$dir" && mkdir -p "$dir" && cp -R "$root/include" "$dir/include"
		apply_mutant "$dir" "$search" "$replace" 2>&1 || { echo "$name: pattern does not apply"; failed=1; }
	done
	rm -rf "$dir"
	[[ $failed == 0 ]] && echo "all ${#mutants[@]} mutants apply"
	exit "$failed"
fi
status=0
ran=0
for m in "${mutants[@]}"; do
	IFS=$'\t' read -r name search replace <<<"$m"
	[[ -z $only || $only == "$(slug "$name")" ]] || continue
	ran=$((ran + 1))
	dir="$work/$(slug "$name")"
	rm -rf "$dir" && mkdir -p "$dir" && cp -R "$root/include" "$dir/include"
	apply_mutant "$dir" "$search" "$replace"
	# shellcheck disable=SC2086
	${compile/ -I/ -I$dir/include -I} "$root/test/simulation_test.cc" "$root/test/driver_simulation.cc" -o "$dir/simulation" 2>"$dir/build.log" || { echo "$name: build failed"; status=1; continue; }
	${compile_unit/ -I/ -I$dir/include -I} "$root/test/consensus_test.cc" -o "$dir/unit" 2>>"$dir/build.log" || { echo "$name: unit build failed"; status=1; continue; }
	${compile_membership/ -I/ -I$dir/include -I} "$root/test/membership_test.cc" -o "$dir/membership" 2>>"$dir/build.log" || { echo "$name: membership build failed"; status=1; continue; }
	# Each suite on its own, so the report shows whether the randomized harness catches the
	# fault, not only the first suite that does.
	unit=no; membership=no; simulation=no
	bounded 600 "$dir/unit" >"$dir/unit.log" 2>&1 || unit=yes
	bounded 600 "$dir/membership" >"$dir/membership.log" 2>&1 || membership=yes
	bounded 3600 "$dir/simulation" "$seeds" "$steps" >"$dir/simulation.log" 2>&1 || simulation=yes
	first=""
	[[ $simulation == yes ]] && first=", first at $(grep -m1 -o '^seed [0-9]*' "$dir/simulation.log" || echo 'the heal')"
	echo "$name: unit $unit, membership $membership, simulation $simulation ($seeds seeds$first)"
	if [[ $unit == no && $membership == no && $simulation == no ]]; then status=1; fi
done
if [[ $ran == 0 ]]; then echo "no mutant named $only"; exit 1; fi
exit $status
