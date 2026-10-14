#!/usr/bin/env bash
# T8: one CPU-hour (or SECONDS each) per decoder, each pinned with FUZZ_DECODER, four at a
# time, from the checked-in corpus. Prints each decoder's executions and any crash.
#   tools/fuzz.sh FUZZ_BUILD_DIR [SECONDS]
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
build="$(cd "${1:?usage: fuzz.sh FUZZ_BUILD_DIR [SECONDS]}" && pwd)"
seconds="${2:-3600}"
names=(control membership founding join batch checkpoint snapshot)
run() {
	local k=$1 dir="$build/fuzz-$1"
	mkdir -p "$dir/corpus"
	cp "$root"/fuzz/corpus/decoders/* "$dir/corpus/"
	(cd "$dir" && FUZZ_DECODER=$k "$build/consensus_fuzz_decoders" corpus -max_total_time="$seconds" -max_len=8192 -print_final_stats=1 >log 2>&1 || true)
	echo "${names[$k]}: $(grep -m1 -o 'stat::number_of_executed_units: *[0-9]*' "$dir/log" | grep -o '[0-9]*$') executions, $(ls "$dir" | grep -cE '^(crash|oom|timeout|leak)-' || true) crashes"
}
export -f run; export build root seconds; export names_list="${names[*]}"
for k in 0 1 2 3 4 5 6; do echo "$k"; done | xargs -P4 -I{} bash -c 'names=($names_list); run {}'
