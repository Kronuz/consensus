#!/bin/bash
# Runs one case of rpc_host_test N times, PARALLEL at once, and fails if any run does: the repeat
# counts of a flake, plain or under TSan (any build of the test).
#   repeat.sh BUILD_DIR CASE N PARALLEL
set -u
build=$1 case=$2 n=$3 parallel=$4
ctest --test-dir "$build" -R consensus_tls_certificates > /dev/null || exit 2
out=$build/repeat-$case
rm -rf "$out"
mkdir -p "$out"
run() {  # run BUILD CASE OUT I
	"$1/consensus_transport_rpc_host_test" "$1/tls-fixture" "$2" > "$3/$4.log" 2>&1
	echo $? > "$3/$4.rc"
}
export -f run
seq 1 "$n" | xargs -n1 -P "$parallel" bash -c 'run "$@"' _ "$build" "$case" "$out"
failed=0
for rc in "$out"/*.rc; do
	if [ "$(cat "$rc")" != 0 ]; then
		failed=$((failed + 1))
		head -c 600 "${rc%.rc}.log"
	fi
done
echo "$case: $failed of $n runs failed, $parallel at once"
for log in "$out"/*.log; do
	grep -ho "longest gap [0-9]* ms\|largest latency [0-9]* ms of [0-9]* allowed" "$log"
done | sort | uniq -c | sort -k3 -n | tail -3
[ "$failed" = 0 ]
