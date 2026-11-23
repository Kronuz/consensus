#!/bin/bash
# Pauses the whole process of one rpc_host_test case (SIGSTOP, as a runner that stops it does) for
# SECONDS, at each offset from START to END by STEP seconds after its start, PARALLEL runs at once;
# fails if any run does.
#   pause.sh BUILD_DIR CASE SECONDS START END STEP PARALLEL
set -u
build=$1 case=$2 seconds=$3 start=$4 end=$5 step=$6 parallel=$7
ctest --test-dir "$build" -R consensus_tls_certificates > /dev/null || exit 2
out=$build/pause-$case
rm -rf "$out"
mkdir -p "$out"
run() {  # run BUILD CASE SECONDS OUT OFFSET
	"$1/consensus_transport_rpc_host_test" "$1/tls-fixture" "$2" > "$4/$5.log" 2>&1 &
	local pid=$!
	sleep "$5"
	kill -STOP $pid 2> /dev/null
	sleep "$3"
	kill -CONT $pid 2> /dev/null
	wait $pid
	echo $? > "$4/$5.rc"
}
export -f run
awk -v a="$start" -v b="$end" -v s="$step" 'BEGIN { for (x = a; x <= b + 1e-9; x += s) printf "%.3f\n", x }' |
	xargs -n1 -P "$parallel" bash -c 'run "$@"' _ "$build" "$case" "$seconds" "$out"
failed=0 total=0
for rc in "$out"/*.rc; do
	total=$((total + 1))
	if [ "$(cat "$rc")" != 0 ]; then
		failed=$((failed + 1))
		echo "offset $(basename "$rc" .rc) s: $(head -c 400 "${rc%.rc}.log")"
	fi
done
echo "$case paused ${seconds} s: $failed of $total start offsets failed"
[ "$failed" = 0 ]
