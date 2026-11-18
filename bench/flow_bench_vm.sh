#!/usr/bin/env bash
# Runs bench/flow_bench.cc's rows in the Linux gate VM, with netem on lo for the round trip
# (docs/perf/flow-control.md): flow_bench_vm.sh smoke|a1|a2|a3, from a checkout's root.
# The VM is left as found: a job that started it stops it. The VM builds this commit once.
set -euo pipefail
VM="${FLOW_VM:-$HOME/.scratch/vm-document-gate/vm.sh} linux"
what=$1
commit=$(git rev-parse HEAD)
started=0
if [[ $($VM status) != running ]]; then
	$VM start
	started=1
fi
cleanup() {
	$VM ssh 'sudo tc qdisc del dev lo root 2>/dev/null; true' || true
	if ((started)); then $VM stop; fi
}
trap cleanup EXIT
if [[ $($VM ssh 'cat ~/consensus-flow-build/COMMIT 2>/dev/null || true') != "$commit" ]]; then
	git archive --prefix=consensus-flow/ HEAD | $VM ssh 'rm -rf ~/consensus-flow && tar -xf - -C ~'
	$VM ssh 'set -e; cd ~/consensus-flow; cmake -S . -B ~/consensus-flow-build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_PROJECT_consensus_INCLUDE=$PWD/bench/flow_bench.cmake >/dev/null
		ninja -C ~/consensus-flow-build consensus_flow_bench | tail -1
		cd ~/consensus-flow-build && ctest -R consensus_tls_certificates >/dev/null'
	$VM ssh "echo $commit > ~/consensus-flow-build/COMMIT"
fi
$VM ssh "bash -s $what" <<'REMOTE'
set -euo pipefail
B=~/consensus-flow-build/consensus_flow_bench F=~/consensus-flow-build/tls-fixture D=~/flow-data
echo "commit $(cat ~/consensus-flow-build/COMMIT); $(uname -srm); $(nproc) CPUs; $(file -b $B | cut -d, -f1-2)"
sudo modprobe sch_netem 2>/dev/null || true
net() {  # RTT_MS [RATE]: half the round trip each way on lo
	sudo tc qdisc del dev lo root 2>/dev/null || true
	if [[ $1 != 0 || -n ${2:-} ]]; then
		sudo tc qdisc add dev lo root netem delay "$(awk "BEGIN{print $1/2}")ms" ${2:+rate $2} limit 10000
	fi
	echo "== rtt $1 ms ${2:+rate $2}: $(tc qdisc show dev lo | head -1)"
}
started=$(date +%s)
case $1 in
smoke)
	net 10
	$B barrier $D 1
	$B one $F $D 4 262144 262144 1
	$B many $F $D 10 1 1
	$B gap $F control 262144 1
	$B gap $F bulk 262144 1;;
a1)
	$B barrier $D 3
	for rtt in 0 1 10; do
		net $rtt
		for t in 1 2 3; do
			$B one $F $D 1 262144 262144 3
			$B one $F $D 4 262144 262144 3
		done
		$B one $F $D 1 0 262144 3
		$B one $F $D 4 0 262144 3
	done;;
a2)
	for rtt in 1 10; do
		net $rtt
		for t in 1 2 3; do
			$B many $F $D 1 1 3
			$B many $F $D 10 1 3
		done
		$B many $F $D 1 0 3
		$B many $F $D 10 0 3
	done;;
a3)
	for rtt in 0 1 10; do
		for rate in "" 1gbit; do
			net $rtt $rate
			$B gap $F none 262144 3
			$B gap $F control 2097152 3
			$B gap $F control 262144 3
			$B gap $F bulk 2097152 3
		done
	done;;
esac
echo "done in $(( $(date +%s) - started )) s"
REMOTE
