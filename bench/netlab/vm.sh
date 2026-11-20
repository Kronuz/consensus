#!/usr/bin/env bash
# Copyright © 2026 Germán Méndez Bravo (Kronuz)
# SPDX-License-Identifier: MIT
#
# bench/netlab's VM side, from a consensus checkout's root, inside a campaign job:
#   vm.sh build                                   node.cc built in a guest, cached by its sources' trees
#   vm.sh run --nodes N [--quick] [--shapes A,B]  bench.py in a guest, every node a netlab namespace
# One guest of Xapiand's VM tier (tools/vm/cluster.sh up --nodes 1) holds the whole cluster: netlab's
# veths and netem shape the links in its kernel, and the nodes share one CLOCK_MONOTONIC. The guest
# is parked on every exit (our trap, and the guardian when this shell dies). Results go to $JOB_OUT.
set -euo pipefail
what=${1:?build or run}
shift
nodes=3
args=()
while [ $# -gt 0 ]; do
	case $1 in
		--nodes) nodes=$2; shift 2 ;;
		--quick) args+=(--quick); shift ;;
		--shapes) args+=(--shapes "$2"); shift 2 ;;
		*) echo "vm.sh: unknown option $1" >&2; exit 2 ;;
	esac
done
XAPIAND=${XAPIAND:-$HOME/code/Kronuz/Xapiand}
CLUSTER="$XAPIAND/tools/vm/cluster.sh"
CACHE=${NETLAB_CACHE:-$HOME/.scratch/netlab-consensus}
CPUS=${NETLAB_CPUS:-8}
MB=${NETLAB_MB:-8192}
key=$(git rev-parse HEAD:bench/netlab/node.cc HEAD:bench/netlab/node.cmake HEAD:include HEAD:CMakeLists.txt | shasum | cut -c1-16)
bin=$CACHE/bin/$key/consensus_netlab_node
rev=$(cut -c1-40 bench/netlab/FAULTLAB_REV)
faultlab=$CACHE/faultlab-$rev
out=${JOB_OUT:-$CACHE/results/$(date +%y%m%d-%H%M%S)}

if [ "$what" = build ] && [ -x "$bin" ]; then
	echo "vm.sh: $bin is built (sources $key)"
	exit 0
fi
if [ "$what" = run ] && [ ! -x "$bin" ]; then
	echo "vm.sh: no $bin: run vm.sh build first" >&2
	exit 2
fi
if [ "$(git -C "$faultlab" rev-parse HEAD 2>/dev/null)" != "$rev" ]; then
	rm -rf "$faultlab"
	git clone -q "${FAULTLAB_REPO:-https://github.com/Kronuz/faultlab.git}" "$faultlab"
	git -C "$faultlab" checkout -q "$rev"
fi

started=$(date +%s)
CLUSTER_VM_RUN=$("$CLUSTER" up --nodes 1 --cpus "$CPUS" --mb "$MB" --owner $$)
export CLUSTER_VM_RUN
trap '"$CLUSTER" down >/dev/null 2>&1 || true' EXIT
trap 'exit 143' INT TERM HUP
guest() { "$CLUSTER" ssh 1 "$@"; }
echo "guest up in $(($(date +%s) - started)) s: $CLUSTER_VM_RUN ($CPUS CPUs, $MB MB; Xapiand $(git -C "$XAPIAND" rev-parse --short HEAD))"

case $what in
	build)
		guest 'sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq g++ cmake ninja-build libssl-dev git >/dev/null'
		git archive HEAD | guest 'rm -rf ~/src && mkdir ~/src && tar -xf - -C ~/src'
		guest 'set -e; cmake -S ~/src -B ~/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_PROJECT_consensus_INCLUDE=$HOME/src/bench/netlab/node.cmake >/dev/null
			ninja -C ~/build consensus_netlab_node | tail -1; file ~/build/consensus_netlab_node; ldd ~/build/consensus_netlab_node'
		mkdir -p "$(dirname "$bin")"
		guest 'cat ~/build/consensus_netlab_node' >"$bin.part"
		chmod +x "$bin.part"
		mv "$bin.part" "$bin"
		echo "vm.sh: built $bin (sources $key) in $(($(date +%s) - started)) s"
		;;
	run)
		mkdir -p "$out"
		tar -C "$(dirname "$bin")" -cf - consensus_netlab_node | guest 'rm -rf ~/nlc && mkdir -p ~/nlc && tar -xf - -C ~/nlc'
		tar -C bench/netlab -cf - bench.py | guest 'tar -xf - -C ~/nlc'
		tar -C "$faultlab" -cf - python/faultlab | guest 'mkdir -p ~/nlc/faultlab && tar -xf - -C ~/nlc/faultlab'
		guest 'nproc; uname -sr; python3 --version; file ~/nlc/consensus_netlab_node | cut -d, -f1-2'
		status=0
		guest "sudo python3 -I ~/nlc/bench.py run --faultlab ~/nlc/faultlab --node ~/nlc/consensus_netlab_node --nodes $nodes ${args[*]-} --data /dev/shm/netlab-consensus --out ~/nlc/out" || status=$?
		guest 'sudo tar -C ~/nlc/out -cf - .' | tar -xf - -C "$out" || true
		guest 'sudo tar -C /dev/shm/netlab-consensus -cf - stderr' | tar -xf - -C "$out" || true
		echo "vm.sh: $nodes nodes, exit $status, results in $out, $(($(date +%s) - started)) s"
		exit $status
		;;
	*) echo "vm.sh: build or run" >&2; exit 2 ;;
esac
