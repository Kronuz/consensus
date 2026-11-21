#!/usr/bin/env bash
# The RPC lane's fuzz target (fuzz/rpc_fuzz.cc) from its checked-in corpus, two workers, for
# SECONDS (one CPU-hour at 1800). -max_len covers the lane's largest frame, 65,584 bytes.
#   fuzz/rpc_fuzz.sh FUZZ_BUILD_DIR [SECONDS]
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
build="$(cd "${1:?usage: rpc_fuzz.sh FUZZ_BUILD_DIR [SECONDS]}" && pwd)"
seconds="${2:-1800}"
dir="$build/fuzz-rpc"
mkdir -p "$dir/corpus"
cp "$root"/fuzz/corpus/rpc/* "$dir/corpus/"
cd "$dir"
"$build/consensus_fuzz_rpc" corpus -fork=2 -max_total_time="$seconds" -max_len=65600 -print_final_stats=1 >log 2>&1 || true
crashes=$(ls | grep -cE '^(crash|oom|timeout|leak)-' || true)
echo "rpc: $(grep -o 'exec/s: [0-9]*' log | tail -1), $(grep -c '^#' log || true) status lines, $crashes crashes"
tail -5 log
test "$crashes" = 0
