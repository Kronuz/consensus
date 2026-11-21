#!/usr/bin/env bash
# Which of the call table's waits a corpus reaches (rpc-lane-codec review N4): replays CORPUS
# through a coverage build of the fuzz target and prints the execution count of each line named
# below, from rpc.h. Needs LLVM's clang, llvm-profdata and llvm-cov (LLVM=/usr/local/opt/llvm).
#   fuzz/rpc_coverage.sh CORPUS
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
corpus="$(cd "${1:?usage: rpc_coverage.sh CORPUS}" && pwd)"
llvm="${LLVM:-/usr/local/opt/llvm}/bin"
out="$root/.scratch/rpc-coverage"
mkdir -p "$out"
"$llvm/clang++" -std=c++20 -O1 -fprofile-instr-generate -fcoverage-mapping -I"$root/include" \
	$(for d in "$root"/build/_deps/*-src/include "$root"/.scratch/*/_deps/*-src/include; do [ -d "$d" ] && printf -- '-I%s ' "$d"; done) \
	"$root/fuzz/rpc_fuzz.cc" "$root/fuzz/replay.cc" -o "$out/replay"
LLVM_PROFILE_FILE="$out/rpc.profraw" "$out/replay" "$corpus" | tail -1
"$llvm/llvm-profdata" merge -o "$out/rpc.profdata" "$out/rpc.profraw"
"$llvm/llvm-cov" show "$out/replay" -instr-profile="$out/rpc.profdata" "$root/include/consensus/transport/rpc.h" >"$out/rpc.txt"
for what in 'ticket = ++tickets_;' 'serve(wakes);' 'granted(h, pool, n);' 'rule 7.6: past credit' \
	'c.roomless = true;' 'reserved_ += c.reserved = size;' 'started(h, n);' 'the peer does not read its refusals' \
	'std::optional<Clock::time_point> first;'; do
	line=$(grep -F -m1 -- "$what" "$out/rpc.txt" || true)
	printf '%-44s %s\n' "$what" "$(echo "$line" | awk -F'|' '{gsub(/ /, "", $2); print ($2 == "" ? "?" : $2)}')"
done
