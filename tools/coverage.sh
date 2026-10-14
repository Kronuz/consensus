#!/usr/bin/env bash
# T8 coverage: every test but the mutants, in a build configured with
# -fprofile-instr-generate -fcoverage-mapping. Prints each header's line and branch
# coverage, unioned over the test binaries by strata's tools/coverage_union.py.
#   [COV_BUILD=dir] [COV_SINGLE_BYTE=1] [COV_CONFIRM=dir] [COV_LIST=1] tools/coverage.sh
# COV_SINGLE_BYTE=1 builds with single-byte coverage in its own directory; COV_CONFIRM=dir
# confirms this build's saturated counts with that build's run; COV_LIST=1 lists uncovered
# lines and outcomes. COV_UNION names coverage_union.py when strata is not a sibling.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
sbc=""
[[ "${COV_SINGLE_BYTE:-}" == 1 ]] && sbc=" -mllvm -enable-single-byte-coverage=true"
B="${COV_BUILD:-$root/build-cov${sbc:+-sbc}}"
LLVM="${LLVM_BIN:-/usr/local/opt/llvm/bin}"
UNION="${COV_UNION:-$root/../strata/tools/coverage_union.py}"
if [[ ! -f "$B/build.ninja" ]]; then
	cmake -S "$root" -B "$B" -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER="$LLVM/clang" -DCMAKE_CXX_COMPILER="$LLVM/clang++" \
		-DCMAKE_CXX_FLAGS="-O1 -fprofile-instr-generate -fcoverage-mapping$sbc" -DCMAKE_EXE_LINKER_FLAGS="-fprofile-instr-generate"
fi
cmake --build "$B"
rm -rf "$B/prof" && mkdir -p "$B/prof"
export LLVM_PROFILE_FILE="$B/prof/%p-%m.profraw"
(cd "$B" && ctest -j4 -E mutants | grep -E "tests passed|Failed")
unset LLVM_PROFILE_FILE
"$LLVM/llvm-profdata" merge -sparse "$B"/prof/*.profraw -o "$B/all.profdata"
binaries() { for b in "$1"/consensus_*_test "$1"/consensus_fuzz_decoders_replay; do echo "$b"; done; }
args=("$LLVM" "$B/all.profdata")
while read -r b; do args+=("$b"); done < <(binaries "$B")
if [[ -n "${COV_CONFIRM:-}" ]]; then
	args+=(--confirm "$COV_CONFIRM/all.profdata")
	while read -r b; do args+=("$b"); done < <(binaries "$COV_CONFIRM")
fi
[[ "${COV_LIST:-}" == 1 ]] && args+=(--list)
args+=(--headers /include/consensus/)
python3 "$UNION" "${args[@]}"
