#!/usr/bin/env bash
# Track C, T1: consensus b7073bb (Detent's pin) must refuse every format Track C added,
# never misread one. Builds b7073bb in a worktree with its pinned dependencies, builds
# tools/compat_old_reader.cc against it, and runs it on test/fixtures/c1 (written by
# compat_test --write).
#   tools/compat.sh BUILD_DIR
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
build="${1:?usage: compat.sh BUILD_DIR}"
old="$build/compat-b7073bb"
if [[ ! -d "$old" ]]; then git -C "$root" worktree add -q --detach "$old" b7073bb; fi
[[ -f "$old/build/build.ninja" ]] || cmake -S "$old" -B "$old/build" -G Ninja -DCMAKE_BUILD_TYPE=Release >/dev/null
D="$old/build/_deps"
c++ -std=c++20 -O1 -I"$old/include" -I"$D/journal-src/include" -I"$D/completion-src/include" -I"$D/metrics-src" -I"$D/metrics-src/include" \
	"$root/tools/compat_old_reader.cc" -o "$build/compat_old_reader"
"$build/compat_old_reader" "$root/test/fixtures/c1"
