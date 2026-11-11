#!/usr/bin/env bash
# Seeds one fault into a copy of the transport headers, rebuilds the peers test against it
# and runs it: exits 0 when the test catches the fault (fails or hangs), 1 when it passes.
#   test/transport/dns_mutants.sh BUILD_DIR no-deadline|shared-thread
# BUILD_DIR is a configured Ninja build of this repository with the peers test built.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
build="$(cd "${1:?usage: dns_mutants.sh BUILD_DIR MUTANT}" && pwd)"
name="${2:?usage: dns_mutants.sh BUILD_DIR MUTANT}"
# name -> file, then search/replace pairs (tab-separated).
case "$name" in
no-deadline) # a dial awaits its lookup with no deadline
	file=peers.h
	edits=($'auto timer = std::make_shared<asio::steady_timer>(executor_, route.answer_by);\tauto timer = std::make_shared<asio::steady_timer>(executor_, std::chrono::steady_clock::time_point::max());') ;;
shared-thread) # every lookup queues on one shared resolver thread, as asio's async_resolve does
	file=resolve.h
	edits=($'#include <thread>\n\t#include <thread>\n#include <asio/post.hpp>\n#include <asio/thread_pool.hpp>\n'
		$'std::thread(std::move(work)).detach();\tstatic asio::thread_pool shared(1); asio::post(shared, std::move(work));') ;;
*) echo "unknown mutant: $name" >&2; exit 2 ;;
esac
work="$build/dns-mutants/$name"
rm -rf "$work" && mkdir -p "$work" && cp -R "$root/include" "$work/include"
for edit in "${edits[@]}"; do
	python3 - "$work/include/consensus/transport/$file" "$edit" <<'PY'
import sys
path, edit = sys.argv[1], sys.argv[2]
search, replace = edit.split("\t", 1)
search, replace = search.replace("\\n", "\n"), replace.replace("\\n", "\n")
text = open(path).read()
if search not in text:
    sys.exit("mutation point not found: " + search)
open(path, "w").write(text.replace(search, replace, 1))
PY
done
commands=$(ninja -C "$build" -t commands consensus_transport_peers_test)
compile=$(grep -- '-c .*peers_test.cc' <<<"$commands")
link=$(grep -- '-o consensus_transport_peers_test' <<<"$commands" | tail -1)
object=$(sed -E 's/.* -o ([^ ]+) -c .*/\1/' <<<"$compile")
compile=$(sed -E "s# -MD -MT [^ ]+ -MF [^ ]+##; s# -o [^ ]+ -c # -o $work/peers_test.o -c #" <<<"$compile")
link="${link/$object/$work/peers_test.o}"
link="${link/-o consensus_transport_peers_test/-o $work/peers_test}"
cd "$build"
eval "${compile/ -I/ -I$work/include -I}" 2>"$work/build.log" || { echo "$name: build failed"; cat "$work/build.log"; exit 2; }
eval "$link" 2>>"$work/build.log" || { echo "$name: link failed"; cat "$work/build.log"; exit 2; }
"$work/peers_test" >"$work/test.log" 2>&1 &
pid=$!
for ((i = 0; i < 120; ++i)); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
kill -9 "$pid" 2>/dev/null || true
rc=0; wait "$pid" || rc=$?
tail -5 "$work/test.log"
if [[ $rc -ne 0 ]]; then echo "$name: caught (exit $rc)"; exit 0; fi
echo "$name: NOT caught"; exit 1
