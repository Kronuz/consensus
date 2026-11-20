#!/usr/bin/env python3
"""Writes a mutant of the RPC lane: a commit on top of REF whose only change is one seeded fault, on
no branch: one of the model's eight in rpc.h, whose rpc test case of the same name must fail, or
one of the lane on Bulk's, whose rpc_host_test case it names must fail
(docs/mechanisms/rpc-lane.md), run at it with `campaign job submit --fails-without --commit SHA`.

    test/transport/rpc_mutants.py REF NAME     prints the mutant commit's sha
    test/transport/rpc_mutants.py --list        names every mutant

Each edit is (search, replace) in rpc.h, or (file, search, replace), applied in order; each search
must occur exactly once."""
import os
import subprocess
import sys
import tempfile

PATH = "include/consensus/transport/rpc.h"
HOST = "include/consensus/transport/host_transport.h"
MUTANTS = {
    # frames for a call no longer live close the lane (DTFT0001's rule)
    "forget-is-violation": [("\t\t\t++late_; // rule 9: a retired id\n\t\t\treturn;",
                             "\t\t\tthrow ProtocolError(\"late\");")],
    # the first window is what was asked, not what the pool holds
    "grant-without-reserve": [(".window = c.in.limit,", ".window = c.in.window,")],
    # a close ends calls but keeps their bytes
    "no-release-on-close": [("\t\tif (c.in.held)\n\t\t\tc.in.pool->release(c.in.held);",
                             "\t\tif (c.in.held && status.outcome != Outcome::connection_aborted)\n\t\t\tc.in.pool->release(c.in.held);")],
    # credit only in the Call: a body past one window never finishes
    "no-regrant": [("\t\tif (from + held > in.limit) {", "\t\tif (false) {")],
    # credit renewed on arrival, not on consumption
    "grant-on-receive": [("\t\t\tin.received += n;\n", "\t\t\tin.received += n;\n\t\t\tregrant(c, in.received);\n")],
    # the consumed bytes go back to the pool, and the regrant takes from it in a second step
    "split-release": [("\t\tconst std::size_t held = in.pool->renew(in.held, wanted);",
                       "\t\tin.pool->release(in.held);\n\t\tconst std::size_t held = in.pool->renew(0, wanted);")],
    # the caller keeps calling after a Drain
    "drain-ignored": [("\t\tpeer_drained_ = true;\n\t\tnot_processed(id);", "\t\t(void)id;")],
    # ids allocated when a call is made, not when its Call is queued
    "id-at-call": [("\t\t\tc.called = true;\n\t\t\tallocate(c, h);", "\t\t\tc.called = true;"),
                   ("\t\tc.method = method;\n", "\t\tc.method = method;\n\t\tallocate(c, h);\n")],
    # group-full: lane frames wait while the groups' data holds the credit (one credit for both)
    "rpc-shared-credit": [(HOST, "rpc::Lane *calls = lane.link->lane == LinkLane::Bulk ? rpc_lane(k, *lane.link) : nullptr;",
                           "rpc::Lane *calls = lane.link->lane == LinkLane::Bulk && stats.uncredited + rpc::chunk_bytes + frame_header <= options_.window ? rpc_lane(k, *lane.link) : nullptr;")],
    # stream: a body goes in chunks that ignore the peer's credit
    "rpc-past-credit": [("\t\treturn std::min<std::uint64_t>(chunk_bytes, o.limit - o.sent);",
                         "\t\treturn chunk_bytes;")],
    # late-deadline (T7): a deadline found late ends the call, as if the loop had run
    "late-deadline": [("\t\t\tif (c.deadline <= now && options_.late && !c.extended)", "\t\t\tif (false)")],
    # hello_test version (review F3): the constructor takes a frame bound below the lane's frame
    "lane-frame-unchecked": [(HOST, "if (options_.links.rpc_max && options_.links.connection.maximum_frame < rpc::frame_bytes)",
                              "if (false)")],
    # rpc_host_test local-throw (review F7): a throw from this end's handler closes the connection
    "local-throw-closes": [("\t\t} catch (...) {\n\t\t\t++failed_;\n\t\t\tpools_.serving.release(held);",
                            "\t\t} catch (const ProtocolError &) {\n\t\t\t++failed_;\n\t\t\tpools_.serving.release(held);"),
                           ("\t\ttry {\n\t\t\tf();\n\t\t\treturn true;\n\t\t} catch (const std::exception &e) {\n\t\t\tfail(h, e.what());\n\t\t} catch (...) {",
                            "\t\ttry {\n\t\t\tf();\n\t\t\treturn true;\n\t\t} catch (const ProtocolError &e) {\n\t\t\tfail(h, e.what());\n\t\t} catch (const std::bad_alloc &) {")],
    # queue-cap, rpc_host_test stalled-peer (A3): no per-connection cap on queued frames
    "rpc-no-queue-cap": [("\t\treturn !(queued_ + reserved_) || queued_ + reserved_ + size <= options_.queue_bytes;",
                          "\t\treturn true;")],
    # reenter (N3, under ASan): a close() inside a callback runs at once
    "close-reenters": [("\t\tif (depth_) {\n\t\t\tclose_wanted_ = true;", "\t\tif (false) {\n\t\t\tclose_wanted_ = true;")],
    # unread-queue (N1): refusals pile up unsent without bound
    "refusals-unbounded": [("\t\t\tif (refusals_ > options_.serving)", "\t\t\tif (false)")],
    # overrun (N2): send takes bytes past a known total when they do not end it
    "send-past-total": [("(bytes.size() > o.total - o.sent || end != (o.sent + bytes.size() == o.total))",
                         "end != (o.sent + bytes.size() == o.total)")],
    # hello_test leader: the transport reports default limits, not the ones the peer's hello carried
    "hello-limits-ignored": [(HOST, "here.report(route.group, {route.source, limits});",
                              "here.report(route.group, {route.source, kronuz::consensus::Limits{}});")],
}


def git(*args, env=None, data=None):
    return subprocess.run(["git", *args], check=True, capture_output=True, env=env, input=data).stdout.decode().strip()


def main():
    if sys.argv[1:] == ["--list"]:
        print("\n".join(MUTANTS))
        return 0
    ref, name = sys.argv[1], sys.argv[2]
    texts = {}
    for edit in MUTANTS[name]:
        path, search, replace = edit if len(edit) == 3 else (PATH, *edit)
        text = texts.setdefault(path, git("show", f"{ref}:{path}") + "\n")
        if text.count(search) != 1:
            sys.exit(f"{name}: mutation point found {text.count(search)} times in {path}")
        texts[path] = text.replace(search, replace)
    with tempfile.TemporaryDirectory(dir=os.path.join(git("rev-parse", "--git-common-dir"))) as tmp:
        env = dict(os.environ, GIT_INDEX_FILE=os.path.join(tmp, "index"))
        git("read-tree", ref, env=env)
        for path, text in texts.items():
            blob = git("hash-object", "-w", "--stdin", data=text.encode())
            git("update-index", "--cacheinfo", f"100644,{blob},{path}", env=env)
        tree = git("write-tree", env=env)
    print(git("commit-tree", tree, "-p", git("rev-parse", ref), "-m", f"Mutant {name}: {', '.join(texts)}"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
