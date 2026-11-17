#!/usr/bin/env python3
"""Writes a mutant: a commit on top of REF whose only change is one seeded fault, on no branch.
The plain test run at it with `campaign job submit --fails-without --commit SHA` proves the test
catches the fault.

    test/transport/mutant_commit.py REF NAME       prints the mutant commit's sha
    test/transport/mutant_commit.py --list          names every mutant

Each mutant is (file, search, replace), or a list of them; each search must occur exactly once."""
import os
import subprocess
import sys
import tempfile

T = "include/consensus/transport/"
MUTANTS = {
    # deadline.h (A1)
    "no-extension": (T + "deadline.h", "if (!self.extended_ && late > wheel::timer_wheel::tick) {", "if (false) {"),
    "uncapped": (T + "deadline.h", "self.extended_ = std::min(late, self.timeout_);", "self.extended_ = late;"),
    "twice": (T + "deadline.h", "if (!self.extended_ && late > wheel::timer_wheel::tick) {", "if (late > wheel::timer_wheel::tick) {"),
    "wake-ignores-extension": (T + "deadline.h", "\t\tif (state_ != outcome::pending)\n\t\t\treturn;\n\t\tstate_ = outcome::woken;",
                               "\t\tif (state_ != outcome::pending || extended_)\n\t\t\treturn;\n\t\tstate_ = outcome::woken;"),
    "entry-left-armed": (T + "deadline.h", "\t\tif (armed())\n\t\t\tloop_.cancel(*this);\n\t\tleave();", "\t\tleave();"),
    # node_pins.h (A2)
    "pin-any-node": (T + "node_pins.h", "if (expected && found != expected)", "if (false)"),
    "pin-self": (T + "node_pins.h", "if (found == local_)", "if (false)"),
    "pin-unpinned": (T + "node_pins.h", "\t\tif (!found)\n", "\t\tif (false)\n"),
    # connection.h (A4, T8's transport half)
    "close-drops-queue": (T + "connection.h", "\t\tclosing_ = true;\n\t\twake_writer();", "\t\tclosing_ = true;\n\t\tout_.clear();\n\t\twake_writer();"),
    "no-close-notify": (T + "connection.h", "if (end_ == ConnectionEnd::open) {\n\t\t\t\tfor (int r;", "if (false) {\n\t\t\t\tfor (int r;"),
    "no-keepalive": (T + "connection.h", "\t\t\t\t\tput_length(0);\n", "\t\t\t\t\t(void)0;\n"),
    # connection.h (security review S1, N2)
    "fail-closes-fd": (T + "connection.h", "\t\t\t::shutdown(socket_.native_handle(), SHUT_RDWR);\n\t\t\tsocket_.cancel(ignored);",
                       "\t\t\tsocket_.close(ignored);"),
    "ssl-after-end": (T + "connection.h", "if (end_ != ConnectionEnd::open)\n\t\t\tthrow", "if (false)\n\t\t\tthrow"),
    # one stuck deadline for all of a flush's waits, not one per wait
    "stuck-spans-flush": [
        (T + "connection.h", "\t\twhile (sent_ != sending_.size()) {", "\t\tlate_deadline whole(loop_, options_.dead_after);\n\t\tspan_ = &whole;\n\t\twhile (sent_ != sending_.size()) {"),
        (T + "connection.h", "\t\tstd::string().swap(sending_);  // an idle", "\t\tspan_ = nullptr;\n\t\tstd::string().swap(sending_);  // an idle"),
        (T + "connection.h", "\t\tlate_deadline stuck(loop_, options_.dead_after);\n\t\ttry {",
         "\t\tlate_deadline own(loop_, options_.dead_after);\n\t\tlate_deadline &stuck = span_ ? *span_ : own;\n\t\ttry {"),
        (T + "connection.h", "\tstd::size_t sent_ = 0;", "\tlate_deadline *span_ = nullptr;\n\tstd::size_t sent_ = 0;"),
    ],
    # connection.h (transport-groups review F1, F4, F5: a draining queue keeps a write wait alive)
    "stuck-ignores-drain": (T + "connection.h", "|| !drained(before))", "|| true)"),
    "stuck-never-fails": (T + "connection.h", "|| !drained(before))", "|| false)"),
    "stuck-equal-drains": (T + "connection.h", "return before && now && *now < *before;", "return before && now && *now <= *before;"),
    # the queue sampled when the flush begins, before its own writes (round 2's sampling point)
    "stuck-samples-before-flush": [
        (T + "connection.h", "\t\twhile (sent_ != sending_.size()) {", "\t\tflush_queued_ = unacknowledged();\n\t\twhile (sent_ != sending_.size()) {"),
        (T + "connection.h", "\t\tconst auto before = unacknowledged();", "\t\tconst auto before = flush_queued_;"),
        (T + "connection.h", "\tstd::size_t sent_ = 0;", "\tstd::optional<std::size_t> flush_queued_;\n\tstd::size_t sent_ = 0;"),
    ],
    # deadline.h, connection.h (fix round 2: a late keepalive goes at once)
    "late-always-extends": (T + "deadline.h", "if (self.extend_late_ && !self.extended_", "if (!self.extended_"),
    "keepalive-extends": (T + "connection.h", "late_deadline idle(loop_, options_.keepalive, false);",
                          "late_deadline idle(loop_, options_.keepalive);"),
    # links.h (A4's tie-break, A1's lookup)
    "refuse-when-offered": (T + "links.h", "return !offered || peer < pins_.local();", "return !offered;"),
    "accept-when-offered": (T + "links.h", "return !offered || peer < pins_.local();", "return true || offered;"),
    "draining-counts": (T + "links.h", "it->second.link && !it->second.link->connection->draining() ? &*it->second.link",
                        "it->second.link ? &*it->second.link"),
    "lookup-no-wake": (T + "links.h", "\t\t\troute.endpoints = interleave(found, route.address.port);\n\t\troute.waiting.wake_all();",
                       "\t\t\troute.endpoints = interleave(found, route.address.port);"),
    # links.h (security review S2, N1)
    "abandon-keeps-dial": (T + "links.h", "\t\t\tconnection->abort();\n\t\t\tforget(connection);\n\t\t\tco_return false;",
                           "\t\t\tco_return false;"),
    "refuse-newest": (T + "links.h", "\t\telse if (handshaking_.size() >= options_.handshakes)\n\t\t\tdrop = handshaking_.begin();",
                      "\t\telse if (handshaking_.size() >= options_.handshakes)\n\t\t\treturn connection->abort();"),
    "no-source-cap": (T + "links.h", ">= options_.handshakes_per_source", ">= std::size_t(-1)"),
    # links.h: gcc (13, 16) miscompiles a co_await in the right operand of && (run with g++)
    "dial-one-expression": (T + "links.h", "\t\tif (!connection)\n\t\t\tco_return false;\n\t\tco_return co_await offer(",
                            "\t\tco_return connection && co_await offer("),
    # host_transport.h, links.h (transport-groups A1, A2)
    "bulk-ahead-of-control": (T + "host_transport.h", "} else if (auto group = next(lane, false)) {",
                              "} else if (auto group = next(lane, true) ? std::nullopt : next(lane, false)) {"),
    "no-credit": (T + "host_transport.h", "return !g.data.empty() && (!g.outstanding || g.outstanding + g.data.front().size() <= options_.window);",
                  "return !g.data.empty();"),
    "reader-loop-delivers": (T + "host_transport.h", "\t\t\tif (to == k) {\n\t\t\t\tdeliver(k, std::move(moved));",
                             "\t\t\tif (true) {\n\t\t\t\tdeliver(k, std::move(moved));"),
    "no-hand-off": (T + "links.h", "Links *home = owner ? owner(peer) : nullptr;", "Links *home = nullptr;"),
    # host_transport.h (transport-groups review F3: credit comes back)
    "no-credit-on-refuse": (T + "host_transport.h", "\t\t\tif (r.data)\n\t\t\t\tcredit[", "\t\t\tif (r.data && taken)\n\t\t\t\tcredit["),
    "no-reset-on-end": [
        (T + "host_transport.h", "\t\tlane.grants.clear();\n\t\tfor (auto &[id, group] : lane.groups)\n\t\t\tgroup.outstanding = 0;\n\t\tlink.connection->on_read",
         "\t\tlane.grants.clear();\n\t\tlink.connection->on_read"),
        (T + "host_transport.h", "\t\tlane.grants.clear();\n\t\tfor (auto &[id, group] : lane.groups)\n\t\t\tgroup.outstanding = 0;\n\t\tif (!lane.groups.empty())",
         "\t\tlane.grants.clear();\n\t\tif (!lane.groups.empty())"),
    ],
}


def git(*args, env=None, data=None):
    return subprocess.run(["git", *args], check=True, capture_output=True, env=env, input=data).stdout.decode().strip()


def main():
    if sys.argv[1:] == ["--list"]:
        print("\n".join(MUTANTS))
        return 0
    ref, name = sys.argv[1], sys.argv[2]
    edits = MUTANTS[name] if isinstance(MUTANTS[name], list) else [MUTANTS[name]]
    texts = {}
    for path, search, replace in edits:
        text = texts.setdefault(path, git("show", f"{ref}:{path}"))
        if text.count(search) != 1:
            sys.exit(f"{name}: mutation point found {text.count(search)} times in {path}")
        texts[path] = text.replace(search, replace)
    with tempfile.TemporaryDirectory(dir=os.path.join(git("rev-parse", "--git-common-dir"))) as tmp:
        env = dict(os.environ, GIT_INDEX_FILE=os.path.join(tmp, "index"))
        git("read-tree", ref, env=env)
        for path, text in texts.items():
            blob = git("hash-object", "-w", "--stdin", data=(text + "\n").encode())
            git("update-index", "--cacheinfo", f"100644,{blob},{path}", env=env)
        tree = git("write-tree", env=env)
    print(git("commit-tree", tree, "-p", git("rev-parse", ref), "-m", f"Mutant {name}: {', '.join(texts)}"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
