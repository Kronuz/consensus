#!/usr/bin/env python3
"""Writes a mutant: a commit on top of REF whose only change is one seeded fault, on no branch.
The plain test run at it with `campaign job submit --fails-without --commit SHA` proves the test
catches the fault.

    test/transport/mutant_commit.py REF NAME       prints the mutant commit's sha
    test/transport/mutant_commit.py --list          names every mutant

Each mutant is (file, search, replace); the search must occur exactly once."""
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
    "no-close-notify": (T + "connection.h", "if (end_ == ConnectionEnd::open) {\n\t\t\t\tlate_deadline stuck", "if (false) {\n\t\t\t\tlate_deadline stuck"),
    "no-keepalive": (T + "connection.h", "\t\t\t\t\tput_length(0);\n", "\t\t\t\t\t(void)0;\n"),
}


def git(*args, env=None, data=None):
    return subprocess.run(["git", *args], check=True, capture_output=True, env=env, input=data).stdout.decode().strip()


def main():
    if sys.argv[1:] == ["--list"]:
        print("\n".join(MUTANTS))
        return 0
    ref, name = sys.argv[1], sys.argv[2]
    path, search, replace = MUTANTS[name]
    text = git("show", f"{ref}:{path}")
    if text.count(search) != 1:
        sys.exit(f"{name}: mutation point found {text.count(search)} times in {path}")
    blob = git("hash-object", "-w", "--stdin", data=(text.replace(search, replace) + "\n").encode())
    with tempfile.TemporaryDirectory(dir=os.path.join(git("rev-parse", "--git-common-dir"))) as tmp:
        env = dict(os.environ, GIT_INDEX_FILE=os.path.join(tmp, "index"))
        git("read-tree", ref, env=env)
        git("update-index", "--cacheinfo", f"100644,{blob},{path}", env=env)
        tree = git("write-tree", env=env)
    print(git("commit-tree", tree, "-p", git("rev-parse", ref), "-m", f"Mutant {name}: {path}"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
