#!/usr/bin/env python3
"""Live heap per idle row by owner (quiescence A6; macOS, lldb and malloc_history).

  test/idle_owners.py BUILD_DIR [ROWS] [--keep DIR]

Runs `consensus_driver_close_test idle_bytes` twice, with ROWS (default 200) and twice ROWS idle
rows, under lldb with MallocStackLogging, stopped at `idle_bytes_pause`. Reads every live
allocation's stack (`malloc_history -callTree -invert`), charges each to the innermost owner frame
(below), and prints what the second run holds past the first, per row: one idle row's heap with
the fixed costs (the host, the Driver, the test) cancelled. Sizes are malloc's blocks, each 16 B
above the requested bytes (the test's counting header), so totals run above idle_bytes' line.
"""
import collections
import pathlib
import re
import subprocess
import sys
import time

LINE = re.compile(r"^(?P<lead>[ +!:|]*?)(?P<count>\d+) \((?P<size>[\d.]+)(?P<unit>K|M|G| bytes)\) (?P<frame>.*)$")
UNIT = {"K": 1024, "M": 1 << 20, "G": 1 << 30, " bytes": 1}
# Most specific first: the innermost frame naming one of these owns the block.
OWNERS = [
    ("kronuz::consensus::Core", "consensus Core"),
    ("kronuz::consensus::RetainedLog", "consensus Core"),
    ("kronuz::consensus::Worker", "consensus Worker"),
    ("kronuz::consensus::Driver", "consensus Driver row"),
    ("kronuz::consensus::", "consensus other"),
    ("kronuz::journal::Host", "journal Host table"),
    ("kronuz::journal::host_detail", "journal Host table"),
    ("kronuz::journal::TurnDriver", "journal turn log"),
    ("kronuz::journal::TurnLog", "journal turn log"),
    ("kronuz::journal::turn_log_detail", "journal turn log"),
    ("kronuz::journal::Admission", "journal admission"),
    ("kronuz::journal::Store", "journal Store"),
    ("kronuz::journal::Journal", "journal Journal"),
    ("kronuz::journal::", "journal other"),
]
WRAPPERS = ("std::make_unique", "std::allocate_shared", "std::make_shared", "std::construct_at", "std::__1::make_unique", "std::__1::allocate_shared", "std::__1::make_shared")


def owner(path):
    """The owner of a block, from its frames innermost first; a make_* names what it makes."""
    for frame in path:
        name = frame.split("  (in ")[0]
        if name.startswith(WRAPPERS) and "<" in name:
            made = name[name.index("<") + 1:]
            for prefix, who in OWNERS:
                if made.startswith(prefix):
                    return who
            continue
        for prefix, who in OWNERS:
            if name.startswith(prefix):
                return who
        if name.startswith(("main", "start", "(anonymous namespace)::")):
            return "test"
    return "other"


def parse(text):
    """(owner, innermost site, bytes) per leaf of the inverted tree."""
    out, stack = [], []
    rows = [m for m in map(LINE.match, text.splitlines()) if m]
    for i, m in enumerate(rows):
        depth = len(m["lead"])
        while stack and stack[-1][0] >= depth:
            stack.pop()
        stack.append((depth, m["frame"]))
        leaf = i + 1 == len(rows) or len(rows[i + 1]["lead"]) <= depth
        if leaf and len(stack) > 1:
            path = [f for _, f in stack[2:]] or [stack[-1][1]]  # past TOTAL and malloc's own frame
            site = next((f for f in path if not f.startswith(("operator new", "std::"))), path[0])
            out.append((owner(path), site.split("  (in ")[0][:110], float(m["size"]) * UNIT[m["unit"]]))
    return out


def live(build, rows, workdir, keep):
    cmds = workdir / f"idle_owners.{rows}.lldb"
    cmds.write_text(
        f"settings set target.env-vars MallocStackLogging=1 IDLE_BYTES_PAUSE=1 IDLE_BYTES_ROWS={rows}\n"
        "b idle_bytes_pause\nrun idle_bytes\nscript import time; time.sleep(900)\nkill\nquit\n")
    logged = workdir / f"idle_owners.{rows}.log"
    binary = build / "consensus_driver_close_test"
    with open(logged, "w") as log:
        lldb = subprocess.Popen(["lldb", "-b", "-s", str(cmds), str(binary)], stdout=log, stderr=subprocess.STDOUT, cwd=workdir)
        try:
            pid = None
            for _ in range(1200):
                time.sleep(0.5)
                if "stop reason = breakpoint" in logged.read_text(errors="replace"):
                    found = subprocess.run(["pgrep", "-f", str(binary)], capture_output=True, text=True).stdout.split()
                    pid = next((p for p in found if int(p) != lldb.pid), None)
                    break
                if lldb.poll() is not None:
                    break
            if not pid:
                return None
            tree = subprocess.run(["malloc_history", pid, "-callTree", "-invert", "-ignoreThreads"], capture_output=True, text=True).stdout
            if keep:
                (keep / f"idle_owners.{rows}.tree").write_text(tree)
        finally:
            lldb.kill()
            lldb.wait()
    return parse(tree)


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        print(__doc__)
        return 2
    build = pathlib.Path(args[0]).resolve()
    rows = int(args[1]) if len(args) > 1 else 200
    keep = pathlib.Path(sys.argv[sys.argv.index("--keep") + 1]).resolve() if "--keep" in sys.argv else None
    workdir = keep or pathlib.Path.cwd()
    workdir.mkdir(parents=True, exist_ok=True)
    by_owner, by_site = collections.Counter(), collections.Counter()
    for count, sign in ((2 * rows, 1), (rows, -1)):
        leaves = live(build, count, workdir, keep)
        if leaves is None:
            print(f"the run with {count} rows did not stop at idle_bytes_pause", file=sys.stderr)
            return 1
        for who, site, size in leaves:
            by_owner[who] += sign * size
            by_site[(who, site)] += sign * size
    total = sum(by_owner.values())
    print(f"live heap per idle row by owner ({2 * rows} rows past {rows}; malloc blocks): {total / rows:.0f} B")
    for who, size in sorted(by_owner.items(), key=lambda kv: -abs(kv[1])):
        if abs(size) / rows >= 1:
            print(f"  {who:22} {size / rows:7.0f} B")
    print("largest sites per row:")
    for (who, site), size in sorted(by_site.items(), key=lambda kv: -abs(kv[1]))[:30]:
        if abs(size) / rows >= 16:
            print(f"  {who:22} {size / rows:7.0f} B  {site}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
