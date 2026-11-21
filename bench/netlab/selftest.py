#!/usr/bin/env python3
# Copyright © 2026 Germán Méndez Bravo (Kronuz)
# SPDX-License-Identifier: MIT

"""bench.py without root or Linux: faultlab's real Netlab over a fake runner, and a fake cluster.

The runner records every command netlab issues and answers ping from the shapes netlab set; the
fake cluster is a small model of a Raft group on the lab's cuts (a majority component elects after
an election timeout; a leader commits only with a majority) on a simulated clock. The clean model
passes every check at 3 and 5 nodes, and each mutant fails the check that guards it:

  rtt-off         ping sees 1.5x the round trip netlab set          -> round trip
  late-unmeasured the timer's lateness reads 0 while netem pays it    -> round trip
  minority-ack    a leader cut off with a minority acknowledges      -> majority only, history
  lost-write      each new leader lacks each key's last committed write -> history linearizable
  slow-election   elections take 4 timeouts                          -> elects after the cut

python3 -I bench/netlab/selftest.py   (fetches faultlab at FAULTLAB_REV unless FAULTLAB_DIR is set)
"""

from __future__ import annotations

import heapq
import os
import random
import re
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import bench

LATE_MS = (
    0.3  # the fake guest's timer wakes this late on each netem hold of 10 us or more
)

FAULTLAB_REPO = os.environ.get(
    "FAULTLAB_REPO", "https://github.com/Kronuz/faultlab.git"
)


def faultlab_checkout() -> Path:
    """FAULTLAB_DIR, or a checkout of the pinned revision under bench/netlab/.faultlab."""
    if os.environ.get("FAULTLAB_DIR"):
        return Path(os.environ["FAULTLAB_DIR"])
    rev = (HERE / "FAULTLAB_REV").read_text().split()[0]
    path = HERE / ".faultlab"
    head = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    if head != rev:
        subprocess.run(["rm", "-rf", str(path)], check=True)
        subprocess.run(["git", "clone", "-q", FAULTLAB_REPO, str(path)], check=True)
        subprocess.run(["git", "-C", str(path), "checkout", "-q", rev], check=True)
    return path


class FakeRunner:
    """Records commands; answers `ip netns list` with nothing and ping from the lab's shapes."""

    def __init__(self, mutant: str | None) -> None:
        self.mutant = mutant
        self.commands: list[tuple[list[str], str | None]] = []
        self.lab: Any = None

    def __call__(self, argv: Sequence[str], stdin: str | None = None) -> str:
        self.commands.append((list(argv), stdin))
        if "ping" in argv:
            target = int(argv[-1].rsplit(".", 1)[1])
            shape = self.lab.shapes[1, target]
            rtt = 2 * shape.ms * (1.5 if self.mutant == "rtt-off" else 1.0) + 0.05
            rtt += 2 * LATE_MS if shape.ms >= 0.01 else 0.0
            return "".join(
                f"64 bytes from {argv[-1]}: icmp_seq={i} ttl=64 time={rtt + 0.001 * (i % 3):.3f} ms\n"
                for i in range(5)
            )
        return ""

    def issued(self, pattern: str) -> bool:
        return any(
            re.search(pattern, " ".join(argv) + " " + (stdin or ""))
            for argv, stdin in self.commands
        )


class FakeCluster:
    """A Raft group as the lab's cuts allow it, on a simulated clock."""

    def __init__(self, lab: Any, nodes: int, mutant: str | None) -> None:
        self.lab, self.nodes, self.mutant = lab, nodes, mutant
        self.t = 5000.0
        self.alive_set: set[int] = set()
        self.term = 0
        self.leader_of: dict[
            int, int
        ] = {}  # node: the term it leads, while it believes it does
        self.lost_leader_at: float | None = 0.0
        self.state: dict[str, str] = {}
        self.history: list[
            tuple[str, str | None]
        ] = []  # committed writes: key, previous value
        self.queue: list[
            tuple[float, int, int, dict[str, Any]]
        ] = []  # due, seq, node, event
        self.held: dict[
            int, list[tuple[str, str]]
        ] = {}  # a cut leader's calls: kind, id
        self.seq = 0

    # The model.

    def rtt(self) -> float:
        return 2 * self.lab.shapes[1, 2].ms / 1000 if self.lab.shapes else 0.0

    def connected(self, a: int, b: int) -> bool:
        return (a, b) not in self.lab.cut and (b, a) not in self.lab.cut

    def component(self, node: int) -> set[int]:
        seen, todo = {node}, [node]
        while todo:
            a = todo.pop()
            for b in self.alive_set:
                if b not in seen and self.connected(a, b):
                    seen.add(b)
                    todo.append(b)
        return seen

    def quorum(self, node: int) -> bool:
        return 2 * len(self.component(node)) > self.nodes

    def emit(self, node: int, event: dict[str, Any], after: float = 0.0) -> None:
        self.seq += 1
        heapq.heappush(
            self.queue,
            (
                self.t + after,
                self.seq,
                node,
                event | {"t": int((self.t + after) * 1e6)},
            ),
        )

    def leading(self, node: int) -> bool:
        return self.leader_of.get(node) == self.term and node in self.alive_set

    def step(self) -> None:
        """Elects in a majority component that has no current leader, after one election timeout;
        steps down a stale leader that hears the current term."""
        current = [n for n in self.alive_set if self.leading(n) and self.quorum(n)]
        if current:
            self.lost_leader_at = None
        elif self.lost_leader_at is None:
            self.lost_leader_at = self.t
        else:
            timeout = 4 * 2.0 if self.mutant == "slow-election" else 1.2
            if self.t - self.lost_leader_at >= timeout + self.rtt():
                majority = [n for n in sorted(self.alive_set) if self.quorum(n)]
                if majority:
                    self.term += 1
                    new = majority[0]
                    self.leader_of[new] = self.term
                    self.emit(
                        new,
                        {
                            "ev": "role",
                            "role": "leader",
                            "term": self.term,
                            "leader": new,
                        },
                    )
                    for n in self.component(new) - {new}:
                        self.emit(
                            n,
                            {
                                "ev": "role",
                                "role": "follower",
                                "term": self.term,
                                "leader": new,
                            },
                        )
                    if (
                        self.mutant == "lost-write"
                    ):  # the new leader lacks each key's last committed write
                        reverted: set[str] = set()
                        for key, previous in reversed(self.history):
                            if key not in reverted:
                                reverted.add(key)
                                if previous is None:
                                    self.state.pop(key, None)
                                else:
                                    self.state[key] = previous
                    self.lost_leader_at = None
        for node, term in list(self.leader_of.items()):
            if (
                term < self.term
                and node in self.alive_set
                and any(self.leading(n) for n in self.component(node))
            ):
                del self.leader_of[node]
                current = next(n for n in self.component(node) if self.leading(n))
                self.emit(
                    node,
                    {
                        "ev": "role",
                        "role": "follower",
                        "term": self.term,
                        "leader": current,
                    },
                )
                for kind, request in self.held.pop(node, []):
                    self.emit(
                        node,
                        {
                            "ev": kind,
                            "id": int(request),
                            "ok": 0,
                            "why": "superseded"
                            if kind == "w"
                            else "rejected NotLeader",
                        },
                    )

    # The bench's interface.

    def now(self) -> float:
        return self.t

    def start(self, node: int, fresh: bool) -> None:
        self.alive_set.add(node)
        self.emit(node, {"ev": "ready", "node": node}, 0.01)

    def alive(self, node: int) -> bool:
        return node in self.alive_set

    def kill(self, node: int) -> None:
        self.alive_set.discard(node)
        self.leader_of.pop(node, None)
        self.held.pop(node, None)
        self.queue = [e for e in self.queue if e[2] != node]
        heapq.heapify(self.queue)

    def stop(self) -> None:
        for node in list(self.alive_set):
            self.kill(node)
        self.leader_of.clear()
        self.lost_leader_at = 0.0
        self.term = 0  # fresh journals: the next shape's terms start over
        self.state.clear()  # the next shape starts a fresh cluster
        self.history.clear()

    def send(self, node: int, line: str) -> None:
        if node not in self.alive_set:
            return
        verb, request, *rest = line.split()
        rtt = self.rtt() + 0.0004
        if verb in ("w", "r"):
            if not self.leading(node):
                self.emit(
                    node,
                    {
                        "ev": verb,
                        "id": int(request),
                        "ok": 0,
                        "why": "rejected NotLeader",
                    },
                    0.0002,
                )
            elif not self.quorum(node):
                if verb == "w" and self.mutant == "minority-ack":
                    self.emit(
                        node, {"ev": "w", "id": int(request), "ok": 1, "index": 0}, rtt
                    )
                else:
                    self.held.setdefault(node, []).append((verb, request))
            elif verb == "w":
                key, value = rest[0], rest[1]
                self.history.append((key, self.state.get(key)))
                self.state[key] = value
                self.emit(
                    node,
                    {
                        "ev": "w",
                        "id": int(request),
                        "ok": 1,
                        "index": len(self.history),
                    },
                    rtt,
                )
            else:
                self.emit(
                    node,
                    {
                        "ev": "r",
                        "id": int(request),
                        "ok": 1,
                        "value": self.state.get(rest[0]),
                    },
                    rtt,
                )
        elif verb == "load":
            seconds, size, inflight = float(rest[0]), int(rest[1]), int(rest[2])
            commits = int(seconds / rtt * inflight)
            us = int(rtt * 1e6)
            self.emit(
                node,
                {"ev": "load", "id": int(request), "commits": commits, "bytes": commits * size, "seconds": seconds, "n": commits,
                 "p50": us, "p90": us, "p99": us + 100, "max": us + 200, "failures": {}},
                seconds,
            )  # fmt: skip
        elif verb == "rpc":
            us = int((self.rtt() + 0.0001) * 1e6)
            self.emit(
                node,
                {
                    "ev": "rpc",
                    "id": int(request),
                    "failed": 0,
                    "n": 100,
                    "p50": us,
                    "p90": us,
                    "p99": us + 50,
                    "max": us + 60,
                },
                0.5,
            )

    def poll(self, timeout: float) -> list[tuple[int, dict[str, Any]]]:
        self.step()
        until = self.t + timeout
        due: list[tuple[int, dict[str, Any]]] = []
        while self.queue and self.queue[0][0] <= until:
            when, _, node, event = heapq.heappop(self.queue)
            self.t = max(self.t, when)
            if node in self.alive_set:
                due.append((node, event))
        self.t = max(self.t, until) if not due else self.t
        return due


def run(
    nodes: int, mutant: str | None, netlab: Any, history: Any, out: Path
) -> tuple[dict[str, Any], FakeRunner]:
    random.seed(7)
    runner = FakeRunner(mutant)
    lab = netlab.Netlab(prefix="selftest", runner=runner)
    runner.lab = lab
    cluster = FakeCluster(lab, nodes, mutant)
    plan = bench.Plan.quick()

    def late(ms: float) -> list[float]:
        wake = 0.0 if mutant == "late-unmeasured" else LATE_MS
        return [wake + 0.01 * (i % 3) for i in range(9)]

    b = bench.Bench(lab, cluster, nodes, plan, out, runner, history, late)
    return bench.measure(b, bench.SHAPES), runner


def failed(result: dict[str, Any]) -> set[str]:
    return {c["name"].split(": ", 1)[-1] for c in result["checks"] if not c["ok"]}


def main() -> int:
    netlab, history = bench.load_faultlab(faultlab_checkout())
    problems: list[str] = []

    def expect(what: str, ok: bool) -> None:
        print(f"{'ok  ' if ok else 'FAIL'} {what}", flush=True)
        if not ok:
            problems.append(what)

    with tempfile.TemporaryDirectory(dir=HERE) as scratch:
        out = Path(scratch)
        results = []
        for nodes in (3, 5):
            result, runner = run(nodes, None, netlab, history, out)
            results.append(result)
            expect(
                f"clean model, {nodes} nodes: every check passes ({len(result['checks'])}, failed {failed(result)})",
                result["ok"],
            )
            expect(
                f"{nodes} nodes: a row per shape",
                [r["shape"] for r in result["rows"]] == [s.name for s in bench.SHAPES],
            )
            expect(
                f"{nodes} nodes: 1% loss at 20 ms set on netem",
                runner.issued(r"netem limit 100000 delay 20ms loss 1%"),
            )
            expect(
                f"{nodes} nodes: 5% reorder at 20 ms set on netem",
                runner.issued(r"netem limit 100000 delay 20ms reorder 5%"),
            )
            expect(
                f"{nodes} nodes: 80 ms one way set on netem",
                runner.issued(r"netem limit 100000 delay 80ms(\s|$)"),
            )
            expect(
                f"{nodes} nodes: the cut and the heal reach nftables",
                runner.issued(r"add element inet faultlab cut")
                and runner.issued(r"delete element inet faultlab cut"),
            )
            part = next(r for r in result["rows"] if r["shape"] == "partition-1ms")[
                "partition"
            ]
            expect(
                f"{nodes} nodes: the partition's history has acknowledged writes and faults",
                part["acknowledged_writes"] > 0 and part["violations"] == 0,
            )
            expect(
                f"{nodes} nodes: the cut leader's term is its own cluster's ({part['terms']})",
                part["terms"]["isolated_leader"] == part["terms"]["before"],
            )
            logged = (out / part["history"]).read_text()
            expect(
                f"{nodes} nodes: the event log holds netlab's isolate and heal",
                '"fault": "isolate"' in logged and '"fault": "heal"' in logged,
            )
        rows = bench.table(results, {3: "J3", 5: "J5"})
        expect(
            "the table has a row per shape and node count",
            rows.count("\n| 3 |") == len(bench.SHAPES)
            and rows.count("\n| 5 |") == len(bench.SHAPES),
        )
        expect("no hunt rows for the clean model", not bench.hunts(results))
        failing = [dict(r) for r in results[:1]]
        failing[0]["rows"] = [dict(r) for r in results[0]["rows"]]
        failing[0]["rows"][1] = dict(
            failing[0]["rows"][1], large_throughput={"error": "no answer in 32 s"}
        )
        failing[0]["rows"][2] = {
            k: v
            for k, v in failing[0]["rows"][2].items()
            if k == "shape" or k == "nodes" or k == "set_rtt_ms"
        }
        try:
            text = bench.table(failing, {})
            bench.hunts(failing)
        except Exception as error:  # noqa: BLE001 - the check is that nothing raises
            text = repr(error)
        expect(
            "an errored measurement and an unmeasured row are table cells, not a crash",
            "| error: no answer in 32 s |" in text
            and "| error: not measured |" in text,
        )
        slow = [dict(r) for r in results[:1]]
        slow[0]["rows"] = [dict(r) for r in results[0]["rows"]]
        slow[0]["rows"][2] = dict(
            slow[0]["rows"][2],
            small_latency=dict(slow[0]["rows"][2]["small_latency"], p99_ms=500.0),
        )
        expect(
            "a commit p99 past 2 round trips is a hunt row",
            any("small_latency p99" in h for h in bench.hunts(slow)),
        )
        for mutant, guard in (
            ("rtt-off", "round trip"),
            ("late-unmeasured", "round trip"),
            ("minority-ack", "majority only"),
            ("minority-ack", "history linearizable"),
            ("lost-write", "history linearizable"),
            ("slow-election", "elects after the cut"),
        ):
            result, _ = run(3, mutant, netlab, history, out)
            expect(
                f"mutant {mutant}: fails {guard!r} (failed {sorted(failed(result))})",
                not result["ok"] and guard in failed(result),
            )
    print(f"selftest: {'FAIL ' + str(len(problems)) if problems else 'ok'}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
