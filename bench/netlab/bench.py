#!/usr/bin/env python3
# Copyright © 2026 Germán Méndez Bravo (Kronuz)
# SPDX-License-Identifier: MIT

"""consensus on a realistic network: N nodes, one per faultlab netlab namespace, at each shape.

Each shape starts a fresh cluster of `consensus_netlab_node` processes (node.cc), one per namespace,
and measures: the round trip netlab set against what ping sees (A1), that a leader cut off with a
minority acknowledges nothing while the majority commits (A1), commit latency at one in flight and
throughput at many for small and 64 KiB entries, RPC-lane echo latency, and election time after the
leader is SIGKILLed and restarted on its journal. The partition shape isolates the leader under a
client workload, heals, and checks the event log with faultlab's history checker (A2). Every node's
event carries CLOCK_MONOTONIC microseconds, the clock this process reads, so cross-node times compare.

Needs Linux and root (netlab). selftest.py runs it without either, on a fake runner and cluster.

python3 -I bench.py run --faultlab DIR --node BIN --nodes 3 [--shapes a,b] [--quick] --out DIR
python3 -I bench.py table RESULTS.json... > rows.md
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import random
import re
import selectors
import shutil
import signal
import statistics
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

PORT = 7400
SMALL = 64  # bytes of a small entry
LARGE = 64 << 10
ELECTION_MIN_MS, ELECTION_MAX_MS = (
    1000,
    2000,
)  # consensus::Timing's defaults, which node.cc keeps
HEARTBEAT_MS = 100
CLIENTS = 3  # history clients per node
OP_SECONDS = (
    1.0  # a history operation unanswered this long is unknown, and its client moves on
)
THINK_SECONDS = (
    0.025  # between a client's operations, which bounds the history the checker reads
)
# A1: ping's median within 10% of the set round trip, past the unshaped path's own round trip (the
# floor), which is also the jitter allowed, at least JITTER_MS.
RTT_TOLERANCE = 0.10
JITTER_MS = 0.1


@dataclass(frozen=True)
class Shape:
    """One network shape: netlab's one-way delay on every link, loss and reorder, and whether the
    leader is partitioned and healed under load."""

    name: str
    ms: float = 0.0
    loss: float = 0.0
    reorder: float = 0.0
    partition: bool = False

    @property
    def rtt_ms(self) -> float:
        return 2 * self.ms


SHAPES = (
    Shape("loopback"),
    Shape("1ms", ms=1),
    Shape("20ms", ms=20),
    Shape("80ms", ms=80),
    Shape("loss1%-20ms", ms=20, loss=0.01),
    Shape("reorder5%-20ms", ms=20, reorder=0.05),
    Shape("partition-1ms", ms=1, partition=True),
)


@dataclass(frozen=True)
class Plan:
    """How long each measurement runs; --quick shrinks it for the dry run."""

    load_seconds: float = 2.0
    inflight: int = 32
    rpc_seconds: float = 1.5
    rpc_max: int = 400
    elections: int = 4
    partition_seconds: float = 4.0
    healed_seconds: float = 3.0
    pings: int = 10

    @staticmethod
    def quick() -> Plan:
        return Plan(
            load_seconds=0.6,
            inflight=8,
            rpc_seconds=0.5,
            rpc_max=50,
            elections=1,
            partition_seconds=3.0,
            healed_seconds=2.0,
            pings=5,
        )


class Lab(Protocol):
    """The part of faultlab's Netlab the bench drives."""

    log: Any  # history.Recorder or None

    def open(self, peer_ports: dict[int, int]) -> None: ...
    def teardown(self) -> None: ...
    def address(self, source: int, target: int) -> str: ...
    def command(self, node: int, argv: Sequence[str]) -> list[str]: ...
    def delay(self, ms: float, a: int | None = None, b: int | None = None) -> None: ...
    def loss(self, p: float, a: int | None = None, b: int | None = None) -> None: ...
    def reorder(self, p: float, a: int | None = None, b: int | None = None) -> None: ...
    def partition(self, a: int, b: int) -> None: ...
    def isolate(self, node: int) -> None: ...
    def heal(self, a: int | None = None, b: int | None = None) -> None: ...


type Event = dict[str, Any]  # a node's decoded JSON line


class Cluster(Protocol):
    """The node processes: started, fed command lines, polled for their events, killed."""

    def now(self) -> float: ...  # CLOCK_MONOTONIC seconds, the nodes' clock
    def start(self, node: int, fresh: bool) -> None: ...
    def send(self, node: int, line: str) -> None: ...
    def poll(self, timeout: float) -> list[tuple[int, Event]]: ...
    def kill(self, node: int) -> None: ...
    def stop(self) -> None: ...
    def alive(self, node: int) -> bool: ...


class Processes:
    """The real cluster: node.cc processes, each in its namespace, its journal under `root`."""

    def __init__(
        self, lab: Lab, binary: Path, fixture: Path, root: Path, nodes: int
    ) -> None:
        self.lab, self.binary, self.fixture, self.root, self.nodes = (
            lab,
            binary,
            fixture,
            root,
            nodes,
        )
        self.procs: dict[int, subprocess.Popen[bytes]] = {}
        self.buffers: dict[int, bytes] = {}
        self.selector = selectors.DefaultSelector()
        self.logs = root / "stderr"

    def now(self) -> float:
        return time.monotonic()

    def start(self, node: int, fresh: bool) -> None:
        directory = self.root / f"node{node}"
        if fresh:
            shutil.rmtree(directory, ignore_errors=True)
        self.logs.mkdir(parents=True, exist_ok=True)
        peers = [
            f"{peer}={self.lab.address(node, peer)}"
            for peer in range(1, self.nodes + 1)
            if peer != node
        ]
        argv = [
            str(self.binary),
            str(self.fixture),
            str(directory),
            str(node),
            str(self.nodes),
            str(PORT),
            *peers,
        ]
        with (self.logs / f"node{node}.log").open("ab") as errors:
            proc = subprocess.Popen(
                self.lab.command(node, argv),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=errors,
            )
        assert proc.stdout is not None
        os.set_blocking(proc.stdout.fileno(), False)
        self.procs[node] = proc
        self.buffers[node] = b""
        self.selector.register(proc.stdout, selectors.EVENT_READ, node)

    def send(self, node: int, line: str) -> None:
        proc = self.procs.get(node)
        if proc is None or proc.poll() is not None or proc.stdin is None:
            return
        with contextlib.suppress(BrokenPipeError):
            proc.stdin.write(line.encode() + b"\n")
            proc.stdin.flush()

    def poll(self, timeout: float) -> list[tuple[int, Event]]:
        events: list[tuple[int, Event]] = []
        for key, _ in self.selector.select(max(timeout, 0)):
            node = int(key.data)
            try:
                chunk = os.read(key.fd, 1 << 16)
            except BlockingIOError:
                continue
            if not chunk:
                self.selector.unregister(key.fileobj)
                continue
            data = self.buffers[node] + chunk
            *lines, self.buffers[node] = data.split(b"\n")
            for line in lines:
                with contextlib.suppress(json.JSONDecodeError):
                    events.append((node, json.loads(line)))
        return events

    def _forget(self, node: int) -> None:
        proc = self.procs.pop(node, None)
        if proc is not None and proc.stdout is not None:
            with contextlib.suppress(KeyError, ValueError):
                self.selector.unregister(proc.stdout)

    def kill(self, node: int) -> None:
        proc = self.procs.get(node)
        if proc is not None:
            proc.send_signal(signal.SIGKILL)
            proc.wait()
            self._forget(node)

    def alive(self, node: int) -> bool:
        proc = self.procs.get(node)
        return proc is not None and proc.poll() is None

    def stop(self) -> None:
        for node in list(self.procs):
            self.send(node, "quit")
        until = time.monotonic() + 5
        for node, proc in list(self.procs.items()):
            try:
                proc.wait(max(0.1, until - time.monotonic()))
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            self._forget(node)


def percentile(values: Sequence[float], q: float) -> float:
    """The q-quantile by nearest rank, as node.cc computes its own."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[int(q * (len(ordered) - 1))]


def ping_rtts(output: str) -> list[float]:
    """Each reply's round trip in ms from ping's output."""
    return [float(m) for m in re.findall(r"time[=<]([0-9.]+) ?ms", output)]


@dataclass
class Check:
    name: str
    ok: bool
    detail: str


@dataclass
class Client:
    """One history client: at most one operation open, on its node."""

    name: str
    node: int
    pending: tuple[str, str, str, float] | None = None  # id, op, key, deadline
    generation: int = 0
    ready: float = 0.0  # when it issues its next operation


class Bench:
    """Drives one node count across shapes; `run_shape` returns its row and records its checks."""

    def __init__(
        self,
        lab: Lab,
        cluster: Cluster,
        nodes: int,
        plan: Plan,
        out: Path,
        run: Callable[[Sequence[str]], str],
        history: Any,
    ) -> None:
        self.lab, self.cluster, self.nodes, self.plan, self.out = (
            lab,
            cluster,
            nodes,
            plan,
            out,
        )
        self.run = run  # runs a command, returns its output (netlab's runner)
        self.history = history  # faultlab.history
        self.checks: list[Check] = []
        self.role: dict[int, tuple[str, int, float]] = {}  # node: (role, term, t)
        self.replies: dict[str, Event] = {}  # by request id
        self.ids = 0
        self.floor_ms = 0.0
        self.recorder: Any = None
        self.clients: list[Client] = []
        self.ops: dict[
            str, tuple[Client, str, str, str, float]
        ] = {}  # request id: client, op, key, value, invoked
        self.minority: set[int] = (
            set()
        )  # nodes cut off with a minority while the partition holds
        self.minority_acks: list[str] = []
        self.values = 0
        self.started_cut = 0.0
        self.skew = (
            0.0  # the largest gap between a node's clock and ours at its ready event
        )

    # Events.

    def check(self, name: str, ok: bool, detail: str) -> bool:
        self.checks.append(Check(name, ok, detail))
        print(f"  {'ok  ' if ok else 'FAIL'} {name}: {detail}", flush=True)
        return ok

    def next_id(self) -> str:
        self.ids += 1
        return str(self.ids)

    def pump(self, seconds: float, until: Callable[[], bool] | None = None) -> bool:
        """Polls events for up to `seconds`, ticking the clients; True once `until` holds."""
        deadline = self.cluster.now() + seconds
        while True:
            if until is not None and until():
                return True
            left = deadline - self.cluster.now()
            if left <= 0:
                return False
            for node, event in self.cluster.poll(min(left, 0.02)):
                self.dispatch(node, event)
            self.tick()

    def dispatch(self, node: int, event: Event) -> None:
        kind = event.get("ev")
        if kind == "ready":
            self.skew = max(
                self.skew, abs(float(event["t"]) / 1e6 - self.cluster.now())
            )
        elif kind == "role":
            self.role[node] = (
                str(event["role"]),
                int(event["term"]),
                float(event["t"]) / 1e6,
            )
        elif kind == "error":
            print(f"  node {node}: {event.get('why')}", flush=True)
        elif "id" in event:
            request = str(event["id"])
            self.replies[request] = event | {"node": node}
            if request in self.ops:
                self.complete(request, event)

    def leader(self, among: Sequence[int] | None = None, above: int = -1) -> int:
        """The live node that last said it leads, in the highest term above `above`, or 0."""
        best, term = 0, above
        for node, (role, t, _) in self.role.items():
            if (
                role == "leader"
                and t > term
                and self.cluster.alive(node)
                and (among is None or node in among)
            ):
                best, term = node, t
        return best

    def call(self, node: int, line: str, seconds: float) -> Event | None:
        """Sends `line` with a fresh id after its verb to `node`, waits for its reply."""
        request = self.next_id()
        verb, _, rest = line.partition(" ")
        self.cluster.send(node, f"{verb} {request} {rest}".rstrip())
        self.pump(seconds, lambda: request in self.replies)
        return self.replies.get(request)

    # The cluster's life.

    def start(self) -> int:
        self.role.clear()
        for node in range(1, self.nodes + 1):
            self.cluster.start(node, fresh=True)
        led = self.pump(30, lambda: self.leader() != 0)
        self.check("elects at start", led, f"leader {self.leader()} of {self.nodes}")
        self.check(
            "one clock",
            self.skew < 0.5,
            f"nodes' clocks within {self.skew * 1000:.1f} ms of the harness's",
        )
        # The leader's first write commits its term's no-op too: then every later number is steady.
        leader = self.leader()
        if leader:
            reply = self.call(leader, f"w warm {self.next_value()} {SMALL}", 10)
            self.check(
                "first write commits", bool(reply and reply.get("ok")), f"{reply}"
            )
        return leader

    def next_value(self) -> str:
        self.values += 1
        return f"v{self.values}"

    def shape(self, shape: Shape) -> None:
        """Sets every link to the shape; a reorder needs its delay set first and lifted last."""
        self.lab.reorder(0)
        self.lab.loss(0)
        self.lab.delay(shape.ms)
        self.lab.loss(shape.loss)
        self.lab.reorder(shape.reorder)

    def measure_floor(self) -> None:
        """The unshaped round trip between namespaces: the tolerance's floor."""
        self.shape(SHAPES[0])
        samples = self.ping_all()
        self.floor_ms = statistics.median(samples) if samples else 0.0
        print(f"  unshaped round trip {self.floor_ms:.3f} ms", flush=True)

    def ping_all(self) -> list[float]:
        medians = []
        for peer in range(2, self.nodes + 1):
            host = self.lab.address(1, peer).rsplit(":", 1)[0]
            argv = [
                "ping",
                "-n",
                "-c",
                str(self.plan.pings),
                "-i",
                "0.02",
                "-W",
                "2",
                host,
            ]
            try:
                output = self.run(self.lab.command(1, argv))
            except Exception as error:  # noqa: BLE001 - a ping that loses every reply exits 1
                output = str(error)
            rtts = ping_rtts(output)
            if rtts:
                medians.append(statistics.median(rtts))
        return medians

    # A1: the network is what netlab set, and only a majority acknowledges.

    def check_rtt(self, shape: Shape, row: dict[str, Any]) -> None:
        medians = self.ping_all()
        expected = shape.rtt_ms
        allowed = RTT_TOLERANCE * expected + max(self.floor_ms, JITTER_MS)
        worst = max(
            (abs(m - expected - self.floor_ms) for m in medians), default=float("inf")
        )
        row["rtt_ms"] = round(statistics.median(medians), 3) if medians else None
        self.check(
            f"{shape.name}: round trip",
            len(medians) == self.nodes - 1 and worst <= allowed,
            f"set {expected:g} ms + floor {self.floor_ms:.3f}, ping medians {[round(m, 3) for m in medians]} ms, allowed ±{allowed:.3f}",
        )

    def cut(self, side: set[int]) -> None:
        for a in side:
            for b in range(1, self.nodes + 1):
                if b not in side:
                    self.lab.partition(a, b)

    def check_majority(self, shape: Shape, row: dict[str, Any]) -> None:
        """The leader and as many followers as keep it a minority are cut off: a write there is
        never acknowledged, one on the majority's new leader is, and after heal it stays unacknowledged."""
        leader = self.leader()
        if not leader:
            self.check(f"{shape.name}: majority only", False, "no leader")
            return
        followers = [n for n in range(1, self.nodes + 1) if n != leader]
        side = {leader, *followers[: (self.nodes - 1) // 2 - 1]}
        majority = [n for n in range(1, self.nodes + 1) if n not in side]
        term = self.role[leader][1]
        self.started_cut = self.cluster.now()
        self.cut(side)
        request = self.next_id()
        self.cluster.send(leader, f"w {request} probe {self.next_value()} {SMALL}")
        elected = self.pump(
            10 + 4 * shape.rtt_ms / 1000, lambda: self.leader(majority, term) != 0
        )
        new = self.leader(majority, term)
        reply = (
            self.call(new, f"w probe {self.next_value()} {SMALL}", 10) if new else None
        )
        early = self.replies.get(request)
        self.lab.heal()
        self.pump(2 * HEARTBEAT_MS / 1000 + 4 * shape.rtt_ms / 1000 + 0.5)
        late = self.replies.get(request)
        row["probe_election_ms"] = (
            round((self.role[new][2] - self.started_cut) * 1000, 1) if new else None
        )
        acked = bool(late and late.get("ok"))
        self.check(
            f"{shape.name}: majority only",
            elected and bool(reply and reply.get("ok")) and not acked,
            f"cut {sorted(side)} of {self.nodes}: its write {'ACKNOWLEDGED' if acked else 'unacknowledged'} "
            f"({(early or late or {}).get('why', 'no answer')}); majority leader {new} {'committed' if reply and reply.get('ok') else 'did not commit'}",
        )

    # Numbers.

    def load(self, leader: int, size: int, inflight: int) -> dict[str, Any]:
        seconds = self.plan.load_seconds
        # 128 MiB of 64 KiB entries per node, which the guest holds twice
        cap = 2048 if size == LARGE else 1_000_000
        reply = self.call(
            leader, f"load {seconds} {size} {inflight} {cap}", seconds + 30
        )
        if not reply:
            return {"error": "no answer"}
        commits, took = int(reply["commits"]), float(reply["seconds"]) or 1.0
        return {
            "commits": commits,
            "per_s": round(commits / took, 1),
            "mb_s": round(int(reply["bytes"]) / took / 1e6, 2),
            "p50_ms": reply["p50"] / 1000,
            "p99_ms": reply["p99"] / 1000,
            "failures": reply.get("failures", {}),
        }

    def rpc(self, leader: int) -> dict[str, Any]:
        peer = next(n for n in range(1, self.nodes + 1) if n != leader)
        reply = self.call(
            leader,
            f"rpc {peer} {SMALL} {self.plan.rpc_seconds} {self.plan.rpc_max}",
            self.plan.rpc_seconds + 30,
        )
        if not reply:
            return {"error": "no answer"}
        return {
            "calls": reply["n"],
            "failed": reply["failed"],
            "p50_ms": reply["p50"] / 1000,
            "p99_ms": reply["p99"] / 1000,
        }

    def elections(self, shape: Shape) -> list[float]:
        """Kills the leader, times the first survivor to lead a later term, restarts the dead node."""
        times: list[float] = []
        for _ in range(self.plan.elections):
            leader = self.leader()
            if not leader:
                break
            term = self.role[leader][1]
            survivors = [n for n in range(1, self.nodes + 1) if n != leader]
            killed = self.cluster.now()
            self.cluster.kill(leader)
            if not self.pump(
                10 * ELECTION_MAX_MS / 1000,
                lambda survivors=survivors, term=term: (
                    self.leader(survivors, term) != 0
                ),
            ):
                self.check(
                    f"{shape.name}: election after kill",
                    False,
                    f"no leader within {10 * ELECTION_MAX_MS} ms",
                )
                break
            new = self.leader(survivors, term)
            times.append((self.role[new][2] - killed) * 1000)
            self.role.pop(leader, None)
            self.cluster.start(leader, fresh=False)  # recovers its journal
            reply = self.call(new, f"w warm {self.next_value()} {SMALL}", 10)
            self.pump(2 * HEARTBEAT_MS / 1000 + 2 * shape.rtt_ms / 1000 + 0.3)
            if not (reply and reply.get("ok")):
                self.check(f"{shape.name}: commits after election", False, f"{reply}")
                break
        return times

    def bound_ms(self, shape: Shape) -> float:
        """One election: a follower's timeout after the last heartbeat it heard, a vote round trip,
        and slack for the persists and the scheduler."""
        return ELECTION_MAX_MS + HEARTBEAT_MS + 2 * shape.rtt_ms + 200

    # A2: the partition of the leader, healed, under clients; the history decides.

    def begin_history(self, shape: Shape) -> Path:
        path = self.out / f"history-{self.nodes}-{shape.name}.jsonl"
        path.unlink(missing_ok=True)
        self.recorder = self.history.Recorder(
            path, scenario=f"netlab-consensus {self.nodes} {shape.name}"
        )
        self.lab.log = self.recorder
        self.clients = [
            Client(f"c{node}-{i}", node)
            for node in range(1, self.nodes + 1)
            for i in range(CLIENTS)
        ]
        self.ops.clear()
        return path

    def tick(self) -> None:
        if self.recorder is None:
            return
        now = self.cluster.now()
        for client in self.clients:
            if client.pending is not None:
                request, op, key, deadline = client.pending
                if now < deadline:
                    continue
                self.ops.pop(
                    request, None
                )  # unknown: a later client name takes the node
                client.generation += 1
                client.name = f"{client.name.split('.')[0]}.{client.generation}"
                client.pending = None
            if not self.cluster.alive(client.node) or now < client.ready:
                continue
            key = f"k{random.randrange(4)}"
            request = self.next_id()
            op, level = (
                ("write", "quorum")
                if random.random() < 0.6
                else ("read", "linearizable")
            )
            value = self.next_value() if op == "write" else ""
            self.recorder.record(
                client.name,
                op,
                key,
                value or None,
                level,
                "invoke",
                node=f"node{client.node}",
            )
            self.ops[request] = (client, op, key, value, now)
            client.pending = (request, op, key, now + OP_SECONDS)
            client.ready = now + THINK_SECONDS
            self.cluster.send(
                client.node,
                f"w {request} {key} {value} {SMALL}"
                if op == "write"
                else f"r {request} {key}",
            )

    def complete(self, request: str, event: Event) -> None:
        client, op, key, value, invoked = self.ops.pop(request)
        if client.pending is None or client.pending[0] != request:
            return
        client.pending = None
        node = f"node{client.node}"
        if event.get("ok"):
            if (
                op == "write"
                and client.node in self.minority
                and invoked > self.started_cut
            ):
                self.minority_acks.append(f"{client.name} {key}={value}")
            self.recorder.record(
                client.name,
                op,
                key,
                value if op == "write" else event.get("value"),
                "quorum" if op == "write" else "linearizable",
                "ok",
                node=node,
            )
        elif str(event.get("why", "")).startswith(("rejected", "superseded")):
            self.recorder.record(
                client.name,
                op,
                key,
                value if op == "write" else None,
                "quorum" if op == "write" else "linearizable",
                "fail",
                node=node,
            )
        else:  # unknown: left open, so the checker completes it at infinity
            client.generation += 1
            client.name = f"{client.name.split('.')[0]}.{client.generation}"

    def end_history(self) -> None:
        self.recorder.close()
        self.recorder = None
        self.lab.log = None
        self.clients = []

    def final_reads(self, leader: int) -> list[str | None]:
        values: list[str | None] = []
        for key in (f"k{i}" for i in range(4)):
            client = f"final{key}"
            self.recorder.record(
                client,
                "read",
                key,
                None,
                "linearizable",
                "invoke",
                node=f"node{leader}",
            )
            reply = self.call(leader, f"r {key}", 10)
            if reply and reply.get("ok"):
                self.recorder.record(
                    client,
                    "read",
                    key,
                    reply.get("value"),
                    "linearizable",
                    "ok",
                    node=f"node{leader}",
                )
                values.append(reply.get("value"))
        return values

    def partition(self, shape: Shape, row: dict[str, Any]) -> None:
        path = self.begin_history(shape)
        self.pump(1.0)  # the clients' steady state
        leader = self.leader()
        term = self.role[leader][1] if leader else 0
        majority = [n for n in range(1, self.nodes + 1) if n != leader]
        self.started_cut = self.cluster.now()
        self.minority = {leader}
        self.lab.isolate(leader)
        elected = self.pump(
            self.plan.partition_seconds + 10, lambda: self.leader(majority, term) != 0
        )
        new = self.leader(majority, term)
        election_ms = (
            (self.role[new][2] - self.started_cut) * 1000 if new else float("inf")
        )
        self.pump(
            max(
                0.0,
                self.plan.partition_seconds - (self.cluster.now() - self.started_cut),
            )
        )
        healed = self.cluster.now()
        self.lab.heal()
        self.minority = set()
        stepped = self.pump(
            5,
            lambda: (
                self.role.get(leader, ("", 0, 0))[0] != "leader"
                or self.role[leader][1] > term
            ),
        )
        rejoin_ms = (
            (self.role[leader][2] - healed) * 1000
            if stepped and self.role[leader][2] >= healed
            else 0.0
        )
        self.pump(self.plan.healed_seconds)
        current = self.leader()
        final = self.final_reads(current) if current else []
        self.end_history()
        log = self.history.load_log(path.read_text().splitlines())
        violations = self.history.check(log.ops, faults=log.faults)
        writes = [op for op in log.ops if op.op == "write" and op.outcome == "ok"]
        row["partition"] = {
            "election_ms": round(election_ms, 1),
            "bound_ms": self.bound_ms(shape),
            "rejoin_ms": round(rejoin_ms, 1),
            "operations": len(log.ops),
            "acknowledged_writes": len(writes),
            "violations": len(violations),
            "history": path.name,
        }
        for violation in violations[:5]:
            print(f"    violation: {violation}", flush=True)
        self.check(
            f"{shape.name}: elects after the cut",
            elected and election_ms <= self.bound_ms(shape),
            f"{election_ms:.0f} ms, bound {self.bound_ms(shape):.0f} ms",
        )
        self.check(
            f"{shape.name}: no acknowledgement from the cut leader",
            not self.minority_acks,
            f"{self.minority_acks[:3]}",
        )
        self.check(
            f"{shape.name}: history linearizable",
            not violations and len(final) == 4 and len(writes) > 0,
            f"{len(log.ops)} operations, {len(writes)} acknowledged writes, {len(log.faults)} faults, final reads {final}: {len(violations)} violations",
        )

    def run_shape(self, shape: Shape) -> dict[str, Any]:
        print(f"== {self.nodes} nodes, {shape.name}", flush=True)
        row: dict[str, Any] = {
            "shape": shape.name,
            "nodes": self.nodes,
            "set_rtt_ms": shape.rtt_ms,
        }
        self.shape(shape)
        self.check_rtt(shape, row)
        try:
            leader = self.start()
            if not leader:
                return row
            if shape.partition:
                self.partition(shape, row)
                return row
            self.check_majority(shape, row)
            leader = self.leader()
            row["small_latency"] = self.load(leader, SMALL, 1)
            row["small_throughput"] = self.load(leader, SMALL, self.plan.inflight)
            row["large_latency"] = self.load(leader, LARGE, 1)
            row["large_throughput"] = self.load(
                leader, LARGE, max(1, self.plan.inflight // 4)
            )
            row["rpc"] = self.rpc(leader)
            times = self.elections(shape)
            row["elections_ms"] = [round(t, 1) for t in times]
            if times:
                self.check(
                    f"{shape.name}: elections",
                    max(times) <= 3 * ELECTION_MAX_MS,
                    f"{[round(t) for t in times]} ms, 3 timeouts {3 * ELECTION_MAX_MS} ms",
                )
            for name in (
                "small_latency",
                "small_throughput",
                "large_latency",
                "large_throughput",
            ):
                if row[name].get("error") or not row[name].get("commits"):
                    self.check(f"{shape.name}: {name}", False, f"{row[name]}")
            return row
        finally:
            self.cluster.stop()


def fixture(directory: Path, run: Callable[[Sequence[str]], str]) -> Path:
    """The test fixture's certificates (test/transport/tls_certificates.cmake), for five nodes."""
    directory.mkdir(parents=True, exist_ok=True)
    if (directory / "five.pem").exists():
        return directory
    ossl = ["openssl"]
    run([*ossl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-subj", "/CN=Consensus-Test-CA", "-keyout", f"{directory}/ca.key",
         "-out", f"{directory}/ca.pem", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign",
         "-addext", "subjectKeyIdentifier=hash"])  # fmt: skip
    (directory / "leaf.ext").write_text(
        "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\n"
        "subjectAltName=DNS:localhost\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\n"
    )
    for name in ("one", "two", "three", "four", "five"):
        run(
            [
                *ossl,
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-subj",
                f"/CN={name}",
                "-keyout",
                f"{directory}/{name}.key",
                "-out",
                f"{directory}/{name}.csr",
            ]
        )
        run([*ossl, "x509", "-req", "-in", f"{directory}/{name}.csr", "-CA", f"{directory}/ca.pem", "-CAkey", f"{directory}/ca.key", "-CAcreateserial",
             "-days", "2", "-extfile", f"{directory}/leaf.ext", "-out", f"{directory}/{name}.pem"])  # fmt: skip
    return directory


def measure(bench: Bench, shapes: Sequence[Shape]) -> dict[str, Any]:
    """Runs each shape; the result holds every row and check, and ok when every check passed."""
    bench.lab.open({node: PORT for node in range(1, bench.nodes + 1)})
    try:
        bench.measure_floor()
        rows = [bench.run_shape(shape) for shape in shapes]
    finally:
        bench.lab.teardown()
    failed = [c for c in bench.checks if not c.ok]
    return {
        "nodes": bench.nodes,
        "floor_rtt_ms": bench.floor_ms,
        "plan": bench.plan.__dict__,
        "rows": rows,
        "checks": [c.__dict__ for c in bench.checks],
        "ok": not failed and bool(bench.checks),
    }


# The table: one row per shape and node count, each against its run's loopback row.


def _ms(value: Any) -> str:
    return "-" if value is None else f"{float(value):.2f}"


def table(results: Sequence[dict[str, Any]], jobs: dict[int, str]) -> str:
    head = (
        "| Nodes | Shape | RTT set / ping ms | Commit p50 / p99 ms (64 B) | (64 KiB) | Commits/s (64 B) | MB/s (64 KiB) "
        "| RPC p50 / p99 ms | Election ms (median, max) | vs loopback | Job |\n"
        "|---|---|---|---|---|---|---|---|---|---|---|\n"
    )
    lines = []
    for result in results:
        base = next((r for r in result["rows"] if r["shape"] == "loopback"), None)
        for row in result["rows"]:
            if "partition" in row:
                p = row["partition"]
                lines.append(
                    f"| {row['nodes']} | {row['shape']} | {row['set_rtt_ms']:g} / {_ms(row.get('rtt_ms'))} | - | - | - | - | - "
                    f"| {p['election_ms']:.0f} (bound {p['bound_ms']:.0f}); rejoin {p['rejoin_ms']:.0f} "
                    f"| {p['operations']} ops, {p['acknowledged_writes']} acked writes, {p['violations']} violations | {jobs.get(row['nodes'], '-')} |"
                )
                continue
            if "small_latency" not in row:
                continue
            sl, ll, st, lt, rpc = (
                row["small_latency"],
                row["large_latency"],
                row["small_throughput"],
                row["large_throughput"],
                row["rpc"],
            )
            el = row.get("elections_ms") or []
            versus = "-"
            if base is not None and base is not row and "small_latency" in base:
                versus = (
                    f"p50 +{sl['p50_ms'] - base['small_latency']['p50_ms']:.2f} ms; "
                    f"{st['per_s'] / max(base['small_throughput']['per_s'], 1e-9):.2f}x commits/s; "
                    f"{lt['mb_s'] / max(base['large_throughput']['mb_s'], 1e-9):.2f}x MB/s"
                )
            lines.append(
                f"| {row['nodes']} | {row['shape']} | {row['set_rtt_ms']:g} / {_ms(row.get('rtt_ms'))} "
                f"| {_ms(sl.get('p50_ms'))} / {_ms(sl.get('p99_ms'))} | {_ms(ll.get('p50_ms'))} / {_ms(ll.get('p99_ms'))} "
                f"| {st.get('per_s', 0):,.0f} | {lt.get('mb_s', 0):.1f} | {_ms(rpc.get('p50_ms'))} / {_ms(rpc.get('p99_ms'))} "
                f"| {f'{statistics.median(el):.0f}, {max(el):.0f}' if el else '-'} | {versus} | {jobs.get(row['nodes'], '-')} |"
            )
    return head + "\n".join(lines) + "\n"


def hunts(results: Sequence[dict[str, Any]]) -> list[str]:
    """What degrades past what the delay explains: commit p99 over 2 round trips plus loopback's
    p99, RPC p99 over a round trip plus loopback's, an election past 3 timeouts."""
    found = []
    for result in results:
        base = next(
            (
                r
                for r in result["rows"]
                if r["shape"] == "loopback" and "small_latency" in r
            ),
            None,
        )
        for row in result["rows"]:
            if "small_latency" not in row or base is None:
                continue
            rtt = row["set_rtt_ms"]
            for name in ("small_latency", "large_latency"):
                limit = 2 * rtt + base[name]["p99_ms"]
                if row[name]["p99_ms"] > limit:
                    found.append(
                        f"{result['nodes']} nodes, {row['shape']}: {name} p99 {row[name]['p99_ms']:.2f} ms > 2 RTT + loopback p99 = {limit:.2f} ms"
                    )
            limit = rtt + base["rpc"]["p99_ms"]
            if row["rpc"].get("p99_ms", 0) > limit:
                found.append(
                    f"{result['nodes']} nodes, {row['shape']}: RPC p99 {row['rpc']['p99_ms']:.2f} ms > RTT + loopback p99 = {limit:.2f} ms"
                )
            for t in row.get("elections_ms") or []:
                if t > 3 * ELECTION_MAX_MS:
                    found.append(
                        f"{result['nodes']} nodes, {row['shape']}: election {t:.0f} ms > 3 timeouts"
                    )
    return found


def load_faultlab(directory: Path) -> tuple[Any, Any]:
    """faultlab's netlab and history modules from the pinned checkout's python/ directory."""
    sys.path.insert(0, str(directory / "python"))
    from faultlab import (
        history,
        netlab,
    )

    return netlab, history


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="bench.py", description=__doc__.split("\n\n")[0]
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser(
        "run", help="measure every shape at one node count (Linux, root)"
    )
    p.add_argument(
        "--faultlab",
        type=Path,
        required=True,
        help="faultlab checkout at bench/netlab/FAULTLAB_REV",
    )
    p.add_argument(
        "--node", type=Path, required=True, help="the consensus_netlab_node binary"
    )
    p.add_argument("--nodes", type=int, default=3, choices=(3, 5))
    p.add_argument("--shapes", default=",".join(s.name for s in SHAPES))
    p.add_argument("--quick", action="store_true", help="the dry run's short plan")
    p.add_argument("--data", type=Path, default=Path("/dev/shm/netlab-consensus"))
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--seed", type=int, default=1)
    p = sub.add_parser("table", help="the markdown table and hunt rows of result files")
    p.add_argument("results", nargs="+", type=Path)
    p.add_argument(
        "--job",
        action="append",
        default=[],
        help="N=JOB: the job id of the N-node result",
    )
    args = parser.parse_args(argv)
    if args.command == "table":
        results = [json.loads(path.read_text()) for path in args.results]
        jobs = {int(k): v for k, _, v in (j.partition("=") for j in args.job)}
        print(table(results, jobs))
        for hunt in hunts(results):
            print(f"- hunt: {hunt}")
        return 0
    netlab, history = load_faultlab(args.faultlab)
    if (reason := netlab.unavailable()) is not None:
        print(f"bench.py: {reason}", file=sys.stderr)
        return 2
    chosen = [s for s in SHAPES if s.name in args.shapes.split(",")]
    random.seed(args.seed)
    args.out.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(args.data, ignore_errors=True)
    lab = netlab.Netlab(prefix="nlc")
    certs = fixture(args.data / "fixture", netlab.run)
    cluster = Processes(lab, args.node, certs, args.data, args.nodes)
    bench = Bench(
        lab,
        cluster,
        args.nodes,
        Plan.quick() if args.quick else Plan(),
        args.out,
        netlab.run,
        history,
    )
    result = measure(bench, chosen)
    (args.out / f"result-{args.nodes}.json").write_text(
        json.dumps(result, indent=1) + "\n"
    )
    print(table([result], {}))
    for hunt in hunts([result]):
        print(f"- hunt: {hunt}")
    failed = [c for c in result["checks"] if not c["ok"]]
    print(
        f"netlab-consensus {args.nodes} nodes: {len(result['checks']) - len(failed)} of {len(result['checks'])} checks passed: {'ok' if result['ok'] else 'FAIL'}"
    )
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
