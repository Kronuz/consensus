#!/usr/bin/env python3
# Copyright © 2026 Germán Méndez Bravo (Kronuz)
# SPDX-License-Identifier: MIT
#
# Usage: full_window_check.py OUT (a netlab bench run's result-3.json and diag-3.jsonl; docs/mechanisms/append-pacing.md).
"""A3's bar on a netlab run: after a 10 s warmup of the sustained large load, every 5 s window
advances the applied index, no role changes during the load, and large_throughput answers."""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main(out: Path, label: str = "65536B-32") -> int:
    result = json.loads((out / "result-3.json").read_text())
    ok = True
    for row in result["rows"]:
        lt = row.get("large_throughput") or {"error": "no row"}
        seconds = float(lt.get("seconds", 0))
        print(
            f"== {row['shape']}: large_throughput {json.dumps({k: lt.get(k) for k in ('commits', 'per_s', 'mb_s', 'p50_ms', 'p99_ms', 'error', 'failures')})}"
        )
        roles = lt.get("roles", [])
        print(f"   role changes during the load: {roles}")
        samples = []
        for line in (out / "diag-3.jsonl").read_text().splitlines():
            s = json.loads(line)
            if s.get("load") != label:
                continue
            applied = [
                int(v["applied"])
                for v in s.get("status", {}).values()
                if "applied" in v
            ]
            if applied:
                samples.append((float(s["t"]), max(applied)))
        windows = []
        if samples and seconds:
            t0 = samples[0][0]
            at = lambda t: max((a for when, a in samples if when <= t), default=0)  # noqa: E731
            edges = [t0 + 10 + 5 * i for i in range(int((seconds - 10) // 5) + 1)]
            windows = [(round(b - t0), at(a), at(b)) for a, b in zip(edges, edges[1:])]
        stalled = [w for w in windows if w[2] <= w[1]]
        print(f"   windows (end s, applied at start, at end): {windows}")
        passed = "error" not in lt and not roles and bool(windows) and not stalled
        print(
            f"   {'PASS' if passed else 'FAIL'}: answered={'error' not in lt} role_changes={len(roles)} windows={len(windows)} stalled={len(stalled)}"
        )
        ok = ok and passed
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1])))
