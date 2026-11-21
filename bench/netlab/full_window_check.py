#!/usr/bin/env python3
# Copyright © 2026 Germán Méndez Bravo (Kronuz)
# SPDX-License-Identifier: MIT
#
# Usage: full_window_check.py OUT (a netlab bench run's result-3.json and diag-3.jsonl; docs/mechanisms/append-pacing.md).
"""A3's bar on a netlab run: after a 10 s warmup of the sustained large load, every 5 s window
advances the applied index, no role changes during the load, and large_throughput answers.

The windows end where the load does: the bench stops sampling when the load answers, which is
at its seconds or sooner, when it commits its 128 MiB cap (2048 x 64 KiB). A load that answers
early passes only with the whole cap committed and no failures."""

from __future__ import annotations

import json
import sys
from itertools import pairwise
from pathlib import Path


def applied_at(samples: list[tuple[float, int]], t: float) -> int:
    """The highest applied index sampled at or before `t`."""
    return max((a for when, a in samples if when <= t), default=0)


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
        samples: list[tuple[float, int]] = []
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
        ran = 0.0
        if samples and seconds:
            t0 = samples[0][0]
            ran = min(seconds, samples[-1][0] - t0)
            edges = [t0 + 10 + 5 * i for i in range(int((ran - 10) // 5) + 1)]
            windows = [
                (round(b - t0), applied_at(samples, a), applied_at(samples, b))
                for a, b in pairwise(edges)
            ]
        stalled = [w for w in windows if w[2] <= w[1]]
        print(
            f"   load ran {ran:.1f} s of {seconds:g}; windows (end s, applied at start, at end): {windows}"
        )
        # A load that answers early must have committed the whole cap, with no failures.
        cap = (128 << 20) // int(label.split("B-")[0])
        whole = ran >= seconds - 1 or (
            lt.get("commits") == cap and not lt.get("failures")
        )
        passed = (
            "error" not in lt and not roles and bool(windows) and not stalled and whole
        )
        print(
            f"   {'PASS' if passed else 'FAIL'}: answered={'error' not in lt} role_changes={len(roles)} windows={len(windows)} stalled={len(stalled)} whole={whole}"
        )
        ok = ok and passed
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1])))
