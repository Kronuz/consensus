#!/usr/bin/env python3
"""Runs connection_test's slow_reader RUNS times under a forced host stall; exits 1 if any fails.

    test/transport/slow_reader_stops.py BINARY FIXTURE RUNS MODE [STOP_MIN_MS STOP_MAX_MS]

MODE: none; stop (SIGSTOP for STOP_MIN..STOP_MAX ms, 200..400 by default, 100..900 ms into each
run, from a fixed seed); spin (2 x ncpu busy processes for the whole run)."""

from __future__ import annotations

import os
import random
import signal
import subprocess
import sys
import time


def spinner() -> subprocess.Popen[bytes]:
    return subprocess.Popen([sys.executable, "-c", "while True: pass"])


def main() -> int:
    binary, fixture, runs, mode = (
        sys.argv[1],
        sys.argv[2],
        int(sys.argv[3]),
        sys.argv[4],
    )
    lo = int(sys.argv[5]) if len(sys.argv) > 5 else 200
    hi = int(sys.argv[6]) if len(sys.argv) > 6 else 400
    rng = random.Random(1)
    spinners = (
        [spinner() for _ in range(2 * (os.cpu_count() or 4))] if mode == "spin" else []
    )
    failed = 0
    try:
        for i in range(runs):
            p = subprocess.Popen(
                [binary, fixture, "slow_reader"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            note = ""
            if mode == "stop":
                at = rng.uniform(0.1, 0.9)
                stop = rng.uniform(lo, hi) / 1000
                time.sleep(at)
                p.send_signal(signal.SIGSTOP)
                time.sleep(stop)
                p.send_signal(signal.SIGCONT)
                note = f"stop {stop * 1000:.0f} ms at {at * 1000:.0f} ms"
            out, _ = p.communicate(timeout=120)
            lines = [
                line
                for line in out.decode(errors="replace").splitlines()
                if line.strip()
            ]
            ok = p.returncode == 0
            failed += not ok
            verdict = "pass" if ok else f"FAIL rc={p.returncode}"
            print(
                f"run {i}: {verdict} {note} | " + " | ".join(lines[-3:]),
                flush=True,
            )
    finally:
        for s in spinners:
            s.kill()
    print(f"{mode}: {failed} of {runs} failed", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
