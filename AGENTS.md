# AGENTS.md — consensus

Working notes for anyone changing this repository. [README.md](README.md) is for
users; [ARCHITECTURE.md](ARCHITECTURE.md) explains the design. Header-only, in
`include/consensus/`; storage is [journal](https://github.com/Kronuz/journal) at the
commit `CMakeLists.txt` pins.

## Repo map

```
include/consensus/
  core.h               Core: the deterministic Raft state machine.
  types.h              Events, actions, messages, Limits, Timing, Flow.
  membership.h         Configuration entries, roles, incarnations.
  retained_log.h       The retained log and its accounting.
  admission.h          plan_event: continuation packs reserved before a step.
  storage.h            Initialization, storage batches, and semantic Recovery.
  checkpoint*.h        Local checkpoint bundles: format, encoder, validation.
  snapshot.h           The portable snapshot descriptor (RFTSNP02).
  snapshot_transfer.h  Transfer frames (RFTXFR01).
  snapshot_channel.h   SnapshotChannel: the windowed, credited transfer between Workers.
  worker.h             Worker: admission, persistence, checkpoints, sources, retirement.
  retirement.h         Bounded teardown of Core state.
  transport/           The optional TLS peer transport (consensus::transport).
test/                  core, worker, simulation, liveness, membership, pipeline,
                       shared journal, compatibility, checkpoint encoder; transport/.
test/fixtures/         Stores written by older revisions (b7073bb, c1) for compatibility.
fuzz/                  libFuzzer target for every decoder, its corpus, and a replay test.
tools/                 mutants.sh, compat.sh, fuzz.sh, coverage.sh.
bench/                 checkpoint and Worker benchmarks.
examples/demo.cc       A single-voter program around the Core loop.
docs/                  MEASUREMENTS.md; consensus.d2, the README diagram's source.
```

## Verify a change

```sh
cmake -S . -B build -G Ninja && cmake --build build && ctest --test-dir build --output-on-failure
tools/mutants.sh build        # every seeded fault must be caught
tools/compat.sh               # old stores recover; old readers refuse new formats
```

`worker_test` has focused modes (`--completion-regressions`, `--review-regressions`,
`--transfer-benchmark`). To redraw the README diagram:
`d2 --layout elk --sketch --theme 302 docs/consensus.d2 docs/consensus.svg`.

## Rules for protocol code

- **The core stays deterministic.** No clocks, randomness, I/O, or callbacks in
  `core.h`; everything enters as an event and leaves as an action.
- **Durable before visible.** Nothing that depends on a persisted change (a reply, a
  commit advance, an application delivery) is released before its matching
  `Persisted`.
- **Every safety property has a mutant.** A new rule gets a simulation case, and a
  seeded fault in `tools/mutants.sh` that the harness must catch.
- **Formats are versioned and pinned.** A wire or storage change gets a new magic or
  version, a fixture written by the old code that the new code still reads, and a
  test that the old reader refuses the new format (`tools/compat.sh`).
- **Additive by default.** Detent links this library. A new knob defaults to today's
  behavior (`Flow{pipeline = 1}`, `SnapshotLimits::window = 1`, follower batching
  off); a change that cannot be additive is coordinated with Detent first. Record
  each landed change in Detent's `docs/LIBRARY_BOUNDARIES.md`.
- **Limits are load-bearing.** Every cap names the mechanism it protects and is
  checked at construction.

## Traps

- **A transport receipt is not an acknowledgement.** Only a matching response or
  `Persisted` advances anything.
- **Never fabricate completions.** Not after cancellation, not on teardown;
  `Worker::close()` reaps only operations that already exist.
- **The Worker is single-executor.** Its APIs, callbacks, spans, and storage
  objects stay on the owning executor; borrowed views expire on consumption.
- **Do not offload `Worker::run_one()` as a whole;** only storage primitives run off
  the owner.
- **Destroy a `SnapshotChannel` only once `drained()`,** after closing its sessions.
