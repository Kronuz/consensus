# AGENTS.md — journal

Working notes for anyone changing this repository. [README.md](README.md) is for users;
[ARCHITECTURE.md](ARCHITECTURE.md) explains the design. Header-only, in
`include/journal/`; the I/O backends come from
[completion](https://github.com/Kronuz/completion) at the commit `CMakeLists.txt` pins.

## Repo map

```
include/journal/
  journal.h       The per-group journal: append, checkpoint generations, recovery, migration.
  store.h         Store: the facade over both backends, with admission and inventory.
  turn_log.h      RFTW0001, the shared turn log (D11).
  turn_driver.h   One core's turns: groups stage records, one barrier per turn.
  artifact.h      Artifact builders, verifiers, readers, and their owned operations.
  mutation.h      Resumable append and manifest-publication state machines.
  reclamation.h   Bounded reclamation.
  inventory.h     The quiescent census.
  admission.h     Storage admission: permits, pools, settlement.
  resources.h     Footprint value types and checked arithmetic.
  codec.h         On-disk encodings.
  primitives.h    Shared helpers.
  memory_io.h     The injected power-loss filesystem the tests use.
test/             journal, inventory, admission, turn_log, turn_log_power (T9),
                  shared_store (R20), completion, linux_completion.
fuzz/             libFuzzer target for turn-log segments and checkpoint records, and its replay test.
bench/            append, checkpoint, reclaim, turn, and completion benchmarks.
examples/demo.cc  A small command-line tool.
docs/             MEASUREMENTS.md; journal.d2, the README diagram's source.
```

## Verify a change

```sh
cmake -S . -B build && cmake --build build && ctest --test-dir build --output-on-failure
```

The power-loss and shared-store tests allow up to 600 s; macOS makes a full flush per
turn. The fuzzer needs Clang: configure with `-DJOURNAL_FUZZ=ON` and run
`build/journal_fuzz`. CTest's fixtures create the build's own `.scratch/`, so a fresh
build directory needs nothing set up by hand. To redraw the README diagram:
`d2 --layout elk --sketch --theme 302 docs/journal.d2 docs/journal.svg`.

## Rules for storage code

- **Both drivers run the same states.** Every mutation is a resumable state machine;
  the synchronous API and the completion drivers execute the same states. A new
  primitive gets fault schedules before and after its side effect, both namespace
  outcomes, and both drivers.
- **Ownership outlives the facade.** An accepted operation retains its backend, files,
  buffers, and the owner lock until its original primitive is reaped. Only a matching
  owner, operation, and step completion advances it.
- **Barriers before settlement.** Final barriers precede frontier and accounting
  settlement. Executed I/O alone never publishes metadata.
- **Never invent outcomes.** Never invent a completion after cancellation, never refund
  a started permit before terminal settlement, and never retry a primitive through a
  second path after it may have executed.
- **Uncertainty fences.** Uncertain I/O fences journal and accounting together; caller
  errors and routine unavailability do not.
- **Formats are versioned.** A format change gets a new magic or version, a reader for
  the old one where the design keeps it, and a test that older binaries reject the new
  one.

## Traps

- **Do not offload `Worker::run_one()` as a whole** (in consensus); only its storage
  primitives run off the owner.
- **Fallback is not native.** completion's automatic fallback keeps a backend working; it
  does not qualify the native path. Report which ran.
- **Startup is synchronous.** Bootstrap and recovery run before admission; only the
  live append, preparation, publication, verification, read, and reclamation paths are
  completion-driven.
- **Do not describe the opt-in append profile as a fully asynchronous service.** The
  default library paths remain synchronous.
