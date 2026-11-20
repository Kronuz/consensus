# Idle-group bytes

An idle consensus group costs its heap, not CPU ([quiescence](../mechanisms/quiescence.md)).
This is what one idle row holds: a lone voter opened through the Driver on a journal host,
created, one command written and applied, and a checkpoint covering it, so it can close. The
Driver's row, its Worker, Core and journal group, against Xapiand's 6,984 B for a consensus row
and its journal group (Xapiand:d5b84cdf4, at 64 groups).

## Per row, by stage

`driver_close_test idle_bytes`: the process heap the test's counting `operator new` sees
(requested bytes; the test's in-memory files left out), 1,000 rows opened together, per row.

| Stage | Before (consensus:b8de3b8) | After (consensus:95eed40) |
|---|---:|---:|
| opened (the Driver's row, Worker built, not created) | 3,693 B | 3,269 B |
| created (leading, its founding records) | 6,697 B | 5,593 B |
| written and applied | 7,027 B | 5,923 B |
| **idle at its checkpoint** | **6,807 B** | **5,703 B** |

Jobs: before 261120-101850-c631af (the test over b8de3b8's `core.h`, `worker.h`, `types.h`),
after 261120-101900-6202e7. The idle row is 1,104 B (16%) smaller, and 1,281 B below Xapiand's
measure.
With the rest and the node-pair detector (consensus:3f3f664) the stages are 3,285, 5,609, 5,939
and 5,719 B, 16 B more idle (job 261120-124658-f2feae), still 1,265 B below Xapiand's.

## By owner

`test/idle_owners.py`: malloc's blocks live at `idle_bytes_pause`, charged to the innermost
owner frame (`malloc_history -callTree -invert`), 400 rows past 200, per row (job
261120-101825-a635c0, at consensus:95eed40). Blocks run above the requested bytes: each carries
the test's 16 B counting header and rounds to malloc's size class.

| Owner | Bytes | What |
|---|---:|---|
| consensus Worker | 1,632 | the Worker object (1,416 B), its Store's handle |
| consensus Core | 1,152 | the Core object (1,008 B), its retained log |
| consensus Driver row | 625 | the row (`Driver::Group`, 433 B) and its table node |
| journal Journal | 925 | the group's `Journal` (845 B) |
| journal other | 816 | the replacement reservation (240 B), the group's directory view (208 B), records |
| journal Host table | 671 | the group's directory view and scan cursor, its table entry |
| journal admission | 350 | the group's `Admission` (286 B) |
| journal turn log | 242 | the group's notes in its loop's turn log |
| journal Store | 48 | the `Store` handle |
| **total** | **6,463** | |

## What was reserved and unused, now released

The idle row's heap was mostly its Core and Worker objects, which carried every rare state
inline. Those now live behind a pointer allocated only while in use (consensus:95eed40):

| State | Inline before | Idle now |
|---|---:|---:|
| Core: a checkpoint's frozen capture | 320 B | 8 B |
| Core: a snapshot installation | 248 B | 8 B |
| Core: a leadership transfer | 40 B | 8 B |
| Core: the wire-limits cache (three caps, not a whole `Limits`) | 104 B | 48 B |
| Core: the pre-vote term (0 for none) and the flags beside a one-byte `Role` | 56 B | 16 B |
| Worker: a snapshot result | 304 B | 8 B |
| Worker: the founding roles (before create only) | 136 B | 8 B |

`sizeof(Core)` 1,688 to 1,008 B, inside macOS malloc's tiny size classes (up to 1,008 B);
`sizeof(Worker)` 1,832 to 1,416 B.

## A three-voter leader: its peers' flights

A lone voter has no peers. A three-voter leader kept, per peer, a `std::deque<Flight>`, and
libc++'s deque holds a 4 KiB block once it has held a flight, empty or not: 8 KiB of an idle
leader. The flights are a vector now (consensus:8611768), at most `Flow::pipeline` of them.
`driver_close_test idle_cpu`, 100,000 leaders at rest, process RSS (the HeapIO files and two
256 MiB turn-log segments included):

| | Flights in a deque | In a vector |
|---|---:|---:|
| RSS, 100,000 idle leaders | 2,143 MB | 1,306 MB |
| per leader, the difference | | **8.4 KB less** |

Jobs 261120-111929-1f3e62 (probe 03e3b99, 8611768's tree with d291fac's `core.h`) and
261120-111932-71ec5b.

## What stays, and why

- **The Worker's admission pack (240 B) and pending persist (96 B), the Core's pending
  persistence (104 B).** Every admitted event fills the pack and every persistence the pending
  slots: boxing them would add an allocation per proposal.
- **The Worker's `Store` (240 B inline).** The group's handle on its journal, used by every
  append.
- **Configuration copies.** The Core, the Worker and the Driver's row each keep the group's
  `Timing` (48 B), and the Core and the Worker its `Limits` (80 B) and `FixedConfiguration`
  (64 B): about 320 B a row. The Worker needs them before its Core exists and the Core takes
  none by reference, so one shared copy is the next cut, not taken here.
- **The journal's 3,052 B** (Journal, directory view, admission, the replacement reservation,
  turn-log notes, the table entry) are the journal library's, measured here for its own quest:
  the replacement reservation (240 B) and the directory scan cursor (160 B) held while idle are
  the first candidates there.

## Reproduce

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --target consensus_driver_close_test
build/consensus_driver_close_test idle_bytes          # IDLE_BYTES_ROWS=N for another count
python3 test/idle_owners.py build 200                 # macOS: lldb and malloc_history
```
