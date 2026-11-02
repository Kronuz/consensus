# Changelog

Notable changes to `consensus`, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the library has no releases, so
entries are dated.

Upgrading a pin from before 2026-10-30: [the consumer upgrade note](https://github.com/Kronuz/cpp-libraries/blob/main/docs/UPGRADING-2026-10.md#consensus) lists what changes for callers, how to find affected inputs, the commits not to pin, and the speedup.

## 2026-11-01

The library's code is unchanged; its journal and reactor pins move, the demo commits its
proposal, and the README changes.

### Changed

- The journal pin moves from `cb66f3d` to `eb99e70` (`6e2b73f`, `35c0f66`). journal fetches
  allocators and crc32c: its `crc32c()` and `Checksum` forward to `kronuz::crc32c` and store the
  same bytes. Its completion pin moves to `42e74ea`, which returns macOS barrier errors other
  than `ENOTSUP`, `EOPNOTSUPP` or `ENOTTY`, where they were retried into success, and brings
  completion's single owner lock and metrics' `AtomicCounter` alias.
- The reactor pin of the optional `consensus::transport` target moves from `e19afd9` to
  `c6b2c3e` (`6e2b73f`, `35c0f66`). Timer and posted-callback exceptions go to `L_EXC`, and a
  repeating `PeriodicTimer` rearms before it propagates one (`fe8c5ce`). The range also removes
  reactor's worker pools and changes the `TcpServer` and `UdpServer` constructors (`67e22a6`);
  the transport uses only `reactor::tls`, so its API is unchanged.
- The README follows the family README standard (`d82be69`): the sections in the standard's
  order, a FetchContent snippet pinned to a commit, and a quick start that is a complete
  single-voter program with its output, whose `Tick` carries an election delay inside `Timing`'s
  bounds. The retained-state notes move to `docs/RETAINED-STATE.md`. The diagram runs top to
  bottom (803 x 562) and reads on light and dark pages (`5796699`).

### Added

- A Performance section in the README (`793a203`), from `consensus_worker_checkpoint_bench` at
  `6e2b73f` on the Intel Core i9-9980HK, the range over three runs: with 64 to 65,535 retained
  commands of 1 KiB and a 1 MiB image, a whole checkpoint takes 217 to 409 ms, the core freeze
  148 to 342 ms, and the longest frozen turn 84 to 88 ms. The Apple M4 Pro runs did not repeat,
  so it has no row. There is no comparator: other Raft libraries persist through their own log
  stores and snapshot formats.

### Fixed

- The demo ticked at an election delay of 100, below `Timing::election_min` (1000), so `Core`
  ignored the tick, the lone voter never campaigned, and the demo exited 1 with
  `proposal rejected: 1`. It ticks at 1000, the delay the README's quick start uses, and commits
  its proposal (`259b217`).

### Removed

- `build.cfg`, the output of one local CMake run committed by mistake; no build or document read
  it (`da14acd`).
