# Changelog

Notable changes to `consensus`, newest first. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); the library has no releases, so
entries are dated.

Upgrading a pin from before 2026-10-30: [the consumer upgrade note](https://github.com/Kronuz/cpp-libraries/blob/main/docs/UPGRADING-2026-10.md#consensus) lists what changes for callers, how to find affected inputs, the commits not to pin, and the speedup.

## 2026-11-13

### Added

- A fetch lane on `Peers`: `fetch(peer, name, done)` asks a peer for a named file and
  `done(error, bytes)` gets its exact bytes, `nullopt` when the peer holds none, or an error
  (`not_connected`, `timed_out`, `connection_aborted`, ...), once, on the owner executor. The
  peer answers through `PeerOptions::reader`, a host function from a name to optional bytes.
  Requests and replies are new `DTFT0001` frames on the bulk lane beside snapshot transfer
  frames (`fetch.h`); a reply spans as many lane frames as it needs, each charged to the
  shared byte budgets one frame at a time. `PeerOptions::fetches` (4) bounds a voter's fetches
  awaiting one peer, `fetch_bytes` (64 MiB) a reply, and `fetch_deadline` (30 s) the wait; a reply still unfinished a deadline after its fetch failed
  closes the bulk lane, since its peer is hung or gone.
  A voter on an older consensus closes the bulk lane on a fetch frame. `PeerStats` counts
  `fetched`, `fetch_failures` and `served`; `PeerOccupancy` counts `fetches` and `serving`.
  See [docs/mechanisms/peer-fetch.md](docs/mechanisms/peer-fetch.md).

## 2026-11-11

### Changed

- A `Peers` whose `PeerOptions::listen` is `::` opens its acceptor with `v6_only` off, so one
  socket serves IPv4 and IPv6 peers on every OS, FreeBSD's v6-only default included. Other listen
  addresses bind as before.
- `PeerOptions::addresses` maps each peer to a `PeerAddress{host, port}`: an IP literal, or a
  host name looked up on a lookup thread of its own (`resolve.h`). A dial of a cached name goes
  to the cached addresses at once while a lookup refreshes them; only a name with none waits, up
  to `resolve_deadline` (1 s), which also spaces lookups of one name. A hung resolver parks one
  thread per name and blocks nothing else. `resolver` (default
  `system_resolver`, `getaddrinfo`) is injectable. Callers that set endpoints pass
  `{address.to_string(), port}`.
- A dial tries every resolved address, IPv6 and IPv4 interleaved, starting the next one
  `stagger` (250 ms) after the one before (RFC 8305); the first to connect wins.
- A dial pins only the peer it dialed, so an address that reaches another voter is refused at
  the handshake before either end admits a hello; the next dial of that peer starts after it.
- `PeerStats` counts `lookups`, `lookup_failures` and `lookup_timeouts` (uncached dials whose
  lookup was late);
  `PeerOccupancy::lookups` counts lookups in flight. See
  [docs/mechanisms/peer-dns.md](docs/mechanisms/peer-dns.md).

## 2026-11-02

### Changed

- The completion test and bench await completion's driver as `task::task<void>`: `AsioCompletionDriver`
  now takes a `task::asio_scheduler&` and its `run()` returns `task::task<void>` (completion's
  `phase/task-driver`). The test's coroutines, sockets and timers move to `task::await_asio`, and its
  cancel request (10 ms into a 100 ms held drive) is a `task::cancel_token` the drive still outlives.
  The two targets link `completion::asio`, and the standalone build sets `COMPLETION_ASIO`. The
  library headers are unchanged.
- Pins journal at `d6d009e`, its `main` head, which pins completion at `576618d`, its `main` head
  (the task driver, with task pinned at its C3 head).
- Pins journal at `c38f708`, its head, which pins completion at `e2c6474` (completion's
  metrics and allocators pins at their heads).

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
