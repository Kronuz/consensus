# The RPC lane: calls and streamed bodies on the host transport

A request/response lane for application calls between any two nodes, on the node-pair
connections consensus already runs, with bodies streamed under the lane's own credit.

- Status: design, revision 1, 2026-11-16, ready for review (lens `design`). Quest
  `xapiand:consensus-rpc-lane`; contract `consensus:rpc-lane` (wire), reviewed again by
  `format-freeze`.
- Read at: consensus `main` `d1065e0`; Detent `main` `8db10cb4` (`docs/design/async-drivers.md`,
  quests `consensus-transport`, `consensus-driver`); strata `docs/design/track-c.md`; Xapiand
  `docs/design/cluster-release.md`.
- Built on: `detent:consensus-transport` (Track C D16, async-drivers D10), which is planned and not
  started. Every place this design needs something from it is a numbered requirement (T1-T7).
- Nothing here is built or timed. The one run is the model below (`rpc-lane-model.py`).

## Abstract

Xapiand's cluster needs node-to-node calls: shard search, `get` of a stored blob up to 4 GiB,
`forward_write` of a streamed upload, keepalives to the root leader (cluster-release D6, D9, D10).
Detent needs one more: its history fetch (Detent msg-44, option 1), today the `DTFT0001` fetch
lane. Cluster-release D9 rules that they all ride the transport consensus already authenticates;
a second TLS listener or HTTP between nodes is rejected.

The recommendation: one lane on the node pair's **Bulk** connection, five frame kinds (`Call`,
`Body`, `Credit`, `Cancel`, `Error`, with "end" as a flag) behind one 24-byte header, versioned
by a range in the connection hello. Every body byte travels in a `Body` chunk of at most 64 KiB,
under a per-call window the receiver grants from its own lane budget. The budget is separate
from every group's credit, so a stalled call cannot hold a heartbeat and a stalled group cannot
hold a call. Lane traffic can open a connection to a node that shares no group. Consensus's
`fetch` becomes method 1 on the lane, and `DTFT0001` is deleted.

An exhaustive model of two calls on one connection (94,139 states) finds no budget overrun, no
double completion, no deadlock and no false protocol violation, under every interleaving of
cancels, deadlines on both sides, errors, refusals and a close. Each of its five mutants fails.

## Why not the obvious things

**Generalize `DTFT0001` in place.** Its rules do not survive cancellation or general use. It closes
the bulk lane on a reply to an id it no longer awaits (`peers.h:380-381`), which a cancel makes
routine. It closes the lane when a peer exceeds `fetches` (`peers.h:373-374`), so voters must
agree on the bound (`peer-fetch.md:52-54`). It takes credit from the `outgoing_bytes` budget that
control frames share (`peers.h:177`, `:779`; `peer-fetch.md:58`). It has no method, deadline or
request body. The model's `forget-is-violation` mutant is its first rule. Under cancels, honest
runs close the lane.

**Raise the frame bound and send a body as one frame.** The transport's length prefix is 32 bits
(`session.h:177-181`). The receiver also allocates each frame whole against a shared budget, and
throws when that budget is short (`session.h:62-64`, `:168`). A 4 GiB frame would need 4 GiB of
receive memory, and it would sit in front of everything behind it on its socket. A2's control
mutant is exactly this.

## Decisions

**D1. The lane rides the Bulk connection.** Track C D16 gives each node pair a Control connection
(Raft messages, control frames first, appends of at most 256 KiB) and a Bulk connection (snapshot
chunks) (`track-c.md:762-789`). Lane frames go on Bulk, as `DTFT0001` does today. On Bulk, the
lane's small frames (`Call`, `Credit`, `Cancel`, `Error`) go before any queued chunk. `Body` chunks
alternate with snapshot chunks while both wait, which generalizes `fetch_turn` (`peers.h:309`).
Calls round-robin among themselves. A heartbeat therefore never shares a socket with a body. A
call's small frames wait behind at most one 64 KiB chunk, plus the 64 KiB of unsent socket bytes
D16 bounds.
*Rejected:* Control, where search responses would compete with appends for the write path's
bandwidth; a third connection per pair, which adds a descriptor and about 41 KB per peer
(async-drivers D10) for a separation Bulk already gives; splitting a call across both
connections, where an end on Control could overtake the last chunk on Bulk.

**D2. Frames.** One lane frame is one transport frame. All integers are little-endian, written
by journal's `put64` and `put32` (`journal/codec.h:44-49`), with fixed widths like `DTC2` and
`DTFT0001`. A `Body` header is 40 bytes, 0.06% of a 64 KiB chunk. Varints were rejected: they
save part of those 40 bytes and add a decode path to fuzz.

| Offset | Bytes | Field | Rule |
|---|---|---|---|
| 0 | 8 | magic | `DTRP` and the negotiated version in four digits: `DTRP0001`; any other value closes the connection |
| 8 | 1 | kind | 1 `Call`, 2 `Body`, 3 `Credit`, 4 `Cancel`, 5 `Error` |
| 9 | 1 | flags | bit 0 `end` (`Call`, `Body` only); other bits zero |
| 10 | 2 | zero | |
| 12 | 4 | word | `Call`: method id, nonzero; `Cancel`, `Error`: code, nonzero; else zero |
| 16 | 8 | call | nonzero; odd from the end that dialed the connection, even from the end that accepted it |

| Kind | Fields after the header | Validity |
|---|---|---|
| `Call` | `timeout_ms` u64, `window` u64, `total` u64, then up to 65,536 request bytes at offset 0 | `timeout_ms` nonzero; `window` is the caller's first response credit limit; `total` is the request length, or 2^64-1 when unknown; `end` means the request ends with these bytes |
| `Body` | `offset` u64, `total` u64, then 1 to 65,536 bytes (0 only with `end`) | `offset` is the next expected byte; `total` equals the stream's first declared value; `offset + n <= total`; `end` requires `offset + n == total` when the total is known |
| `Credit` | `limit` u64 | cumulative: the peer may send that stream's bytes up to offset `limit`; it strictly increases |
| `Cancel`, `Error` | up to 1,024 bytes of diagnostic text | the call ends; see D4 |

The largest frame is 65,584 bytes (a `Call` with 65,536 inline bytes), under Bulk's existing
bound of `maximum_transfer_bytes` = 65,688 (`snapshot_transfer.h:37`), so Bulk keeps one bound.
A frame's direction follows from the call id's parity. A `Body` or `Credit` sent by the caller is
for the request stream or the response stream respectively, and the server's are the reverse, so
no direction bit exists. `Error` codes 1-255 belong to the lane: `unknown_method`, `overloaded`,
`deadline_exceeded`, `cancelled`, `message_size`, `handler_failed`, `shutting_down`. Codes of 256
and above belong to the method and reach the caller unchanged. `Cancel` uses `cancelled`,
`deadline_exceeded`, `message_size` and `source_failed`.
*End is a flag, not a kind.* A one-frame call stays one frame, and every stream ends one way.

**D3. Versioning through the hello's range (T1).** The connection hello carries `rpc_min` and
`rpc_max` (u64 each, as every hello field is, `hello.h:24-30`). Both ends run
`v = min(max_a, max_b)` when `v >= max(min_a, min_b)`. Otherwise the lane is off on that
connection: calls fail with `protocol_not_supported`, and group frames are unaffected. Version 1
is this document. Bytes may still change before `format-freeze` (Xapiand D87), and a change
bumps the version.

**D4. A call's life, and late frames.** The caller allocates increasing ids of its parity. Every
call ends exactly once at each end, by one of five outcomes:

1. The response's `end`.
2. An `Error` from the server.
3. A `Cancel` from the caller (explicit, or its deadline).
4. The server's deadline, answered with `Error deadline_exceeded`.
5. The connection closing, which fails every live call with `connection_aborted`.

The server may end the response before it reads the whole request, and it discards the rest.
Once an end is done with a call, the id is *retired*. A frame for a retired id is discarded and
counted (`late`), never treated as a violation, because cancels and deadlines cross frames in
flight. These close the connection: a frame for an id above any the peer allocated, a reused
`Call` id, a chunk past its credit or out of offset order, a total that changes, or a `Credit`
that does not increase. Each one means a peer bug, under the trusted-cluster threat model.
`timeout_ms` is a relative budget, because nodes' clocks differ. The server's deadline is arrival
plus the budget, clamped to its `max_call_deadline` (1 h by default). Call deadlines follow
consensus-transport A6's one rule for late timers after a process stop (T7).

**D5. Credit: per-call windows from a lane budget, independent of groups.**

- Each host has its own lane budgets, `rpc_incoming_bytes` (256 MiB by default, D15's node send
  budget) and `rpc_outgoing_bytes`. Group frames use theirs. Neither ever charges the other (T2).
- The receiver of a stream reserves budget before it grants credit, so every chunk that arrives
  within credit already has its memory. The reader never waits on a consumer, the hazard
  `reader_test` shows (a follower's heartbeats held 3,000 ms behind a busy group, async-drivers
  D10).
- The caller grants the response's first window in `Call.window`. If its budget is empty, it
  waits locally before it sends the `Call`. The server grants the request's first window in a
  `Credit` when it accepts the call. When the consumer has taken half a window, the receiver
  raises `limit` to `consumed + min(window, free budget)`. The window defaults to 4 MiB,
  D15's `window_bytes`: 4 GB/s at a 1 ms round trip (arithmetic, not measured). With a grant every half window,
  a 4 GiB body takes 2,048 `Credit` frames.
- A live call never drops to zero credit while its stream is unfinished. A grant happens only as
  its own consumer frees budget, and `free >= 1` then. So no grant waits on another call. The
  model's `grant-on-receive` mutant (credit raised when bytes arrive) overruns the budget.
- Sending is bounded the same way. A writer awaits credit, so its queued bytes are at most one
  chunk beyond what the peer granted.
- `Call` frames are not credit-gated. A server at its serving bound (256 calls per peer, above
  the concurrent-stream defaults of nginx, 128, and Go's HTTP/2 server, 250) or without budget for one chunk answers
  `Error overloaded` at once and frees the frame. It never queues the call or closes the
  connection.
- *Rejected:* a lane-wide connection window beside the per-call windows (HTTP/2 and QUIC keep
  both, as D16 does for groups). The grantor already owns the memory, so a second window bounds
  nothing more.

**D6. Streaming API (shape; exact types follow `consensus-transport`'s `task` API).**

```cpp
using MethodId = std::uint32_t;  // 0 invalid; 1-65535 consensus's; above, the host's
struct CallOptions { std::chrono::milliseconds deadline; std::size_t window = 4 << 20;
                     std::optional<std::uint64_t> request_total; };
class Stream {  // one direction of one call's body
  public:
	task::task<std::optional<Chunk>> next();     // nullopt at end; releasing a Chunk returns credit
	task::task<void> write(std::string_view);    // waits for credit; splits into 64 KiB chunks
	task::task<void> finish();
};
struct Call { NodeId peer; Stream request, response; };   // peer: the authenticated node
task::task<Call> Lane::call(NodeId, MethodId, CallOptions, std::string inline_request = {});
void Lane::serve(MethodId, std::function<task::task<void>(Call &, task::token)>);
task::task<std::string> collect(Stream &, std::size_t limit);  // a whole body, or message_size
```

A call's state lives on its connection's loop. A consumer on another loop pays the D10 hop per
chunk: 832 ns of CPU posted alone (job 261111-012904-94206c), per 64 KiB.

**D7. Nodes that share no group (T3).** The lane holds a connection open while it has a live call
or a host `hold(node)`, the root-leader keepalive of cluster-release D6. It also keeps it for
`lane_idle` (60 s) after the last call. The peer set becomes the union of the local groups'
members and the lane's demand. Addresses come from the host's resolver (`peer-dns.md`), and the
peer is admitted by its node pin (async-drivers Q7 (a)). A lane-only pair opens Bulk only;
Control opens when a group becomes shared. Lane bytes and calls count in per-peer `LaneStats`
(calls by outcome, bytes in and out, `late`, budget high water) and never in a group's credit or
`PeerStats`. Liveness on a lane-only connection is the transport's per-connection keepalive (T4).

**D8. History fetch is method 1.** `Peers::fetch(peer, name, done)` keeps its signature and its
outcome table (`peer-fetch.md:16-25`). It becomes `Lane::call(peer, 1, {fetch_deadline},
name)`, served by the host's `FetchReader`, and the outcomes map one to one:

| Outcome today | On the lane |
|---|---|
| The peer holds the name, so its bytes | `Body` with `end`; `total` is the file's size |
| The peer holds none | `Error` 256 (fetch's `missing`) |
| The reader threw, or the reply is over the peer's `fetch_bytes` | `Error handler_failed`, `Error message_size` → `io_error` |
| The reply is over this voter's `fetch_bytes` | the first `Body`'s `total` → `Cancel message_size` → `message_size` |
| `fetch_deadline` | the call's deadline → `timed_out` |
| `fetches` await the peer | the caller's local bound → `resource_unavailable_try_again` |
| The lane closed, or no lane | `connection_aborted`, `not_connected` |

The "one more deadline, then close the lane" rule for a hung peer (`peer-fetch.md:94-101`) goes.
`Cancel` frees the server's slot, and the per-connection keepalive (T4) detects a half-open peer.
`DTFT0001`'s codec and `Peers`' `Fetching`/`Serving` state (`peers.h:275-310`, `:369-430`) are
deleted, together with their decoder in `fuzz_decoders.cc`. Consensus then has one fetch
mechanism. That holds only if Q1 is answered (a).

## Requirements on `consensus-transport`

| # | Need |
|---|---|
| T1 | The hello carries `rpc_min` and `rpc_max`; the 120-byte `DTHL0001` (`hello.h:17`) becomes `DTHL0002` |
| T2 | Bulk routes a frame by its first 8 bytes, as today (`peer-fetch.md:33-34`). Its sender keeps a small-frames queue and alternates chunk classes (D1). The lane's budgets are their own `ByteBudget`s |
| T3 | `Transport::hold(node)` and `release(node)` keep a Bulk connection to a node outside every group |
| T4 | A keepalive on every connection, a lane-only Bulk included (its A5: "once per connection") |
| T5 | The reader hands the lane each frame synchronously on the connection's loop, from a per-connection read buffer, so a frame never waits on a shared budget (`session.h:168` does today) |
| T6 | Duplicate dials resolve to one connection before any lane frame, so call-id parity is defined |
| T7 | Its A6 late-deadline rule covers call deadlines, in its one place |

## Acceptance criteria as checks

| | Check | Control |
|---|---|---|
| A1 | `test/transport/rpc_test.cc` over TLS loopback, two hosts of 4 loops each, under TSan in CI. (1) A deadline fires at the caller and the server's token is cancelled. A mid-stream cancel ends both ends once. Crossing cancel and end both pass. Budgets return to 0. (2) Group 1's admission is full for 3 s while calls run: the longest call latency is printed and asserted under 100 ms. (3) A call whose consumer never reads fills its window and the lane budget, while group 2's 100 ms heartbeats keep the longest gap under 1,000 ms (`election_min`, `types.h:115`), printed. Job: `campaign verify --for A1` | `--fails-without` at mutant `rpc-shared-budget`: the lane charges the groups' `incoming_bytes` |
| A2 | `rpc_stream_test 4g`, a strict job at most 10 min: 4 GiB from a generator, hashed on arrival, with no 4 GiB buffer at either end. Group heartbeats every 100 ms on the pair's Control connection, and a 64-byte `echo` call every 100 ms on the same Bulk connection. It prints the largest gap of each, throughput, and budget high water, and asserts each gap under 1,000 ms and the high water at most one window | `--fails-without` at mutant `rpc-one-frame`: the body as one frame, refused by the frame bound or held behind it |
| A3 | `rpc_test lane-only`: hosts with no shared group. A call connects on Bulk only, and its bytes are exact. `LaneStats` counts it, while every group's stats and credit stay at 0. The connection closes after `lane_idle`, and Control opens once a group is shared | `--fails-without` at mutant `rpc-groups-only`: the peer set ignores lane demand → `not_connected` |
| A4 | consensus's `peers_test --fetch` scenarios (`peers_test.cc:1328-1329`) pass unchanged over method 1. A job builds Detent `main` against this commit (`-DFETCHCONTENT_SOURCE_DIR_CONSENSUS=$JOB_REF_CONSENSUS`) and runs `test_authority_history_transport` and `history_mutant.sh` | the history mutants stay killed |
| A5 | `rpc_stream_test soak`: 1,000 calls with bodies from 0 B to 64 MiB, each compared to its source's SHA-256 (Kronuz/sha256). It runs beside the consensus-fetch-macos load: placed 2.1 s `SIGSTOP`s and a host load of 30-40. The job runs on macOS, and CI runs it on Linux, labeled `timing` per D100 so it stays off CI's default run. It asserts 0 errors and 0 mismatches | `--fails-without` at mutant `late-deadline` (the call deadline on the bare timer) |
| A6 | `fuzz/rpc_fuzz.cc`, a libFuzzer target on `decode_rpc`: decode or throw, re-encode canonically, and drive a call table with decoded frames for the D4 rules. Its corpus in `fuzz/rpc_corpus/` replays in the suite as `consensus_rpc_fuzz_replay`. The `fuzz` check runs `-fork=2 -max_total_time=1800`, one CPU-hour, with zero findings | |
| model | `python3 docs/design/rpc-lane-model.py`, invariants I1-I5 over 94,139 states | five mutants, each exit 1: `forget-is-violation`, `grant-without-reserve`, `no-release-on-close`, `no-regrant`, `grant-on-receive` |

## Limits

- **Silent: a consumer that never reads** holds its window, at most 4 MiB, until its deadline. A
  host with many stalled calls answers new ones `overloaded`, and callers see that as load, not
  as a bug. `LaneStats`' budget high water is the only signal.
- A caller whose budget is empty waits before it sends a `Call`. That wait counts against the
  call's deadline, so a host whose lane budget is exhausted times its own calls out.
- One 4 GiB body runs at most `window / round trip`. A raised window spends lane budget that
  other calls then lack.
- Bulk is shared with snapshot transfers at a 1:1 alternation, so an install and a blob `get`
  each get half of the connection.
- The lane retries nothing. Idempotency is the method's: write retries go by version
  (async-drivers X11).
- Every rule above depends on T1-T7, in a quest that has not started.

## Questions for the owner and the lead

**Q1. Where A4's Detent test runs.** Detent reaches consensus through today's `Peers` until its
quest 6 (`detent-on-drivers`).

- (a) This quest also carries `Peers`' bulk lane on the lane's frames, deleting `DTFT0001`
  everywhere at once. Detent's test then passes at its current API through a pin override.
- (b) A4 waits for Detent's quest 6.

*Recommendation: (a).* It is a deletion rather than an improvement of replaced code: one fetch
mechanism, net fewer lines in `peers.h`.

**Q2. The file set.** The quest's set lacks the paths the build and the design need:

- `CMakeLists.txt` (to register the tests and the fuzzer)
- `fuzz/fuzz_decoders.cc` (to drop `DTFT0001`)
- `test/transport/peers_test.cc` (only under Q1 (a))
- `docs/design/rpc-lane*`
- `CHANGELOG.md`, `ARCHITECTURE.md`

*Recommendation:* add them. Detent's driver also touches `CMakeLists.txt`, so it lands first, as
ordered.

**Q3. The `fuzz` check's command.** It configures without `CONSENSUS_FUZZ` (`CMakeLists.txt:80`)
and with Apple clang, which lacks libFuzzer. `ctest -R rpc_fuzz` would then match only the
replay, and the check would pass without fuzzing. *Recommendation:*
`-DCMAKE_CXX_COMPILER=/usr/local/opt/llvm/bin/clang++ -DCONSENSUS_FUZZ=ON`, plus
`--no-tests=error`, with the hour-long entry registered only under `CONSENSUS_FUZZ`, so the
`build-test` check never runs it.

**Q4. A2's "the same connection".** Under D1 a heartbeat and a body never share a socket. A2
measures the heartbeat on the pair's Control connection and a lane `echo` on the Bulk connection
the body uses. *Recommendation:* accept both as A2's evidence.

## First buildable steps

1. Once the driver lands: `include/consensus/transport/rpc.h`, the codec (D2), with
   `fuzz/rpc_fuzz.cc` and its seed corpus. It is pure, so it runs on `main`'s build at once.
2. The call table and credit (D4, D5) over `test/transport/async_pipe.h`, mirroring the model's
   events, with the model's five mutants as unit tests.
3. On the host transport: placement on Bulk, T1-T7, then A1-A3.
4. Method 1 and Q1's answer (A4); then the 4 GiB stream and soak jobs (A2, A5);
   `docs/mechanisms/rpc-lane.md` (A7).
