# The RPC lane: calls and streamed bodies on the host transport

A request/response lane for application calls between any two nodes, on the node-pair
connections consensus already runs, with bodies streamed under the lane's own credit.

- Status: design, revision 2, 2026-11-16. It answers the design review at Xapiand:e9efa08f3
  (`.campaign/xapiand/reviews/consensus-rpc-lane-design.md`, CHANGES); the findings map is at the
  end. Contract `consensus:rpc-lane` (wire), reviewed again by `format-freeze`.
- Builders. This document is the one spec of the lane's bytes. `detent:consensus-transport`
  criterion A7 builds the lane: decisions marked **[transport]**, T1-T8, the codec and its fuzz
  target. Quest `xapiand:consensus-rpc-lane` builds what is marked **[rpc-lane]**: streamed
  request bodies, the server's early end, and Xapiand's methods. All of it is in version 1, so
  the rpc-lane quest changes no byte.
- Read at: consensus `main` `d1065e0`; Detent `main` `195a46d6` (`docs/design/async-drivers.md`,
  quest `consensus-transport`); strata `docs/design/track-c.md`; Xapiand
  `docs/design/cluster-release.md`.
- Nothing here is built or timed. The model (`rpc-lane-model.py`) has run; the review's map at the
  end names its jobs.

## Abstract

Xapiand's cluster needs node-to-node calls: shard search, `get` of a stored blob up to 4 GiB,
`forward_write` of a streamed upload, and keepalives to the root leader (cluster-release D6, D9,
D10). Detent's history fetch needs one too (Detent msg-44, option 1); today it is the `DTFT0001`
fetch lane. Cluster-release D9 rules that all of them ride the transport consensus already
authenticates, so a second TLS listener or HTTP between nodes is rejected.

The lane rides the node pair's **Bulk** connection. It has six frame kinds (`Call`, `Body`,
`Credit`, `Cancel`, `Error`, `Drain`), with "end" as a flag, behind one 24-byte header, and the
connection hello negotiates its version from a range. Every body byte travels in a `Body` chunk
of at most 64 KiB, under a per-call window. The receiver grants that window from lane pools that
no group's credit touches, so a stalled call cannot hold a heartbeat and a stalled group cannot
hold a call. Before an end closes a connection on purpose, it sends `Drain`. Calls the `Drain`
does not cover are known never to have started, and are safe to retry. Lane traffic can open a
connection to a node that shares no group. Fetch is method 1.

An exhaustive model of two calls on one connection, at a caller budget of two windows, enumerates
every interleaving. That covers cancels, deadlines, errors, refusals, `Drain`, graceful and
abrupt closes, out-of-order small frames and the server's early end. It finds no budget overrun,
double completion, deadlock, false protocol violation or call lost to a graceful close. Eight
mutants each fail it.

## Why not the obvious things

**Generalize `DTFT0001` in place.** Its rules do not survive cancellation or general use:

- A reply to an id it no longer awaits closes the bulk lane (`peers.h:380-381`), and a cancel
  makes that routine.
- A peer over `fetches` closes the lane (`peers.h:373-374`), so voters must agree on the bound
  (`peer-fetch.md:52-54`).
- Its credit comes from the `outgoing_bytes` budget that control frames share (`peers.h:177`,
  `:779`).
- It has no method, no deadline and no request body.

The model's `forget-is-violation` mutant is its first rule.

**Send a body as one frame.** The transport's length prefix is 32 bits (`session.h:177-181`). A
receiver allocates each frame whole against a shared budget and throws when it is short
(`session.h:62-64`, `:168`). A 4 GiB frame needs 4 GiB of receive memory and blocks its socket
for as long as it takes to cross. A2's control mutant is exactly this.

## Decisions

**D1. The lane rides Bulk [transport].** Track C D16 gives each node pair a Control connection
(Raft messages, control frames first, appends of at most 256 KiB) and a Bulk connection (snapshot
chunks) (`track-c.md:762-789`). Lane frames go on Bulk, as `DTFT0001` does. On Bulk, the lane's
small frames (`Call`, `Credit`, `Cancel`, `Error`, `Drain`) go first, in FIFO order among
themselves, which `Credit`'s increasing rule needs. Lane `Body` chunks and snapshot chunks
alternate while both wait, which generalizes `fetch_turn` (`peers.h:309`). Calls' chunks
round-robin, and each call's chunks stay in order. A heartbeat therefore never shares a socket
with a body, and a small frame waits for at most one 64 KiB chunk plus D16's 64 KiB of unsent
socket bytes.

Rejected:

- Control. Search responses would compete with appends.
- A third connection per pair. It costs a descriptor and about 41 KB per peer (async-drivers
  D10).
- Splitting a call across both connections. Its end could overtake its last chunk.

**D2. Frames [transport; request-direction fields used by rpc-lane].** One lane frame is one
transport frame. Integers are little-endian and fixed-width (journal's `put32` and `put64`,
`journal/codec.h:44-49`), as in `DTC2` and `DTFT0001`. A `Body` header is 40 bytes, 0.06% of a
64 KiB chunk. Varints were rejected: they would save part of those 40 bytes and add a decode path
to fuzz.

| Offset | Bytes | Field | Rule |
|---|---|---|---|
| 0 | 8 | magic | `DTRP` and the negotiated version in four digits: `DTRP0001` |
| 8 | 1 | kind | 1 `Call`, 2 `Body`, 3 `Credit`, 4 `Cancel`, 5 `Error`, 6 `Drain` |
| 9 | 1 | flags | bit 0 `end`, on `Call` and `Body` only |
| 10 | 2 | zero | |
| 12 | 4 | word | `Call`: method id; `Cancel`, `Error`: code; others zero |
| 16 | 8 | call | the call's id: odd from the end that dialed, even from the end that accepted. `Drain`: the highest `Call` id the sender accepted, 0 for none |

| Kind | After the header (frame length) | Meaning |
|---|---|---|
| `Call` | `timeout_ms` u64, `window` u64, `total` u64, then 0 to 65,536 request bytes at offset 0 (48 to 65,584) | `timeout_ms` is the budget left when the frame is queued; `window` is the caller's first response credit limit; `total` is the request's length, or 2^64-1 when unknown |
| `Body` | `offset` u64, `total` u64, then 0 to 65,536 bytes (40 to 65,576) | the next chunk of the sender's stream of the call: the request from the caller, the response from the server |
| `Credit` | `limit` u64 (exactly 32) | cumulative: the peer may send its stream of the call up to byte `limit` |
| `Cancel`, `Error` | 0 to 1,024 bytes of diagnostic text (24 to 1,048) | the call ends; `Cancel` from the caller, `Error` from the server |
| `Drain` | nothing (exactly 24) | D4 |

The largest frame, 65,584 bytes, fits Bulk's existing bound of `maximum_transfer_bytes`, 65,688
(`snapshot_transfer.h:37`). `Error` codes 1-6 are the lane's: `unknown_method`, `overloaded`,
`deadline_exceeded`, `cancelled`, `message_size` and `handler_failed`. Codes 7-255 are reserved.
Codes of 256 and above are the method's and reach the caller unchanged. `Cancel` codes are
`cancelled`, `deadline_exceeded`, `message_size` and `source_failed`. Outcomes that never travel
are `not_processed`, `connection_aborted`, `timed_out`, `not_connected` and
`protocol_not_supported`. End is a flag rather than a kind, so a one-frame call stays one frame.

**D3. Validity [transport].** A frame that breaks any of these rules closes the connection. Under
the trusted-cluster threat model each one is a peer bug. `decode_rpc` checks rules 1-5 alone; the
call table checks rules 6-10.

1. The magic is not `DTRP` with the negotiated version.
2. The kind is 0 or above 6. `flags` has a bit other than bit 0 set, or bit 0 set on a kind
   other than `Call` and `Body`. Bytes 10-11 are nonzero.
3. `word` is zero on `Call`, `Cancel` or `Error`, or nonzero elsewhere. An `Error` code is
   7-255. A `Cancel` code is outside its four.
4. The frame length is outside its kind's range in D2's table.
5. `call` is zero on a kind other than `Drain`. A `Call` has `timeout_ms` = 0 or `window` = 0.
   Its inline length exceeds its total. It has `end` while its known total differs from its
   inline length, or lacks `end` while they match.
6. The frame comes from the wrong side, judged by the id's parity: the caller sends `Call`,
   request `Body`, `Cancel` and response `Credit`, and the server sends response `Body`, request
   `Credit` and `Error`. A `Call` id is not greater than the highest one of its parity. Any other
   id is above every id of its parity allocated so far.
7. A `Body` for a live call carries any of these:
   1. An `offset` other than the next byte.
   2. Zero bytes without `end`.
   3. `n > total - offset`, computed in that form so it cannot overflow.
   4. A `total` that differs from its stream's first.
   5. `end` set without `offset + n == total`, or missing with it, when the total is known.
   6. `n > limit - offset`, past credit.
   7. Arrival after its stream's end.
8. A `Credit` for a live call has a `limit` no greater than the stream's previous one.
9. A frame for a *retired* id, one at or below the highest of its parity and no longer live, is
   discarded and counted (`late`), never fatal (D4).
10. A second `Drain` arrives from the same end, or a `Drain` id exceeds the highest `Call` id
    the receiver allocated.

**D4. A call's life [transport; early end rpc-lane].** The caller allocates an id when it queues
the `Call`, after any local wait for budget (D5), so ids reach the peer in increasing order. A
call ends exactly once at each end, by one of six outcomes:

1. The response's `end`.
2. An `Error` from the server.
3. A `Cancel` from the caller, explicit or at its deadline.
4. The server's deadline, which sends `Error deadline_exceeded`.
5. A `Drain` that does not cover the call, so the call ends `not_processed`.
6. A connection failure, so the call ends `connection_aborted`, with an unknown outcome.

**[rpc-lane]** The server may end its response before it reads the whole request, and it
discards the rest. The caller then stops sending. Once an end is done with a call, the id is
retired (D3 rule 9), because cancels and deadlines cross frames in flight. The server's deadline
is arrival plus `timeout_ms`, clamped to its `max_call_deadline` (1 h by default). Call deadlines
follow consensus-transport A6's one rule for late timers after a process stop (T7).

*Drain.* Before an end closes a connection on purpose, after `lane_idle` or at shutdown, it
sends `Drain` with the highest `Call` id it accepted from the peer, then:

1. It discards any later `Call` unanswered.
2. It starts no call of its own on the connection.
3. It finishes its calls in both directions.
4. It closes gracefully (T8).

The receiver fails its calls above the id with `not_processed`, which is safe to retry on a new
connection. It starts no more calls on this connection and finishes the rest. A shutdown waits
at most `drain_deadline` (5 s by default). Calls still live after that fail with
`connection_aborted`, as a connection failure does. HTTP/2's GOAWAY (RFC 9113 §6.8) has this
shape. The model's `drain-ignored` mutant loses a call to a graceful close.

**D5. Credit [transport; request direction rpc-lane].**

- *Pools.* Each host has two receive pools, `rpc_serving_bytes` for the request bodies it serves
  and `rpc_calling_bytes` for its own calls' responses (256 MiB each by default, D15's node send
  budget). Group frames never charge either (T2). With separate pools, a host's served requests
  cannot take the bytes its own calls need.
- *Reserve before grant.* A receiver reserves a pool's bytes before it grants them as credit.
  Every chunk that arrives within credit already has its memory, so the reader never waits on a
  consumer. That is the hazard `reader_test` shows: a follower's heartbeats held 3,000 ms behind a
  busy group (async-drivers D10).
- *First grants.* The caller grants the response's first window in `Call.window`. It waits
  locally for its pool before it queues the `Call`, and that wait counts against the deadline.
  The server grants the request's first window in a `Credit` when it accepts the call.
- *Regrant.* When a consumer has taken half a window, the receiver raises `limit` to
  `consumed + min(window, pool - other calls' reservations)`. The consumed bytes move into the new
  grant in one operation, and only the excess returns to the pool:
  `ByteBudget::renew(returned, wanted)`. Its result is the call's new reservation, the call's own
  `returned` bytes are taken first, and it is atomic, since a pool spans the host's loops and
  today's `ByteBudget` is one executor's (`session.h:10-32`). The window defaults to 4 MiB, D15's
  `window_bytes`, which is 4 GB/s at a 1 ms round trip (arithmetic). A grant every half window
  sends 2,048 `Credit` frames for a 4 GiB body.
- *Why no grant waits on another call.* A live call's reservation never reaches zero while its
  stream is unfinished. Its own consumption frees the bytes its renewal takes back, so the
  renewal grants at least one chunk. The model's `split-release` mutant (release, then a separate
  re-acquire) lets another call take the freed bytes and deadlocks. `grant-on-receive` overruns
  the pool.
- *Sending.* A writer queues a chunk only within the peer's credit, and only after charging
  `rpc_outgoing_bytes` (64 MiB per host by default). It waits, first in first out, when the pool
  is short, and the charge returns when the chunk reaches the socket. A host's queued lane bytes
  therefore stay under 64 MiB, whatever its peers grant.
- *Refusal.* `Call` frames are not credit-gated. A server at its serving bound refuses with
  `Error overloaded` at once and frees the frame. The bound is 256 calls per peer, above the
  concurrent-stream defaults of nginx (128) and Go's HTTP/2 server (250). So does a server whose
  pool lacks one chunk. Neither queues the call or closes the connection. A served call's slot
  frees when the call ends, not when its handler returns, so handlers that ignore their token
  cannot refuse a peer forever.

Rejected: a lane-wide connection window beside the per-call windows. The grantor already owns
the memory, so a second window bounds nothing more.

**D6. Streaming API [transport; `Stream::write` on a request rpc-lane].** This is the shape;
exact types follow `consensus-transport`'s `task` API.

```cpp
using MethodId = std::uint32_t;  // 0 invalid; 1-65535 consensus's; above, the host's
struct CallOptions { std::chrono::milliseconds deadline; std::size_t window = 4 << 20;
                     std::optional<std::uint64_t> request_total; };
class Stream {  // one direction of one call's body
  public:
	task::task<std::optional<Chunk>> next();   // nullopt at end; releasing a Chunk renews credit
	task::task<void> write(std::string_view);  // waits for credit and rpc_outgoing_bytes
	task::task<void> finish();
};
struct Call { NodeId peer; Stream request, response; };  // peer: the authenticated node
task::task<Call> Lane::call(NodeId, MethodId, CallOptions, std::string inline_request = {});
void Lane::serve(MethodId, std::function<task::task<void>(Call &, task::token)>);
task::task<std::string> collect(Stream &, std::size_t limit);  // a whole body, or message_size
```

A call's state lives on its connection's loop. A consumer on another loop pays D10's hop once
per 64 KiB chunk: 832 ns of CPU when posted alone (job 261111-012904-94206c).

**D7. Nodes that share no group [transport] (T3).** The lane holds a connection open while it has
a live call or a host `hold(node)`, such as cluster-release D6's keepalive to the root leader. It
keeps the connection `lane_idle` (60 s) after the last call, then drains it (D4). The peer set is
the local groups' members plus the lane's demand. Addresses come from the host's resolver, and
the peer is admitted by its node pin (async-drivers Q7 (a)). A lane-only pair opens Bulk alone.
Per-peer `LaneStats` counts calls by outcome, bytes in and out, `late` frames and pool high
water, and never touches a group's credit or `PeerStats`.

**D8. Fetch is method 1 [transport].** `Peers::fetch(peer, name, done)` keeps its outcome table
(`peer-fetch.md:16-25`), as `Lane::call(peer, 1, {fetch_deadline}, name)` served by the host's
`FetchReader`:

| Today | On the lane |
|---|---|
| The bytes | `Body` with `end`; `total` is the file's size |
| The peer holds none | `Error` 256 (`missing`) |
| The peer's reader threw, or the file is over its `fetch_bytes` | `Error handler_failed` or `message_size` → `io_error` |
| The file is over this voter's `fetch_bytes` | the first `Body`'s `total` → `Cancel message_size` → `message_size` |
| `fetch_deadline`; `fetches` already await the peer | `timed_out`; a local bound → `resource_unavailable_try_again` |
| The lane closed, or there is none | `connection_aborted`, `not_connected` |

`DTFT0001` and its two-deadline rule for a hung peer go: `Cancel` frees the server's slot, and
the per-connection keepalive (T4) finds a half-open peer. Today's `Peers` is not ported, because
worker-pump-removal deletes it.

**D9. Streamed request bodies and Xapiand's methods [rpc-lane].** This quest builds:

1. `Stream::write` on a request, with `Call.total` known or unknown.
2. The request-direction `Body` and `Credit`.
3. The server's early end.
4. The methods `forward_write` (a streamed upload) and `get` (a streamed blob), plus one
   remote-search-shaped method with msgpack bodies.

All of it runs on the transport's lane and codec.

## Requirements on `consensus-transport` (its A7)

| # | Need |
|---|---|
| T1 | The hello carries `rpc_min` and `rpc_max`; DTHL0001's 120 bytes (`hello.h:17`) become DTHL0002. The version is `min(max_a, max_b)` when that is at least `max(min_a, min_b)`; otherwise the lane is off on that connection (`protocol_not_supported`), with group frames unaffected |
| T2 | Bulk routes a frame by its first 8 bytes (`peer-fetch.md:33-34`). Its sender keeps a FIFO small-frames queue first and alternates chunk classes (D1). The lane's three pools are their own thread-safe `ByteBudget`s with `renew` (D5), and lane frames sit outside D16's Bulk connection window as well as its group budgets |
| T3 | `Transport::hold(node)` and `release(node)` keep a Bulk connection to a node outside every group |
| T4 | A keepalive on every connection, lane-only Bulk included: the transport's own A5 ("once per connection"), listed as a dependency |
| T5 | The reader hands the lane each frame synchronously on the connection's loop, from a per-connection buffer, never waiting on a shared budget (`session.h:168` does today) |
| T6 | No call starts on a duplicate connection that the dial tie-break will close |
| T7 | Its A6 late-deadline rule covers call deadlines, in its one place |
| T8 | A graceful close writes every queued frame, then TLS `close_notify`, and the peer reads to that end; `lane_idle` closes and shutdowns go through `Drain` (D4) |

## Acceptance criteria as checks (this quest's, over the transport's lane)

| | Check | Control |
|---|---|---|
| A1 | `test/transport/rpc_test.cc`, TLS loopback, two hosts of 4 loops each, under TSan in CI. (1) Deadlines and cancels: a caller's deadline cancels the server's token; a mid-stream cancel of a streamed request ends both sides once; crossing cancel and end; pools back to 0. (2) Group 1's admission full for 3 s, and a snapshot install on the same Bulk connection with its group's credit full, while calls run: the largest call latency is printed and asserted under 100 ms. (3) A call whose server never reads fills its request window and `rpc_serving_bytes`, while group 2's 100 ms heartbeats keep the largest gap under 1,000 ms (`types.h:115`), printed | `--fails-without` at `rpc-shared-budget`: lane bytes charge the groups' `incoming_bytes` |
| A2 | `rpc_stream_test 4g`, strict, at most 10 min: a 4 GiB generated request streamed with `forward_write`'s shape, hashed on arrival. Heartbeats every 100 ms on Control, plus a 64-byte call every 100 ms on the same Bulk connection. It prints each largest gap, throughput and the high water of both pools and `rpc_outgoing_bytes`, and asserts gaps under 1,000 ms and each high water within its bound | `--fails-without` at `rpc-one-frame`: the body as one frame |
| A3 | `rpc_test lane-only`: two hosts with no shared group. A streamed call connects on Bulk alone, its bytes are exact, `LaneStats` counts it and groups' stats stay 0. A second call is in flight when `lane_idle` expires: it ends `not_processed` or completes, never `connection_aborted` | `--fails-without` at `rpc-groups-only` (no lane demand in the peer set) and `rpc-no-drain` (close without `Drain`) |
| A4 | `rpc_methods_test`: `forward_write`, `get` and the search-shaped method, each with streamed bodies and an early end, over the transport's lane | |
| A5 | `rpc_stream_test soak`: 1,000 calls, bodies 0 B to 64 MiB in both directions, each checked against its source's SHA-256 (Kronuz/sha256), beside placed 2.1 s `SIGSTOP`s and a host load of 30 to 40 (consensus-fetch-macos's load). It runs on macOS as a job and on Linux in CI with the `timing` label (D100); 0 errors and 0 mismatches | `--fails-without` at `late-deadline` |
| A6 | `fuzz/rpc_stream_fuzz.cc`: the transport's `decode_rpc` target, extended to drive a call table with the request-stream, early-end and `Drain` rules of D3 rules 6-10 and D4. Corpus in `fuzz/rpc_stream_corpus/`, replayed in the suite; `-fork=2 -max_total_time=1800` is one CPU-hour, with zero findings | |
| model | `rpc-lane-model.py` at budgets 4/1: invariants I1-I6 under the rules `drain,reorder`, `early-end` and `early-end,drain`, one job each | eight mutants, each exiting 1: `forget-is-violation`, `grant-without-reserve`, `no-release-on-close`, `no-regrant`, `grant-on-receive`, `split-release`, `drain-ignored`, `id-at-call` |

## Limits

- **Silent: a consumer that never reads** holds its window, at most 4 MiB, until its deadline. A
  host with many stalled calls answers new ones `overloaded`, and only `LaneStats`' high water
  shows why.
- **Fairness between peers is open.** One peer's slow handlers can take all of
  `rpc_serving_bytes`, and every other peer is then refused `overloaded`.
- **Nested calls.** A handler that waits for `rpc_calling_bytes` while it holds an unconsumed
  request ties two pools together. Cycles across hosts end only at deadlines, so a handler
  consumes or finishes its request first.
- One body runs at most `window / round trip`. Bulk splits 1:1 between snapshot and lane chunks.
- A2 runs on loopback, which does not show a saturated link's queueing in front of Control's
  heartbeats.
- The lane retries nothing except, safely, `not_processed`. Idempotency belongs to the method:
  writes retry by version (async-drivers X11).
- The model covers one connection, one direction of calls and a pool per side. It does not cover
  pools shared across connections and roles, unknown totals, or fairness, and the three rules run
  as separate jobs, because all three together outgrow a Python run.

## Questions for the lead

**Q1. File set.** The quest lacks `CMakeLists.txt` (to register tests and the fuzzer),
`CHANGELOG.md` and `docs/design/rpc-lane*`. *Recommendation:* add them; they land after the
driver's changes, as ordered.

**Q2. The `fuzz` check's command.** It configures without `CONSENSUS_FUZZ` (`CMakeLists.txt:80`)
and with Apple clang, which lacks libFuzzer, so `ctest -R rpc_fuzz` passes unfuzzed.
*Recommendation:* `-DCMAKE_CXX_COMPILER=/usr/local/opt/llvm/bin/clang++ -DCONSENSUS_FUZZ=ON`,
`--no-tests=error`, and the hour-long entry registered only under `CONSENSUS_FUZZ`. The pattern
becomes `-R rpc_stream_fuzz`.

**Q3. A4's "real call".** A remote search over prospect lives in Xapiand (`shard-rpc`'s files),
not in consensus. *Recommendation:* A4 here is the three methods' shapes, and `shard-rpc` runs the
real search over this lane.

## First buildable steps

For the transport's A7:

1. `include/consensus/transport/rpc.h`: the codec (D2, D3 rules 1-5) and its fuzz target.
2. The call table, credit and `Drain` (D3 rules 6-10, D4, D5), over `test/transport/async_pipe.h`,
   with the model's mutants as unit tests.
3. On Bulk: T1-T8, lane-only connections (D7).
4. Fetch as method 1, and the removal of `DTFT0001` (D8).

For this quest, after A7:

5. Request streaming and the early end (D9 items 1-3), with A1 and A3.
6. The methods (A4), then the 4 GiB and soak jobs (A2, A5), the fuzz extension (A6), and
   `docs/mechanisms/rpc-lane.md` (A7).

## Review findings, revision 2

| ID | Fix |
|---|---|
| F1 | Builders marked; request-direction bytes in version 1; the `Peers` port dropped; steps 1-4 are the transport's |
| F2 | D5 *Regrant*: `ByteBudget::renew`, atomic; `split-release` mutant |
| F3 | D5 *Sending*: `rpc_outgoing_bytes` charged before a chunk is queued; A2 asserts its high water |
| F4 | `Drain`, kind 6 (D2, D4, T8); `drain-ignored` mutant; A3's close with a call in flight |
| F5 | D3, the full validity list |
| F6 | A1 (2): a snapshot install on the same Bulk connection with its credit full |
| F7 | D4: an id is allocated when its `Call` is queued; `id-at-call` mutant |
| F8 | The model at 4/1 with `reorder`, `early-end` and `drain`; Limits name the nested-call and fairness gaps; two pools |
| F9 | T6 reworded; T4 marked as the transport's own |
| F10 | D5's regrant formula is written as the model computes it |
| F11 | D1 small frames FIFO; T2 outside Bulk's connection window; `timeout_ms` is the budget left at queueing |
| F12 | The lead's; A2's loopback limit is in Limits |
