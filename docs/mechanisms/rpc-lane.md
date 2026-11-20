# The RPC lane: codec, call table and credit

`include/consensus/transport/rpc.h` holds the request/response lane of the RPC lane design
(consensus:0fd8a7f `docs/design/rpc-lane.md`, revision 2): its frames, one connection's call
table, and the host-wide byte pools that credit draws from. It does no I/O. The host transport
runs one call table per Bulk connection (On Bulk, below); the unit tests run them over pipes.

## Frames

One lane frame is one transport frame: a 24-byte header (`DTRP0001`, kind, flags, two zero
bytes, a 32-bit word, a 64-bit call id), then the kind's fields, then bytes. `decode` rejects a
frame that breaks D3 rules 1-5, and the parts of rule 7 one frame shows, and names the rule in
its `journal::Corruption`:

| Kind | Fields | Length |
|---|---|---|
| 1 `Call` | method, `timeout_ms`, `window`, `total`; inline request | 48-65,584 |
| 2 `Body` | `offset`, `total`; a chunk | 40-65,576 |
| 3 `Credit` | `limit` | 32 |
| 4 `Cancel`, 5 `Error` | code; text | 24-1,048 |
| 6 `Drain` | the highest `Call` id accepted, or 0 | 24 |

`Error` carries 1-6 (`unknown_method`, `overloaded`, `deadline_exceeded`, `cancelled`,
`message_size`, `handler_failed`) or a method's code from 256. `Cancel` carries
`deadline_exceeded`, `cancelled`, `message_size` or `source_failed`, which is 7. A `Body` is
checked as `offset <= total && n <= total - offset`, so `offset + n` never wraps.

## The call table

A `Lane` lives on its connection's loop. The dialer's calls take odd ids and the acceptor's even
ones, so a frame's id tells which end is the caller, and `receive` throws `ProtocolError` for a
frame from the wrong side (rule 6), a `Call` id that does not increase, an id never allocated, a
`Body` off its next byte, past its credit, with a changed total or after its end (rule 7), a
`Credit` that does not increase (rule 8), a second `Drain` or a `Drain` id that is not the
receiver's parity or 0 or was never allocated (rule 10). A frame for a retired id is counted in
`late()` and dropped (rule 9). On a throw the connection closes and calls `close()`.

A call ends once at each end, and its `done` runs once: `ok` when the response is consumed to its
end (the caller) or queued to its end (the server); `error` or `cancel` with the frame's code;
`not_processed` when a `Drain` did not cover it or it never queued; `connection_aborted` on
`close()`; `timed_out` when the caller's deadline passed before its `Call` queued. The caller's
deadline sends `Cancel deadline_exceeded`; the server's, arrival plus `timeout_ms` clamped to
`max_call_deadline`, sends `Error deadline_exceeded`. A served call's slot frees when the call
ends. The id is allocated when the `Call` is queued, after every wait, so ids reach the peer in
order.

## Credit and pools

`ByteBudget` is a mutex-guarded pool shared by every loop of a host: `rpc_serving_bytes` for
served request bodies, `rpc_calling_bytes` for this host's responses, `rpc_outgoing_bytes` for
queued body bytes. Waits are first in first out, and a wake runs on the releasing thread, which
posts it to the waiting lane's loop.

- A receiver reserves before it grants: the caller's first window in `Call.window`, the server's
  in a `Credit` when it accepts. A server short of one chunk, or at its serving bound, answers
  `Error overloaded`.
- When the consumer has taken half a window, `renew(returned, wanted)` returns the stream's whole
  holding and takes up to a window back in one step, its own bytes first. The holding never
  drops below what the peer may still send, so no other call can strand the stream; it grows
  beyond its own bytes only while nobody waits.
- A chunk, and a `Call`'s inline bytes, charge `rpc_outgoing_bytes` before they queue; the charge
  returns when the frame's write completes. Headers and the small frames are not charged.
- `next()` returns small frames first, in order, then chunks round-robin by call.

## Drain

`drain()` queues `Drain` with the highest `Call` id accepted, fails this end's unqueued calls
`not_processed`, and discards later `Call`s unanswered. The receiver fails its calls above the id
`not_processed`, which is safe to retry, and starts no more. `drained()` is true once no call is
live at the drained end: the connection writes what `next()` still holds, then closes.

## On Bulk

`HostTransport` (`host_transport.h`) carries the lane on each peer node's Bulk connection, beside
groups' snapshot frames:

- **One call table per Bulk link**, on the connection's loop, made when the link is established:
  the dialer's calls take odd ids, and the version is the one the hellos agreed (`Link::rpc`). A
  link whose hellos share no version ends its waiting calls `protocol_not_supported`; group
  frames are unaffected.
- **Calls.** `call(node, method, options, request, events)` runs on `loop_of_node(node)`. With
  no Bulk link, the call waits and the transport dials Bulk, to any node with an address, so two
  nodes that share no group connect for it; a failed dial is tried again every `redial` until the
  call's deadline (`timed_out`). `sendable`, `write`, `consume`, `total` and `stop` act on a call
  by its `RpcCall`, on the same loop; a call still waiting has nothing sendable. `handle(method,
  serve)` registers a method before `serve()`; the handler gets the `RpcCall` it answers on and
  acts from the call's events.
- **Receive.** A frame starting `DTRP` goes to the link's call table at once, from the
  connection's buffer (T5). One that breaks a rule, or arrives on Control or with the lane off,
  closes the connection (`malformed`), and its calls end `connection_aborted`.
- **Send.** A feed hands over, in order: credit owed to groups, the lane's small frames (FIFO),
  groups' control frames, then lane chunks and groups' data frames in turn while both wait. Lane
  frames sit outside every group's credit: a group whose window is full holds none of them, and a
  call whose credit is full holds no group frame. A chunk's `rpc_outgoing_bytes` charge returns
  when the connection takes it, and the connection takes a feed only once its queue is out.
- **Deadlines.** One timer per peer, at its earliest deadline: a waiting call's, the call
  table's (`Lane::expire`), or the redial.
- **Accounting.** `LaneStats` per loop counts calls, served calls, frames and bytes each way and
  each outcome. Lane frames never move `HostTransportStats`, so a lane-only pair leaves every
  group counter at 0.

Not built here: `lane_idle` and `Drain` on an idle or shutting-down link (the host transport
keeps connections up until `stop()`, which aborts them), `hold(node)`, and the late-deadline rule
for call deadlines (T7).

## Evidence

`test/transport/rpc_test.cc` has a case per rule with values, a call's life, the pools under
threads, and two lanes over pipes on two loops (run under TSan). Each of the model's eight
mutants (`test/transport/rpc_mutants.py`) fails the case of its name. `fuzz/rpc_fuzz.cc` drives
the decoder and a call table; its corpus `fuzz/corpus/rpc` replays as `consensus_fuzz_rpc_replay`.

`test/transport/rpc_host_test.cc`, two hosts over loopback TLS, one ctest entry per case:

| Case | What it shows | Control |
|---|---|---|
| `calls` | a 1 MiB streamed echo exact; a 300 ms deadline ends both ends `deadline_exceeded`; a cancel mid-stream ends each end once; every pool back to 0 | |
| `group-full` | a group's receiving loop stalls 3 s with its 4 MiB credit full on Bulk; calls on the same connection keep their largest latency under 250 ms | `rpc-shared-credit` (lane frames wait for the groups' credit) |
| `call-full` | a call whose server never reads holds its 4 MiB credit and pool; heartbeats on Control keep every gap under 1,000 ms | |
| `lane-only` | no shared group: the call dials Bulk alone, its bytes exact, one link per end, every group counter 0 | |
| `stream` | a 32 MiB request under the lane's credit, heartbeats' gaps under 1,000 ms, the serving pool's high water within one window | `rpc-one-frame` (the body as one frame) |
