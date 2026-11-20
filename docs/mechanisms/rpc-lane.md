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
`late()` and dropped (rule 9). On a throw the connection closes and calls `close()`. A throw from
this end's own code (the accept callback, a call's `body`, `credit` or `done`) is not the peer's:
it is counted in `failed()` and fails that call alone, `Error handler_failed` from a server and
`Cancel cancelled` from a caller, with the exception's text. A callback may call into the table:
a `close()` from one (a `ready` whose write failed at once, say) runs when the table's outermost
call returns, never while it holds a call (N3). No callback may destroy the table. `send` refuses
bytes past a call's known total at once, before anything changes (N2).

A call ends once at each end, and its `done` runs once: `ok` when the response is consumed to its
end (the caller) or queued to its end (the server); `error` or `cancel` with the frame's code;
`not_processed` when a `Drain` did not cover it or it never queued; `connection_aborted` on
`close()`; `timed_out` when the caller's deadline passed before its `Call` queued. The caller's
deadline sends `Cancel deadline_exceeded`; the server's, arrival plus `timeout_ms` clamped to
`max_call_deadline`, sends `Error deadline_exceeded`. With `LaneOptions::late` set, `expire()`
first gives a call whose deadline passed more than a wheel tick ago that time again, once, capped
at its timeout (T7: `late_extension` in `deadline.h`, the transport's one late-fire rule). A served
call's slot frees when the call ends. The id is allocated when the `Call` is queued, after every wait, so ids reach the peer in
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
- Every queued frame is charged to `rpc_outgoing_bytes`, header included, until `next()` hands
  it over and its write completes. A `Call` or `Body` waits for the charge, first in first out;
  the small frames (`Credit`, `Cancel`, `Error`, `Drain`) answer the peer and take it at once,
  past the limit if they must (`ByteBudget::charge`), so nobody else takes until they go.
- **A per-connection queue** (A3; Xapiand:a3b4b4903 R2): a table queues at most
  `LaneOptions::queue_bytes` unsent (4 frames, 262,336 bytes), small frames included. A `Call` or
  `Body` that does not fit waits for room, in order, before it asks `rpc_outgoing_bytes`; one
  frame always fits an empty queue. A peer that stops reading therefore holds at most its
  connection's queue, plus the frame being handed over, of the host's pool, and calls to every
  other peer go on.
- **A peer that never reads** (N1): a served response waits for room like any stream, so its
  call keeps its slot and `serving` bounds them; a `Call` refused past that is answered `Error`,
  and more than `serving` refusals unsent close the connection (`ProtocolError`).
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
  connection's buffer, on the connection's loop (T5): it never waits on another loop or a shared
  budget. One that breaks a rule, or arrives on Control or with the lane off,
  closes the connection (`malformed`), and its calls end `connection_aborted`.
- **Send.** A feed hands over, in order: the lane's small frames (FIFO), groups' control frames,
  credit owed to groups, then lane chunks and groups' data frames in turn while both wait. Lane
  frames sit outside every group's credit: a group whose window is full holds none of them, and a
  call whose credit is full holds no group frame. A chunk's `rpc_outgoing_bytes` charge returns
  when the connection takes it, and the connection takes a feed only once its queue is out.
- **Deadlines.** One timer per peer, at its earliest deadline: a waiting call's, the call
  table's (`Lane::expire`), or the redial. Both kinds of call deadline take the late-fire rule
  (T7), so a process stop does not end calls that the stop alone made late.
- **Frame bound.** The constructor refuses a `maximum_frame` below the lane's largest frame
  (`rpc::frame_bytes`, a `Call` with a full 64 KiB chunk: 65,584 bytes), beside the `rpc_bytes`
  check.
- **Accounting.** `LaneStats` per loop counts calls, served calls, frames and bytes each way and
  each outcome. Lane frames never move `HostTransportStats`, so a lane-only pair leaves every
  group counter at 0.

- **Fetch is method 1 (D8).** `fetch(node, name, done)` on `loop_of_node(node)` is a call of
  method 1 whose request is the name, with `fetch_deadline` (30 s); `done` runs on that loop,
  never within the call. With `RpcOptions::reader` set, the host serves method 1: the reader's
  bytes under the call's credit with `total` the file's size, `Error` 256 (`fetch_missing`) for a
  name it holds none of, `handler_failed` when the reader throws and `message_size` past its
  `fetch_bytes`. The fetcher maps them to bytes, nullopt and `io_error`; it cancels a reply past
  its own `fetch_bytes` (`message_size`) once the bytes it holds would exceed it; past `fetches`
  (4) awaiting the node a fetch ends `resource_unavailable_try_again`, as does one a drain refused
  (`not_processed`); `timed_out`, `connection_aborted` and `not_connected` pass through. Its reply
  shares the lane's pools and Bulk's alternation with groups' data frames, so heartbeats on Control
  keep their gaps (`fetch`). `Peers` and `DTFT0001` stay until worker-pump-removal deletes them.
- **Holds and lane_idle (T3, D7).** `hold(node)` dials Bulk to `node` and keeps it, sharing a
  group or not, until as many `release(node)`; a failed dial is tried again every `redial`. A
  Bulk connection that has carried no group frame either way, with no hold, no call and nothing
  waiting, drains after `RpcOptions::idle` (60 s). One that carried a group frame stays until
  `stop()`, as Control does.
- **Drain and the graceful close (D4, T8).** Draining sends the lane's `Drain`; once the table is
  drained (no call live at this end), the feed closes the connection gracefully: the frames queued,
  then `close_notify`. A call the peer started past the Drain ends `not_processed` there, and a
  call this host makes on a lane either end drained waits for the next connection, so a call
  racing a close ends `ok` or `not_processed`, never `connection_aborted`. A drain whose calls are
  still live at `drain_deadline` (5 s) aborts the connection. `drain()` drains every lane for a
  shutdown before `stop()`: new calls end `not_processed`, and `lanes_drained(loop)` says when the
  loop's lanes are closed. `LaneStats::drains` and `retired` count them.

## Evidence

`test/transport/rpc_test.cc` has a case per rule with values, a call's life, the pools under
threads, and two lanes over pipes on two loops (run under TSan). `reenter` closes the table from
`ready` inside `called()`, `stop()` and a served end (ASan; control `close-reenters`), `overrun`
sends past a known total (`send-past-total`), `unread-queue` feeds a table that is never read
until it throws, every unsent byte charged (`refusals-unbounded`), and `queue-cap` streams three
1 MiB requests into a two-frame queue (`rpc-no-queue-cap`). Each of the model's eight
mutants (`test/transport/rpc_mutants.py`) fails the case of its name, and so does `late-deadline`
(T7: a 2.1 s stop past a 10 s deadline gives 2.1 s once, a 30 s one gives 10 s). `fuzz/rpc_fuzz.cc` drives
the decoder and a call table whose pools are below two windows and whose queue is below two frames,
so calls wait for `calling`, `outgoing` and room (N4); its corpus `fuzz/corpus/rpc` (with `seed-past-credit`, a served request's Body past its 4 KiB window, rule 7.6) replays as
`consensus_fuzz_rpc_replay`, and `fuzz/rpc_fuzz.sh` fuzzes it at `-max_len=65600`.

`test/transport/rpc_host_test.cc`, two hosts over loopback TLS, one ctest entry per case:

| Case | What it shows | Control |
|---|---|---|
| `calls` | a 1 MiB streamed echo exact; a 300 ms deadline ends both ends `deadline_exceeded`; a cancel mid-stream ends each end once; every pool back to 0 | |
| `group-full` | a group's receiving loop stalls 4 s with its 4 MiB credit full on Bulk; calls on the same connection keep their largest latency under 250 ms | `rpc-shared-credit` (lane frames wait for the groups' credit) |
| `call-full` | a call whose server never reads holds its 4 MiB credit and pool; heartbeats on Control keep every gap under 1,000 ms | |
| `lane-only` | no shared group: the call dials Bulk alone, its bytes exact, one link per end, every group counter 0 | |
| `stream` | a 32 MiB request under the lane's credit, heartbeats' gaps under 1,000 ms, the serving pool's and the sender's queued bytes' (`rpc_outgoing_bytes`) high water within one window | `rpc-past-credit` (chunks that ignore the peer's credit) |
| `stalled-peer` | four calls hold 16 MiB of host 2's credit when host 2 stops reading for 4 s; host 1's 2 MiB `rpc_outgoing_bytes` holds at most that connection's queue, so a 1 MiB echo to host 3 completes before host 2 resumes | `rpc-no-queue-cap` |
| `hold` | `hold(2)` on a lane-only pair with a 300 ms lane_idle: Bulk comes up and stays 1 s; `release(2)` drains it about 300 ms later (1 Drain, closed gracefully) | `hold-ignored` |
| `idle-race` | host 1's 300 ms lane_idle drains while host 2's loop is held 600 ms, in which it starts 5 calls on the old connection: all 5 end `not_processed`, none `connection_aborted`, and the next call dials again and completes | `rpc-no-drain` |
| `shutdown` | `drain()` with a 4 MiB echo streaming: it completes whole, a later call ends `not_processed`, the lanes close, no call ends `connection_aborted` | |
| `fetch` | host 2 uploads 4 groups' 1 MiB data frames to host 1 on Bulk while host 1 fetches 16 MiB on the same connection: it arrives exact in about 1.1 s beside 131 MiB of uploads, heartbeats on Control keep a 210 ms longest gap; a missing name gives nullopt, a throwing reader `io_error`, 48 MiB against a 32 MiB `fetch_bytes` `message_size`, and past 4 awaiting `resource_unavailable_try_again` | |
| `reader` | host 2's loop that does not read node 1's connection is held; 10 echoes on that connection complete, the slowest in under 200 ms (T5) | `lane-via-other-loop` |
| `local-throw` | a handler acting outside its events and a body consumer that throw each end their call `handler_failed`; the link count, `malformed` 0, a group's frames on Bulk and the next 1 MiB echo show the connection went on | `local-throw-closes` (a local throw closes the connection) |

`hello_test version` refuses a 32 KiB `maximum_frame` below the lane's 65,584-byte frame
(control `lane-frame-unchecked`).
