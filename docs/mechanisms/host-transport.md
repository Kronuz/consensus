# The host transport: every group over two connections per peer node

A host runs one transport (`include/consensus/transport/host_transport.h`) for all its groups,
as Detent `docs/design/async-drivers.md` D10 and strata Track C D16 specify: a Control and a
Bulk connection per peer node ([host-connection.md](host-connection.md)), shared by every group
the two nodes have in common. This page describes where each connection runs, how a frame
reaches its group's loop, which frames a group admits, and how a sender orders and bounds what
it queues. `Peers` stays beside it until worker-pump-removal deletes that.

## Placement

A host runs L loops. A group runs on loop `(slot x L) >> 12`, `slot` being its id hashed into
4,096 slots (journal's `host_detail::slot`, the one the journal host places its turn logs by),
and a peer node's connections run on the loop its node id places on by the same rule. Each
loop has its own `Links`, which holds the connections of the peers placed on it. Nothing is
looked up: both placements are arithmetic on the id.

A dial starts on the peer's loop, so a dialed connection is born there. An accept arrives on
loop 0's listener before the peer is known; once its TLS handshake authenticates the peer, the
connection moves to the peer's loop (`Connection::rehome`: the descriptor and its SSL state
change loops while nothing waits on them) and the hellos and the tie-break run there
(`Links::adopt`). The peer's `Links` counts the move from the moment it is posted, so it is
not `drained()` while one is on its way.

## Frames

Every frame on a host connection starts with its kind:

| Kind | Bytes after the kind | Meaning |
|---|---|---|
| 1 control | group, source and destination incarnations (8 each), message | a message that goes first |
| 2 data | the same | a message within the group's credit |
| 3 credit | group (8), bytes (8) | the receiver delivered that many bytes of the group's data frames |
| 4 open | group (8), cluster, group identity, epoch (16 each), profile (32) | the group's open on this connection |
| 5 closed | group (8) | the receiver refused the group on this connection: open it again |

Integers are little-endian. Any other kind, or a credit, open or closed frame of another
length, ends the connection. `send_message` puts a Driver's `Send` on Control: an append that
carries entries is data, every other message (votes, pre-votes, heartbeats, responses,
`TimeoutNow`) is control. Bulk carries the same kinds for snapshot chunks (rpc-lane-on-bulk).

## Receive: a batch per loop, posted once per read

The reader hands each frame to the transport synchronously. A credit frame applies on the
spot. A message frame joins the batch of its group's loop for this read; once the read's frames
are out (`Connection::on_read`), each non-empty batch goes to its loop in one post, and the
reader's own loop's batch is delivered in place. The loop hands each frame to the host's
`Deliver`, which returns false when the group cannot take it now: the frame is dropped and
counted (Raft resends), exactly what D10's `reader_test` prototype assumes. The reader never
waits on a group, so a group whose admission is full never holds another group's heartbeat.

After delivering a batch, the group's loop sends the batch's data credit back to the
connection's loop in one post. Credit is per connection: frames that came on a connection
that has since ended return nothing, and a new connection starts every group at zero.

## Identity: the per-frame rule and the group-open exchange

A connection authenticates a peer node, not a member of one group (async-drivers Q7 (a)), so
what Peers' per-group hello checked moves to the group-open exchange, and each frame is
checked against what the receiving host stores. The host tells the transport, on the group's
loop, which groups it stores: `store(group, incarnation, open)` and `forget(group)`. A group
not stored sends nothing.

**The group-open exchange.** Before a group's first frame on a connection, the sender sends
the group's open: its cluster, group identity, epoch and profile fingerprint, which every
member computes alike and none of which is membership. The receiver checks it on the group's
loop, in order with the group's frames: the group opens on that connection (its node, lane and
generation) when it is stored and every field matches; otherwise it closes there, and the
receiver answers `closed`. Each direction opens on its own. A sender opens the group again
after a `closed`, on a new connection, and after the host stores it again (a new epoch, say),
so a group stored late, or stored again on both ends, opens at the next frame.

**The per-frame rule (R7).** A message frame reaches the host's `Deliver` only when:

1. the host stores its group (`unstored`, answered `closed`);
2. the group's open matched on this connection (`unopened`, answered `closed`);
3. its destination is the incarnation the host holds (`misrouted`);
4. where the receiver's membership names its source incarnation, that incarnation's node is
   the connection's node (`spoofed`).

The membership comes from the host's `member_of(group, incarnation)` (required; Detent
`docs/design/async-drivers.md` D10 Identity): `Core` takes `Receive::authenticated_peer` as
authenticated. An incarnation the membership does not name (a joiner's leader, a leader a
follower lags) is admitted, and `Core` decides by term, incarnation and log: membership is never
a filter. A refused frame is counted by its reason; its data credit returns as for any other.
Until groups have a closed state (Q4), a stored group is open and the incarnation it holds is
the one stored.

What a node can still do is speak for its own incarnations in groups it shares, and for
incarnations the receiver's membership does not name yet: the async-drivers Q7 trust model, a
trusted cluster with honest faults. A credit frame changes only the receiver's own sending, so
it is not checked.

## Send: per-group queues, control first, credit

A Driver's `Send`s go to the transport on the group's loop. They collect per connection loop
and go there in one post once the running work yields (one post per connection loop per
Driver resume). On the connection's loop each group has two queues per connection, control
and data, which exist only while they hold a frame or uncredited bytes: an idle group holds
nothing in the transport.

The connection asks for frames when its own queue is out (`Connection::on_drained`), and the
transport also feeds it when a frame arrives for an idle connection. Each feed hands over up
to 64 KiB, picking in this order:

1. Credit owed to the peer (window updates), coalesced per group, then the groups it refused
   (`closed`).
2. A control frame, round robin across groups, after the group's open when the connection does
   not carry its latest.
3. A data frame, round robin across groups that have credit: a group may have at most
   `window` (4 MiB) of data sent and not yet credited, and one frame always fits when nothing
   is outstanding.

A data frame is handed over whole, so a control frame waits behind at most one data frame, the
last feed and what the kernel's socket buffers hold. A 2 MiB append at 40 MB/s is 50 ms,
against `election_min` of 1 s.

The socket keeps its default buffers; `TCP_NOTSENT_LOWAT` is not set. On Linux it also caps
what a write may queue, so a writer to a peer that reads steadily but slowly sees progress
only as the peer's window opens, and its stuck deadline (a write wait of `dead_after` in which
the send queue did not drain) ends the connection (`connection_test`'s `slow_reader` and `busy_reader`). On macOS it bought
nothing measurable in `upload` (a 233 ms gap without it, 195 ms with it).

Queues are bounded: a group queues at most 64 control frames and 8 MiB of data frames per
connection; more are dropped and counted. A lane with frames and no connection dials, at
most once per `redial` (200 ms) after a failed dial.

## What this leaves to later quests

- A frame for a group stored here but closed opens it (X8) once Q4 gives groups a closed
  state.
- A connection closes when no group shares its peer, and its idle cost is measured, in
  transport-idle-cost. Here connections stay up until `stop()`.
- Track C D16's connection-wide window beside the per-group one is not built: the per-group
  windows and the feed bound already bound what one connection queues.

## Checks

`consensus_transport_admission_test` checks the rule and the open's fields as values: each
refusal, an admitted learner and unnamed source, and each field differing in one byte, after an
encode and decode. `consensus_transport_groups_test <fixture> <case>`, each a ctest entry:

| Case | What it shows | Mutants it kills |
|---|---|---|
| `threads` | two hosts of 4 loops, a thread each, exchange 64 groups' frames both ways (run under TSan): each arrives on its group's loop, in order per kind; one Control connection, on the peer's loop at both ends; posts at most one per read per loop | `reader-loop-delivers`, `no-hand-off` |
| `credit` | a group whose loop stalls 1.5 s has at most its window at the receiver; another group's heartbeats keep a 59 ms gap | `no-credit` |
| `reader` | `reader_test`: a group refused for 3 s beside a heartbeat group on the reader's own loop; the heartbeat's longest gap stays near 100 ms | |
| `upload` | four groups upload 2 MiB appends of 1 MiB commands to a reader throttled to 40 MB/s; a heartbeat and a vote on the same connection keep under the 1,000 ms election timeout | `bulk-ahead-of-control` (the gap becomes the whole 4 s upload) |
| `driver` | three nodes of 2 loops, a Driver per loop with its Sink's `Send` over the transport: two groups elect and commit, the first group's leader restarts, and both commit again on all three | |
| `refill` | frames of 40 KiB against a 64 KiB window, so one goes at a time: after the receiver refuses frames for 1 s, and after it restarts with a frame outstanding, a marker frame still arrives | `no-credit-on-refuse`, `no-reset-on-end` |
| `identity` | host 1 sends 20 rounds to host 2, which stores `ok`, four groups each with one open field differing, and one group not yet: none of the mismatched or unstored groups' frames arrive (80 `unopened`), a destination not held and a source on another node are refused (20 `misrouted`, 20 `spoofed`), and an unnamed source is admitted (20); the late group opens once stored; host 1 storing `ok` under another epoch is refused until host 2 stores the same; a forgotten group's frames are refused | `admit-unstored`, `admit-unopened`, `admit-misrouted`, `admit-spoofed`, `open-cluster`, `open-group`, `open-epoch`, `open-profile`, `closed-ignored`, `open-once` |
| `joiner` | `frame_admission`: four nodes of 2 loops; incarnation 4 on node 4 starts with an empty membership, catches up as a learner, is promoted and commits with the others | `voters-only` (the control) |
| `lagging` | `frame_admission`: node 3 is cut off while 4 joins, is promoted and leads by transfer; 3's membership never names 4, and 3 catches up from leader 4 once the cut heals | `voters-only` |
| `relayout` | three nodes of 2 loops with on-disk journals commit four groups; node 1 restarts with 3 loops, its journal re-partitions, two groups move loop, and all four commit on all three with node 1's frames on each group's new loop | |

A timing case that the host stalled 250 ms or more runs again, at most twice. Each step prints
as it starts, and a case still running after 100 s prints its step, whether each loop still
turns, the loops' counters and every thread's stack (`sample` on macOS, `gdb` elsewhere), then
exits 3. Mutants are
written by `test/transport/mutant_commit.py`; the plain case run at one must fail.
