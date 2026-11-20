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
are out (`Connection::on_read`, which also follows a read that fails partway, so a batch always
leaves under the connection its frames came on), each non-empty batch goes to its loop in one post, and the
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
not stored queues nothing; frames already queued on a connection when it is forgotten still go.
A host stores a group once `member_of` answers for it: before that, every source is unnamed and
admitted. `store` refuses a zero cluster, group or epoch, as a hello did.

**The group-open exchange.** Before a group's first frame on a connection, the sender sends
the group's open: its cluster, group identity, epoch and profile fingerprint, which every
member computes alike and none of which is membership. The receiver checks it on the group's
loop, in order with the group's frames: the group opens on that connection when it is stored
and every field matches; otherwise it closes there, and the receiver answers `closed`. A group
loop keeps, per connection (node and lane), the generation it last saw and the groups opened on
it; a batch of a newer generation empties that set, and a batch of an older generation is
stale: its frames are refused and its opens change nothing. Every connection of a node and lane
lives on one loop, which posts its batches in order, and a host's connection drains only at the
peer's `close_notify`, after which it reads nothing; so a stale batch would take a lane that
drains while its replacement reads, which the transport never starts. Each direction opens on its own. A sender opens the group again
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
Driver resume). On the connection's loop each group with something to send has an `Active`
record per connection: its control and data queues, its queued and uncredited bytes, and the
open of its latest frame. It exists only while one of those is nonzero. What the host owes the
peer for the groups it receives is apart from it: each batch's credit and `closed`s come back
to the connection's loop as one vector of 24 B `Owed` records (group, bytes, closed), a run of
one group's frames in one record, and the lane repays them in order and frees them once all are
repaid. While its connection cannot send, the records merge per group each time they double
(at 4,096 at least), so a lane owes O(groups) records. Receiving builds no per-group table. An
idle group keeps only the serial of the open the connection carries.

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

Step 1 is the lane's `Owed` records, in order. Steps 2 and 3 are ready rings, FIFOs of groups
linked through their `Active` records: `control` holds the groups with a control frame, `data`
those whose next data frame fits their credit. A group joins a ring where it becomes ready (a frame queued, credit arriving, a
connection beginning or ending, which rebuilds the rings from the `Active` records), and a
feed pops one group per frame; a group still ready goes to the back. A feed's work is O(1) per frame whatever the
groups a host stores, and round robin runs in the order groups became ready. A group blocked on
credit is in no ring until its credit arrives.

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

## Tables

Every per-group table is one `GroupTable<V>`: entries dense in a vector (grown by 1.5x), found
through an open-addressed index of `u32` slots (splitmix64, then `(h * n) >> 64` for the position,
so no division; linear probing at load 0.5-0.75; backward-shift deletion; an erase moves the last
entry into the hole). A set is `GroupTable<Empty>`, 8 B an entry.

| Table | Loop | Entry |
|---|---|---|
| `Loop::stored` | the group's | incarnation, serial, open (96 B) |
| `Opened::groups`, per connection | the group's | the group (8 B) |
| `Lane::opened` | the connection's | the serial of the open it carries (16 B) |
| `Lane::active` | the connection's | `Active`, while it has something to send or uncredited |

`Lane::owed`, what the host owes the peer, is a vector of records in arrival order, not a table
(Send, above).

What an idle group holds, field by field: its id (8), incarnation (8), serial (8) and open (80:
cluster, group identity and epoch, 16 each, and the profile fingerprint, 32, which hashes the
group's own configuration and voters, so nothing in it is shared between groups); per
connection whose peer opened it, its id on the group loop (8); per connection that carries its
open, its id and serial on the connection's loop (16); and the tables' index and growth. A
serial rises per store on the group's loop (a group never changes loop). Below this the open
leaves the transport: an `open_of(group)` callback, as `member_of` is, or an open frame that
carries a digest.

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
| `upload` | four groups upload 2 MiB appends of 1 MiB commands to a reader throttled to 40 MB/s; a heartbeat and a vote on the same connection keep under the 1,000 ms election timeout, or under 1.6 times the 8 MiB window over the reader's delivery rate across the run when the runner slows it (a control frame waits behind at most one window; 216 macos-15-intel probe runs put the gap at 0.34-1.44 of that, the reader at 6-26 MiB/s against 38 nominal); a run delivering under two windows fails | `bulk-ahead-of-control` (the gap becomes the whole 4 s upload) |
| `driver` | three nodes of 2 loops, a Driver per loop with its Sink's `Send` over the transport: two groups elect and commit, the first group's leader restarts, and both commit again on all three | |
| `refill` | frames of 40 KiB against a 64 KiB window, so one goes at a time: after the receiver refuses frames for 1 s, and after it restarts with a frame outstanding, a marker frame still arrives | `no-credit-on-refuse`, `no-reset-on-end` |
| `identity` | host 1 sends 20 rounds to host 2, which stores `ok`, four groups each with one open field differing, and one group not yet: none of the mismatched or unstored groups' frames arrive (80 `unopened`), a destination not held and a source on another node are refused (20 `misrouted`, 20 `spoofed`), and an unnamed source is admitted (20); the late group opens once stored; host 1 storing `ok` under another epoch is refused until host 2 stores the same; a forgotten group's frames are refused | `admit-unstored`, `admit-unopened`, `admit-misrouted`, `admit-spoofed`, `open-cluster`, `open-group`, `open-epoch`, `open-profile`, `closed-ignored`, `open-once` |
| `joiner` | `frame_admission`: four nodes of 2 loops; incarnation 4 on node 4 starts with an empty membership, catches up as a learner, is promoted and commits with the others | `voters-only` (the control) |
| `lagging` | `frame_admission`: node 3 is cut off while 4 joins, is promoted and leads by transfer; 3's membership never names 4, and 3 catches up from leader 4 once the cut heals | `voters-only` |
| `scan` | for 100, 1,000 and 10,000 groups on one loop, a control and a data frame each: the groups both hosts' feeds visit stay under 2 per frame sent at each count, and every group's credit returns (`uncredited` back to 0) | `main-feed`: the feed before the rings (consensus:edbb162) with `visited` counted in its scan; `uncredited-kept` |
| `order` | one turn queues data and control frames for three groups; the receiver gets control first, round robin (a0 b0 c0 a1 b1 a2 b2), then data round robin (a0 b0 a1); with a 100-byte window and group a's loop stalled, all of b's frames arrive and exactly one of a's crosses, then a's arrive in order | `ring-at-front`, `bulk-ahead-of-control`, `no-credit` |
| `reconnect` | node 1 over raw Links opens g on host 2 and its frame arrives; after that connection ends, g's frame on the next one without the open is refused (1 `unopened`) and closed back, and arrives once node 1 opens g there | `no-generation-clear`; `no-current` survives (a stale batch never reaches a group loop: see Identity) |
| `backlog` | node 1 over raw Links, through a gate that leaves host 2's bytes unread, opens 6,000 groups on host 2; with the gate held, host 2 sends it 24 MiB and node 1 sends 3 data frames to each group: host 2 owes 18,000 records with its queue full, so they merge per group; once all have arrived the gate opens and each group's credit comes back whole as values, in fewer credit frames than data frames | `coalesce-drops-credit` |
| `relayout` | three nodes of 2 loops with on-disk journals commit four groups; node 1 restarts with 3 loops, its journal re-partitions, two groups move loop, and all four commit on all three with node 1's frames on each group's new loop | |

A timing case that the host stalled 250 ms or more runs again, at most twice. Each step prints
as it starts, and a case still running after 100 s prints its step, whether each loop still
turns, the loops' counters and every thread's stack (`sample` on macOS, `gdb` elsewhere), then
exits 3. Mutants are
written by `test/transport/mutant_commit.py`; the plain case run at one must fail.
