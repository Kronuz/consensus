# Quiescence and the closed state

An idle consensus group costs bytes, not CPU (strata Track C D12, Detent D97 Q4). A group
that stops is closed: its replica keeps one small record, the closed state, beside the
checkpoint that covers its applied position, and holds nothing else. A closed group reopens on a
frame or a request without an election when the leader it closed under is unchanged, so a
reopened lone voter's first write costs the same two barriers as a write to an open group: its
entry, and the commit index after it.

## Rest

With `Timing::quiesce`, an open group at rest arms no timer and sends nothing; the host's
node-pair detector (one per connection, not one per group) watches for a lost peer in place of
each group's heartbeats and election timers.

- **A leader rests** once every replica has acknowledged its whole log and an append carrying its
  commit index, its term's entry is committed, the membership in force is committed with no
  voter leaving, and no read, transfer or snapshot is in progress: no heartbeat, no CheckQuorum.
- **A follower rests** on an append from its leader whose commit index covers the follower's
  whole log, under a committed membership in which that leader votes and is not leaving (a
  removed leader steps down without a frame): its leader's next frame may be long off, so it
  stops its election timer. While at
  rest it holds its leader current: it refuses pre-votes and votes as within `election_min` of a
  heartbeat, except from that leader itself, whose campaign shows it no longer leads.
- **A rest ends** for a leader on any frame, a request (`Propose`, `Read`, `ChangeMembership`,
  `TransferLeadership`, `Repair`), a `PeerLimits` report or `Wake`: it heartbeats at once and
  starts CheckQuorum's window afresh, and rests again once every replica acknowledges that
  round. A follower's rest ends on `Wake` or on another member's pre-vote or vote (refused, as
  within `election_min`), on a frame that leaves it behind its leader's commit, on a term
  change, or on its own campaign (a transfer's `TimeoutNow`). Every role change ends a rest: a
  candidate or a new leader runs its timers. A follower's election timer then runs.
- **A follower that leaves rest under its leader re-acknowledges.** When a follower's rest ends
  and it keeps its leader and term (a wake, or a frame from that leader that does not rest it),
  it sends that leader an acknowledgement of its durable log: on a wake at once, or as soon as
  its persistence completes, and again after every heartbeat interval with no frame from that
  leader, until a frame rests it again. A leader at rest wakes on it. An awake leader takes it,
  since it answers none of its appends, as that follower awake: it rests again only on the
  follower's answer to a frame sent after the acknowledgement arrived, so an answer the
  acknowledgement overtook does not count. A leader that no longer leads ignores it, and the
  follower elects at its timeout.
- **A follower's running timer is fed.** A leader rests only once every replica answered a frame
  that rests it, so a follower awake under a leader at rest left rest after that answer without
  leaving its leader: the case above. Its acknowledgement goes out within a heartbeat interval,
  whether a persistence deferred it, it overtook the
  answer, or a lost one is resent, and the leader heartbeats it back to rest; a leader rests over
  a running election timer for at most a heartbeat interval (`quiescence_test`, s1 to s6). Only
  acknowledgements lost for an election timeout let the follower elect, as lost heartbeats would
  for an awake follower. Without the rule, a member's pre-vote that woke the leader and its
  followers alike left the leader at rest over a follower's timer, and that follower deposed it
  (`driver_replace_test 1 1080 quiesce`).
- **`LeaderGone{node}`** is the detector's report that `node` (a `Member::node`) stopped
  answering. A follower whose leader is on that node drops it and elects within its election
  timeout, as after a missed heartbeat. `Driver::leader_gone(node)` raises it at every open group
  following a leader there; `Driver::wake(id)` submits `Wake`. The host transport's Control
  connection per peer node is the detector: `HostTransport::on_node_gone` reports its end
  ([host-transport](host-transport.md), Node gone; `groups_test driver`: a stopped leader
  node's groups elect 1.6 s after its stop, at a 1-2 s election timeout).
- **`Gone{term}`** is the per-group report of a replica that stopped while its node stays up,
  which no node-pair detector sees. When a group fences, its Driver sends `Gone` in the group's
  term to every other member, and a follower of that replica in that term handles it as
  `LeaderGone`; from anyone else or in another term it changes nothing, and it never raises a
  term. The fenced Core sends nothing itself: the
  Driver reads its term and membership (`driver_test fence_tells_peers`; `groups_test driver`:
  a leader's group fenced at rest on a live node, a follower elects within the timeout with no
  node-gone report).

Rest changes no safety argument: a commit still needs a quorum's acknowledgement in the leader's
term and a read a quorum's probe, so a leader at rest that a partition deposed commits nothing
and serves no read in its old term. What it trades is detection: a follower at rest under a
leader it cannot reach elects only on `LeaderGone` or `Gone`, so a host enables `quiesce` only
with a detector. A restarted member is never locked out: its campaign wakes the leader at rest, or, if
it was the leader, its own pre-vote is not refused. A follower that alone hears `LeaderGone`
wakes the other with its pre-vote, and the two elect within two timeouts (`simulation_test`, quiescence cases; half of
the random seeds quiesce under a detector oracle).

What rest saves: at 1M groups on one host, no wake and no timer against 1,000 wakes/s and
about 168 us of CPU per group per second with per-group heartbeats
([quiescence-idle-cpu](../perf/quiescence-idle-cpu.md)).

## The closed state

The closed state is the replica's consensus position at close: its hard state, the end of its
log, the membership in force, the leader it knew, and the identities that bind the record to one
replica of one group. The host stores the record (Xapiand in strata's checkpoint bundle, a
parked journal group in its table entry) and hands it back at reopen. The record never carries
the applied position: that is the checkpoint's.

### Bytes

`RFTCLOS1`, little-endian, `132 + m` bytes, where `m` is the membership's encoding:

| Offset | Bytes | Field |
|---|---|---|
| 0 | 8 | magic and version, `RFTCLOS1` |
| 8 | 4 | record length, `132 + m` |
| 12 | 16 | cluster identity |
| 28 | 16 | configuration identity |
| 44 | 8 | group: the journal host's opaque group id |
| 52 | 8 | group incarnation: the journal table's incarnation of that group |
| 60 | 8 | local: this replica's member incarnation |
| 68 | 8 | current term |
| 76 | 8 | voted for, 0 for none |
| 84 | 8 | commit index |
| 92 | 8 | last log index |
| 100 | 8 | last log term |
| 108 | 8 | leader at close, 0 for none |
| 116 | 8 | membership index |
| 124 | 4 | `m`, the membership's length |
| 128 | `m` | the membership in force (`RFTM`, `encode_membership`): voters, witness, learners, the next incarnation, the leaving voter, its limits |
| 128 + `m` | 4 | CRC32C of bytes `[0, 128 + m)` |

The membership index is the log index of the `Configuration` entry that set the membership in
force, while the retained log holds it; otherwise the checkpoint's base index, at which that
membership is in force; 0 for a founding membership with no checkpoint. A lone voter's record is
192 bytes, a three-voter member's 240.

### Decoding refuses

`decode_closed_state(bytes, expected)` throws `kronuz::journal::Corruption` for:

- another magic, a length that is not the record's size, or a CRC32C that does not match;
- a cluster, configuration, group, group incarnation or local incarnation other than the
  expected one: a replaced replica (a new group incarnation, a new member incarnation) never
  takes an old replica's record;
- term 0 with a vote, a vote for incarnation 0, a commit index past the last index, a last term
  past the term, a last index and last term not both zero or both nonzero;
- a membership that does not decode (`decode_membership`), a membership index past the last
  index, or a leader that is this replica without its ballot (a leader voted for itself in its
  term). The leader need not be a member of the membership in force: a leader leads until its own
  removal commits, and its followers follow it until then.

### Closing

`Core::closed_state()` returns the record's fields only when the replica can close: started,
not fenced or retiring, no storage operation pending, no committed range out for application,
applied equal to the commit index, and a checkpoint at that position (the base index equals
applied). A follower may hold entries past its commit index; the record names its last one.
The host then closes the group: it publishes nothing more for it and stores the record beside the
checkpoint.

### Reopening

The host recovers the group from its checkpoint as after any restart and hands the record to
`Core::reopen(closed)` before `Start`. The core uses it only when it matches the recovered state
exactly: the hard state, the last index and term, the membership in force and its index. A
record that does not match is stale (the replica wrote after it, or the host kept an older one)
and is not used: the group starts as after any restart, a lone voter campaigning at once.

A matching record starts the replica in its closed term, with no election and no term change:

- **The leader at close is this replica.** It leads again in the same term: at `Start` it
  reports `RoleChanged{Leader}`, replicates to every member from its last index, and starts
  CheckQuorum's window afresh. It appends no no-op: its term's entries are already in its log.
- **Another leader.** It is that leader's follower, heard from at `Start`, so the leader-contact
  guard holds for `election_min` and a frame from that leader finds it already following.
- **No leader.** It starts as after any restart.

**The term record.** A reopened replica's first storage batch carries its hard state with
whatever that batch writes: for a leader, the first proposal's entry. One batch is one record in
one turn, so the reopen adds no barrier: the first write costs its entry's barrier and the
commit index's, as on an open group. The log after the closed state starts with the hard state
the replica resumed under.

### Through the Driver

`Driver::closed_state(id)` gives the record's bytes, bound to the configuration's identities, the
member incarnation and the host table's incarnation of the group, when the group can close now:
started, no call or frame waiting, and `Core::closed_state()`. The host checkpoints, takes the
record, then calls `close(id)`; the row closes into the host's table. `recover(id, restore,
closed)` decodes the record against the same identity before any recovery IO (damaged or
another replica's bytes complete the call with `Corruption`), recovers, and hands it to
`Core::reopen` before `Start`.

The journal stages a group reopened from its table with the table's checkpoint record as its
first record (journal's `recover_shared`). A request issued with the recover rides that turn: the
reopened leader's entry and term record go in it, and the request costs two barriers, as on an
open group. A request issued after the recover completes finds that turn written and costs a
third. Reopened without the record, a lone voter's request costs five (`driver_close_test
reopen_barriers`).

### Why reopening without an election is safe

A term has at most one leader, so a replica that led term `T` and still holds term `T` can lead
`T` again: no other replica can lead `T`, its log holds every entry it sent in `T` (a leader
sends an entry only once it is durable, and nothing else writes its log in `T`), and commitment
still needs a quorum's acknowledgement in `T`, which a replica that moved to a later term
refuses. A follower that resumes following its leader of `T` follows the only one `T` has. The
ballot survives too: the record's vote is the recovered vote, so a reopen never votes twice in a
term.

Safety needs only the term; the exact match keeps the rule conservative and the record
truthful. A reopened replica starts exactly at the position its record states, so a host can
read that position from the record instead of scanning the log, and a record the replica moved
past (a vote, an entry, a commit, a membership) costs an election instead of a resumed term.

The leader must be recorded, not inferred. A lone voter that voted for itself in `T` did not
necessarily lead `T`: a three-voter group's leader `X` of `T` can remove the third voter and then
itself, leaving that replica the only voter, with its own ballot in `T`, while `X` still leads
until its removal commits. Leading `T` again there would put two leaders in `T`, and a
conflicting entry at one index and term fences the replica.

### Crashes

Closing writes nothing to the consensus log, and a reopen writes only with its first batch, so a
crash at any step leaves either the record and the state it matches (the reopen resumes) or a
state past it (the record is stale, and the group elects as after any restart). A host that keeps
an older record, or none, costs an election, never safety. `driver_close_test close_crash` cuts
power at every IO of rounds that write, checkpoint, close into the table with the closed state and
reopen with it, on 8 seeds (926 cuts): every acknowledged record survives, a reopen in the live
host resumes the closed term, and after the cut the last record resumes its term 162 times and is
refused as stale 462 times (the group elects a later term), never a term back.
