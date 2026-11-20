# Quiescence and the closed state

An idle consensus group costs bytes, not CPU (strata Track C D12, Detent D97 Q4). A group
that stops is closed: its replica keeps one small record, the closed state, beside the
checkpoint that covers its applied position, and holds nothing else. A closed group reopens on a
frame or a request without an election when the leader it closed under is unchanged, so a
reopened lone voter's first write costs the same two barriers as a write to an open group: its
entry, and the commit index after it.

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
- a membership that does not decode (`decode_membership`), a leader that does not vote in it, or
  a leader that is this replica without its ballot (a leader voted for itself in its term).

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

### Why reopening without an election is safe

A term has at most one leader, so a replica that led term `T` and still holds term `T` with the
log it led with can lead `T` again: no other replica can lead `T`, its log holds every entry it
sent in `T` (a leader sends an entry only once it is durable), and commitment still needs a
quorum's acknowledgement in `T`, which a replica that moved to a later term refuses. The exact
match is what proves "still holds": any durable change after the record (a new term, a vote, an
entry, a commit) makes it stale. The ballot survives too: the record's vote is the recovered
vote, so a reopen never votes twice in a term.

The leader must be recorded, not inferred. A lone voter that voted for itself in `T` did not
necessarily lead `T`: a three-voter group's leader `X` of `T` can remove the third voter and then
itself, leaving that replica the only voter, with its own ballot in `T`, while `X` still leads
until its removal commits. Leading `T` again there would put two leaders in `T`, and a
conflicting entry at one index and term fences the replica.

### Crashes

Closing writes nothing to the consensus log, and a reopen writes only with its first batch, so a
crash at any step leaves either the record and the state it matches (the reopen resumes) or a
state past it (the record is stale, and the group elects as after any restart). A host that keeps
an older record, or none, costs an election, never safety.
