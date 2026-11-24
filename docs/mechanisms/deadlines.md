# Core's deadlines

`Core::next_deadline()` is the earliest `Tick::now` at which a Tick changes Core's state, beyond
its clock and election delay; `maximum` when none would. A host that sleeps until it and then
ticks delays none of Core's work, and a Tick before it is harmless. Nothing else in Core reads
the clock, so a host may hold every Tick before it: `test/liveness_test.cc` does, and its
`LIVENESS_VERIFY=1` steps each held Tick and fails one that acts or moves the deadline.

## What a Tick does

`step` retires a bounded slice of erased log entries on every event, then `handle(Tick)` records
the clock and the election delay and, once started and with nothing persisting, `drive`s. Every
`Persisted` that leaves Core idle also drives, without the Tick's admission, so it does all but a
campaign, an install request and a D3 removal. The table lists every action a Tick takes, by
role, and the deadline `next_deadline()` reports for it.

| Role | Action on a Tick | Reported deadline |
|---|---|---|
| any | Retire up to 256 erased entries; until retired they count against the log limits (`charged_entries`) | now, while any remain |
| none | Before Start, persisting, fenced, retiring or at rest: nothing | `maximum` |
| follower, candidate | Fail a forwarded read unanswered after `election_max` (`expire_reads`) | its send + `election_max` |
| follower, candidate | Resend an install request, ask again after `snapshot_timeout`, or fail it after `election_max` (`ask_install`) | `install_deadline()` |
| follower, candidate | A voter below the last term pre-votes, or with a quorum of one campaigns, at its election deadline | the election deadline |
| follower, candidate | A non-voter (learner, joiner, removed member): nothing; every path that makes it a voter resets its election timer | none needed |
| follower | Re-acknowledge to its leader after a rest (`reacknowledge`, after every step) | `acknowledge_at_` |
| leader | Step down when fewer than a quorum of voters answered within `election_max` (CheckQuorum) | each voter's answer + `election_max` |
| leader | Abort a transfer not ended in `election_max` | its `until` |
| leader | Resend a lost TimeoutNow | the heartbeat deadline |
| leader | D9: a witness that leads starts a hand-off once a data voter answered live | now, while a target exists and no transfer runs |
| leader | D4: a removed leader hands off, or steps down once its transfer ends | the commit's `Persisted` drives it; then the transfer's `until` |
| leader | D4: drop the replicas a committed removal removed | the commit's `Persisted` drives it |
| leader | D3: propose the committed replacement's removal (Tick only: its admission funds the append) | now, while `removal()` holds |
| leader | Release a snapshot whose source the base outran | a base moves only in a `Persisted`, which drives |
| leader | Release a snapshot after `snapshot_timeout` | its start + `snapshot_timeout` |
| leader | Time out a flight after `rpc_timeout` | the oldest flight's send + `rpc_timeout` |
| leader | Heartbeat each replica with nothing in flight | the heartbeat deadline, with any replica |
| leader | Send a held-back append (`retry_after`, `snapshot_retry_clock`) | `retry_after`; the clock + 1 |
| leader | Keep a snapshot's peer alive | its keepalive's send + heartbeat |
| leader | Send TimeoutNow once the target matched | the acknowledgement that matches it sends it |

A follower reports each deadline as it stands, past or not. A leader reports a past heartbeat,
and every other deadline only while ahead of the clock: the last drive acted on each one due, so
one behind the clock is disarmed (a `retry_after` of 0, an answered flight).

`removal()` is the change the next admitted Tick proposes: it holds only when the Configuration
entry fits the retained log, so a full log does not report a removal that no Tick can append.
The erased entries that a compaction leaves to retire count against that room; while they
remain, each Tick retires a slice, so the removal's deadline follows from theirs.

## Omissions this table found

Each has a deterministic test in `test/core_deadline_test.cc` that fails at the code before the
fix: the deadline it names, and a Tick at it that acts, by value.

1. **D3 removal.** Once a replacement commits, the leader's removal of the leaving member waits
   for a Tick, and no deadline named it: a host that slept to `next_deadline()` delayed it to the
   next heartbeat or other deadline.
2. **D9 hand-off.** A witness leader hands off once a data voter has answered since it took
   office. That answer arrives in a `Receive`, which does not drive: the hand-off waited for the
   next heartbeat.
3. **D4 hand-off.** The drive that started a removed leader's transfer returned before its
   flights: a flight timeout due at that moment waited for the next heartbeat. That drive now
   continues, as every later one does.
4. **Retirement.** A compaction's erased entries retire 256 per step and count against the
   retained log until then; no deadline named them, so a host that slept kept them charged, and
   a proposal or removal could fail `LogFull` that a Tick would have admitted.

A non-voter's Tick at its election deadline reset that deadline, which nothing reads while it
does not vote: the write is gone, and the non-voter's Tick changes nothing.
