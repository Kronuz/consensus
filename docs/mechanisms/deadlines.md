# Core's deadlines

`Core::next_deadline()` is the earliest `Tick::now` at which a Tick changes Core's state, beyond
its clock and election delay; `maximum` when none would. A host that sleeps until it and then
ticks delays none of Core's work, and a Tick before it is harmless. Nothing else in Core reads
the clock, so a host may hold every Tick before it: `test/liveness_test.cc` does, and its
`LIVENESS_VERIFY=1` steps each held Tick and fails one that acts or moves the deadline.

## What a Tick does

`step` returns at once for a fenced or retiring Core; otherwise it retires a bounded slice of
erased log entries on every event (at most 256, and at most `command_bytes` of payload, but always
one), then `handle(Tick)` records
the clock and the election delay and, once started and with nothing persisting, `drive`s. Every
`Persisted` that leaves Core idle also drives, without the Tick's admission, so it does all but a
campaign, an install request and a D3 removal. The table lists every action a Tick takes, by
role, and the deadline `next_deadline()` reports for it.

| Role | Action on a Tick | Reported deadline |
|---|---|---|
| any | Retire a slice of erased entries; until retired they count against the log limits (`charged_entries`) | now, while any remain |
| none | Fenced or retiring: nothing, retirement included | `maximum` |
| none | Before Start, persisting or at rest: nothing but retirement | `maximum`, but for retirement at rest (persisting, the `Persisted` steps next) |
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
| leader | D3: propose the committed replacement's removal (Tick only: its admission funds the append) | now, while `removal()` holds and its entry fits |
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

A removal is due only when its Configuration entry fits the retained log, so a full log does not
report a removal that no Tick can append. Retirement is reported first: when the fit is read,
nothing is left to retire, so the Tick's own step frees no more room than `next_deadline()` saw.
While erased entries remain, each Tick retires a slice, and the removal's deadline follows.

## What the Driver re-arms

The Driver keeps one wheel entry per group, armed at `Worker::next_deadline()`. A turn the Worker
ends with Pressure (a Control reservation of the Tick's plan does not fit the pool) does not move
Core's clock or deadline. When that deadline is behind the clock, arming at it would re-step the
group on every wheel pass, so the Driver arms `retry_ns` (1 ms, the wheel's slot) ahead instead:
a pressured group costs one step per `retry_ns`, not one per wheel pass. A deadline ahead of the
clock, or `maximum`, is armed as it stands: a group whose pressure comes with no due deadline
takes no extra step.

Nothing else polls: a pool is one group's (an `Admission` per Store), and each of its releases (a
permit refunded, a durable reclaim, an IO or turn completion) is a step of that group or is
followed by one, which retries the Tick. The `retry_ns` is the floor for a release from outside a
step. The refusal loses no deadline: Core's is unchanged, and the Tick that runs after the release
carries the clock of that time. `test/driver_pressure_test.cc` counts the group's arms over 100 ms
of 0.1 ms wheel passes (`hold`: at most one per `retry_ns`, none in the past), the refused
election Tick pre-voting within one `retry_ns` of the release (`clears`), and no arm beyond Core's
deadline when it is ahead (`ahead`) or `maximum` (`rest`).

## What a planner reads

A Worker plans each event's storage before stepping it (`plan_event`), after its turn retires a
slice, and the event's step retires another before Core acts. A plan that reserves nothing for an
append Core then makes fences the group, so every Core query a planner reads must give the answer
the step acts on, whatever retirement does in between:

| Query | Read for | Depends on retirement |
|---|---|---|
| `busy()` | every event's plan; a busy Core refuses or holds | no |
| `role()` | a leader's Tick is planned as its removal, or as nothing | no |
| `removal()` | the removal's append, reserved in the leader's Tick plan | no: its conditions are role, commit and membership, never the log's room; a removal that does not fit is still planned, and `propose_membership` refuses it without an append |
| `capture_usage()` | a checkpoint's or install's retained suffix | no: it counts the live suffix, not erased entries |

Every other plan is fixed by the event's kind, so retirement can only leave a reservation unused,
never an append unreserved. `removal_plan_matches_tick` checks the removal's case where it could
differ: a lowered `log_entries` and a compaction that leaves more than one slice to retire.

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
4. **Retirement.** A compaction's erased entries retire a slice per step and count against the
   retained log until then; no deadline named them, so a host that slept kept them charged, and
   a proposal or removal could fail `LogFull` that a Tick would have admitted. A driver now steps
   until they are retired: about `log_entries`/512 + 2 · `log_bytes`/`command_bytes` Worker turns
   at most, since a turn retires twice (`run_one` and the step), each making progress.

A non-voter's Tick at its election deadline reset that deadline, which nothing reads while it
does not vote: the write is gone, and the non-voter's Tick changes nothing.
