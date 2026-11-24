# Follower read

A follower answers a `Read` only at an index its leader confirmed current with a quorum, after
the request reached the leader, once the follower itself applied that index. This is ReadIndex
(Raft dissertation, section 6.4) with the leader's existing probe round: no leases, no clock
bound. Detent's voter read freshness (D144, `docs/design/voter-read-freshness.md` in Detent)
builds on it.

## The rule

```
follower: Read{request} ──ReadIndexRequest{term, start, token}──▶ leader: one more PendingRead
                                                                  probe sent after admission
          ◀──ReadIndexResponse{term, start, token, Ready, index}── quorum answered that probe
          applied >= index before send + election_max ──▶ ReadReady{request, index}
          otherwise, or on a refusal, a leader change or a term change ──▶ Reject
```

`index` is the leader's commit index when the request arrived. A quorum answering a probe sent
after that arrival proves no other leader committed anything the request could miss, so every
write completed before the follower's `Read` is at or below `index`. The follower answers from
its own applied state at or past it.

## Follower

| State at `Read` | Result |
|---|---|
| Busy (a persistence outstanding) | `Reject Busy`, as at a leader |
| Not started, a witness, a candidate, no leader known, no start nonce | `Reject NotLeader` at once |
| `limits.reads` reads already forwarded | `Reject Busy` |
| A request id already forwarded | `Reject DuplicateRequest` |
| A follower with a leader | `ReadIndexRequest` to it; the read waits |

A forwarded read completes or ends on the first of:

- **Ready, then applied.** `ReadReady{request, index}` once `applied >= index`.
- **A refusal.** `NotReady`, `Busy` or `NotLeader` from the leader, at once, with that reason.
- **Leader or term change.** Any change of the follower's (term, leader): a higher term heard
  from anyone, `LeaderGone`, `Gone`, its own campaign. `Reject NotLeader` at once.
- **The deadline.** `election_max` after the send, if neither the answer nor the apply arrived:
  `Reject NotLeader`. One deadline covers both, so a follower that learns the index but not the
  entries (a cut between them, a slow catch-up) refuses within the same bound. The cost: a
  follower more than `election_max` behind in apply refuses, and the client moves to a member
  that is current.

A request is sent once. A lost frame costs the read its deadline; the host retries.

## Identity

A forwarded read is `{start, token}`. `start` is `Start::nonce`, a random value the host draws at
each Core start; the Driver takes 64 bits from `std::random_device` (never its seeded stream, so a
restart under a fixed seed still names a new start). `token` is the Core's counter, which restarts
at 0. The follower accepts a response only:

1. Naming its current `start` and a pending token not yet answered.
2. From the peer the read went to.
3. At the read's term, while the follower's term and leader are those it sent under.

Everything else is dropped. A follower that restarts in the same term while the leader still holds
its earlier read reuses that read's token; the leader's answer to it carries the old `start`, so
it never completes the new read, whose index must cover writes that completed after the old one
was admitted (review V1). Within one start each token is used once, so an answer queued during a
stall matches only the read that asked it, and a read issued while another is in flight never
completes from that one's answer.

A host that gives no nonce (`Start{}`) keeps today's behavior: a follower refuses `Read` with
`NotLeader`.

## Leader

A `ReadIndexRequest` is one more `PendingRead`, under the local read's rules:

| Condition | Answer |
|---|---|
| Not the leader of the request's term, or a witness | `NotLeader` |
| No entry of its own term committed yet (after every election) | `NotReady` |
| `limits.reads` reads pending, local and forwarded together | `Busy` |
| A resend of a pending `{peer, start, token}` | nothing; the pending read answers once |
| Otherwise | admitted at the commit index, confirmed by a probe sent after admission |

Confirmed, a forwarded read is answered `Ready` with its index at once: the follower, not the
leader, waits for the apply. A read admitted while another's probe is in flight waits for a later
probe (`needed_probe`), so its confirmation always follows its arrival (review V5). On a step-down
every pending forwarded read is answered `NotLeader`, so the follower learns it at once rather than
at its deadline. A leader that fences answers nothing; its followers refuse at their deadlines.

A minority leader (cut off with some followers) never gathers its quorum: its followers' reads
wait until its CheckQuorum steps it down (`NotLeader`), or until their own deadlines, whichever is
first. Both are within `election_max`.

## Rest

A follower holding a forwarded read never rests (its leader's frames do not rest it), `Read`
wakes a resting follower, and the read's deadline is in `Core::next_deadline()`, so a host that
ticks only at deadlines refuses on time even when the leader goes silent after its last
heartbeat set a later election timeout. A resting leader wakes on the `ReadIndexRequest`, as on
any frame, and stays awake while a read is pending.

## Wire

Two DTC2 control frames, little-endian u64 fields after the kind and term:

| Kind | Frame | Fields |
|---|---|---|
| 12 | `ReadIndexRequest` | term, start, token |
| 13 | `ReadIndexResponse` | term, start, token, result (0 Ready, 1 NotReady, 2 Busy, 3 NotLeader), index |

`index` is nonzero exactly when the result is `Ready`; encoding refuses anything else
(`std::invalid_argument`), and decoding refuses it, a result above 3 and trailing bytes
(`Corruption`). The Core also drops a frame with a zero term, start or token. `fuzz/seed.cc` and
`fuzz/corpus/decoders/seed-read-index-*` seed both kinds for `fuzz_decoders`.

**Version.** `wire_version` stays 1, as for kinds 9 to 11: formats are open until the first
release, and every node of a cluster runs one build. Beside an older build, a follower sends kind
12 only when its host calls `Read` there, and the older leader drops the unknown kind; the read
refuses at its deadline.

## Cost

Per read: two control frames of 36 and 52 bytes, and the leader's probe round, which every read
admitted before the probe goes out shares. Idle state: four words per Core (the start nonce and an
empty vector of forwarded reads); a pending read is 48 bytes at the follower and the leader's
`PendingRead` gains three words.

## Tests

`test/follower_read_test.cc` (`consensus_follower_read`), one function per case, each named on the
command line (`follower_read_test CASE [seeds]`). `test/follower_read_mutants.py REF NAME` writes
each mutant as a commit on REF; the plain test of its case fails there.

| Case | Shows | Mutant |
|---|---|---|
| `confirmed` | the follower's read completes at the leader's commit index at arrival, only once the follower applied it | `apply-skipped` |
| `stale` | a leader cut off with one of five voters confirms nothing while the majority commits; the follower's read is refused, never answered | `ready-without-quorum` |
| `lost` | `LeaderGone` in the same term refuses the read at once; the old leader's later answer completes nothing | `leader-change-kept` |
| `term` | the follower's own campaign, and a higher term from another voter, refuse it at once | `campaign-keeps-reads` |
| `identity` | answers from another peer, at another term, for another start or token are ignored; the leader's own completes | `any-peer`, `any-term` |
| `restart` | a follower restarts in the same term while the leader holds its read unconfirmed and a write commits; its new read reuses the token; the leader's answer to the old read, below the write, does not complete it (V1) | `start-ignored` |
| `deadline` | an answered read whose apply never arrives, and an unanswered one whose leader heartbeats then goes silent, refuse at `send + election_max`, with the host ticking only at `next_deadline()` (V4) | `no-deadline`, `deadline-unarmed` |
| `in-flight` | a read arriving while another's probe flies is confirmed only by its own probe; at the follower one answer completes only its read (V5) | `probe-shared`, `answer-completes-all` |
| `refusals` | `NotReady`, `Busy` and a step-down's `NotLeader` reach the follower at once (V3) | `refusal-unsent` |
| `rest` | a read at a rested group completes at once and the group rests again; with the answer lost, the follower stays awake and refuses at its deadline (V7) | `rests-with-read` |
| `nonce` | with no start nonce, a follower's read is refused and nothing is sent | `nonce-unchecked` |
| `codec` | both frames round-trip; malformed ones are refused | `codec-unchecked` |
| `randomized` | 3 and 5 voters under random partitions, drops, crashes and restarts with new nonces: every completed read, at a leader or a follower, returns an index at or above the highest commit index any replica held when it began, at a replica that applied it | |

The Driver simulation (`consensus_simulation`, through `test/driver_simulation.cc`) issues reads
at followers too, so its read oracle now covers forwarded reads.
