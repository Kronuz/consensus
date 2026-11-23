# Install request

A replica whose application state is damaged or lost below what `repair` can rewind to gets it
back from its leader's image. `Driver::reinstall(group)` asks; the leader ships its base through
the existing snapshot channel; the follower installs it and completes the call as `repair` does,
at the image's boundary with the epoch advanced. `repair(b)` only rewinds within
[own base, own commit], so it cannot restore files the replica lost; this closes that gap.

## The request

```
follower                         leader
  reinstall ──InstallRequest{term, token, base}──▶  one image per requester per leader base?
            ◀─InstallRequestResponse{Started}────   requested = true
            ◀════ SnapshotNeeded … image ═══════   the snapshot channel, as for a lagging peer
  install (log and commit kept), activate, re-deliver (boundary, commit], RepairCompleted
```

- **Follower.** `RequestInstall` is refused `Busy` while anything persists, an image is in
  flight, a delivered range is outstanding or a request is pending; `LeaderMustTransfer` on the
  leader; `InvalidRepair` on a witness; `NotLeader` with no leader known. Otherwise the Core
  records the request (leader, term, token) and sends `InstallRequest` with its base boundary.
  While it is pending the follower delivers nothing, refuses `LocalCheckpoint` and `Repair` as
  `Busy`, and never rests, so damaged state never becomes a base.
- **Leader.** It answers `NotLeader` (with the leader it knows) when it does not lead the
  frame's term or is a witness; `NotMember` to a witness or an incarnation it does not replicate
  to; `InFlight` while an image to the peer is under way or owed; `Current` when it has no image
  (base 0), its base is below the requester's, or it already shipped this requester an image at
  its current base; else `Started`. Started marks the peer `requested`, which sends the peer down
  the snapshot path once its flights drain. `matched` is left alone, so verified progress never
  rewinds.
- **Acceptance.** An image at or below the commit index is `CaughtUp` as before, unless the
  follower's pending request was to that image's leader in that term and it retains the
  boundary's entry. Then the install keeps the log above the boundary and the commit index; base
  and applied move to the boundary. At activation the epoch advances, `RepairCompleted` answers
  the call, and the committed entries after the boundary are delivered from the follower's own
  log. No acknowledged entry is dropped, so quorum durability holds. A follower whose log lags
  below the leader's base takes the image through the ordinary path, and the request binds to it.
- **Answers.** `Current` fails the call `rejected{Current}`, `NotLeader` as `NotLeader`,
  `NotMember` as `InvalidRepair`. `Started` and `InFlight` keep it waiting.

## One image per requester per leader base

The leader's `Peer` keeps `imaged`, the base of the last image that member installed or rejected
(any install: requested or lagging). A request at that base is `Current`. The entry lives
in the leader's per-peer state, so a leader change drops it and a new leader starts afresh; a
base move (a leader checkpoint) makes it stale, allowing one more image. A voter whose base is
above the leader's never gets one, however often it asks; a damaged replica at the leader's base
gets exactly one installed. The worst case is one image per member per leader checkpoint or
leader change, plus one per lost request record (below), and only when the host calls `reinstall`.

`requested` stays set across a transfer that fails or goes stale (`Unavailable`, `TimedOut`,
`Stale`), so the leader retries the same base's image through the existing retry clocks. It
clears when the peer answers for a whole image: `Installed`, `CaughtUp` or `Rejected`. A
follower that rejects its image is not sent another at that base: its next request is `Current`.

`CaughtUp` leaves `imaged` unchanged. The follower's request lives only in memory, so a restart,
or a call that fails `NotLeader` with its answers lost, drops it while the leader still owes the
image; the image then lands with no request pending and is `CaughtUp`. Counting it would leave a
damaged replica at the leader's base `Current` until the leader checkpoints. Not counting it
opens no loop: a follower with a request pending to that leader in that term accepts its image
(its base cannot move while the request is pending, and the boundary is committed, so it retains
the boundary's entry), so a `CaughtUp` image means the record was lost, and each further image
needs a fresh host call.

## Races

- **Leadership moves before the image is admitted.** The leader releases its snapshot
  (`LeadershipLost`). The follower's request is bound to (leader, term): on any change of either,
  or on its own campaign, it fails `NotLeader`, and the host asks the new leader. A stale image
  arriving after the new term is refused `StaleTerm`; a partial one ends at the follower's
  `snapshot_timeout`, and the next call waits behind it (Worker `Busy`).
- **Leadership moves after admission.** The pending install owns the request; it finishes and
  completes the call. Its boundary is committed, so it is valid under any later leader.
- **A second request.** A second `reinstall` is `Busy`. Resent frames get `InFlight` while the
  image is under way, `Current` once it is done: one install either way.
- **A request to a non-leader.** `NotLeader` with the leader it knows; the requester's Core learns
  its leader from appends, as usual.
- **Lost frames.** Unanswered, the follower resends each heartbeat; unanswered for an election
  timeout, the call fails `NotLeader`. Answered `Started` or `InFlight`, it asks again after
  `snapshot_timeout` with no image admitted.
- **The leader's base moves while its source is pending.** The existing `Stale` release applies;
  `requested` stays set, and the leader retries at the new base.

## Wire

Two DTC2 control frames, big-endian u64 fields after the kind and term:

| Kind | Frame | Fields |
|---|---|---|
| 10 | `InstallRequest` | term, request token, base index, base term |
| 11 | `InstallRequestResponse` | term, request token, result (0 Started, 1 InFlight, 2 Current, 3 NotLeader, 4 NotMember), leader |

Decoding refuses a result above 4 and trailing bytes; `fuzz/seed.cc` and
`fuzz/corpus/decoders/seed-install-request*` seed both kinds for `fuzz_decoders`.

**Version.** `wire_version` (node_hello.h) stays 1. Formats are open until the first release:
no released build speaks wire 1, and every node of a cluster runs one build, as when `Gone`
(kind 9) was added at wire 1. The frames are also safe beside an older build: a follower sends
kind 10 only from `reinstall`, and a leader sends kind 11 only in answer, so an older node
never receives one unprompted; an older leader drops or refuses the unknown kind, and the call
fails `NotLeader` within an election timeout. After the first release a new kind bumps
`wire_version`.

## Cost

One image per real need, plus two 44-byte control frames per request, resent only until the
leader answers. Idle state: two words per leader peer (`requested`, `imaged`) and one pointer per
Core (the follower's pending request, off the idle object).

## Tests

`test/driver_install_request_test.cc`, one ctest per case (`consensus_install_request_<case>`):

| Case | Shows |
|---|---|
| `lost_state` | a current follower with lost state gets one image at the leader's base, its commit index kept, and re-delivers (base, commit] from its own log; the same on a bare Core (A1) |
| `behind` | a follower whose log lags the leader's base gets one install and catches up (A1) |
| `current` | a voter above the leader's base asks 100 times: every call `Current`, no SnapshotNeeded, no frame (A2) |
| `after_install` | a damaged replica at the leader's base gets exactly one image in 21 calls (A2; kills the loop-guard mutant) |
| `lost_record` | the requester's call fails `NotLeader` with its answers lost; the owed image then lands `CaughtUp`, and the next call still gets the image at the leader's base, then `Current` (A2) |
| `unrequested` | an image at or below the commit index with no request, or from another node, is `CaughtUp` (A2) |
| `leader_lost` | leadership moves after SnapshotNeeded and mid-transfer: `NotLeader`, no activation; the new leader ships one (A3) |
| `second_request` | a second call is `Busy`, a resent frame gets `InFlight`, one transfer (A3) |
| `non_leader` | a follower redirects; `LeaderMustTransfer`, `NotLeader`, `InvalidRepair` for a witness and a removed incarnation, `NotLeader` when unanswered (A3) |
| `witness_bound` | `Driver::witness_bound` is Core's; a witness truncates through it keeping every record above, and one past it is `InvalidCheckpoint` (A4; kills the bound+1 mutant) |
