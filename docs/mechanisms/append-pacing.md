# Append pacing: frames a path can carry within the election timeout

A leader sizes each Append so its answer comes back within `frame_target()`, half the shorter of
`Timing::rpc_timeout` and `Timing::election_min` (250 ms at the defaults). Each peer keeps its
own frame size, `Peer::frame` (`include/consensus/core.h`), under the node's and group's
`rpc_bytes` and any `Flow::frame_bytes` cap. One entry always goes, whatever its size; an entry
larger than the frame goes alone.

## The rule

- A leader starts every peer at 64 KiB (`frame_floor`).
- A successful answer to an append that carried entries paces the next one. If it took longer
  than the target, the frame becomes `bytes * target / took`, but never less than the floor. If
  it took at most half the target and filled at least half the frame, the frame doubles, up to
  `Limits::rpc_bytes`.
- A flight that times out (`rpc_timeout`) drops the frame back to the floor, along with the
  existing return to one flight at verified progress.

On a fast path the frame reaches `rpc_bytes` in five answers (64 KiB to 2 MiB). On a slow path
it settles where one append crosses in about the target. With pipeline 1, throughput is then
roughly `target / (target + round trip)` of the path's rate.

The floor bounds the pacing from below. Under about 256 KB/s a 64 KiB frame already takes longer
than the target, and under about 128 KB/s it takes longer than `rpc_timeout`, so floor frames are
resent while they still cross.

## Why: the full-window stall

Xapiand's netlab bench (consensus:e12ee46, job 261120-172038-99d4ac) keeps 32 commands of
64 KiB in flight across three nodes over 20 ms links with 1% loss or 5% reordering. Before
pacing, the cluster stopped committing and its sockets went idle on heartbeats. With one command
in flight it committed steadily.

Each step of the cause, with the code as it stood at consensus:8801b99:

1. **The batch bound was `rpc_bytes`** (`core.h:1183`). A Driver group's `Core` was built with
   `Flow{}` (`worker.h:150`; `WorkerLimits::flow` now sets it, default `Flow{}`), so `frame_bytes` is 0 and an append carries up to 2 MiB. Thirty-two
   64 KiB commands fill exactly one such append.
2. **One frame held the connection for seconds.** Under netem loss or reordering, TCP cubic at
   40 ms round trip moves about 0.5 MB/s, so a 2 MiB frame took about 4 s to cross. Every Raft
   message to that follower used the same Control connection (`send_message`,
   `transport/host_transport.h`), so heartbeats and votes queued behind the frame. With a flight
   out, the leader also sent no heartbeat (`core.h:1122`).
3. **The group lost its leader.** The followers heard nothing for longer than `election_min`
   (1 s), so they ran pre-votes. A follower whose log was missing the batch could win, because
   the other follower granted. The leader's CheckQuorum (`core.h:1077-1079`) also saw no answer
   within `election_max` (2 s) and stepped down.
4. **Timeouts duplicated the batch.** At `rpc_timeout` 500 ms, the leader resent the whole batch
   while its first copy was still crossing (`core.h:1121`). That explains the bench's four copies
   in 16 s. The copy's answer then arrived for a superseded flight, which did not slow the next
   one.
5. **The bench went idle.** The commands placed on the old leader were truncated by the new
   leader. Their `committed()` waits until some term commits at their indexes. The bench is a
   closed loop, so it stopped proposing and nothing else committed. The bench's diag file records
   the leader only when the load starts (`bench.py` `begin_sampling`), so the elections did not
   show there.

The same happens at 31 commands (1.94 MiB). The bound matters only through the time one frame
takes to cross, not through any edge at exactly `rpc_bytes`.

Pacing bounds that time, so the followers hear their leader within the election timeout. A
grown frame that stops being answered returns to the floor, so later flights stop resending a
full batch while its first copy is still crossing.

## Evidence

`test/full_window_stall_test.cc` runs three `Core`s at default `Timing`, `Limits` and `Flow`.
Its links deliver in order at a fixed byte rate after 20 ms, as TCP does under loss. Each
heartbeat or vote waits behind the bytes already on the link.

| Case | Before (consensus:8801b99) | After |
|---|---|---|
| 600 B/ms, 32 x 64 KiB in flight, 30 s | 4 elections, 2.3 commits/s, 2 MiB appends | 0 elections, 6.7 commits/s (the path carries 9.2) |
| 50,000 B/ms, 2 s | 2 MiB appends | appends grow to 1 MiB, 0 elections |
| 50,000 then 1,500 B/ms, 10 s measured | 3.3 commits/s | 15.8 commits/s (the path carries 22.9) |

Without the timeout's return to the floor, the last case commits 6.4 commits/s: a grown frame
that times out is answered only for a superseded flight, which paces nothing.

In a longer scratch model of the same links (32 in flight, 20 s), pacing matched the old
throughput at 10,000 B/ms (752 against 770 commits) and beat it at 3,000 B/ms (31.4 against
11.2 commits/s) and 1,500 B/ms (15.7 against 3.3).

The netem bench is criterion A3 of Detent's consensus-full-window-stall quest. Its sustained run
keeps 32 x 64 KiB in flight for 60 s over three nodes at 20 ms, and
`bench/netlab/full_window_check.py` checks that every 5 s window after the first 10 s commits,
with no role change. The windows end where the load does: the bench caps a load at 128 MiB
(2,048 x 64 KiB), so a fast load answers before 60 s.

| Shape | Before (consensus:8801b99) | After |
|---|---|---|
| 1% loss | elections and stalled windows in both runs: no answer in 90 s; 21 role changes and 63 commits | every window commits, no role change: 0.44 and 0.49 MB/s |
| 5% reorder | every window commits: 1.82 and 3.62 MB/s, p99 2,784 and 1,694 ms | every window commits: 3.15 and 2.99 MB/s, p99 1,253 and 1,343 ms |

Each cell gives two runs. Under 5% reorder both builds commit steadily; their runs vary more than
they differ, so two runs do not separate their throughput.

Under reordering the leader's paced appends average 213 to 794 KiB (median 576 KiB, largest 1,152
KiB) where the unpaced leader sends 2 MiB; nothing shrank them to a stall.
