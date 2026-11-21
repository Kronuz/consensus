# Idle CPU per group: at rest against per-group heartbeats

What an idle group costs in CPU once it rests ([quiescence](../mechanisms/quiescence.md)),
against the same groups kept awake by per-group heartbeats, at 250,000 and 1,000,000 groups on
one host.

## The probe

`driver_close_test idle_cpu`: one loop of a journal host opens N three-voter groups, each led
from this host, on the manual clock in 1 ms steps. The two peers are synthesized: every
request the leader sends is answered at once, as caught-up voters would, so only the leader's
side is counted (the peers' own Driver cost is not). Once every group leads and its first round
is acknowledged, the window runs 1,000 steps per second and counts:

- wakes: steps where the loop's scheduler had anything queued (one wake per 1 ms step at most,
  so 1,000/s is "every step");
- handles: scheduler handles run;
- answers: peer responses delivered;
- CPU: the loop thread's CPU time (`CLOCK_THREAD_CPUTIME_ID`), per second of the window and per
  group;
- group timers armed at the end of the window (`Driver::armed_group_timers`).

The two modes differ only in `Timing::quiesce`: `rest` lets a group stop its timers after its
round of acknowledgements, `heartbeat` keeps `Timing{}`'s 100 ms heartbeat to each peer (20
sends and 20 answers per group per second). Setup uses 256 MiB turn-log segments so no live
record is forwarded while groups are created (journal's `TurnLog::forward_slice` scans every
group per turn, which would make setup quadratic).

## Results

Strict campaign jobs on this Mac (Intel Core i9-9980HK, 8 cores, 16 threads, 32 GB, macOS
15.8.1, Apple clang 17.0.0, Release), beside light holds on the other cores.

| Groups | Mode | Wakes/s per host | Handles/s | Answers/s | CPU s per s | CPU per idle group per s | Timers armed | Max RSS | Job |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1,000,000 | rest | **0** | 0 | 0 | **0.000** | **0.1 ns** | 0 | 8.2 GB | 261120-111949-10693b |
| 1,000,000 | heartbeat | 1,000 | 1,376,602 | 20,000,000 | 167.714 | 167,714 ns | 1,000,000 | 8.1 GB | 261120-113151-e99e39 |
| 250,000 | heartbeat | 1,000 | 345,294 | 5,000,000 | 45.692 | 182,768 ns | 250,000 | 2.5 GB | 261120-113138-07d7c2 |

At rest the host does nothing for its idle groups: no wake, no handle, no timer, and the 10 s
window's CPU rounds to 0.1 ns per group per second (the loop's own empty checks). With
per-group heartbeats every group costs about 170 to 180 us of CPU per second on the leader's
side alone, linear in the group count: one host leading 1M groups would need 168 cores of this
CPU to keep pace, and 250,000 groups 46. At this part's 45 W TDP for 8 cores (5.6 W a core, an
estimate, not a power measurement), that is about 940 W per host at 1M groups, before the peers'
side; at rest it is none.

The 1M heartbeat run took 729 s: 347 s to create the groups, 201 s more to elect them and
deliver a round, and 168 s for the 1 s window, so it ran deferred (`--long`) past a strict job's
10 minutes. The rest run's window is 10 s and the heartbeat runs' 1 s; both counters are
per second.

## What is not counted

- The peers: a follower's side of a heartbeat (receive, answer, re-arm its election timer) is
  not measured here.
- The transport: the frames are handed to the Sink and answered in memory. The node-pair
  detector that replaces per-group timers at rest costs one connection's keepalive per peer, not
  per group ([transport-idle-cost.md](transport-idle-cost.md)).
- Linux: no Linux host timed this; the probe runs on the manual clock, so its counts do not
  depend on the platform, its CPU does.

## Reproduce

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCONSENSUS_TRANSPORT=ON
cmake --build build --target consensus_driver_close_test
IDLE_CPU_GROUPS=1000000 IDLE_CPU_MODE=rest IDLE_CPU_SECONDS=10 build/consensus_driver_close_test idle_cpu
IDLE_CPU_GROUPS=250000 IDLE_CPU_MODE=heartbeat IDLE_CPU_SECONDS=1 build/consensus_driver_close_test idle_cpu
```
