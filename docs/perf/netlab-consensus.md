# consensus on a realistic network (netlab)

Three and five consensus nodes, each its own process in its own Linux network namespace, joined
by faultlab netlab's veth pairs with netem on every link: commit latency and throughput, RPC-lane
latency and election time at each network shape, against the same cluster on unshaped links.
The partition shape isolates the leader under a client workload, heals, and checks the clients'
history for linearizability.

## Where it was measured

One host, one VM, exclusive. Every number below ran at consensus:e12ee46 (harness) with faultlab
pinned at faultlab:b64102f, as three `exclusive` campaign jobs: nothing else ran on the host.

- **Host.** An Intel Core i9-9980HK (16 threads, 32 GiB) Mac, Darwin 24.6.0.
- **Guest.** One qemu x86_64 guest under hvf (`-cpu host`), 8 vCPUs, 8 GiB, Linux 6.8.0-142-generic,
  Python 3.12.3. All 3 or 5 nodes share its 8 vCPUs; journals are on its tmpfs.
- **Jobs.** 3 nodes: `261120-174938-19c0ac` (241 s). 5 nodes: `261120-174941-0e6ee9` (220 s).
  Calibration: `261120-174928-5e07e0`. Results are `result-N.json` under the job's `a1/out/`.
- **One run per row.** No repeats: a cell is one 1.5 to 2 s sample, and the sample counts are small
  at long delays (Results, Sample sizes). Nothing here is a confidence interval.
- **Not a production network.** Veth links with netem are a model: no switch buffers, no real
  congestion, loss and reorder drawn i.i.d. per packet.

## How it is measured

`bench/netlab/bench.py` drives it; `bench/netlab/vm.sh run --nodes N` runs it in a guest.

- **Nodes.** `bench/netlab/node.cc` is one node per process on consensus's production path: a
  `Driver` over a journal (`PosixIO`) and the host transport over mutual TLS with the test
  fixture's certificates, one voter of a fixed group. It takes commands on stdin and writes one
  JSON event per line, each stamped with `CLOCK_MONOTONIC` microseconds, the clock the harness
  reads, so a kill and the next leader's event compare across processes. Defaults throughout:
  `Timing` (election 1 to 2 s, heartbeat 100 ms), `Flow{}` (one append in flight per follower,
  which is what a `Worker` builds), `Limits` but a 768 MiB retained log (a run never
  checkpoints). Journals live on tmpfs, so a barrier is nearly free and a shape's delta over
  loopback is the network's.
- **Network.** One guest of Xapiand's VM tier (`tools/vm/cluster.sh up --nodes 1`) holds the
  whole cluster: faultlab netlab (pinned at `bench/netlab/FAULTLAB_REV`) makes a namespace per
  node and a veth pair per pair of nodes, netem on each end's egress and an nftables drop set per
  namespace. A shape sets every link: one-way delay (so the round trip is twice it), loss and
  reorder. One guest rather than one per node: the guests' shared L2 relays every frame through
  the host's Python hub, which adds its own latency and loses bursts, while veths and netem act
  inside one kernel; and one kernel gives every node one clock.
- **Each shape** starts a fresh cluster and runs, in order:
  1. Round trip (A1): ping from node 1 to each peer, 10 replies each; the median must be within
     10% of the set round trip past the guest's floor at that delay, below.
  2. Majority only (A1): the leader and as many followers as keep it a minority are cut off
     (netlab `partition` both ways); a write on the cut leader must stay unacknowledged through
     the heal, while the majority's new leader commits one.
  3. Commit latency: the leader proposes one entry at a time for 2 s, 64 B and 64 KiB; the
     latency runs from `propose` to `committed` on the leader.
  4. Throughput: 256 entries of 64 B in flight, then 32 of 64 KiB (2 MiB, an append batch's
     bound), for 2 s each.
  5. RPC lane: one cold call opens the Bulk link (dial, TLS, hellos), then 64 B echo calls one at
     a time for 1.5 s, from the leader to a follower.
  6. Elections: four times, the leader is SIGKILLed; the election time runs from the kill to the
     first survivor's event that it leads a later term. The killed node restarts on its journal
     and the new leader commits once before the next kill.
- **Partition (A2).** On the 1 ms shape, three clients per node run writes (60%) and
  linearizable reads (`Driver::read`) on four keys, each recording invocations and completions in
  faultlab's event log (`history.Recorder`; netlab writes its `isolate` and `heal` there too). After
  1 s the leader is isolated for 4 s, then healed; the clients run 3 s more, the healed leader
  reads every key, and faultlab's history checker must find no violation. A write is `fail` only
  when refused (`rejected`, `superseded`); one unanswered for 1 s stays open (unknown) and its
  client takes a new name. The new leader must be elected within one election: `election_max` +
  heartbeat + 2 round trips + 200 ms.
- **Floor at a delay.** netem holds each packet on an hrtimer, once per direction, and the guest
  wakes that timer late, so a delayed shape reads above its setting. The floor at a delay `d` is
  the unshaped round trip plus two timer wakes, each as late as a sleep of `d` wakes in the same
  guest (`timer_late`: 30 sleeps with timer slack 1 ns, their median), measured right after each
  peer's ping, so each peer has its own floor: the wakes drift by tenths of a ms within a second
  (Calibration 5). The jitter allowed is twice the wakes' spread (p90 less median), at least 0.1 ms.
  `bench.py calibrate` checks the model: ping under netem holds of 1 us to 80 ms beside the
  sleep lateness at each (Calibration, below).
- **Hunt rows** compare with ping's measured round trip plus A1's 10%: a commit at one in flight
  is one round trip, so its p50 over that plus loopback's p99, or its p99 over two round trips plus
  loopback's p99; an RPC p99 over one round trip plus loopback's; an election past 3 timeouts.

`bench/netlab/selftest.py` runs the same harness without root: faultlab's real `Netlab` over a
fake runner that records every command and answers ping from the shapes set, and a fake cluster
that elects only in a majority component, its guest timer waking 0.3 ms late, and 0.3 ms later
still during one peer's ping at 1 ms and the samples after it (a host stall the check must pass).
Six mutants each fail their check: ping off by 50% (round trip), one peer's link off by 50%
(round trip), a timer lateness measured as 0 (round trip), a
cut leader that acknowledges (majority only, history), a new leader that lacks each key's last
write (history), elections at 4 timeouts (the election bound). It also checks that a failed
measurement is a table cell, not a crash, and that the cut leader's term is read from its own
cluster's role changes.

## Results

### Calibration of the timer floor

`bench.py calibrate` (job `261120-174928-5e07e0`, exclusive): ping between two namespaces with
netem holding each packet `hold` each way (40 pings, round trip median), beside 30 sleeps of `hold`.
Model: ping's excess over `2 x hold` equals two timer wakes, `2 x` the sleeps' median lateness.

| hold ms | ping p50 | ping p10 / p90 | excess ms | 2 x late p50 | model less excess | late p90 less p50 |
|---|---|---|---|---|---|---|
| 0.001 | 0.41 | 0.26 / 0.65 | 0.41 | 0.35 | -0.06 | 1.56 |
| 0.01 | 0.33 | 0.19 / 0.69 | 0.31 | 0.23 | -0.08 | 0.72 |
| 0.1 | 2.50 | 0.84 / 3.51 | 2.30 | 3.49 | +1.20 | 0.28 |
| 0.5 | 3.70 | 3.06 / 4.12 | 2.70 | 2.90 | +0.21 | 0.13 |
| 1 | 3.42 | 3.12 / 4.25 | 1.42 | 1.91 | +0.49 | 0.49 |
| 2 | 6.12 | 5.47 / 7.44 | 2.12 | 2.74 | +0.62 | 0.11 |
| 5 | 13.95 | 11.80 / 15.70 | 3.95 | 3.05 | -0.90 | 1.31 |
| 20 | 52.70 | 46.10 / 57.90 | 12.70 | 9.19 | -3.51 | 3.97 |
| 80 | 164.00 | 162.00 / 169.00 | 4.00 | 13.94 | +9.94 | 2.57 |
| unshaped | 0.06 | | | | | |

What it shows:

1. A held packet costs one to two milliseconds more than its hold at any hold from 0.1 ms up: a 0.1
   ms hold reads a 2.5 ms round trip. The guest's hrtimers wake late by about 1 ms each way. At 1 and
   10 us the excess is 0.3 to 0.4 ms, consistent with no timer being armed (hypothesis).
2. The model fits within 1.2 ms from 1 us to 5 ms, inside the ping's own p10 to p90 width. At 20 ms ping reads 3.5 ms above the model and at 80 ms 9.9 ms below it, with ping
   spreads of 11.8 and 7.0 ms: the calibration does not pin the floor at long delays.
3. The sleep lateness is not stable between runs. The same measurement at 80 ms read a 7.0 ms
   median here, 1.2 ms in the earlier light calibration (2 x late p50 = 2.45 ms), and 0.9 to 1.0 ms inside the
   shapes. Unexplained. The round-trip check does not depend on it at long delays: its allowance
   is 10% of the set round trip (16 ms at 80 ms), and the floor term is 2 ms there.
4. In the shapes themselves, every ping median is within 1.1 ms of set + floor (below 0.4 ms at the
   1 ms shapes, where the allowance is 0.3 to 0.6 ms) at 3 and 5 nodes.
5. The lateness drifts within a run. In 261121-103053-6192c5 (5 nodes, 1 ms) node 2's
   ping median read 3.225 ms and nodes 3 to 5 read 2.51 to 2.64 ms, against set 2 ms + floor 0.601
   (lateness 0.279 ms, sampled once after all four pings) ± 0.494. The same link at the same delay
   read 2.69 ms in the partition shape later in that run, beside the others' 2.655 to 2.695 ms, so
   the shaping was right: node 2's 200 ms of pings saw wakes about 0.3 ms later than the samples
   0.6 s after them. The same row's events reached the harness up to 27 ms late, a stall of the
   guest. The floor is now sampled after each peer's ping, and each row keeps every peer's ping
   median and lateness p50 and p90 (`pings`).

### Results

| Nodes | Shape | RTT set / ping ms | Commit p50 / p99 ms (64 B) | (64 KiB) | Commits/s (64 B) | MB/s (64 KiB) | RPC p50 / p99 ms (cold) | Election ms (median, max) | vs loopback | Job |
|---|---|---|---|---|---|---|---|---|---|---|
| 3 | loopback | 0 / 0.04 | 0.41 / 0.98 | 0.96 / 45.01 | 9,750 | 9.0 | 0.24 / 0.47 (cold 4.72) | 1542, 1833 | - | 261120-174938-19c0ac |
| 3 | 1ms | 2 / 3.19 | 3.61 / 6.04 | 4.43 / 14.63 | 21,175 | 6.1 | 3.15 / 5.42 (cold 17.25) | 1797, 3468 | p50 +3.20 ms; 2.17x commits/s; 0.68x MB/s | 261120-174938-19c0ac |
| 3 | 20ms | 40 / 41.98 | 42.34 / 43.69 | 42.71 / 44.93 | 3,135 | 14.0 | 42.06 / 49.83 (cold 174.73) | 2137, 3489 | p50 +41.93 ms; 0.32x commits/s; 1.56x MB/s | 261120-174938-19c0ac |
| 3 | 80ms | 160 / 161.25 | 162.81 / 165.23 | 163.10 / 326.44 | 964 | 4.8 | 162.34 / 164.04 (cold 654.34) | 3540, 4881 | p50 +162.41 ms; 0.10x commits/s; 0.54x MB/s | 261120-174938-19c0ac |
| 3 | loss1%-20ms | 40 / 41.98 | 42.76 / 61.88 | 168.15 / 294.30 | 2,123 | error: no answer in 32 s | 42.45 / 44.25 (cold 1258.61) | 2141, 6276 | p50 +42.35 ms; 0.22x commits/s; - MB/s | 261120-174938-19c0ac |
| 3 | reorder5%-20ms | 40 / 41.50 | 42.40 / 44.56 | 84.91 / 86.52 | 3,141 | 1.1 | 42.05 / 44.57 (cold 157.29) | 1615, 3120 | p50 +41.99 ms; 0.32x commits/s; 0.12x MB/s | 261120-174938-19c0ac |
| 3 | partition-1ms | 2 / 3.36 | - | - | - | - | - | 1395 (bound 2304); rejoin 1423 | 2411 ops, 441 acked writes, 0 violations | 261120-174938-19c0ac |
| 5 | loopback | 0 / 0.04 | 0.53 / 1.15 | 1.17 / 39.73 | 4,832 | 8.6 | 0.22 / 0.54 (cold 5.42) | 1279, 1564 | - | 261120-174941-0e6ee9 |
| 5 | 1ms | 2 / 3.30 | 4.41 / 6.73 | 5.05 / 8.02 | 17,947 | 6.4 | 3.44 / 5.75 (cold 18.22) | 1393, 1596 | p50 +3.88 ms; 3.71x commits/s; 0.74x MB/s | 261120-174941-0e6ee9 |
| 5 | 20ms | 40 / 41.95 | 43.76 / 45.74 | 43.24 / 44.62 | 3,073 | 17.4 | 42.59 / 45.01 (cold 175.74) | 1539, 2968 | p50 +43.23 ms; 0.64x commits/s; 2.02x MB/s | 261120-174941-0e6ee9 |
| 5 | 80ms | 160 / 162.00 | 163.58 / 165.55 | 162.95 / 323.76 | 672 | 1.8 | 162.14 / 162.93 (cold 655.95) | 1590, 1931 | p50 +163.05 ms; 0.14x commits/s; 0.21x MB/s | 261120-174941-0e6ee9 |
| 5 | loss1%-20ms | 40 / 41.88 | 42.97 / 74.79 | 167.74 / 170.09 | 2,113 | error: no answer in 32 s | 42.42 / 44.39 (cold 180.39) | 1290, 1530 | p50 +42.44 ms; 0.44x commits/s; - MB/s | 261120-174941-0e6ee9 |
| 5 | reorder5%-20ms | 40 / 41.85 | 43.47 / 44.85 | 86.07 / 87.03 | 3,436 | 1.1 | 42.15 / 43.40 (cold 154.22) | 1260, 1340 | p50 +42.94 ms; 0.71x commits/s; 0.13x MB/s | 261120-174941-0e6ee9 |
| 5 | partition-1ms | 2 / 3.10 | - | - | - | - | - | 1455 (bound 2304); rejoin 1480 | 3968 ops, 444 acked writes, 0 violations | 261120-174941-0e6ee9 |

The round trip is the ping median, set by netem plus the guest's floor. "vs loopback" is the shape's
64 B commit p50 less loopback's, its commits/s and its 64 KiB MB/s as ratios of loopback's. The
partition row is a different measurement: election after the leader's isolation (bound
`election_max` + heartbeat + 2 round trips + 200 ms = 2,304 ms), the cut leader's rejoin after the
heal, and the linearizability result of the clients' history.

Checks: 3 nodes 43 of 45 passed, 5 nodes 44 of 45. The three failures:

1. 3 nodes and 5 nodes, loss1%-20ms: large_throughput, "no answer in 32 s". The 32 x 64 KiB load
   never answered (Finding 1).
2. 3 nodes, loss1%-20ms: elections [1,620, 1,352, 6,276, 2,662] ms; the third is past the 3 election
   timeouts (6,000 ms) check (Finding 1).

Hunt rows (flags that are not checks):

- hunt: 3 nodes, 1ms: RPC p99 5.42 ms > RTT + loopback p99 = 3.97 ms
- hunt: 3 nodes, 20ms: RPC p99 49.83 ms > RTT + loopback p99 = 46.64 ms
- hunt: 3 nodes, loss1%-20ms: large_latency p50 168.15 ms > RTT + loopback p99 = 91.18 ms
- hunt: 3 nodes, loss1%-20ms: large_latency p99 294.30 ms > 2 RTT + loopback p99 = 137.35 ms
- hunt: 3 nodes, loss1%-20ms: election 6276 ms > 3 timeouts
- hunt: 5 nodes, 1ms: RPC p99 5.75 ms > RTT + loopback p99 = 4.17 ms
- hunt: 5 nodes, loss1%-20ms: large_latency p50 167.74 ms > RTT + loopback p99 = 85.79 ms
- hunt: 5 nodes, loss1%-20ms: large_latency p99 170.09 ms > 2 RTT + loopback p99 = 131.85 ms
- hunt: 5 nodes, reorder5%-20ms: large_latency p50 86.07 ms > RTT + loopback p99 = 85.76 ms

Sample sizes: a one-in-flight latency row holds as many commits as fit in 2 s. At 3 nodes the 64 B
and 64 KiB p99 come from 48 and 46 commits at 20 ms, 12 and 11 at 80 ms, 42 and 11 under loss,
49 and 25 under reorder (5 nodes: 46 and 46, 13 and 11, 45 and 13, 47 and 24). A p99 of 11 samples
is its maximum. The RPC rows have 10 calls at 80 ms and 30 to 39 at 20 ms; loopback and 1 ms have 400.

## Findings

1. **A full 2 MiB window stalls under 1% loss, and the election check fails with it.** At 3 and 5
   nodes `loss1%-20ms` large_throughput (32 x 64 KiB in flight, one full `Limits::rpc_bytes` batch)
   never answers in 32 s, and 3 nodes' third election took 6,276 ms (past 3 timeouts). Reorder 5%
   completes the same load at 1.1 MB/s (0.12x of loopback). Measured earlier (job
   `261120-172038-99d4ac`, light, 3 nodes, 20 ms): with loss 1% every node's applied index stays flat
   from 4 s to 90 s, TCP delivers what it is given (`bytes_acked` tracks `bytes_sent`), the sockets go
   idle (heartbeats only), and one entry in flight commits under the same loss. Raising
   `Timing::rpc_timeout` to 5 s does not cure it, and the stall is not the bench's timeout. The
   lead's diagnosis (msg-185): election churn. A Driver group's Core sends up to 2 MiB per append;
   at about 0.5 MB/s under loss or reorder that holds the Control connection for about 4 s,
   heartbeats and votes queue behind it, and CheckQuorum steps the leader down. 31 x 64 KiB stalls
   the same way. This is the bench's finding for Detent's consensus-full-window-stall (per-peer
   append pacing); these rows are before that fix, and the fix's rerun is the check that it holds.
2. **A reordering link doubles a 64 KiB commit to 2 round trips; the hunt flag at 5 nodes is the
   bound's tail, not noise.** With reorder 5% the one-in-flight 64 KiB commit reads p50 84.91 / p99
   86.52 ms at 3 nodes (25 commits) and 86.07 / 87.03 ms at 5 (24 commits): 2 round trips (83.7)
   plus 1.9 to 2.4 ms, with p99 within 2 ms of p50. The same link commits 64 B in 1 round trip
   (42.40 and 43.47 ms) and the 20 ms shape commits 64 KiB in 1 (42.71 and 43.24 ms). The 5-node
   flag (86.07 against a 85.76 ms bound) is 0.31 ms over, 0.4%. The bound is one round trip +
   loopback's 64 KiB p99 + 10%: 41.85 + 39.73 + 4.19. Loopback's p50 is 1.17 ms; its p99 is a
   tail of about 40 ms (3 nodes: 45.01). The 3-node row passes (84.91 against 90.7 ms) only
   because loopback's tail happened to be 5 ms longer in that run. Both runs sit at 2 round trips,
   so the system, not the margin, makes the row, and a tighter bound would flag both. The
   mechanism is not measured. Hypothesis: reordered segments of a 64 KiB write (about 45 segments)
   trigger duplicate ACKs and a spurious fast retransmit, which cuts the connection's window, and the
   next entry needs an extra round trip to send. A `ss -tin` trace of the leader's socket during
   this row would show or refute it.
3. **Loss costs a 64 KiB commit 4 round trips at the median.** `loss1%-20ms` large_latency p50 is
   168.15 ms (3 nodes, 11 commits) and 167.74 ms (5 nodes, 13 commits): 4 x 42, against 1 round trip
   unshaped, and a p99 of 294 and 170 ms. A 64 B commit reads 42.76 and 42.97 ms (p99 61.88 and
   74.79 ms). Hypothesis: a lost segment of a 45-segment entry waits for TCP's tail-loss probe or
   retransmission timer. Small samples.
4. **The timer floor is real and about 1 ms per hold.** See Calibration: netem holds at 0.1 ms to 5
   ms cost 1 to 2 ms more than their setting, so the 1 ms shape's 3.19 ms round trip is the 2 ms
   set plus the 1.14 ms floor, not an error. The round-trip check's floor is measured per delay,
   not assumed; the unshaped path's 0.035 ms (0.049 ms at 5 nodes) is the only constant. At 20 and
   80 ms the floor is small against the 10% allowance and its model is not pinned (Calibration,
   point 2).
5. **Throughput does not fall with delay as a window would.** 64 B at 256 in flight: loopback
   9,750/s (3 nodes), 1 ms 21,175/s (2.17x), 20 ms 3,135/s, 80 ms 964/s; 5 nodes 4,832, 17,947
   (3.71x), 3,073 and 672. At 20 ms the window would allow 256 entries per round trip, 6,100/s; the
   measured rate is half. 64 KiB at 20 ms is 14.0 MB/s (3 nodes) and 17.4 MB/s (5 nodes), above
   loopback's 9.0 and 8.6 MB/s. Hypothesis: loopback and 1 ms are CPU-bound on the guest's 8
   shared vCPUs (3 or 5 nodes plus the clients), and delay lets appends carry bigger batches that
   cost less per entry; not measured. The throughput rows are one 2 s sample.
6. **Partition: no violation; an election 1.4 s after the cut; a rejoin of 1.4 s.** At 1 ms one-way:
   3 nodes elected at 1,395 ms (bound 2,304) and rejoined at 1,423 ms after the heal (2,411
   operations, 441 acknowledged writes, 0 violations); 5 nodes elected at 1,455 ms and rejoined at
   1,480 ms (3,968 operations, 444 acknowledged writes, 0 violations). The cut leader steps down
   about 2.0 s after the cut, acknowledging nothing, and stays at term 1 until it rejoins at
   term 2. The rejoin is 14 heartbeats, not the next one. Hypothesis, unmeasured: TCP retransmission
   backoff on the links the partition dropped, which wake after the heal when the retransmit
   interval comes due (a 4 s cut backs a 200 ms RTO off to retransmit intervals up to 1.6 s). Earlier dry runs read 218 to
   1,334 ms.
7. **Elections vary with the shape and the node count; the cause is unexplained.** 3 nodes at 80 ms:
   [3,426, 4,881, 3,520, 3,560] ms (median 3,540), all four past the 2 s timeout maximum, against 5
   nodes at 80 ms [1,450, 1,455, 1,725, 1,931]. 3 nodes at 1 ms has one of 3,468 ms and at 20 ms two
   of 2,935 and 3,489 ms. A tied vote costing one more timeout would not repeat four times in four,
   so a structural cost at 3 nodes (two survivors, both needed for a majority) is more likely
   (hypothesis); the vote log was not read.
8. **Bench bug found and fixed: the "term 11/8" in the partition row.** The isolated leader's term
   read earlier shapes' clusters; the role-change log is now reset per shape (`Bench.start`) and the
   selftest fails without it. The rows above read before 1, new leader 2, isolated leader 1.

Calibration and the selftest's mutants stand as the A1 evidence; the rows' own checks (round trip,
majority only, history) are in each job's log.
