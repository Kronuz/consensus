# Flow control on the host transport (Track C T7)

Track C's T7 (strata `docs/design/track-c.md`) on the host transport
([host-transport.md](../mechanisms/host-transport.md)): one group's replication throughput at 0,
1 and 10 ms of added round trip, pipelined against `pipeline = 1`; 100 groups on one connection
with one follower's barrier slowed 10×; and the longest wait of a control frame behind data
frames, which stands as R19's evidence (Detent D123). T7's snapshot row waits for
`group-install`.

## Where it was measured

The Linux gate VM: Ubuntu, Linux 6.8.0 x86_64, 4 vCPUs and 6 GiB under qemu with HVF, its
virtio disk a file on APFS, on a MacBook Pro with an i9-9980HK (8 cores, 16 threads) and 32 GiB
running macOS 15.8.1. Each row ran as an exclusive job (alone on the host), at consensus
`e7a453b` (`d511a44` plus the VM script's `build` mode).

## How it is measured

`bench/flow_bench.cc` runs three nodes in one process, each one loop thread with a `Core` per
group, over loopback mutual TLS with the test fixture's certificates. `bench/flow_bench_vm.sh`
builds it in the Linux gate VM and sets `netem` on `lo` for each row: half the round trip each
way, and for the R19 rows optionally a 1 Gb/s rate.

The bench drives `Core` directly, not the Driver: `Worker` builds every `Core` with `Flow{}`, so a
Driver group always runs `pipeline = 1` (`include/consensus/worker.h`). Each node's persists go
to its storage, one thread that writes every pending batch into a preallocated 64 MiB file and
makes them durable with one `fdatasync`: a turn barrier, on the VM's virtio disk. Appends that
arrive while a group's `Core` persists wait in a 256-message inbox and step as one `Receive`
(the Worker's merge rule: same leader, same term, contiguous). Node 1 campaigns first, so it
leads every group; each run measures after the group commits and a 1 s warm-up.

- `one`: one three-voter group proposes 256 KiB commands whenever its leader is not persisting
  (a leader persists one proposal at a time). `Flow{pipeline, 4 MiB, frame}` with frame 256 KiB
  (R19's append cap, one command per append) or 0 (`rpc_bytes`, 2 MiB). Committed MB/s at the
  leader, and commit latency from proposal to the leader's `Committed`.
- `many`: 100 three-voter groups, `Flow{4, 4 MiB, 256 KiB}`, each proposing a 4 KiB command every
  10 ms. Node 2 stores group 0 on its own storage thread whose barrier is stretched to 10× its
  own `fdatasync` (the unslowed arm splits storage the same way at 1×). Hot rows make group 0
  propose 256 KiB commands as fast as it can. With three voters the leader commits through node 3
  whatever node 2 does, so the row also reports when node 2 applies each entry: the latency a
  head-of-line block on the shared connection would move. Group 0's undelivered bytes are the
  entry bytes its leader sent to node 2 less those node 2's loop was handed, sampled every 2 ms.
- `gap`: two nodes of two loops; a control frame every 10 ms carries its send time, while four
  groups keep their 4 MiB window and two frames more queued on the Control lane (the same
  connection) or the Bulk lane. Each control frame's delay is its arrival less its send time on
  the same clock; the stated bound is one data frame, one 64 KiB feed and the kernel's socket
  buffers (`tcp_wmem` and `tcp_rmem` maxima) at the measured data rate.

To reproduce, from a checkout's root (CMakeLists.txt stays as it is: the hook runs at the end of
`project(consensus)` and defers the target until `consensus::transport` exists):

```sh
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_PROJECT_consensus_INCLUDE=$PWD/bench/flow_bench.cmake
ninja -C build consensus_flow_bench && (cd build && ctest -R consensus_tls_certificates)
bench/flow_bench_vm.sh a1   # or smoke, a2, a3: the rows below, in the gate VM
```

Each row prints the leader's proposals, what refused them, and its log at the end. The bench
never checkpoints, so it sets the retained log limit to 1 GiB (`FLOW_LOG_MIB`): at the default
64 MiB a group of 256 KiB commands is full after 256 entries, and every later proposal is refused
`LogFull` (job `261118-113818-5a98ce`: 231 to 759 refusals per row). Rows that hit
`uncommitted_entries` (1024) say so as `refused busy`.

The project-named variable is the one the jobs used; the plain `CMAKE_PROJECT_INCLUDE` would also
run for each dependency's `project()` call and define the target twice.

## A1: one group's throughput

Job `261118-114928-9bef7d`. Each cell is committed MB/s, then commits per second (commands
over the 3.00 timed seconds), then commit p50 from proposal to the leader's `Committed`. The
256 KiB rows ran three times each and show the median, with the range in MB/s; the others ran
once. The storage's own barrier: `fdatasync` of 2 MiB p50 2.30 ms (738 MB/s); the modeled 1 ms
barrier measures p50 2.56 ms in the VM.

| Barrier, frame | RTT | `pipeline = 1` | `pipeline = 4` | Pipelined |
|---|---|---|---|---|
| modeled, 256 KiB | 0 ms | 48.6 MB/s (43.9-49.4), 185/s, 376 ms | 54.4 MB/s (54.3-55.5), 208/s, 10.3 ms | 1.12× faster |
| modeled, 256 KiB | 1 ms | 32.8 MB/s (32.2-33.2), 125/s, 1.54 s | 55.5 MB/s (55.2-57.1), 212/s, 12.5 ms | 1.69× faster |
| modeled, 256 KiB | 10 ms | 14.8 MB/s (14.6-15.0), 56/s, 2.22 s | 54.8 MB/s (54.0-54.9), 209/s, 23.9 ms | 3.70× faster |
| modeled, 2 MiB | 0 ms | 62.8 MB/s, 240/s, 10.7 ms | 54.2 MB/s, 207/s, 10.4 ms | 14% slower |
| modeled, 2 MiB | 1 ms | 71.0 MB/s, 271/s, 15.2 ms | 54.3 MB/s, 207/s, 12.5 ms | 24% slower |
| modeled, 2 MiB | 10 ms | 88.1 MB/s, 336/s, 1.15 s | 56.2 MB/s, 214/s, 22.8 ms | 36% slower |
| fdatasync, 256 KiB | 0 ms | 69.6 MB/s, 265/s, 1.13 s | 112.9 MB/s, 431/s, 5.8 ms | 1.62× faster |
| fdatasync, 256 KiB | 1 ms | 61.2 MB/s, 234/s, 1.65 s | 112.2 MB/s, 428/s, 7.4 ms | 1.83× faster |
| fdatasync, 256 KiB | 10 ms | 17.1 MB/s, 65/s, 2.28 s | 77.5 MB/s, 296/s, 1.40 s | 4.53× faster |

At R19's 256 KiB frame, `pipeline = 1` moves one append per round trip and follower barrier, so
it falls with RTT (48.6 to 14.8 MB/s), while `pipeline = 4` holds 54-56 MB/s from 0 to 10 ms.
The commit latencies at `pipeline = 1` are queue age: the bench proposes whenever the leader is
not persisting, so the leader runs ahead of replication up to `uncommitted_entries` (the
`fdatasync` rows at `pipeline = 1` reach the cap, 1024, and are refused `Busy`).

`pipeline = 4` is held by the leader's storage, not the transport. Its leader makes two barriers
per command, half carrying the proposal and half only the commit index (128 KiB each on
average, about 1250 barriers in 3 s, the storage thread busy throughout): a `Core` persists one
proposal at a time, and with one entry per append each acknowledgment advances the commit by
one entry and persists it. With 2 MiB appends `pipeline = 1` carries up to eight entries per
append (node 2's barriers average 2048 KiB at 10 ms), one commit persist covers them, and it
beats `pipeline = 4`, which still sends each entry as soon as the leader has persisted it. With
the 2.3 ms `fdatasync` the same pair gives 431 commands/s at 0 ms; at 10 ms the pipelined leader
also runs ahead to the uncommitted cap.

## A2: 100 groups, one follower's barrier slowed 10×

Job `261118-114946-13cf10`, modeled 1 ms barrier, three runs per arm (hot rows) or one (the
others and fdatasync). The 99 other groups offer 40.55 MB/s (4 KiB every 10 ms each).

| RTT | Arm | Others: leader commit | Others: node 2 applies p99 | Group 0 at node 2 | Group 0 undelivered, most |
|---|---|---|---|---|---|
| 1 ms | hot, slow 1× | 40.51-40.69 MB/s | 37.3-52.0 ms | 32.9-34.8 MB/s, p99 40-58 ms | 512 KiB |
| 1 ms | hot, slow 10× | 40.50-40.57 MB/s | 35.7-36.7 ms | 22.6-23.8 MB/s, p99 1.34-1.44 s | 768 KiB |
| 1 ms | 4 KiB, slow 1× | 40.52 MB/s | 35.5 ms | 0.41 MB/s, p99 31 ms | 8 KiB |
| 1 ms | 4 KiB, slow 10× | 40.55 MB/s | 34.9 ms | 0.41 MB/s, p99 77 ms | 12 KiB |
| 10 ms | hot, slow 1× | 40.46-40.63 MB/s | 49.8-51.1 ms | 34.9-35.7 MB/s, p99 55-57 ms | 768 KiB |
| 10 ms | hot, slow 10× | 40.51-40.61 MB/s | 49.8-50.3 ms | 20.4-21.7 MB/s, p99 1.74-1.79 s | 768 KiB |
| 10 ms | 4 KiB, slow 1× | 40.57 MB/s | 49.5 ms | 0.41 MB/s, p99 46 ms | 8 KiB |
| 10 ms | 4 KiB, slow 10× | 40.57 MB/s | 49.0 ms | 0.41 MB/s, p99 89 ms | 12 KiB |
| 1 ms, fdatasync | hot, slow 1× | 39.68 MB/s | 108 ms | 36.2 MB/s, p99 953 ms | 1 MiB |
| 1 ms, fdatasync | hot, slow 10× | 39.96 MB/s | 69 ms | 25.0 MB/s, p99 2.24 s | 1 MiB |

The slowed barrier stays with its group. The other 99 groups commit all they offer in every
arm, and node 2 applies them no later with group 0 slowed than without (p99 35.7-36.7 ms against
37.3-52.0 ms at 1 ms, the same 50 ms at 10 ms): no head-of-line block on the shared connection.
Group 0's leader still commits 33-36 MB/s through node 3; at node 2 it applies 28-43% less
(20.4-23.8 against 32.9-35.7 MB/s) with a p99 above a second. Its undelivered bytes peak at
768 KiB (1 MiB with `fdatasync`), within `Flow`'s own bound of four 256 KiB appends and far under
the transport's stated bound for one group on one connection, its 4 MiB window plus its 8 MiB
data queue. No inbox overflowed and the leader's transport dropped no frame.

## A3: the longest control-frame wait behind data (R19)

PENDING
