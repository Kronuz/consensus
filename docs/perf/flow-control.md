# Flow control on the host transport (Track C T7)

Track C's T7 (strata `docs/design/track-c.md`) on the host transport
([host-transport.md](../mechanisms/host-transport.md)): one group's replication throughput at 0,
1 and 10 ms of added round trip, pipelined against `pipeline = 1`; 100 groups on one connection
with one follower's barrier slowed 10×; and the longest wait of a control frame behind data
frames, which stands as R19's evidence (Detent D123). T7's snapshot row waits for
`group-install`.

## Where it was measured

PENDING: the jobs' host line.

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

The project-named variable is the one the jobs used; the plain `CMAKE_PROJECT_INCLUDE` would also
run for each dependency's `project()` call and define the target twice.

## A1: one group's throughput

Each row states committed MB/s and the group's commits per second (commands over the timed
seconds) at `pipeline = 1` and pipelined side by side.

PENDING

## A2: 100 groups, one follower's barrier slowed 10×

PENDING

## A3: the longest control-frame wait behind data (R19)

PENDING
