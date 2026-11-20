# consensus on a realistic network (netlab)

Three and five consensus nodes, each its own process in its own Linux network namespace, joined
by faultlab netlab's veth pairs with netem on every link: commit latency and throughput, RPC-lane
latency and election time at each network shape, against the same cluster on unshaped links.
The partition shape isolates the leader under a client workload, heals, and checks the clients'
history for linearizability.

## Where it was measured

TABLE_WHERE

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
     10% of the set round trip past the held floor, below.
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
- **Held floor.** netem in the guest pays its timer's latency each time it holds a packet, so
  every delayed shape reads above its setting by a constant. The floor is ping's median with netem
  holding each packet 1 us each way, measured before the shapes; the unshaped round trip is
  recorded beside it.
- **Hunt rows** compare with ping's measured round trip plus A1's 10%: a commit at one in flight
  is one round trip, so its p50 over that plus loopback's p99, or its p99 over two round trips plus
  loopback's p99; an RPC p99 over one round trip plus loopback's; an election past 3 timeouts.

`bench/netlab/selftest.py` runs the same harness without root: faultlab's real `Netlab` over a
fake runner that records every command and answers ping from the shapes set, and a fake cluster
that elects only in a majority component. Four mutants each fail their check: ping off by 50%
(round trip), a cut leader that acknowledges (majority only, history), a new leader that lacks
each key's last write (history), elections at 4 timeouts (the election bound).

## Results

TABLE_RESULTS

## Findings

TABLE_FINDINGS
