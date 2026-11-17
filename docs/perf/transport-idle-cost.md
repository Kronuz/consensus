# The host transport's idle cost and hop CPU

What an idle group, an idle connection end and a host hold in the host transport
([host-transport.md](../mechanisms/host-transport.md)), and what the hop of a received batch to
its group's loop costs in CPU, against Detent `docs/design/async-drivers.md` D10's budgets.

## Where it was measured

Every number here comes from this Mac: Intel Core i9-9980HK (8 cores, 16 threads), macOS
15.8.1, Apple clang 17.0.0, x86-64 binaries (checked with `file`), OpenSSL 4.1.0. No Linux
machine is rented, so nothing here was timed on the Linux gate host the quest names. CI's Linux
runners check only the counts, which do not depend on timing: the ctest entry
`consensus_transport_idle` runs `cost --quick` on every CI platform and fails when an idle group
holds a descriptor, an armed timer, a frame or more than 64 bytes, or an idle connection end more
than 48 KB. CI runs once the quest branch is pushed.

## How it is counted

`bench/transport_idle.cc` runs H hosts in one process, each of L loops on its own thread, over
loopback mutual TLS with the test fixture's certificates. Bytes are counted exactly, by usable
size: OpenSSL's allocations through `CRYPTO_set_mem_functions`, and the C++ heap through
`operator new`. Blocks of 64 KiB or more are set apart: they are the loops' timer wheels
(`task::asio_scheduler`'s `timer_loop`, one per loop that armed a timer), which a host's Drivers
arm whatever the transport does. Every count is read while every loop is parked, so no loop is in
a write or a handshake.

macOS's zone statistics (`malloc_zone_statistics`) are printed beside the counts only: they move
by tens of kilobytes per host between identical runs, and report about one block per frame sent
that `heap`'s walk of the same process does not find (9,745 live nodes in all, at 40,004 frames).

Per configuration: the mesh comes up one pair at a time (the lower node dials both lanes, so no
dials race and no listener sees more than its 2 handshakes per source address; every host is
127.0.0.1), every node sends back, and 200 ms later the per-host and per-end counts are read.
Then 1,000 groups per host each send a control and a data frame to every peer from their own
loops; once every frame arrived and the transport's counters stopped moving for 200 ms, the
per-group counts are read. Per host is the counted bytes over H; per end, the slope of a least
squares fit of bytes per host against 2P, over H = 2, 3 and 5.

<!-- A1 and A2 tables follow from jobs. -->
