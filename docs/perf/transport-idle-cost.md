# The host transport's idle cost and hop CPU

What an idle group, an idle connection end and a host hold in the host transport
([host-transport.md](../mechanisms/host-transport.md)), and what the hop of a received batch to
its group's loop costs in CPU, against Detent `docs/design/async-drivers.md` D10's budgets and
the per-host budget Detent PLAN.md D108 set, `52 KB + 2P x 44 KB`.

## Where it was measured

Every number here was measured on this Mac: Intel Core i9-9980HK (8 cores, 16 threads), macOS
15.8.1, Apple clang 17.0.0, x86-64 binaries (checked with `file`), OpenSSL 4.1.0, timings in
exclusive campaign jobs (byte counts, which load does not move, also in light ones). No Linux gate host was used and nothing here was timed on Linux: A2 names the
Linux gate host, and this Mac stands in for it (Detent PLAN.md D111).

CI's Linux runners gave only the counts that do not depend on timing. The ctest entry
`consensus_transport_idle` runs `cost --quick` on every CI platform and fails when an idle group
holds a descriptor, an armed timer or more than `164 + 52P` bytes on average (A1 below), when a
frame is not delivered or its credit does not come back, or when an idle connection end holds more
than 48 KB. In CI run 44139926700 (Kronuz/consensus, branch commit consensus:0264544) it passed on
linux-x86_64-gcc, linux-x86_64-clang and linux-arm64-gcc, and under linux-tsan, linux-asan-ubsan
and freebsd-x86_64, where bytes are not counted (a sanitizer's allocator pads every block) and only
descriptors, timers and frames are checked. CI prints no numbers for a test that passes.

The same run failed on macos-x86_64 (OpenSSL 3.6.3, macOS's older allocator): its loops' timer
wheels are 40,960- and 45,056-byte blocks there, under the 64 KiB the bench then used to set
wheels apart, so they landed on the per-end fit (56,317 B at 4 loops). The bench now sets apart
every block at least `sizeof(wheel::timer_wheel)` (40,640 B) as a wheel, which on this Mac's
allocator are 65,536-byte blocks, so this Mac's counts do not move. macos-arm64 never got a
runner. CI run 44169921897 (consensus:4c80573) checks the cut: `consensus_transport_idle` passed
on macos-x86_64, macos-arm64 and every Linux and FreeBSD job; that run's one failure is
`consensus_transport_peers` on macos-x86_64, the known retry flake (quest peers-retry-flake).

## How it is counted

`bench/transport_idle.cc` runs H hosts in one process, each of L loops on its own thread, over
loopback mutual TLS with the test fixture's certificates. Bytes are counted exactly on this Mac, by
usable size: OpenSSL's allocations through `CRYPTO_set_mem_functions`, and the C++ heap through
`operator new`. Blocks at least a timer wheel's size are set apart: they are the loops' timer
wheels (`task::asio_scheduler`'s `timer_loop`, one per loop that armed a timer), which a host's
Drivers arm whatever the transport does. Each loop's OpenSSL thread state is set apart too: before
the first count the bench draws OpenSSL's public and private DRBGs on every loop, which OpenSSL
otherwise makes on a thread's first TLS work, and prints those bytes per loop (9,408-9,412 B under
OpenSSL 4.1.0). Every count is read while every loop is parked, so no
loop is in a write or a handshake. On glibc, Asio's handler memory goes through
`std::aligned_alloc`, which the bench's `operator new` does not see; on this Mac that hides nothing
at idle (review probe 261117-133750-4b45cc), and Linux byte counts are not verified.

macOS's zone statistics (`malloc_zone_statistics`) are printed beside the counts only: they move
by tens of kilobytes per host between identical runs, and report about one block per frame sent
that `heap`'s walk of the same process does not find (9,745 live nodes in all, at 40,004 frames).

Per configuration: the mesh comes up one pair at a time (the lower node dials both lanes, so no
dials race and no listener sees more than its 2 handshakes per source address; every host is
127.0.0.1), every node sends back, and 200 ms later the per-host and per-end counts are read.
Then 1,000 groups per host each send a control and a data frame to every peer from their own
loops; once every frame arrived, every group's credit came back (`HostTransportStats::uncredited`
is 0 on every loop, waited for up to 10 s) and the transport's counters stopped moving for 200 ms,
the per-group counts are read. The credit wait matters: a credit frame arriving moves neither the
sent nor the received count, and a sending group still waiting for its credit holds its record and
its queues' emptied blocks (2 x 4,112 B under libc++), so without it a run that read too early
counted groups in flight, not idle ones. Per host is the counted bytes over H; per end, the slope
of a least squares fit of bytes per host against 2P, over H = 2, 3 and 5.

At 10,000 groups per host (D109), the configuration whose connections ended before the feed took
ready rings (job 261117-092742-231632, 3 of 3), job 261118-003133-4c4eb0 (strict,
consensus:303cad1) ran `one 5 1 10000` three times: no connection ended, 400,040 of 400,040 frames
arrived each time, and an idle group held 298.2 B (stored 127.8 B, its frames 170.5 B), within
`164 + 52P` (372 B at P = 4). Its credit came back 561-683 ms after the last frame arrived: at
10,000 groups the last credit frames are still in flight, queued in the receivers' sockets behind
the burst, when every message frame has arrived.

The per-host count is the steady state after a host's TLS context is built. `cost` first runs an
unprinted warm-up configuration (2 hosts, 1 loop, 100 groups), which absorbs what a process builds
once: OpenSSL's context, certificate store and first-handshake state. Building it costs about
281,016 B more per host, once (562 KB for the warm-up's two hosts): the first configuration in a
process holds 422,176 B per host, the same configuration after the warm-up 141,160 B (review probe
261117-133940-27f2ae). D108's `52 KB + 2P x 44 KB` budgets the steady state, not that one-time
cost (Detent PLAN.md D111).

## A1: idle cost

Job 261118-003133-eee91a (`cost --quick`, consensus:303cad1) measured every row below and passed.

Per idle group, over G x H = 1,000 x H groups, at H = 2, 3, 5 (P = 1, 2, 4 peers) and L = 1, 4:
0 descriptors, 0 armed timers, every frame delivered (4,004 to 40,040 per configuration), 0
dropped, refused or malformed. Its bytes are all C++ heap (OpenSSL 0): the identity store
transport-identity added (Detent PLAN.md D112) and the records of the connections that carry
its open.

| L | P | Per idle group | Stored | Its frames | Minimum `104 + 24P` | Bound `164 + 52P` |
|---|---|---|---|---|---|---|
| 1 | 1 | 173.4 B | 136.8 | 36.7 | 128 | 216 |
| 1 | 2 | 210.2 B | 136.8 | 73.4 | 152 | 268 |
| 1 | 4 | 283.6 B | 136.8 | 146.8 | 200 | 372 |
| 4 | 1 | 172.1 B | 130.6 | 41.4 | 128 | 216 |
| 4 | 2 | 213.1 B | 130.6 | 82.5 | 152 | 268 |
| 4 | 4 | 295.2 B | 130.6 | 164.6 | 200 | 372 |

The bound is `164 + 52P` everywhere: in the bench, which fails a run over it, in CI's `cost
--quick`, and here. It is per peer with the group's frames on one lane (the bench sends on
Control); a group opened on both lanes of a peer holds the per-peer part twice. Where it comes
from:

1. The minimum, field by field (host-transport.md, Tables): what a group is stored as, its id,
   incarnation and serial (8 each) and its open (80), 104 B; per peer, its id in the group
   loop's set for that connection (8) and its id and serial in the connection's `Lane::opened`
   (16), 24 B. So `104 + 24P`.
2. The ceiling: each entry at the tables' worst slack, 1.5x its size (the dense vector grows by
   1.5x) plus 8 B of index (`u32` slots at load 0.5). Stored: 104 x 1.5 + 8 = 164. Per peer: 8 x
   1.5 + 8 = 20 and 16 x 1.5 + 8 = 32, 52. So `164 + 52P`.

The measured fit sits between them: about `137 + 37P` at L = 1 and `131 + 41P` at L = 4. Stored
is 27-33 B under 164, and each peer 11-15 B under 52.

Since transport-group-bytes the open is the host's (`open_of(group)`, host-transport.md,
Identity), and the last source a connection reported sits beside its id in the group loop's set
(16 B, not 8). Stored is the id, incarnation and serial, 24 B; per peer, 16 and 16, 32 B. So the
minimum is `24 + 32P` and the bound `44 + 64P` (24 x 1.5 + 8; 2 x (16 x 1.5 + 8)), and the bench
also fails a group stored over D108's 64 B. The rows at the new commit are in transport-group-bytes'
section below.

The per-end and per-host rows below are job 261118-075717-797949 (light, consensus:33f8540, Release
and no build type, 3 runs each; byte counts do not depend on load).

Per idle connection end, the fit's slope: 44,978 B at L = 1 and 46,681-46,686 B at L = 4 in a
Release build; 45,128 B and 46,736-46,737 B with no build type. Budget: 48 KB (49,152 B). Two
armed timers per end, keepalive and dead peer. The L = 4 slope still sits about 1.7 KB above
L = 1's: per end, the C++ heap holds 400-600 B more at L = 4 (Asio's per-thread handler memory,
not set apart), while OpenSSL's per-end bytes match L = 1's within 300 B.

### Each loop's thread state, set apart

A host's connections to a peer sit on that peer node's loop (`loop_of_node`), so at L = 4 the
number of loops that ever run TLS grows with P, and each brings its own DRBGs. Counted per host,
that per-loop cost grew with P and the fit put it in the slope: 49,107-49,109 B at L = 4 in a
Release build and 49,154-49,159 B with no build type, at main consensus:5923b6c (job
261118-073615-da90ae, 3 runs each). The ctest entry passed in a Release suite (job
261118-051311-f0eff6) and failed in jobs that configured no build type (261118-072938-1fb596), by
2 to 7 bytes. Counting the DRBGs per host instead (consensus:d2eeb0d, job 261118-074432-acdca6)
charges every loop's, used or not: 30 KB per 4-loop host, which puts those hosts 11-26% over
`52 KB + 2P x 44 KB`. Set apart per loop, beside the timer wheels, neither the host nor the end
carries them.

A connection end 4 KB larger fails the check in both builds: job 261118-075718-7593b8 adds a
4,096-byte member to `Connection` and reads L = 4 slopes of 50,829 B (Release) and 50,874 B (no
build type). At L = 1 the same mutant reads 49,121 B in Release, 31 B under the budget, so only
the L = 4 fit catches it there.

Per host, against `52 KB + 2P x 44 KB` (53,248 + 90,112 P bytes), within 10% (Release):

| L | P | Counted bytes per host | Budget | Against it |
|---|---|---|---|---|
| 1 | 1 | 134,232 | 143,360 | 6.4% under |
| 1 | 2 | 224,523 | 233,472 | 3.8% under |
| 1 | 4 | 404,170 | 413,696 | 2.3% under |
| 4 | 1 | 142,648 | 143,360 | 0.5% under |
| 4 | 2 | 237,019 | 233,472 | 1.5% over |
| 4 | 4 | 422,941 | 413,696 | 2.2% over |

Fits: `44,410 + 2P x 44,978` at L = 1, `49,689 + 2P x 46,681` at L = 4. Besides these, each loop
that armed a timer holds one 64 KiB timer wheel, the scheduler's, and each loop that ran TLS about
9.4 KB of OpenSSL thread state. Descriptors per host: 1 + 2P.

An idle host spends 289-981 us of CPU per second (0.03-0.10% of a core) at H = 2 to 5: the
keepalives, one per connection end per second. macOS reports no voluntary context switches through
`getrusage`, so the bench's wake count reads 0 here and counts nothing.

## A2: hop CPU

Host 1 sends to host 2, whose two loops hold 256 groups on the connection's loop ("same") and 256
on the other ("hop"). The metric is host 2's CPU over both loops per frame delivered. A round runs
same, hop, hop, same; the effect is the hop pair's mean minus the same pair's, the interval is the
median and range over rounds, and the A/A band is the largest same-to-same or hop-to-hop
difference of any round. An effect counts only when its median clears that band.

| Row | Job | Same | Hop | Hop - same, median [range] | A/A band | Verdict |
|---|---|---|---|---|---|---|
| Per frame, control stream | 261117-172946-981ce0 | 296 ns | 448 ns | 153 ns [137, 172] | 62 ns | clears |
| Per frame, data stream | 261117-172946-bbf981 | 642 ns | 863 ns | 222 ns [182, 232] | 66 ns | clears |
| Per idle wake, a frame alone every 1 ms | 261117-172946-10ce1e | 46.6 us | 59.0 us | 12.5 us [10.3, 16.2] | 10.1 us | clears |

Each row is one exclusive job at consensus:76bacd2: 7 rounds for the streams, 9 rounds of 8,000
frames per run for the idle wake. All three costs clear their bands. In the streams the reader
posts 204.8 frames per hop, so the hop's post is spread over them; what is left is 153-222 ns per
frame. A frame alone costs the receiver about 47 us of CPU on its own loop (a TLS record read, the
wake, the dispatch), and the hop adds the group loop's wake, 12.5 us; its p50 latency from send to
delivery is 141.3 us against 120.9 us. The full-bench job 261117-172946-7024b4 (7 rounds of 2,000
frames per run in the same pass) agrees on the streams (162 ns [134, 183], band 86; 209 ns [189,
215], band 31) and puts the wake at 14.4 us [11.6, 23.3], within its 15.7 us band: 2,000 frames
per run are too few to separate it from run-to-run noise, as the earlier 7-round job
261117-092717-30ce96 found (11.8 us [1.8, 19.3], band 15.3 us). The wake row's 8,000-frame
job is the one that counts.

The rows before transport-identity (consensus:0d92ef5, jobs 261117-122822-488d1f, -122836-fb319f,
-122848-92c892) were 131 ns, 216 ns and 15.3 us: identity moved none of them outside its band.

Against main's feed (consensus:edbb162), the same streams in one job, base, branch, branch,
base, at 3 rounds each (strict, beside 26 light holds, job 261118-002026-757230,
consensus:303cad1): data, same 1,041, 758, 531 and 747 ns of receiver CPU per frame, hop 1,186,
936, 660 and 866 ns, so the branch is at or under main at each position; control, same 513, 605,
429 and 331 ns, about 20-30% over main. The control cost is the group loop's two `GroupTable`
lookups per frame (`stored` and the connection's opened set), against one `unordered_map` find
and a short vector scan on main (profile job 261118-001853-9bb0ba). Before the receiver's credit
became a vector of records (consensus:01e2c06, job 261117-205819-0701eb) the data stream cost
1,575-1,608 ns against main's 644-647 ns: each batch built a credit table and a sender's record
per credited group, then freed both.

Beside it, the bare Asio hop with no TLS and no transport (Detent `run.sh hop_cost`, job
261117-092732-0475d9, exclusive, Detent:d15d063): 793-851 ns per frame posted alone, 21-22 ns in
batches of 64, and 7.2 us of CPU per batch that wakes an idle loop (2.7 us at 16 frames). D10's
design job 261111-012904-94206c measured 832 ns, 22 ns and 6.5-7.8 us on the same Mac. The
transport's per-frame hop, 153-222 ns at about 205 frames per post, sits above `hop_cost`'s batched
figure; this bench does not split that difference further.

At 100,000 streamed frames a second, the hop costs 1.5-2.2% of a core.
