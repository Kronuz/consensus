# The host transport's idle cost and hop CPU

What an idle group, an idle connection end and a host hold in the host transport
([host-transport.md](../mechanisms/host-transport.md)), and what the hop of a received batch to
its group's loop costs in CPU, against Detent `docs/design/async-drivers.md` D10's budgets and
the per-host budget Detent PLAN.md D108 set, `52 KB + 2P x 44 KB`.

## Where it was measured

Every number here was measured on this Mac: Intel Core i9-9980HK (8 cores, 16 threads), macOS
15.8.1, Apple clang 17.0.0, x86-64 binaries (checked with `file`), OpenSSL 4.1.0, in exclusive
campaign jobs. No Linux gate host was used and nothing here was timed on Linux: A2 names the
Linux gate host, and this Mac stands in for it (Detent PLAN.md D111).

CI's Linux runners gave only the counts that do not depend on timing. The ctest entry
`consensus_transport_idle` runs `cost --quick` on every CI platform and fails when an idle group
holds a descriptor, an armed timer or more than `230 + 40P` bytes on average (D112), when a frame is not
delivered, or when an idle connection end holds more than 48 KB. In CI run 44139926700 (Kronuz/consensus, branch commit consensus:0264544) it passed on
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
Drivers arm whatever the transport does. Every count is read while every loop is parked, so no
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
loops; once every frame arrived and the transport's counters stopped moving for 200 ms, the
per-group counts are read. Per host is the counted bytes over H; per end, the slope of a least
squares fit of bytes per host against 2P, over H = 2, 3 and 5. Ten thousand groups per host is
quest transport-feed-scan's check (D109), not this one's: `HostTransport::feed` scans every group
per frame, and at 10,000 groups connections end (job 261117-092742-231632).

The per-host count is the steady state after a host's TLS context is built. `cost` first runs an
unprinted warm-up configuration (2 hosts, 1 loop, 100 groups), which absorbs what a process builds
once: OpenSSL's context, certificate store and first-handshake state. Building it costs about
281,016 B more per host, once (562 KB for the warm-up's two hosts): the first configuration in a
process holds 422,176 B per host, the same configuration after the warm-up 141,160 B (review probe
261117-133940-27f2ae). D108's `52 KB + 2P x 44 KB` budgets the steady state, not that one-time
cost (Detent PLAN.md D111).

## A1: idle cost

Job 261117-172945-6b1b06 (exclusive, consensus:76bacd2) measured every row below and passed.

Per idle group, over G x H = 1,000 x H groups, at H = 2, 3, 5 (P = 1, 2, 4 peers) and L = 1, 4:
0 descriptors, 0 armed timers, every frame delivered (4,004 to 40,040 per configuration), 0
dropped, refused or malformed. Its bytes are all C++ heap (OpenSSL 0), the identity store
transport-identity added to the host transport (Detent PLAN.md D112) and not what traffic leaves
(0.2 B before identity, D108's 64 B):

| L | P | Per idle group | Stored | Its frames | `230 + 40P` |
|---|---|---|---|---|---|
| 1 | 1 | 253.1 B | 204.8 | 48.3 | 270 |
| 1 | 2 | 285.5 B | 204.8 | 80.7 | 310 |
| 1 | 4 | 366.2 B | 204.8 | 161.5 | 390 |
| 4 | 1 | 255.0 B | 206.3 | 48.7 | 270 |
| 4 | 2 | 287.3 B | 206.3 | 81.0 | 310 |
| 4 | 4 | 367.9 B | 206.3 | 161.5 | 390 |

"Stored" is what a group holds the moment it is stored: 206 B at 4 loops, the identity store. The
job's C++ block counts per 100 groups name it: the `Stored` node (80 B), its `Opening` (a
`make_shared` of the 80 B `GroupOpen` and its serial, 112 B) and the rest in the per-loop
`unordered_map` bucket arrays (12.8 or 14.3 B per group). "Its frames" is what its traffic then
adds: the connections that open the group each record it, about 40 B per peer (the 32 B
`Stored::opened` block and the lanes' sorted `opened` vectors). The bound is `230 + 40P` (D112):
30 B over the fit `200 + 40P`, which the measured points exceed by 6-15 B; they sit 15-25 B under
`230 + 40P`. D112 sends the shrinking to quest transport-feed-scan
(A5: back to 64 B, or the minimum field by field).

Per idle connection end, the fit's slope: 44,666 B at L = 1 and 48,792 B at L = 4 (inferred, not
counted per thread: each loop that runs TLS adds per-thread OpenSSL state, which grows with the
ends). Budget: 48 KB (49,152 B). Two armed timers per end, keepalive and dead peer.

Per host, against `52 KB + 2P x 44 KB` (53,248 + 90,112 P bytes), within 10%:

| L | P | Counted bytes per host | Budget | Against it |
|---|---|---|---|---|
| 1 | 1 | 142,712 | 143,360 | 0.5% under |
| 1 | 2 | 232,379 | 233,472 | 0.5% under |
| 1 | 4 | 410,778 | 413,696 | 0.7% under |
| 4 | 1 | 153,064 | 143,360 | 6.8% over |
| 4 | 2 | 252,341 | 233,472 | 8.1% over |
| 4 | 4 | 446,154 | 413,696 | 7.8% over |

Fits: `53,513 + 2P x 44,666` at L = 1, `56,158 + 2P x 48,792` at L = 4. Besides these, each loop
that armed a timer holds one 64 KiB timer wheel, the scheduler's. Descriptors per host: 1 + 2P.
The host rows are 1.5 KB (1%) above the job before identity at both L; that is not attributed.

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

Beside it, the bare Asio hop with no TLS and no transport (Detent `run.sh hop_cost`, job
261117-092732-0475d9, exclusive, Detent:d15d063): 793-851 ns per frame posted alone, 21-22 ns in
batches of 64, and 7.2 us of CPU per batch that wakes an idle loop (2.7 us at 16 frames). D10's
design job 261111-012904-94206c measured 832 ns, 22 ns and 6.5-7.8 us on the same Mac. The
transport's per-frame hop, 153-222 ns at about 205 frames per post, sits above `hop_cost`'s batched
figure; this bench does not split that difference further.

At 100,000 streamed frames a second, the hop costs 1.5-2.2% of a core.
