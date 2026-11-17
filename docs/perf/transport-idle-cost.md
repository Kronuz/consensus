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
holds a descriptor, an armed timer or more than 64 bytes on average, when a frame is not
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

Job 261117-122812-45a7bf (exclusive, consensus:0d92ef5) measured every row below and passed
against D108. The earlier job 261117-092707-067ee0 (consensus:2c8e710) gave the same bytes and
failed only against the old per-host formula, `29 KB + 2P x 41 KB`.

Per idle group, at H = 2, 3, 5 and L = 1, 4: at most 0.2 bytes on average over G x H groups
(OpenSSL 0), 0 descriptors, 0
armed timers; every frame delivered (4,004 to 40,040 per configuration), 0 dropped, refused or
malformed. Budget: 64 bytes and none of the rest.

Per idle connection end, the fit's slope: 44,454 B at L = 1 (OpenSSL about 40.8 KB, the
transport's C++ about 3.6 KB: Connection 464 B, Link 176 B, coroutine frames, deadlines) and
48,394 B at L = 4 (inferred, not counted per thread: each loop that runs TLS adds
per-thread OpenSSL state, which grows with the ends). Budget: 48 KB (49,152 B). Two armed timers per end, keepalive and dead peer.

Per host, against `52 KB + 2P x 44 KB` (53,248 + 90,112 P bytes), within 10%:

| L | P | Counted bytes per host | Budget | Against it |
|---|---|---|---|---|
| 1 | 1 | 141,160 | 143,360 | 1.5% under |
| 1 | 2 | 230,117 | 233,472 | 1.4% under |
| 1 | 4 | 407,891 | 413,696 | 1.4% under |
| 4 | 1 | 151,608 | 143,360 | 5.8% over |
| 4 | 2 | 249,536 | 233,472 | 6.9% over |
| 4 | 4 | 442,198 | 413,696 | 6.9% over |

Fits: `52,273 + 2P x 44,454` at L = 1, `55,277 + 2P x 48,394` at L = 4. Besides these, each loop
that armed a timer holds one 64 KiB timer wheel, the scheduler's. Descriptors per host: 1 + 2P.

An idle host spends 284-1,291 us of CPU per second (0.03-0.13% of a core) at H = 2 to 5: the
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
| Per frame, control stream | 261117-122822-488d1f | 290 ns | 421 ns | 131 ns [90, 169] | 77 ns | clears |
| Per frame, data stream | 261117-122836-fb319f | 603 ns | 813 ns | 216 ns [185, 255] | 70 ns | clears |
| Per idle wake, a frame alone every 1 ms | 261117-122848-92c892 | 52.9 us | 68.4 us | 15.3 us [12.9, 20.8] | 8.7 us | clears |

Each row is one exclusive job at consensus:0d92ef5: 7 rounds for the streams, 9 rounds of 8,000
frames per run for the idle wake. In the streams the reader posts 204.8 frames per hop, so the
hop's post is spread over them; what is left is 131-216 ns per frame. A frame alone costs the
receiver about 53 us of CPU on its own loop (a TLS record read, the wake, the dispatch), and the
hop adds the group loop's wake, 15.3 us, which clears its band; its p50 latency from send to
delivery is 157.2 us against 138.1 us. The earlier 7-round job 261117-092717-30ce96 (2,000
frames per run, consensus:2c8e710) put the wake at 11.8 us [1.8, 19.3], within its 15.3 us band:
2,000 frames per run were too few to separate it from run-to-run noise.

Beside it, the bare Asio hop with no TLS and no transport (Detent `run.sh hop_cost`, job
261117-092732-0475d9, exclusive, Detent:d15d063): 793-851 ns per frame posted alone, 21-22 ns in
batches of 64, and 7.2 us of CPU per batch that wakes an idle loop (2.7 us at 16 frames). D10's
design job 261111-012904-94206c measured 832 ns, 22 ns and 6.5-7.8 us on the same Mac. The
transport's per-frame hop, 131-216 ns at about 205 frames per post, sits above `hop_cost`'s batched
figure; this bench does not split that difference further.

At 100,000 streamed frames a second, the hop costs 1.3-2.2% of a core.
