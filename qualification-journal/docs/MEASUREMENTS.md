# journal measurements

Point-in-time measurements and qualification runs, each with its revision, machine, and date. They describe what was true when recorded; rerun the named benchmark to measure the current code.

## Initial measurement

`journal_bench` measures 64 sequential durable groups per case, each containing opaque 256-byte operations. Initialization is excluded. On an Intel Core i9-9980HK running macOS 15.8.1, the AppleClang 17 Release build at revision `55e3072` produced these local-filesystem results on October 2, 2026:

| Operations/group | Payload bytes/group | Total wall seconds | Process CPU seconds | Operations/second | p50 batch ms | p99 batch ms |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 256 | 4.006861 | 0.070486 | 15.972603 | 62.164488 | 78.147896 |
| 32 | 8,192 | 4.020624 | 0.076460 | 509.373713 | 62.886624 | 65.350456 |
| 256 | 65,536 | 4.021802 | 0.090530 | 4,073.795345 | 62.942453 | 64.144920 |

This measures durability-barrier amortization, with no transaction encoding, replication, networking, or application execution. The maximum of 64 samples is the reported empirical p99; it cannot characterize a production tail. CPU time is measured using `getrusage`, and no hosting-price or power measurement is available to convert it into cost. The standalone demo occupies 44,544 bytes on disk; the benchmark occupies 42,248 bytes. These executable sizes are not incremental daemon footprint measurements. Run `build/journal_bench` from the repository root to reproduce the workload; it creates and removes dedicated directories under `.scratch/`.

## Checkpoint pause measurement

The initial `checkpoint_bench` separately measured preparing two opaque artifacts and publishing them as bundle plus application dependency. The same Intel Mac and AppleClang 17 Release configuration produced these single-sample results on October 2, 2026:

| Application bytes | Bundle bytes | Preparation wall s | Preparation CPU s | Publication wall s | Publication CPU s |
| --- | --- | --- | --- | --- | --- |
| 1,048,576 | 1,048,576 | 0.054763 | 0.010354 | 0.069765 | 0.007462 |
| 67,108,864 | 67,108,864 | 0.470017 | 0.361971 | 0.385382 | 0.321468 |

At revision `b497af6`, publication included rereading and validating both artifacts before the durable generation cutover. The approximately 385 ms sample for 128 MiB of artifacts is a material pause when setting election deadlines; it is not a tail bound. These opaque bytes do not represent a measured Detent key corpus or actual suffix encoder. Run `build/journal_checkpoint_bench` from the repository root; initialization is excluded, and the workload removes only its own dedicated scratch directories. No cost or power conversion is available.

The version 2 Release demo occupies 57,008 bytes, versus 44,544 bytes for the initial version 1 build (+12,464 bytes). The append benchmark occupies 49,032 bytes versus 42,248 (+6,784 bytes); the new checkpoint benchmark occupies 64,448 bytes. These are standalone executable file sizes from the same local toolchain, not measured runtime memory or integrated daemon footprint.

Readback now runs explicitly before cutover. Repeating the workload with preparation plus verification measured separately produced:

| Application bytes | Bundle bytes | Preparation and verification wall s | CPU s | Publication wall s | CPU s |
| --- | --- | --- | --- | --- | --- |
| 1,048,576 | 1,048,576 | 0.077054 | 0.011474 | 0.063021 | 0.001249 |
| 67,108,864 | 67,108,864 | 0.706718 | 0.655203 | 0.063774 | 0.000888 |

For the 128 MiB case, the single local publication sample fell from 385 ms to 64 ms, with preparation and readback outside that interval. This shifts work outside the freeze rather than eliminating checksum work. A real consensus bundle must be encoded and written after freezing its retained suffix; that work remains a separate pause budget. These samples do not establish production tail latency.

## Reclamation measurement

`reclaim_bench` publishes two checkpoint generations with two opaque artifacts each, releases preparation handles, then scans and synchronizes deletion of four obsolete files. On the same Intel Mac/AppleClang 17 Release configuration, the October 3, 2026 single samples were:

| Bytes/artifact | Logical bytes before | Logical bytes after | Logical bytes unlinked | Files removed | Cleanup wall s | CPU s |
| --- | --- | --- | --- | --- | --- | --- |
| 1,048,576 | 4,194,843 | 2,097,460 | 2,097,383 | 4 | 0.021816 | 0.001311 |
| 67,108,864 | 268,435,995 | 134,218,036 | 134,217,959 | 4 | 0.030882 | 0.010203 |

The obsolete payloads are intentionally not read; ownership headers establish eligibility. These results at revision `d93d88e` measure a nine-file directory and its deletion barrier, not large-directory throughput, physical space recovery, or production tails. Run `build/journal_reclaim_bench` from the repository root. Fixture creation is excluded, and only each workload's own scratch directory is removed afterward.

## Durable staging ownership measurement

Artifact version 2 adds one file synchronization per preparation before payload admission. The same Intel Mac/AppleClang 17 Release checkpoint workload produced these October 3, 2026 single samples:

| Bytes per artifact (two artifacts) | Preparation and verification wall s | CPU s | Publication wall s | CPU s |
| ---: | ---: | ---: | ---: | ---: |
| 1,048,576 | 0.101068 | 0.014643 | 0.062845 | 0.001569 |
| 67,108,864 | 0.766365 | 0.671041 | 0.062841 | 0.001165 |

The preceding version 1 artifact samples took 0.077054 and 0.706718 seconds preparing and verifying the same workloads. These individual runs expose the additional barrier cost but do not isolate it statistically. Publication remains approximately 63 ms in these samples. The format adds 12 header bytes per artifact, does not duplicate payloads, and does not change the checkpoint encoder. Reproduce with `checkpoint_bench`; no power or hosting-cost conversion is available.

## Qualification log

Each completion-driven milestone was qualified across the cluster, Detent, and sanitizer suites before it landed.

### Resumable append operations

The injected append tests run both drivers with identical before/after IO faults, partial writes and v1/v2 frontiers, comparing recovered histories under both namespace outcomes. Suspended completions prove that executed IO alone cannot publish metadata, stale tokens cannot release work, and facade retirement retains backend and lock lifetime. Native journal, consensus and Worker regressions pass; ASan/UBSan passes all three relevant suites. The initial native backend is described below; complete Store/Worker asynchronous accounting integration remains pending.

### Native BSD completion backend

The real-file fixture writes a 150,000-byte batch using five native writes and five fallback primitives, then recovers an append completed after its facade and original caller disappear. The Asio fixture holds a completion for 100 ms while the owner services timers and UDP loopback traffic, verifies no early frontier publication, and emits cancellation while persistence continues. Both native and ASan/UBSan tests pass on the Intel Mac. All ten root cluster CTests and all 30 Detent CTests pass after integration. Standalone Release fixture executables occupy 125,000 bytes (backend) and 426,592 bytes (Asio); these include their test programs and do not measure incremental daemon footprint or resident memory. The existing service Worker still runs synchronous storage; backend qualification alone does not establish an asynchronous authority service.

### Completion-driven Store appends

Append ownership qualification passes all ten native cluster CTests and all 30 native Detent CTests. The final retirement regression passes in the focused 35-check Worker suite; journal, Worker and both completion fixtures pass under ASan/UBSan. These gates preserve synchronous behavior and exercise the opt-in append path, rather than certifying the unfinished asynchronous service.

### Completion-driven artifact preparation

Artifact preparation qualification passes all ten native cluster suites (58.12 seconds), all 30 Detent suites (37.23 seconds), and journal/Worker/both completion fixtures under ASan/UBSan (45.10 seconds). Builds report no warnings. These gates qualify the shared artifact algorithms and Store ownership, with Worker artifact dispatch and the complete production asynchronous service still pending.

### Completion-driven checkpoint publication

Checkpoint publication qualification passes ten native cluster suites (58.13 seconds), all 30 Detent suites (36.85 seconds), and journal/Worker/both completion fixtures under ASan/UBSan (46.24 seconds). The final builds report no warnings. These gates qualify the shared publication protocol and owned Store settlement; production activation remains pending.

### Owned payload read completion

The BSD backend submits payload reads with native `aio_read()` and separately reports completed read bytes. A real macOS fixture verifies 150 KB through native reads without fallback and reaps a read after all reader/Journal/backend facades disappear. Model tests cover short reads, held results, stale tokens, exact verifier settlement, old capabilities, invalid progress, before/after IO failures and borrowed-backend rejection. GC primitives, Worker metadata/payload dispatch and Linux backend qualification remain pending.

Owned payload-read qualification passes ten native cluster suites (58.69 seconds), all 30 Detent suites (37.09 seconds), and four storage-related ASan/UBSan suites (47.18 seconds), with warning-free builds. A local paired synchronous-read sample compares the prior committed headers with this shared driver using Apple Clang `-O3`, one 32 MiB artifact read/CRC-verified four times per sample, 64 KiB chunks, and three alternating before/after trials. Median process CPU is 0.308539 seconds before and 0.308893 after; median wall time is 0.308966 and 0.309304 seconds. The approximately 0.1% difference is below the observed sample variation and establishes no speed or cost/power improvement. On this Intel Mac, `sizeof(ArtifactReader)` changes from 96 to 128 bytes and `sizeof(ArtifactVerifier)` from 112 to 160 bytes; these exclude shared-control allocations, files and owned asynchronous buffers. This cached synchronous microbenchmark does not measure cold-storage or asynchronous service capacity.

### Completion-driven artifact metadata opening

Metadata opening qualification passes ten native cluster suites (58.66 seconds), 30 Detent suites (36.15 seconds), and journal/Worker/both completion ASan/UBSan suites (48.35 seconds), with warning-free builds. This qualifies shared metadata/payload operations and their facade-retirement ownership, while production Worker dispatch and the complete authenticated service remain pending.

### Linux owned completion backend

The local Linux 6.8/ext4 guest qualification passes five standalone suites and all eleven cluster suites, both natively and under ASan/UBSan. Detent passes 42 native tests and 41 sanitizer tests plus its added native snapshot profile. Real TLS fixtures require native setup and exercise snapshot catch-up. The standalone `completion_bench native 200` and `completion_bench fallback 200` commands compare 200 fresh-store 4 KiB appends. Three alternating pairs give median wall times of 1126.24 and 1102.45 ms, process CPU of 325.907 and 324.098 ms, and peak RSS of 3840 KiB in both modes. This sample establishes no speed, CPU, cost or power saving; it qualifies ownership and execution away from the owner, not fleet capacity or host power-loss durability.
