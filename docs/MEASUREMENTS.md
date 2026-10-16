# consensus measurements

Point-in-time measurements and qualification records, each with its machine, build, and date. Rerun the named benchmark to measure the current code.

## Local checkpoint measurement

`consensus_checkpoint_bench` establishes its fixture through bounded durable batches, then measures the freeze from `LocalCheckpoint` through matching `Persisted` and compaction. Application-artifact preparation and verification are excluded. On the Intel Core i9-9980HK/macOS 15.8.1/AppleClang 17 Release setup, single samples on October 2, 2026 were:

| Retained entries | Payload bytes | Encoded bundle bytes | Capture wall s | Capture CPU s | Encode/write/publish wall s | CPU s | Full frozen wall s | Process peak RSS bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1,024 | 1,197 | 0.000006 | 0.000011 | 0.085114 | 0.002370 | 0.085141 | 946,176 |
| 65,535 | 67,107,840 | 68,484,227 | 0.064412 | 0.064330 | 0.570458 | 0.477923 | 0.635166 | 572,751,872 |

The largest fixture reserves one retained slot for its included no-op; its command suffix approaches the default 64 MiB payload bound. Process peak RSS comes from `getrusage`, includes fixture initialization and earlier cases, and is not incremental daemon memory. At baseline revision `96ea4dd`, suffix capture, owning bundle argument, validation state, and encoded buffer coexisted, so bounded payloads required a larger memory budget. These single samples do not establish tail latency, fleet capacity, energy, or cost. Run `build/consensus_checkpoint_bench` from the repository root; its dedicated scratch directories are removed after each case. Production integration must budget this pause or change the mechanism.

Const-reference semantic validation and encoding now avoid two unnecessary suffix copies, and encoding reserves its exact checked size. Use `encode_checkpoint(action.state, covered_sequence, application.descriptor())` when the action already owns the immutable suffix. The owning-bundle overload delegates to the same implementation. Repeating the identical fixture produced:

| Retained entries | Capture wall s | Capture CPU s | Encode/write/publish wall s | CPU s | Full frozen wall s | Process peak RSS bytes |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 0.000019 | 0.000024 | 0.083942 | 0.001707 | 0.083970 | 983,040 |
| 65,535 | 0.062530 | 0.062465 | 0.326839 | 0.240048 | 0.389764 | 286,265,344 |

For the large fixture, process peak RSS falls from approximately 546 MiB to 273 MiB, and the measured full freeze from 635 ms to 390 ms. The encoded bundle remains exactly 68,484,227 bytes; validation and durable ordering remain intact. The same single-sample and process-lifetime limitations apply, and 390 ms still needs an explicit production timing budget.

## Worker checkpoint measurement

`consensus_worker_checkpoint_bench` measures the complete owned lifecycle with a 1 MiB immutable application image and a retained command suffix at the default Core limits. Setup seeds real durable journal batches of at most 256 entries and recovers them through Worker. This exercises a maximum legal recovered log, not ordinary proposal throughput. Run the Release executable from the repository root; its dedicated scratch directories are removed after each case.

Single samples on the same Intel/macOS/AppleClang Release setup on October 3, 2026 were:

| Suffix commands | Bundle bytes | Whole checkpoint wall s | CPU s | Core freeze wall s | CPU s | Longest frozen turn wall s | CPU s | Process peak RSS bytes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 64 | 67,032 | 0.154070 | 0.008895 | 0.104915 | 0.002567 | 0.062957 | 0.001341 | 2,670,592 |
| 4,096 | 4,280,472 | 0.181037 | 0.031619 | 0.133159 | 0.026329 | 0.064627 | 0.004445 | 24,391,680 |
| 65,535 | 68,484,227 | 0.587517 | 0.371868 | 0.519703 | 0.367490 | 0.082745 | 0.079962 | 291,508,224 |

Whole-checkpoint time includes streamed application preparation. Core freeze starts immediately before the capture turn and ends after matching completion and compaction; ordinary admission pauses earlier when application sealing records cutover debt. The longest frozen turn exposes whole-suffix capture/encoding and durability-call pauses despite chunked bundle writes. RSS is process-lifetime peak, including fixture construction and earlier cases. These samples establish neither tails nor production capacity, and do not support energy or dollar savings claims. They are not a paired comparison with the earlier adapter benchmark.

## Snapshot transfer

Run `.scratch/build/cluster_worker_test --transfer-benchmark` from the repository root for 1 MiB and 64 MiB transfers. It writes `.scratch/snapshot-transfer-benchmark.csv`, measuring wall/CPU time, wire bytes, occupied frames, allocated payload-buffer capacity, process high-water RSS and sender/receiver payload-read calls. Setup is excluded from timing; RSS includes fixture/application copies, assertions and previous workloads. These individual local samples establish no production tails, fleet capacity, hosting cost or power savings.
Final component qualification passes all eight root CTests and both ASan/UBSan consensus/Worker CTests: 36,171 Core checks and 92,227 Worker checks in each profile. The focused correction suite passes 1,295 checks. A pre-correction reproduction fails eight assertions for duplicate Accepted, term-zero outcomes, held-source reclamation and unbounded concatenated-frame dispatch. The additional teardown regression reaches actual published activation, proves cancellation is TooLate, then verifies matching activation failure fences and drains the adapter without output. Warning and diff checks are clean.
Single local Release samples on the Intel Mac, with duplicate-Begin injection disabled:
| Image | Wall seconds | CPU seconds | Wire bytes | Occupied frames | Allocated payload buffers | Process peak RSS bytes | Source / receiver payload reads |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 MiB | 1.30680 | 0.053057 | 1,054,344 | 131,224 | 459,519 | 8,581,120 | 32 / 16 |
| 64 MiB | 2.41281 | 1.017820 | 67,421,064 | 131,224 | 459,519 | 412,426,240 | 2,048 / 1,024 |
Source reads include full pre-transmit verification and streaming reread. Timed work includes election, checkpoint capture, transfer and suffix application. Process RSS includes application/fixture copies; it is not adapter memory. The generated CSV and benchmark entry point make these samples reproducible. Authenticated sockets, authority integration and fleet qualification remain pending.

## Qualification log

### Qualification

Native AppleClang 17 and Clang 23 AddressSanitizer/UndefinedBehaviorSanitizer Release tests pass the recorded schedules. The deterministic host tests actual persistence/application completions and semantic operation replay. A POSIX integration test writes through the journal, reopens it, and reconstructs committed consensus state. Focused cases cover barriers, wrong completion tokens, affirmative majorities, inherited-prefix commitment through a no-op, simultaneous disk/application work, isolated-leader reads, overlapping reads, blocked application, partial quorum, capacity backoff, stale RPCs, failed storage transitions, semantic corruption, control reserve exhaustion, and maximum terms.

### Owned sources and bounded snapshot channels

Final component qualification passes all eight root CTests and both ASan/UBSan consensus/Worker CTests: 36,171 Core checks and 92,227 Worker checks in each profile. The focused correction suite passes 1,295 checks. A pre-correction reproduction fails eight assertions for duplicate Accepted, term-zero outcomes, held-source reclamation and unbounded concatenated-frame dispatch. The additional teardown regression reaches actual published activation, proves cancellation is TooLate, then verifies matching activation failure fences and drains the adapter without output. Warning and diff checks are clean.

### Application runtime observations

The full completion profile passes the existing ten native suites (68.85 seconds), an expanded 2,420-check focused Worker suite, and the native Asio Worker fixture (1.34 seconds). The latter holds an accepted checkpoint primitive while UDP and timers progress, then requires exact durable publication. Four Journal/Worker/completion ASan/UBSan suites pass (55.58 seconds), and all 30 Detent suites pass (37.04 seconds). The focused suite includes pending create/write/seal/header/payload cancellation, control-output pressure, disconnect and duplicate-frame handling, funded append priority, and three-Worker transfers with session loss and lost results. These gates qualify Worker dispatch; production service activation and authenticated multi-process acceptance remain outstanding.

Worker's private staging Chunk uses an explicit zero-initializing constructor to avoid early nested-type constructibility evaluation in Clang 18 with libstdc++ 13. A standalone reproduction passes with GCC 13 and fails with Clang 18 before the correction; both compile afterward. Fields, layout, staged-byte initialization and public APIs are unchanged. The macOS Worker regression suite also passes (50.58 seconds).
