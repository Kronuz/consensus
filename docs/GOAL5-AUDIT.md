# Goal 5 audit: threads, deferred reclamation and retirement

Strata's Goal 5 (strata `docs/PLAN.md`, strata:491a1bdc) forbids, in strata and every library it
uses, a tracing collector, a collector thread, and deferred reclamation of unbounded work: every
object has an owner and is freed deterministically, and reclamation runs in bounded slices. This
audit lists every thread creation, deferred reclamation and retirement path in the five libraries
below, each with the bound that keeps it finite, cited as `library:path:line`.

| Library | Audited commit |
|---|---|
| completion | completion:d8b538c6778f |
| journal | journal:ce05f0efd411 |
| consensus | consensus:b7f48b2414d8 |
| task | task:95798a02f68c |
| wheel | wheel:238b1ae9f58b |

Scope is library code: `include/` and the top-level headers. Tests, benches, fuzzers, examples and
tools are out of scope; they create threads to drive the library, and nothing ships them. No
library has a tracing collector or a collector thread. Two paths have no bound (Findings); five rows
change in open Detent quests and are marked so rather than filed.

The check at the end greps the five commits for thread creation and deferred-destruction sites and
fails when a hit has no row here (A2).

## completion

| Row | Site | What | Bound |
|---|---|---|---|
| CP1 | `completion:include/completion/linux_completion.h:72` | Fallback IO worker thread, one per `LinuxCompletionQueue`, started even when the io_uring ring is native | Joined by the destructor (linux_completion.h:85-87); one operation in flight per queue (`submit` refuses a second, :102-103); destroying a queue with an operation in flight terminates (:83-84), so no work outlives it |
| CP2 | `completion:include/completion/bsd_completion.h:60` | Fallback IO worker thread, one per `BsdCompletionQueue` | Joined by the destructor (bsd_completion.h:73-75); one operation in flight (:93-94); terminates when destroyed with one pending (:70-71) |
| CP3 | `completion:include/completion/linux_completion.h:67` `completion:include/completion/linux_completion.h:74` `completion:include/completion/linux_completion.h:88` | `retire_ring()` unmaps the ring and closes its descriptor | Inline and constant: three `munmap` and one `close` (:359-370), after the worker joined (:87) |

Threads per process equal the queues the embedder builds (`make_native_queue`,
native_completion.h:40); no library here builds one. Reuse of a reclaim candidate file
(posix.h:999-1014) opens one named file per call and frees nothing.

## journal

| Row | Site | What | Bound |
|---|---|---|---|
| JN1 | `journal:include/journal/turn_driver.h:145` | `forget()` records a retired-sequence floor per closed group in `retired_` (:653) | **None: finding G5-J1.** Nothing erases `retired_` (its only uses are :145, :221, :653); it grows by one map node per distinct group id closed while the Host stays open |
| JN2 | `journal:include/journal/turn_log.h:312` | `completed()` calls `reclaim()` (:371-384): every oldest segment with no live record is recycled into the spare pool or removed, then one directory sync | The data segments: forwarding keeps them near `retain_segments` (4, :51) by moving at most `forward_bytes` (1 MiB, :52) of live records per turn (:348-363). No fixed per-turn count (Observation O2) |
| JN3 | `journal:include/journal/journal.h:216` `journal:include/journal/journal.h:218` | `reclaim_step` and `begin_reclaim`: one reclamation slice over the directory | `start_reclaim` refuses a budget outside 1..4096 (journal.h:512); one reclamation per owner at a time (`mutation_active`, reclamation.h:197-206) |
| JN4 | `journal:include/journal/reclamation.h:226` | The slice stops scanning | At `scanned >= budget_`: removals per slice are at most the entries scanned |
| JN5 | `journal:include/journal/store.h:541` `journal:include/journal/store.h:551` `journal:include/journal/store.h:588` `journal:include/journal/store.h:644` | Store's synchronous, completion and task reclamation slices | Same budget, checked again at store.h:539 and :587; `begin_reclaim` defaults to 1 (:551); admission is credited only when the slice finishes (:280-290) |
| JN6 | `journal:include/journal/blocking.h:94` | The blocking entry's root frame | Destroyed by its RAII owner when `run` returns |
| JN7 | `journal:include/journal/host.h:304` `journal:include/journal/host.h:314` `journal:include/journal/host.h:533` | `opened_` keeps every group a Store created or recovered this open; close does not erase it | Changing in detent:journal-reopen-closed (its A7 drops a released group's in-memory state; consumed by detent:consensus-worker-release) |
| JN8 | `journal:include/journal/host.h:285` | Expired group views in `ranges_` are swept on each `group_io` | Live views plus the ones closed since the last call (Observation O4) |
| JN9 | `journal:include/journal/store.h:112` | `dispose_known` settles an abandoned replacement's staged artifacts | At most two artifacts per replacement (:115-117) and one replacement per Store (:391); their files go by JN3's slices |

TurnDriver's other per-group state is erased by `forget`: `refused_` (turn_driver.h:148), sealers
(:152-155), the turn log's index and sequence (turn_log.h:217-224) and recovered records
(turn_driver.h:157). Re-partition removes the old generation's logs at open (host.h:442-447,
:516-520), bounded by one generation's files. Journal creates no thread.

## consensus

| Row | Site | What | Bound |
|---|---|---|---|
| CN1 | `consensus:include/consensus/transport/resolve.h:99` | `Lookup::start` runs each name lookup on a detached thread | **Not owned: finding G5-C1.** One lookup per peer name in flight per `Links`/`Peers` (CN2), but `abandon()` (:102-105) only stops delivery; the thread and its closure live until the resolver returns, and nothing joins or counts them |
| CN2 | `consensus:include/consensus/transport/links.h:396` `consensus:include/consensus/transport/peers.h:545` | Starters of CN1 | A new lookup only when none is in flight and the last deadline passed (links.h:390, peers.h:541); `route.lookup` resets only on an answer (links.h:423, peers.h:576) |
| CN3 | `consensus:include/consensus/driver.h:914` | `launch` spawns a root per storage operation the Worker issues | The Worker holds at most two operations, `io_job_` and `maintenance_io_` (worker.h:1141, :1143); `g.io` counts them and close waits for zero (driver.h:904) |
| CN4 | `consensus:include/consensus/driver.h:928` | `watch` spawns a root waiting for the group's next durable record | One per group: guarded by `g.watching` (driver.h:647, :927) |
| CN5 | `consensus:include/consensus/driver.h:993` | The blocking entry's loop and work roots | Destroyed by their RAII owner when `run` returns |
| CN6 | `consensus:include/consensus/driver.h:184` `consensus:include/consensus/driver.h:906` | Driver close drops the Worker but keeps the `Group` row until a reopen of the id erases it | Changing in detent:consensus-close-fenced |
| CN7 | `consensus:include/consensus/worker.h:179` | `Worker::close` sets `closing_` and never closes its Store, so journal keeps the row's group | Changing in detent:consensus-worker-release |
| CN8 | `consensus:include/consensus/transport/links.h:247` | One accept root per accepted socket | Unauthenticated handshakes are capped at `handshakes` (4) and `handshakes_per_source` (2) (links.h:24); `admit` aborts the oldest over the cap (:365-378) |
| CN9 | `consensus:include/consensus/transport/links.h:194` | An authenticated accept moves to its peer's loop as an `adopt` root | One per CN8 accept that authenticated; counted in `incoming_` (:192, :204) and `drained()` waits for it (:82) |
| CN10 | `consensus:include/consensus/transport/links.h:346` | One `run` root per established link | One reserved slot per (peer, lane) (:220), released at the link's end (:336-340) |
| CN11 | `consensus:include/consensus/transport/host_transport.h:192` | The accept loop root | One per `serve` call |
| CN12 | `consensus:include/consensus/transport/host_transport.h:344` | One dial root per (node, lane) | Guarded by `lane.dialing`, with a redial backoff (host_transport.h:341-350) |
| CN13 | `consensus:include/consensus/transport/peers.h:456` | `Peers` co_spawns its accept loop, its dials and one root per incoming connection | Accept loop once (:125); two lanes per higher peer (:126-129); incoming refused past `handshakes` pending or `2 x peers + handshakes` connections (:487-495) |
| CN14 | `consensus:include/consensus/core.h:96` `consensus:include/consensus/core.h:284` `consensus:include/consensus/worker.h:471` | Each step and each Worker turn retire the log's detached entries | 256 entries and `command_bytes` per call (core.h:96); the backlog counts against the log caps (`charged_entries`/`charged_bytes`, retained_log.h:163-164; checked at core.h:702, :708, :947) |
| CN15 | `consensus:include/consensus/retained_log.h:134` `consensus:include/consensus/retained_log.h:228` `consensus:include/consensus/retained_log.h:235` `consensus:include/consensus/retained_log.h:236` | Removing a range detaches its subtree onto the retired list; `retire` frees nodes under a budget | One budget unit and the payload's bytes per node (:232, retirement.h:32-44) |
| CN16 | `consensus:include/consensus/retained_log.h:155` | The destructor frees the whole retired list inline | The charged entries, at most the log's entry cap (CN14) |
| CN17 | `consensus:include/consensus/core.h:99` `consensus:include/consensus/core.h:130` `consensus:include/consensus/worker.h:625` | A closing Worker's retirement turns free Core's actions, captures, reads, peers, grants and log | 32 units and `command_bytes` per turn (worker.h:618), shared across every container (core.h:104-131); `drained()` gates close (worker.h:181-184) |

RPC calls retire their ids without remembering them (rpc.h:729-745; a late frame only counts,
:510). Snapshot chunks in flight stay within the window (snapshot_channel.h:163). Consensus
creates no other thread.

## task

| Row | Site | What | Bound |
|---|---|---|---|
| TK1 | `task:task.hh:644` `task:task.hh:596` `task:task.hh:616` `task:task.hh:620` `task:task.hh:692` | Per-thread frame free lists in 32 classes of 64 bytes | 1,024 frames per class (task.hh:588, :619-621), freed at thread exit (:591-598); frames over 2 KiB bypass the pool (:603, :615-617); `TASK_FRAME_POOL=0` frees each at once (:692). Worst case 33 MiB per thread (Observation O1) |
| TK2 | `task:task.hh:289` | Per-thread trampoline queue of resumes | Cleared at the end of each run (:304); its capacity is the longest chain run |
| TK3 | `task:task.hh:253` `task:task.hh:255` `task:task.hh:369` `task:task.hh:702` `task:task.hh:703` `task:task.hh:704` `task:task.hh:705` `task:task_asio.hh:299` | Per-thread scalars and test hooks | Constant: a pointer or integer each, owning nothing |
| TK4 | `task:task.hh:784` `task:task.hh:792` | A `task` destroys its frame when reassigned or destroyed | The owner's lifetime |
| TK5 | `task:task.hh:1946` `task:task.hh:1960` | A `when_all` child destroys itself at its end, or its owner destroys it | One per child the caller passed |
| TK6 | `task:task.hh:897` `task:task.hh:937` `task:task_asio.hh:71` `task:task_asio.hh:116` | Teardown destroys every spawned root still suspended; spawn on a closed list destroys the task at once | The live roots, each bounded by its spawner (CN3-CN13); phases in task_asio.hh:59-64 |
| TK7 | `task:task.hh:1261` `task:task.hh:1263` `task:task.hh:1266` | A cancel state frees itself on its last release (:1078-1079) | Its references: tokens, children, and, for a deadline, the wheel's one reference, released by the fire (:1214-1217) or by `drop_timer` (:1482-1486, :1496-1500) |
| TK8 | `task:task.hh:1697` | A lock waiter frees itself on its last release | Two references, the queue entry and the awaiter (:1692) |
| TK9 | `task:task_asio.hh:206` `task:task_asio.hh:209` | A deadline dropped off the loop thread posts its unfile, holding the state until the loop runs it | One post per dropped deadline, run at the loop's next pass; none once closed (:207), when the wheel's destructor fires the entry (WH1) |
| TK10 | `task:task.hh:483` `task:task_asio.hh:109` | Teardown unbinds the wheel's owner thread so another thread may destroy it | Constant; the wheel then frees as WH1 |

Task creates no thread; its schedulers run on threads their owners provide.

## wheel

| Row | Site | What | Bound |
|---|---|---|---|
| WH1 | `wheel:wheel.hh:91` | The destructor fires every armed or queued entry with `expired = false` | The armed entries: intrusive, 32 bytes inside their owners, so the wheel allocates nothing (wheel.hh:47-56) |
| WH2 | `wheel:wheel.hh:236` | Per-thread stripe index for remote arms | Constant: one `unsigned` |

The remote inbox (`arm_remote`, wheel.hh:134-145; `drain`, :257) is intrusive: each entry is
linked at most once and filed at the owner's next expire. Wheel creates no thread.

## Findings

Each is a quest for its library; none is fixed here.

- **G5-J1 (journal): closed groups' retired floors are never freed.** `TurnDriver::forget`
  (turn_driver.h:145) inserts into `retired_` (:653) and nothing erases it, so a Host open for
  the millions-of-indexes workload grows one `std::map` node (about 48 bytes) per distinct group
  id it closes. The floor must outlive the closed incarnation only until the group's next
  incarnation or its table entry carries it; detent:journal-reopen-closed's branch (`park`) does
  not touch it.
- **G5-C1 (consensus): lookup threads are detached and unowned.** `Lookup::start`
  (resolve.h:99) detaches one thread per lookup. Per `Links` or `Peers` instance at most one per
  peer name runs, but `abandon()` leaves a thread blocked in the resolver alive with its closure,
  and a transport rebuilt meanwhile starts new ones: the count is bounded only by the resolver's
  own timeout, and a custom `PeerResolver` may have none. Goal 5 wants an owner that joins them,
  such as a resolver thread per `HostTransport` with a bounded queue, joined at stop.

## Observations

Bounded, so not findings; listed for the lead.

- **O1 (task):** the frame pool's cap allows 1,024 x 64 x (1 + ... + 32) = 34,603,008 bytes, 33 MiB,
  cached per thread at worst (task.hh:588).
- **O2 (journal):** `TurnLog::reclaim` (turn_log.h:371) has no fixed per-turn segment count:
  after a mass `forget` one turn may remove every freed segment, each an unlink, then one sync.
- **O3 (completion):** each native queue starts its fallback thread even when io_uring serves
  every primitive (linux_completion.h:72): one idle thread per queue.
- **O4 (journal):** `group_io` sweeps all of `ranges_` per call (host.h:285): O(open groups) per
  open, quadratic over a burst of opens.

## Check (A2)

Run from the consensus checkout root; `K` is the directory holding the five repositories. It
prints each hit as matched or MISSING and fails when any hit has no row above.

```sh
# goal5-grep begin
set -u
K=${K:-$HOME/code/Kronuz}
DOC=${DOC:-docs/GOAL5-AUDIT.md}
P='std::thread *\(|std::jthread|pthread_create|std::async *\(|\.detach\(\)|thread_local|Lookup::start\(|task::spawn\(|co_spawn\(|\.destroy\(\);|destroy_all\(\);|destroy_roots\(\);|delete (this|self|static_cast)|::operator delete|retired_\[|push_retired\(std|retire\((256|budget|shared)|retire_log\(\)|begin_retirement\(\)|retirement_step\(budget|retire_ring\(\);|reclaim\(\);|start_reclaim\((scan_)?budget|scanned >= budget_|fire\(\*e, false\)|wheel_?(->|\.)release\(\)|self->unfile\('
hits=0 missing=0
for spec in completion:d8b538c6778f journal:ce05f0efd411 consensus:b7f48b2414d8 task:95798a02f68c wheel:238b1ae9f58b; do
  lib=${spec%%:*} rev=${spec#*:}
  while IFS= read -r line; do
    site="$lib:$(printf '%s' "$line" | cut -d: -f2-3)"
    hits=$((hits + 1))
    if grep -qF "\`$site\`" "$DOC"; then echo "matched  $site"; else echo "MISSING  $site"; missing=$((missing + 1)); fi
  done < <(git -C "$K/$lib" grep -nE "$P" "$rev" -- include '*.hh' ':!test' ':!bench' ':!fuzz' ':!examples' ':!tools')
done
echo "$hits sites, $missing without an audit row"
[ "$hits" -gt 0 ] && [ "$missing" -eq 0 ]
# goal5-grep end
```

Run it as `awk '/^# goal5-grep begin/{f=1} f; /^# goal5-grep end/{f=0}' docs/GOAL5-AUDIT.md | bash`.
