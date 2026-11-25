# The consensus Driver

`include/consensus/driver.h`: one `Driver` per reactor loop runs every consensus group placed on
that loop as state, stepping a group only when it has work: an input, a completed IO, or a timer
on the loop's wheel. Callers await `task::task` calls; what no caller awaits goes to a Sink the
host supplies. `consensus::run` runs the same Driver to completion on the calling thread. Detent
`docs/design/async-drivers.md` D1-D8 is the design; this file is the contract (campaign contract
`consensus:driver`), and the Driver stands beside the `Worker` pump API until
worker-pump-removal deletes that.

## Contract

```cpp
namespace kronuz::consensus {

using GroupId = std::uint64_t;                  // the journal host's opaque group id

struct Placement { Term term = 0; Index index = 0; };   // where Core placed an entry

// Errors a call completes with, beside task::cancelled.
struct closed : std::runtime_error;             // the group is closing or closed, or not open
struct fenced : std::runtime_error;             // the group's Core or storage fenced; what() is the reason
struct rejected : std::runtime_error { RejectReason reason; };  // Core refused the call (Reject)
struct superseded : std::runtime_error;         // committed(): another term's entry committed there
struct compacted : std::runtime_error;          // committed(): the index is below the retained log, its term unknown
struct transfer_failed : std::runtime_error;    // transfer_leadership(): deadline passed, or another node leads
struct needs_peer : std::runtime_error;         // consensus::run only: the call needs another voting member
struct close_in_doubt : std::runtime_error;     // create(), recover(): the host holds the id in doubt after a failed close, until it reopens
struct recover_refused : fenced { LogBoundary base; Index last; Membership membership; };  // recover() with accept: the log retains none (Recover gate)
using stalled = kronuz::journal::stalled;       // consensus::run only: a lost wake

struct GroupOptions {
	FixedConfiguration configuration;
	std::optional<Membership> founding{};       // create() founds the group with these roles (Worker::found)
	kronuz::journal::AdmissionLimits admission;
	Limits limits{};
	Timing timing{};
	WorkerLimits scheduling{};                  // scheduling.flow: the replication Flow of every Core the group builds
	std::optional<SnapshotLimits> snapshots{};
	std::size_t inbox = 256;                    // received messages queued per group; more are dropped and counted
};

// Replication flow (D15). `WorkerLimits::flow`, so `GroupOptions::scheduling.flow` on a Driver
// and the `scheduling` argument of a Worker, is the `Flow` of every Core the Worker builds (create,
// recover and its validation Core). Its default, `Flow{}`, is one append in flight of up to
// min(rpc_bytes, the leader's pacing); `Flow{4, 4 << 20, 256 << 10}` keeps up to four appends in
// flight to each verified follower, at most 4 MiB unacknowledged and at most 256 KiB per append.
// `Flow::pipeline` and `window_bytes` must be above 0, or the Worker refuses the group with
// std::invalid_argument. The leader's per-follower append pacing (docs/mechanisms/append-pacing.md)
// applies within the Flow. At 10 ms round trip, one 256 KiB flight moves 16 MB/s; four move 64.

// The loop's seams. The host's journal (journal/host.h) holds every group's Store and the
// loop's turn driver, host.driver(loop), which the host runs (TurnDriver::run) on the same
// scheduler, or consensus::run commits inline. `seed` seeds the election jitter; 0 draws one.
// A group's Worker refuses, with std::invalid_argument, a configuration whose largest record
// does not fit one empty segment of the host's turn log: a batch of Limits::rpc_entries and
// rpc_bytes (WorkerLimits::follower_batch_* too, when larger), or the founding record. Size
// the host's TurnLogOptions::segment_bytes above the batch: the default 2 MiB rpc batch needs
// segments big enough for it, or explicit lower Limits. A merged Receive a caller submits
// beyond those limits (up to the retained policy's batch bounds) can still exceed the room,
// and then fences its group.
struct Seams {
	task::scheduler_ref scheduler;
	kronuz::journal::completion_ref io;
	kronuz::journal::Host& host;
	std::uint32_t loop = 0;
	std::uint64_t seed = 0;
};

// What the Driver hands the host: `sink(group, notice)` for each notice type, with static
// dispatch; a generic `template <class N> void operator()(GroupId, N&&)` satisfies it.
//   Send               a message for a peer incarnation (the transport's)
//   Committed          the next committed range for the application; Core holds it until applied().
//                      Never for a witness's range (from its promotion's range on, its removal's
//                      included): it keeps the log, no application state, so the Driver
//                      acknowledges that range itself
//   RoleChanged        role, term and leader changes
//   SendTransfer       a snapshot transfer frame (RFTXFR01) for a peer incarnation (the transport's)
//   ValidateImage      a received image, durable and CRC-checked: answer validated() or validation_failed()
//   ActivateImage      an installed image to activate; answer with activated() or activation_failed()
//   CheckpointPublished, RepairCompleted   only when no caller awaits it (the caller was cancelled)
//   SnapshotNeeded, SnapshotTransmit, SnapshotReleased   a leader's sender flights
//   Fenced             the group fenced; every waiter completes with `fenced`
template <class S> concept sink = /* invocable as above for each notice */;

// The install notices (Install, below). `image` is borrowed for the Sink call only.
struct SendTransfer { NodeId peer; std::string frame; };
struct ValidateImage { Token token; LogBoundary boundary; NodeId source; kronuz::journal::ArtifactReader& image; };
struct ActivateImage { Token token; LogBoundary boundary; kronuz::journal::ArtifactReader& image; };

template <class C> concept capture = requires(C& capture, std::span<char> chunk) {
	{ capture.metadata() } -> std::convertible_to<CaptureMetadata>;   // through, term, epoch (request ignored)
	{ capture.read(chunk) } -> std::convertible_to<std::size_t>;      // the next bytes, at most 64 KiB; 0 at the end
};

template <sink Sink> class Driver {
public:
	Driver(Seams seams, Sink& sink);
	~Driver();                                  // after run() returned and every group closed or never opened

	// The one long-lived frame: steps ready groups, a bounded number of steps per resume, then
	// yields. When `stop` is requested it shuts the loop down (D8 step 1): every waiter completes
	// with task::cancelled (cancel_reason::shutdown), new calls complete the same way, and run()
	// returns once no IO the groups issued is in flight. Refused while consensus::run holds it.
	task::task<void> run(task::cancel_token stop = {});

	// Plain calls, on the loop's thread (Debug-asserted with runs_here()); from another thread,
	// post_call them. Each only queues and marks the group ready: none steps inline, so a Sink
	// may call them. On an id that is not open, closed ones included, they are dropped and counted (dropped()).
	void open(GroupId, GroupOptions);           // state only; throws if the id is open on this Driver
	void receive(GroupId, Receive);
	void applied(GroupId, Applied);
	void application_failed(GroupId, Index through, std::string reason);
	void transfer(GroupId, NodeId source, std::string frame);   // a received RFTXFR01 frame from `source`
	void validated(GroupId, Token token);       // ValidateImage::token
	void validation_failed(GroupId, Token token);
	void activated(GroupId, Token token);       // ActivateImage::token
	void activation_failed(GroupId, Token token, std::string reason);
	void peer_limits(GroupId, PeerLimits);
	std::uint64_t dropped(GroupId) const;       // an open group's inbox overflow; for an id not open (closed, never opened), the Driver-wide count of plain calls dropped on ids not open

	// Task calls, from any scheduler: a call made off the loop hops there (one post) and resumes
	// the caller on its own scheduler. Each completes per its D8 row (below).
	task::task<void> create(GroupId, kronuz::journal::Identity, task::cancel_token = {});
	template <class Restore> task::task<void> recover(GroupId, Restore, task::cancel_token = {});
	// recover, then reopen with the RFTCLOS1 bytes closed_state() gave at close: a matching record
	// resumes its term with no election (docs/mechanisms/quiescence.md); damaged or another
	// replica's bytes complete it with kronuz::journal::Corruption before any recovery IO.
	template <class Restore> task::task<void> recover(GroupId, Restore, std::optional<std::string> closed, task::cancel_token = {});
	// Gated (Recover gate, below): before Start, a log that retains none of `accept` fails the
	// call with recover_refused. An empty `accept` is no gate, as the overloads above.
	template <class Restore> task::task<void> recover(GroupId, Restore, std::optional<std::string> closed, std::vector<LogBoundary> accept, task::cancel_token = {});
	task::task<Placement> propose(GroupId, std::string command, task::cancel_token = {});
	task::task<void> committed(GroupId, Placement, task::cancel_token = {});
	task::task<Placement> change_membership(GroupId, Membership next, task::cancel_token = {});
	task::task<void> transfer_leadership(GroupId, NodeId target, task::cancel_token = {});
	task::task<RepairCompleted> repair(GroupId, LogBoundary, task::cancel_token = {});
	// A follower whose application state is lost asks its leader for its image
	// (docs/mechanisms/install-request.md); completes as repair does, at the image's boundary.
	task::task<RepairCompleted> reinstall(GroupId, task::cancel_token = {});
	template <capture Capture> task::task<CheckpointPublished> checkpoint(GroupId, std::uint64_t application_cap, Capture, task::cancel_token = {});
	task::task<ReadReady> read(GroupId, task::cancel_token = {});
	task::task<void> close(GroupId);            // retirement until drained; no caller token

	// On the loop: an open group's role, term, leader, indexes (last: its log end) and membership in force.
	struct GroupState { Role role; Term term; NodeId leader; Index committed, applied, last; Membership membership; bool resting = false; };
	std::optional<GroupState> state(GroupId) const;
	// The closed state (RFTCLOS1) when the group can close now: started, no call or frame
	// waiting, at rest at a checkpoint covering applied; bound to the host table's incarnation.
	std::optional<std::string> closed_state(GroupId) const;
	std::size_t armed_group_timers() const;     // groups whose wheel entry is armed
	std::optional<Index> witness_bound(GroupId) const;  // Core::witness_bound: a witness truncates through it (D9); absent for a group not open
	std::vector<std::uint64_t> waiting_groups() const;  // groups holding a waiter, in order
	std::size_t in_flight() const;              // IO tasks and turn watches: a group's only frames
};

// The blocking entry (D5): runs `work` and the Driver on the calling thread over `scheduler`,
// committing the loop's turns inline, and returns once `work` finished and nothing is staged.
// The Driver must have been built over this scheduler and an InlineCompletion.
template <class Sink, class T>
T run(Driver<Sink>& driver, task::blocking_scheduler& scheduler, task::task<T> work);

}  // namespace kronuz::consensus
```

## What completes when

| Call | Completes | On its token | When its group closes |
|---|---|---|---|
| any call not yet admitted to Core | - | unlinked, never submitted: `task::cancelled` | `closed` |
| `create`, `recover` | the group is ready (initialization durable, admission counted) and `Start` delivered | `task::cancelled`; the group finishes its current durable step and stays open | `closed` once that step ends |
| `propose`, `change_membership` | `ProposalPlaced`: the entry is durable here, not committed; `rejected` on Core's `Reject` | `task::cancelled`; an accepted entry is still written and may commit (unknown outcome) | admitted: completes when its record is written, with its `Placement`; else `closed` |
| `committed` | the commit index reaches the placement with its term; `superseded` if another term's entry committed there; `compacted` below the retained log | `task::cancelled`; the entry is unaffected | `closed` |
| `read` | the read index is confirmed and the application has applied through it (`applied`) | `task::cancelled`; the slot is dropped when the index arrives | `closed` |
| `transfer_leadership` | the target leads (`RoleChanged`); `transfer_failed` when Core's transfer deadline passes or another node leads | `task::cancelled`; Core carries on | `closed` |
| `repair` | `RepairCompleted` | `task::cancelled`; Core carries on, the Sink gets `RepairCompleted` | `closed` |
| `reinstall` | `RepairCompleted` once the leader's image is activated; `rejected` with `Current`, `NotLeader`, `Busy`, `LeaderMustTransfer` or `InvalidRepair` (docs/mechanisms/install-request.md) | `task::cancelled`; Core keeps the request until its image activates or it fails, and the Sink gets `RepairCompleted` | `closed` |
| `checkpoint` | `CheckpointPublished` | before publication starts it is cancelled and its reservation reclaimed; after, publication finishes and the Sink gets `CheckpointPublished`; the caller gets `task::cancelled` either way | started publication: completes when published; else `closed` |
| `close` | the group's state is freed and its row erased: `groups()` no longer counts it, and the id opens again on this Driver; a late plain call is dropped and counted with the ids not open. Close completes on an open, fenced or never-created row, and on an id the Driver never held. The drained Worker hands its group back to the host: a row whose last record is a checkpoint covering every batch closes into the host's table, which then keeps nothing of it; any other row (fenced, or with records after its checkpoint) is released, its records durable in the log and its in-memory state evicted. Either way the id recovers again in the same live host. A close whose table commit fails fences the row, which is then released; the host keeps the group in doubt (journal's `Host::hold` refuses it with EIO), so `create` and `recover` of that id on any Driver over that host complete with `close_in_doubt` until the host reopens and recovers it from its disk | no token | - |

Every call completes with `fenced` once its group fences, and with `task::cancelled`
(`cancel_reason::shutdown`) once the loop shuts down; a staged record is still written. A throw that ends `run()` completes them the same way, but a staged record may or may not be written: the outcome is unknown, and a retry may duplicate it. A call on
an id that is not open, or is closing or closed, completes with `closed` and submits nothing.

**Shutdown order.** A host closes its groups while `run()` steps the loop, then requests `stop`.
After `stop`, `close` completes with `task::cancelled` like every other call and frees nothing;
`~Driver` frees what the groups still hold. `run()` does not wait for a turn barrier after `stop`:
the turn driver writes what is staged while it runs, and a record a step stages after it returns
stays staged (`TurnDriver::staged`) until a turn driver runs again.

**The Sink must not throw.** A throw out of a Sink call fences the group: the rest of that step's
actions are lost, so its waiters complete with `fenced` and the Sink hears `Fenced`.

**A witness stays a witness.** A witness member's ranges are acknowledged, never applied, so its
incarnation never becomes a learner or voter: `change_membership` rejects that
(`InvalidMembership`). The witness leaves by removal, and its node rejoins as a data member under
a new incarnation, catching up by snapshot.

**Waiters.** A caller awaiting a group holds its waiter in its own frame: a claim word, the result
slot, its home scheduler, a `cancel_callback` and an intrusive link in the group's list. The loop
links it. Completion, on the loop, unlinks the waiter and then claims it, and posts the caller's
resume if the claim wins. The token's callback, on any thread, claims it and posts a hop to the
loop (`post_call`), which unlinks it if still linked and then posts the resume. Only the claim's
winner resumes the caller, after the group's last touch of the waiter; an idle group holds none.
A waiter destroyed unresumed (task teardown, phase 3) unlinks itself.

**Requests.** The Driver assigns every Core request id; callers never name one. A `Reject`,
`ProposalPlaced`, `ReadReady` or `RepairCompleted` whose caller is gone is dropped, except
`RepairCompleted`, which goes to the Sink.

**Time.** A group's wheel entry is armed at `Core::next_deadline()` after every step and cancelled
when it has none (a learner, a fenced group, or a leader with no peers to heartbeat): an idle group holds no frame, thread, descriptor
or armed timer of its own beyond that entry. Core sees `Tick{now / 1 ms, jitter}`, `now` being the
scheduler's clock and the jitter drawn per tick in `[election_min, election_max]`.

**A lone voter** (the only voting member, no witness) is due to campaign at `Start`: it campaigns
at the Tick the Driver delivers right after `Start`, with no clock advance, so its first proposal
after `create` or `recover` succeeds at once.

**The Sink** runs inside a step and applies nothing inline: it hands a `Committed` range to the
application, which calls `applied` later. Core holds one range until then, and `close` does not
wait for it.

**The apply index never passes what the application was delivered.** `applied(through)` must name
the end of the range the Sink delivered last; Core fences any other (`invalid application
completion`). Otherwise the apply index moves only to the boundary of a checkpoint the application
restored (`recover`), an image it activated (`ActivateImage`) or a `repair` it asked for.

The one exception is a witness's range, which the Driver acknowledges itself instead of handing it
to the Sink. The witness transitions, both ways:

1. Learner to witness. A range is the witness's from the first one whose last index is in a
   membership that makes this node a witness, a committed Configuration entry that no later term
   can truncate. A range ending below the promotion goes to the Sink, and so does one ending while
   the promotion is appended but not committed, which a later term may truncate.
2. Witness to no member. The incarnation never becomes a data member again ("A witness stays a
   witness"), so every range after its first is its own too: its removal's and any past it. A
   range whose last index is past the removal is also the witness's when this node was a witness
   at the index before it (the apply index), as after a restart from the witness's checkpoint.

From its first acknowledged range on, a witness's apply index follows its commit index, and its
application receives no further range. A restart from a checkpoint taken after the removal finds
this node a witness at neither index, so its later ranges go to the Sink, which restored that
checkpoint and continues from its boundary.

## Recover gate

`recover(id, restore, closed, accept)` takes the log boundaries the host accepts (the contract
`consensus:recover-gate`; strata passes its index's head, and a complete install stage's bundle).
Once the log is recovered and `closed` applied, and before any turn runs, the Driver checks each
boundary with `Core::retains_suffix`: the boundary is the log's base, or an entry the log retains
with that term. When none is retained, the Worker fences and the call fails with
`recover_refused`, whose `what()` is

```text
consensus: the recovered log retains no accepted boundary: base (B, term T), last L
```

and whose fields are the recovered base, last index and membership. Nothing that changes
consensus state ran: no election, vote, append, checkpoint or term record. The typed
initialization of a bootstrap-only group and the pre-Start reclaim of a store above its headroom
run in turns, so they never run either. The row stays open and fenced: later calls fail with
`fenced` and the same reason, the Sink hears no `Fenced` and the peers no `Gone`, since the call
carries the refusal. `close` erases the row and writes no close record, so the next `recover`
finds the same log. The one write before the gate is the journal's own: a group closed into the
host's table reopens its entry at recovery (the journal's bytes change, its recovered state does
not). An empty `accept` is no gate (a damaged replica's, a witness's); `Worker::recover` has none.

`driver_test recover_gate_lone`, `recover_gate_closed`, `recover_gate_follower` and
`recover_gate_accepts` check it by value; `test/driver_mutants.py gate-after-start` runs the gate
after Start, which they catch.

## Install

A group opened with `GroupOptions::snapshots` installs snapshots: a leader sends its latest
published checkpoint to a member its log no longer reaches, and the member installs it as its
own. The Driver owns the group's `SnapshotChannel` (created at the first transfer, freed at
close); the host carries its frames, and the application validates and activates the image.

```
 leader's Driver                          host transport (Bulk, data)        follower's Driver
 Core: SnapshotNeeded ──▶ Sink            (strata pins its checkpoint)
 source reads checkpoint ── SendTransfer ──▶ send_transfer ═══▶ is_transfer ──▶ transfer()
   Begin, Chunk x n (64 KiB, window)                                           chunks staged and synced
 ◀── transfer() ◀══ Accepted, Credit ════════════════════════ SendTransfer ◀── read back, CRC32C-checked
                                                                               Sink: ValidateImage
                                                                               ◀── validated(token)
                                                                               consensus persists the install
                                                                               Sink: ActivateImage
                                                                               ◀── activated(token)
 Core: SnapshotReleased ◀══ Result (Installed) ════════════════ SendTransfer ◀── entries past the boundary
```

**The image** is the application part of the leader's latest published checkpoint (the bytes its
`checkpoint` capture wrote, at most `SnapshotLimits::application_bytes`), with the checkpoint's
boundary. A leader with no published checkpoint at its log's base answers `SnapshotNeeded` with
a failed flight and asks again later: install needs a checkpoint at the base.

**Frames.** Transfer frames are RFTXFR01 (`snapshot_transfer.h`): Begin (the image's
descriptor), Accepted, Chunk (offset, up to 64 KiB, final), Credit (cumulative, up to the
chunk it names), Result (installed, caught up, rejected or failed, with the receiver's term) and
Cancel. A sender keeps `SnapshotLimits::window` chunks in flight. The Sink's `SendTransfer` is
one whole frame for the peer incarnation `peer`; the host sends it with
`transport::send_transfer(transport, loop, group, local, node, transfer.peer, transfer.frame)`,
a data frame on the Bulk connection. A received message whose payload `transport::is_transfer`
names goes to `transfer(group, received.route.source, std::move(received.payload))`, and its
`Deliver` returns true; every other one goes to `receive`. A frame is bound to its group's
cluster and configuration identities and to its source and destination incarnations (the
per-frame rule admitted the source); a frame that does not decode, or does not follow its
transfer's sequence, ends that peer's transfer here. Received frames queue per group
(`GroupOptions::inbox` of them; more are dropped and counted). Nothing retransmits a frame: a
transfer that lost one (a refused or dropped frame, a connection that ended) stalls until the
leader's `Timing::snapshot_timeout`, and the leader then starts it over. The follower ends its
side after its own `snapshot_timeout` without a frame, a resting group included (it wakes for
that deadline). Once the final chunk arrives, the image waits for `validated` or
`validation_failed` however long it takes. Until the image ends, the follower's `repair`,
`reinstall` and `checkpoint` calls wait behind it.

**Validation.** The follower writes the chunks to a replacement artifact, syncs it, and reads it
back through its CRC32C. Then the Sink gets `ValidateImage` once per transfer: `token`, the
image's `boundary`, the `source` incarnation that sent it, and `image`, a reader of the verified
bytes (`image.read_at(offset, span)`, up to 64 KiB a call; `image.descriptor().length` bytes),
valid only during the call. Nothing is installed yet: consensus has not persisted anything of
it. The application checks the image and prepares what it needs to activate it (strata stages
its pinned files here), on its own time, then answers on the loop:

- `validated(group, token)`: consensus installs it (its bundle published, its boundary its new
  base), and the Sink then gets `ActivateImage`.
- `validation_failed(group, token)`: the replacement is discarded and the follower keeps its
  state; the leader hears a failed transfer and tries again later.

A transfer must finish, validation included, within the leader's `Timing::snapshot_timeout` of
its start, or the leader cancels it (its `SnapshotReleased`, reason timed out); an answer for a
transfer that ended, or a token that is not the current one, is dropped and counted. An
application whose preparation is long keeps what it prepared and resumes it on the next try.

**Activation.** Once the install is durable (consensus's checkpoint is now the image, the log
below its boundary gone), the Sink gets `ActivateImage`: `token`, `boundary` and `image`, the
same bytes, valid only during the call. The application makes the image its state (strata
activates its stage) and answers `activated(group, token)`; the apply index moves to the
boundary and `Committed` ranges continue past it. `activation_failed(group, token, reason)`
fences the group. A crash after the install is durable and before `activated` recovers from it:
`recover`'s Restore reads the same image, so activation is what a restore does, and a host that
staged something at validation finishes it at recovery when the restored boundary is the
staged one. A crash before it recovers the old replica: consensus persisted nothing of the image.

**Leader side.** `SnapshotNeeded`, `SnapshotTransmit` and `SnapshotReleased` reach the Sink for
each flight (an application pins what a follower's validation will fetch at `SnapshotNeeded` and
releases it at `SnapshotReleased`); the Driver reads the checkpoint and frames it itself.

| Call | Notice it answers | Effect | On a stale token, a closed or fenced group |
|---|---|---|---|
| `transfer` | - | the frame joins the group's transfer queue | dropped and counted |
| `validated` | `ValidateImage` | install proceeds | dropped and counted |
| `validation_failed` | `ValidateImage` | install discarded, old state kept | dropped and counted |
| `activated` | `ActivateImage` | apply index at the boundary | dropped and counted |
| `activation_failed` | `ActivateImage` | the group fences | dropped and counted |

## The blocking entry

`consensus::run(driver, scheduler, work)` holds the loop's turn driver (journal's `Hold`, so a
`TurnDriver::run` holding it is refused) and the Driver (its `run()` is refused meanwhile), and
loops:

1. Run the scheduler until its queue is empty.
2. If records are staged in the loop's turn, commit them inline and go to 1.
3. If `work` finished, return its result or rethrow its exception; a throw that ended the Driver's
   loop is rethrown first, and the Driver then stays stopped.
4. Otherwise, if an operation deadline (a `task::deadline` on the scheduler) is armed, sleep
   toward it or until another thread posts (a cancel's hop), and go to 1. Group timers never
   count, though they fire as the clock passes them.
5. Otherwise throw `stalled`, naming the groups that hold a waiter.

`propose`, `committed`, `change_membership`, `transfer_leadership` and `read` on a group whose
membership has a voting member (voter or witness) other than this node throw `needs_peer` and
submit nothing, and so does `reinstall`, which asks the leader; `create`, `recover`,
`checkpoint`, `repair` and `close` need no peer.

## From the Worker's `try_submit`

| `try_submit(...)` today | Driver |
|---|---|
| `Start{}` | inside `create` and `recover` |
| `Tick{...}` | the group's wheel entry |
| `Propose{request, command}` | `propose(group, command)`, then `committed(group, placement)` |
| `Read{request}` | `read(group)` |
| `Receive{...}` | `receive(group, receive)` |
| `ChangeMembership{request, next}` | `change_membership(group, next)` |
| `TransferLeadership{request, target}` | `transfer_leadership(group, target)` |
| `Repair{request, boundary}` | `repair(group, boundary)` |
| `RequestInstall{request}` | `reinstall(group)` |
| `PeerLimits{...}` | `peer_limits(group, limits)` |
| `applied`, `application_failed` | the same plain calls |
| `reserve_checkpoint`, `attach_capture`, `offer_application_chunk` | `checkpoint(group, cap, capture)` |
| `SnapshotChannel` over `Peers`: `register_session`, `receive`, `outbound`, `run_one` | the Driver's own channel: `transfer(group, source, frame)` in, the Sink's `SendTransfer` out |
| `validation_chunk`, `consume_validation`, `validation_succeeded`, `reject_snapshot_validation` | the Sink's `ValidateImage`, then `validated` or `validation_failed` |
| `snapshot_activated`, `snapshot_activation_failed` | `activated`, `activation_failed` after the Sink's `ActivateImage` |
