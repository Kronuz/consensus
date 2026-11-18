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
using stalled = kronuz::journal::stalled;       // consensus::run only: a lost wake

struct GroupOptions {
	FixedConfiguration configuration;
	std::optional<Membership> founding{};       // create() founds the group with these roles (Worker::found)
	kronuz::journal::AdmissionLimits admission;
	Limits limits{};
	Timing timing{};
	WorkerLimits scheduling{};
	std::optional<SnapshotLimits> snapshots{};
	std::size_t inbox = 256;                    // received messages queued per group; more are dropped and counted
};

// The loop's seams. The host's journal (journal/host.h) holds every group's Store and the
// loop's turn driver, host.driver(loop), which the host runs (TurnDriver::run) on the same
// scheduler, or consensus::run commits inline. `seed` seeds the election jitter; 0 draws one.
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
//                      Never for a witness member: it keeps the log, no application state, so the
//                      Driver acknowledges its ranges itself
//   RoleChanged        role, term and leader changes
//   ActivateInstall    an installed image to activate; answer with activated() or activation_failed()
//   CheckpointPublished, RepairCompleted   only when no caller awaits it (the caller was cancelled)
//   SnapshotNeeded, SnapshotTransmit, SnapshotReleased   a leader's sender flights
//   Fenced             the group fenced; every waiter completes with `fenced`
template <class S> concept sink = /* invocable as above for each notice */;

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
	void activated(GroupId, Token token);       // ActivateInstall::token
	void activation_failed(GroupId, Token token, std::string reason);
	void peer_limits(GroupId, PeerLimits);
	std::uint64_t dropped(GroupId) const;       // an open group's inbox overflow; for an id not open (closed, never opened), the Driver-wide count of plain calls dropped on ids not open

	// Task calls, from any scheduler: a call made off the loop hops there (one post) and resumes
	// the caller on its own scheduler. Each completes per its D8 row (below).
	task::task<void> create(GroupId, kronuz::journal::Identity, task::cancel_token = {});
	template <class Restore> task::task<void> recover(GroupId, Restore, task::cancel_token = {});
	task::task<Placement> propose(GroupId, std::string command, task::cancel_token = {});
	task::task<void> committed(GroupId, Placement, task::cancel_token = {});
	task::task<Placement> change_membership(GroupId, Membership next, task::cancel_token = {});
	task::task<void> transfer_leadership(GroupId, NodeId target, task::cancel_token = {});
	task::task<RepairCompleted> repair(GroupId, LogBoundary, task::cancel_token = {});
	template <capture Capture> task::task<CheckpointPublished> checkpoint(GroupId, std::uint64_t application_cap, Capture, task::cancel_token = {});
	task::task<ReadReady> read(GroupId, task::cancel_token = {});
	task::task<void> close(GroupId);            // retirement until drained; no caller token

	// On the loop: an open group's role, term, leader, indexes and membership in force.
	struct GroupState { Role role; Term term; NodeId leader; Index committed, applied; Membership membership; };
	std::optional<GroupState> state(GroupId) const;
	std::size_t armed_group_timers() const;     // groups whose wheel entry is armed
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
| `checkpoint` | `CheckpointPublished` | before publication starts it is cancelled and its reservation reclaimed; after, publication finishes and the Sink gets `CheckpointPublished`; the caller gets `task::cancelled` either way | started publication: completes when published; else `closed` |
| `close` | the group's state is freed and its row erased: `groups()` no longer counts it, and the id opens again on this Driver; a late plain call is dropped and counted with the ids not open. Close completes on an open, fenced or never-created row, and on an id the Driver never held. The journal host still refuses `create` or `recover` of an id it created or recovered until the host reopens | no token | - |

Every call completes with `fenced` once its group fences, and with `task::cancelled`
(`cancel_reason::shutdown`) once the loop shuts down; a staged record is still written. A call on
an id that is not open, or is closing or closed, completes with `closed` and submits nothing.

**Shutdown order.** A host closes its groups while `run()` steps the loop, then requests `stop`.
After `stop`, `close` completes with `task::cancelled` like every other call and frees nothing;
`~Driver` frees what the groups still hold.

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

## The blocking entry

`consensus::run(driver, scheduler, work)` holds the loop's turn driver (journal's `Hold`, so a
`TurnDriver::run` holding it is refused) and the Driver (its `run()` is refused meanwhile), and
loops:

1. Run the scheduler until its queue is empty.
2. If records are staged in the loop's turn, commit them inline and go to 1.
3. If `work` finished, return its result or rethrow its exception.
4. Otherwise, if an operation deadline (a `task::deadline` on the scheduler) is armed, sleep
   toward it or until another thread posts (a cancel's hop), and go to 1. Group timers never
   count, though they fire as the clock passes them.
5. Otherwise throw `stalled`, naming the groups that hold a waiter.

`propose`, `committed`, `change_membership`, `transfer_leadership` and `read` on a group whose
membership has a voting member (voter or witness) other than this node throw `needs_peer` and
submit nothing; `create`, `recover`, `checkpoint`, `repair` and `close` need no peer.

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
| `PeerLimits{...}` | `peer_limits(group, limits)` |
| `applied`, `application_failed` | the same plain calls |
| `reserve_checkpoint`, `attach_capture`, `offer_application_chunk` | `checkpoint(group, cap, capture)` |
| `snapshot_activated`, `snapshot_activation_failed` | `activated`, `activation_failed` after the Sink's `ActivateInstall` |
