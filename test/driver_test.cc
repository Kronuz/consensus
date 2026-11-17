/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// The consensus Driver (docs/DRIVER.md) on one loop: a lone voter's calls through Driver on
// inline_scheduler (no real time; the manual clock moves only when a case says so), each
// call's completion and cancellation row (async-drivers D8), close, and the blocking entry
// (D5). `driver_test [case]`; no case runs them all.
#include "consensus/driver.h"
#include "journal/memory_io.h"

#include <cstdio>
#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <vector>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace jt = kronuz::journal::testing;

namespace {

int checks = 0, failures = 0;
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; if (failures < 40) { std::cerr << "FAIL: " << what << '\n'; } }
}
template <class F> bool throws_as(F&& f, const std::function<bool(std::exception_ptr)>& match) {
	try { f(); } catch (...) { return match(std::current_exception()); }
	return false;
}

constexpr Timing compact{20, 40, 100, 200, 60000};
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{4u << 20, 4096}, {64 * 1024, 3}, {256 * 1024, 4}, 16, 3};
const j::Identity host_identity{'H', 'O', 'S', 'T'};

Limits limits() { Limits l; l.command_bytes = 1024; l.rpc_bytes = 4096; l.log_bytes = 64 * 1024; l.log_entries = 64; l.control_entries = 4; l.rpc_entries = 8; return l; }
GroupOptions options(std::vector<NodeId> voters, NodeId local = 1, std::optional<Membership> founding = {}) {
	GroupOptions o;
	o.configuration.local = local; o.configuration.voters = std::move(voters);
	o.configuration.cluster[0] = 'C'; o.configuration.configuration[0] = 'G';
	o.founding = std::move(founding);
	o.admission = admission; o.limits = limits(); o.timing = compact; o.scheduling = WorkerLimits{16, 128};
	return o;
}

// What the Driver hands the host, kept for the checks.
struct Sink {
	std::vector<std::pair<GroupId, Send>> sent;
	std::vector<std::pair<GroupId, Committed>> committed;
	std::vector<std::pair<GroupId, RoleChanged>> roles;
	std::vector<std::pair<GroupId, ActivateInstall>> activations;
	std::vector<std::pair<GroupId, CheckpointPublished>> published;
	std::vector<std::pair<GroupId, RepairCompleted>> repaired;
	std::vector<std::pair<GroupId, Fenced>> fenced;
	std::size_t snapshots = 0;
	void operator()(GroupId g, Send n) { sent.emplace_back(g, std::move(n)); }
	void operator()(GroupId g, Committed n) { committed.emplace_back(g, std::move(n)); }
	void operator()(GroupId g, RoleChanged n) { roles.emplace_back(g, n); }
	void operator()(GroupId g, ActivateInstall n) { activations.emplace_back(g, n); }
	void operator()(GroupId g, CheckpointPublished n) { published.emplace_back(g, n); }
	void operator()(GroupId g, RepairCompleted n) { repaired.emplace_back(g, n); }
	void operator()(GroupId g, Fenced n) { fenced.emplace_back(g, std::move(n)); }
	void operator()(GroupId, SnapshotNeeded) { ++snapshots; }
	void operator()(GroupId, SnapshotTransmit) { ++snapshots; }
	void operator()(GroupId, SnapshotReleased) { ++snapshots; }
	// The application: applies each committed range at once (the test calls applied()).
	Index last_committed(GroupId g) const {
		Index last = 0;
		for (const auto& [id, c] : committed) { if (id == g && !c.entries.empty()) { last = std::max(last, c.entries.back().index); } }
		return last;
	}
};
static_assert(sink<Sink>);

// One host and one loop on an inline_scheduler: the turn driver's run() and the Driver's
// run() as roots. Turns and the Worker's own IO go through InlineCompletion, or through a
// HeldCompletion the case completes by hand.
struct Loop {
	jt::Disk& disk;
	task::inline_scheduler scheduler;
	j::InlineCompletion inline_io;
	j::HeldCompletion held;
	std::unique_ptr<j::Host> host;
	Sink sink;
	std::unique_ptr<Driver<Sink>> driver;
	task::cancel_source stop;
	bool turns_done = false, driver_done = false;
	explicit Loop(jt::Disk& d, bool hold_turns = false, bool hold_io = false, std::uint64_t seed = 7) : disk(d) {
		host = std::make_unique<j::Host>(std::make_shared<jt::MemoryIO>(disk, "host/"), host_identity, j::HostOptions{1, turn_options});
		auto io = hold_io ? j::completion_ref::of(held) : j::completion_ref::of(inline_io);
		auto turns = hold_turns ? j::completion_ref::of(held) : j::completion_ref::of(inline_io);
		driver = std::make_unique<Driver<Sink>>(Seams{task::scheduler_ref::of(scheduler), io, *host, 0, seed}, sink);
		auto ref = task::scheduler_ref::of(scheduler);
		task::spawn(ref, host->driver(0).run(turns, stop.token()), [this](std::exception_ptr) { turns_done = true; });
		task::spawn(ref, driver->run(stop.token()), [this](std::exception_ptr) { driver_done = true; });
		scheduler.run();
	}
	~Loop() { finish(); }
	void finish() {
		if (!driver) { return; }
		stop.request(task::cancel_reason::shutdown);
		release();
		check(turns_done && driver_done, "the turn driver's and the Driver's runs end at shutdown");
		driver.reset(); host.reset();
	}
	// Completes every held primitive, and what they lead to.
	void release() {
		scheduler.run();
		while (held.held()) { held.complete(held.tickets().front()); scheduler.run(); }
	}
	// Starts `work` and runs the loop with no clock advance.
	template <class T> struct Started {
		bool done = false;
		std::exception_ptr error;
		std::optional<T> value;
	};
	template <class T> std::shared_ptr<Started<T>> start(task::task<T> work) {
		auto state = std::make_shared<Started<T>>();
		task::spawn(task::scheduler_ref::of(scheduler), std::move(work), [state](std::exception_ptr e, std::optional<T> v) { state->done = true; state->error = e; state->value = std::move(v); });
		scheduler.run();
		return state;
	}
	std::shared_ptr<Started<bool>> start(task::task<void> work) {
		auto state = std::make_shared<Started<bool>>();
		auto wrapped = [](task::task<void> inner) -> task::task<bool> { co_await std::move(inner); co_return true; };
		task::spawn(task::scheduler_ref::of(scheduler), wrapped(std::move(work)), [state](std::exception_ptr e, std::optional<bool> v) { state->done = true; state->error = e; state->value = v; });
		scheduler.run();
		return state;
	}
	template <class T> bool settle(task::task<T> work, std::optional<T>& out, std::exception_ptr& error) {
		auto state = start(std::move(work));
		release();
		out = state->value; error = state->error;
		return state->done;
	}
	bool settle(task::task<void> work, std::exception_ptr& error) {
		auto state = start(std::move(work));
		release();
		error = state->error;
		return state->done;
	}
	// The application: applies each Committed range as Core delivers it, one range at a time.
	void apply(GroupId g) {
		for (Index done = 0;;) {
			auto last = sink.last_committed(g);
			if (last <= done) { return; }
			driver->applied(g, Applied{last}); done = last;
			release();
		}
	}
	// A lone voter, created and leading.
	void lone(GroupId g) {
		driver->open(g, options({1}));
		std::exception_ptr error;
		check(settle(driver->create(g, j::Identity{'L', static_cast<char>(g)}), error) && !error, "create");
	}
};

template <class E> bool is(std::exception_ptr error) {
	if (!error) { return false; }
	try { std::rethrow_exception(error); } catch (const E&) { return true; } catch (...) {}
	return false;
}
bool committed_command(const Sink& sink, GroupId g, const std::string& command) {
	for (const auto& [id, c] : sink.committed) {
		if (id != g) { continue; }
		for (const auto& e : c.entries) { if (e.kind == EntryKind::Command && e.payload == command) { return true; } }
	}
	return false;
}

// A lone voter's first proposal succeeds with no clock advance, after create and after
// recover (X5, R9).
void lone_voter() {
	jt::Disk disk;
	constexpr GroupId g = 11;
	Placement first;
	{
		Loop loop(disk);
		loop.driver->open(g, options({1}));
		std::exception_ptr error;
		check(loop.settle(loop.driver->create(g, j::Identity{'L', 'V'}), error) && !error, "create completes with no clock advance");
		const auto start = loop.scheduler.now();
		std::optional<Placement> placed;
		check(loop.settle(loop.driver->propose(g, "first"), placed, error) && !error && placed, "a lone voter's first proposal after create is placed with no clock advance");
		check(loop.scheduler.now() == start, "no clock advance");
		if (placed) { first = *placed; check(placed->term == 1 && placed->index == 2, "the first command follows the leader's no-op at term 1"); }
		check(loop.settle(loop.driver->committed(g, first), error) && !error, "committed completes for the lone voter");
		loop.apply(g);
		auto state = loop.driver->state(g);
		check(state && state->role == Role::Leader && state->committed >= first.index, "the lone voter leads and committed its proposal");
		check(loop.sink.last_committed(g) == first.index && state && state->applied == first.index, "the Sink receives the committed ranges, applied through the proposal");
		std::optional<ReadReady> ready;
		check(loop.settle(loop.driver->read(g), ready, error) && !error && ready && ready->index >= first.index, "a read completes once applied reaches its index");
	}
	{
		Loop loop(disk);
		loop.driver->open(g, options({1}));
		std::exception_ptr error;
		check(loop.host->status(g) == j::GroupStatus::stored, "the group is stored");
		check(loop.settle(loop.driver->recover(g, [](j::ArtifactReader&) {}), error) && !error, "recover completes with no clock advance");
		std::optional<Placement> placed;
		check(loop.settle(loop.driver->propose(g, "after recover"), placed, error) && !error && placed, "a lone voter's first proposal after recover is placed with no clock advance");
		check(placed && placed->term == 2 && placed->index > first.index, "it leads a new term above the recovered log");
		check(loop.scheduler.now() == 0, "no clock advance");
	}
}

// D8, row by row, on a lone voter.
void rows() {
	constexpr GroupId g = 21;
	{  // a call not yet admitted: unlinked, never submitted
		jt::Disk disk; Loop loop(disk);
		loop.driver->open(g, options({1}));
		task::cancel_source cancel;
		auto call = loop.start(loop.driver->propose(g, "never", cancel.token()));
		check(!call->done, "a call on a group not yet created waits");
		cancel.request(); loop.scheduler.run();
		check(call->done && is<task::cancelled>(call->error), "cancelled before admission completes with task::cancelled");
		std::exception_ptr error;
		check(loop.settle(loop.driver->create(g, j::Identity{'R', 'O'}), error) && !error, "create");
		std::optional<Placement> placed;
		check(loop.settle(loop.driver->propose(g, "later"), placed, error) && placed, "a later proposal is placed");
		loop.apply(g);
		check(!committed_command(loop.sink, g, "never") && committed_command(loop.sink, g, "later"), "the cancelled call was never submitted");
	}
	{  // accepted by Core, staged, then cancelled: the record is still written and commits
		jt::Disk disk; Loop loop(disk, true);
		loop.lone(g);
		task::cancel_source cancel;
		auto call = loop.start(loop.driver->propose(g, "staged", cancel.token()));
		check(!call->done && loop.held.held() == 1, "the proposal's record waits in a held turn");
		cancel.request(); loop.scheduler.run();
		check(call->done && is<task::cancelled>(call->error), "the caller completes with task::cancelled");
		loop.release(); loop.apply(g);
		check(committed_command(loop.sink, g, "staged"), "the cancelled proposal's record is written and commits");
	}
	{  // committed: unlinked on cancel; read: unlinked on cancel while awaiting the apply
		jt::Disk disk; Loop loop(disk);
		loop.lone(g);
		std::exception_ptr error; std::optional<Placement> placed;
		check(loop.settle(loop.driver->propose(g, "c"), placed, error) && placed, "placed");
		task::cancel_source cancel;
		auto later = loop.start(loop.driver->committed(g, Placement{placed->term, placed->index + 5}, cancel.token()));
		auto read = loop.start(loop.driver->read(g, cancel.token()));
		check(!later->done && !read->done, "committed waits for its index; read waits for the apply");
		cancel.request(); loop.scheduler.run();
		check(later->done && is<task::cancelled>(later->error) && read->done && is<task::cancelled>(read->error), "both complete with task::cancelled");
		check(loop.settle(loop.driver->committed(g, *placed), error) && !error, "the entry is unaffected");
		check(loop.settle(loop.driver->committed(g, Placement{placed->term + 1, placed->index}), error) && is<superseded>(error), "another term at the index is superseded");
	}
	{  // transfer_leadership on a lone voter is refused by Core; repair completes at RepairCompleted
		jt::Disk disk; Loop loop(disk);
		loop.lone(g);
		std::exception_ptr error;
		check(loop.settle(loop.driver->transfer_leadership(g, 2), error) && is<rejected>(error), "a transfer with no target voter is rejected");
		std::optional<Placement> placed;
		check(loop.settle(loop.driver->propose(g, "r"), placed, error) && placed, "placed");
		loop.apply(g);
		std::optional<RepairCompleted> repaired;
		check(loop.settle(loop.driver->repair(g, LogBoundary{placed->index, placed->term}), repaired, error) && !error && repaired, "repair completes at RepairCompleted");
		check(repaired && repaired->boundary.index == placed->index && repaired->epoch == 1, "at its boundary, in a new epoch");
	}
	{  // change_membership is a proposal: it completes at ProposalPlaced
		jt::Disk disk; Loop loop(disk);
		loop.lone(g);
		Membership next; next.members = {{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Learner}}; next.next_incarnation = 3;
		std::exception_ptr error; std::optional<Placement> placed;
		check(loop.settle(loop.driver->change_membership(g, next), placed, error) && !error && placed, "a membership change is placed");
		auto state = loop.driver->state(g);
		check(state && state->membership == next, "the membership in force is the proposed one");
	}
}

struct Capture {
	std::string image; std::size_t at = 0; Index through; Term term; std::uint64_t epoch = 0;
	CaptureMetadata metadata() const { return {0, through, term, epoch}; }
	std::size_t read(std::span<char> out) {
		auto n = std::min(out.size(), image.size() - at);
		std::copy_n(image.data() + at, n, out.data()); at += n; return n;
	}
};

// checkpoint: completes at CheckpointPublished; cancelled before publication it is discarded.
void checkpoints() {
	constexpr GroupId g = 31;
	{
		jt::Disk disk; Loop loop(disk);
		loop.lone(g);
		std::exception_ptr error; std::optional<Placement> placed;
		check(loop.settle(loop.driver->propose(g, "k"), placed, error) && placed, "placed");
		loop.apply(g);
		std::optional<CheckpointPublished> published;
		Capture capture{std::string(100000, 'x'), 0, placed->index, placed->term};
		check(loop.settle(loop.driver->checkpoint(g, 1 << 20, capture), published, error) && !error && published, "checkpoint completes at CheckpointPublished");
		check(published && published->through == placed->index, "through the capture's boundary");
		check(loop.sink.published.empty(), "the Sink hears nothing a caller awaited");
	}
	{
		jt::Disk disk; Loop loop(disk, false, true);
		loop.lone(g);
		std::exception_ptr error; std::optional<Placement> placed;
		check(loop.settle(loop.driver->propose(g, "k"), placed, error) && placed, "placed");
		loop.apply(g);
		task::cancel_source cancel;
		auto call = loop.start(loop.driver->checkpoint(g, 1 << 20, Capture{std::string(1000, 'y'), 0, placed->index, placed->term}, cancel.token()));
		check(!call->done && loop.held.held() >= 1, "the checkpoint's artifact IO is held");
		cancel.request(); loop.scheduler.run();
		check(call->done && is<task::cancelled>(call->error), "cancelled before publication completes with task::cancelled");
		loop.release();
		check(loop.sink.published.empty(), "a checkpoint cancelled before publication is never published");
	}
}

// Close: waiters holding no staged record complete with `closed`; an admitted proposal
// completes when its record is written; late plain calls are dropped and counted.
void close_rows() {
	constexpr GroupId g = 41;
	jt::Disk disk; Loop loop(disk, true);
	loop.lone(g);
	auto staged = loop.start(loop.driver->propose(g, "staged"));
	auto queued = loop.start(loop.driver->propose(g, "queued"));
	auto later = loop.start(loop.driver->committed(g, Placement{1, 100}));
	auto read = loop.start(loop.driver->read(g));
	check(loop.held.held() == 1 && !staged->done && !queued->done && !later->done && !read->done, "one proposal staged, the rest waiting");
	auto closing = loop.start(loop.driver->close(g));
	check(queued->done && is<closed>(queued->error), "a call not yet admitted completes with closed");
	check(later->done && is<closed>(later->error) && read->done && is<closed>(read->error), "committed and read complete with closed");
	check(!staged->done && !closing->done, "the staged proposal drains; close waits for it");
	auto refused = loop.start(loop.driver->propose(g, "refused"));
	check(refused->done && is<closed>(refused->error), "a call on a closing group completes with closed");
	loop.release();
	check(staged->done && !staged->error && staged->value && staged->value->index > 0, "the staged proposal completes with its placement once written");
	check(closing->done && !closing->error, "close completes once the group drained");
	check(!loop.driver->state(g), "the group's state is freed");
	loop.driver->receive(g, Receive{2, VoteRequest{9, 0, 0}}); loop.driver->applied(g, Applied{1});
	check(loop.driver->dropped(g) == 2, "a late receive and applied are dropped and counted");
}

// A fenced group completes every waiter with `fenced`; shutdown cancels the rest.
void fence_and_shutdown() {
	constexpr GroupId g = 51, h = 52;
	jt::Disk disk; Loop loop(disk);
	loop.lone(g); loop.lone(h);
	std::exception_ptr error; std::optional<Placement> placed;
	check(loop.settle(loop.driver->propose(g, "f"), placed, error) && placed, "placed");
	auto read = loop.start(loop.driver->read(g));
	loop.driver->application_failed(g, loop.sink.last_committed(g), "the application failed");
	loop.scheduler.run();
	check(read->done && is<fenced>(read->error), "a waiter on a fenced group completes with fenced");
	check(!loop.sink.fenced.empty() && loop.sink.fenced.back().first == g, "the Sink hears Fenced");
	auto waiting = loop.start(loop.driver->read(h));
	check(!waiting->done, "a read waits for its apply");
	loop.stop.request(task::cancel_reason::shutdown); loop.scheduler.run();
	check(waiting->done && is<task::cancelled>(waiting->error), "shutdown completes a waiter with task::cancelled");
	auto refused = loop.start(loop.driver->propose(h, "x"));
	check(refused->done && is<task::cancelled>(refused->error), "a call after shutdown completes with task::cancelled");
}

const std::map<std::string, void (*)()> cases{
	{"lone_voter", &lone_voter},
	{"rows", &rows},
	{"checkpoints", &checkpoints},
	{"close", &close_rows},
	{"fence", &fence_and_shutdown},
};

}  // namespace

int main(int argc, char** argv) {
	if (argc > 1) {
		auto found = cases.find(argv[1]);
		if (found == cases.end()) { std::cerr << "unknown case " << argv[1] << '\n'; return 2; }
		found->second();
	} else {
		for (const auto& [name, run] : cases) { run(); }
	}
	std::cout << checks << " driver checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
