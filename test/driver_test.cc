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
// (D5), whose cross-thread wake case runs under TSan. `driver_test [case]`; no case runs them all.
#include "consensus/driver.h"
#include "faultlab/faultfs.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace fl = kronuz::faultlab;

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

// The host's directory on `disk`, made on first use.
std::shared_ptr<j::IO> host_io(fl::filesystem& disk) {
	const auto names = disk.list();
	if (std::find(names.begin(), names.end(), "host") == names.end()) { disk.make_directory("host"); }
	return std::make_shared<fl::io>(disk, "host");
}

constexpr Timing compact{20, 40, 100, 200, 60000};
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 32, 3};
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
	std::vector<std::pair<GroupId, LogBoundary>> activations;
	std::vector<std::pair<GroupId, CheckpointPublished>> published;
	std::vector<std::pair<GroupId, RepairCompleted>> repaired;
	std::vector<std::pair<GroupId, Fenced>> fenced;
	std::size_t snapshots = 0;
	bool throw_committed = false;  // the application's handler fails
	void operator()(GroupId g, Send n) { sent.emplace_back(g, std::move(n)); }
	void operator()(GroupId g, Committed n) {
		if (throw_committed) { throw std::runtime_error("the application's handler failed"); }
		committed.emplace_back(g, std::move(n));
	}
	void operator()(GroupId g, RoleChanged n) { roles.emplace_back(g, n); }
	void operator()(GroupId, SendTransfer) {}
	void operator()(GroupId, ValidateImage) {}
	void operator()(GroupId g, ActivateImage n) { activations.emplace_back(g, n.boundary); }
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
	fl::filesystem& disk;
	task::inline_scheduler scheduler;
	j::InlineCompletion inline_io;
	j::HeldCompletion held;
	std::unique_ptr<j::Host> host;
	Sink sink;
	std::unique_ptr<Driver<Sink>> driver;
	task::cancel_source stop;
	bool turns_done = false, driver_done = false;
	explicit Loop(fl::filesystem& d, bool hold_turns = false, bool hold_io = false, std::uint64_t seed = 7) : disk(d) {
		host = std::make_unique<j::Host>(host_io(disk), host_identity, j::HostOptions{1, turn_options});
		auto io = hold_io ? j::completion_ref::of(held) : j::completion_ref::of(inline_io);
		auto turns = hold_turns ? j::completion_ref::of(held) : j::completion_ref::of(inline_io);
		driver = std::make_unique<Driver<Sink>>(Seams{task::scheduler_ref::of(scheduler), io, *host, 0, seed}, sink);
		auto ref = task::scheduler_ref::of(scheduler);
		task::spawn(ref, host->driver(0).run(turns, stop.token()), [this](std::exception_ptr) { turns_done = true; });
		task::spawn(ref, driver->run(stop.token()), [this](std::exception_ptr) { driver_done = true; });
		run();
	}
	~Loop() { finish(); }
	// Runs what is queued, failing (not hanging) if the loop never settles.
	void run() {
		std::size_t budget = 1'000'000;
		scheduler.run_until([&] { return --budget == 0; });
		if (!budget) {
			auto s = driver ? driver->state(current) : std::nullopt;
			check(false, "the loop spins: role " + std::to_string(s ? int(s->role) : -1) + " committed " + std::to_string(s ? s->committed : 0) + " held " + std::to_string(held.held()));
			std::exit(1);
		}
	}
	GroupId current = 0;
	void finish() {
		if (!driver) { return; }
		stop.request(task::cancel_reason::shutdown);
		release();
		check(turns_done && driver_done, "the turn driver's and the Driver's runs end at shutdown");
		driver.reset(); host.reset();
	}
	// Completes every held primitive, and what they lead to.
	void release() {
		run();
		for (int n = 0; held.held(); ++n) {
			if (n == 100000) { check(false, "held IO never runs out"); std::exit(1); }
			held.complete(held.tickets().front()); run();
		}
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
		run();
		return state;
	}
	std::shared_ptr<Started<bool>> start(task::task<void> work) {
		auto state = std::make_shared<Started<bool>>();
		auto wrapped = [](task::task<void> inner) -> task::task<bool> { co_await std::move(inner); co_return true; };
		task::spawn(task::scheduler_ref::of(scheduler), wrapped(std::move(work)), [state](std::exception_ptr e, std::optional<bool> v) { state->done = true; state->error = e; state->value = v; });
		run();
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
		current = g;
		driver->open(g, options({1}));
		std::exception_ptr error;
		check(settle(driver->create(g, j::Identity{'L', static_cast<char>(g)}), error) && !error, "create");
		apply(g);
	}
};

std::string what(std::exception_ptr error) {
	if (!error) { return "no error"; }
	try { std::rethrow_exception(error); } catch (const std::exception& e) { return e.what(); } catch (...) {}
	return "unknown";
}
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
	fl::filesystem disk;
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
	std::cerr << " rows: not admitted\n";
	{  // a call not yet admitted: unlinked, never submitted
		fl::filesystem disk; Loop loop(disk);
		loop.driver->open(g, options({1}));
		task::cancel_source cancel;
		auto call = loop.start(loop.driver->propose(g, "never", cancel.token()));
		check(!call->done, "a call on a group not yet created waits");
		cancel.request(); loop.run();
		check(call->done && is<task::cancelled>(call->error), "cancelled before admission completes with task::cancelled");
		std::exception_ptr error;
		check(loop.settle(loop.driver->create(g, j::Identity{'R', 'O'}), error) && !error, "create");
		std::optional<Placement> placed;
		check(loop.settle(loop.driver->propose(g, "later"), placed, error) && placed, "a later proposal is placed");
		loop.apply(g);
		check(!committed_command(loop.sink, g, "never") && committed_command(loop.sink, g, "later"), "the cancelled call was never submitted");
	}
	std::cerr << " rows: staged\n";
	{  // accepted by Core, staged, then cancelled: the record is still written and commits
		fl::filesystem disk; Loop loop(disk, true);
		loop.lone(g);
		task::cancel_source cancel;
		auto call = loop.start(loop.driver->propose(g, "staged", cancel.token()));
		check(!call->done && loop.held.held() == 1, "the proposal's record waits in a held turn");
		cancel.request(); loop.run();
		check(call->done && is<task::cancelled>(call->error), "the caller completes with task::cancelled");
		loop.release(); loop.apply(g);
		check(committed_command(loop.sink, g, "staged"), "the cancelled proposal's record is written and commits");
	}
	std::cerr << " rows: committed and read\n";
	{  // committed: unlinked on cancel; read: unlinked on cancel while awaiting the apply
		fl::filesystem disk; Loop loop(disk);
		loop.lone(g);
		std::exception_ptr error; std::optional<Placement> placed;
		check(loop.settle(loop.driver->propose(g, "c"), placed, error) && placed, "placed");
		task::cancel_source cancel;
		auto later = loop.start(loop.driver->committed(g, Placement{placed->term, placed->index + 5}, cancel.token()));
		auto read = loop.start(loop.driver->read(g, cancel.token()));
		check(!later->done && !read->done, "committed waits for its index; read waits for the apply");
		cancel.request(); loop.run();
		check(later->done && is<task::cancelled>(later->error) && read->done && is<task::cancelled>(read->error), "both complete with task::cancelled");
		check(loop.settle(loop.driver->committed(g, *placed), error) && !error, "the entry is unaffected");
		check(loop.settle(loop.driver->committed(g, Placement{placed->term + 1, placed->index}), error) && is<superseded>(error), "another term at the index is superseded");
	}
	std::cerr << " rows: transfer and repair\n";
	{  // transfer_leadership on a lone voter is refused by Core; repair completes at RepairCompleted
		fl::filesystem disk; Loop loop(disk);
		loop.lone(g);
		std::exception_ptr error;
		check(loop.settle(loop.driver->transfer_leadership(g, 2), error) && is<rejected>(error), "a transfer with no target voter is rejected");
		std::optional<Placement> placed;
		check(loop.settle(loop.driver->propose(g, "r"), placed, error) && placed, "placed after the refused transfer: " + what(error));
		loop.apply(g);
		std::optional<RepairCompleted> repaired;
		check(loop.settle(loop.driver->repair(g, LogBoundary{placed->index, placed->term}), repaired, error) && !error && repaired, "repair completes at RepairCompleted");
		check(repaired && repaired->boundary.index == placed->index && repaired->epoch == 1, "at its boundary, in a new epoch");
	}
	std::cerr << " rows: membership\n";
	{  // change_membership is a proposal: it completes at ProposalPlaced
		fl::filesystem disk; Loop loop(disk);
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
		fl::filesystem disk; Loop loop(disk);
		loop.lone(g);
		std::exception_ptr error; std::optional<Placement> placed;
		check(loop.settle(loop.driver->propose(g, "k"), placed, error) && placed, "placed");
		loop.apply(g);
		std::optional<CheckpointPublished> published;
		Capture capture{std::string(100000, 'x'), 0, placed->index, placed->term};
		const bool done = loop.settle(loop.driver->checkpoint(g, 1 << 20, capture), published, error);
		check(done && !error && published, "checkpoint completes at CheckpointPublished: done " + std::to_string(done) + ", " + what(error));
		check(published && published->through == placed->index, "through the capture's boundary");
		check(loop.sink.published.empty(), "the Sink hears nothing a caller awaited");
	}
	{
		fl::filesystem disk; Loop loop(disk, false, true);
		loop.lone(g);
		std::exception_ptr error; std::optional<Placement> placed;
		check(loop.settle(loop.driver->propose(g, "k"), placed, error) && placed, "placed");
		loop.apply(g);
		task::cancel_source cancel;
		auto call = loop.start(loop.driver->checkpoint(g, 1 << 20, Capture{std::string(1000, 'y'), 0, placed->index, placed->term}, cancel.token()));
		check(!call->done && loop.held.held() >= 1, "the checkpoint's artifact IO is held");
		cancel.request(); loop.run();
		check(call->done && is<task::cancelled>(call->error), "cancelled before publication completes with task::cancelled");
		loop.release();
		check(loop.sink.published.empty(), "a checkpoint cancelled before publication is never published");
	}
}


// msg-1: a checkpoint never covers less than an acknowledged record. Checkpoints land at the
// applied index while placed records above it are unapplied, and beside a proposal staged in
// the same held turn; after recovery every placed record above the last checkpoint commits.
void checkpoint_keeps_acknowledged() {
	constexpr GroupId g = 71;
	fl::filesystem disk;
	std::vector<std::pair<std::string, Placement>> acknowledged;
	Index covered = 0;
	{
		Loop loop(disk, true);
		loop.lone(g);
		std::exception_ptr error; std::optional<Placement> placed;
		auto propose = [&](const std::string& command) {
			auto call = loop.start(loop.driver->propose(g, command));
			loop.release();
			check(call->done && !call->error && call->value, "placed: " + command + ": " + what(call->error));
			if (call->value) { acknowledged.emplace_back(command, *call->value); }
		};
		for (int round = 0; round < 4; ++round) {
			propose("applied-" + std::to_string(round));
			loop.apply(g);
			const auto through = loop.sink.last_committed(g);
			const auto term = acknowledged.back().second.term;
			// Acknowledged, committed and handed to the Sink, never applied: above the checkpoint.
			propose("above-" + std::to_string(round) + "a");
			propose("above-" + std::to_string(round) + "b");
			// One more proposal staged in the held turn as the checkpoint starts.
			auto staged = loop.start(loop.driver->propose(g, "staged-" + std::to_string(round)));
			auto checkpoint = loop.start(loop.driver->checkpoint(g, 1 << 20, Capture{std::string(5000, char('a' + round)), 0, through, term}));
			loop.release();
			check(staged->done && !staged->error && staged->value, "the staged proposal is placed: " + what(staged->error));
			if (staged->value) { acknowledged.emplace_back("staged-" + std::to_string(round), *staged->value); }
			check(checkpoint->done && !checkpoint->error && checkpoint->value && checkpoint->value->through == through, "the checkpoint publishes at the applied index: " + what(checkpoint->error));
			if (checkpoint->value) { covered = checkpoint->value->through; }
			loop.apply(g);
		}
		propose("last");
	}
	Loop loop(disk);
	loop.driver->open(g, options({1}));
	std::exception_ptr error;
	check(loop.settle(loop.driver->recover(g, [](j::ArtifactReader&) {}), error) && !error, "recover: " + what(error));
	loop.apply(g);
	std::size_t above = 0;
	for (const auto& [command, placement] : acknowledged) {
		if (placement.index <= covered) { continue; }
		++above;
		check(committed_command(loop.sink, g, command), "acknowledged record " + command + " at " + std::to_string(placement.index) + " survives recovery above checkpoint " + std::to_string(covered));
		check(loop.settle(loop.driver->committed(g, placement), error) && !error, "and commits with its term: " + command + ": " + what(error));
	}
	check(covered > 0 && above >= 4, "checkpoints published and acknowledged records above the last one: " + std::to_string(above));
}
// Close: waiters holding no staged record complete with `closed`; an admitted proposal
// completes when its record is written; late plain calls are dropped and counted.
void close_rows() {
	constexpr GroupId g = 41;
	fl::filesystem disk; Loop loop(disk, true);
	loop.lone(g);
	auto staged = loop.start(loop.driver->propose(g, "staged"));
	auto queued = loop.start(loop.driver->propose(g, "queued"));
	auto later = loop.start(loop.driver->committed(g, Placement{1, 100}));
	auto read = loop.start(loop.driver->read(g));
	// The other kinds, queued behind the staged turn: linked, none admitted.
	Membership next; next.members = {{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Learner}}; next.next_incarnation = 3;
	auto membership = loop.start(loop.driver->change_membership(g, next));
	auto transfer = loop.start(loop.driver->transfer_leadership(g, 2));
	auto repair = loop.start(loop.driver->repair(g, LogBoundary{1, 1}));
	auto checkpoint = loop.start(loop.driver->checkpoint(g, 1 << 20, Capture{std::string(1000, 'c'), 0, 1, 1}));
	std::vector<std::pair<const char*, std::function<std::pair<bool, std::exception_ptr>()>>> others{
		{"change_membership", [&] { return std::pair{membership->done, membership->error}; }},
		{"transfer_leadership", [&] { return std::pair{transfer->done, transfer->error}; }},
		{"repair", [&] { return std::pair{repair->done, repair->error}; }},
		{"checkpoint", [&] { return std::pair{checkpoint->done, checkpoint->error}; }},
	};
	check(loop.held.held() == 1 && !staged->done && !queued->done && !later->done && !read->done, "one proposal staged, the rest waiting");
	for (auto& [name, result] : others) { check(!result().first, std::string(name) + " waits, linked: " + what(result().second)); }
	auto closing = loop.start(loop.driver->close(g));
	check(queued->done && is<closed>(queued->error), "a call not yet admitted completes with closed");
	check(later->done && is<closed>(later->error) && read->done && is<closed>(read->error), "committed and read complete with closed");
	for (auto& [name, result] : others) { auto [done, error] = result(); check(done && is<closed>(error), std::string(name) + " completes with closed: " + what(error)); }
	check(!staged->done && !closing->done, "the staged proposal drains; close waits for it: " + what(staged->error));
	auto refused = loop.start(loop.driver->propose(g, "refused"));
	check(refused->done && is<closed>(refused->error), "a call on a closing group completes with closed");
	loop.release();
	check(staged->done && !staged->error && staged->value && staged->value->index > 0, "the staged proposal completes with its placement once written: " + what(staged->error));
	check(closing->done && !closing->error, "close completes once the group drained");
	check(!loop.driver->state(g), "the group's state is freed");
	loop.driver->receive(g, Receive{2, VoteRequest{9, 0, 0}}); loop.driver->applied(g, Applied{1});
	check(loop.driver->dropped(g) == 2, "a late receive and applied are dropped and counted");
}

// A fenced group completes every waiter with `fenced`; shutdown cancels the rest.
void fence_and_shutdown() {
	constexpr GroupId g = 51, h = 52;
	fl::filesystem disk; Loop loop(disk);
	loop.lone(g); loop.lone(h);
	std::exception_ptr error; std::optional<Placement> placed;
	check(loop.settle(loop.driver->propose(g, "f"), placed, error) && placed, "placed");
	auto read = loop.start(loop.driver->read(g));
	loop.driver->application_failed(g, loop.sink.last_committed(g), "the application failed");
	loop.run();
	check(read->done && is<fenced>(read->error), "a waiter on a fenced group completes with fenced");
	check(!loop.sink.fenced.empty() && loop.sink.fenced.back().first == g, "the Sink hears Fenced");
	check(loop.settle(loop.driver->propose(h, "w"), placed, error) && placed, "placed on the other group");
	auto waiting = loop.start(loop.driver->read(h));  // its range is committed, not yet applied
	check(!waiting->done, "a read waits for its apply");
	loop.stop.request(task::cancel_reason::shutdown); loop.run();
	check(waiting->done && is<task::cancelled>(waiting->error), "shutdown completes a waiter with task::cancelled");
	auto refused = loop.start(loop.driver->propose(h, "x"));
	check(refused->done && is<task::cancelled>(refused->error), "a call after shutdown completes with task::cancelled");
	auto closing = loop.start(loop.driver->close(h));
	check(closing->done && is<task::cancelled>(closing->error), "close after shutdown completes with task::cancelled: " + what(closing->error));
	check(loop.driver->waiting_groups().empty(), "and links no waiter on the stopped loop");
}

// A Sink that throws mid-batch fences the group: nothing after the throw is silently lost.
void sink_throws() {
	constexpr GroupId g = 61;
	fl::filesystem disk; Loop loop(disk);
	loop.lone(g);
	loop.sink.throw_committed = true;
	std::exception_ptr error; std::optional<Placement> placed;
	loop.settle(loop.driver->propose(g, "lost"), placed, error);
	loop.run();
	check(!loop.sink.fenced.empty() && loop.sink.fenced.back().first == g, "the group fences when its Sink throws, and the Sink hears Fenced");
	loop.sink.throw_committed = false;
	check(loop.settle(loop.driver->propose(g, "after"), placed, error) && is<fenced>(error), "a later call completes with fenced: " + what(error));
}

// D9: a learner promoted to witness is a data member until the promotion is in force at a
// range's position. A Committed range below an appended promotion goes to the Sink, so a later
// term that truncates the promotion leaves the apply index at what the application received.
void truncated_promotion() {
	constexpr GroupId g = 71;
	fl::filesystem disk; Loop loop(disk);
	loop.current = g;
	loop.driver->open(g, options({}, 4));
	std::exception_ptr error;
	check(loop.settle(loop.driver->create(g, j::Identity{'J', 'L'}), error) && !error, "a joiner creates its group");
	const Membership founding{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 4, 0};
	Membership learner = founding; learner.members.push_back({4, 4, MemberRole::Learner}); learner.next_incarnation = 5;
	Membership promoted = learner; promoted.members.back().role = MemberRole::Witness; promoted.leaving = 3;
	auto configuration = [](Index index, const Membership& m) { return Entry{index, 1, EntryKind::Configuration, encode_membership(m)}; };
	// Leader 1, term 1: the learner's own entry, a command, and the promotion; commit below it.
	loop.driver->receive(g, Receive{1, AppendRequest{1, 101, 0, 0, 3, 0, {Entry{1, 1, EntryKind::NoOp, {}}, configuration(2, learner), Entry{3, 1, EntryKind::Command, "a"}, configuration(4, promoted)}}});
	loop.release();
	auto s = loop.driver->state(g);
	check(s && s->committed == 3, "the learner committed through 3 with the promotion appended at 4");
	check(s && s->applied <= loop.sink.last_committed(g), "the apply index stays within what the Sink received, below an appended promotion: applied " +
		std::to_string(s ? s->applied : 0) + ", delivered " + std::to_string(loop.sink.last_committed(g)));
	loop.apply(g);
	// Leader 2, term 2, never saw the promotion: its no-op at 4 truncates it.
	loop.driver->receive(g, Receive{2, AppendRequest{2, 102, 3, 1, 4, 0, {Entry{4, 2, EntryKind::NoOp, {}}}}});
	loop.release();
	loop.apply(g);
	s = loop.driver->state(g);
	const auto* self = s ? find_member(s->membership, 4) : nullptr;
	check(self && self->role == MemberRole::Learner, "the truncated promotion leaves node 4 a learner");
	check(s && s->applied == 4 && loop.sink.last_committed(g) == 4, "the learner's application received every index through its apply index");
	Index next = 1;
	for (const auto& [id, c] : loop.sink.committed) {
		if (id != g) { continue; }
		check(c.first == next, "each Committed range starts after the last one the application received");
		next = c.entries.empty() ? next : c.entries.back().index + 1;
	}
}

// D9 both ways on one joiner, node 4: a learner at 2, promoted to witness at 4 (replacing
// witness 3), removed at 7. Leader 1, term 1, sends two entries per request, its data voters
// matched through the commit index; Core delivers two entries per Committed range.
namespace witness_case {
const Membership founding{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 4, 0};
const Membership learner{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}, {4, 4, MemberRole::Learner}}, 5, 0};
const Membership promoted{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}, {4, 4, MemberRole::Witness}}, 5, 3};
const Membership removed{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 5, 0};
Entry configuration(Index index, const Membership& m) { return Entry{index, 1, EntryKind::Configuration, encode_membership(m)}; }
Entry command(Index index) { return Entry{index, 1, EntryKind::Command, "c" + std::to_string(index)}; }
GroupOptions group_options() { auto o = options({}, 4); o.limits.rpc_entries = 2; return o; }
void append(Loop& loop, GroupId g, Token rpc, Index previous, Index commit, std::vector<Entry> entries) {
	loop.driver->receive(g, Receive{1, AppendRequest{1, rpc, previous, previous ? 1u : 0u, commit, 0, std::move(entries), commit}});
	loop.release();
}
// Ranges [1..2] (a learner's), [3..4] (ending at the promotion) and [5..6], all committed.
void promote(Loop& loop, GroupId g) {
	loop.current = g;
	loop.driver->open(g, group_options());
	std::exception_ptr error;
	check(loop.settle(loop.driver->create(g, j::Identity{'W', 'R'}), error) && !error, "a joiner creates its group");
	append(loop, g, 1, 0, 0, {Entry{1, 1, EntryKind::NoOp, {}}, configuration(2, learner)});
	append(loop, g, 2, 2, 0, {command(3), configuration(4, promoted)});
	append(loop, g, 3, 4, 0, {command(5), command(6)});
	append(loop, g, 4, 6, 6, {});
	loop.apply(g);
}
// The application received exactly `through` (1..through, contiguous) and the apply index is `applied`.
void expect(Loop& loop, GroupId g, Index first, Index through, Index applied, const std::string& when) {
	auto s = loop.driver->state(g);
	std::string ranges;
	Index next = first;
	bool contiguous = true;
	for (const auto& [id, c] : loop.sink.committed) {
		if (id != g || c.entries.empty()) { continue; }
		ranges += " [" + std::to_string(c.first) + ".." + std::to_string(c.entries.back().index) + "]";
		contiguous = contiguous && c.first == next;
		next = c.entries.back().index + 1;
	}
	const auto delivered = loop.sink.last_committed(g);
	const auto seen = when + ": committed " + std::to_string(s ? s->committed : 0) + ", applied " + std::to_string(s ? s->applied : 0) + ", Sink" + ranges;
	check(contiguous, seen + "; each range the Sink received starts after the last");
	check(delivered == through, seen + "; the Sink received through " + std::to_string(through));
	check(s && s->committed == applied && s->applied == applied, seen + "; the apply index reaches " + std::to_string(applied));
}
}  // namespace witness_case

// A promotion counts from the range that ends at it: [3..4] is the witness's, [1..2] the
// application's. Judging at a range's first index or at the commit index fails this.
void witness_promotion() {
	constexpr GroupId g = 81;
	fl::filesystem disk; Loop loop(disk);
	witness_case::promote(loop, g);
	witness_case::expect(loop, g, 1, 2, 6, "promoted at 4");
}

// The witness's removal at 7 is in force at the ends of [7..8] and [9..9]: both are the witness's
// own, so the application, which holds nothing past 2, receives neither.
void witness_removal() {
	constexpr GroupId g = 82;
	fl::filesystem disk; Loop loop(disk);
	witness_case::promote(loop, g);
	witness_case::append(loop, g, 5, 6, 6, {witness_case::configuration(7, witness_case::removed), witness_case::command(8)});
	witness_case::append(loop, g, 6, 8, 9, {witness_case::command(9)});
	loop.apply(g);
	auto s = loop.driver->state(g);
	check(s && !find_member(s->membership, 4), "node 4 is removed at 7");
	witness_case::expect(loop, g, 1, 2, 9, "removed at 7");
}

// After a restart from the witness's checkpoint at 6, its removal's range [7..7] is still its own.
void witness_removal_restart() {
	constexpr GroupId g = 83;
	fl::filesystem disk;
	{
		Loop loop(disk);
		witness_case::promote(loop, g);
		std::exception_ptr error; std::optional<CheckpointPublished> published;
		check(loop.settle(loop.driver->checkpoint(g, 1 << 20, Capture{"witness", 0, 6, 1}), published, error) && !error && published && published->through == 6,
			"the witness checkpoints at 6: " + what(error));
	}
	Loop loop(disk);
	loop.current = g;
	loop.driver->open(g, witness_case::group_options());
	std::exception_ptr error;
	check(loop.settle(loop.driver->recover(g, [](j::ArtifactReader&) {}), error) && !error, "recover: " + what(error));
	loop.apply(g);
	witness_case::append(loop, g, 7, 6, 7, {witness_case::configuration(7, witness_case::removed)});
	loop.apply(g);
	witness_case::expect(loop, g, 7, 0, 7, "removed at 7 after a restart at 6");
}

// A wake() from another thread ends the blocking entry's sleep toward an operation deadline
// within 50 ms (journal turn-seal.md): a journal group beside the Driver's, sealed on the same
// turn driver, gets its open record written and the work's wait returns, round after round. A
// deaf entry sleeps until its deadline's wheel pass; the first such round ends the case.
void entry_wake_threads() {
	constexpr int rounds = 20;
	constexpr std::uint64_t other = 77;
	constexpr auto budget = std::chrono::milliseconds(50);
	fl::filesystem disk;
	task::blocking_scheduler scheduler;
	j::InlineCompletion io;
	j::Host host(host_io(disk), host_identity, j::HostOptions{1, turn_options});
	Sink sink;
	Driver<Sink> driver(Seams{task::scheduler_ref::of(scheduler), j::completion_ref::of(io), host, 0, 7}, sink);
	auto& turns = driver.turns();
	struct Open { std::mutex mutex; std::string payload; std::uint64_t sequence = 0; } open;
	turns.seal(other, [&](std::uint64_t, bool) -> std::optional<j::TurnRecord> {
		std::lock_guard hold(open.mutex);
		if (open.payload.empty()) { return std::nullopt; }
		return j::TurnRecord{other, open.sequence, j::TurnRecordKind::Batch, std::exchange(open.payload, {})};
	});
	auto wait = [](task::blocking_scheduler& s, j::TurnDriver& t, std::uint64_t sequence, std::atomic<bool>& waiting) -> task::task<void> {
		task::deadline<task::blocking_scheduler> limit(s, std::chrono::steady_clock::now() + std::chrono::seconds(2));
		waiting.store(true, std::memory_order_release);
		co_await t.durable(other, sequence, limit.token());  // the sealer has nothing yet: the entry sleeps
	};
	int durable = 0;
	auto slowest = std::chrono::steady_clock::duration::zero();
	for (int round = 0; round < rounds; ++round) {
		const auto sequence = turns.sequence(other) + 1;
		std::atomic<bool> waiting{false};
		std::atomic<std::chrono::steady_clock::rep> woken{0};
		std::thread writer([&] {
			while (!waiting.load(std::memory_order_acquire)) { std::this_thread::yield(); }
			std::this_thread::sleep_for(std::chrono::milliseconds(2));
			std::lock_guard hold(open.mutex);
			open.sequence = sequence; open.payload = "w";
			woken.store(std::chrono::steady_clock::now().time_since_epoch().count(), std::memory_order_release);
			turns.wake();  // the record's first transaction, under its mutex
		});
		bool ok = true;
		try { run(driver, scheduler, wait(scheduler, turns, sequence, waiting)); ++durable; }
		catch (const std::exception& error) { ok = false; check(false, std::string("a woken round threw: ") + error.what()); }
		const auto returned = std::chrono::steady_clock::now();
		waiting.store(true, std::memory_order_release);  // a round that threw first still frees the writer
		writer.join();
		const auto latency = returned - std::chrono::steady_clock::time_point(std::chrono::steady_clock::duration(woken.load(std::memory_order_acquire)));
		slowest = std::max(slowest, latency);
		if (!ok || latency >= budget) { break; }
	}
	check(durable == rounds && turns.sequence(other) == std::uint64_t(rounds), "every woken record is durable (" + std::to_string(durable) + " of " + std::to_string(rounds) + ")");
	check(slowest < budget, "each wake ends the entry's sleep within 50 ms");
	std::cerr << " entry_wake_threads: slowest wake to return " << std::chrono::duration_cast<std::chrono::microseconds>(slowest).count() << " us\n";
}

const std::map<std::string, void (*)()> cases{
	{"lone_voter", &lone_voter},
	{"rows", &rows},
	{"checkpoints", &checkpoints},
	{"checkpoint_keeps_acknowledged", &checkpoint_keeps_acknowledged},
	{"close", &close_rows},
	{"fence", &fence_and_shutdown},
	{"sink_throws", &sink_throws},
	{"truncated_promotion", &truncated_promotion},
	{"witness_promotion", &witness_promotion},
	{"witness_removal", &witness_removal},
	{"witness_removal_restart", &witness_removal_restart},
	{"entry_wake_threads", &entry_wake_threads},
};

}  // namespace

int main(int argc, char** argv) {
	if (argc > 1) {
		auto found = cases.find(argv[1]);
		if (found == cases.end()) { std::cerr << "unknown case " << argv[1] << '\n'; return 2; }
		std::cerr << "case " << argv[1] << '\n';
		found->second();
	} else {
		for (const auto& [name, run] : cases) { std::cerr << "case " << name << '\n'; run(); }
	}
	std::cout << checks << " driver checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
