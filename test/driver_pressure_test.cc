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

// A Tick the Worker refuses with Pressure re-arms the group's wheel entry once the pressure can
// have cleared, never at Core's past clock (docs/mechanisms/deadlines.md, "What the Driver
// re-arms"). A two-voter follower's election deadline falls due while a Control permit holds
// the group's pool; the manual clock moves in wheel passes and the scheduler records each arm
// of the group's entry. The test reaches into the Driver and the Worker's Store to hold and
// release that permit (-fno-access-control, set in CMakeLists.txt). `driver_pressure_test`.
#include "consensus/driver.h"
#include "faultlab/faultfs.h"

#include <algorithm>
#include <cstdio>
#include <functional>
#include <map>
#include <optional>
#include <iostream>
#include <string>
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

// The manual clock's scheduler, which records every arm: the time it was asked for and the clock then.
struct CountingScheduler : task::inline_scheduler {
	struct Arm { const wheel::timer_entry* entry; std::uint64_t at, now; };
	std::vector<Arm> arms;
	void arm(wheel::timer_entry& e, std::uint64_t at) { arms.push_back({&e, at, now()}); task::inline_scheduler::arm(e, at); }
};

std::shared_ptr<j::IO> host_io(fl::filesystem& disk) {
	const auto names = disk.list();
	if (std::find(names.begin(), names.end(), "host") == names.end()) { disk.make_directory("host"); }
	return std::make_shared<fl::io>(disk, "host");
}

constexpr Timing compact{20, 40, 100, 200, 60000};
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::Identity host_identity{'H', 'O', 'S', 'T'};

Limits limits() { Limits l; l.command_bytes = 1024; l.rpc_bytes = 4096; l.log_bytes = 64 * 1024; l.log_entries = 64; l.control_entries = 4; l.rpc_entries = 8; return l; }

const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 5, 3};
GroupOptions options(std::vector<NodeId> voters) {
	GroupOptions o;
	o.configuration.local = 1; o.configuration.voters = std::move(voters);
	o.configuration.cluster[0] = 'C'; o.configuration.configuration[0] = 'G';
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
	CountingScheduler scheduler;
	j::InlineCompletion inline_io;
	j::HeldCompletion held;
	std::unique_ptr<j::Host> host;
	Sink sink;
	std::unique_ptr<Driver<Sink>> driver;
	task::cancel_source stop;
	bool turns_done = false, driver_done = false;
	explicit Loop(fl::filesystem& d, bool hold_turns = false, bool hold_io = false, std::uint64_t seed = 7, j::TurnLogOptions log_options = turn_options) : disk(d) {
		host = std::make_unique<j::Host>(host_io(disk), host_identity, j::HostOptions{1, log_options});
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
};


std::size_t pre_votes(const Sink& sink, GroupId g) {
	std::size_t n = 0;
	for (const auto& [id, send] : sink.sent) { n += id == g && std::holds_alternative<PreVoteRequest>(send.message); }
	return n;
}

constexpr GroupId g = 31;
constexpr std::uint64_t pass_ns = 100'000, retry_ns = 1'000'000;  // a wheel pass of the test's clock; the Driver's retry

// A two-voter follower whose election deadline is about to fall due, with every Control ticket
// but the last held: the Worker's first reservation for the Tick's campaign pack fails.
struct Pressured {
	fl::filesystem disk;
	Loop loop{disk};
	std::vector<std::optional<j::AppendReservation>> held;
	std::uint64_t due = 0;  // Core's deadline, in ns
	Driver<Sink>::Group& group() { return *loop.driver->groups_.at(g); }
	Pressured() {
		loop.current = g;
		loop.driver->open(g, options({1, 2}));
		std::exception_ptr error;
		check(loop.settle(loop.driver->create(g, j::Identity{'P', 'R'}), error) && !error, "create");
		auto state = loop.driver->state(g);
		check(state && state->role == Role::Follower, "a two-voter group's member follows");
		// Four tickets are the Control class's cap (maximum_tickets - 1).
		for (int n = 0; n < 4; ++n) { held.push_back(group().worker->store_.reserve_append(j::AdmissionClass::Control, storage_batch_size(true, false))); }
		check(std::all_of(held.begin(), held.end(), [](const auto& permit) { return bool(permit); }), "the test holds four Control permits");
		due = group().worker->next_deadline() * 1'000'000;
	}
	// Wheel passes of `pass_ns` through `span` ns, each run to rest.
	void pass(std::uint64_t span) { for (std::uint64_t at = 0; at < span; at += pass_ns) { loop.scheduler.advance(pass_ns); loop.run(); } }
	std::size_t pre_votes() const { return ::pre_votes(loop.sink, g); }
};

// A1: the Tick is refused with Pressure from its deadline for 100 ms of 0.1 ms wheel passes. The
// group's entry is re-armed once per retry_ns, never at or before the clock (at main: once per
// pass, always in the past), and Core's deadline stays where it was.
void hold() {
	Pressured f;
	f.loop.scheduler.advance(f.due - f.loop.scheduler.now()); f.loop.run();  // the deadline: the first refusal
	const auto base = f.loop.scheduler.arms.size();
	const auto begin = f.loop.scheduler.now();
	f.pass(100'000'000);
	const auto elapsed = f.loop.scheduler.now() - begin;
	std::size_t arms = 0, past = 0;
	for (auto arm = f.loop.scheduler.arms.begin() + base; arm != f.loop.scheduler.arms.end(); ++arm) {
		if (arm->entry != &f.group()) { continue; }
		++arms; past += arm->at <= arm->now;
	}
	check(f.pre_votes() == 0, "the refused Tick pre-votes for nobody");
	check(f.group().worker->next_deadline() * 1'000'000 == f.due, "Core's deadline is where it was");
	check(past == 0, "no arm at or before the clock: " + std::to_string(past));
	check(arms <= elapsed / retry_ns + 1, "at most one step per retry_ns for " + std::to_string(elapsed) + " ns: " + std::to_string(arms));
	check(arms >= elapsed / retry_ns - 1, "and the group still retries: " + std::to_string(arms));
}

// A2: the pressure clears after a hold; the refused Tick runs within one retry_ns and its
// action, the pre-vote, happens once.
void clears() {
	Pressured f;
	f.loop.scheduler.advance(f.due - f.loop.scheduler.now()); f.loop.run();
	f.pass(20'000'000);
	check(f.pre_votes() == 0, "no pre-vote while the pressure holds");
	f.held.clear();
	f.loop.scheduler.advance(retry_ns); f.loop.run();
	check(f.pre_votes() == 1, "within one retry_ns of the release the refused Tick pre-votes: " + std::to_string(f.pre_votes()));
}

}  // namespace

int main(int argc, char** argv) {
	const std::map<std::string, void (*)()> cases{{"hold", hold}, {"clears", clears}};
	if (argc > 1) {
		auto found = cases.find(argv[1]);
		if (found == cases.end()) { std::cerr << "unknown case " << argv[1] << '\n'; return 2; }
		found->second();
	} else {
		for (const auto& [name, run] : cases) { std::cerr << "case " << name << '\n'; run(); }
	}
	std::cout << checks << " driver pressure checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
