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
// run() as roots, IO through InlineCompletion.
struct Loop {
	jt::Disk& disk;
	task::inline_scheduler scheduler;
	j::InlineCompletion inline_io;
	std::unique_ptr<j::Host> host;
	Sink sink;
	std::unique_ptr<Driver<Sink>> driver;
	task::cancel_source stop;
	bool turns_done = false, driver_done = false;
	explicit Loop(jt::Disk& d, std::uint64_t seed = 7) : disk(d) {
		host = std::make_unique<j::Host>(std::make_shared<jt::MemoryIO>(disk, "host/"), host_identity, j::HostOptions{1, turn_options});
		driver = std::make_unique<Driver<Sink>>(Seams{task::scheduler_ref::of(scheduler), j::completion_ref::of(inline_io), *host, 0, seed}, sink);
		auto ref = task::scheduler_ref::of(scheduler);
		task::spawn(ref, host->driver(0).run(j::completion_ref::of(inline_io), stop.token()), [this](std::exception_ptr) { turns_done = true; });
		task::spawn(ref, driver->run(stop.token()), [this](std::exception_ptr) { driver_done = true; });
		scheduler.run();
	}
	~Loop() { finish(); }
	void finish() {
		if (!driver) { return; }
		stop.request(task::cancel_reason::shutdown);
		scheduler.run();
		check(turns_done && driver_done, "the turn driver's and the Driver's runs end at shutdown");
		driver.reset(); host.reset();
	}
	// Runs `work` with no clock advance; false if it did not finish.
	template <class T> bool settle(task::task<T> work, std::optional<T>& out, std::exception_ptr& error) {
		bool done = false;
		task::spawn(task::scheduler_ref::of(scheduler), std::move(work), [&](std::exception_ptr e, std::optional<T> v) { done = true; error = e; out = std::move(v); });
		scheduler.run();
		return done;
	}
	bool settle(task::task<void> work, std::exception_ptr& error) {
		bool done = false;
		task::spawn(task::scheduler_ref::of(scheduler), std::move(work), [&](std::exception_ptr e) { done = true; error = e; });
		scheduler.run();
		return done;
	}
	void apply(GroupId g) {
		auto last = sink.last_committed(g);
		if (last) { driver->applied(g, Applied{last}); scheduler.run(); }
	}
};

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
		auto state = loop.driver->state(g);
		check(state && state->role == Role::Leader && state->committed >= first.index, "the lone voter leads and committed its proposal");
		check(!loop.sink.committed.empty() && loop.sink.last_committed(g) == first.index, "the Sink receives the committed range");
		loop.apply(g);
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

const std::map<std::string, void (*)()> cases{
	{"lone_voter", &lone_voter},
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
