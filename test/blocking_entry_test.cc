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

// consensus::run, the blocking entry (docs/DRIVER.md; async-drivers D5), over a real
// blocking_scheduler and journal host on a memory disk: a lone voter's first proposal with no
// clock wait; `stalled` on a lost wake while the group's heartbeat is armed, also under a live
// token; `needs_peer` for a group with another voting member, a witness included; a proposal
// staged then cancelled is written before run returns; a cancel from another thread toward an
// operation deadline returns `cancelled`; the entry refuses a Driver its run() holds. Run it
// under TSan.
//   blocking_entry_test [case]
#include "consensus/driver.h"
#include "journal/memory_io.h"

#include <chrono>
#include <cstdio>
#include <functional>
#include <map>
#include <string>
#include <thread>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace jt = kronuz::journal::testing;

namespace {

int checks = 0, failures = 0;
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; std::fprintf(stderr, "FAIL: %s\n", what.c_str()); }
}
template <class E, class F> bool throws(F&& f) {
	try { f(); } catch (const E&) { return true; } catch (...) {}
	return false;
}

constexpr GroupId g = 9;
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 32, 3};

GroupOptions options(std::vector<NodeId> voters, std::optional<Membership> founding = {}) {
	GroupOptions o;
	o.configuration.local = 1; o.configuration.voters = std::move(voters);
	o.configuration.cluster[0] = 'B'; o.configuration.configuration[0] = 'E';
	o.founding = std::move(founding);
	o.admission = admission; o.timing = Timing{20, 40, 100, 200, 60000}; o.scheduling = WorkerLimits{16, 128};
	o.limits.command_bytes = 1024; o.limits.rpc_bytes = 4096; o.limits.log_bytes = 1 << 20; o.limits.log_entries = 4096; o.limits.control_entries = 4; o.limits.rpc_entries = 8;
	return o;
}

// The application: applies each committed range at once (a plain call, so the Sink may make it).
struct Sink {
	Driver<Sink>* driver = nullptr;
	std::vector<std::string> commands;
	void operator()(GroupId id, Committed c) {
		for (const auto& e : c.entries) { if (e.kind == EntryKind::Command) { commands.push_back(e.payload); } }
		if (driver && !c.entries.empty()) { driver->applied(id, Applied{c.entries.back().index}); }
	}
	template <class N> void operator()(GroupId, N&&) {}
};

struct Entry {
	jt::Disk disk;
	task::blocking_scheduler scheduler;
	j::InlineCompletion io;
	std::unique_ptr<j::Host> host;
	Sink sink;
	std::unique_ptr<Driver<Sink>> driver;
	Entry() {
		host = std::make_unique<j::Host>(std::make_shared<jt::MemoryIO>(disk, "host/"), j::Identity{'B', 'E'}, j::HostOptions{1, turn_options});
		driver = std::make_unique<Driver<Sink>>(Seams{task::scheduler_ref::of(scheduler), j::completion_ref::of(io), *host, 0, 3}, sink);
		sink.driver = driver.get();
	}
	template <class T> T run(task::task<T> work) { return kronuz::consensus::run(*driver, scheduler, std::move(work)); }
	// A created lone voter (or `voters`), opened on the Driver.
	void create(std::vector<NodeId> voters = {1}, std::optional<Membership> founding = {}) {
		run([](Entry& e, std::vector<NodeId> v, std::optional<Membership> f) -> task::task<void> {
			e.driver->open(g, options(std::move(v), std::move(f)));
			co_await e.driver->create(g, j::Identity{'G', '1'});
		}(*this, std::move(voters), std::move(founding)));
	}
};

void lone_voter() {
	Entry e;
	const auto start = std::chrono::steady_clock::now();
	e.create();
	auto placed = e.run([](Entry& e) -> task::task<Placement> { co_return co_await e.driver->propose(g, "first"); }(e));
	const auto elapsed = std::chrono::steady_clock::now() - start;
	check(placed.term == 1 && placed.index == 2, "a lone voter's first proposal is placed after its no-op");
	check(elapsed < std::chrono::milliseconds(100), "with no wait for an election timeout (" + std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()) + " ms)");
	e.run([](Entry& e, Placement p) -> task::task<void> { co_await e.driver->committed(g, p); }(e, placed));
	auto state = e.driver->state(g);
	check(state && state->role == Role::Leader && state->committed >= placed.index, "it leads and committed the proposal");
}

// Step 5: a wait nothing in the entry can complete throws, though the leader's heartbeat
// timer is armed on the scheduler's wheel.
void stalled_with_heartbeat() {
	Entry e;
	e.create();
	// A three-voter member beside it, waiting out its election: its timer stays armed.
	e.run([](Entry& e) -> task::task<void> {
		e.driver->open(g + 1, options({1, 2, 3}));
		co_await e.driver->create(g + 1, j::Identity{'G', '2'});
	}(e));
	check(e.scheduler.armed_timers() >= 1 && e.driver->armed_group_timers() >= 1, "a group's timer entry is armed");
	auto lost = [&](task::cancel_token token) {
		return [&, token] { e.run([](Entry& e, task::cancel_token t) -> task::task<void> { co_await e.driver->committed(g, Placement{1, 100}, t); }(e, token)); };
	};
	bool named = false;
	try { lost({})(); } catch (const stalled& error) { named = error.groups == std::vector<std::uint64_t>{g}; } catch (...) {}
	check(named, "a lost wake throws stalled naming the group, with a group timer armed");
	task::cancel_source live;
	check(throws<stalled>(lost(live.token())), "a lost wake under a live token nobody requests throws stalled");
	auto state = e.driver->state(g);
	check(state && state->role == Role::Leader, "the group is untouched");
}

// D5: calls that need another voting member fail at the call and submit nothing.
void needs_peer_rules() {
	{
		Entry e;
		e.create({1, 2, 3});
		check(throws<needs_peer>([&] { e.run([](Entry& e) -> task::task<Placement> { co_return co_await e.driver->propose(g, "x"); }(e)); }), "propose on a three-voter group throws needs_peer");
		check(throws<needs_peer>([&] { e.run([](Entry& e) -> task::task<ReadReady> { co_return co_await e.driver->read(g); }(e)); }), "read throws needs_peer");
		check(throws<needs_peer>([&] { e.run([](Entry& e) -> task::task<void> { co_await e.driver->committed(g, Placement{1, 1}); }(e)); }), "committed throws needs_peer");
		check(throws<needs_peer>([&] { e.run([](Entry& e) -> task::task<void> { co_await e.driver->transfer_leadership(g, 2); }(e)); }), "transfer_leadership throws needs_peer");
		check(e.sink.commands.empty(), "nothing was submitted");
	}
	{
		// A 2 + witness group: the other voting members include a witness.
		Entry e;
		e.create({1, 2, 3}, Membership{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 4, 0});
		check(throws<needs_peer>([&] { e.run([](Entry& e) -> task::task<Placement> { co_return co_await e.driver->propose(g, "x"); }(e)); }), "propose beside a witness throws needs_peer");
	}
}

// A proposal staged in the turn and then cancelled: the caller sees cancelled, and the record
// is written before run returns.
void staged_then_cancelled() {
	Entry e;
	e.create();
	const auto before = e.driver->turns().sequence(g);
	const bool cancelled = e.run([](Entry& e) -> task::task<bool> {
		task::cancel_source cancel;
		auto& turns = e.driver->turns();
		auto canceller = [](j::TurnDriver& turns, task::cancel_source& cancel) -> task::task<void> {
			while (turns.idle()) { co_await task::yield(); }
			cancel.request();
		};
		task::spawn(task::current(), canceller(turns, cancel), [](std::exception_ptr) {});
		try { co_await e.driver->propose(g, "staged", cancel.token()); } catch (const task::cancelled&) { co_return true; }
		co_return false;
	}(e));
	check(cancelled, "the staged proposal's caller sees cancelled");
	check(e.driver->turns().sequence(g) > before, "its record was written before run returned");
	e.run([](Entry& e) -> task::task<void> { co_await e.driver->propose(g, "after"); }(e));
	check(std::find(e.sink.commands.begin(), e.sink.commands.end(), "staged") != e.sink.commands.end(), "and it commits");
}

// Step 4: an operation deadline lets the entry sleep; a cancel from another thread cuts the
// sleep short and the call returns cancelled.
void cross_thread_cancel() {
	Entry e;
	e.create();
	task::cancel_source other;
	std::thread canceller([&] { std::this_thread::sleep_for(std::chrono::milliseconds(50)); other.request(); });
	const auto start = std::chrono::steady_clock::now();
	const bool cancelled = e.run([](Entry& e, task::cancel_token parent) -> task::task<bool> {
		task::deadline<task::blocking_scheduler> deadline(e.scheduler, std::chrono::steady_clock::now() + std::chrono::seconds(30), parent);
		try { co_await e.driver->committed(g, Placement{1, 100}, deadline.token()); } catch (const task::cancelled&) { co_return true; }
		co_return false;
	}(e, other.token()));
	canceller.join();
	const auto elapsed = std::chrono::steady_clock::now() - start;
	check(cancelled, "a cross-thread cancel toward a deadline returns cancelled");
	check(elapsed < std::chrono::seconds(10), "long before the deadline");
}

// The entry and the Driver's run() refuse each other.
void ownership() {
	Entry e;
	e.create();
	check(throws<std::logic_error>([&] { e.run([](Entry& e) -> task::task<void> { co_await e.driver->run(); }(e)); }), "run() inside the entry is refused");
	task::cancel_source stop;
	bool ended = false;
	task::spawn(task::scheduler_ref::of(e.scheduler), e.driver->run(stop.token()), [&](std::exception_ptr) { ended = true; });
	e.scheduler.drive([] { return true; }, [] { return false; });  // run() starts and parks
	check(throws<std::logic_error>([&] { e.run([](Entry& e) -> task::task<void> { co_await e.driver->propose(g, "x"); }(e)); }), "the entry refuses a Driver its run() holds");
	stop.request(task::cancel_reason::shutdown);
	e.scheduler.drive([&] { return ended; }, [] { return false; });
	check(ended, "run() ends at its stop");
}

const std::map<std::string, void (*)()> cases{
	{"lone_voter", &lone_voter},
	{"stalled", &stalled_with_heartbeat},
	{"needs_peer", &needs_peer_rules},
	{"staged_then_cancelled", &staged_then_cancelled},
	{"cross_thread_cancel", &cross_thread_cancel},
	{"ownership", &ownership},
};

}  // namespace

int main(int argc, char** argv) {
	if (argc > 1) {
		auto found = cases.find(argv[1]);
		if (found == cases.end()) { std::fprintf(stderr, "unknown case %s\n", argv[1]); return 2; }
		found->second();
	} else {
		for (const auto& [name, run] : cases) { std::fprintf(stderr, "case %s\n", name.c_str()); run(); }
	}
	std::printf("%d blocking entry checks, %d failures\n", checks, failures);
	return failures ? 1 : 0;
}
