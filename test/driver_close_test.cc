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

// Driver::close(id) on a row that is open, fenced, half-opened (opened, never created) or
// unknown: it completes, the Driver erases the row and returns its bytes, and the id opens
// again. `driver_close_test [case]`; no case runs them all.
#include "consensus/driver.h"
#include "journal/memory_io.h"

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <map>
#include <new>
#include <string>
#include <vector>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace jt = kronuz::journal::testing;

// Live heap bytes, counted at the allocation and the sized free, each block carrying its size.
// Single-threaded: the loop is an inline scheduler. TSan's runtime defines operator new, so
// there the count is off and the bytes case only reports.
#if defined(__SANITIZE_THREAD__)
#define COUNT_BYTES 0
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define COUNT_BYTES 0
#endif
#endif
#ifndef COUNT_BYTES
#define COUNT_BYTES 1
#endif
#if COUNT_BYTES
namespace {
std::size_t live_bytes = 0;
constexpr std::size_t header = alignof(std::max_align_t);
}
void* operator new(std::size_t n) {
	void* raw = std::malloc(n + header);
	if (!raw) { throw std::bad_alloc(); }
	*static_cast<std::size_t*>(raw) = n; live_bytes += n;
	return static_cast<char*>(raw) + header;
}
void operator delete(void* p) noexcept {
	if (!p) { return; }
	void* raw = static_cast<char*>(p) - header;
	live_bytes -= *static_cast<std::size_t*>(raw);
	std::free(raw);
}
void operator delete(void* p, std::size_t) noexcept { operator delete(p); }
#else
namespace { std::size_t live_bytes = 0; }
#endif

namespace {

int checks = 0, failures = 0;
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; if (failures < 40) { std::cerr << "FAIL: " << what << '\n'; } }
}

constexpr Timing compact{20, 40, 100, 200, 60000};
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 32, 3};
const j::Identity host_identity{'H', 'O', 'S', 'T'};

Limits limits() { Limits l; l.command_bytes = 1024; l.rpc_bytes = 4096; l.log_bytes = 64 * 1024; l.log_entries = 64; l.control_entries = 4; l.rpc_entries = 8; return l; }
GroupOptions options(std::vector<NodeId> voters, NodeId local = 1) {
	GroupOptions o;
	o.configuration.local = local; o.configuration.voters = std::move(voters);
	o.configuration.cluster[0] = 'C'; o.configuration.configuration[0] = 'G';
	o.admission = admission; o.limits = limits(); o.timing = compact; o.scheduling = WorkerLimits{16, 128};
	return o;
}

struct Sink {
	std::size_t fenced = 0;
	void operator()(GroupId, Send) {}
	void operator()(GroupId, Committed) {}
	void operator()(GroupId, RoleChanged) {}
	void operator()(GroupId, ActivateInstall) {}
	void operator()(GroupId, CheckpointPublished) {}
	void operator()(GroupId, RepairCompleted) {}
	void operator()(GroupId, Fenced) { ++fenced; }
	void operator()(GroupId, SnapshotNeeded) {}
	void operator()(GroupId, SnapshotTransmit) {}
	void operator()(GroupId, SnapshotReleased) {}
};
static_assert(sink<Sink>);

// One host and one loop on an inline_scheduler; the Worker's IO and the turns go through
// InlineCompletion, or through a HeldCompletion the case completes by hand.
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
	explicit Loop(jt::Disk& d, bool hold = false) : disk(d) {
		host = std::make_unique<j::Host>(std::make_shared<jt::MemoryIO>(disk, "host/"), host_identity, j::HostOptions{1, turn_options});
		auto io = hold ? j::completion_ref::of(held) : j::completion_ref::of(inline_io);
		driver = std::make_unique<Driver<Sink>>(Seams{task::scheduler_ref::of(scheduler), io, *host, 0, 7}, sink);
		auto ref = task::scheduler_ref::of(scheduler);
		auto turns = hold ? j::completion_ref::of(held) : j::completion_ref::of(inline_io);
		task::spawn(ref, host->driver(0).run(turns, stop.token()), [this](std::exception_ptr) { turns_done = true; });
		task::spawn(ref, driver->run(stop.token()), [this](std::exception_ptr) { driver_done = true; });
		run();
	}
	~Loop() { finish(); }
	void run() {
		std::size_t budget = 10'000'000;
		scheduler.run_until([&] { return --budget == 0; });
		if (!budget) { check(false, "the loop spins"); std::exit(1); }
	}
	void finish() {
		if (!driver) { return; }
		stop.request(task::cancel_reason::shutdown);
		release();
		check(turns_done && driver_done, "the turn driver's and the Driver's runs end at shutdown");
		driver.reset(); host.reset();
	}
	void release() {
		run();
		for (int n = 0; held.held(); ++n) {
			if (n == 100000) { check(false, "held IO never runs out"); std::exit(1); }
			held.complete(held.tickets().front()); run();
		}
	}
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
	bool settle(task::task<void> work, std::exception_ptr& error) {
		auto state = start(std::move(work));
		release();
		error = state->error;
		return state->done;
	}
	// A lone voter, created and leading.
	void lone(GroupId g) {
		driver->open(g, options({1}));
		std::exception_ptr error;
		check(settle(driver->create(g, j::Identity{'L', static_cast<char>(g)}), error) && !error, "create");
	}
	// close(g) to completion: whether it completed, and how.
	bool close(GroupId g, std::exception_ptr& error) { return settle(driver->close(g), error); }
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

// A1: a group fenced by its application, in a live process, closes; the id opens again.
void fenced_row() {
	constexpr GroupId g = 11;
	jt::Disk disk; Loop loop(disk);
	loop.lone(g);
	auto read = loop.start(loop.driver->read(g));
	loop.driver->application_failed(g, 1, "the application failed");
	loop.run();
	check(read->done && is<fenced>(read->error), "a waiter on the fenced group completes with fenced");
	check(loop.sink.fenced == 1, "the Sink heard Fenced");
	check(loop.driver->groups() == 1, "the fenced row is held");
	std::exception_ptr error;
	check(loop.close(g, error) && !error, "close completes on a fenced row: " + what(error));
	check(loop.driver->groups() == 0 && !loop.driver->state(g), "the fenced row is erased");
	auto refused = loop.start(loop.driver->read(g));
	check(refused->done && is<closed>(refused->error), "a call on the erased id completes with closed");
	loop.driver->open(g, options({1}));
	check(loop.driver->groups() == 1, "the id opens again");
	// The host refuses a second recover of an id until it reopens: a journal guard, not Driver state.
	check(loop.close(g, error) && !error && loop.driver->groups() == 0, "and closes again");
}

// A1: a Sink that throws mid-batch fences the row with a proposal's IO in flight; close still ends.
void fenced_in_flight() {
	constexpr GroupId g = 12;
	jt::Disk disk; Loop loop(disk, true);
	loop.lone(g);
	auto proposal = loop.start(loop.driver->propose(g, "staged"));
	check(!proposal->done && loop.held.held() > 0, "the proposal's record waits in a held turn");
	loop.driver->application_failed(g, 1, "the application failed");
	loop.run();
	auto closing = loop.start(loop.driver->close(g));
	loop.release();
	check(closing->done && !closing->error, "close completes with a record staged on a fenced row: " + what(closing->error));
	check(proposal->done, "the staged proposal's caller completed");
	check(loop.driver->groups() == 0, "the row is erased");
}

// A2: open succeeded and the caller then failed: no create, no records.
void half_opened_row() {
	constexpr GroupId g = 21;
	jt::Disk disk; Loop loop(disk);
	loop.driver->open(g, options({1}));
	check(loop.driver->groups() == 1, "the row is held");
	std::exception_ptr error;
	check(loop.close(g, error) && !error, "close completes on a row never created: " + what(error));
	check(loop.driver->groups() == 0 && !loop.driver->state(g), "the row is erased");
	loop.driver->open(g, options({1}));
	check(loop.settle(loop.driver->create(g, j::Identity{'H', 'O'}), error) && !error, "the id opens and creates again: " + what(error));
	check(loop.driver->state(g) && loop.driver->state(g)->role == Role::Leader, "and leads");
}

// A2: a create that failed (the group cancelled before it ran) leaves a row that closes.
void failed_create() {
	constexpr GroupId g = 22;
	jt::Disk disk; Loop loop(disk);
	loop.driver->open(g, options({1}));
	task::cancel_source cancel; cancel.request();
	std::exception_ptr error;
	check(loop.settle(loop.driver->create(g, j::Identity{'F', 'C'}, cancel.token()), error) && is<task::cancelled>(error), "a cancelled create completes with task::cancelled");
	check(loop.close(g, error) && !error && loop.driver->groups() == 0, "close frees the row: " + what(error));
}

// An id the Driver never held: close completes at once.
void unknown_row() {
	jt::Disk disk; Loop loop(disk);
	std::exception_ptr error;
	check(loop.close(31, error) && !error && loop.driver->groups() == 0, "close of an unknown id completes: " + what(error));
	loop.lone(32);
	check(loop.close(32, error) && !error, "close of an open row completes: " + what(error));
	check(loop.close(32, error) && !error, "and again, once erased");
	loop.driver->open(32, options({1}));
	check(loop.driver->groups() == 1, "the id opens again");
	bool threw = false;
	try { loop.driver->open(32, options({1})); } catch (const std::logic_error&) { threw = true; }
	check(threw, "an open row refuses a second open");
}

// A4: a close with a call waiting and IO in flight completes it and then the row, once the IO lands.
void close_in_flight() {
	constexpr GroupId g = 41;
	jt::Disk disk; Loop loop(disk, true);
	loop.lone(g);
	auto staged = loop.start(loop.driver->propose(g, "staged"));
	auto read = loop.start(loop.driver->read(g));
	check(loop.held.held() > 0 && !staged->done && !read->done, "a record staged, a read waiting");
	auto closing = loop.start(loop.driver->close(g));
	check(read->done && is<closed>(read->error), "the read completes with closed");
	check(!closing->done && loop.driver->groups() == 1, "close waits for the staged record");
	loop.release();
	check(staged->done && !staged->error, "the staged proposal completes with its placement");
	check(closing->done && !closing->error, "close completes once it is durable");
	check(loop.driver->groups() == 0 && loop.driver->in_flight() == 0, "the row is erased with no IO in flight");
}

// A3: 10,000 rows opened and closed leave the Driver's heap where it was.
void bytes() {
	constexpr GroupId rows = 10'000;
	jt::Disk disk; Loop loop(disk);
	std::exception_ptr error;
	// One warm round, so one-time allocations do not count.
	loop.driver->open(1, options({1}));
	check(loop.close(1, error) && !error, "warm close");
	const auto before = live_bytes;
	for (GroupId g = 1; g <= rows; ++g) { loop.driver->open(g, options({1})); }
	const auto opened = live_bytes;
	check(loop.driver->groups() == rows, "10,000 rows held");
	for (GroupId g = 1; g <= rows; ++g) {
		auto closing = loop.start(loop.driver->close(g));
		if (!closing->done || closing->error) { check(false, "close " + std::to_string(g) + " completes: " + what(closing->error)); break; }
	}
	check(loop.driver->groups() == 0, "no row held after the closes");
	// The Host keeps an expired weak entry per group it handed an IO, until its next group_io
	// call purges them: opening one more row is that call.
	const auto unpurged = live_bytes;
	loop.driver->open(rows + 1, options({1}));
	check(loop.close(rows + 1, error) && !error, "the purging row closes");
	const auto after = live_bytes;
	std::printf("per row: %zu bytes held open, %zu left after close before the Host's purge, %zu after\n", (opened - before) / std::size_t(rows),
		unpurged > before ? (unpurged - before) / std::size_t(rows) : std::size_t(0), after > before ? (after - before) / std::size_t(rows) : std::size_t(0));
	// What stays is the row table's bucket array, which an unordered_map never shrinks: a pointer a row.
	check(!COUNT_BYTES || after <= before + rows * sizeof(void*) * 2, "closed rows return their bytes: " + std::to_string(after) + " against " + std::to_string(before));
}

const std::map<std::string, void (*)()> cases{
	{"fenced_row", &fenced_row},
	{"fenced_in_flight", &fenced_in_flight},
	{"half_opened_row", &half_opened_row},
	{"failed_create", &failed_create},
	{"unknown_row", &unknown_row},
	{"close_in_flight", &close_in_flight},
	{"bytes", &bytes},
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
	std::cout << checks << " driver close checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
