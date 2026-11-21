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

// The Driver's waiter protocol (docs/DRIVER.md, "Waiters"; async-drivers D8) explored over
// every interleaving: the Driver and its group on loop A, callers on loop B, a token's cancel
// from a third thread, and close, the application's `applied`, the transport's `receive` and
// teardown injected at any point. Each post from one loop to another is also a scheduling
// point: the target may run what it has queued before the poster's next statement. Every run
// checks each caller completes exactly once, with a result its D8 row allows, close frees the
// group's state, and after teardown no waiter stays linked; ASan checks no frame is touched
// after it is freed. test/driver_mutants.py seeds the faults this must catch.
//   waiter_test [case [limit]]   rows-propose, rows-committed, rows-read, rows-propose-pair, close,
//                                close-read, close-open, close-late, teardown, teardown-close, threads
#include "consensus/driver.h"
#include "faultlab/faultfs.h"

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <mutex>
#include <random>
#include <functional>
#include <map>
#include <string>
#include <thread>
#include <vector>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace fl = kronuz::faultlab;

namespace {

std::uint64_t runs = 0, failures = 0;
std::string schedule;
void fail(const std::string& what) {
	if (failures++ < 10) { std::fprintf(stderr, "FAIL (run %llu, schedule %s): %s\n", (unsigned long long)runs, schedule.c_str(), what.c_str()); }
}

// Stateless exploration: each run follows a prefix of choices, then takes the first option;
// next() backtracks to the deepest choice with an option left. Sampling, each choice is drawn
// instead, so a bounded run also varies its early choices.
struct Explorer {
	std::vector<unsigned> prefix, taken, widths;
	std::mt19937_64 random{1};
	bool sampling = false;
	unsigned choose(unsigned n) {
		if (n <= 1) { return 0; }
		const auto at = taken.size();
		const unsigned c = sampling ? static_cast<unsigned>(random() % n) : at < prefix.size() ? prefix[at] : 0;
		taken.push_back(c); widths.push_back(n);
		return c;
	}
	bool next() {
		while (!taken.empty() && taken.back() + 1 >= widths.back()) { taken.pop_back(); widths.pop_back(); }
		if (taken.empty()) { return false; }
		prefix = taken; ++prefix.back();
		taken.clear(); widths.clear();
		return true;
	}
	std::string trace() const {
		std::string s;
		for (auto c : taken) { s += char('0' + c); }
		return s;
	}
};

// A loop the explorer steps by hand. A post from another running loop is a scheduling point:
// the explorer may run everything this loop has queued, up to the new call, before the poster
// goes on, as this loop's own thread could.
struct manual_loop {
	Explorer* explorer = nullptr;
	std::deque<std::pair<void (*)(void*), void*>> queue;
	task::root_list roots_;
	bool closed = false, running = false;
	void post(std::coroutine_handle<> h) { push(&resume, h.address()); }
	void post_call(void (*f)(void*), void* a) { push(f, a); }
	std::uint64_t now() const noexcept { return 0; }
	task::root_list* roots() noexcept { return &roots_; }
	static void resume(void* a) { std::coroutine_handle<>::from_address(a).resume(); }
	void push(void (*f)(void*), void* a) {
		if (closed) { return; }
		queue.emplace_back(f, a);
		const auto from = task::current();
		if (explorer && !running && from && !(from == task::scheduler_ref::of(*this)) && explorer->choose(2) == 1) {
			for (auto n = queue.size(); n; --n) { step(); }
		}
	}
	void step() {
		if (queue.empty()) { return; }
		auto [f, a] = queue.front();
		queue.pop_front();
		const bool was = std::exchange(running, true);
		{
			task::running_on scope(task::scheduler_ref::of(*this));
			f(a);
		}
		running = was;
	}
	void drain() { while (!queue.empty()) { step(); } }
};

constexpr GroupId g = 5;
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 32, 3};

GroupOptions options() {
	GroupOptions o;
	o.configuration.local = 1; o.configuration.voters = {1};
	o.configuration.cluster[0] = 'W'; o.configuration.configuration[0] = 'T';
	o.admission = admission; o.timing = Timing{20, 40, 100, 200, 60000}; o.scheduling = WorkerLimits{16, 128};
	o.limits.command_bytes = 1024; o.limits.rpc_bytes = 4096; o.limits.log_bytes = 1 << 20; o.limits.log_entries = 4096; o.limits.control_entries = 4; o.limits.rpc_entries = 8;
	return o;
}

struct Sink {
	std::optional<Index> pending;  // the Committed range the application has not acknowledged
	std::vector<std::string> commands;
	void operator()(GroupId, Committed c) {
		for (const auto& e : c.entries) { if (e.kind == EntryKind::Command) { commands.push_back(e.payload); } }
		pending = c.entries.back().index;
	}
	template <class N> void operator()(GroupId, N&&) {}
};

enum class Kind { propose, committed, read, create };
struct Caller {
	Kind kind;
	bool token = true;
	std::string command;
	task::cancel_source cancel;
	int completions = 0;
	std::exception_ptr error;
	bool value = false;
	Placement placement;   // propose's result
	Index read_index = 0;  // read's result
};

template <class E> bool is(std::exception_ptr error) {
	if (!error) { return false; }
	try { std::rethrow_exception(error); } catch (const E&) { return true; } catch (...) {}
	return false;
}

struct Case {
	std::vector<Kind> callers;
	bool close = false, teardown = false, apply = false, late = false, opened = true, cancel = true;
	bool untokened_first = false;  // caller 0 waits with no token: only close can end its wait
};

// One run of a case under the explorer's choices.
struct World {
	fl::filesystem disk;
	manual_loop a, b;
	j::InlineCompletion io;
	std::unique_ptr<j::Host> host;
	Sink sink;
	std::unique_ptr<Driver<Sink>> driver;
	task::cancel_source stop;
	std::vector<std::unique_ptr<Caller>> callers;
	bool closed_done = false, turns_done = false, driver_done = false;

	World() {
		disk.make_directory("host");
		host = std::make_unique<j::Host>(std::make_shared<fl::io>(disk, "host"), j::Identity{'W', 'T'}, j::HostOptions{1, turn_options});
		driver = std::make_unique<Driver<Sink>>(Seams{task::scheduler_ref::of(a), j::completion_ref::of(io), *host, 0, 7}, sink);
		task::spawn(task::scheduler_ref::of(a), host->driver(0).run(j::completion_ref::of(io), stop.token()), [this](std::exception_ptr) { turns_done = true; });
		task::spawn(task::scheduler_ref::of(a), driver->run(stop.token()), [this](std::exception_ptr) { driver_done = true; });
		a.drain();
	}
	// Setup, not explored: a created lone voter that applied its no-op, with "p0" committed and
	// its range not yet acknowledged, so a read waits for the application.
	void setup(bool created) {
		driver->open(g, options());
		if (!created) { return; }
		bool done = false;
		task::spawn(task::scheduler_ref::of(a), driver->create(g, j::Identity{'G', '1'}), [&](std::exception_ptr e) { done = !e; });
		a.drain();
		apply();
		task::spawn(task::scheduler_ref::of(a), driver->propose(g, "p0"), [&](std::exception_ptr e, std::optional<Placement>) { done = done && !e; });
		a.drain();
		if (!done || !sink.pending) { fail("setup"); }
	}
	// The application's acknowledgement, a plain call on loop A.
	void applied() {
		if (sink.pending) { driver->applied(g, Applied{*std::exchange(sink.pending, std::nullopt)}); }
	}
	void apply() { applied(); a.drain(); }
	task::task<void> call(Caller& c) {
		auto token = c.token ? c.cancel.token() : task::cancel_token{};
		switch (c.kind) {
		case Kind::propose: c.placement = co_await driver->propose(g, c.command, token); break;
		case Kind::committed: co_await driver->committed(g, Placement{1, 100}, token); break;
		case Kind::read: c.read_index = (co_await driver->read(g, token)).index; break;
		case Kind::create: co_await driver->create(g, j::Identity{'G', '1'}, token); break;
		}
		c.value = true;
	}
	void start(Caller& c) {
		task::spawn(task::scheduler_ref::of(b), call(c), [&c](std::exception_ptr e) { ++c.completions; c.error = e; });
	}
	// Phases 1-4 of teardown: the loops stop, their queues and later posts are dropped, the
	// unresumed frames are destroyed (each waiter unlinks itself), and then the Driver goes.
	void teardown() {
		a.explorer = b.explorer = nullptr;
		a.closed = b.closed = true; a.queue.clear(); b.queue.clear();
		a.roots_.close(); b.roots_.close();
		b.roots_.destroy_all(); a.roots_.destroy_all();
		if (!driver->waiting_groups().empty()) { fail("a destroyed frame's waiter stays linked"); }
		driver.reset(); host.reset();
	}
	// The normal end: shutdown, then every caller has completed once.
	void finish() {
		a.explorer = b.explorer = nullptr;
		stop.request(task::cancel_reason::shutdown);
		for (int round = 0; round < 1000 && !(a.queue.empty() && b.queue.empty() && turns_done && driver_done); ++round) { a.drain(); b.drain(); }
		if (!turns_done || !driver_done) { fail("the loops did not stop at shutdown"); }
		driver.reset(); host.reset();
	}
};

void run_case(const Case& spec, Explorer& explorer) {
	World w;
	w.setup(spec.opened);
	for (std::size_t i = 0; i < spec.callers.size(); ++i) {
		auto c = std::make_unique<Caller>();
		c->kind = spec.callers[i]; c->command = "x" + std::to_string(i);
		c->token = !(spec.untokened_first && i == 0);
		w.callers.push_back(std::move(c));
	}
	w.a.explorer = w.b.explorer = &explorer;
	for (auto& c : w.callers) { w.start(*c); }
	bool cancelled = !spec.cancel, closing = !spec.close, torn = false, late = !spec.late;
	int applies = spec.apply ? 1 : 0;
	enum Action { step_a, step_b, cancel, close, apply, late_inputs, tear };
	for (;;) {
		std::vector<Action> enabled;
		if (!w.a.queue.empty()) { enabled.push_back(step_a); }
		if (!w.b.queue.empty()) { enabled.push_back(step_b); }
		if (!cancelled) { enabled.push_back(cancel); }
		if (!closing) { enabled.push_back(close); }
		if (applies && w.sink.pending) { enabled.push_back(apply); }
		if (!late) { enabled.push_back(late_inputs); }
		if (spec.teardown && !torn) { enabled.push_back(tear); }
		if (enabled.empty()) { break; }
		switch (enabled[explorer.choose(static_cast<unsigned>(enabled.size()))]) {
		case step_a: w.a.step(); break;
		case step_b: w.b.step(); break;
		case cancel: cancelled = true; for (auto& c : w.callers) { if (c->token) { c->cancel.request(); break; } } break;
		case close:
			closing = true;
			task::spawn(task::scheduler_ref::of(w.a), w.driver->close(g), [&w](std::exception_ptr e) { w.closed_done = !e; });
			break;
		case apply:
			--applies;
			w.a.post_call([](void* p) { static_cast<World*>(p)->applied(); }, &w);
			break;
		case late_inputs:
			late = true;
			w.a.post_call([](void* p) {
				auto& world = *static_cast<World*>(p);
				world.driver->receive(g, Receive{2, VoteRequest{0, 0, 0}});
			}, &w);
			break;
		case tear:
			torn = true;
			w.teardown();
			break;
		}
		if (torn) { break; }
	}
	if (torn) {
		for (auto& c : w.callers) { if (c->completions > 1) { fail("a caller completed twice"); } }
		return;
	}
	// The application catches up, so every committed command reaches the Sink.
	w.a.explorer = w.b.explorer = nullptr;
	if (!spec.close) { for (int n = 0; n < 100 && w.sink.pending; ++n) { w.apply(); w.b.drain(); } }
	if (spec.close && !w.closed_done) { fail("close did not complete"); }
	if (spec.close && w.driver->state(g)) { fail("close left the group's state"); }
	if (spec.close) {
		const auto dropped = w.driver->dropped(g);
		w.driver->receive(g, Receive{2, VoteRequest{0, 0, 0}});
		if (w.driver->dropped(g) != dropped + 1) { fail("a closed group does not drop and count a late receive"); }
	}
	for (std::size_t i = 0; i < w.callers.size(); ++i) {
		auto& c = *w.callers[i];
		if (c.completions != 1) { fail("caller " + std::to_string(i) + " completed " + std::to_string(c.completions) + " times before shutdown"); continue; }
		if (c.value) {
			// A result is only what its D8 row completes with: a placement, a confirmed read; no
			// run commits index 100.
			if (c.kind == Kind::propose && c.placement.index == 0) { fail("a proposal completed with no placement"); }
			if (c.kind == Kind::read && c.read_index == 0) { fail("a read completed with no read index"); }
			if (c.kind == Kind::committed) { fail("committed completed below its index"); }
			if (c.kind == Kind::propose && std::find(w.sink.commands.begin(), w.sink.commands.end(), c.command) == w.sink.commands.end() && !spec.close) {
				fail("a placed proposal never committed");
			}
			continue;
		}
		if (is<task::cancelled>(c.error)) { if (!c.token) { fail("a caller with no token was cancelled"); } continue; }
		if (is<closed>(c.error)) {
			if (!spec.close) { fail("closed without a close"); }
			if (c.kind == Kind::propose && std::find(w.sink.commands.begin(), w.sink.commands.end(), c.command) != w.sink.commands.end()) { fail("a proposal completed with closed was committed"); }
			continue;
		}
		if (c.kind == Kind::committed && is<superseded>(c.error)) { continue; }
		std::string what = "unknown";
		try { std::rethrow_exception(c.error); } catch (const std::exception& e) { what = e.what(); } catch (...) {}
		fail("caller " + std::to_string(i) + " completed with " + what);
	}
	w.finish();
}

// Explores the case's schedules in order; past half the limit with schedules left, the rest
// of the limit samples schedules at random (seeded, so a failure's schedule replays).
std::uint64_t explore(const char* name, const Case& spec, std::uint64_t limit = 2'000'000) {
	Explorer explorer;
	std::uint64_t count = 0, sampled = 0;
	bool more = true;
	while (more && count < limit) {
		++runs; ++count;
		schedule = explorer.trace();
		run_case(spec, explorer);
		schedule = explorer.trace();
		if (failures) { std::fprintf(stderr, "%s: failed at schedule %s\n", name, schedule.c_str()); break; }
		if (explorer.sampling) { ++sampled; }
		if (explorer.sampling || count >= limit / 2) { explorer.sampling = true; explorer.taken.clear(); explorer.widths.clear(); }
		else { more = explorer.next(); }
	}
	std::printf("%s: %llu schedules%s\n", name, (unsigned long long)count, sampled ? (" (" + std::to_string(count - sampled) + " in order, " + std::to_string(sampled) + " sampled)").c_str() : "");
	return count;
}

// Close racing a cancel and teardown on real threads: the loop on one thread, the callers'
// loop on another, cancels from a third (run under TSan).
struct thread_loop {
	std::mutex m;
	std::condition_variable ready;
	std::deque<std::pair<void (*)(void*), void*>> queue;
	task::root_list roots_;
	bool stopping = false, closed = false;
	std::thread thread;
	thread_loop() : thread([this] { run(); }) {}
	~thread_loop() { stop(); }
	void post(std::coroutine_handle<> h) { post_call(&manual_loop::resume, h.address()); }
	void post_call(void (*f)(void*), void* a) {
		std::lock_guard lock(m);
		if (closed) { return; }
		queue.emplace_back(f, a);
		ready.notify_one();
	}
	std::uint64_t now() const noexcept { return 0; }
	task::root_list* roots() noexcept { return &roots_; }
	void run() {
		task::running_on scope(task::scheduler_ref::of(*this));
		for (;;) {
			std::pair<void (*)(void*), void*> item;
			{
				std::unique_lock lock(m);
				ready.wait(lock, [&] { return stopping || !queue.empty(); });
				if (stopping) { return; }
				item = queue.front(); queue.pop_front();
			}
			item.first(item.second);
		}
	}
	// Phase 1: the thread stops with its queue unrun. Phase 2: the queue and later posts drop.
	void stop() {
		{ std::lock_guard lock(m); stopping = true; }
		ready.notify_one();
		if (thread.joinable()) { thread.join(); }
		std::lock_guard lock(m);
		closed = true; queue.clear();
	}
	// Runs f on the loop and waits for it.
	void sync(std::function<void()> f) {
		std::mutex done_m; std::condition_variable done_cv; bool done = false;
		struct Job { std::function<void()> f; std::mutex* m; std::condition_variable* cv; bool* done; };
		Job job{std::move(f), &done_m, &done_cv, &done};
		post_call([](void* p) {
			auto& job = *static_cast<Job*>(p);
			job.f();
			std::lock_guard lock(*job.m); *job.done = true; job.cv->notify_one();
		}, &job);
		std::unique_lock lock(done_m);
		done_cv.wait(lock, [&] { return done; });
	}
};

void threads(int rounds) {
	for (int round = 0; round < rounds && !failures; ++round) {
		++runs;
		fl::filesystem disk;
		j::InlineCompletion io;
		Sink sink;
		auto loop = std::make_unique<thread_loop>();
		auto callers_loop = std::make_unique<thread_loop>();
		std::unique_ptr<j::Host> host;
		std::unique_ptr<Driver<Sink>> driver;
		task::cancel_source stop;
		std::atomic<int> created{0};
		loop->sync([&] {
			disk.make_directory("host");
			host = std::make_unique<j::Host>(std::make_shared<fl::io>(disk, "host"), j::Identity{'W', 'T'}, j::HostOptions{1, turn_options});
			driver = std::make_unique<Driver<Sink>>(Seams{task::scheduler_ref::of(*loop), j::completion_ref::of(io), *host, 0, 7}, sink);
			task::spawn(task::scheduler_ref::of(*loop), host->driver(0).run(j::completion_ref::of(io), stop.token()), [](std::exception_ptr) {});
			task::spawn(task::scheduler_ref::of(*loop), driver->run(stop.token()), [](std::exception_ptr) {});
			driver->open(g, options());
			task::spawn(task::scheduler_ref::of(*loop), driver->create(g, j::Identity{'G', '1'}), [&](std::exception_ptr e) { created = e ? -1 : 1; });
		});
		while (!created) { std::this_thread::yield(); }
		constexpr int n = 4;
		std::vector<task::cancel_source> cancels(n);
		std::atomic<int> completions[n] = {};
		for (int i = 0; i < n; ++i) {
			auto work = [](Driver<Sink>& d, int i, task::cancel_token token) -> task::task<void> {
				if (i % 2) { co_await d.read(g, token); } else { co_await d.propose(g, "t" + std::to_string(i), token); }
			};
			task::spawn(task::scheduler_ref::of(*callers_loop), work(*driver, i, cancels[i].token()), [&completions, i](std::exception_ptr) { ++completions[i]; });
		}
		std::thread canceller([&] { for (int i = 0; i < n; i += 2) { cancels[i].request(); } });
		const bool tear = round % 3 == 2;
		if (!tear) {
			std::atomic<bool> closed_done{false};
			task::spawn(task::scheduler_ref::of(*loop), driver->close(g), [&](std::exception_ptr) { closed_done = true; });
			canceller.join();
			for (int spin = 0; spin < 2'000'000 && !closed_done; ++spin) { std::this_thread::yield(); }
			if (!closed_done) { fail("close did not complete on real threads"); }
			for (int i = 0; i < n; ++i) {
				for (int spin = 0; spin < 2'000'000 && !completions[i]; ++spin) { std::this_thread::yield(); }
				if (completions[i] != 1) { fail("a caller on real threads completed " + std::to_string(completions[i].load()) + " times"); }
			}
			loop->sync([&] { stop.request(task::cancel_reason::shutdown); });
		} else {
			canceller.join();
		}
		// Teardown: both threads stop, queues drop, frames are destroyed on this thread.
		callers_loop->stop(); loop->stop();
		callers_loop->roots_.close(); loop->roots_.close();
		callers_loop->roots_.destroy_all(); loop->roots_.destroy_all();
		if (!driver->waiting_groups().empty()) { fail("a destroyed frame's waiter stays linked after teardown"); }
		driver.reset(); host.reset();
		for (int i = 0; i < n; ++i) { if (completions[i] > 1) { fail("a caller completed twice"); } }
	}
	std::printf("threads: %d rounds\n", rounds);
}

// Each case explores one spec up to `limit` schedules (threads: rounds).
const std::map<std::string, std::function<void(std::uint64_t)>> cases{
	{"rows-propose", [](std::uint64_t n) { explore("rows propose", Case{{Kind::propose}}, n); }},
	{"rows-committed", [](std::uint64_t n) { explore("rows committed", Case{{Kind::committed}}, n); }},
	{"rows-read", [](std::uint64_t n) { explore("rows read", Case{{Kind::read}, false, false, true}, n); }},
	{"rows-propose-pair", [](std::uint64_t n) { explore("rows propose pair", Case{{Kind::propose, Kind::propose}}, n); }},
	{"close", [](std::uint64_t n) { explore("close", Case{{Kind::propose, Kind::propose, Kind::committed}, true}, n); }},
	{"close-read", [](std::uint64_t n) { explore("close-read", Case{{Kind::read, Kind::read}, true, false, true, false, true, true, true}, n); }},
	{"close-open", [](std::uint64_t n) { explore("close-open", Case{{Kind::create, Kind::create}, true, false, false, false, false}, n); }},
	{"close-late", [](std::uint64_t n) { explore("close-late", Case{{Kind::propose, Kind::propose}, true, false, true, true}, n); }},
	{"teardown", [](std::uint64_t n) { explore("teardown", Case{{Kind::propose, Kind::read}, false, true}, n); }},
	{"teardown-close", [](std::uint64_t n) { explore("teardown close", Case{{Kind::propose, Kind::committed}, true, true}, n); }},
	{"threads", [](std::uint64_t n) { threads(static_cast<int>(std::min<std::uint64_t>(n, 300))); }},
};

}  // namespace

// `waiter_test [CASE [LIMIT]]` explores CASE, or every case, up to LIMIT schedules (threads:
// rounds, at most 300); the default, 20,000 (threads 100 rounds), is ctest's run of each case.
int main(int argc, char** argv) {
	const auto limit = [&](const std::string& name) -> std::uint64_t { return argc > 2 ? std::strtoull(argv[2], nullptr, 10) : name == "threads" ? 100 : 20'000; };
	if (argc > 1) {
		auto found = cases.find(argv[1]);
		if (found == cases.end()) { std::fprintf(stderr, "unknown case %s\n", argv[1]); return 2; }
		found->second(limit(found->first));
	} else {
		for (const auto& [name, run] : cases) { run(limit(name)); }
	}
	std::printf("waiter: %llu runs, %llu failures\n", (unsigned long long)runs, (unsigned long long)failures);
	return failures ? 1 : 0;
}
