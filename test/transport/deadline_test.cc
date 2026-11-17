// transport/deadline.h on inline_scheduler, whose advance() is a process stop: the clock jumps
// and every timer it passes fires late, in that advance.
//   consensus_transport_deadline_test [case]
#include "consensus/transport/deadline.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <memory>
#include <optional>

using namespace kronuz::consensus::transport;
using namespace std::chrono_literals;
using outcome = late_deadline::outcome;

namespace {
constexpr std::uint64_t ms = 1'000'000;

struct Loop {
	task::inline_scheduler s;
	task::scheduler_ref ref() { return task::scheduler_ref::of(s); }
	// Normal running: the clock moves a millisecond at a time, the loop runs between.
	void run_for(std::uint64_t ns) {
		for (std::uint64_t t = 0; t < ns; t += ms) {
			s.advance(ms);
			s.run();
		}
	}
	void stop(std::uint64_t ns) { s.advance(ns); }  // the process stopped; nothing ran
};

// A wait in a coroutine, as the transport's users await it; records when it ended.
struct Waiter {
	std::optional<outcome> result;
	std::uint64_t ended = 0;
	task::task<void> run(Loop &loop, late_deadline &d) {
		result = co_await d.wait();
		ended = loop.s.now();
	}
	void start(Loop &loop, late_deadline &d) {
		task::spawn(loop.ref(), run(loop, d));
		loop.s.run();
	}
};

void on_time() {
	Loop loop;
	late_deadline d(loop.ref(), 100ms);
	Waiter w;
	w.start(loop, d);
	loop.run_for(99 * ms);
	assert(!w.result && !d.token().requested());
	loop.run_for(2 * ms);
	assert(w.result == outcome::expired && d.extended() == 0ns);
	assert(w.ended >= 100 * ms && w.ended <= 101 * ms);
	assert(d.token().requested() && d.token().reason() == task::cancel_reason::deadline);
	assert(loop.s.armed_timers() == 0);
	std::printf("on_time: expired at %llu ms, no extension\n", (unsigned long long)(w.ended / ms));
}

// A 30 ms stop past a 100 ms deadline: the peer gets the 30 ms again.
void stop_extends() {
	Loop loop;
	late_deadline d(loop.ref(), 100ms);
	Waiter w;
	w.start(loop, d);
	loop.stop(130 * ms);
	loop.s.run();
	assert(!w.result && d.extended() == 30ms && !d.token().requested());
	loop.run_for(29 * ms);
	assert(!w.result);
	loop.run_for(2 * ms);
	assert(w.result == outcome::expired && w.ended >= 160 * ms && w.ended <= 161 * ms);
	std::printf("stop_extends: 30 ms late, extended 30 ms, expired at %llu ms\n", (unsigned long long)(w.ended / ms));
}

// A stop of three timeouts: the extension is one timeout.
void capped() {
	Loop loop;
	late_deadline d(loop.ref(), 100ms);
	Waiter w;
	w.start(loop, d);
	loop.stop(400 * ms);
	loop.s.run();
	assert(!w.result && d.extended() == 100ms);
	loop.run_for(99 * ms);
	assert(!w.result);
	loop.run_for(2 * ms);
	assert(w.result == outcome::expired && w.ended >= 500 * ms && w.ended <= 501 * ms);
	std::printf("capped: 300 ms late, extended 100 ms\n");
}

// A second stop inside the extension expires it: the extension is granted once.
void once() {
	Loop loop;
	late_deadline d(loop.ref(), 100ms);
	Waiter w;
	w.start(loop, d);
	loop.stop(130 * ms);
	loop.s.run();
	assert(d.extended() == 30ms && !w.result);
	loop.stop(500 * ms);
	loop.s.run();
	assert(w.result == outcome::expired && d.extended() == 30ms && w.ended == 630 * ms);
	std::printf("once: a second late fire expires\n");
}

// F11, F17: the timer fired late and armed the extension; the event it waits for lands in the
// same batch after the stop. The wait ends then, not at the extension's end.
void wake_after_late_fire() {
	Loop loop;
	late_deadline d(loop.ref(), 100ms);
	Waiter w;
	w.start(loop, d);
	loop.stop(600 * ms);  // the fire runs in here and extends
	assert(d.extended() == 100ms && d.state() == outcome::pending);
	d.wake();  // the answer queued during the stop
	assert(!w.result);  // never resumed inside wake()
	loop.s.run();
	assert(w.result == outcome::woken && w.ended == 600 * ms);
	assert(!d.token().requested() && loop.s.armed_timers() == 0);
	loop.run_for(200 * ms);
	assert(w.result == outcome::woken && !d.token().requested());
	std::printf("wake_after_late_fire: woken at the stop's end, 0 ms of the extension waited\n");
}

void wake_before() {
	Loop loop;
	late_deadline d(loop.ref(), 100ms);
	Waiter w;
	w.start(loop, d);
	loop.run_for(50 * ms);
	d.wake();
	d.wake();
	loop.s.run();
	assert(w.result == outcome::woken && w.ended == 50 * ms && loop.s.armed_timers() == 0);
	loop.run_for(100 * ms);
	assert(!d.token().requested());
	// A wait that starts after the wake does not suspend.
	Waiter late;
	late.start(loop, d);
	assert(late.result == outcome::woken);
	std::printf("wake_before: woken at 50 ms, the deadline unfiled\n");
}

// F10: the wait owns its timer. A deadline destroyed with its extension armed unfiles it and
// leaves its list, so neither the wheel nor a waker can reach it afterwards.
void owns_timer() {
	Loop loop;
	wait_list answers;
	{
		auto d = std::make_unique<late_deadline>(loop.ref(), 100ms);
		d->join(answers);
		loop.stop(150 * ms);
		assert(d->extended() == 50ms && loop.s.armed_timers() == 1 && !answers.empty());
	}
	assert(loop.s.armed_timers() == 0 && answers.empty());
	answers.wake_all();
	loop.run_for(200 * ms);
	std::printf("owns_timer: the destroyed deadline left the wheel and its list\n");
}

// wake_all wakes every member and empties the list; a woken deadline that is not in a list
// keeps its outcome.
void wake_all() {
	Loop loop;
	wait_list answers;
	late_deadline a(loop.ref(), 100ms), b(loop.ref(), 200ms), c(loop.ref(), 300ms);
	a.join(answers);
	b.join(answers);
	Waiter wa, wb, wc;
	wa.start(loop, a);
	wb.start(loop, b);
	wc.start(loop, c);
	loop.run_for(10 * ms);
	answers.wake_all();
	loop.s.run();
	assert(wa.result == outcome::woken && wb.result == outcome::woken && !wc.result && answers.empty());
	loop.run_for(300 * ms);
	assert(wc.result == outcome::expired && loop.s.armed_timers() == 0);
	std::printf("wake_all: two woken at 10 ms, the third expired\n");
}

// An operation awaited with the token is cancelled at expiry, with the deadline's reason.
void token() {
	Loop loop;
	late_deadline d(loop.ref(), 100ms);
	std::optional<task::cancel_reason> seen;
	task::cancel_callback cb(d.token(), [&](task::cancel_reason r) { seen = r; });
	loop.stop(150 * ms);
	assert(!seen);  // extended, not expired
	loop.run_for(51 * ms);
	assert(seen == task::cancel_reason::deadline);
	std::printf("token: requested at expiry, after the extension\n");
}

struct Case {
	const char *name;
	void (*run)();
};
constexpr Case cases[] = {{"on_time", on_time},	 {"stop_extends", stop_extends},
						  {"capped", capped},	 {"once", once},
						  {"wake_after_late_fire", wake_after_late_fire},
						  {"wake_before", wake_before}, {"owns_timer", owns_timer},
						  {"wake_all", wake_all}, {"token", token}};
} // namespace

int main(int argc, char **argv) {
	int ran = 0;
	for (const auto &c : cases) {
		if (argc > 1 && std::strcmp(argv[1], c.name) != 0)
			continue;
		c.run();
		++ran;
	}
	if (!ran) {
		std::fprintf(stderr, "unknown case\n");
		return 2;
	}
	std::printf("deadline: %d cases passed\n", ran);
	return 0;
}
