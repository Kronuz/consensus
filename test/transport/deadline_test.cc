// detail::async_deadline: the wait owns its timer, and a cancelled wait never extends.
#include "consensus/transport/session.h"
#include <cassert>
#include <cstdio>
#include <thread>

using namespace std::chrono_literals;
namespace detail = kronuz::consensus::transport::detail;
using Clock = std::chrono::steady_clock;

// The owner (a Fetching entry) drops its timer after the timer expired and before the wait's
// completion runs; the wait must not touch freed memory (ASan: heap-use-after-free otherwise).
static void dropped_timer() {
	asio::io_context io;
	auto timer = std::make_shared<asio::steady_timer>(io);
	timer->expires_after(10ms);
	int completions = 0;
	detail::async_deadline(timer, 500ms, [&](asio::error_code) { ++completions; });
	asio::post(io, [&] {
		timer->cancel();
		timer.reset();
	});
	std::this_thread::sleep_for(30ms); // the timer is due before run()
	io.run();
	assert(completions == 1);
}

// A wait cancelled after its timer expired completes at once with operation_aborted: it does not
// wait out the extension (here ~600 ms, the timer's lateness, under the 1 s cap).
static void cancelled_wait() {
	asio::io_context io;
	auto timer = std::make_shared<asio::steady_timer>(io);
	timer->expires_after(10ms);
	asio::cancellation_signal signal;
	asio::error_code result;
	bool done = false;
	detail::async_deadline(timer, 1s, asio::bind_cancellation_slot(signal.slot(), [&](asio::error_code error) {
		result = error;
		done = true;
	}));
	asio::post(io, [&] { signal.emit(asio::cancellation_type::terminal); });
	std::this_thread::sleep_for(600ms);
	const auto started = Clock::now();
	io.run();
	const auto took = Clock::now() - started;
	std::printf("cancelled wait: %s after %lld ms\n", result.message().c_str(),
				static_cast<long long>(std::chrono::duration_cast<std::chrono::milliseconds>(took).count()));
	assert(done && result == asio::error::operation_aborted && took < 250ms);
}

int main() {
	dropped_timer();
	cancelled_wait();
	std::printf("deadline: ok\n");
	return 0;
}
