// libFuzzer target for the RPC lane (consensus/transport/rpc.h). data[0] even: one frame through
// decode, which must decode or throw journal::Corruption and re-encode to the same bytes. Odd:
// a call table (an acceptor for 1 mod 4, a dialer for 3) with two calls of its own and a server
// for method 7, driven by records of [op][u16 length][bytes]: op 0 receives a frame, 1 consumes
// every delivered byte, 2 advances time 40 ms and expires, 3 drains, 4 stops a call. A
// violation closes the lane. Then: every call ended exactly once and every pool is back to 0.
#include "consensus/transport/rpc.h"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <map>
#include <string_view>

namespace {
using namespace kronuz::consensus::transport::rpc;
void require(bool value) {
	if (!value)
		std::abort();
}

struct Table {
	ByteBudget serving{1 << 18}, calling{1 << 18}, outgoing{1 << 18};
	Clock::time_point time = Clock::time_point{} + std::chrono::hours(1);
	std::deque<std::function<void()>> posted;
	std::map<Handle, int> done;          // completions per call
	std::map<Handle, std::size_t> owed;  // delivered, not yet consumed
	std::optional<Lane> lane;
	Events events(Handle *h, bool serving) {
		return {.body = [this, h, serving](std::string_view bytes, bool end) {
					owed[*h] += bytes.size();
					if (auto k = serving && end ? lane->sendable(*h) : 0)
						lane->send(*h, std::string("ok").substr(0, k), false);
				},
				.credit = {},
				.done = [this, h](const Status &) { ++done[*h]; }};
	}
	std::deque<Handle> handles;
	explicit Table(bool dialer) {
		lane.emplace(
			dialer, Pools{serving, calling, outgoing}, LaneOptions{.window = 1 << 17, .serving = 4},
			[this](const Incoming &in) -> std::optional<Events> {
				if (in.method != 7)
					return std::nullopt;
				handles.push_back(in.handle);
				done[in.handle];
				return events(&handles.back(), true);
			},
			[this](std::function<void()> f) { posted.push_back(std::move(f)); }, [] {}, [this] { return time; });
		for (int i = 0; i < 2; ++i) {
			handles.push_back(0);
			Handle *h = &handles.back();
			*h = lane->call(7, {.deadline = time + std::chrono::milliseconds(100 + 50 * i), .window = 1 << 17, .total = i ? unknown_total : 3}, "abc",
							events(h, false));
			done[*h];
		}
	}
	void settle() {
		while (!posted.empty()) {
			auto f = std::move(posted.front());
			posted.pop_front();
			f();
		}
		while (lane->next()) {
		}
	}
};
} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, std::size_t size) {
	if (size == 0)
		return 0;
	std::string_view in(reinterpret_cast<const char *>(data) + 1, size - 1);
	if (data[0] % 2 == 0) {
		try {
			require(encode(decode(in)) == in);
		} catch (const kronuz::journal::Corruption &) {
		}
		return 0;
	}
	Table t(data[0] % 4 == 3);
	try {
		while (in.size() >= 3) {
			const auto op = static_cast<std::uint8_t>(in[0]) % 5;
			const std::size_t n = std::min<std::size_t>(in.size() - 3, static_cast<std::uint8_t>(in[1]) | static_cast<std::uint8_t>(in[2]) << 8);
			const auto bytes = in.substr(3, n);
			in.remove_prefix(3 + n);
			t.settle();
			if (op == 0) {
				t.lane->receive(bytes);
			} else if (op == 1) {
				for (auto &[h, n] : t.owed)
					t.lane->consume(h, std::exchange(n, 0));
			} else if (op == 2) {
				t.time += std::chrono::milliseconds(40);
				t.lane->expire();
			} else if (op == 3) {
				t.lane->drain();
			} else if (!t.handles.empty()) {
				t.lane->stop(t.handles[n % t.handles.size()], Code::cancelled); // valid as Cancel and Error
			}
		}
		t.settle();
	} catch (const std::runtime_error &e) { // Corruption or ProtocolError: the connection closes
		if (std::getenv("FUZZ_DEBUG"))
			std::fprintf(stderr, "rpc table: %s\n", e.what());
	}
	if (std::getenv("FUZZ_DEBUG"))
		std::fprintf(stderr, "rpc table: %zu live, %llu late\n", t.lane->live(), (unsigned long long)t.lane->late());
	t.lane->close();
	t.settle();
	for (auto &[h, n] : t.done)
		require(n == 1);
	require(!t.serving.used() && !t.calling.used() && !t.outgoing.used());
	return 0;
}
