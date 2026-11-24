// The host's lag: the longest a thread that only sleeps 1 ms went without running, in ms. A runner
// that pauses or starves the process delays every thread, which no transport rule can, so a
// latency bound adds it. Self-contained: it names no test fixture.
#pragma once
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>

struct Lag {
	std::atomic<bool> stopped{false};
	std::atomic<std::uint64_t> worst{0};
	std::thread sampler{[this] {
		auto last = std::chrono::steady_clock::now();
		while (!stopped) {
			std::this_thread::sleep_for(std::chrono::milliseconds(1));
			const auto now = std::chrono::steady_clock::now();
			worst = std::max<std::uint64_t>(worst, std::uint64_t(std::chrono::duration_cast<std::chrono::milliseconds>(now - last).count()));
			last = now;
		}
	}};
	// Stops the sampler and returns its worst: read it once the measured work is over.
	std::uint64_t stop() {
		stopped = true;
		if (sampler.joinable())
			sampler.join();
		return worst;
	}
	~Lag() { stop(); }
};
