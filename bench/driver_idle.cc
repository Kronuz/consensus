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

// What an idle group costs on a Driver (async-drivers Y6, Y9): N groups on one loop over a
// journal host on real files, created and settled, then counted. Per group: threads,
// descriptors, frames in flight, armed timers, and heap bytes (the Worker, its Core and Store,
// the Driver's group entry, and for a three-voter member the snapshot channel, D9).
//   driver_idle [groups] [lone|three]   (defaults 1000, both)
// Exits nonzero when an idle group holds a thread, descriptor or frame, a lone voter an armed
// timer, a three-voter member more than its one wheel entry, an idle channel a frame buffer,
// or, on Linux, a three-voter member more than 8 KiB. Threads are counted on macOS, FreeBSD
// and Linux; heap bytes on macOS and glibc.
#include "consensus/driver.h"
#include "consensus/snapshot_channel.h"
#include "completion/posix.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <system_error>
#include <vector>
#include <unistd.h>
#include <fcntl.h>

#if defined(__APPLE__)
#include <mach/mach.h>
#include <malloc/malloc.h>
#elif defined(__linux__)
#include <malloc.h>
#elif defined(__FreeBSD__)
#include <sys/types.h>
#include <sys/sysctl.h>
#include <sys/user.h>
#endif

using namespace kronuz::consensus;
namespace j = kronuz::journal;

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define DRIVER_IDLE_SANITIZED 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer) || __has_feature(memory_sanitizer)
#define DRIVER_IDLE_SANITIZED 1
#endif
#endif

namespace {
constexpr kronuz::journal::PosixIOOptions full_flush{kronuz::journal::FlushMode::full};

// A sanitizer's allocator pads every block and reports no zone statistics: bytes go unchecked.
// Heap bytes are read from macOS's zone statistics and glibc's mallinfo2 only.
#if defined(DRIVER_IDLE_SANITIZED) || !(defined(__APPLE__) || defined(__linux__))
constexpr bool heap_measured = false;
#else
constexpr bool heap_measured = true;
#endif

std::size_t heap_in_use() {
#if defined(__APPLE__)
	malloc_statistics_t stats{};
	malloc_zone_statistics(nullptr, &stats);
	return stats.size_in_use;
#elif defined(__linux__)
	return mallinfo2().uordblks;
#else
	return 0;
#endif
}
std::size_t threads() {
#if defined(__APPLE__)
	thread_act_array_t list; mach_msg_type_number_t count = 0;
	if (task_threads(mach_task_self(), &list, &count) != KERN_SUCCESS) { return 0; }
	for (mach_msg_type_number_t i = 0; i < count; ++i) { mach_port_deallocate(mach_task_self(), list[i]); }
	vm_deallocate(mach_task_self(), reinterpret_cast<vm_address_t>(list), count * sizeof(thread_act_t));
	return count;
#elif defined(__FreeBSD__)
	int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PID, ::getpid()};
	struct kinfo_proc info{};
	std::size_t length = sizeof(info);
	if (::sysctl(mib, 4, &info, &length, nullptr, 0) != 0) { throw std::system_error(errno, std::generic_category(), "sysctl kern.proc.pid"); }
	return static_cast<std::size_t>(info.ki_numthreads);
#else
	std::size_t count = 0;
	for ([[maybe_unused]] const auto& entry : std::filesystem::directory_iterator("/proc/self/task")) { ++count; }
	return count;
#endif
}
std::size_t descriptors() {
	std::size_t count = 0;
	for (int fd = 0; fd < 4096; ++fd) { count += ::fcntl(fd, F_GETFD) != -1; }
	return count;
}

struct Sink { template <class N> void operator()(GroupId, N&&) {} };

// Segments large enough that creating the groups rotates none: a rotation's descriptor is the host's.
const j::TurnLogOptions turn_options{4u << 20, 32 << 10, 1, 4, 1 << 20};
const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 32, 3};

GroupOptions options(bool three) {
	GroupOptions o;
	o.configuration.local = 1; o.configuration.voters = three ? std::vector<NodeId>{1, 2, 3} : std::vector<NodeId>{1};
	o.configuration.cluster[0] = 'I'; o.configuration.configuration[0] = 'D';
	o.admission = admission; o.scheduling = WorkerLimits{16, 128};
	return o;
}

int failures = 0;
void expect(bool value, const char* what) {
	if (!value) { ++failures; std::printf("  FAIL: %s\n", what); }
}

void measure(std::size_t groups, bool three) {
	const auto directory = std::filesystem::current_path() / ".scratch" / ("driver-idle-" + std::to_string(::getpid()));
	std::filesystem::remove_all(directory);
	std::filesystem::create_directories(directory);
	{
		task::inline_scheduler scheduler;
		auto ref = task::scheduler_ref::of(scheduler);
		j::InlineCompletion io;
		j::Host host(std::make_shared<j::PosixIO>(directory, full_flush), j::Identity{'I', 'D'}, j::HostOptions{1, turn_options});
		Sink sink;
		Driver<Sink> driver(Seams{ref, j::completion_ref::of(io), host, 0, 11}, sink);
		task::cancel_source stop;
		task::spawn(ref, host.driver(0).run(j::completion_ref::of(io), stop.token()), [](std::exception_ptr) {});
		task::spawn(ref, driver.run(stop.token()), [](std::exception_ptr) {});
		scheduler.run();
		std::size_t created = 0;
		auto create = [&](GroupId id) {
			driver.open(id, options(three));
			task::spawn(ref, driver.create(id, j::Identity{'G', 'I'}), [&created](std::exception_ptr e) { created += !e; });
			scheduler.run();
		};
		// The first group opens what the host shares (the turn log's segment): the baseline
		// follows it, so what is counted is each further group's own cost.
		create(1);
		const auto threads_before = threads(), fds_before = descriptors(), timers_before = scheduler.armed_timers();
		const auto heap_before = heap_in_use();
		for (GroupId id = 2; id <= groups + 1; ++id) { create(id); }
		scheduler.run();
		const auto heap_after = heap_in_use();
		const auto per_group = double(heap_after - heap_before) / double(groups);
		const auto timers = scheduler.armed_timers() - timers_before;
		const auto fds = descriptors() - fds_before;
		const auto extra_threads = threads() - threads_before;
		std::printf("%s x %zu: created %zu; per group %.0f bytes, %.3f armed timers, %.3f descriptors, %zu threads in all, %zu frames in flight\n",
			three ? "three-voter member" : "lone voter", groups, created - 1, per_group, double(timers) / double(groups), double(fds) / double(groups), extra_threads, driver.in_flight());
		expect(created == groups + 1, "every group created");
		expect(extra_threads == 0, "no thread per group");
		expect(fds == 0, "no descriptor per idle group");
		expect(driver.in_flight() == 0 && driver.waiting_groups().empty(), "no frame per idle group");
		expect(three ? timers <= groups : timers == 0, three ? "at most one armed wheel entry per member" : "no armed timer per idle lone voter");
		if (three) {
			// D9: an idle member's snapshot channel holds no frame buffer; its bytes join the member's.
			// Many channels, so a freed block the allocator reuses cannot hide one channel's bytes.
			Worker worker(host, groups + 2, options(true).configuration, admission);
			constexpr std::size_t channel_count = 1000;
			std::vector<std::unique_ptr<SnapshotChannel>> channels;
			channels.reserve(channel_count);
			const auto channel_before = heap_in_use();
			for (std::size_t i = 0; i < channel_count; ++i) { channels.push_back(std::make_unique<SnapshotChannel>(worker)); }
			const auto channel_bytes = double(heap_in_use() - channel_before) / double(channel_count);
			std::size_t buffered = 0;
			for (const auto& channel : channels) { buffered += channel->buffer_capacity(); }
			std::printf("  snapshot channel: %.0f bytes, %zu buffered\n", channel_bytes, buffered);
			expect(buffered == 0, "an idle snapshot channel holds no frame buffer (D9)");
			std::printf("  objects: Worker %zu, Core %zu, SnapshotChannel %zu bytes\n", sizeof(Worker), sizeof(Core), sizeof(SnapshotChannel));
			const auto member = per_group + channel_bytes;
			std::printf("  three-voter member's consensus state: %.0f bytes (Y9: 8192)\n", member);
#if defined(__linux__)
			if (heap_measured) { expect(member <= 8192, "a three-voter member's consensus state within 8 KiB (Y9)"); }
#endif
			if (!heap_measured) { std::printf("  (bytes not checked: a sanitizer, or no heap statistics here)\n"); }
		}
		stop.request(task::cancel_reason::shutdown);
		scheduler.run();
	}
	std::filesystem::remove_all(directory);
}

}  // namespace

int main(int argc, char** argv) {
	const std::size_t groups = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 1000;
	const std::string mode = argc > 2 ? argv[2] : "both";
	if (mode != "three") { measure(groups, false); }
	if (mode != "lone") { measure(groups, true); }
	std::printf("driver idle: %s\n", failures ? "FAILED" : "ok");
	return failures ? 1 : 0;
}
