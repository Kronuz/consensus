// Lookup threads (resolve.h): lookups deliver; an abandoned lookup's late answer is never
// delivered, a queued one is freed unstarted, and abandon() waits for a delivery in progress;
// transport rebuilds over a resolver that never answers keep the process's threads within the
// bound; destruction, and the process's exit, join every lookup thread, waiting for a stuck
// resolver, and a lookup started after exit closed them throws (docs/mechanisms/peer-dns.md).
#include "consensus/transport/resolve.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#if defined(__APPLE__)
#include <mach/mach.h>
#elif defined(__FreeBSD__)
#include <sys/sysctl.h>
#include <sys/types.h>
#include <sys/user.h>
#endif

using namespace kronuz::consensus::transport;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

namespace {
#define CHECK(condition)                                                                           \
	do {                                                                                           \
		if (!(condition)) {                                                                        \
			std::fprintf(stderr, "%s: CHECK failed at line %d: %s\n", __func__, __LINE__, #condition); \
			std::fflush(stderr);                                                                   \
			std::abort();                                                                          \
		}                                                                                          \
	} while (0)

long long ms(Clock::duration d) { return std::chrono::duration_cast<std::chrono::milliseconds>(d).count(); }

// The process's threads, as the system counts them.
std::size_t os_threads() {
#if defined(__APPLE__)
	thread_act_array_t list = nullptr;
	mach_msg_type_number_t count = 0;
	CHECK(task_threads(mach_task_self(), &list, &count) == KERN_SUCCESS);
	for (mach_msg_type_number_t i = 0; i != count; ++i)
		mach_port_deallocate(mach_task_self(), list[i]);
	vm_deallocate(mach_task_self(), reinterpret_cast<vm_address_t>(list), count * sizeof(*list));
	return count;
#elif defined(__FreeBSD__)
	int mib[4] = {CTL_KERN, KERN_PROC, KERN_PROC_PID | KERN_PROC_INC_THREAD, getpid()};
	std::size_t size = 0;
	CHECK(sysctl(mib, 4, nullptr, &size, nullptr, 0) == 0);
	std::vector<kinfo_proc> procs(size / sizeof(kinfo_proc) + 8);
	size = procs.size() * sizeof(kinfo_proc);
	CHECK(sysctl(mib, 4, procs.data(), &size, nullptr, 0) == 0);
	return size / sizeof(kinfo_proc);
#else
	std::size_t count = 0;
	for ([[maybe_unused]] const auto &entry : std::filesystem::directory_iterator("/proc/self/task"))
		++count;
	return count;
#endif
}

template <class Done> bool until(Done done, Clock::duration limit = 5s) {
	const auto end = Clock::now() + limit;
	while (!done()) {
		if (Clock::now() >= end)
			return done();
		std::this_thread::sleep_for(1ms);
	}
	return true;
}

// A resolver that parks every call until open(), counting calls entered and returned.
struct Gate {
	std::mutex mutex;
	std::condition_variable wake;
	bool opened = false;
	std::atomic<unsigned> entered{0}, returned{0};
	std::vector<asio::ip::address> resolve(const std::string &) {
		++entered;
		{
			std::unique_lock lock(mutex);
			wake.wait(lock, [&] { return opened; });
		}
		++returned;
		return {asio::ip::address_v4::loopback()};
	}
	void open() {
		std::lock_guard lock(mutex);
		opened = true;
		wake.notify_all();
	}
	static PeerResolver resolver(std::shared_ptr<Gate> self) {
		return [self](const std::string &host) { return self->resolve(host); };
	}
};

// Answers delivered to one lookup's caller.
struct Answers {
	std::mutex mutex;
	std::vector<std::vector<asio::ip::address>> got;
	Lookup::Delivery delivery(std::shared_ptr<Answers> self) {
		return [self](std::vector<asio::ip::address> found) {
			std::lock_guard lock(self->mutex);
			self->got.push_back(std::move(found));
		};
	}
	std::size_t size() {
		std::lock_guard lock(mutex);
		return got.size();
	}
};

std::size_t baseline = 0;  // the process's threads before any lookup
Clock::time_point returned_at;

// A3: a lookup delivers its resolver's answer, in order and whole; a throwing resolver
// delivers an empty answer. Over the process's threads and over an owned LookupThreads.
void delivers() {
	const auto v4 = asio::ip::make_address("192.0.2.1"), v6 = asio::ip::make_address("2001:db8::1");
	PeerResolver answering = [&](const std::string &host) {
		if (host == "throws")
			throw std::runtime_error("SERVFAIL");
		return host == "two" ? std::vector<asio::ip::address>{v4, v6} : std::vector<asio::ip::address>{};
	};
	auto process = std::make_shared<Answers>(), owned = std::make_shared<Answers>(),
		 thrown = std::make_shared<Answers>();
	auto a = Lookup::start(answering, "two", process->delivery(process));
	{
		LookupThreads threads(2);
		auto b = threads.start(answering, "two", owned->delivery(owned));
		auto c = threads.start(answering, "throws", thrown->delivery(thrown));
		CHECK(until([&] { return owned->size() == 1 && thrown->size() == 1; }));
	}
	CHECK(until([&] { return process->size() == 1; }));
	const std::vector<asio::ip::address> expected{v4, v6};
	CHECK(process->got[0] == expected && owned->got[0] == expected && thrown->got[0].empty());
	std::printf("delivers: two addresses in order over the process's and an owned LookupThreads; a throw delivers none\n");
}

// A3: an abandoned running lookup's late answer is never delivered; an abandoned queued lookup
// leaves the queue at once, its resolver never called and its closures freed.
void abandoned() {
	auto gate = std::make_shared<Gate>();
	auto late = std::make_shared<Answers>(), queued = std::make_shared<Answers>(),
		 after = std::make_shared<Answers>();
	LookupThreads threads(1);
	auto running = threads.start(Gate::resolver(gate), "running", late->delivery(late));
	CHECK(until([&] { return gate->entered == 1; }));
	auto in_resolver = std::make_shared<int>(0), in_delivery = std::make_shared<int>(0);
	std::weak_ptr<int> resolver_alive = in_resolver, delivery_alive = in_delivery;
	auto waiting = threads.start([in_resolver](const std::string &) { return std::vector<asio::ip::address>{}; },
								 "queued", [in_delivery, queued](std::vector<asio::ip::address> found) {
									 std::lock_guard lock(queued->mutex);
									 queued->got.push_back(std::move(found));
								 });
	in_resolver.reset();
	in_delivery.reset();
	CHECK(!resolver_alive.expired() && !delivery_alive.expired());
	CHECK(threads.queued() == 1 && threads.threads() == 1);
	running->abandon();
	waiting->abandon();
	CHECK(threads.queued() == 0);
	CHECK(resolver_alive.expired());
	CHECK(delivery_alive.expired());
	gate->open();
	// One thread runs lookups in order: this one's answer comes after the abandoned one returned.
	auto next = threads.start(Gate::resolver(gate), "after", after->delivery(after));
	CHECK(until([&] { return after->size() == 1; }));
	CHECK(gate->returned == 2 && late->size() == 0 && queued->size() == 0);
	std::printf("abandoned: the late answer was dropped; the queued lookup was freed unstarted\n");
}

// A3: abandon() during a delivery returns only after that delivery returns, so the caller's
// state outlives every delivery to it.
void abandon_waits() {
	std::mutex mutex;
	std::condition_variable wake;
	bool in_delivery = false, open = false, delivery_returned = false;
	LookupThreads threads(1);
	auto lookup = threads.start([](const std::string &) { return std::vector<asio::ip::address>{}; }, "delivering",
								[&](std::vector<asio::ip::address>) {
									std::unique_lock lock(mutex);
									in_delivery = true;
									wake.notify_all();
									wake.wait(lock, [&] { return open; });
									delivery_returned = true;
								});
	{
		std::unique_lock lock(mutex);
		CHECK(wake.wait_for(lock, 5s, [&] { return in_delivery; }));
	}
	std::atomic<bool> abandoning{false}, abandoned{false};
	bool returned_first = false;
	std::thread abandoner([&] {
		abandoning = true;
		lookup->abandon();
		abandoned = true;
		std::lock_guard lock(mutex);
		returned_first = delivery_returned;
	});
	CHECK(until([&] { return abandoning.load(); }));
	const bool early = until([&] { return abandoned.load(); }, 200ms);
	{
		std::lock_guard lock(mutex);
		open = true;
		wake.notify_all();
	}
	abandoner.join();
	std::printf("abandon_waits: abandon() %s the delivery in progress returned\n",
				early ? "returned before" : "waited until");
	CHECK(!early && returned_first);
}

// A2: destruction drops the queue, delivers nothing, and joins every thread, waiting for a
// resolver that returns 300 ms after destruction began.
void shutdown() {
	auto gate = std::make_shared<Gate>();
	auto answers = std::make_shared<Answers>();
	std::vector<std::shared_ptr<Lookup>> lookups;
	CHECK(until([&] { return os_threads() == baseline; }));
	auto threads = std::make_unique<LookupThreads>(4);
	for (unsigned i = 0; i != 6; ++i)
		lookups.push_back(threads->start(Gate::resolver(gate), "host" + std::to_string(i), answers->delivery(answers)));
	CHECK(until([&] { return gate->entered == 4 && os_threads() == baseline + 4; }));
	CHECK(threads->threads() == 4 && threads->queued() == 2);
	lookups.front()->abandon();  // abandon() before and after destruction is harmless
	const auto start = Clock::now();
	std::thread opener([gate] {
		std::this_thread::sleep_for(300ms);
		gate->open();
	});
	threads.reset();
	const auto took = Clock::now() - start;
	opener.join();
	for (auto &lookup : lookups)
		lookup->abandon();
	// A joined thread may linger in the system's count for a moment after the join returns.
	CHECK(until([&] { return os_threads() == baseline; }, 1s));
	std::printf("shutdown: a resolver stuck until 300 ms after destruction began held it %lld ms\n", ms(took));
	CHECK(took >= 290ms && gate->entered == 4 && gate->returned == 4 && answers->size() == 0);
}

// A1: 1,000 transport rebuilds, each starting lookups of three names over a resolver that never
// answers and abandoning them, as stop() does. The process's threads stay within the bound and
// the abandoned lookups leave no queue; once the resolver answers, the threads return.
void rebuilds() {
	auto gate = std::make_shared<Gate>();
	auto answers = std::make_shared<Answers>();
	auto &process = LookupThreads::process();
	const auto bound = LookupThreads::process_bound;
	CHECK(until([&] { return os_threads() == baseline; }));
	std::size_t peak = 0;
	for (unsigned rebuild = 0; rebuild != 1000; ++rebuild) {
		std::vector<std::shared_ptr<Lookup>> transport;
		const unsigned before = gate->entered;
		for (const char *name : {"one", "two", "three"})
			transport.push_back(Lookup::start(Gate::resolver(gate), name, answers->delivery(answers)));
		// Each lookup is parked in the resolver, or queued while every thread is.
		CHECK(until([&] {
			const auto queued = process.queued();
			const unsigned entered = gate->entered;
			return entered - before + queued == 3 && (queued == 0 || entered >= bound);
		}));
		for (auto &lookup : transport)
			lookup->abandon();
		peak = std::max(peak, os_threads() - baseline);
		if (peak > LookupThreads::process_bound) {
			std::printf("rebuilds: %zu threads past the baseline at rebuild %u, over the bound %zu\n", peak, rebuild,
						LookupThreads::process_bound);
			CHECK(peak <= LookupThreads::process_bound);
		}
	}
	const auto queued = process.queued();
	std::printf("rebuilds: 1000 rebuilds over a hung resolver peaked at %zu lookup threads (bound %zu), %zu queued, %u resolver calls\n",
				peak, LookupThreads::process_bound, queued, gate->entered.load());
	CHECK(peak <= bound && queued == 0 && gate->entered == bound);
	gate->open();
	CHECK(until([&] { return os_threads() == baseline; }));
	CHECK(answers->size() == 0 && gate->returned == bound);
}

// A2 at exit: runs after the process's LookupThreads is closed (registered before it was
// built); every thread it ran has been joined, and a lookup started now throws.
void exited() {
	const auto took = Clock::now() - returned_at;
	const bool joined = until([] { return os_threads() == baseline; }, 1s);
	std::printf("exit: waited %lld ms for the lookup in flight; %s\n", ms(took),
				joined ? "every lookup thread joined" : "a lookup thread is left");
	bool canceled = false;
	try {
		Lookup::start([](const std::string &) { return std::vector<asio::ip::address>{}; }, "after exit",
					  [](std::vector<asio::ip::address>) {});
	} catch (const std::system_error &error) {
		canceled = error.code() == std::errc::operation_canceled;
	}
	std::printf("exit: a lookup started after exit closed the threads %s\n",
				canceled ? "threw operation_canceled" : "did not throw it");
	std::fflush(stdout);
	if (!joined || took < 150ms || !canceled)
		std::_Exit(1);
}
} // namespace

int main(int argc, char **argv) {
	std::thread([] {}).join();  // a sanitizer's own threads start with the first one
	baseline = os_threads();
	std::atexit(exited);
	const std::pair<const char *, void (*)()> cases[] = {
		{"delivers", delivers}, {"abandoned", abandoned}, {"abandon_waits", abandon_waits},
		{"shutdown", shutdown}, {"rebuilds", rebuilds}};
	bool ran = false;
	for (const auto &[name, run] : cases)
		if (argc < 2 || std::strcmp(argv[1], name) == 0) {
			run();
			ran = true;
		}
	CHECK(ran);
	// Leaves one lookup in its resolver for exit to join: it returns 200 ms after it entered.
	static std::atomic<bool> entered{false};
	Lookup::start(
		[](const std::string &) {
			entered = true;
			std::this_thread::sleep_for(200ms);
			return std::vector<asio::ip::address>{};
		},
		"exit", [](std::vector<asio::ip::address>) {});
	CHECK(until([] { return entered.load(); }));
	std::printf("main: returning with a lookup in its resolver\n");
	returned_at = Clock::now();
	return 0;
}
