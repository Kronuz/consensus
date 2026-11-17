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

// The host transport's idle cost and hop CPU (docs/perf/transport-idle-cost.md; async-drivers
// D10). Hosts run in one process over loopback mutual TLS, each loop on its own thread.
//   cost [--quick]  meshes of H = 2, 3 and 5 hosts (P = H - 1 peers) of L = 1 and 4 loops: the
//                   bytes and descriptors per host and per connection end once idle, then G
//                   groups that each sent a control and a data frame to every peer, gone idle:
//                   bytes, descriptors, armed timers and frames per group; and an idle host's
//                   CPU and wakes. --quick: fewer groups, no idle window (the ctest entry).
//   hop [rounds]    host 1 sends to host 2 (2 loops) for 256 groups on the connection's loop
//                   ("same") or the other ("hop"): CPU of host 2's loops per frame, streaming
//                   control and data frames, and per frame sent alone every 1 ms (each wakes
//                   the idle group loop); rounds of same, hop, hop, same (default 7).
//   consensus_transport_idle <fixture dir> cost|hop [...]
// cost exits nonzero when an idle group holds a descriptor, an armed timer or a frame, or, where
// heap bytes are read (macOS, glibc; no sanitizer), more than 64 bytes; when an idle connection
// end holds more than 48 KB, or a host more than 10% over 29 KB + 2P x 41 KB.
#include "consensus/transport/host_transport.h"
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <openssl/pem.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#if defined(__APPLE__)
#include <malloc/malloc.h>
#elif defined(__linux__)
#include <malloc.h>
#endif

#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define TRANSPORT_IDLE_SANITIZED 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer) || __has_feature(memory_sanitizer)
#define TRANSPORT_IDLE_SANITIZED 1
#endif
#endif

// Live C++ heap by block size (operator new's usable sizes, 16-byte buckets): what the transport
// and its coroutine frames hold, apart from OpenSSL's malloc.
#if !defined(TRANSPORT_IDLE_SANITIZED) && (defined(__APPLE__) || defined(__linux__))
#define TRANSPORT_IDLE_COUNTS_NEW 1
namespace live {
constexpr std::size_t buckets = 4097;  // the last holds every block of 64 KiB or more
std::atomic<std::int64_t> bytes{0}, count[buckets];
inline std::size_t usable(void *p) {
#if defined(__APPLE__)
	return malloc_size(p);
#else
	return malloc_usable_size(p);
#endif
}
inline void add(void *p, std::int64_t sign) {
	const auto n = usable(p);
	bytes.fetch_add(sign * std::int64_t(n), std::memory_order_relaxed);
	count[std::min(n / 16, buckets - 1)].fetch_add(sign, std::memory_order_relaxed);
}
}  // namespace live
void *operator new(std::size_t n) {
	void *p = std::malloc(n ? n : 1);
	if (!p)
		throw std::bad_alloc();
	live::add(p, 1);
	return p;
}
void *operator new[](std::size_t n) { return operator new(n); }
void *operator new(std::size_t n, std::align_val_t a) {
	void *p = nullptr;
	if (posix_memalign(&p, std::max(sizeof(void *), static_cast<std::size_t>(a)), n ? n : 1))
		throw std::bad_alloc();
	live::add(p, 1);
	return p;
}
void *operator new[](std::size_t n, std::align_val_t a) { return operator new(n, a); }
void operator delete(void *p) noexcept {
	if (p) {
		live::add(p, -1);
		std::free(p);
	}
}
void operator delete[](void *p) noexcept { operator delete(p); }
void operator delete(void *p, std::size_t) noexcept { operator delete(p); }
void operator delete[](void *p, std::size_t) noexcept { operator delete(p); }
void operator delete(void *p, std::align_val_t) noexcept { operator delete(p); }
void operator delete[](void *p, std::align_val_t) noexcept { operator delete(p); }
void operator delete(void *p, std::size_t, std::align_val_t) noexcept { operator delete(p); }
void operator delete[](void *p, std::size_t, std::align_val_t) noexcept { operator delete(p); }
#endif

using namespace kronuz::consensus::transport;
using kronuz::consensus::NodeId;
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
namespace j = kronuz::journal;

namespace {
// A sanitizer's allocator pads every block: bytes are read from macOS's zone statistics and
// glibc's mallinfo2 only.
#if defined(TRANSPORT_IDLE_SANITIZED) || !(defined(__APPLE__) || defined(__linux__))
constexpr bool heap_measured = false;
#else
constexpr bool heap_measured = true;
#endif
std::int64_t heap_in_use(std::int64_t *blocks = nullptr) {
#if defined(__APPLE__)
	malloc_statistics_t stats{};
	malloc_zone_statistics(nullptr, &stats);
	if (blocks)
		*blocks = std::int64_t(stats.blocks_in_use);
	return std::int64_t(stats.size_in_use);
#elif defined(__linux__)
	return std::int64_t(mallinfo2().uordblks);
#else
	return 0;
#endif
}
std::int64_t descriptors() {
	std::int64_t count = 0;
	for (int fd = 0; fd < 4096; ++fd)
		count += ::fcntl(fd, F_GETFD) != -1;
	return count;
}
std::uint64_t thread_cpu_ns() {
	timespec t{};
	clock_gettime(CLOCK_THREAD_CPUTIME_ID, &t);
	return std::uint64_t(t.tv_sec) * 1'000'000'000 + std::uint64_t(t.tv_nsec);
}
std::uint64_t steady_ns() {
	return std::uint64_t(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}

int failures = 0;
void expect(bool value, const std::string &what) {
	if (!value) {
		++failures;
		std::printf("  FAIL: %s\n", what.c_str());
	}
}

std::filesystem::path fixture;
const char *names[] = {"", "one", "two", "three", "four", "five"};
constexpr NodeId max_node = 5;

Fingerprint pin_of(NodeId node) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file((fixture / (std::string(names[node]) + ".pem")).c_str(), "r"), BIO_free);
	std::unique_ptr<X509, decltype(&X509_free)> leaf(PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
	if (!leaf)
		throw std::runtime_error(std::string("no certificate for node ") + names[node]);
	return certificate_fingerprint(leaf.get());
}
std::shared_ptr<asio::ssl::context> context(NodeId node) {
	auto c = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	const std::string name = names[node];
	reactor::tls::configure(*c, {(fixture / (name + ".pem")).string(), (fixture / (name + ".key")).string(), (fixture / "ca.pem").string()},
							reactor::tls::Role::Peer);
	return c;
}

// One loop on its own thread; its reactor exists before anything is counted.
struct Loop {
	asio::io_context io;
	task::asio_scheduler scheduler{io.get_executor()};
	asio::executor_work_guard<asio::io_context::executor_type> work = asio::make_work_guard(io);
	std::thread thread{[this] { io.run(); }};
	Loop() {
		at([this] {
			tcp::socket s(io);
			s.open(tcp::v4());
		});
	}
	template <class F> auto at(F f) -> decltype(f()) {
		std::packaged_task<decltype(f())()> call(std::move(f));
		auto result = call.get_future();
		asio::post(io, [&call] { call(); });
		return result.get();
	}
	std::uint64_t cpu() {
		return at([] { return thread_cpu_ns(); });
	}
	void join() {
		work.reset();
		io.stop();
		if (thread.joinable())
			thread.join();
	}
	~Loop() { join(); }
};
struct Totals {
	std::uint64_t links = 0, sent = 0, received = 0, posts = 0, dropped = 0, refused = 0, malformed = 0;
};

// One host: L loops, a listener on loop 0 and the transport; it takes every frame.
struct Host {
	NodeId node;
	std::vector<std::unique_ptr<Loop>> loops;
	std::unique_ptr<tcp::acceptor> acceptor;
	std::unique_ptr<HostTransport> transport;
	std::unique_ptr<std::atomic<std::uint64_t>[]> delivered;
	std::function<void(std::uint32_t, const Received &)> hook;  // set before the first frame

	Host(NodeId n, std::uint32_t L) : node(n), delivered(new std::atomic<std::uint64_t>[L]) {
		for (std::uint32_t k = 0; k != L; ++k) {
			loops.push_back(std::make_unique<Loop>());
			delivered[k] = 0;
		}
	}
	std::uint32_t L() const { return std::uint32_t(loops.size()); }
	void listen() { acceptor = std::make_unique<tcp::acceptor>(loops[0]->io, tcp::endpoint{asio::ip::address_v4::loopback(), 0}); }
	std::uint16_t port() const { return acceptor->local_endpoint().port(); }
	void open(const std::map<NodeId, PeerAddress> &addresses, HostTransportOptions options = {}) {
		auto tls = context(node);
		std::map<NodeId, Fingerprint> pins;
		for (NodeId n = 1; n <= max_node; ++n)
			pins[n] = pin_of(n);
		std::vector<HostLoop> at;
		for (auto &l : loops)
			at.push_back({l->io.get_executor(), task::scheduler_ref::of(l->scheduler)});
		kronuz::consensus::Identity cluster{};
		cluster.fill('I');
		transport = std::make_unique<HostTransport>(at, tls, NodePins(node, pins, tls->native_handle()), cluster, addresses, options);
		for (std::uint32_t k = 0; k != L(); ++k)
			transport->on_deliver(k, [this, k](const Received &r) {
				if (hook)
					hook(k, r);
				delivered[k].fetch_add(1, std::memory_order_relaxed);
				return true;
			});
		loops[0]->at([this] { transport->serve(*acceptor); });
	}
	std::uint64_t total() const {
		std::uint64_t n = 0;
		for (std::uint32_t k = 0; k != L(); ++k)
			n += delivered[k].load(std::memory_order_relaxed);
		return n;
	}
	Totals sum() const {
		Totals s;
		for (std::uint32_t k = 0; k != L(); ++k) {
			const auto &t = transport->stats(k);
			s.links += t.links;
			s.sent += t.sent;
			s.received += t.received;
			s.posts += t.posts;
			s.dropped += t.dropped;
			s.refused += t.refused;
			s.malformed += t.malformed;
		}
		return s;
	}
	// Sends from loop k, the group's.
	template <class F> void on(std::uint32_t k, F f) { asio::post(loops[k]->io, std::move(f)); }
	~Host() {
		if (transport) {
			transport->stop();
			for (std::uint32_t k = 0; k != L(); ++k)
				for (int i = 0; !loops[k]->at([&] { return transport->drained(k); }); ++i) {
					if (i == 2000)
						std::abort();
					std::this_thread::sleep_for(5ms);
				}
		}
		for (auto &l : loops)
			l->join();
		transport.reset();
		acceptor.reset();
	}
};

std::map<NodeId, PeerAddress> peers_of(const std::vector<std::unique_ptr<Host>> &hosts, NodeId self) {
	std::map<NodeId, PeerAddress> r;
	for (auto &h : hosts)
		if (h->node != self)
			r[h->node] = {"127.0.0.1", h->port()};
	return r;
}
const std::vector<std::unique_ptr<Host>> *watched = nullptr;  // printed when a wait times out
// Waits until done(); at `limit`, false, or when `fatal` the counters and exit 1.
template <class P> bool wait_for(P done, const char *what, std::chrono::seconds limit = 60s, bool fatal = true) {
	const auto until = std::chrono::steady_clock::now() + limit;
	while (!done()) {
		if (std::chrono::steady_clock::now() > until) {
			std::printf("  %s: timed out: %s\n", fatal ? "FAIL" : "note", what);
			if (!fatal)
				return false;
			for (std::size_t i = 0; watched && i != watched->size(); ++i)
				for (std::uint32_t k = 0; k != (*watched)[i]->L(); ++k) {
					const auto &h = (*watched)[i];
					const auto &s = h->transport->stats(k);
					std::printf("  host %llu loop %u: links %llu sent %llu received %llu delivered %llu dropped %llu\n", (unsigned long long)h->node, k,
								(unsigned long long)s.links, (unsigned long long)s.sent, (unsigned long long)s.received,
								(unsigned long long)h->delivered[k], (unsigned long long)s.dropped);
				}
			std::fflush(stdout);
			std::_Exit(1);
		}
		std::this_thread::sleep_for(1ms);
	}
	return true;
}

// What every loop holds at one moment: each loop parks while the heap and descriptors are read,
// so no loop is in the middle of a write or a handshake.
struct Sample {
	std::int64_t heap = 0, fds = 0, timers = 0, blocks = 0, cxx = 0;
	std::array<std::int64_t, 4097> sizes{};  // live C++ blocks by 16-byte bucket
};
// The C++ blocks `b` holds beyond `a`, per `unit`: "count x size" for each bucket that changed by
// at least a tenth of a block per unit.
std::string sizes(const Sample &a, const Sample &b, double unit) {
	std::string r;
	for (std::size_t i = 0; i != a.sizes.size(); ++i) {
		const double n = double(b.sizes[i] - a.sizes[i]) / unit;
		if (std::abs(n) < 0.1)
			continue;
		char buffer[64];
		std::snprintf(buffer, sizeof buffer, " %.2fx%zu", n, i * 16);
		r += buffer;
	}
	return r.empty() ? " none" : r;
}
Sample paused(const std::vector<std::unique_ptr<Host>> &hosts) {
	std::mutex m;
	std::condition_variable cv;
	std::size_t total = 0, parked = 0, left = 0;
	bool go = false;
	std::int64_t timers = 0;
	for (auto &h : hosts)
		for (auto &l : h->loops) {
			++total;
			asio::post(l->io, [&, loop = l.get()] {
				std::unique_lock lock(m);
				timers += std::int64_t(loop->scheduler.armed_timers());
				++parked;
				cv.notify_all();
				cv.wait(lock, [&] { return go; });
				++left;
				cv.notify_all();
			});
		}
	std::unique_lock lock(m);
	cv.wait(lock, [&] { return parked == total; });
	Sample s;
	s.fds = descriptors();
	s.timers = timers;
	s.heap = heap_in_use(&s.blocks);
#ifdef TRANSPORT_IDLE_COUNTS_NEW
	s.cxx = live::bytes.load();
	for (std::size_t i = 0; i != s.sizes.size(); ++i)
		s.sizes[i] = live::count[i].load();
#endif
	go = true;
	cv.notify_all();
	cv.wait(lock, [&] { return left == total; });
	return s;
}

const std::string payload39(39, 'p');  // a 64-byte frame: kind, group, source, destination, 39 bytes

struct Cost {
	int peers = 0;
	std::uint32_t loops = 0;
	double host = 0;    // bytes per host once its connections are idle
	bool ended = false; // a connection ended during the run: the host stalled past dead_after
};

Cost cost(int H, std::uint32_t L, std::size_t G, double idle_seconds, bool print) {
	const int P = H - 1;
	const std::uint64_t ends = 2ull * std::uint64_t(H) * std::uint64_t(P);  // two lanes per pair, two ends each
	std::vector<std::unique_ptr<Host>> hosts;
	for (int i = 1; i <= H; ++i)
		hosts.push_back(std::make_unique<Host>(NodeId(i), L));
	std::this_thread::sleep_for(20ms);
	const auto s0 = paused(hosts);
	for (auto &h : hosts)
		h->listen();
	for (auto &h : hosts)
		h->open(peers_of(hosts, h->node));
	const auto s1 = paused(hosts);
	watched = &hosts;

	// The mesh, one pair at a time: the lower node dials both lanes, so no two dials race and no
	// listener sees more than its 2 handshakes per source address (every host is 127.0.0.1); then
	// every node sends back on them.
	const GroupId warm = 1;
	auto total = [&] {
		std::uint64_t n = 0;
		for (auto &h : hosts)
			n += h->total();
		return n;
	};
	std::uint64_t expected = 0;
	for (int pass = 0; pass != 2; ++pass) {
		for (auto &from : hosts)
			for (auto &to : hosts) {
				if (pass == 0 ? from->node >= to->node : from->node <= to->node)
					continue;
				const auto k = from->transport->loop_of_group(warm);
				from->on(k, [&host = *from, k, node = to->node, warm] {
					for (auto lane : {LinkLane::Control, LinkLane::Bulk})
						host.transport->send(k, lane, FrameKind::control, {warm, host.node, node}, node, payload39);
				});
				expected += 2;
				wait_for([&] { return total() == expected; }, "the mesh comes up");
			}
	}
	std::this_thread::sleep_for(200ms);
	const auto s2 = paused(hosts);
	if (const char *hold = std::getenv("TRANSPORT_IDLE_HOLD")) {  // a heap profiler's moment
		std::printf("holding pid %d for %s s\n", int(::getpid()), hold);
		std::fflush(stdout);
		std::this_thread::sleep_for(std::chrono::seconds(std::atoi(hold)));
	}
	std::uint64_t links = 0;
	for (auto &h : hosts)
		links += h->sum().links;

	// G groups per host, on their loops, each sending a control and a data frame to every peer.
	std::vector<std::vector<GroupId>> on_loop(L);
	for (GroupId g = 1'000'000; g != 1'000'000 + G; ++g)
		on_loop[j::host_detail::loop(j::host_detail::slot(g), L)].push_back(g);
	const auto before_groups = paused(hosts);  // the lists above are the bench's own
	for (auto &from : hosts)
		for (std::uint32_t k = 0; k != L; ++k)
			from->on(k, [&host = *from, &hosts, &groups = on_loop[k], k] {
				for (auto g : groups)
					for (auto &to : hosts)
						if (to->node != host.node)
							for (auto kind : {FrameKind::control, FrameKind::data})
								host.transport->send(k, LinkLane::Control, kind, {g, host.node, to->node}, to->node, payload39);
			});
	expected += std::uint64_t(H) * G * std::uint64_t(P) * 2;
	const bool arrived = wait_for([&] { return total() == expected; }, "every group's frames arrive", 60s, false);
	// Credit returns, then nothing moves: sent and received stay put over 200 ms.
	std::uint64_t moved = 0;
	for (std::uint64_t last = ~0ull;;) {
		std::this_thread::sleep_for(200ms);
		std::uint64_t now = 0;
		for (auto &h : hosts)
			now += h->sum().sent + h->sum().received;
		if (now == last)
			break;
		last = now;
		++moved;
	}
	const auto s3 = paused(hosts);
	std::uint64_t dropped = 0, refused = 0, malformed = 0, links_after = 0;
	for (auto &h : hosts) {
		const auto s = h->sum();
		dropped += s.dropped;
		refused += s.refused;
		malformed += s.malformed;
		links_after += s.links;
	}
	// A connection that ended loses what it carried and its descriptors and timers.
	const bool ended = !arrived || links_after != links || s3.fds < before_groups.fds || s3.timers < before_groups.timers;
	if (ended) {
		std::printf("cost hosts=%d loops=%u: a connection ended during the run (links %llu, then %llu); again\n", H, L,
					(unsigned long long)links, (unsigned long long)links_after);
		watched = nullptr;
		return {P, L, 0, true};
	}

	// An idle host: CPU and wakes while only keepalives run.
	double cpu_us_per_s = 0, wakes_per_s = 0;
	if (idle_seconds > 0) {
		rusage a{}, b{};
		getrusage(RUSAGE_SELF, &a);
		const auto t0 = steady_ns();
		std::this_thread::sleep_for(std::chrono::duration<double>(idle_seconds));
		getrusage(RUSAGE_SELF, &b);
		const double wall = double(steady_ns() - t0) / 1e9;
		auto us = [](const rusage &r) {
			return double(r.ru_utime.tv_sec + r.ru_stime.tv_sec) * 1e6 + double(r.ru_utime.tv_usec + r.ru_stime.tv_usec);
		};
		cpu_us_per_s = (us(b) - us(a)) / wall / H;
		wakes_per_s = double(b.ru_nvcsw - a.ru_nvcsw) / wall / H;
	}

	const double host = double(s2.heap - s0.heap) / H, fixed = double(s1.heap - s0.heap) / H;
	const double end = double(s2.heap - s1.heap) / double(ends);
	const double budget = 29.0 * 1024 + 2.0 * P * 41 * 1024;
	const double group = double(s3.heap - before_groups.heap) / double(G * H);
	const auto host_fds = double(s2.fds - s0.fds) / H, group_fds = double(s3.fds - before_groups.fds);
	const auto end_timers = double(s2.timers - s1.timers) / double(ends), group_timers = double(s3.timers - before_groups.timers);
	if (print) {
		std::printf("cost hosts=%d peers=%d loops=%u groups=%zu: per host %.0f bytes (fixed %.0f, %.0f%% of 29 KB + 2P x 41 KB = %.0f), %.2f descriptors (1 + 2P = %d); per connection end %.0f bytes in %.1f blocks, %.2f armed timers; links %llu of %llu\n",
					H, P, L, G, host, fixed, 100 * host / budget, budget, host_fds, 1 + 2 * P, end, double(s2.blocks - s1.blocks) / double(ends), end_timers, (unsigned long long)links,
					(unsigned long long)ends);
		std::printf("  per connection end, C++ blocks (%.0f bytes):%s\n  per host before any connection, C++ blocks (%.0f bytes):%s\n",
					double(s2.cxx - s1.cxx) / double(ends), sizes(s1, s2, double(ends)).c_str(), double(s1.cxx - s0.cxx) / H,
					sizes(s0, s1, H).c_str());
		std::printf("  per idle group, C++ %.1f bytes, %.3f blocks:%s\n", double(s3.cxx - before_groups.cxx) / double(G * H),
					double(s3.blocks - before_groups.blocks) / double(G * H), sizes(before_groups, s3, double(G * H) / 100).c_str());
		std::printf("  per idle group: %.1f bytes, %.0f descriptors, %.0f armed timers in all; frames %llu delivered of %llu, %llu dropped, %llu refused, %llu malformed; settled after %llu x 200 ms\n",
					group, group_fds, group_timers, (unsigned long long)total(), (unsigned long long)expected, (unsigned long long)dropped,
					(unsigned long long)refused, (unsigned long long)malformed, (unsigned long long)moved);
		if (idle_seconds > 0)
			std::printf("  idle host: %.1f us of CPU per second, %.2f wakes per second (%.2f per connection end)\n", cpu_us_per_s, wakes_per_s,
						wakes_per_s * H / double(ends));
		const std::string at = " (hosts " + std::to_string(H) + ", loops " + std::to_string(L) + ")";
		expect(links == ends && links_after == ends, "one connection per lane and pair" + at);
		expect(total() == expected && dropped == 0 && refused == 0 && malformed == 0, "every frame delivered, none dropped" + at);
		expect(group_fds == 0, "no descriptor per idle group" + at);
		expect(group_timers == 0, "no armed timer per idle group" + at);
		expect(host_fds == 1 + 2 * P, "1 + 2P descriptors per host" + at);
		if (heap_measured) {
			expect(group <= 64, "an idle group within 64 bytes" + at);
			expect(end <= 48 * 1024, "an idle connection end within 48 KB" + at);
			expect(host <= 1.10 * budget, "a host within 10% of 29 KB + 2P x 41 KB" + at);
		}
	}
	watched = nullptr;
	return {P, L, host, false};
}
// A configuration whose connection ended runs again, at most twice.
Cost measured(int H, std::uint32_t L, std::size_t G, double idle_seconds) {
	for (int attempt = 1;; ++attempt) {
		const auto r = cost(H, L, G, idle_seconds, true);
		if (!r.ended)
			return r;
		if (attempt == 3) {
			expect(false, "a run without a connection ending (hosts " + std::to_string(H) + ", loops " + std::to_string(L) + ")");
			return r;
		}
	}
}

// Least squares of bytes per host against 2P: the intercept is what a host holds without peers,
// the slope what each connection end adds.
void fit(const std::vector<Cost> &rows, std::uint32_t L) {
	double n = 0, sx = 0, sy = 0, sxx = 0, sxy = 0;
	for (auto &r : rows)
		if (r.loops == L) {
			const double x = 2.0 * r.peers;
			n += 1, sx += x, sy += r.host, sxx += x * x, sxy += x * r.host;
		}
	if (n < 2)
		return;
	const double slope = (n * sxy - sx * sy) / (n * sxx - sx * sx), intercept = (sy - slope * sx) / n;
	std::printf("fit loops=%u: per host %.0f + 2P x %.0f bytes (29 KB + 2P x 41 KB = 29696 + 2P x 41984)\n", L, intercept, slope);
	if (heap_measured)
		expect(slope <= 48 * 1024, "an idle connection end within 48 KB, by the fit (loops " + std::to_string(L) + ")");
}

int run_cost(bool quick) {
	if (!heap_measured)
		std::printf("(bytes not checked: a sanitizer, or no heap statistics here)\n");
	std::printf("openssl=%s\n", OpenSSL_version(OPENSSL_VERSION));
	cost(2, 1, 100, 0, false);  // OpenSSL's and Asio's one-time state
	std::vector<Cost> rows;
	for (std::uint32_t L : {1u, 4u})
		for (int H : {2, 3, 5})
			if (auto r = measured(H, L, quick ? 2000 : 10000, quick ? 0 : 5); !r.ended)
				rows.push_back(r);
	for (std::uint32_t L : {1u, 4u})
		fit(rows, L);
	std::printf("transport idle cost: %s\n", failures ? "FAILED" : "ok");
	return failures ? 1 : 0;
}

// The hop: host 1 (one loop) sends to host 2 (two loops), whose connection to host 1 is on loop
// R; an arm's groups are on R ("same") or the other loop ("hop").
struct Hop {
	std::vector<std::unique_ptr<Host>> hosts;
	std::uint32_t R = 0;
	std::vector<GroupId> groups[2];  // [0] on R, [1] on the other loop
	std::vector<std::uint64_t> latency[2];
	Hop() {
		hosts.push_back(std::make_unique<Host>(1, 1));
		hosts.push_back(std::make_unique<Host>(2, 2));
		for (auto &h : hosts)
			h->listen();
		for (auto &h : hosts)
			h->open(peers_of(hosts, h->node));
		R = hosts[1]->transport->loop_of_node(1);
		for (GroupId g = 1'000'000; groups[0].size() < 256 || groups[1].size() < 256; ++g) {
			auto &arm = groups[hosts[1]->transport->loop_of_group(g) == R ? 0 : 1];
			if (arm.size() < 256)
				arm.push_back(g);
		}
		latency[0].reserve(4096);
		latency[1].reserve(4096);
		hosts[1]->hook = [this](std::uint32_t k, const Received &r) {
			if (r.payload.size() >= 8 && r.payload[0] == 't') {
				std::string_view v(r.payload);
				v.remove_prefix(1);
				latency[k == R ? 0 : 1].push_back(steady_ns() - j::get64(v));
			}
		};
		one().on(0, [this] { one().transport->send(0, LinkLane::Control, FrameKind::control, {groups[0][0], 1, 2}, 2, payload39); });
		wait_for([&] { return hosts[1]->total() == 1; }, "the connection comes up");
	}
	Host &one() { return *hosts[0]; }
	Host &two() { return *hosts[1]; }
	void send(int arm, FrameKind kind, int per_group) {
		one().on(0, [this, arm, kind, per_group] {
			for (int i = 0; i != per_group; ++i)
				for (auto g : groups[arm])
					one().transport->send(0, LinkLane::Control, kind, {g, 1, 2}, 2, payload39);
		});
	}
	struct Run {
		double ns_per_frame, frames_per_post, sender_ns_per_frame;
	};
	// Receiver CPU across both loops per frame, for `frames` frames of the arm.
	template <class Drive> Run measure(std::uint64_t frames, Drive drive) {
		const auto r0 = two().loops[0]->cpu() + two().loops[1]->cpu(), s0 = one().loops[0]->cpu();
		const auto p0 = two().sum().posts, d0 = two().total();
		drive();
		wait_for([&] { return two().total() == d0 + frames; }, "the run's frames arrive");
		const auto r1 = two().loops[0]->cpu() + two().loops[1]->cpu(), s1 = one().loops[0]->cpu();
		const auto posts = two().sum().posts - p0;
		return {double(r1 - r0) / double(frames), posts ? double(frames) / double(posts) : 0, double(s1 - s0) / double(frames)};
	}
	// 200 rounds of 60 frames for each of the arm's 256 groups, each round once the last arrived.
	Run stream(int arm, FrameKind kind) {
		constexpr int rounds = 200, per_group = 60;  // within a group's 64 queued control frames
		const std::uint64_t round = std::uint64_t(per_group) * groups[arm].size();
		return measure(round * rounds, [&] {
			const auto d0 = two().total();
			for (int r = 0; r != rounds; ++r) {
				send(arm, kind, per_group);
				wait_for([&] { return two().total() == d0 + round * std::uint64_t(r + 1); }, "a round arrives");
			}
		});
	}
	// One frame every 1 ms: each read carries one frame, and the group loop sleeps between them.
	Run alone(int arm, std::uint64_t frames = 2000) {
		latency[arm].clear();
		return measure(frames, [&] {
			const auto start = std::chrono::steady_clock::now();
			for (std::uint64_t i = 0; i != frames; ++i) {
				std::this_thread::sleep_until(start + std::chrono::milliseconds(i));
				one().on(0, [this, arm, g = groups[arm][i % groups[arm].size()]] {
					std::string p = "t";
					j::put64(p, steady_ns());
					p.resize(payload39.size(), 'p');
					one().transport->send(0, LinkLane::Control, FrameKind::control, {g, 1, 2}, 2, p);
				});
			}
		});
	}
	std::uint64_t p50(int arm) {
		auto v = two().loops[arm == 0 ? R : 1 - R]->at([&] { return latency[arm]; });
		std::sort(v.begin(), v.end());
		return v.empty() ? 0 : v[v.size() / 2];
	}
};

double median(std::vector<double> v) {
	std::sort(v.begin(), v.end());
	const auto n = v.size();
	return n % 2 ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2;
}

int run_hop(int rounds) {
	std::printf("openssl=%s\n", OpenSSL_version(OPENSSL_VERSION));
	Hop hop;
	std::printf("host 2: connection on loop %u; groups same=%zu hop=%zu\n", hop.R, hop.groups[0].size(), hop.groups[1].size());
	hop.stream(0, FrameKind::control);  // warm-up
	hop.stream(1, FrameKind::control);
	struct Series {
		const char *name;
		std::vector<double> effect, aa, same, hopped, fpp_same, fpp_hop;
	};
	Series series[3] = {{"stream control"}, {"stream data"}, {"alone, 1 ms apart"}};
	std::vector<double> lat_same, lat_hop;
	for (int round = 0; round != rounds; ++round) {
		for (int s = 0; s != 3; ++s) {
			auto one = [&](int arm) {
				if (s == 2) {
					auto r = hop.alone(arm);
					(arm ? lat_hop : lat_same).push_back(double(hop.p50(arm)));
					return r;
				}
				return hop.stream(arm, s == 0 ? FrameKind::control : FrameKind::data);
			};
			const auto a1 = one(0), b1 = one(1), b2 = one(1), a2 = one(0);  // same, hop, hop, same
			auto &x = series[s];
			const double same = (a1.ns_per_frame + a2.ns_per_frame) / 2, hopped = (b1.ns_per_frame + b2.ns_per_frame) / 2;
			x.same.push_back(same);
			x.hopped.push_back(hopped);
			x.effect.push_back(hopped - same);
			x.aa.push_back(std::max(std::abs(a2.ns_per_frame - a1.ns_per_frame), std::abs(b2.ns_per_frame - b1.ns_per_frame)));
			x.fpp_same.push_back((a1.frames_per_post + a2.frames_per_post) / 2);
			x.fpp_hop.push_back((b1.frames_per_post + b2.frames_per_post) / 2);
			std::printf("round %d %s: same %.0f %.0f, hop %.0f %.0f ns of receiver CPU per frame; hop - same %.0f; frames per post %.1f (sender %.0f ns per frame)\n",
						round + 1, x.name, a1.ns_per_frame, a2.ns_per_frame, b1.ns_per_frame, b2.ns_per_frame, hopped - same,
						x.fpp_hop.back(), (a1.sender_ns_per_frame + b1.sender_ns_per_frame) / 2);
		}
	}
	for (auto &x : series) {
		const auto [lo, hi] = std::minmax_element(x.effect.begin(), x.effect.end());
		const double band = *std::max_element(x.aa.begin(), x.aa.end());
		std::printf("hop %s: same %.0f, hop %.0f ns per frame (medians of %d rounds); hop - same median %.0f [%.0f, %.0f], A/A band %.0f: %s; frames per post %.1f\n",
					x.name, median(x.same), median(x.hopped), rounds, median(x.effect), *lo, *hi, band,
					std::abs(median(x.effect)) > band ? "clears the band" : "within the band", median(x.fpp_hop));
	}
	std::printf("alone, latency p50 (send to delivery, medians of rounds): same %.1f us, hop %.1f us\n", median(lat_same) / 1000, median(lat_hop) / 1000);
	std::printf("transport hop: ok\n");
	return 0;
}
}  // namespace

int main(int argc, char **argv) {
	if (argc < 3) {
		std::fprintf(stderr, "usage: consensus_transport_idle <fixture dir> cost [--quick] | hop [rounds]\n");
		return 2;
	}
#ifndef SO_NOSIGPIPE
	std::signal(SIGPIPE, SIG_IGN);
#endif
	fixture = argv[1];
	const std::string mode = argv[2];
	if (mode == "cost")
		return run_cost(argc > 3 && std::string(argv[3]) == "--quick");
	if (mode == "one" && argc > 5) {  // one configuration: hosts loops groups
		cost(2, 1, 100, 0, false);
		measured(std::atoi(argv[3]), std::uint32_t(std::atoi(argv[4])), std::size_t(std::atoll(argv[5])), 0);
		return failures ? 1 : 0;
	}
	if (mode == "hop")
		return run_hop(argc > 3 ? std::atoi(argv[3]) : 7);
	std::fprintf(stderr, "unknown mode %s\n", mode.c_str());
	return 2;
}
