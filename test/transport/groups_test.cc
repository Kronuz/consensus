// The host transport over loopback with the TLS fixture, every loop on its own thread:
//   threads  two hosts of 4 loops exchange 64 groups' frames both ways: each arrives on its
//            group's loop, in order, batched per loop and posted once per read (run under TSan)
//   credit   a group whose loop is stalled holds only its own window; another group's
//            heartbeats go on
//   reader   reader_test: a group with full admission for 3 s, whose frames are refused, never
//            delays another group's heartbeat on the same loop past the 1,000 ms election timeout
//   upload   a maximal upload (2 MiB appends of 1 MiB commands) to a reader throttled to 40 MB/s
//            never delays another group's heartbeat or vote past the election timeout
//   driver   three Drivers' Sinks over the transport: a three-voter group elects, commits and
//            survives a peer's restart
//   refill   credit comes back: for refused frames, and with a new connection after the
//            receiver restarts with the window full
// A timing case the host stalled (a 250 ms oversleep) runs again, at most twice. Each step prints
// as it starts; a case still running after 100 s prints its step, which loops stopped turning
// and every thread's stack, then exits 3.
//   consensus_transport_groups_test <fixture dir> case
#include "consensus/driver.h"
#include "consensus/transport/host_transport.h"
#include "journal/memory_io.h"
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <algorithm>
#include <cassert>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <mutex>
#include <openssl/pem.h>
#include <thread>
#include <unistd.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif

using namespace kronuz::consensus::transport;
using namespace kronuz::consensus;
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
using steady = std::chrono::steady_clock;
namespace j = kronuz::journal;
namespace jt = kronuz::journal::testing;

namespace {
std::filesystem::path fixture;
const char *names[] = {"", "one", "two", "three"};

Fingerprint pin_of(const std::string &name) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file((fixture / (name + ".pem")).c_str(), "r"), BIO_free);
	std::unique_ptr<X509, decltype(&X509_free)> leaf(PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
	assert(leaf);
	return certificate_fingerprint(leaf.get());
}
std::shared_ptr<asio::ssl::context> context(const std::string &name) {
	auto c = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	reactor::tls::configure(*c, {(fixture / (name + ".pem")).string(), (fixture / (name + ".key")).string(),
								 (fixture / "ca.pem").string()},
							reactor::tls::Role::Peer);
	return c;
}
std::uint64_t ms_since(steady::time_point t) {
	return std::uint64_t(std::chrono::duration_cast<std::chrono::milliseconds>(steady::now() - t).count());
}

// The step the case is in, printed as it starts and named by the watchdog.
const steady::time_point began = steady::now();
std::mutex step_mutex;
std::string current_step = "start";
void step(std::string what) {
	std::lock_guard hold(step_mutex);
	std::printf("[%6llu ms] %s\n", (unsigned long long)ms_since(began), what.c_str());
	current_step = std::move(what);
}

// One loop on its own thread.
struct Loop {
	asio::io_context io;
	task::asio_scheduler scheduler{io.get_executor()};
	asio::executor_work_guard<asio::io_context::executor_type> work = asio::make_work_guard(io);
	std::thread thread;
	void start() {
		thread = std::thread([this] { io.run(); });
	}
	void join() {
		work.reset();
		io.stop();
		if (thread.joinable())
			thread.join();
	}
	// Runs f on the loop and returns its result.
	template <class F> auto at(F f) -> decltype(f()) {
		std::packaged_task<decltype(f())()> call(std::move(f));
		auto result = call.get_future();
		asio::post(io, [&call] { call(); });
		return result.get();
	}
	~Loop() { join(); }
};

// One host: L loops, a listener on loop 0 and the transport.
struct Host {
	NodeId node;
	std::vector<std::unique_ptr<Loop>> loops;
	tcp::acceptor acceptor;
	std::unique_ptr<HostTransport> transport;
	bool running = false, quiet = false;
	Host(NodeId n, std::uint32_t L, std::uint16_t port = 0) : node(n), loops(make(L)), acceptor(loops[0]->io, {asio::ip::address_v4::loopback(), port}) {
		std::lock_guard hold(hosts_mutex);
		hosts.push_back(this);
	}
	static std::vector<std::unique_ptr<Loop>> make(std::uint32_t L) {
		std::vector<std::unique_ptr<Loop>> loops;
		for (std::uint32_t k = 0; k != L; ++k)
			loops.push_back(std::make_unique<Loop>());
		return loops;
	}
	std::uint16_t port() const { return acceptor.local_endpoint().port(); }
	void open(const std::map<NodeId, PeerAddress> &addresses, HostTransportOptions options = {}) {
		auto tls = context(names[node]);
		std::map<NodeId, Fingerprint> pins{{1, pin_of("one")}, {2, pin_of("two")}, {3, pin_of("three")}};
		std::vector<HostLoop> at;
		for (auto &l : loops)
			at.push_back({l->io.get_executor(), task::scheduler_ref::of(l->scheduler)});
		Identity cluster{};
		cluster.fill('K');
		transport = std::make_unique<HostTransport>(at, tls, NodePins(node, pins, tls->native_handle()), cluster, addresses, options);
	}
	void start() {
		transport->serve(acceptor);
		for (auto &l : loops)
			l->start();
		running = true;
	}
	// Runs f on loop k, the group's, without waiting.
	template <class F> void post(std::uint32_t k, F f) { asio::post(loops[k]->io, std::move(f)); }
	// The transport stops and drains; the loops keep running.
	void quiesce() {
		if (!running || quiet)
			return;
		quiet = true;
		step("host " + std::to_string(node) + ": the transport stops");
		transport->stop();
		for (std::uint32_t k = 0; k != loops.size(); ++k) {
			step("host " + std::to_string(node) + ": loop " + std::to_string(k) + " drains");
			for (int i = 0; !loops[k]->at([&] { return transport->drained(k); }); ++i) {
				assert(i < 2000 && "transport drains");
				std::this_thread::sleep_for(5ms);
			}
		}
	}
	void stop() {
		if (!running)
			return;
		quiesce();
		step("host " + std::to_string(node) + ": the loops join");
		for (auto &l : loops)
			l->join();
		running = false;
	}
	~Host() {
		stop();
		std::lock_guard hold(hosts_mutex);
		std::erase(hosts, this);
	}
	// The live hosts, for the watchdog.
	static inline std::mutex hosts_mutex;
	static inline std::vector<Host *> hosts;
};

// What a case that hangs leaves: its step, each host's loops (turning, blocked or stopped) with
// their counters, and every thread's stack.
void report_hang() {
	{
		std::lock_guard hold(step_mutex);
		std::printf("HUNG after %llu ms in step: %s\n", (unsigned long long)ms_since(began), current_step.c_str());
	}
	{
		std::lock_guard hold(Host::hosts_mutex);
		for (auto *h : Host::hosts)
			for (std::uint32_t k = 0; k != h->loops.size(); ++k) {
				auto &loop = *h->loops[k];
				auto turned = std::make_shared<std::atomic<bool>>(false);
				if (!loop.io.stopped())
					asio::post(loop.io, [turned] { *turned = true; });
				for (int i = 0; i != 100 && !*turned; ++i)
					std::this_thread::sleep_for(10ms);
				const char *state = loop.io.stopped() ? "stopped" : *turned ? "turning" : "BLOCKED for 1 s";
				std::printf("host %llu loop %u: %s", (unsigned long long)h->node, k, state);
				if (h->transport) {
					const auto &s = h->transport->stats(k);
					std::printf("; links %llu sent %llu received %llu reads %llu posts %llu dropped %llu refused %llu",
								(unsigned long long)s.links, (unsigned long long)s.sent, (unsigned long long)s.received,
								(unsigned long long)s.reads, (unsigned long long)s.posts, (unsigned long long)s.dropped,
								(unsigned long long)s.refused);
				}
				std::printf("\n");
			}
	}
	const auto pid = std::to_string(::getpid());
#ifdef __APPLE__
	const std::string stacks = "sample " + pid + " 1 -file /dev/stdout 2>&1";
#else
	const std::string stacks = "gdb -p " + pid + " -batch -ex 'thread apply all bt' 2>&1";
#endif
	std::fflush(stdout);
	if (std::system(stacks.c_str()) != 0)
		std::printf("(no stacks: %s failed)\n", stacks.c_str());
	std::fflush(stdout);
}
// Reports a hang and exits 3 once `limit` passes, unless done() came first.
class Watchdog {
  public:
	explicit Watchdog(std::chrono::seconds limit)
		: thread_([this, limit] {
			  std::unique_lock lock(mutex_);
			  if (wake_.wait_for(lock, limit, [this] { return done_; }))
				  return;
			  lock.unlock();
			  report_hang();
			  std::_Exit(3);
		  }) {}
	~Watchdog() {
		{
			std::lock_guard hold(mutex_);
			done_ = true;
		}
		wake_.notify_all();
		thread_.join();
	}

  private:
	std::mutex mutex_;
	std::condition_variable wake_;
	bool done_ = false;
	std::thread thread_;
};
std::map<NodeId, PeerAddress> addresses(std::initializer_list<const Host *> hosts) {
	std::map<NodeId, PeerAddress> r;
	for (auto *h : hosts)
		r[h->node] = {"127.0.0.1", h->port()};
	return r;
}
// The first group ids from 1000 that place on loop `k` of L.
std::vector<GroupId> groups_on(std::uint32_t L, std::uint32_t k, std::size_t n) {
	std::vector<GroupId> r;
	for (GroupId g = 1000; r.size() < n; ++g)
		if (j::host_detail::loop(j::host_detail::slot(g), L) == k)
			r.push_back(g);
	return r;
}
// Heartbeats as one loop sees them: how many, and the longest time between two.
struct Beats {
	steady::time_point last{};
	std::uint64_t worst = 0;
	std::atomic<std::uint64_t> count{0};
	void beat() {
		const auto now = steady::now();
		if (last != steady::time_point{})
			worst = std::max<std::uint64_t>(worst, std::chrono::duration_cast<std::chrono::milliseconds>(now - last).count());
		last = now;
		++count;
	}
};
// Sends `send` from the test thread until `arrived` holds: the connection is up.
template <class S, class A> void warm(S send, A arrived) {
	for (int i = 0; !arrived(); ++i) {
		assert(i < 1000 && "the connection comes up");
		send();
		std::this_thread::sleep_for(10ms);
	}
}
std::string number(std::uint64_t v) {
	std::string s;
	j::put64(s, v);
	return s;
}
std::uint64_t number(std::string_view s) { return j::get64(s); }

// The longest oversleep of a 10 ms nap while it runs: how long the host stopped this process.
struct Stalls {
	std::atomic<bool> done{false};
	std::atomic<std::uint64_t> worst{0};
	std::thread thread{[this] {
		while (!done) {
			const auto t = steady::now();
			std::this_thread::sleep_for(10ms);
			worst = std::max<std::uint64_t>(worst, ms_since(t) - std::min<std::uint64_t>(ms_since(t), 10));
		}
	}};
	std::uint64_t stop() {
		done = true;
		thread.join();
		return worst;
	}
};
// Runs a timing case; one the host stalled for 250 ms or more runs again, at most twice.
void timed(const char *name, const std::function<bool()> &run) {
	for (int attempt = 1;; ++attempt) {
		step(std::string(name) + ": attempt " + std::to_string(attempt));
		Stalls stalls;
		const bool ok = run();
		const auto worst = stalls.stop();
		if (ok)
			return;
		if (worst < 250 || attempt == 3) {
			std::printf("%s: FAILED (worst stall %llu ms)\n", name, (unsigned long long)worst);
			std::exit(1);
		}
		std::printf("%s: the host stalled %llu ms; again\n", name, (unsigned long long)worst);
	}
}

// Two hosts of 4 loops, 64 groups sending 80 frames each both ways from their own loops, every
// fourth one data: each arrives on its group's loop and in order within its kind.
void threads() {
	constexpr std::uint32_t L = 4;
	constexpr std::uint64_t per_group = 80;  // 60 control frames: within a group's queue before the link is up
	Host one(1, L), two(2, L);
	one.open(addresses({&two}));
	two.open(addresses({&one}));
	struct Seen {
		std::uint64_t control = 0, data = 0;
	};
	std::map<NodeId, std::map<GroupId, Seen>> seen;  // each group's entry is written on its loop only
	std::atomic<std::uint64_t> delivered{0}, misplaced{0}, disordered{0};
	for (Host *h : {&one, &two}) {
		for (GroupId g = 1; g <= 64; ++g)
			seen[h->node][g];
		for (std::uint32_t k = 0; k != L; ++k)
			h->transport->on_deliver(k, [h, k, &seen, &delivered, &misplaced, &disordered](const Received &r) {
				if (h->transport->loop_of_group(r.route.group) != k || !h->loops[k]->io.get_executor().running_in_this_thread() ||
					r.route.destination != h->node || r.route.source != r.node)
					++misplaced;
				auto &s = seen.at(h->node).at(r.route.group);
				if (number(r.payload) != (r.data ? s.data++ : s.control++))
					++disordered;
				++delivered;
				return true;
			});
		h->start();
	}
	for (Host *h : {&one, &two}) {
		const NodeId peer = h->node == 1 ? 2 : 1;
		for (std::uint32_t k = 0; k != L; ++k)
			h->post(k, [h, k, peer] {
				std::uint64_t control[65] = {}, data[65] = {};
				for (std::uint64_t i = 0; i != per_group; ++i)
					for (GroupId g = 1; g <= 64; ++g)
						if (h->transport->loop_of_group(g) == k) {
							const bool d = i % 4 == 3;
							h->transport->send(k, LinkLane::Control, d ? FrameKind::data : FrameKind::control, {g, h->node, peer}, peer,
											   number(d ? data[g]++ : control[g]++));
						}
			});
	}
	step("threads: 64 groups' frames both ways");
	const auto start = steady::now();
	while (delivered < 2 * 64 * per_group && ms_since(start) < 20'000)
		std::this_thread::sleep_for(10ms);
	std::uint64_t reads = 0, posts = 0, received = 0;
	for (Host *h : {&one, &two})
		for (std::uint32_t k = 0; k != L; ++k) {
			reads += h->transport->stats(k).reads;
			posts += h->transport->stats(k).posts;
			received += h->transport->stats(k).received;
			assert(h->transport->stats(k).posts <= h->transport->stats(k).reads * (L - 1));
			// One Control connection, on the peer's loop at both ends, whichever end accepted it.
			assert(h->transport->stats(k).links == (k == h->transport->loop_of_node(h->node == 1 ? 2 : 1)));
		}
	std::printf("threads: 2 hosts x %u loops, delivered %llu of %llu, misplaced %llu, out of order %llu; %llu frames in %llu reads, %llu cross-loop posts (%.1f frames a post)\n",
				L, (unsigned long long)delivered.load(), (unsigned long long)(2 * 64 * per_group), (unsigned long long)misplaced.load(),
				(unsigned long long)disordered.load(), (unsigned long long)received, (unsigned long long)reads, (unsigned long long)posts,
				posts ? double(received) * (L - 1) / L / double(posts) : 0.0);
	assert(delivered == 2 * 64 * per_group && !misplaced && !disordered);
	one.stop();
	two.stop();
}

// Group 1's loop on the receiver stalls 1.5 s while its sender queues 40 data frames of 64 KiB
// against a 256 KiB window: the receiver holds at most the window of them, group 2's
// heartbeats on another loop keep arriving, and every frame arrives once the loop resumes.
bool credit() {
	constexpr std::uint32_t L = 2;
	HostTransportOptions options;
	options.window = 256 * 1024;
	Host one(1, L), two(2, L);
	one.open(addresses({&two}), options);
	two.open(addresses({&one}), options);
	// Group 2 runs on the loop that reads node 1's connection on host 2; group 1 on the other.
	const auto reading = two.transport->loop_of_node(1), stalled = 1 - reading;
	const auto g1 = groups_on(L, stalled, 1)[0], g2 = groups_on(L, reading, 1)[0];
	std::atomic<std::uint64_t> g1_delivered{0}, held_at_resume{0}, disordered{0};
	Beats g2_beats;  // on loop `reading`
	two.transport->on_deliver(stalled, [&](const Received &r) {
		if (!g1_delivered)  // the first after the stall: what arrived at the reader meanwhile
			held_at_resume = two.transport->stats(reading).received - g2_beats.count;
		disordered += number(r.payload) != g1_delivered++;
		return true;
	});
	two.transport->on_deliver(reading, [&](const Received &) {
		g2_beats.beat();
		return true;
	});
	one.start();
	two.start();
	auto heartbeat = [&] {
		const auto k = one.transport->loop_of_group(g2);
		one.post(k, [&, k] { one.transport->send(k, LinkLane::Control, FrameKind::control, {g2, 1, 2}, 2, "heartbeat"); });
	};
	step("credit: the connection comes up");
	warm(heartbeat, [&] { return g2_beats.count > 0; });
	step("credit: group 1's loop stalls 1.5 s under 40 frames");
	const auto before = g2_beats.count.load();
	two.post(stalled, [] { std::this_thread::sleep_for(1500ms); });
	const auto start = steady::now();
	one.post(one.transport->loop_of_group(g1), [&] {
		for (std::uint64_t i = 0; i != 40; ++i)
			one.transport->send(one.transport->loop_of_group(g1), LinkLane::Control, FrameKind::data, {g1, 1, 2}, 2,
								number(i) + std::string(64 * 1024 - 8, 'x'));
	});
	while ((ms_since(start) < 2500 || g1_delivered < 40) && ms_since(start) < 15'000) {
		heartbeat();
		std::this_thread::sleep_for(50ms);
	}
	one.stop();
	two.stop();
	held_at_resume -= std::min(held_at_resume.load(), before);
	const auto g2_gap = g2_beats.worst;
	const auto g2_delivered = g2_beats.count - before;
	const auto window_frames = options.window / (64 * 1024 + frame_header) + 1;
	std::printf("credit: group 1's loop stalled 1.5 s; the reader held %llu of its 40 frames (window %llu frames), all %llu arrived in order (%llu out); group 2 got %llu heartbeats, longest gap %llu ms\n",
				(unsigned long long)held_at_resume.load(), (unsigned long long)window_frames, (unsigned long long)g1_delivered.load(),
				(unsigned long long)disordered.load(), (unsigned long long)g2_delivered, (unsigned long long)g2_gap);
	assert(g1_delivered == 40 && !disordered);
	return held_at_resume <= window_frames + 2 && g2_gap < 500;
}

// reader_test (async-drivers D10): group 1's admission is full for 3 s, so its frames are refused,
// while its leader sends an append every 10 ms; group 2, on the same loop, gets a heartbeat every
// 100 ms. Its longest gap stays under its 1,000 ms election timeout.
bool reader() {
	constexpr std::uint32_t L = 2;
	Host one(1, L), two(2, L);
	one.open(addresses({&two}));
	two.open(addresses({&one}));
	// Both groups run on the loop where host 2 reads node 1's connection.
	const auto reading = two.transport->loop_of_node(1);
	const auto ids = groups_on(L, reading, 2);
	const GroupId g1 = ids[0], g2 = ids[1];
	std::uint64_t delivered_1 = 0, refused_1 = 0;  // on loop `reading`
	Beats beats_2;
	std::atomic<bool> busy{false};
	steady::time_point start;
	two.transport->on_deliver(reading, [&](const Received &r) {
		if (r.route.group == g1) {
			if (busy && ms_since(start) < 3000) {
				++refused_1;
				return false;
			}
			++delivered_1;
			return true;
		}
		beats_2.beat();
		return true;
	});
	one.start();
	two.start();
	AppendRequest append{1, 1, 0, 0, 0, 0, {{1, 1, EntryKind::Command, std::string(1024, 'a')}}};
	AppendRequest heartbeat{1, 1, 0, 0, 0, 0, {}};
	const auto hb = one.transport->loop_of_group(g2);
	step("reader: the connection comes up");
	warm([&] { one.post(hb, [&] { send_message(*one.transport, hb, g2, 1, 2, Send{2, heartbeat}); }); },
		 [&] { return beats_2.count > 0; });
	step("reader: 5 s of appends and heartbeats");
	start = steady::now();
	busy = true;
	// A signed tick: libc++'s steady sleep_until subtracts in the time point's own rep, so with an
	// unsigned one a slot already past (a loaded host) sleeps for centuries.
	for (std::int64_t tick = 0; ms_since(start) < 5000; ++tick) {
		const auto at = one.transport->loop_of_group(g1);
		one.post(at, [&, at] { send_message(*one.transport, at, g1, 1, 2, Send{2, append}); });
		if (tick % 10 == 0)
			one.post(hb, [&] { send_message(*one.transport, hb, g2, 1, 2, Send{2, heartbeat}); });
		std::this_thread::sleep_until(start + (tick + 1) * 10ms);
	}
	std::this_thread::sleep_for(200ms);
	step("reader: the hosts stop");
	one.stop();
	two.stop();
	const bool ok = beats_2.worst < 1000 && refused_1 && delivered_1;
	std::printf("reader_test: group2_heartbeats=%llu group2_longest_gap_ms=%llu group1_delivered=%llu group1_refused=%llu %s\n",
				(unsigned long long)beats_2.count.load(), (unsigned long long)beats_2.worst, (unsigned long long)delivered_1,
				(unsigned long long)refused_1, ok ? "ok" : "FAILS: group 2 would start an election");
	return ok;
}

// A2: four groups upload 2 MiB appends of two 1 MiB commands (types.h's command and RPC caps) on
// the Control connection, faster than the receiver reads them (25 ms a MiB, 40 MB/s, on the
// connection's own loop). A follower group's heartbeat every 100 ms and a vote every 300 ms on
// the same connection each arrive within the 1,000 ms election timeout.
bool upload() {
	constexpr std::uint32_t L = 2;
	HostTransportOptions options;
	options.window = 8 << 20;
	Host one(1, L), two(2, L);
	one.open(addresses({&two}), options);
	two.open(addresses({&one}), options);
	const auto reader_loop = two.transport->loop_of_node(1), other = 1 - reader_loop;
	const auto uploads = groups_on(L, reader_loop, 4);
	const auto followers = groups_on(L, other, 2);
	std::atomic<std::uint64_t> uploaded{0};
	Beats beats;  // on loop `other`
	two.transport->on_deliver(reader_loop, [&](const Received &r) {
		std::this_thread::sleep_for(std::chrono::microseconds(r.payload.size() * 25'000 / (1 << 20)));
		uploaded += r.payload.size();
		return true;
	});
	two.transport->on_deliver(other, [&](const Received &) {
		beats.beat();
		return true;
	});
	one.start();
	two.start();
	const std::string command(Limits{}.command_bytes, 'c');
	const AppendRequest chunk{1, 1, 0, 0, 0, 0, {{1, 1, EntryKind::Command, command}, {2, 1, EntryKind::Command, command}}};
	const AppendRequest heartbeat{1, 1, 0, 0, 0, 0, {}};
	const VoteRequest vote{2, 0, 0};
	const auto f = one.transport->loop_of_group(followers[0]), v = one.transport->loop_of_group(followers[1]);
	step("upload: the connection comes up");
	warm([&] { one.post(f, [&] { send_message(*one.transport, f, followers[0], 1, 2, Send{2, heartbeat}); }); },
		 [&] { return beats.count > 0; });
	step("upload: 4 s of uploads, heartbeats and votes");
	const auto before = beats.count.load();
	const auto start = steady::now();
	for (std::int64_t tick = 0; ms_since(start) < 4000; ++tick) {  // signed: see reader()
		for (auto g : uploads) {
			const auto k = one.transport->loop_of_group(g);
			one.post(k, [&, g, k] { send_message(*one.transport, k, g, 1, 2, Send{2, chunk}); });
		}
		one.post(f, [&] { send_message(*one.transport, f, followers[0], 1, 2, Send{2, heartbeat}); });
		if (tick % 3 == 0)
			one.post(v, [&] { send_message(*one.transport, v, followers[1], 1, 2, Send{2, vote}); });
		std::this_thread::sleep_until(start + (tick + 1) * 100ms);
	}
	const auto elapsed = ms_since(start);
	two.post(other, [&] { beats.beat(); });  // a silence at the end counts
	std::this_thread::sleep_for(100ms);
	step("upload: the hosts stop");
	one.stop();
	two.stop();
	const auto controls = beats.count - before, gap = beats.worst;
	const bool ok = gap < 1000 && controls >= 40;
	std::printf("upload: %.1f MiB uploaded in %llu ms on the shared connection; %llu heartbeats and votes, longest gap %llu ms %s\n",
				double(uploaded) / (1 << 20), (unsigned long long)elapsed, (unsigned long long)controls, (unsigned long long)gap,
				ok ? "ok" : "FAILS: a follower would start an election");
	return ok;
}

// A3: three nodes, each 2 loops with a Driver per loop (and its own in-memory journal), the
// Drivers' Sends over the transport.
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 32, 3};

struct Node;
struct GroupSink {
	Node *node = nullptr;
	std::uint32_t loop = 0;
	void operator()(GroupId g, Send n);
	void operator()(GroupId g, Committed n);
	template <class N> void operator()(GroupId, N &&) {}
};
struct Node {
	struct PerLoop {
		std::unique_ptr<j::Host> journal;
		j::InlineCompletion io;
		GroupSink sink;
		std::unique_ptr<Driver<GroupSink>> driver;
		task::cancel_source stop;
		std::atomic<bool> turns_done{false}, driver_done{false};
	};
	Host host;
	std::vector<std::unique_ptr<PerLoop>> per;
	std::mutex mutex;
	std::map<GroupId, std::vector<std::string>> applied;
	Node(NodeId id, std::uint16_t port, std::vector<jt::Disk> &disks, const std::vector<GroupId> &groups,
		 const std::map<NodeId, PeerAddress> &peers)
		: host(id, 2, port) {
		host.open(peers);
		for (std::uint32_t k = 0; k != 2; ++k) {
			auto p = std::make_unique<PerLoop>();
			p->journal = std::make_unique<j::Host>(std::make_shared<jt::MemoryIO>(disks[k], "host/"), j::Identity{'N', char('0' + id)},
												   j::HostOptions{1, turn_options});
			p->sink = {this, k};
			auto ref = task::scheduler_ref::of(host.loops[k]->scheduler);
			p->driver = std::make_unique<Driver<GroupSink>>(Seams{ref, j::completion_ref::of(p->io), *p->journal, 0, id * 31 + k}, p->sink);
			task::spawn(ref, p->journal->driver(0).run(j::completion_ref::of(p->io), p->stop.token()), [q = p.get()](std::exception_ptr) { q->turns_done = true; });
			task::spawn(ref, p->driver->run(p->stop.token()), [q = p.get()](std::exception_ptr) { q->driver_done = true; });
			host.transport->on_deliver(k, [this, k](const Received &r) {
				auto receive = receive_message(r);
				if (!receive || r.route.source != r.node || r.route.destination != host.node)
					return false;  // fixed voters: each incarnation is its node
				per[k]->driver->receive(r.route.group, std::move(*receive));
				return true;
			});
			per.push_back(std::move(p));
		}
		host.start();
		for (auto g : groups) {
			const auto k = host.transport->loop_of_group(g);
			auto *driver = per[k]->driver.get();
			std::promise<void> opened;
			auto done = opened.get_future();
			host.post(k, [&, driver, g, k] {
				GroupOptions o;
				o.configuration.local = host.node;
				o.configuration.voters = {1, 2, 3};
				o.configuration.cluster[0] = 'T';
				o.configuration.configuration[0] = 'G';
				o.admission = admission;
				o.scheduling = WorkerLimits{16, 128};
				driver->open(g, std::move(o));
				auto opening = per[k]->journal->status(g) == j::GroupStatus::stored
								   ? driver->recover(g, [](j::ArtifactReader &) {})
								   : driver->create(g, j::Identity{'G', char('0' + host.node)});
				task::spawn(task::scheduler_ref::of(host.loops[k]->scheduler), std::move(opening), [&opened](std::exception_ptr e) {
					assert(!e && "create or recover");
					opened.set_value();
				});
			});
			done.wait();
		}
	}
	std::optional<Driver<GroupSink>::GroupState> state(GroupId g) {
		const auto k = host.transport->loop_of_group(g);
		return host.loops[k]->at([&] { return per[k]->driver->state(g); });
	}
	// Proposes `command` here and waits for its commit: false on any error or after 3 s.
	bool propose(GroupId g, std::string command) {
		const auto k = host.transport->loop_of_group(g);
		auto *driver = per[k]->driver.get();
		auto done = std::make_shared<std::promise<bool>>();
		auto result = done->get_future();
		auto work = [](Driver<GroupSink> *d, GroupId g, std::string c) -> task::task<void> {
			auto placed = co_await d->propose(g, std::move(c));
			co_await d->committed(g, placed);
		};
		host.post(k, [=, this] {
			task::spawn(task::scheduler_ref::of(host.loops[k]->scheduler), work(driver, g, command), [done](std::exception_ptr e) { done->set_value(!e); });
		});
		return result.wait_for(3s) == std::future_status::ready && result.get();
	}
	bool has(GroupId g, const std::string &command) {
		std::lock_guard hold(mutex);
		auto &a = applied[g];
		return std::find(a.begin(), a.end(), command) != a.end();
	}
	// The transport stops, then each Driver, which is destroyed on its loop, then the loops.
	~Node() {
		host.quiesce();
		for (std::uint32_t k = 0; k != per.size(); ++k) {
			auto &p = *per[k];
			host.loops[k]->at([&] { p.stop.request(task::cancel_reason::shutdown); });
			for (int i = 0; !(p.turns_done && p.driver_done); ++i) {
				assert(i < 2000 && "the Driver stops");
				std::this_thread::sleep_for(5ms);
			}
			host.loops[k]->at([&] {
				p.driver.reset();
				p.journal.reset();
			});
		}
		host.stop();
	}
};
void GroupSink::operator()(GroupId g, Send n) { send_message(*node->host.transport, loop, g, node->host.node, n.peer, n); }
void GroupSink::operator()(GroupId g, Committed n) {
	Index last = 0;
	{
		std::lock_guard hold(node->mutex);
		for (auto &e : n.entries) {
			if (e.kind == EntryKind::Command)
				node->applied[g].push_back(e.payload);
			last = e.index;
		}
	}
	if (last)
		node->host.post(loop, [n = node, k = loop, g, last] {
			if (auto &d = n->per[k]->driver)
				d->applied(g, Applied{last});
		});
}

// Proposes `command` through whichever node leads, until every node in `nodes` applied it.
bool commit_everywhere(std::vector<Node *> nodes, GroupId g, const std::string &command, std::uint64_t limit_ms) {
	const auto start = steady::now();
	bool committed = false;
	while (ms_since(start) < limit_ms) {
		if (!committed)
			for (auto *n : nodes) {
				auto s = n->state(g);
				if (s && s->role == Role::Leader && n->propose(g, command)) {
					committed = true;
					break;
				}
			}
		if (committed && std::all_of(nodes.begin(), nodes.end(), [&](Node *n) { return n->has(g, command); }))
			return true;
		std::this_thread::sleep_for(50ms);
	}
	return false;
}

void driver() {
	std::vector<jt::Disk> disks[4];
	for (auto &d : disks)
		d.resize(2);
	// Ports first, so every node knows every address; a restarted node binds its port again.
	std::uint16_t ports[4] = {};
	for (NodeId n = 1; n <= 3; ++n) {
		asio::io_context io;
		tcp::acceptor probe(io, {asio::ip::address_v4::loopback(), 0});
		ports[n] = probe.local_endpoint().port();
	}
	auto peers = [&](NodeId self) {
		std::map<NodeId, PeerAddress> r;
		for (NodeId n = 1; n <= 3; ++n)
			if (n != self)
				r[n] = {"127.0.0.1", ports[n]};
		return r;
	};
	const std::vector<GroupId> groups{groups_on(2, 0, 1)[0], groups_on(2, 1, 1)[0]};  // one on each loop
	std::unique_ptr<Node> nodes[4];
	for (NodeId n = 1; n <= 3; ++n)
		nodes[n] = std::make_unique<Node>(n, ports[n], disks[n], groups, peers(n));
	auto all = [&] { return std::vector<Node *>{nodes[1].get(), nodes[2].get(), nodes[3].get()}; };
	for (auto g : groups) {
		step("driver: group " + std::to_string(g) + " elects and commits");
		const bool ok = commit_everywhere(all(), g, "before-" + std::to_string(g), 30'000);
		std::printf("driver: group %llu elected and committed on all three nodes: %s\n", (unsigned long long)g, ok ? "yes" : "NO");
		assert(ok);
	}
	NodeId restarted = 1;  // the first group's leader
	for (NodeId n = 1; n <= 3; ++n)
		if (auto s = nodes[n]->state(groups[0]); s && s->role == Role::Leader)
			restarted = n;
	step("driver: node " + std::to_string(restarted) + " restarts");
	nodes[restarted].reset();
	nodes[restarted] = std::make_unique<Node>(restarted, ports[restarted], disks[restarted], groups, peers(restarted));
	for (auto g : groups) {
		step("driver: group " + std::to_string(g) + " commits again");
		const bool ok = commit_everywhere(all(), g, "after-" + std::to_string(g), 30'000);
		auto s = nodes[restarted]->state(g);
		std::printf("driver: after node %llu's restart (group %llu's leader), group %llu committed on all three: %s (node %llu term %llu, committed %llu)\n",
					(unsigned long long)restarted, (unsigned long long)groups[0], (unsigned long long)g, ok ? "yes" : "NO",
					(unsigned long long)restarted, s ? (unsigned long long)s->term : 0ull, s ? (unsigned long long)s->committed : 0ull);
		assert(ok);
	}
}

// Credit comes back. Frames of 40 KiB against a 64 KiB window go one at a time, so any credit
// lost wedges the group: the receiver refuses frames for 1 s, then a marker frame still arrives
// (refused frames return their credit); then it restarts while a frame it never delivered is
// outstanding, and a marker arrives over the next connection (which starts with none).
void refill() {
	constexpr std::uint32_t L = 2;
	HostTransportOptions options;
	options.window = 64 * 1024;
	auto one = std::make_unique<Host>(1, L);
	auto two = std::make_unique<Host>(2, L);
	const auto port = two->port();
	one->open(addresses({two.get()}), options);
	two->open(addresses({one.get()}), options);
	const GroupId g = groups_on(L, 1, 1)[0];
	std::atomic<std::uint64_t> taken{0}, refused{0}, marked{0};
	std::atomic<bool> refuse{false}, slow{false};
	auto receive = [&](Host &h) {
		for (std::uint32_t k = 0; k != L; ++k)
			h.transport->on_deliver(k, [&](const Received &r) {
				if (refuse) {
					++refused;
					return false;
				}
				if (slow)
					std::this_thread::sleep_for(300ms);
				++taken;
				marked += r.payload == "M";
				return true;
			});
	};
	receive(*two);
	one->start();
	two->start();
	const auto k = one->transport->loop_of_group(g);
	auto send = [&](int n) {
		one->post(k, [&, n, k] {
			for (int i = 0; i != n; ++i)
				one->transport->send(k, LinkLane::Control, FrameKind::data, {g, 1, 2}, 2, std::string(40 * 1024, 'x'));
		});
	};
	// A marker data frame every 50 ms until one arrives, for at most 10 s.
	auto marker_arrives = [&] {
		marked = 0;
		for (int i = 0; i != 200 && !marked; ++i) {
			one->post(k, [&, k] { one->transport->send(k, LinkLane::Control, FrameKind::data, {g, 1, 2}, 2, "M"); });
			std::this_thread::sleep_for(50ms);
		}
		return marked > 0;
	};
	step("refill: the connection comes up");
	warm([&] { send(1); }, [&] { return taken > 0; });
	step("refill: frames refused for 1 s");
	refuse = true;
	for (int i = 0; i != 50; ++i) {
		send(2);
		std::this_thread::sleep_for(20ms);
	}
	std::this_thread::sleep_for(300ms);
	refuse = false;
	step("refill: a marker after the refusals");
	const bool after_refusals = marker_arrives();
	step("refill: the receiver restarts with a frame outstanding");
	slow = true;
	send(3);
	std::this_thread::sleep_for(150ms);  // the first is in the receiver's 300 ms delivery
	two.reset();
	slow = false;
	two = std::make_unique<Host>(2, L, port);
	two->open(addresses({one.get()}), options);
	receive(*two);
	two->start();
	step("refill: a marker over the next connection");
	const bool after_restart = marker_arrives();
	one->stop();
	two->stop();
	std::printf("refill: %llu frames refused, then a marker arrived: %s; after the receiver's restart with a frame outstanding, a marker arrived: %s\n",
				(unsigned long long)refused.load(), after_refusals ? "yes" : "NO (credit leaked)",
				after_restart ? "yes" : "NO (credit leaked)");
	assert(refused >= 8 && after_refusals && after_restart);  // 8 frames: 5 windows
}
} // namespace

int main(int argc, char **argv) {
	if (argc != 3) {
		std::fprintf(stderr, "usage: %s <fixture dir> threads|credit|reader|upload|driver|refill\n", argv[0]);
		return 2;
	}
#ifndef SO_NOSIGPIPE
	std::signal(SIGPIPE, SIG_IGN);
#endif
#ifdef __linux__
	::prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY);  // the watchdog's gdb attaches to its parent
#endif
	std::setvbuf(stdout, nullptr, _IOLBF, 0);  // what ran reaches the log of a run that is killed
	fixture = argv[1];
	const std::string which = argv[2];
	Watchdog watchdog(100s);  // under ctest's 120 s
	if (which == "threads")
		threads();
	else if (which == "credit")
		timed("credit", credit);
	else if (which == "reader")
		timed("reader", reader);
	else if (which == "upload")
		timed("upload", upload);
	else if (which == "driver")
		driver();
	else if (which == "refill")
		refill();
	else
		return 2;
	std::printf("%s: ok\n", which.c_str());
	return 0;
}
