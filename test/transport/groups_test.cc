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
//   identity the per-frame rule and the group-open exchange over the wire: each refusal, with
//            the receiver's counters; a refused group opens once stored; a store opens it again
//   joiner   frame_admission: a joiner (empty membership) catches up as a learner and is promoted
//   lagging  frame_admission: a follower that missed a voter's addition catches up from it
//   leftover a read that fails partway hands its frames over under its own connection
//   relayout a host restarted with another L serves every group from its new loop
//   scan     the groups a feed visits per frame stay constant at 100, 1,000 and 10,000 groups
//   order    control first, round robin across ready groups and per-group credit, as values
// A timing case the host stalled (a 250 ms oversleep) runs again, at most twice. Each step prints
// as it starts; a case still running after 100 s prints its step, which loops stopped turning
// and every thread's stack, then exits 3.
//   consensus_transport_groups_test <fixture dir> case
#include "consensus/driver.h"
#include "consensus/transport/host_transport.h"
#include <completion/posix.h>
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
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
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

namespace {
std::filesystem::path fixture;
const char *names[] = {"", "one", "two", "three", "four"};

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
	// Without a member_of, the membership puts incarnation k on node k for k up to 4.
	void open(const std::map<NodeId, PeerAddress> &addresses, HostTransportOptions options = {}) {
		auto tls = context(names[node]);
		std::map<NodeId, Fingerprint> pins;
		for (NodeId n = 1; n <= 4; ++n)
			pins[n] = pin_of(names[n]);
		if (!options.member_of)
			options.member_of = [](GroupId, NodeId incarnation) -> std::optional<Member> {
				if (incarnation < 1 || incarnation > 4)
					return std::nullopt;
				return Member{incarnation, incarnation, MemberRole::Voter};
			};
		std::vector<HostLoop> at;
		for (auto &l : loops)
			at.push_back({l->io.get_executor(), task::scheduler_ref::of(l->scheduler)});
		Identity cluster{};
		cluster.fill('K');
		transport = std::make_unique<HostTransport>(at, tls, NodePins(node, pins, tls->native_handle()), cluster, addresses, options);
	}
	// Before start(): stores each group as incarnation `node`, with `open`.
	void stores(const std::vector<GroupId> &groups, const GroupOpen &open = test_open()) {
		for (auto g : groups)
			transport->store(g, node, open);
	}
	static GroupOpen test_open() {
		GroupOpen o;
		o.cluster.fill('T');
		o.group.fill('G');
		o.epoch.fill('E');
		o.profile.fill(0x5A);
		return o;
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
	std::vector<GroupId> all;
	for (GroupId g = 1; g <= 64; ++g)
		all.push_back(g);
	for (Host *h : {&one, &two}) {
		h->stores(all);
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
	// A read posts to each other loop at most once, in any snapshot that loads posts first.
	auto bounded = [](const HostTransportStats &s) {
		const std::uint64_t posts = s.posts;
		return posts <= s.reads * (L - 1);
	};
	const auto start = steady::now();
	while (delivered < 2 * 64 * per_group && ms_since(start) < 20'000) {
		for (Host *h : {&one, &two})
			for (std::uint32_t k = 0; k != L; ++k)
				assert(bounded(h->transport->stats(k)));
		std::this_thread::yield();
	}
	std::uint64_t reads = 0, posts = 0, received = 0;
	for (Host *h : {&one, &two})
		for (std::uint32_t k = 0; k != L; ++k) {
			reads += h->transport->stats(k).reads;
			posts += h->transport->stats(k).posts;
			received += h->transport->stats(k).received;
			assert(bounded(h->transport->stats(k)));
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
	one.stores({g1, g2});
	two.stores({g1, g2});
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
	one.stores(ids);
	two.stores(ids);
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

// The sum of a counter over a host's loops.
std::uint64_t total(Host &h, std::atomic<std::uint64_t> HostTransportStats::*counter) {
	std::uint64_t sum = 0;
	for (std::uint32_t k = 0; k != h.transport->loops(); ++k)
		sum += (h.transport->stats(k).*counter).load();
	return sum;
}
// Each loopback TCP socket of this process on `ports`: local and peer port, bytes unread
// (FIONREAD), bytes in its send buffer, and on macOS the windows, cwnd, RTO and retransmits.
std::string sockets(std::initializer_list<std::uint16_t> ports) {
	std::string r;
	for (int fd = 3; fd != 1024; ++fd) {
		sockaddr_in local{}, peer{};
		socklen_t a = sizeof local, b = sizeof peer;
		if (::getsockname(fd, reinterpret_cast<sockaddr *>(&local), &a) != 0 || local.sin_family != AF_INET ||
			::getpeername(fd, reinterpret_cast<sockaddr *>(&peer), &b) != 0)
			continue;
		const auto lp = ntohs(local.sin_port), pp = ntohs(peer.sin_port);
		if (std::find(ports.begin(), ports.end(), lp) == ports.end() && std::find(ports.begin(), ports.end(), pp) == ports.end())
			continue;
		int unread = -1, unsent = -1;
		::ioctl(fd, FIONREAD, &unread);
#if defined(SO_NWRITE)
		socklen_t n = sizeof unsent;
		::getsockopt(fd, SOL_SOCKET, SO_NWRITE, &unsent, &n);
#elif defined(SIOCOUTQ)
		::ioctl(fd, SIOCOUTQ, &unsent);
#endif
		char line[256];
		std::snprintf(line, sizeof line, " [%u>%u unread %d sndq %d", lp, pp, unread, unsent);
		r += line;
#if defined(TCP_CONNECTION_INFO)
		tcp_connection_info i{};
		socklen_t m = sizeof i;
		if (::getsockopt(fd, IPPROTO_TCP, TCP_CONNECTION_INFO, &i, &m) == 0) {
			std::snprintf(line, sizeof line, " sndwnd %u rcvwnd %u cwnd %u rto %u srtt %u rexmit %llu", i.tcpi_snd_wnd, i.tcpi_rcv_wnd,
						  i.tcpi_snd_cwnd, i.tcpi_rto, i.tcpi_srtt, (unsigned long long)i.tcpi_txretransmitpackets);
			r += line;
		}
#endif
		r += "]";
	}
	return r;
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
	for (Host *h : {&one, &two}) {
		h->stores(uploads);
		h->stores(followers);
	}
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
	std::vector<std::string> samples;  // every 250 ms: progress, links, host 1's frames, host 2's reads, the sockets
	std::atomic<bool> sampling{true};
	std::thread sampler([&] {
		while (sampling) {
			std::this_thread::sleep_for(250ms);
			char line[256];
			std::snprintf(line, sizeof line, "upload: %5llu ms %6.1f MiB %3llu beats; links %llu/%llu sent %llu dropped %llu reads %llu;",
						  (unsigned long long)ms_since(start), double(uploaded) / (1 << 20), (unsigned long long)(beats.count - before),
						  (unsigned long long)total(one, &HostTransportStats::links), (unsigned long long)total(two, &HostTransportStats::links),
						  (unsigned long long)total(one, &HostTransportStats::sent), (unsigned long long)total(one, &HostTransportStats::dropped),
						  (unsigned long long)total(two, &HostTransportStats::reads));
			samples.push_back(line + sockets({one.port(), two.port()}));
		}
	});
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
	sampling = false;
	sampler.join();
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
	if (!ok)
		for (auto &s : samples)
			std::printf("%s\n", s.c_str());
	return ok;
}

// Nodes of L loops over one journal host, a Driver per loop, the Drivers' Sends over the
// transport; the membership the transport's per-frame rule reads is the Driver's.
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 32, 3};
std::atomic<NodeId> cut{0};  // a node whose frames, both ways, every node refuses

struct Node;
struct GroupSink {
	Node *node = nullptr;
	std::uint32_t loop = 0;
	void operator()(GroupId g, Send n);
	void operator()(GroupId g, Committed n);
	template <class N> void operator()(GroupId, N &&) {}
};
task::task<void> proposing(Driver<GroupSink> *d, GroupId g, std::string command) {
	auto placed = co_await d->propose(g, std::move(command));
	co_await d->committed(g, placed);
}
task::task<void> changing(Driver<GroupSink> *d, GroupId g, Membership next) {
	auto placed = co_await d->change_membership(g, std::move(next));
	co_await d->committed(g, placed);
}
task::task<void> transferring(Driver<GroupSink> *d, GroupId g, NodeId target) { co_await d->transfer_leadership(g, target); }

struct Node {
	struct PerLoop {
		j::InlineCompletion io;
		GroupSink sink;
		std::unique_ptr<Driver<GroupSink>> driver;
		task::cancel_source stop;
		std::atomic<bool> turns_done{false}, driver_done{false};
	};
	Host host;
	std::unique_ptr<j::Host> journal;
	std::vector<std::unique_ptr<PerLoop>> per;
	std::mutex mutex;
	std::map<GroupId, std::vector<std::string>> applied;
	std::atomic<std::uint64_t> misplaced{0};  // frames delivered off their group's loop
	// Each group is founded by `voters`; with none, this node is a joiner (D5).
	Node(NodeId id, std::uint32_t L, std::uint16_t port, std::shared_ptr<j::IO> io, const std::vector<GroupId> &groups,
		 const std::map<NodeId, PeerAddress> &peers, std::vector<NodeId> voters = {1, 2, 3})
		: host(id, L, port) {
		HostTransportOptions options;
		options.member_of = [this](GroupId g, NodeId incarnation) -> std::optional<Member> {
			auto s = per[host.transport->loop_of_group(g)]->driver->state(g);
			const Member *m = s ? find_member(s->membership, incarnation) : nullptr;
			return m ? std::optional<Member>(*m) : std::nullopt;
		};
		host.open(peers, options);
		journal = std::make_unique<j::Host>(std::move(io), j::Identity{'N', char('0' + id)}, j::HostOptions{L, turn_options});
		assert(journal->loops() == L);
		for (std::uint32_t k = 0; k != L; ++k) {
			auto p = std::make_unique<PerLoop>();
			p->sink = {this, k};
			auto ref = task::scheduler_ref::of(host.loops[k]->scheduler);
			p->driver = std::make_unique<Driver<GroupSink>>(Seams{ref, j::completion_ref::of(p->io), *journal, k, id * 31 + k}, p->sink);
			task::spawn(ref, journal->driver(k).run(j::completion_ref::of(p->io), p->stop.token()), [q = p.get()](std::exception_ptr) { q->turns_done = true; });
			task::spawn(ref, p->driver->run(p->stop.token()), [q = p.get()](std::exception_ptr) { q->driver_done = true; });
			host.transport->on_deliver(k, [this, k](const Received &r) {
				if (const NodeId c = cut; c && (host.node == c || r.node == c))
					return false;
				if (host.transport->loop_of_group(r.route.group) != k || journal->loop(r.route.group) != k)
					++misplaced;
				auto receive = receive_message(r);
				if (!receive)
					return false;
				per[k]->driver->receive(r.route.group, std::move(*receive));
				return true;
			});
			per.push_back(std::move(p));
		}
		host.start();
		for (auto g : groups) {
			const auto k = host.transport->loop_of_group(g);
			assert(journal->loop(g) == k);
			auto *driver = per[k]->driver.get();
			std::promise<void> opened;
			auto done = opened.get_future();
			host.post(k, [&, driver, g, k] {
				GroupOptions o;
				o.configuration.local = host.node;
				o.configuration.voters = voters;
				o.configuration.cluster[0] = 'T';
				o.configuration.configuration[0] = 'G';
				o.admission = admission;
				o.scheduling = WorkerLimits{16, 128};
				driver->open(g, std::move(o));
				auto opening = journal->status(g) == j::GroupStatus::stored ? driver->recover(g, [](j::ArtifactReader &) {})
																			: driver->create(g, j::Identity{'G', char('0' + host.node)});
				// stored once member_of answers for it (host-transport.md, Identity)
				task::spawn(task::scheduler_ref::of(host.loops[k]->scheduler), std::move(opening), [this, g, &opened](std::exception_ptr e) {
					assert(!e && "create or recover");
					host.transport->store(g, host.node, Host::test_open());
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
	// Runs make(driver)'s task on the group's loop: its error, a timeout's after `limit`, or null.
	template <class F> std::exception_ptr attempt(GroupId g, F make, std::chrono::milliseconds limit = 5s) {
		const auto k = host.transport->loop_of_group(g);
		auto *driver = per[k]->driver.get();
		auto done = std::make_shared<std::promise<std::exception_ptr>>();
		auto result = done->get_future();
		host.post(k, [=, this] {
			task::spawn(task::scheduler_ref::of(host.loops[k]->scheduler), make(driver), [done](std::exception_ptr e) { done->set_value(e); });
		});
		if (result.wait_for(limit) != std::future_status::ready)
			return std::make_exception_ptr(std::runtime_error("timed out"));
		return result.get();
	}
	// Runs make(driver)'s task on the group's loop: false on any error or after `limit`.
	template <class F> bool call(GroupId g, F make, std::chrono::milliseconds limit = 5s) { return !attempt(g, std::move(make), limit); }
	bool propose(GroupId g, std::string command) {
		return call(g, [=](Driver<GroupSink> *d) { return proposing(d, g, command); });
	}
	bool has(GroupId g, const std::string &command) {
		std::lock_guard hold(mutex);
		auto &a = applied[g];
		return std::find(a.begin(), a.end(), command) != a.end();
	}
	// The transport stops, then each Driver, which is destroyed on its loop, then the journal and the loops.
	~Node() {
		host.quiesce();
		for (std::uint32_t k = 0; k != per.size(); ++k) {
			auto &p = *per[k];
			host.loops[k]->at([&] { p.stop.request(task::cancel_reason::shutdown); });
			for (int i = 0; !(p.turns_done && p.driver_done); ++i) {
				assert(i < 2000 && "the Driver stops");
				std::this_thread::sleep_for(5ms);
			}
			host.loops[k]->at([&] { p.driver.reset(); });
		}
		host.loops[0]->at([&] { journal.reset(); });
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
// Polls `done` every 50 ms for up to `limit_ms`.
template <class F> bool until(F done, std::uint64_t limit_ms) {
	for (const auto start = steady::now(); !done(); std::this_thread::sleep_for(50ms))
		if (ms_since(start) > limit_ms)
			return false;
	return true;
}

// Up to four nodes on fixed ports, each with its own journal directory (under the working
// directory, named for the case) that survives a restart.
struct Cluster {
	std::filesystem::path root;
	std::vector<GroupId> groups;
	std::uint16_t ports[5] = {};
	std::unique_ptr<Node> nodes[5];
	Cluster(const std::string &name, std::vector<GroupId> ids)
		: root(std::filesystem::current_path() / ("groups-" + name)), groups(std::move(ids)) {
		std::filesystem::remove_all(root);
		for (NodeId n = 1; n <= 4; ++n) {
			asio::io_context io;
			tcp::acceptor probe(io, {asio::ip::address_v4::loopback(), 0});
			ports[n] = probe.local_endpoint().port();
			std::filesystem::create_directories(directory(n));
			::chmod(directory(n).c_str(), 0700);
		}
	}
	~Cluster() {
		for (auto &n : nodes)
			n.reset();
		std::filesystem::remove_all(root);
	}
	std::filesystem::path directory(NodeId n) const { return root / ("node" + std::to_string(n)); }
	// Starts node `n` with L loops, or restarts it on its journal.
	void start(NodeId n, std::uint32_t L, std::vector<NodeId> voters = {1, 2, 3}) {
		nodes[n].reset();
		std::map<NodeId, PeerAddress> peers;
		for (NodeId m = 1; m <= 4; ++m)
			if (m != n)
				peers[m] = {"127.0.0.1", ports[m]};
		nodes[n] = std::make_unique<Node>(n, L, ports[n], std::make_shared<j::PosixIO>(directory(n)), groups, peers, std::move(voters));
	}
	std::vector<Node *> of(std::initializer_list<NodeId> ids) {
		std::vector<Node *> r;
		for (auto id : ids)
			r.push_back(nodes[id].get());
		return r;
	}
	// The node among `ids` that leads `g` in the highest term, or 0.
	NodeId leader(std::initializer_list<NodeId> ids, GroupId g) {
		NodeId best = 0;
		Term term = 0;
		for (auto id : ids)
			if (auto s = nodes[id]->state(g); s && s->role == Role::Leader && s->term >= term)
				best = id, term = s->term;
		return best;
	}
	Membership membership(NodeId n, GroupId g) {
		auto s = nodes[n]->state(g);
		assert(s);
		return s->membership;
	}
};

// A3 of transport-groups: three nodes of 2 loops; two groups elect and commit, the first
// group's leader restarts, and both commit again.
void driver() {
	const std::vector<GroupId> groups{groups_on(2, 0, 1)[0], groups_on(2, 1, 1)[0]};  // one on each loop
	Cluster c("driver", groups);
	for (NodeId n = 1; n <= 3; ++n)
		c.start(n, 2);
	auto all = c.of({1, 2, 3});
	for (auto g : groups) {
		step("driver: group " + std::to_string(g) + " elects and commits");
		const bool ok = commit_everywhere(all, g, "before-" + std::to_string(g), 30'000);
		std::printf("driver: group %llu elected and committed on all three nodes: %s\n", (unsigned long long)g, ok ? "yes" : "NO");
		assert(ok);
	}
	NodeId restarted = c.leader({1, 2, 3}, groups[0]);
	restarted = restarted ? restarted : 1;
	step("driver: node " + std::to_string(restarted) + " restarts");
	c.start(restarted, 2);
	all = c.of({1, 2, 3});
	for (auto g : groups) {
		step("driver: group " + std::to_string(g) + " commits again");
		const bool ok = commit_everywhere(all, g, "after-" + std::to_string(g), 30'000);
		auto s = c.nodes[restarted]->state(g);
		std::printf("driver: after node %llu's restart (group %llu's leader), group %llu committed on all three: %s (node %llu term %llu, committed %llu)\n",
					(unsigned long long)restarted, (unsigned long long)groups[0], (unsigned long long)g, ok ? "yes" : "NO",
					(unsigned long long)restarted, s ? (unsigned long long)s->term : 0ull, s ? (unsigned long long)s->committed : 0ull);
		assert(ok);
	}
}

Membership with_learner(Membership m, NodeId incarnation) {
	m.members.push_back({incarnation, incarnation, MemberRole::Learner});
	m.next_incarnation = std::max(m.next_incarnation, incarnation + 1);
	return m;
}
Membership promoted(Membership m, NodeId incarnation) {
	for (auto &x : m.members)
		if (x.incarnation == incarnation)
			x.role = MemberRole::Voter;
	return m;
}
// Adds incarnation 4 (on node 4, a joiner) as a learner through `leader`, waits until it applied
// `seen`, then promotes it: false, naming the step, when one fails.
bool add_voter_4(Cluster &c, NodeId leader, GroupId g, const std::string &seen, const char *name) {
	step(std::string(name) + ": node " + std::to_string(leader) + " adds incarnation 4 as a learner");
	if (!c.nodes[leader]->call(g, [g, m = with_learner(c.membership(leader, g), 4)](Driver<GroupSink> *d) { return changing(d, g, m); }, 10s))
		return std::printf("%s: adding learner 4 did not commit\n", name), false;
	step(std::string(name) + ": learner 4 catches up");
	if (!until([&] { return c.nodes[4]->has(g, seen); }, 30'000)) {
		auto s = c.nodes[4]->state(g);
		const auto &t = c.nodes[4]->host.transport;
		std::uint64_t spoofed = 0, unopened = 0;
		for (std::uint32_t k = 0; k != t->loops(); ++k)
			spoofed += t->stats(k).spoofed, unopened += t->stats(k).unopened;
		return std::printf("%s: learner 4 never caught up (committed %llu, members %zu, spoofed %llu, unopened %llu)\n", name,
						   s ? (unsigned long long)s->committed : 0ull, s ? s->membership.members.size() : 0, (unsigned long long)spoofed,
						   (unsigned long long)unopened),
			   false;
	}
	// D3 (c) refuses the promotion as NotReady until the leader hears 4 matched its commit index,
	// which can trail 4 applying `seen` (the learner's own entry commits after it): retry only that.
	step(std::string(name) + ": incarnation 4 promoted");
	const auto start = steady::now();
	for (int not_ready = 0;; ++not_ready) {
		auto e = c.nodes[leader]->attempt(g, [g, m = promoted(c.membership(leader, g), 4)](Driver<GroupSink> *d) { return changing(d, g, m); }, 10s);
		if (!e)
			return std::printf("%s: 4 promoted after %d NotReady refusals\n", name, not_ready), true;
		int why = -1;
		try {
			std::rethrow_exception(e);
		} catch (const rejected &r) {
			why = int(r.reason);
		} catch (...) {
		}
		if (why != int(RejectReason::NotReady) || ms_since(start) >= 10'000)
			return std::printf("%s: promoting 4 did not commit (reject reason %d, -1 for none; %d NotReady before it)\n", name, why, not_ready), false;
		std::this_thread::sleep_for(10ms);
	}
}

std::uint64_t misplaced(Cluster &c) {
	std::uint64_t sum = 0;
	for (auto &n : c.nodes)
		sum += n ? n->misplaced.load() : 0;
	return sum;
}

// frame_admission's joiner: incarnation 4 on node 4 starts with an empty membership, so nothing
// it knows names its leader; it catches up as a learner over connections the group's voters
// share, is promoted, and commits with the others (async-drivers X1, Y4).
void joiner() {
	const GroupId g = groups_on(2, 1, 1)[0];
	Cluster c("joiner", {g});
	for (NodeId n = 1; n <= 3; ++n)
		c.start(n, 2);
	c.start(4, 2, {});
	step("joiner: the group elects and commits");
	bool ok = commit_everywhere(c.of({1, 2, 3}), g, "a", 60'000);
	const NodeId leader = c.leader({1, 2, 3}, g);
	ok = ok && leader && add_voter_4(c, leader, g, "a", "joiner");
	step("joiner: the four commit");
	ok = ok && commit_everywhere(c.of({1, 2, 3, 4}), g, "b", 60'000);
	auto s = c.nodes[4]->state(g);
	const bool votes = s && votes_in(s->membership, 4);
	std::printf("joiner: learner 4 caught up from leader %llu, was promoted and commits with the others: %s (votes %d, committed %llu, members %zu, misplaced %llu)\n",
				(unsigned long long)leader, ok ? "yes" : "NO", int(votes), s ? (unsigned long long)s->committed : 0ull,
				s ? s->membership.members.size() : 0, (unsigned long long)misplaced(c));
	assert(ok && votes && !misplaced(c));
}

// frame_admission's lagging follower: node 3 is cut off while incarnation 4 joins, is promoted
// and takes over by transfer, so 3's membership never names 4; once the cut heals, 3 catches up
// from leader 4.
void lagging() {
	const GroupId g = groups_on(2, 0, 1)[0];
	Cluster c("lagging", {g});
	for (NodeId n = 1; n <= 3; ++n)
		c.start(n, 2);
	c.start(4, 2, {});
	step("lagging: the group elects and commits");
	bool ok = commit_everywhere(c.of({1, 2, 3}), g, "a", 60'000);
	step("lagging: node 3 is cut off");
	cut = 3;
	ok = ok && commit_everywhere(c.of({1, 2}), g, "a2", 60'000);
	const NodeId leader = c.leader({1, 2}, g);
	ok = ok && leader && add_voter_4(c, leader, g, "a2", "lagging");
	step("lagging: leadership moves to 4");
	ok = ok && c.nodes[leader]->call(g, [g](Driver<GroupSink> *d) { return transferring(d, g, 4); }, 10s) &&
		 until([&] { return c.leader({1, 2, 4}, g) == 4; }, 10'000);
	const bool unnamed = !find_member(c.membership(3, g), 4);
	const auto lag = c.nodes[3]->state(g)->committed;
	step("lagging: the cut heals");
	cut = 0;
	ok = ok && commit_everywhere(c.of({1, 2, 3, 4}), g, "b", 60'000);
	const NodeId after = c.leader({1, 2, 3, 4}, g);
	auto s = c.nodes[3]->state(g);
	const bool named = votes_in(s->membership, 4);
	std::printf("lagging: node 3 missed 4's addition (4 unnamed in its membership: %d, committed %llu), then caught up from leader %llu: %s (committed %llu, 4 votes in its membership: %d, misplaced %llu)\n",
				int(unnamed), (unsigned long long)lag, (unsigned long long)after, ok ? "yes" : "NO", (unsigned long long)s->committed,
				int(named), (unsigned long long)misplaced(c));
	assert(ok && unnamed && named && !misplaced(c));
}

// A2: node 1 runs 2 loops, then restarts on its journal with 3. The journal re-partitions, the
// transport places each group by the same slot rule, and every group commits on all three with
// node 1 receiving each group's frames on its new loop.
void relayout() {
	std::vector<GroupId> groups, moving;
	for (GroupId g = 1000, staying = 0; moving.size() < 2 || staying < 2; ++g) {
		const auto slot = j::host_detail::slot(g);
		const bool moves = j::host_detail::loop(slot, 2) != j::host_detail::loop(slot, 3);
		if (moves ? moving.size() < 2 : staying < 2) {
			groups.push_back(g);
			moves ? moving.push_back(g) : void(++staying);
		}
	}
	Cluster c("relayout", groups);
	for (NodeId n = 1; n <= 3; ++n)
		c.start(n, 2);
	for (auto g : groups) {
		step("relayout: group " + std::to_string(g) + " commits on 2 loops");
		const bool committed = commit_everywhere(c.of({1, 2, 3}), g, "before", 60'000);
		assert(committed);
	}
	step("relayout: node 1 restarts with 3 loops");
	c.start(1, 3);
	auto &one = *c.nodes[1];
	assert(one.journal->loops() == 3 && one.host.transport->loops() == 3);
	bool ok = true;
	for (auto g : groups) {
		step("relayout: group " + std::to_string(g) + " commits with node 1 on 3 loops");
		const auto k = one.host.transport->loop_of_group(g);
		const bool committed = commit_everywhere(c.of({1, 2, 3}), g, "after", 60'000);
		const auto s = one.state(g);
		std::printf("relayout: group %llu on loop %u of 2, then %u of 3 (journal: %u), %s; committed on all three: %s (node 1 term %llu, committed %llu)\n",
					(unsigned long long)g, j::host_detail::loop(j::host_detail::slot(g), 2), k, one.journal->loop(g),
					std::find(moving.begin(), moving.end(), g) != moving.end() ? "moved" : "stayed", committed ? "yes" : "NO",
					s ? (unsigned long long)s->term : 0ull, s ? (unsigned long long)s->committed : 0ull);
		ok = ok && committed && one.journal->loop(g) == k && s;
	}
	std::uint64_t received = 0;
	for (std::uint32_t k = 0; k != 3; ++k)
		received += one.host.transport->stats(k).received;
	std::printf("relayout: node 1 received %llu frames on 3 loops, %llu off their group's loop\n", (unsigned long long)received,
				(unsigned long long)one.misplaced.load());
	assert(ok && received && !one.misplaced);
}

// The per-frame rule and the group-open exchange over the wire, host 1 sending to host 2. Host
// 2 stores `ok` as host 1 does, four groups each with one open field differing, and `late` not
// yet. Each refusal shows in host 2's counters and in what reached its groups.
void identity() {
	constexpr std::uint32_t L = 2;
	Host one(1, L), two(2, L);
	one.open(addresses({&two}));
	two.open(addresses({&one}));
	const GroupId ok = 1001, cluster = 1002, group = 1003, epoch = 1004, profile = 1005, late = 1006;
	const auto differing = [](void (*change)(GroupOpen &)) {
		auto o = Host::test_open();
		change(o);
		return o;
	};
	one.stores({ok, cluster, group, epoch, profile, late});
	two.stores({ok});
	two.stores({cluster}, differing([](GroupOpen &o) { o.cluster[0] = 'X'; }));
	two.stores({group}, differing([](GroupOpen &o) { o.group[15] = 'X'; }));
	two.stores({epoch}, differing([](GroupOpen &o) { o.epoch[8] = 'X'; }));
	two.stores({profile}, differing([](GroupOpen &o) { o.profile[31] ^= 1; }));
	int zeros = 0;  // a zero cluster, group or epoch is refused at store
	for (auto zero : {+[](GroupOpen &o) { o.cluster = {}; }, +[](GroupOpen &o) { o.group = {}; }, +[](GroupOpen &o) { o.epoch = {}; }})
		try {
			two.stores({late}, differing(zero));
		} catch (const std::invalid_argument &) {
			++zeros;
		}
	std::printf("identity: store refused %d of 3 opens with a zero cluster, group or epoch\n", zeros);
	assert(zeros == 3);
	std::mutex mutex;
	std::map<std::pair<GroupId, std::string>, std::uint64_t> got;  // by group and payload
	for (std::uint32_t k = 0; k != L; ++k)
		two.transport->on_deliver(k, [&](const Received &r) {
			std::lock_guard hold(mutex);
			++got[{r.route.group, r.payload}];
			return true;
		});
	auto received = [&](GroupId g, const std::string &payload) {
		std::lock_guard hold(mutex);
		return got[{g, payload}];
	};
	auto from_group = [&](GroupId g) {
		std::lock_guard hold(mutex);
		std::uint64_t sum = 0;
		for (auto &[key, n] : got)
			sum += key.first == g ? n : 0;
		return sum;
	};
	auto send = [&](GroupId g, NodeId source, NodeId destination, std::string payload) {
		const auto k = one.transport->loop_of_group(g);
		one.post(k, [&one, g, source, destination, k, payload] {
			one.transport->send(k, LinkLane::Control, FrameKind::control, {g, source, destination}, 2, payload);
		});
	};
	auto counters = [&] {
		std::printf("identity: host 2 counts opens %llu, mismatched %llu, unstored %llu, unopened %llu, misrouted %llu, spoofed %llu; host 1 heard closed %llu\n",
					(unsigned long long)total(two, &HostTransportStats::opens), (unsigned long long)total(two, &HostTransportStats::mismatched),
					(unsigned long long)total(two, &HostTransportStats::unstored), (unsigned long long)total(two, &HostTransportStats::unopened),
					(unsigned long long)total(two, &HostTransportStats::misrouted), (unsigned long long)total(two, &HostTransportStats::spoofed),
					(unsigned long long)total(one, &HostTransportStats::closed));
	};
	one.start();
	two.start();
	step("identity: the connection comes up");
	warm([&] { send(ok, 1, 2, "warm"); }, [&] { return received(ok, "warm") > 0; });
	step("identity: 20 rounds of each refusal");
	for (int i = 0; i != 20; ++i) {
		for (auto g : {cluster, group, epoch, profile, late})
			send(g, 1, 2, "x");
		send(ok, 1, 7, "misrouted");  // a destination incarnation host 2 does not hold
		send(ok, 3, 2, "spoofed");    // incarnation 3, which the membership puts on node 3
		send(ok, 9, 2, "unnamed");    // an incarnation the membership does not name: admitted
		std::this_thread::sleep_for(20ms);
	}
	const bool settled = until(
		[&] {
			return total(two, &HostTransportStats::misrouted) == 20 && total(two, &HostTransportStats::spoofed) == 20 &&
				   total(two, &HostTransportStats::unopened) == 80 && received(ok, "unnamed") == 20;
		},
		20'000);
	counters();
	std::printf("identity: delivered of 20 each: cluster %llu, group %llu, epoch %llu, profile %llu, late %llu, misrouted %llu, spoofed %llu, unnamed %llu\n",
				(unsigned long long)from_group(cluster), (unsigned long long)from_group(group), (unsigned long long)from_group(epoch),
				(unsigned long long)from_group(profile), (unsigned long long)from_group(late), (unsigned long long)received(ok, "misrouted"),
				(unsigned long long)received(ok, "spoofed"), (unsigned long long)received(ok, "unnamed"));
	assert(settled);
	for (auto g : {cluster, group, epoch, profile, late})
		assert(from_group(g) == 0);
	assert(!received(ok, "misrouted") && !received(ok, "spoofed"));
	assert(total(two, &HostTransportStats::mismatched) >= 4 && total(two, &HostTransportStats::unstored) >= 20);

	step("identity: host 2 stores the late group; host 1 opens it again");
	two.loops[two.transport->loop_of_group(late)]->at([&] { two.transport->store(late, 2, Host::test_open()); });
	const bool late_opened = until([&] { return send(late, 1, 2, "late"), received(late, "late") > 0; }, 20'000);
	std::printf("identity: once host 2 stored it, the late group's frames arrive: %s\n", late_opened ? "yes" : "NO");
	assert(late_opened);

	step("identity: host 1 stores ok again under another epoch");
	const auto epoch2 = differing([](GroupOpen &o) { o.epoch.fill('Y'); });
	one.loops[one.transport->loop_of_group(ok)]->at([&] { one.transport->store(ok, 1, epoch2); });
	const auto mismatched = total(two, &HostTransportStats::mismatched);
	for (int i = 0; i != 20; ++i) {
		send(ok, 1, 2, "epoch2");
		std::this_thread::sleep_for(20ms);
	}
	const bool reopened = until([&] { return total(two, &HostTransportStats::mismatched) > mismatched; }, 20'000);
	std::this_thread::sleep_for(300ms);
	std::printf("identity: under host 1's new epoch, host 2 refused its open: %s, and delivered %llu of 20\n", reopened ? "yes" : "NO",
				(unsigned long long)received(ok, "epoch2"));
	assert(reopened && !received(ok, "epoch2"));
	step("identity: host 2 stores ok under the same epoch");
	two.loops[two.transport->loop_of_group(ok)]->at([&] { two.transport->store(ok, 2, epoch2); });
	const bool agreed = until([&] { return send(ok, 1, 2, "agreed"), received(ok, "agreed") > 0; }, 20'000);
	std::printf("identity: once both store the new epoch, frames arrive: %s\n", agreed ? "yes" : "NO");
	assert(agreed);
	step("identity: host 2 stores ok again under a third epoch");
	two.loops[two.transport->loop_of_group(ok)]->at([&] { two.transport->store(ok, 2, differing([](GroupOpen &o) { o.epoch.fill('Z'); })); });
	const auto restored = total(two, &HostTransportStats::mismatched);
	for (int i = 0; i != 20; ++i) {
		send(ok, 1, 2, "epoch3");
		std::this_thread::sleep_for(20ms);
	}
	const bool closed_again = until([&] { return total(two, &HostTransportStats::mismatched) > restored; }, 20'000);
	std::this_thread::sleep_for(300ms);
	std::printf("identity: once host 2 stored another epoch, host 1's open no longer holds: refused %s, delivered %llu of 20\n",
				closed_again ? "yes" : "NO", (unsigned long long)received(ok, "epoch3"));
	assert(closed_again && !received(ok, "epoch3"));

	step("identity: host 2 forgets ok");
	two.loops[two.transport->loop_of_group(ok)]->at([&] { two.transport->forget(ok); });
	const auto unstored = total(two, &HostTransportStats::unstored);
	for (int i = 0; i != 20; ++i) {
		send(ok, 1, 2, "forgotten");
		std::this_thread::sleep_for(20ms);
	}
	const bool refused = until([&] { return total(two, &HostTransportStats::unstored) >= unstored + 20; }, 20'000);
	counters();
	std::printf("identity: after host 2 forgot it, the group's frames are refused: %s, delivered %llu of 20\n", refused ? "yes" : "NO",
				(unsigned long long)received(ok, "forgotten"));
	assert(refused && !received(ok, "forgotten"));
	one.stop();
	two.stop();
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
	one->stores({g});
	two->stores({g});
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
	two->stores({g});
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

// leftover: frames a read handed over before it failed (an oversized length) leave under their
// own connection, never under the next read's: node 1 never opens g, so g's frame is unopened.
void leftover() {
	Host two(2, 1), three(3, 1);
	two.open(addresses({&three}));
	three.open(addresses({&two}));
	const GroupId g = 1001;
	two.stores({g});
	three.stores({g});
	std::mutex mutex;
	std::map<NodeId, std::uint64_t> by_source;
	std::vector<std::string> seen;
	two.transport->on_deliver(0, [&](const Received &r) {
		std::lock_guard hold(mutex);
		++by_source[r.route.source];
		if (r.route.source == 1)
			seen.push_back("source 1 over node " + std::to_string(r.node) + ": " + r.payload);
		return true;
	});
	auto delivered = [&](NodeId source) {
		std::lock_guard hold(mutex);
		return by_source[source];
	};
	two.start();
	three.start();
	auto honest = [&] {
		three.post(0, [&three, g] { three.transport->send(0, LinkLane::Control, FrameKind::control, {g, 3, 2}, 2, "honest"); });
	};
	step("leftover: host 3 opens g on host 2");
	warm(honest, [&] { return delivered(3) > 0; });

	step("leftover: node 1 connects over raw Links and never opens g");
	Loop attacker;
	auto tls = context(names[1]);
	std::map<NodeId, Fingerprint> pins;
	for (NodeId n = 1; n <= 4; ++n)
		pins[n] = pin_of(names[n]);
	Identity cluster{};
	cluster.fill('K');
	LinkOptions options;
	options.connection.maximum_frame = 8 << 20;
	auto links = std::make_unique<Links>(attacker.io.get_executor(), task::scheduler_ref::of(attacker.scheduler), tls,
										 NodePins(1, pins, tls->native_handle()), cluster,
										 std::map<NodeId, PeerAddress>{{2, {"127.0.0.1", two.port()}}}, options);
	std::atomic<const Link *> link{nullptr};
	std::atomic<bool> ended{false};
	links->on_established = [&](const Link &l) { link = &l; };
	links->on_ended = [&](const Link &) { ended = true; };
	links->on_frame = [](const Link &, std::string_view) {};
	attacker.start();
	attacker.at([&] {
		task::spawn(task::scheduler_ref::of(attacker.scheduler), links->dial(2, LinkLane::Control),
					[](std::exception_ptr, std::optional<bool>) {});
	});
	const bool up = until([&] { return link.load() != nullptr; }, 10'000);
	std::printf("leftover: node 1's connection to host 2 is up: %s\n", up ? "yes" : "NO");
	assert(up);

	step("leftover: node 1 sends a frame for g, then one over host 2's bound");
	auto frame = [&](std::string payload) {
		std::string f(1, char(FrameKind::control));
		j::put64(f, g);
		j::put64(f, 1);
		j::put64(f, 2);
		return f + payload;
	};
	const auto evil = frame("evil"), big = frame(std::string(5 << 20, 'B'));
	attacker.at([&] {
		link.load()->connection->send(evil);
		link.load()->connection->send(big);
	});
	const bool failed = until([&] { return ended.load(); }, 10'000);
	std::printf("leftover: host 2 ended node 1's connection: %s\n", failed ? "yes" : "NO");

	step("leftover: host 3 sends 20 frames");
	for (int i = 0; i != 20; ++i) {
		honest();
		std::this_thread::sleep_for(20ms);
	}
	std::this_thread::sleep_for(500ms);
	std::uint64_t unopened = 0, received = 0;
	for (std::uint32_t k = 0; k != two.transport->loops(); ++k)
		unopened += two.transport->stats(k).unopened, received += two.transport->stats(k).received;
	{
		std::lock_guard hold(mutex);
		for (auto &s : seen)
			std::printf("leftover: host 2 delivered %s\n", s.c_str());
	}
	const auto bad = delivered(1);
	std::printf("leftover: host 2 received %llu frames, refused %llu unopened; delivered from incarnation 1 (never opened g): %llu; from 3: %llu\n",
				(unsigned long long)received, (unsigned long long)unopened, (unsigned long long)bad, (unsigned long long)delivered(3));
	std::printf("leftover: %s\n", bad ? "node 1's frame was admitted under host 3's open" : "no frame of node 1 admitted");
	attacker.at([&] { links->stop(); });
	const bool stopped = until([&] { return attacker.at([&] { return links->drained(); }); }, 10'000);
	attacker.at([&] { links.reset(); });
	assert(failed && !bad && unopened >= 1 && delivered(3) > 0 && stopped);
}
// scan: the feed's work per frame. Host 1 stores N groups and sends a control and a data frame
// per group to host 2, for N = 100, 1,000 and 10,000, every group on one loop: the groups both
// hosts' feeds visit per frame they send stay under 2 at each N (a scan per frame grows with N).
void scan() {
	Host one(1, 1), two(2, 1);
	one.open(addresses({&two}));
	two.open(addresses({&one}));
	std::atomic<std::uint64_t> delivered{0};
	two.transport->on_deliver(0, [&](const Received &) {
		++delivered;
		return true;
	});
	one.stores({1});
	two.stores({1});
	one.start();
	two.start();
	step("scan: the connection comes up");
	warm([&] { one.post(0, [&] { one.transport->send(0, LinkLane::Control, FrameKind::control, {1, 1, 2}, 2, "w"); }); },
		 [&] { return delivered > 0; });
	auto totals = [&] {
		std::pair<std::uint64_t, std::uint64_t> r;  // visited, sent
		for (Host *h : {&one, &two}) {
			r.first += h->transport->stats(0).visited;
			r.second += h->transport->stats(0).sent;
		}
		return r;
	};
	GroupId next = 1'000'000;
	std::vector<double> ratios;
	for (std::size_t n : {100, 1'000, 10'000}) {
		std::vector<GroupId> groups;
		for (std::size_t i = 0; i != n; ++i)
			groups.push_back(next++);
		for (Host *h : {&one, &two})
			h->loops[0]->at([&] { h->stores(groups); });
		std::this_thread::sleep_for(100ms);
		const auto before = totals();
		const auto d0 = delivered.load();
		step("scan: " + std::to_string(n) + " groups send");
		one.post(0, [&one, groups] {
			for (auto g : groups)
				for (auto kind : {FrameKind::control, FrameKind::data})
					one.transport->send(0, LinkLane::Control, kind, {g, 1, 2}, 2, std::string(39, 'p'));
		});
		const auto start = steady::now();
		while (delivered != d0 + 2 * n) {
			assert(ms_since(start) < 60'000 && "every frame arrives");
			std::this_thread::sleep_for(1ms);
		}
		for (auto last = totals();;) {  // credit returns, then nothing moves for 100 ms
			std::this_thread::sleep_for(100ms);
			const auto now = totals();
			if (now == last)
				break;
			last = now;
		}
		for (const auto credit_start = steady::now(); one.transport->stats(0).uncredited || two.transport->stats(0).uncredited;) {
			assert(ms_since(credit_start) < 10'000 && "every group's credit returns");
			std::this_thread::sleep_for(1ms);
		}
		const auto after = totals();
		const double visited = double(after.first - before.first), sent = double(after.second - before.second);
		ratios.push_back(visited / sent);
		std::printf("scan: %zu groups: %.0f frames sent, %.0f groups visited, %.3f a frame (%.1f ms)\n", n, sent, visited, ratios.back(),
					double(ms_since(start)));
	}
	one.stop();
	two.stop();
	for (auto r : ratios)
		assert(r < 2 && "groups visited per frame sent stays constant");
}

// order: what a feed hands a connection, as values.
//   Host 1 queues, in one turn of its one loop: data a0 a1, control a0 a1 a2, b0 b1 b2, c0, data
//   b0. Host 2 (one loop: delivery in wire order) gets control first, round robin across the
//   groups ready (a0 b0 c0 a1 b1 a2 b2), then data round robin (a0 b0 a1).
//   Per-group credit: host 2 of 2 loops stalls group a's loop; a's data frames are over its
//   window (64-byte frames, a 100-byte window: one at a time), b's are not stalled. All of b's
//   arrive, exactly one of a's crosses the connection until the stall ends, then a's arrive in
//   order.
void order() {
	using Seen = std::vector<std::string>;
	auto tag = [](GroupId g, bool data, int i) { return std::string(1, char('a' + (g - 1001))) + (data ? "d" : "c") + std::to_string(i); };
	{
		Host one(1, 1), two(2, 1);
		one.open(addresses({&two}));
		two.open(addresses({&one}));
		Seen seen;  // written on host 2's loop
		one.stores({1, 1001, 1002, 1003});
		two.stores({1, 1001, 1002, 1003});
		std::atomic<bool> up{false};
		two.transport->on_deliver(0, [&](const Received &r) {
			if (r.route.group == 1)
				up = true;
			else
				seen.push_back(r.payload);
			return true;
		});
		one.start();
		two.start();
		step("order: the connection comes up");
		warm([&] { one.post(0, [&] { one.transport->send(0, LinkLane::Control, FrameKind::control, {1, 1, 2}, 2, "w"); }); },
			 [&] { return up.load(); });
		std::this_thread::sleep_for(100ms);
		step("order: one turn queues every frame");
		one.post(0, [&] {
			auto send = [&](GroupId g, bool data, int i) {
				one.transport->send(0, LinkLane::Control, data ? FrameKind::data : FrameKind::control, {g, 1, 2}, 2, tag(g, data, i));
			};
			send(1001, true, 0);
			send(1001, true, 1);
			for (int i = 0; i != 3; ++i)
				send(1001, false, i);
			for (int i = 0; i != 3; ++i)
				send(1002, false, i);
			send(1003, false, 0);
			send(1002, true, 0);
		});
		const Seen expected = {"ac0", "bc0", "cc0", "ac1", "bc1", "ac2", "bc2", "ad0", "bd0", "ad1"};
		Seen got;
		for (int i = 0; got.size() < expected.size(); ++i) {
			assert(i < 5000 && "every frame arrives");
			std::this_thread::sleep_for(1ms);
			got = two.loops[0]->at([&] { return seen; });
		}
		std::string line;
		for (auto &s : got)
			line += " " + s;
		std::printf("order: delivered%s\n", line.c_str());
		one.stop();
		two.stop();
		assert(got == expected && "control first, round robin across ready groups");
	}
	{
		HostTransportOptions options;
		options.window = 100;
		Host one(1, 1), two(2, 2);
		one.open(addresses({&two}), options);
		two.open(addresses({&one}), options);
		const auto R = two.transport->loop_of_node(1);
		const GroupId a = groups_on(2, 1 - R, 1)[0], b = groups_on(2, R, 1)[0];
		std::mutex seen_mutex;
		std::map<GroupId, Seen> seen;
		std::atomic<bool> up{false};
		for (std::uint32_t k = 0; k != 2; ++k)
			two.transport->on_deliver(k, [&](const Received &r) {
				if (r.route.group == b && r.payload == "w") {
					up = true;
					return true;
				}
				std::lock_guard hold(seen_mutex);
				seen[r.route.group].push_back(r.payload);
				return true;
			});
		one.stores({a, b});
		two.stores({a, b});
		one.start();
		two.start();
		auto count = [&](GroupId g) {
			std::lock_guard hold(seen_mutex);
			return seen[g].size();
		};
		step("order: credit: the connection comes up");
		warm([&] { one.post(0, [&] { one.transport->send(0, LinkLane::Control, FrameKind::control, {b, 1, 2}, 2, "w"); }); },
			 [&] { return up.load(); });
		std::promise<void> release;
		auto stalled = release.get_future().share();
		two.post(1 - R, [stalled] { stalled.wait(); });
		std::this_thread::sleep_for(50ms);
		const std::uint64_t received = two.transport->stats(R).received;
		step("order: credit: a's loop stalls; a and b each queue 3 data frames over the window");
		one.post(0, [&] {
			for (int i = 0; i != 3; ++i)
				for (auto g : {a, b}) {
					std::string payload(39, 'p');
					payload[0] = char('0' + i);
					one.transport->send(0, LinkLane::Control, FrameKind::data, {g, 1, 2}, 2, payload);
				}
		});
		for (int i = 0; count(b) < 3; ++i) {
			assert(i < 5000 && "b's frames arrive while a's loop stalls");
			std::this_thread::sleep_for(1ms);
		}
		std::this_thread::sleep_for(300ms);
		const std::uint64_t crossed = two.transport->stats(R).received - received;
		const auto a_during = count(a);
		release.set_value();
		for (int i = 0; count(a) < 3; ++i) {
			assert(i < 5000 && "a's frames arrive once its loop runs");
			std::this_thread::sleep_for(1ms);
		}
		std::string a_order;
		{
			std::lock_guard hold(seen_mutex);
			for (auto &p : seen[a])
				a_order += p[0];
			for (auto &p : seen[b])
				a_order += p[0];
		}
		std::printf("order: credit: while a's loop stalled, b delivered 3, a %zu, frames crossing %llu (3 of b's, 1 of a's); then a and b in order: %s\n",
					a_during, (unsigned long long)crossed, a_order.c_str());
		one.stop();
		two.stop();
		assert(a_during == 0 && crossed == 4 && a_order == "012012" && "credit is per group");
	}
}
} // namespace

int main(int argc, char **argv) {
	if (argc != 3) {
		std::fprintf(stderr, "usage: %s <fixture dir> threads|credit|reader|upload|driver|refill|identity|joiner|lagging|leftover|relayout|scan|order\n", argv[0]);
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
	// Under ctest's 120 s, and 240 s for the cases that elect and change membership more than once.
	const bool long_case = which == "joiner" || which == "lagging" || which == "relayout";
	Watchdog watchdog(long_case ? 220s : 100s);
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
	else if (which == "identity")
		identity();
	else if (which == "leftover")
		leftover();
	else if (which == "joiner")
		joiner();
	else if (which == "lagging")
		lagging();
	else if (which == "relayout")
		relayout();
	else if (which == "scan")
		scan();
	else if (which == "order")
		order();
	else
		return 2;
	std::printf("%s: ok\n", which.c_str());
	return 0;
}
