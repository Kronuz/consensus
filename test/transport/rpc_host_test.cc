// The RPC lane on the host transport's Bulk connection, over loopback with the TLS fixture, every
// loop on its own thread (docs/mechanisms/rpc-lane.md, On Bulk):
//   calls       deadlines and cancels over shared connections: a caller's deadline ends the
//               server's call, a cancel mid-stream ends both ends once, and every pool is back to 0
//   group-full  a group whose receiving loop stalls 3 s fills its credit on Bulk; calls on the
//               same connection keep their latency (control: rpc-shared-credit)
//   call-full   a call whose server never reads fills its credit and pool; another group's
//               heartbeats keep every gap under the 1,000 ms election timeout
//   lane-only   two nodes that share no group: a streamed call dials Bulk alone, completes, and
//               no group's counter moves
//   stream      a 32 MiB request streams under the lane's credit while heartbeats on Control keep
//               every gap under 1,000 ms (control: rpc-one-frame)
//   consensus_transport_rpc_host_test <fixture dir> case
#include "consensus/transport/host_transport.h"
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <cassert>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <mutex>
#include <openssl/pem.h>
#include <thread>

using namespace kronuz::consensus::transport;
using namespace kronuz::consensus;
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
using steady = std::chrono::steady_clock;

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
	template <class F> auto at(F f) -> decltype(f()) {
		std::packaged_task<decltype(f())()> call(std::move(f));
		auto result = call.get_future();
		asio::post(io, [&call] { call(); });
		return result.get();
	}
	~Loop() { join(); }
};

struct Host {
	NodeId node;
	std::vector<std::unique_ptr<Loop>> loops;
	tcp::acceptor acceptor;
	std::unique_ptr<HostTransport> transport;
	bool running = false;
	Host(NodeId n, std::uint32_t L) : node(n), loops(make(L)), acceptor(loops[0]->io, {asio::ip::address_v4::loopback(), 0}) {}
	static std::vector<std::unique_ptr<Loop>> make(std::uint32_t L) {
		std::vector<std::unique_ptr<Loop>> loops;
		for (std::uint32_t k = 0; k != L; ++k)
			loops.push_back(std::make_unique<Loop>());
		return loops;
	}
	std::uint16_t port() const { return acceptor.local_endpoint().port(); }
	void open(const std::map<NodeId, PeerAddress> &addresses, HostTransportOptions options = {}) {
		auto tls = context(names[node]);
		std::map<NodeId, Fingerprint> pins;
		for (NodeId n = 1; n <= 4; ++n)
			pins[n] = pin_of(names[n]);
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
	void stores(const std::vector<GroupId> &groups) {
		GroupOpen o;
		o.cluster.fill('T');
		o.group.fill('G');
		o.epoch.fill('E');
		o.profile.fill(0x5A);
		for (auto g : groups)
			transport->store(g, node, o);
	}
	void start() {
		transport->serve(acceptor);
		for (auto &l : loops)
			l->start();
		running = true;
	}
	template <class F> void post(std::uint32_t k, F f) { asio::post(loops[k]->io, std::move(f)); }
	// Runs f on the loop of node `peer`'s connections and waits for it.
	template <class F> auto at(NodeId peer, F f) { return loops[transport->loop_of_node(peer)]->at(std::move(f)); }
	std::uint64_t total(std::atomic<std::uint64_t> HostTransportStats::*counter) const {
		std::uint64_t sum = 0;
		for (std::uint32_t k = 0; k != loops.size(); ++k)
			sum += transport->stats(k).*counter;
		return sum;
	}
	std::uint64_t lane(std::atomic<std::uint64_t> LaneStats::*counter) const {
		std::uint64_t sum = 0;
		for (std::uint32_t k = 0; k != loops.size(); ++k)
			sum += transport->lane_stats(k).*counter;
		return sum;
	}
	std::uint64_t ended(rpc::Outcome o) const {
		std::uint64_t sum = 0;
		for (std::uint32_t k = 0; k != loops.size(); ++k)
			sum += transport->lane_stats(k).ended(o);
		return sum;
	}
	void stop() {
		if (!running)
			return;
		transport->stop();
		for (std::uint32_t k = 0; k != loops.size(); ++k)
			for (int i = 0; !loops[k]->at([&] { return transport->drained(k); }); ++i) {
				assert(i < 2000 && "transport drains");
				std::this_thread::sleep_for(5ms);
			}
		for (auto &l : loops)
			l->join();
		running = false;
	}
	~Host() { stop(); }
};
std::map<NodeId, PeerAddress> addresses(std::initializer_list<const Host *> hosts) {
	std::map<NodeId, PeerAddress> r;
	for (auto *h : hosts)
		r[h->node] = {"127.0.0.1", h->port()};
	return r;
}
std::vector<GroupId> groups_on(std::uint32_t L, std::uint32_t k, std::size_t n) {
	std::vector<GroupId> r;
	for (GroupId g = 1000; r.size() < n; ++g)
		if (kronuz::journal::host_detail::loop(kronuz::journal::host_detail::slot(g), L) == k)
			r.push_back(g);
	return r;
}
template <class P> bool until(P done, std::chrono::milliseconds limit) {
	const auto start = steady::now();
	while (!done()) {
		if (steady::now() - start > limit)
			return false;
		std::this_thread::sleep_for(5ms);
	}
	return true;
}
struct Beats {
	steady::time_point last{};
	std::atomic<std::uint64_t> worst{0}, count{0};
	void beat() {
		const auto now = steady::now();
		if (last != steady::time_point{})
			worst = std::max<std::uint64_t>(worst, std::uint64_t(std::chrono::duration_cast<std::chrono::milliseconds>(now - last).count()));
		last = now;
		++count;
	}
};

// The generated byte at `offset` of every test stream.
char byte_at(std::uint64_t offset) { return char((offset * 2654435761u) >> 13); }
std::string bytes_at(std::uint64_t offset, std::size_t n) {
	std::string s(n, '\0');
	for (std::size_t i = 0; i != n; ++i)
		s[i] = byte_at(offset + i);
	return s;
}

// One end's stream of a call: written within the peer's credit, each time credit arrives.
struct Writer {
	HostTransport *transport = nullptr;
	RpcCall call;
	std::uint64_t total = 0, sent = 0, limit = ~std::uint64_t{0};  // limit: where the writer stops
	std::atomic<bool> failed{false};
	void pump() {
		try {
			while (sent < std::min(total, limit)) {
				const auto n = std::min<std::uint64_t>(transport->sendable(call), std::min(total, limit) - sent);
				if (!n)
					return;
				transport->write(call, bytes_at(sent, n), sent + n == total);
				sent += n;
			}
		} catch (const std::exception &e) {
			std::printf("write failed: %s\n", e.what());
			failed = true;
		}
	}
};

// One call's state as the test sees it, at either end.
struct Seen {
	std::atomic<bool> done{false};
	std::atomic<int> ends{0};
	rpc::Status status;
	std::atomic<std::uint64_t> received{0}, mismatched{0};
	steady::time_point started = steady::now(), finished;
	Writer out;
};
// Takes the peer's stream, checks each byte against the generator, and consumes it at once.
rpc::Events checking(HostTransport &t, std::shared_ptr<Seen> seen, std::function<void(bool)> on_end = {}) {
	rpc::Events e;
	e.body = [&t, seen, on_end](std::string_view bytes, bool end) {
		for (std::size_t i = 0; i != bytes.size(); ++i)
			seen->mismatched += bytes[i] != byte_at(seen->received + i);
		seen->received += bytes.size();
		t.consume(seen->out.call, bytes.size());
		if (on_end)
			on_end(end);
	};
	e.credit = [seen] { seen->out.pump(); };
	e.done = [seen](const rpc::Status &s) {
		seen->status = s;
		seen->finished = steady::now();
		++seen->ends;
		seen->done = true;
	};
	return e;
}

// The methods host 2 serves: 7 echoes a request of its total back once read; 8 never reads nor
// answers; 9 reads its request and answers 8 bytes, its length.
constexpr rpc::MethodId echo = 7, hang = 8, sink = 9;
struct Server {
	std::mutex mutex;
	std::vector<std::shared_ptr<Seen>> calls;
	void install(HostTransport &t) {
		for (auto method : {echo, hang, sink})
			t.handle(method, [this, &t, method](RpcCall c, const rpc::Incoming &in) -> std::optional<rpc::Events> {
				auto seen = std::make_shared<Seen>();
				seen->out.transport = &t;
				seen->out.call = c;
				{
					std::lock_guard hold(mutex);
					calls.push_back(seen);
				}
				if (method == hang) {
					rpc::Events e;
					e.body = [](std::string_view, bool) {};
					e.done = [seen](const rpc::Status &s) {
						seen->status = s;
						++seen->ends;
						seen->done = true;
					};
					return e;
				}
				const auto total = in.total;
				return checking(t, seen, [&t, seen, method, total](bool end) {
					if (!end)
						return;
					seen->out.total = method == echo ? total : 0;
					if (method == sink) {
						std::string n;
						kronuz::journal::put64(n, seen->received);
						t.total(seen->out.call, 8);
						t.write(seen->out.call, n, true);
						return;
					}
					t.total(seen->out.call, total);
					seen->out.pump();
				});
			});
	}
	std::shared_ptr<Seen> last() {
		std::lock_guard hold(mutex);
		return calls.empty() ? nullptr : calls.back();
	}
};

// Starts a call from host `from` to node `to`, streaming `total` request bytes; its state.
std::shared_ptr<Seen> start_call(Host &from, NodeId to, rpc::MethodId method, std::uint64_t total, std::chrono::milliseconds deadline,
							 std::uint64_t limit = ~std::uint64_t{0}) {
	auto seen = std::make_shared<Seen>();
	from.at(to, [&] {
		auto &t = *from.transport;
		const auto inline_bytes = std::min<std::uint64_t>(total, 1024);
		seen->out.transport = &t;
		seen->out.total = total;
		seen->out.sent = inline_bytes;
		seen->out.limit = limit;
		seen->started = steady::now();
		seen->out.call = t.call(to, method, {.deadline = rpc::Clock::now() + deadline, .total = total}, bytes_at(0, inline_bytes),
								checking(t, seen));
		seen->out.pump();
		return 0;
	});
	return seen;
}
bool pools_empty(Host &h) {
	auto &t = *h.transport;
	return !t.serving_pool().used() && !t.calling_pool().used() && !t.outgoing_pool().used();
}

void watchdog(std::chrono::seconds limit) {
	std::thread([limit] {
		std::this_thread::sleep_for(limit);
		std::printf("HUNG after %llds\n", (long long)limit.count());
		std::fflush(stdout);
		std::_Exit(3);
	}).detach();
}

bool calls() {
	Host one(1, 4), two(2, 4);
	Server server;
	one.open(addresses({&two}));
	two.open(addresses({&one}));
	server.install(*two.transport);
	const auto g = groups_on(4, 1, 1)[0];
	one.stores({g});
	two.stores({g});
	one.start();
	two.start();
	bool ok = true;
	auto check = [&](bool value, const char *what) {
		std::printf("calls: %s: %s\n", what, value ? "ok" : "FAILED");
		ok &= value;
	};
	// A group frame on Control, so the lane shares the nodes' connections with a group.
	one.post(one.transport->loop_of_group(g), [&] {
		one.transport->send(one.transport->loop_of_group(g), LinkLane::Control, FrameKind::control, {g, 1, 2}, 2, "hello");
	});
	auto a = start_call(one, 2, echo, 1 << 20, 10s);
	check(until([&] { return a->done.load(); }, 10s), "a streamed 1 MiB echo completes");
	check(a->status.outcome == rpc::Outcome::ok && a->received == 1 << 20 && !a->mismatched, "its response is the request, byte for byte");

	auto b = start_call(one, 2, hang, 64, 300ms);
	check(until([&] { return b->done.load(); }, 5s), "a call past its deadline ends");
	const auto served_b = server.last();
	check(until([&] { return served_b && served_b->done.load(); }, 5s), "its server's call ends too");
	std::printf("calls: deadline 300 ms: the caller ended after %llu ms with outcome %d code %u; the server with outcome %d code %u\n",
				(unsigned long long)std::chrono::duration_cast<std::chrono::milliseconds>(b->finished - b->started).count(),
				int(b->status.outcome), b->status.code, int(served_b->status.outcome), served_b->status.code);
	check(b->status.code == std::uint32_t(rpc::Code::deadline_exceeded) &&
			  served_b->status.code == std::uint32_t(rpc::Code::deadline_exceeded),
		  "both ends name deadline_exceeded");

	auto c = start_call(one, 2, sink, 8 << 20, 10s, 1 << 20);  // the writer stops after 1 MiB of 8
	check(until([&] {
		return one.at(2, [&] { return c->out.sent; }) == 1 << 20 && server.last() != served_b && server.last()->received >= 512 << 10;
	}, 10s), "a streamed request is mid-way");
	const auto served_c = server.last();
	one.at(2, [&] {
		one.transport->stop(c->out.call, rpc::Code::cancelled, "enough");
		return 0;
	});
	check(until([&] { return c->done && served_c->done; }, 5s), "a cancel mid-stream ends both ends");
	std::this_thread::sleep_for(200ms);
	check(c->ends == 1 && served_c->ends == 1 && c->status.outcome == rpc::Outcome::cancel &&
			  served_c->status.outcome == rpc::Outcome::cancel && served_c->status.code == std::uint32_t(rpc::Code::cancelled) &&
			  served_c->status.text == "enough",
		  "each end once, as cancelled");
	check(until([&] { return pools_empty(one) && pools_empty(two); }, 2s), "every pool is back to 0");
	std::printf("calls: high water serving %zu calling %zu outgoing %zu; lane frames out %llu\n",
				two.transport->serving_pool().high_water(), one.transport->calling_pool().high_water(),
				one.transport->outgoing_pool().high_water(), (unsigned long long)one.lane(&LaneStats::frames_out));
	check(one.ended(rpc::Outcome::ok) == 1 && one.ended(rpc::Outcome::cancel) == 2 && two.lane(&LaneStats::served) == 3,
		  "the lane counts each call by outcome");
	one.stop();
	two.stop();
	return ok;
}

// reader_test's shape for the lane: group 1's receiving loop stalls 3 s while its sender fills the
// group's credit on Bulk; small calls on the same Bulk connection keep their latency.
bool group_full() {
	constexpr std::uint32_t L = 4;
	Host one(1, L), two(2, L);
	Server server;
	one.open(addresses({&two}));
	two.open(addresses({&one}));
	server.install(*two.transport);
	const auto reading = two.transport->loop_of_node(1);
	const auto g1 = groups_on(L, (reading + 1) % L, 1)[0];
	one.stores({g1});
	two.stores({g1});
	std::atomic<std::uint64_t> delivered{0};
	two.transport->on_deliver((reading + 1) % L, [&](const Received &) {
		++delivered;
		return true;
	});
	one.start();
	two.start();
	auto warm = start_call(one, 2, echo, 64, 10s);
	if (!until([&] { return warm->done.load(); }, 10s))
		return false;
	const auto sender = one.transport->loop_of_node(2), at = one.transport->loop_of_group(g1);
	two.post((reading + 1) % L, [] { std::this_thread::sleep_for(3000ms); });
	one.post(at, [&] {
		for (int i = 0; i != 100; ++i)
			one.transport->send(at, LinkLane::Bulk, FrameKind::data, {g1, 1, 2}, 2, std::string(64 * 1024, 'g'));
	});
	const bool full = until([&] { return one.transport->stats(sender).uncredited >= 4u << 20; }, 2s);
	std::uint64_t worst = 0, done = 0;
	const auto start = steady::now();
	while (ms_since(start) < 2000) {
		auto c = start_call(one, 2, echo, 64, 10s);
		if (until([&] { return c->done.load(); }, 5s) && c->status.outcome == rpc::Outcome::ok)
			++done;
		worst = std::max<std::uint64_t>(worst, std::chrono::duration_cast<std::chrono::milliseconds>(c->finished - c->started).count());
		std::this_thread::sleep_for(50ms);
	}
	const auto uncredited = one.transport->stats(sender).uncredited.load();
	until([&] { return delivered == 100; }, 10s);
	one.stop();
	two.stop();
	const bool ok = full && done >= 20 && worst < 250 && delivered == 100;
	std::printf("group-full: group 1's credit full (%s, %llu bytes uncredited during the calls); %llu calls, largest latency %llu ms; group 1 delivered %llu of 100 %s\n",
				full ? "yes" : "no", (unsigned long long)uncredited, (unsigned long long)done, (unsigned long long)worst,
				(unsigned long long)delivered.load(), ok ? "ok" : "FAILED");
	return ok;
}

// A call whose server never reads holds its credit and pool bytes; group 2's heartbeats on
// Control keep every gap under the election timeout.
bool call_full() {
	constexpr std::uint32_t L = 4;
	Host one(1, L), two(2, L);
	Server server;
	one.open(addresses({&two}));
	two.open(addresses({&one}));
	server.install(*two.transport);
	const auto g2 = groups_on(L, two.transport->loop_of_node(1), 1)[0];
	one.stores({g2});
	two.stores({g2});
	Beats beats;
	two.transport->on_deliver(two.transport->loop_of_group(g2), [&](const Received &) {
		beats.beat();
		return true;
	});
	one.start();
	two.start();
	const auto hb = one.transport->loop_of_group(g2);
	auto heartbeat = [&] { one.post(hb, [&] { one.transport->send(hb, LinkLane::Control, FrameKind::control, {g2, 1, 2}, 2, "beat"); }); };
	if (!until([&] { heartbeat(); return beats.count > 0; }, 10s))
		return false;
	auto c = start_call(one, 2, hang, 64 << 20, 30s);
	const bool full = until([&] { return two.transport->serving_pool().used() >= 4u << 20 && one.at(2, [&] { return one.transport->sendable(c->out.call); }) == 0; }, 5s);
	const auto before = beats.count.load();
	beats.worst = 0;
	const auto start = steady::now();
	for (std::int64_t tick = 0; ms_since(start) < 3000; ++tick) {
		heartbeat();
		std::this_thread::sleep_until(start + (tick + 1) * 100ms);
	}
	std::this_thread::sleep_for(150ms);
	const auto held = two.transport->serving_pool().used(), sent = one.at(2, [&] { return c->out.sent; });
	one.at(2, [&] {
		one.transport->stop(c->out.call, rpc::Code::cancelled);
		return 0;
	});
	const bool freed = until([&] { return pools_empty(two); }, 2s);
	one.stop();
	two.stop();
	const bool ok = full && beats.worst < 1000 && beats.count - before >= 25 && freed;
	std::printf("call-full: the unread call sent %llu bytes, its server holds %zu bytes of serving pool; group 2 got %llu heartbeats, longest gap %llu ms; pool freed after the cancel: %s %s\n",
				(unsigned long long)sent, held, (unsigned long long)(beats.count - before), (unsigned long long)beats.worst,
				freed ? "yes" : "no", ok ? "ok" : "FAILED");
	return ok;
}

// Two nodes that share no group: a call dials Bulk alone and no group's counter moves.
bool lane_only() {
	Host one(1, 2), two(2, 2);
	Server server;
	one.open(addresses({&two}));
	two.open(addresses({&one}));
	server.install(*two.transport);
	one.start();
	two.start();
	auto c = start_call(one, 2, echo, 256 << 10, 10s);
	const bool done = until([&] { return c->done.load(); }, 10s);
	const auto links = one.total(&HostTransportStats::links) + two.total(&HostTransportStats::links);
	const auto groups = one.total(&HostTransportStats::sent) + one.total(&HostTransportStats::received) +
						two.total(&HostTransportStats::sent) + two.total(&HostTransportStats::received) +
						one.total(&HostTransportStats::uncredited) + two.total(&HostTransportStats::uncredited);
	const bool ok = done && c->status.outcome == rpc::Outcome::ok && c->received == 256 << 10 && !c->mismatched &&
					links == 2 && !groups && two.lane(&LaneStats::served) == 1 && one.lane(&LaneStats::calls) == 1;
	std::printf("lane-only: the call ended %d with %llu of %u bytes, %llu mismatched; links %llu (Bulk at each end); group frames sent and received %llu; lane frames out %llu in %llu %s\n",
				int(c->status.outcome), (unsigned long long)c->received, 256u << 10, (unsigned long long)c->mismatched,
				(unsigned long long)links, (unsigned long long)groups, (unsigned long long)one.lane(&LaneStats::frames_out),
				(unsigned long long)two.lane(&LaneStats::frames_in), ok ? "ok" : "FAILED");
	one.stop();
	two.stop();
	return ok;
}

// A 32 MiB request streams on Bulk under the lane's credit while heartbeats go on Control.
bool stream() {
	constexpr std::uint32_t L = 2;
	constexpr std::uint64_t body = 32 << 20;
	Host one(1, L), two(2, L);
	Server server;
	one.open(addresses({&two}));
	two.open(addresses({&one}));
	server.install(*two.transport);
	const auto g = groups_on(L, two.transport->loop_of_node(1), 1)[0];
	one.stores({g});
	two.stores({g});
	Beats beats;
	two.transport->on_deliver(two.transport->loop_of_group(g), [&](const Received &) {
		beats.beat();
		return true;
	});
	one.start();
	two.start();
	const auto hb = one.transport->loop_of_group(g);
	auto heartbeat = [&] { one.post(hb, [&] { one.transport->send(hb, LinkLane::Control, FrameKind::control, {g, 1, 2}, 2, "beat"); }); };
	if (!until([&] { heartbeat(); return beats.count > 0; }, 10s))
		return false;
	beats.worst = 0;
	const auto start = steady::now();
	auto c = start_call(one, 2, sink, body, 60s);
	for (std::int64_t tick = 0; !c->done && ms_since(start) < 60'000; ++tick) {
		heartbeat();
		std::this_thread::sleep_until(start + (tick + 1) * 100ms);
	}
	const auto elapsed = std::max<std::uint64_t>(1, ms_since(start));
	const auto served = server.last();
	const auto window = HostTransportOptions{}.rpc.lane.window;
	const auto serving = two.transport->serving_pool().high_water(), outgoing = one.transport->outgoing_pool().high_water();
	const bool ok = c->done && c->status.outcome == rpc::Outcome::ok && !c->out.failed && served && served->received == body &&
					!served->mismatched && beats.worst < 1000 && serving <= window && outgoing <= HostTransportOptions{}.rpc.outgoing_bytes;
	std::printf("stream: %llu MiB in %llu ms (%.1f MB/s), outcome %d, the server read %llu bytes, %llu mismatched; heartbeats' longest gap %llu ms; serving high water %zu (window %zu), outgoing %zu %s\n",
				(unsigned long long)(body >> 20), (unsigned long long)elapsed, double(body) / 1e3 / double(elapsed),
				int(c->status.outcome), (unsigned long long)(served ? served->received : 0),
				(unsigned long long)(served ? served->mismatched : 0), (unsigned long long)beats.worst, serving, window, outgoing,
				ok ? "ok" : "FAILED");
	one.stop();
	two.stop();
	return ok;
}
} // namespace

int main(int argc, char **argv) {
	if (argc != 3) {
		std::fprintf(stderr, "usage: %s <fixture dir> calls|group-full|call-full|lane-only|stream\n", argv[0]);
		return 2;
	}
	fixture = argv[1];
	const std::string which = argv[2];
	watchdog(std::chrono::seconds(120));
	bool ok;
	if (which == "calls")
		ok = calls();
	else if (which == "group-full")
		ok = group_full();
	else if (which == "call-full")
		ok = call_full();
	else if (which == "lane-only")
		ok = lane_only();
	else if (which == "stream")
		ok = stream();
	else
		return 2;
	std::fflush(stdout);
	return ok ? 0 : 1;
}
