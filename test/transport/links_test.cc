// Links over loopback with the TLS fixture: a dial and its frames, simultaneous dials settled
// to one connection before any frame (T6), a draining link that does not count, a name's lookup
// through the late deadline, two hosts on two loops (run under TSan), a given-up dial released,
// and a flood of idle connections that cannot lock a real dialer out (security review S2, N1),
// nor, from an address no route names, evict a peer's handshake (round 2, N9).
//   consensus_transport_links_test <fixture dir> [case]
#include "consensus/transport/links.h"
#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <openssl/pem.h>
#include <thread>

using namespace kronuz::consensus::transport;
using namespace std::chrono_literals;
using kronuz::consensus::NodeId;
using tcp = asio::ip::tcp;

namespace {
std::filesystem::path fixture;

Fingerprint pin_of(const std::string &name) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file((fixture / (name + ".pem")).c_str(), "r"), BIO_free);
	std::unique_ptr<X509, decltype(&X509_free)> leaf(PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
	assert(leaf);
	return key_pin(leaf.get());
}
std::shared_ptr<asio::ssl::context> context(const std::string &name) {
	auto c = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	reactor::tls::configure(*c, {(fixture / (name + ".pem")).string(), (fixture / (name + ".key")).string(),
								 (fixture / "ca.pem").string()},
							reactor::tls::Role::Peer);
	return c;
}
kronuz::consensus::Identity cluster() {
	kronuz::consensus::Identity r{};
	r.fill('K');
	return r;
}
const char *names[] = {"", "one", "two"};

struct Loop {
	asio::io_context io;
	task::asio_scheduler scheduler{io.get_executor()};
	task::scheduler_ref ref() { return task::scheduler_ref::of(scheduler); }
	template <class P> bool until(P done, std::chrono::milliseconds limit = 10s) {
		const auto stop = std::chrono::steady_clock::now() + limit;
		while (!done()) {
			if (std::chrono::steady_clock::now() >= stop)
				return false;
			io.restart();
			io.run_for(5ms);
		}
		return true;
	}
	template <class F> void call(F f) {
		bool done = false;
		asio::post(io, [&] {
			f();
			done = true;
		});
		assert(until([&] { return done; }));
	}
	~Loop() {
		io.stop();
		scheduler.close();
		scheduler.destroy_roots();
	}
};

// One host: its listener and its Links, on `loop`.
struct Host {
	NodeId node;
	Loop &loop;
	tcp::acceptor acceptor;
	std::unique_ptr<Links> links;
	std::atomic<std::size_t> frames{0};
	std::vector<std::string> received;
	std::optional<tcp::acceptor> v6;  // a second listener, on ::1
	Host(NodeId n, Loop &l) : node(n), loop(l), acceptor(l.io, tcp::endpoint(asio::ip::address_v4::loopback(), 0)) {}
	std::uint16_t port() const { return acceptor.local_endpoint().port(); }
	std::uint16_t port6() const { return v6->local_endpoint().port(); }
	void open(const std::map<NodeId, PeerAddress> &addresses, LinkOptions options = {}) {
		auto tls = context(names[node]);
		std::map<NodeId, Fingerprint> pins{{1, pin_of("one")}, {2, pin_of("two")}};
		links = std::make_unique<Links>(loop.io.get_executor(), loop.ref(), tls, NodePins(node, pins, tls->native_handle()),
										cluster(), addresses, options);
		links->on_frame = [this](const Link &, std::string_view f) {
			if (frames++ < 16)
				received.emplace_back(f);
		};
		task::spawn(loop.ref(), links->serve(acceptor));
		if (v6)
			task::spawn(loop.ref(), links->serve(*v6));
	}
	void stop() {
		loop.call([&] {
			asio::error_code ignored;
			acceptor.close(ignored);  // Links closes only the last acceptor it serves
			if (v6)
				v6->close(ignored);
			links->stop();
		});
		assert(loop.until([&] { return links->drained(); }));
	}
};
std::map<NodeId, PeerAddress> to(const Host &peer) { return {{peer.node, {"127.0.0.1", peer.port()}}}; }

// The same link seen from both ends: one connection, dialed by `dialer`.
void same_link(Host &a, Host &b, LinkLane lane, NodeId dialer) {
	const Link *x = a.links->find(b.node, lane), *y = b.links->find(a.node, lane);
	assert(x && y);
	assert(x->local.connection == y->remote.connection && x->remote.connection == y->local.connection);
	assert(x->dialed == (a.node == dialer) && y->dialed == (b.node == dialer));
}

template <class T> struct Result {
	std::optional<T> value;
	void spawn(Loop &loop, task::task<T> t) {
		task::spawn(loop.ref(), std::move(t), [this](std::exception_ptr e, std::optional<T> v) {
			assert(!e);
			value = std::move(v);
		});
	}
};

void dial() {
	Loop loop;
	Host one(1, loop), two(2, loop);
	one.open(to(two));
	two.open(to(one));
	Result<bool> dialed;
	dialed.spawn(loop, one.links->dial(2, LinkLane::Control));
	assert(loop.until([&] { return dialed.value.has_value(); }));
	assert(*dialed.value && one.links->stats().established == 1 && two.links->stats().established == 1);
	same_link(one, two, LinkLane::Control, 1);
	assert(one.links->find(2, LinkLane::Control)->rpc == 1 && !one.links->find(2, LinkLane::Bulk));
	loop.call([&] {
		one.links->find(2, LinkLane::Control)->connection->send("ping");
		two.links->find(1, LinkLane::Control)->connection->send("pong");
	});
	assert(loop.until([&] { return one.frames == 1 && two.frames == 1; }));
	assert(two.received[0] == "ping" && one.received[0] == "pong");
	// A second dial of the lane finds the link and does nothing.
	Result<bool> again;
	again.spawn(loop, one.links->dial(2, LinkLane::Control));
	assert(loop.until([&] { return again.value.has_value(); }) && !*again.value);
	assert(one.links->stats().established == 1);
	one.stop();
	two.stop();
	std::printf("dial: node 1 dialed node 2, one link, frames both ways\n");
}

// T6: both nodes dial each other and both hellos are out before either end decides. The dial
// of the lower node wins at both ends, before any frame; the other is refused.
void simultaneous() {
	Loop loop;
	Host one(1, loop), two(2, loop);
	one.open(to(two));
	two.open(to(one));
	Result<std::shared_ptr<Connection>> c1, c2;
	c1.spawn(loop, one.links->connect(2, LinkLane::Control));
	c2.spawn(loop, two.links->connect(1, LinkLane::Control));
	assert(loop.until([&] { return c1.value && c2.value; }));
	assert(*c1.value && *c2.value);
	Result<bool> o1, o2;
	loop.call([&] {  // in one loop turn: both hellos count before either is read
		o1.spawn(loop, one.links->offer(2, LinkLane::Control, *c1.value));
		o2.spawn(loop, two.links->offer(1, LinkLane::Control, *c2.value));
	});
	assert(loop.until([&] { return o1.value && o2.value; }));
	std::printf("simultaneous: node 1's dial %s, node 2's %s; refused %llu and %llu\n", *o1.value ? "won" : "lost",
				*o2.value ? "won" : "lost", (unsigned long long)one.links->stats().refused,
				(unsigned long long)two.links->stats().refused);
	assert(*o1.value && !*o2.value);
	same_link(one, two, LinkLane::Control, 1);
	assert(one.links->stats().established == 1 && two.links->stats().established == 1);
	assert(one.links->stats().refused == 1 && two.links->stats().refused == 1);
	loop.call([&] { two.links->find(1, LinkLane::Control)->connection->send("after"); });
	assert(loop.until([&] { return one.frames == 1; }) && one.received[0] == "after");
	one.stop();
	two.stop();
}

// A draining link does not count: node 2 drains its link to node 1 and dials again at once;
// both ends take the new link while the old one finishes closing.
void draining() {
	Loop loop;
	Host one(1, loop), two(2, loop);
	one.open(to(two));
	two.open(to(one));
	Result<bool> first;
	first.spawn(loop, one.links->dial(2, LinkLane::Bulk));
	assert(loop.until([&] { return first.value.has_value(); }) && *first.value);
	std::size_t ended = 0;
	one.links->on_ended = [&](const Link &) { ++ended; };
	Result<bool> second;
	loop.call([&] {
		two.links->find(1, LinkLane::Bulk)->connection->send("last on the old link");
		two.links->retire(1, LinkLane::Bulk);
		assert(!two.links->find(1, LinkLane::Bulk));
		second.spawn(loop, two.links->dial(1, LinkLane::Bulk));
	});
	assert(loop.until([&] { return second.value.has_value() && ended == 1; }));
	std::printf("draining: the new dial %s; the old link ended at node 1: %zu\n", *second.value ? "won" : "lost", ended);
	assert(*second.value && one.received.size() == 1 && one.received[0] == "last on the old link");
	same_link(one, two, LinkLane::Bulk, 2);
	assert(one.links->stats().established == 2 && two.links->stats().established == 2);
	one.stop();
	two.stop();
}

// A name dials once its lookup answers, woken by the answer; a lookup that never answers in
// time fails the dial at resolve_deadline, the late deadline's rule.
void lookup() {
	Loop loop;
	Host one(1, loop), two(2, loop);
	LinkOptions options;
	options.resolve_deadline = 1000ms;
	// The slow lookup's thread outlives this case (abandoned, it ends on its own): it captures nothing.
	options.resolver = [](const std::string &host) {
		std::this_thread::sleep_for(host == "slow.test" ? 3000ms : 100ms);
		return std::vector<asio::ip::address>{asio::ip::address_v4::loopback()};
	};
	one.open({{2, {"two.test", two.port()}}}, options);
	two.open(to(one));
	auto start = std::chrono::steady_clock::now();
	Result<bool> dialed;
	dialed.spawn(loop, one.links->dial(2, LinkLane::Control));
	assert(loop.until([&] { return dialed.value.has_value(); }));
	auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
	std::printf("lookup: answered in ~100 ms, dialed in %lld ms\n", (long long)ms);
	// Woken by the answer, not the 1 s deadline: no lookup timeout (the bound in ms is printed;
	// a runner that stops the process stretches it, D100).
	assert(*dialed.value && ms >= 95 && one.links->stats().lookups == 1);
	assert(one.links->stats().lookup_timeouts == 0);
	one.stop();
	two.stop();

	Loop slow_loop;
	Host three(1, slow_loop), four(2, slow_loop);
	options.resolve_deadline = 200ms;
	three.open({{2, {"slow.test", four.port()}}}, options);
	four.open(to(three));
	start = std::chrono::steady_clock::now();
	Result<bool> slow;
	slow.spawn(slow_loop, three.links->dial(2, LinkLane::Control));
	assert(slow_loop.until([&] { return slow.value.has_value(); }));
	ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
	std::printf("lookup: a 3 s resolver failed the dial in %lld ms, lookup_timeouts %llu\n", (long long)ms,
				(unsigned long long)three.links->stats().lookup_timeouts);
	assert(!*slow.value && ms >= 195 && three.links->stats().lookup_timeouts == 1);
	three.stop();  // abandons the lookup; its thread ends on its own
	four.stop();
}

// Two hosts on two loop threads dial each other on both lanes at once and exchange frames.
void threads() {
	Loop l1, l2;
	Host one(1, l1), two(2, l2);
	one.open(to(two));
	two.open(to(one));
	std::atomic<bool> stop{false};
	auto spin = [&](Loop &l) {
		while (!stop) {
			l.io.restart();
			l.io.run_for(5ms);
		}
	};
	std::thread t1(spin, std::ref(l1)), t2(spin, std::ref(l2));
	std::atomic<int> dials{0};
	auto start_dials = [&](Host &h, NodeId peer) {
		asio::post(h.loop.io, [&h, peer, &dials] {
			for (auto lane : {LinkLane::Control, LinkLane::Bulk})
				task::spawn(h.loop.ref(), h.links->dial(peer, lane), [&dials](std::exception_ptr, std::optional<bool>) { ++dials; });
		});
	};
	start_dials(one, 2);
	start_dials(two, 1);
	auto wait = [](auto done) {
		for (int i = 0; i != 1000 && !done(); ++i)
			std::this_thread::sleep_for(10ms);
		return done();
	};
	assert(wait([&] { return dials == 4; }));
	// Each host sends 1,000 frames on each lane's link, from its own loop.
	std::atomic<int> sent{0};
	for (Host *h : {&one, &two}) {
		const NodeId peer = h->node == 1 ? 2 : 1;
		asio::post(h->loop.io, [h, peer, &sent] {
			for (auto lane : {LinkLane::Control, LinkLane::Bulk}) {
				auto *link = h->links->find(peer, lane);
				assert(link);
				for (int i = 0; i != 1000; ++i)
					link->connection->send("frame " + std::to_string(i));
			}
			++sent;
		});
	}
	assert(wait([&] { return sent == 2 && one.frames == 2000 && two.frames == 2000; }));
	std::atomic<int> stopped{0};
	for (Host *h : {&one, &two})
		asio::post(h->loop.io, [h, &stopped] {
			h->links->stop();
			++stopped;
		});
	assert(wait([&] { return stopped == 2; }));
	std::atomic<bool> drained{false};
	assert(wait([&] {
		bool d1 = false, d2 = false;
		std::atomic<int> asked{0};
		asio::post(l1.io, [&] { d1 = one.links->drained(); ++asked; });
		asio::post(l2.io, [&] { d2 = two.links->drained(); ++asked; });
		while (asked != 2)
			std::this_thread::yield();
		return d1 && d2;
	}));
	stop = true;
	t1.join();
	t2.join();
	std::printf("threads: two loops, both lanes dialed both ways, 2,000 frames each way\n");
	(void)drained;
}

// S2: node 1's dial finishes TLS, then node 2's own dial establishes the lane, so node 1's
// offer gives up quietly. The given-up connection is aborted and released, not kept to stop().
void abandoned_dial() {
	Loop loop;
	Host one(1, loop), two(2, loop);
	one.open(to(two));
	two.open(to(one));
	Result<std::shared_ptr<Connection>> c1;
	c1.spawn(loop, one.links->connect(2, LinkLane::Control));
	assert(loop.until([&] { return c1.value.has_value(); }) && *c1.value);
	Result<bool> two_dialed;
	two_dialed.spawn(loop, two.links->dial(1, LinkLane::Control));
	assert(loop.until([&] { return two_dialed.value.has_value(); }) && *two_dialed.value);
	std::weak_ptr<Connection> weak = *c1.value;
	Result<bool> offered;
	offered.spawn(loop, one.links->offer(2, LinkLane::Control, std::move(*c1.value)));
	c1.value.reset();
	assert(loop.until([&] { return offered.value.has_value(); }));
	const bool released = loop.until([&] { return weak.expired(); }, 1s);
	std::printf("abandoned_dial: offer %s, abandoned %llu, the given-up connection %s\n", *offered.value ? "won" : "gave up",
				(unsigned long long)one.links->stats().abandoned, released ? "released" : "still held");
	assert(!*offered.value && one.links->stats().abandoned == 1 && released);
	assert(one.links->find(2, LinkLane::Control) && !one.links->find(2, LinkLane::Control)->dialed);
	one.stop();
	two.stop();
}

// N1: 16 idle connections that never start TLS, all from 127.0.0.1 as the real dialer is.
// With the default per-source cap they keep 2 slots; with the cap lifted above the pool's
// size they fill it, and a full pool drops its oldest. Either way node 1's dial still links.
void flood() {
	for (const bool per_source : {true, false}) {
		Loop loop;
		Host one(1, loop), two(2, loop);
		LinkOptions options;
		if (!per_source)
			options.handshakes_per_source = options.handshakes + 1;
		one.open(to(two));
		two.open(to(one), options);
		std::vector<tcp::socket> idle;
		for (int i = 0; i != 16; ++i) {
			idle.emplace_back(loop.io);
			idle.back().connect(tcp::endpoint(asio::ip::address_v4::loopback(), two.port()));
		}
		const auto kept = per_source ? options.handshakes_per_source : options.handshakes;
		const bool settled = loop.until([&] { return two.links->stats().evicted == 16 - kept; }, 5s);
		std::printf("flood: %s, 16 idle connections, %llu evicted\n", per_source ? "per-source cap 2" : "pool of 4 only",
					(unsigned long long)two.links->stats().evicted);
		assert(settled);
		Result<bool> dialed;
		dialed.spawn(loop, one.links->dial(2, LinkLane::Control));
		assert(loop.until([&] { return dialed.value.has_value(); }));
		std::printf("flood: node 1's dial %s with the idle connections still open\n", *dialed.value ? "linked" : "failed");
		assert(*dialed.value && two.links->stats().established == 1 && two.links->stats().rejected == 0);
		same_link(one, two, LinkLane::Control, 1);
		one.stop();
		two.stop();
	}
}

// N9: node 1 is configured at ::1 and dials from it; 127.0.0.1 is an address no route names, and
// its per-source cap is lifted so one address fills the pool of 4. A pending handshake from ::1
// (a socket that never starts TLS) outlives 16 unknown arrivals; then, while a thread opens
// connections from 127.0.0.1 as fast as it can, node 1's dials of both lanes link.
bool open_socket(tcp::socket &s) {
	char c;
	const auto n = ::recv(s.native_handle(), &c, 1, MSG_DONTWAIT | MSG_PEEK);
	return n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK);
}
void churn() {
	Loop loop;
	Host one(1, loop), two(2, loop);
	one.v6.emplace(loop.io, tcp::endpoint(asio::ip::address_v6::loopback(), 0));
	two.v6.emplace(loop.io, tcp::endpoint(asio::ip::address_v6::loopback(), 0));
	LinkOptions options;
	options.handshakes_per_source = options.handshakes + 1;
	one.open({{2, {"::1", two.port6()}}});
	two.open({{1, {"::1", one.port6()}}}, options);
	tcp::socket pending(loop.io);
	pending.connect(tcp::endpoint(asio::ip::address_v6::loopback(), two.port6()));
	loop.until([] { return false; }, 200ms);  // node 2 accepts it first: the pool's oldest
	std::vector<tcp::socket> idle;
	for (int i = 0; i != 16; ++i) {
		idle.emplace_back(loop.io);
		idle.back().connect(tcp::endpoint(asio::ip::address_v4::loopback(), two.port()));
	}
	const bool settled = loop.until([&] { return two.links->stats().evicted == 16 - 3; }, 5s);
	loop.until([] { return false; }, 100ms);
	const bool kept = open_socket(pending);
	std::printf("churn: 16 connections from 127.0.0.1, %llu evicted; the pending one from ::1 %s\n",
				(unsigned long long)two.links->stats().evicted, kept ? "kept" : "EVICTED");
	assert(settled && kept);
	idle.clear();
	pending.close();

	std::atomic<bool> stop{false};
	std::atomic<std::uint64_t> opened{0};
	std::thread flood([&] {
		asio::io_context io;
		while (!stop) {
			tcp::socket s(io);
			asio::error_code error;
			s.connect(tcp::endpoint(asio::ip::address_v4::loopback(), two.port()), error);
			opened += !error;
		}
	});
	const auto begun = std::chrono::steady_clock::now();
	Result<bool> control, bulk;
	control.spawn(loop, one.links->dial(2, LinkLane::Control));
	bulk.spawn(loop, one.links->dial(2, LinkLane::Bulk));
	loop.until([&] { return control.value.has_value() && bulk.value.has_value(); }, 15s);
	const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begun).count();
	stop = true;
	flood.join();
	const bool linked = control.value && *control.value && bulk.value && *bulk.value;
	std::printf("churn: %llu connections from 127.0.0.1 in %lld ms (%llu/s); node 1's dials from ::1 %s\n", (unsigned long long)opened,
				(long long)ms, (unsigned long long)(ms ? opened * 1000 / std::uint64_t(ms) : 0), linked ? "linked" : "FAILED");
	assert(linked);
	one.stop();
	two.stop();
}

// N5 (transport-connection review): a lookup's thread outlives its Links. The resolver blocks on
// a gate until Links, its hosts and their io_context are destroyed; released, its answer finds
// nothing to deliver to (run under ASan: no use after free).
void resolver_outlives() {
	struct Gate {
		std::mutex mutex;
		std::condition_variable cv;
		bool open = false, entered = false, returned = false;
	};
	auto gate = std::make_shared<Gate>();
	{
		Loop loop;
		Host one(1, loop), two(2, loop);
		LinkOptions options;
		options.resolve_deadline = 100ms;
		options.resolver = [gate](const std::string &) {
			std::unique_lock hold(gate->mutex);
			gate->entered = true;
			gate->cv.notify_all();
			gate->cv.wait(hold, [&] { return gate->open; });
			gate->returned = true;
			gate->cv.notify_all();
			return std::vector<asio::ip::address>{asio::ip::address_v4::loopback()};
		};
		one.open({{2, {"gate.test", two.port()}}}, options);
		two.open(to(one));
		Result<bool> dialed;
		dialed.spawn(loop, one.links->dial(2, LinkLane::Control));
		assert(loop.until([&] { return dialed.value.has_value(); }));
		std::unique_lock hold(gate->mutex);
		assert(gate->entered && !*dialed.value);
		hold.unlock();
		one.stop();
		two.stop();
		one.links.reset();
		two.links.reset();
	}
	std::unique_lock hold(gate->mutex);
	gate->open = true;
	gate->cv.notify_all();
	const bool returned = gate->cv.wait_for(hold, 5s, [&] { return gate->returned; });
	hold.unlock();
	std::this_thread::sleep_for(200ms);  // the answer's delivery runs after the resolver returns
	std::printf("resolver_outlives: the lookup's thread returned %s after Links and its loop were gone\n", returned ? "and ended" : "NEVER");
	assert(returned);
}

struct Case {
	const char *name;
	void (*run)();
};
constexpr Case cases[] = {{"dial", dial},	  {"simultaneous", simultaneous},	  {"draining", draining},
						  {"lookup", lookup}, {"threads", threads}, {"abandoned_dial", abandoned_dial},
						  {"flood", flood}, {"churn", churn},
						  {"resolver_outlives", resolver_outlives}};
} // namespace

int main(int argc, char **argv) {
	assert(argc >= 2);
	fixture = argv[1];
	int ran = 0;
	for (const auto &c : cases) {
		if (argc > 2 && std::strcmp(argv[2], c.name) != 0)
			continue;
		c.run();
		++ran;
	}
	if (!ran) {
		std::fprintf(stderr, "unknown case\n");
		return 2;
	}
	std::printf("links: %d cases passed\n", ran);
	return 0;
}
