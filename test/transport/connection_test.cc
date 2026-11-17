// The host connection over loopback TCP with the TLS fixture: node authentication by the
// host's pins (A2), frames, the keepalive and the dead-peer deadline, graceful close (A4).
//   consensus_transport_connection_test <fixture dir> [case]
#include "consensus/transport/connection.h"
#include <asio/connect.hpp>
#include <asio/io_context.hpp>
#include <asio/post.hpp>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <openssl/pem.h>
#include <optional>

using namespace kronuz::consensus::transport;
using namespace std::chrono_literals;
using kronuz::consensus::NodeId;
using tcp = asio::ip::tcp;

namespace {
std::filesystem::path fixture;

Fingerprint pin_of(const std::string &name) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file((fixture / (name + ".pem")).c_str(), "r"), BIO_free);
	assert(input);
	std::unique_ptr<X509, decltype(&X509_free)> leaf(PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
	assert(leaf);
	return certificate_fingerprint(leaf.get());
}
// A node's context; `ca` is the authority it trusts. An empty name presents no certificate.
std::shared_ptr<asio::ssl::context> context(const std::string &name, const std::string &ca = "ca") {
	auto c = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	if (name.empty())
		reactor::tls::configure(*c, {"", "", (fixture / (ca + ".pem")).string()}, reactor::tls::Role::Client);
	else
		reactor::tls::configure(*c, {(fixture / (name + ".pem")).string(), (fixture / (name + ".key")).string(),
									 (fixture / (ca + ".pem")).string()},
								reactor::tls::Role::Peer);
	return c;
}
std::map<NodeId, Fingerprint> pins12() { return {{1, pin_of("one")}, {2, pin_of("two")}}; }
kronuz::consensus::Identity id(char c) {
	kronuz::consensus::Identity r{};
	r.fill(c);
	return r;
}

// One loop on this thread: the io_context and the task scheduler over it.
struct Loop {
	asio::io_context io;
	task::asio_scheduler scheduler{io.get_executor()};
	task::scheduler_ref ref() { return task::scheduler_ref::of(scheduler); }
	// Runs the loop until done() or `limit` passes; false on the limit.
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
	template <class T> std::optional<T> run(task::task<T> t, std::exception_ptr *error = nullptr) {
		std::optional<T> result;
		bool done = false;
		task::spawn(ref(), std::move(t), [&](std::exception_ptr e, std::optional<T> v) {
			if (error)
				*error = e;
			else if (e)
				std::rethrow_exception(e);
			result = std::move(v);
			done = true;
		});
		assert(until([&] { return done; }));
		return result;
	}
	void run(task::task<void> t) {
		bool done = false;
		task::spawn(ref(), std::move(t), [&](std::exception_ptr e) {
			if (e)
				std::rethrow_exception(e);
			done = true;
		});
		assert(until([&] { return done; }));
	}
	// Runs f on the loop's thread, as every Connection call must be.
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

// A connected loopback pair, (dialed, accepted).
std::pair<tcp::socket, tcp::socket> tcp_pair(asio::io_context &io) {
	tcp::acceptor acceptor(io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
	tcp::socket a(io), b(io);
	a.connect(acceptor.local_endpoint());
	acceptor.accept(b);
	return {std::move(a), std::move(b)};
}

struct Pair {
	Loop &loop;
	std::unique_ptr<Connection> dialer, acceptor;
	Pair(Loop &l, std::shared_ptr<asio::ssl::context> dialing, std::shared_ptr<asio::ssl::context> accepting,
		 ConnectionOptions options = {})
		: loop(l) {
		auto [a, b] = tcp_pair(l.io);
		dialer = std::make_unique<Connection>(l.ref(), std::move(a), std::move(dialing), true, options);
		acceptor = std::make_unique<Connection>(l.ref(), std::move(b), std::move(accepting), false, options);
	}
	// Both handshakes; each side's authenticated node, or the error it threw.
	struct Side {
		NodeId node = 0;
		std::string error;
	};
	task::task<void> side(Connection &c, const NodePins &pins, NodeId expected, Side &out) {
		late_deadline d(loop.ref(), 2s);
		try {
			out.node = co_await c.handshake(pins, expected, d.token());
		} catch (const std::exception &e) {
			out.error = e.what();
			c.abort();
		}
	}
	std::pair<Side, Side> handshake(const NodePins &dialing, NodeId expected, const NodePins &accepting) {
		Side d, a;
		loop.run(task::when_all(side(*dialer, dialing, expected, d), side(*acceptor, accepting, 0, a)));
		return {d, a};
	}
	// Handshakes and hellos, as a connection is established.
	void establish(const NodePins &dialing, const NodePins &accepting) {
		auto [d, a] = handshake(dialing, accepting.local(), accepting);
		assert(d.node == accepting.local() && a.node == dialing.local());
		auto hellos = [&]() -> task::task<void> {
			late_deadline deadline(loop.ref(), 2s);
			NodeHello mine{dialing.local(), LinkLane::Control, false, id('K'), id('d'), 1, 1};
			NodeHello theirs{accepting.local(), LinkLane::Control, false, id('K'), id('a'), 1, 1};
			co_await dialer->write_hello(mine, deadline.token());
			auto got = co_await acceptor->read_hello(deadline.token());
			assert(got == mine);
			co_await acceptor->write_hello(theirs, deadline.token());
			assert(co_await dialer->read_hello(deadline.token()) == theirs);
		};
		loop.run(hellos());
	}
};

struct Ends {
	std::optional<ConnectionEnd> dialer, acceptor;
	std::vector<std::string> to_dialer, to_acceptor;
};
// Runs both ends' run() as roots; the result lands in `ends`.
void run_both(Pair &p, Ends &ends) {
	task::spawn(p.loop.ref(), p.dialer->run([&](std::string_view f) { ends.to_dialer.emplace_back(f); }),
				[&](std::exception_ptr e, std::optional<ConnectionEnd> v) {
					assert(!e);
					ends.dialer = v;
				});
	task::spawn(p.loop.ref(), p.acceptor->run([&](std::string_view f) { ends.to_acceptor.emplace_back(f); }),
				[&](std::exception_ptr e, std::optional<ConnectionEnd> v) {
					assert(!e);
					ends.acceptor = v;
				});
}

void authenticate() {
	Loop loop;
	NodePins one(1, pins12(), context("one")->native_handle());
	NodePins two(2, pins12(), context("two")->native_handle());
	Pair p(loop, context("one"), context("two"));
	auto [d, a] = p.handshake(one, 2, two);
	assert(d.node == 2 && a.node == 1 && d.error.empty() && a.error.empty());
	std::printf("authenticate: node 1 dialed node 2, each authenticated the other by its pin\n");
}

// A dial meant for node 2 reaches node 3, which is pinned: refused before any frame.
void wrong_node() {
	Loop loop;
	auto pins = pins12();
	pins.emplace(3, pin_of("three"));
	NodePins one(1, pins, context("one")->native_handle());
	NodePins three(3, pins, context("three")->native_handle());
	Pair p(loop, context("one"), context("three"));
	auto [d, a] = p.handshake(one, 2, three);
	assert(d.node == 0 && d.error == "node pins: another node's certificate");
	// A peer presenting this node's own certificate.
	Pair self(loop, context("one"), context("one"));
	auto [ds, as] = self.handshake(one, 0, one);
	assert(ds.node == 0 && ds.error == "node pins: the peer presented this node's certificate");
	assert(as.node == 0 && as.error == "node pins: the peer presented this node's certificate");
	std::printf("wrong_node: another node's and this node's own certificate refused\n");
}

// A certificate the CA signed but no pin names, on either side.
void unpinned() {
	Loop loop;
	auto with3 = pins12();
	with3.emplace(3, pin_of("three"));
	NodePins one(1, pins12(), context("one")->native_handle());
	NodePins three(3, with3, context("three")->native_handle());
	{
		Pair p(loop, context("one"), context("three"));
		auto [d, a] = p.handshake(one, 0, three);
		assert(d.node == 0 && d.error == "node pins: unpinned peer certificate");
	}
	{
		Pair p(loop, context("three"), context("one"));
		auto [d, a] = p.handshake(three, 1, one);
		assert(a.node == 0 && a.error == "node pins: unpinned peer certificate");
	}
	std::printf("unpinned: refused by the dialer and by the acceptor\n");
}

// No client certificate, or a server the client's CA did not sign: TLS refuses.
void no_certificate() {
	Loop loop;
	NodePins two(2, pins12(), context("two")->native_handle());
	NodePins one(1, pins12(), context("one")->native_handle());
	{
		Pair p(loop, context(""), context("two"));
		auto [d, a] = p.handshake(one, 2, two);
		assert(a.node == 0 && a.error == "connection: TLS handshake failed");
	}
	{
		Pair p(loop, context("one", "rogue"), context("two"));
		auto [d, a] = p.handshake(one, 2, two);
		assert(d.node == 0 && d.error == "connection: TLS handshake failed");
	}
	std::printf("no_certificate: a client without one and an untrusted server refused\n");
}

void pins() {
	auto ctx = context("one");
	auto refused = [&](NodeId local, std::map<NodeId, Fingerprint> p, std::string_view expect) {
		try {
			NodePins n(local, std::move(p), ctx->native_handle());
		} catch (const std::invalid_argument &e) {
			assert(std::string_view(e.what()) == expect);
			return;
		}
		assert(false);
	};
	auto p = pins12();
	p.emplace(0, pin_of("three"));
	refused(1, p, "node pins: node 0 or a repeated certificate");
	p = pins12();
	p.emplace(3, pin_of("two"));
	refused(1, p, "node pins: node 0 or a repeated certificate");
	refused(3, pins12(), "node pins: no pin for the local node");
	refused(2, pins12(), "node pins: the local certificate is not the local node's pin");
	std::printf("pins: node 0, a repeated pin, a missing and a mismatched local pin refused\n");
}

NodePins pins_of(NodeId node, const char *name) { return NodePins(node, pins12(), context(name)->native_handle()); }

void frames() {
	Loop loop;
	auto one = pins_of(1, "one"), two = pins_of(2, "two");
	Pair p(loop, context("one"), context("two"));
	p.establish(one, two);
	Ends ends;
	const std::string big(4 * 1024 * 1024, 'b');
	p.dialer->send("a");
	p.dialer->send(std::string(100000, 'm'));
	p.dialer->send(big);
	p.acceptor->send("reply");
	run_both(p, ends);
	assert(loop.until([&] { return ends.to_acceptor.size() == 3 && ends.to_dialer.size() == 1; }));
	assert(ends.to_acceptor[0] == "a" && ends.to_acceptor[1] == std::string(100000, 'm') && ends.to_acceptor[2] == big);
	assert(ends.to_dialer[0] == "reply");
	loop.call([&] { p.dialer->close(); });
	assert(loop.until([&] { return ends.dialer && ends.acceptor; }));
	std::printf("frames: 1 B, 100 KB and 4 MiB in order, one reply\n");
}

// An idle connection sends a keepalive each interval, once per connection, and stays open.
void keepalive() {
	Loop loop;
	auto one = pins_of(1, "one"), two = pins_of(2, "two");
	ConnectionOptions options;
	options.keepalive = 50ms;
	options.dead_after = 300ms;
	Pair p(loop, context("one"), context("two"), options);
	p.establish(one, two);
	Ends ends;
	run_both(p, ends);
	const auto start = std::chrono::steady_clock::now();
	loop.until([] { return false; }, 1000ms);
	const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
	const auto sent = p.dialer->keepalives_sent(), received = p.acceptor->keepalives_received();
	std::printf("keepalive: %lld ms idle, %llu sent, %llu received, %llu the other way\n", (long long)ms,
				(unsigned long long)sent, (unsigned long long)received, (unsigned long long)p.acceptor->keepalives_sent());
	assert(!ends.dialer && !ends.acceptor);
	assert(sent >= 10 && sent <= std::uint64_t(ms / 50) + 1 && received + 1 >= sent);
	assert(p.acceptor->keepalives_sent() >= 10 && p.acceptor->keepalives_sent() <= std::uint64_t(ms / 50) + 1);
	loop.call([&] { p.dialer->close(); });
	assert(loop.until([&] { return ends.dialer && ends.acceptor; }));
	assert(ends.dialer == ConnectionEnd::closed && ends.acceptor == ConnectionEnd::closed);
}

// A peer that stops sending, keepalives included, ends the connection at dead_after.
void dead_peer() {
	Loop loop;
	auto one = pins_of(1, "one"), two = pins_of(2, "two");
	ConnectionOptions options;
	options.keepalive = 50ms;
	options.dead_after = 300ms;
	Pair p(loop, context("one"), context("two"), options);
	p.establish(one, two);
	std::optional<ConnectionEnd> end;
	const auto start = std::chrono::steady_clock::now();
	task::spawn(loop.ref(), p.dialer->run({}), [&](std::exception_ptr e, std::optional<ConnectionEnd> v) {
		assert(!e);
		end = v;
	});
	assert(loop.until([&] { return end.has_value(); }, 3s));
	const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
	std::printf("dead_peer: ended %s after %lld ms\n", end == ConnectionEnd::dead ? "dead" : "otherwise", (long long)ms);
	assert(end == ConnectionEnd::dead && ms >= 295 && ms < 1000);
}

// T8's transport half: close() sends every queued frame, then close_notify; the peer reads
// them all and both ends finish closed, not failed.
void graceful_close() {
	Loop loop;
	auto one = pins_of(1, "one"), two = pins_of(2, "two");
	Pair p(loop, context("one"), context("two"));
	p.establish(one, two);
	Ends ends;
	run_both(p, ends);
	loop.until([] { return false; }, 20ms);  // both writers idle, waiting
	bool refused = false;
	loop.call([&] {
		for (int i = 0; i != 200; ++i)
			p.dialer->send(std::to_string(i) + std::string(10000, 'x'));
		p.dialer->close();
		try {
			p.dialer->send("late");
		} catch (const std::logic_error &) {
			refused = true;
		}
	});
	assert(refused && p.dialer->draining());
	assert(loop.until([&] { return ends.dialer && ends.acceptor; }));
	std::printf("graceful_close: %zu of 200 frames arrived; dialer %d, acceptor %d\n", ends.to_acceptor.size(),
				int(*ends.dialer), int(*ends.acceptor));
	assert(ends.to_acceptor.size() == 200);
	for (int i = 0; i != 200; ++i)
		assert(ends.to_acceptor[i] == std::to_string(i) + std::string(10000, 'x'));
	assert(ends.dialer == ConnectionEnd::closed && ends.acceptor == ConnectionEnd::closed);
}

// An abort sends no close_notify: the peer ends failed (a truncated stream), never closed.
void abort_truncates() {
	Loop loop;
	auto one = pins_of(1, "one"), two = pins_of(2, "two");
	Pair p(loop, context("one"), context("two"));
	p.establish(one, two);
	Ends ends;
	run_both(p, ends);
	loop.until([] { return false; }, 50ms);
	loop.call([&] { p.dialer->abort(); });
	assert(loop.until([&] { return ends.dialer && ends.acceptor; }));
	assert(ends.dialer == ConnectionEnd::failed && ends.acceptor == ConnectionEnd::failed);
	std::printf("abort_truncates: both ends failed\n");
}

struct Case {
	const char *name;
	void (*run)();
};
constexpr Case cases[] = {{"authenticate", authenticate}, {"wrong_node", wrong_node}, {"unpinned", unpinned},
						  {"no_certificate", no_certificate}, {"pins", pins},		 {"frames", frames},
						  {"keepalive", keepalive},	   {"dead_peer", dead_peer}, {"graceful_close", graceful_close},
						  {"abort_truncates", abort_truncates}};
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
	std::printf("connection: %d cases passed\n", ran);
	return 0;
}
