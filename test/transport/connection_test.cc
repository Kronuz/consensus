// The host connection over loopback TCP with the TLS fixture: node authentication by the
// host's pins (A2), frames, the keepalive and the dead-peer deadline, graceful close (A4), the
// fd kept until run() returns, a slow but steady reader and a busy one (security review S1, N2),
// a peer that never reads, and loops handshaking at once with the context Links took.
//   consensus_transport_connection_test <fixture dir> [case]
#include "consensus/transport/connection.h"
#include "consensus/transport/links.h"
#include <algorithm>
#include <asio/connect.hpp>
#include <asio/io_context.hpp>
#include <arpa/inet.h>
#include <asio/post.hpp>
#include <atomic>
#include <cassert>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <netinet/in.h>
#include <openssl/pem.h>
#include <optional>
#include <poll.h>
#include <sys/ioctl.h>
#include <thread>
#include <unistd.h>

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
		: Pair(l, tcp_pair(l.io), std::move(dialing), std::move(accepting), options, options) {}
	Pair(Loop &l, std::pair<tcp::socket, tcp::socket> sockets, std::shared_ptr<asio::ssl::context> dialing,
		 std::shared_ptr<asio::ssl::context> accepting, ConnectionOptions dialing_options,
		 ConnectionOptions accepting_options)
		: loop(l) {
		dialer = std::make_unique<Connection>(l.ref(), std::move(sockets.first), std::move(dialing), true,
											  dialing_options);
		acceptor = std::make_unique<Connection>(l.ref(), std::move(sockets.second), std::move(accepting), false,
												accepting_options);
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
	// Open after 1 s against a 300 ms dead_after: the keepalives kept it. The counts' upper bound
	// is one per interval; a runner that stops the process only lowers them (D100).
	assert(!ends.dialer && !ends.acceptor);
	assert(sent >= 1 && sent <= std::uint64_t(ms / 50) + 1 && received + 1 >= sent);
	assert(p.acceptor->keepalives_sent() >= 1 && p.acceptor->keepalives_sent() <= std::uint64_t(ms / 50) + 1);
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
	assert(end == ConnectionEnd::dead && ms >= 295);  // never early; a runner's stop only delays it
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

// A socket that takes the connection's fd number after an abort (dup2 onto it, the worst a
// socket opened elsewhere could do): the connection must neither read its bytes nor write onto
// it. `kept` is the stranger's own end under another number, `remote` its peer.
struct Stranger {
	static constexpr std::string_view bytes = "STRANGER-BYTES-0123456789";
	int kept = -1, remote = -1;
	void take(int old) {
		int fds[2];
		assert(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
		kept = fds[0];
		remote = fds[1];
		assert(::dup2(kept, old) == old);
		for (int fd : {kept, remote})
			::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
		assert(::write(remote, bytes.data(), bytes.size()) == ssize_t(bytes.size()));
	}
	~Stranger() {
		for (int fd : {kept, remote})
			if (fd >= 0)
				::close(fd);
	}
	// The stranger's bytes still unread, and the bytes something wrote from its end.
	void check(const char *name) const {
		int unread = 0;
		::ioctl(kept, FIONREAD, &unread);
		char buffer[4096];
		const ssize_t got = ::recv(remote, buffer, sizeof buffer, 0);
		std::printf("%s: of the stranger's %zu bytes %d unread, %zd written onto it\n", name, bytes.size(), unread,
					got < 0 ? ssize_t(0) : got);
		assert(unread == int(bytes.size()) && got <= 0);
	}
};
// The local port of `fd`, 0 when it is not an open socket.
std::uint16_t port_of(int fd) {
	sockaddr_storage address{};
	socklen_t size = sizeof address;
	if (::getsockname(fd, reinterpret_cast<sockaddr *>(&address), &size) != 0)
		return 0;
	return ntohs(reinterpret_cast<sockaddr_in &>(address).sin_port);
}
int fd_of(const Connection &c) { return const_cast<tcp::socket &>(c.socket()).native_handle(); }

// S1: a frame handler aborts, as an on_frame that drops a peer would. The fd stays the
// connection's until run() returns, and the reader makes no SSL call after the abort.
void fd_reuse_reader() {
	Loop loop;
	auto one = pins_of(1, "one"), two = pins_of(2, "two");
	Pair p(loop, context("one"), context("two"));
	p.establish(one, two);
	Stranger s;
	std::optional<ConnectionEnd> a;
	const int old = fd_of(*p.acceptor);
	const auto port = port_of(old);
	task::spawn(loop.ref(), p.acceptor->run([&](std::string_view) {
		if (s.kept >= 0)
			return;
		p.acceptor->abort();
		assert(port_of(old) == port);  // still this connection's socket
		s.take(old);
	}),
				[&](std::exception_ptr e, std::optional<ConnectionEnd> v) {
					assert(!e);
					a = v;
				});
	task::spawn(loop.ref(), p.dialer->run({}), [](std::exception_ptr, std::optional<ConnectionEnd>) {});
	loop.call([&] {
		p.dialer->send("one");
		p.dialer->send("two");
	});
	assert(loop.until([&] { return a.has_value(); }, 3s));
	loop.until([] { return false; }, 50ms);
	assert(s.kept >= 0 && a == ConnectionEnd::failed);
	s.check("fd_reuse_reader");
}

// S1: abort() from the loop with a frame queued; the writer makes no SSL call after it.
void fd_reuse_writer() {
	Loop loop;
	auto one = pins_of(1, "one"), two = pins_of(2, "two");
	Pair p(loop, context("one"), context("two"));
	p.establish(one, two);
	Ends ends;
	run_both(p, ends);
	loop.until([] { return false; }, 30ms);  // both writers idle
	Stranger s;
	loop.call([&] {
		p.acceptor->send("queued before the abort");
		const int old = fd_of(*p.acceptor);
		const auto port = port_of(old);
		p.acceptor->abort();
		assert(port_of(old) == port);
		s.take(old);
	});
	assert(loop.until([&] { return ends.acceptor && ends.dialer; }, 3s));
	loop.until([] { return false; }, 50ms);
	assert(ends.acceptor == ConnectionEnd::failed && ends.dialer == ConnectionEnd::failed);
	s.check("fd_reuse_writer");
}

// The longest this process went unscheduled during a run: a thread asks to sleep 1 ms and
// records how long it slept. A host stop or starvation shows here as a gap.
class stall_watch {
  public:
	~stall_watch() {
		stop_ = true;
		thread_.join();
	}
	long long worst_ms() const { return worst_; }

  private:
	std::atomic<bool> stop_{false};
	std::atomic<long long> worst_{0};
	std::thread thread_{[this] {
		while (!stop_) {
			const auto before = std::chrono::steady_clock::now();
			std::this_thread::sleep_for(1ms);
			const auto slept =
				std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - before).count();
			worst_ = std::max<long long>(worst_, slept);
		}
	}};
};

// The longest a socket's send queue held bytes without draining, sampled every 2 ms from its
// own thread (on a dup of the fd, so the number stays this socket's): a host whose network stack
// withholds the bytes, as macOS does to a background process's sockets, shows here.
class drain_watch {
  public:
	explicit drain_watch(int fd) : fd_(::dup(fd)) {}
	~drain_watch() {
		stop_ = true;
		thread_.join();
		::close(fd_);
	}
	long long worst_ms() const { return worst_; }

  private:
	long queued() const {
		int n = -1;
#if defined(SIOCOUTQ)
		(void)::ioctl(fd_, SIOCOUTQ, &n);
#elif defined(SO_NWRITE)
		socklen_t size = sizeof n;
		(void)::getsockopt(fd_, SOL_SOCKET, SO_NWRITE, &n, &size);
#elif defined(FIONWRITE)
		(void)::ioctl(fd_, FIONWRITE, &n);
#endif
		return n;
	}
	int fd_;
	std::atomic<bool> stop_{false};
	std::atomic<long long> worst_{0};
	std::thread thread_{[this] {
		auto drained = std::chrono::steady_clock::now();
		for (long last = 0; !stop_; std::this_thread::sleep_for(2ms)) {
			const long now = queued();
			const auto at = std::chrono::steady_clock::now();
			if (now <= 0 || now < last)
				drained = at;
			last = now;
			worst_ = std::max<long long>(worst_, std::chrono::duration_cast<std::chrono::milliseconds>(at - drained).count());
		}
	}};
};

// A reader on its own loop thread whose handler holds that loop `per_frame` for each frame.
// With `coarse`, the kernel reports the writer's socket writable only once 1.5 MiB drained (its
// send buffer 1.5 MiB, its low-water mark too where settable) and the reader's buffer is 512 KiB.
struct reader_load {
	const char *name;
	int frames;
	std::size_t size;
	std::chrono::milliseconds per_frame, dead_after;
	bool coarse = false;
};
struct reader_run {
	int got;
	long long ms, stalled_ms, held_ms;
	ConnectionEnd dialer, acceptor;
	std::uint64_t keepalives, drained_expiries;
	bool coarse_wakes;  // the writer's low-water mark took: it wakes only at an empty queue
};

reader_run read_slowly_once(const reader_load &load) {
	stall_watch watch;
	Loop l1, l2;
	ConnectionOptions options;
	options.keepalive = 50ms;
	options.dead_after = load.dead_after;
	tcp::acceptor listener(l1.io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
	tcp::socket a(l1.io), b(l2.io);
	bool coarse_wakes = false;
	if (load.coarse) {
		const int buffer = 3 << 19, reading = 512 * 1024;
		a.open(tcp::v4());
		::setsockopt(a.native_handle(), SOL_SOCKET, SO_SNDBUF, &buffer, sizeof buffer);
		(void)::setsockopt(a.native_handle(), SOL_SOCKET, SO_SNDLOWAT, &buffer, sizeof buffer);  // Linux refuses it
		::setsockopt(listener.native_handle(), SOL_SOCKET, SO_RCVBUF, &reading, sizeof reading);
		int lowat = 0;
		socklen_t size = sizeof lowat;
		coarse_wakes = ::getsockopt(a.native_handle(), SOL_SOCKET, SO_SNDLOWAT, &lowat, &size) == 0 &&
					   lowat >= buffer / 2;
	}
	a.connect(listener.local_endpoint());
	listener.accept(b);
	Connection dialer(l1.ref(), std::move(a), context("one"), true, options);
	Connection acceptor(l2.ref(), std::move(b), context("two"), false, options);
	auto one = pins_of(1, "one"), two = pins_of(2, "two");
	std::atomic<bool> stop{false};
	std::thread t2([&] {
		while (!stop) {
			l2.io.restart();
			l2.io.run_for(5ms);
		}
	});
	std::atomic<int> ready{0};
	auto accept_side = [&]() -> task::task<void> {
		late_deadline d(l2.ref(), 2s);
		(void)co_await acceptor.handshake(two, 0, d.token());
		(void)co_await acceptor.read_hello(d.token());
		co_await acceptor.write_hello({2, LinkLane::Bulk, false, id('K'), id('a'), 1, 1}, d.token());
	};
	auto dial_side = [&]() -> task::task<void> {
		late_deadline d(l1.ref(), 2s);
		(void)co_await dialer.handshake(one, 2, d.token());
		co_await dialer.write_hello({1, LinkLane::Bulk, false, id('K'), id('d'), 1, 1}, d.token());
		(void)co_await dialer.read_hello(d.token());
	};
	auto counted = [&](std::exception_ptr e) {
		assert(!e);
		++ready;
	};
	asio::post(l2.io, [&] { task::spawn(l2.ref(), accept_side(), counted); });
	task::spawn(l1.ref(), dial_side(), counted);
	assert(l1.until([&] { return ready == 2; }));
	const int count = load.frames;
	const std::size_t size = load.size;
	std::atomic<int> got{0};
	std::atomic<bool> acceptor_done{false};
	std::optional<ConnectionEnd> acceptor_end, dialer_end;
	asio::post(l2.io, [&] {
		task::spawn(l2.ref(), acceptor.run([&](std::string_view f) {
			assert(f.size() == size);
			std::this_thread::sleep_for(load.per_frame);
			++got;
		}),
					[&](std::exception_ptr e, std::optional<ConnectionEnd> v) {
						assert(!e);
						acceptor_end = v;
						acceptor_done = true;
					});
	});
	task::spawn(l1.ref(), dialer.run({}), [&](std::exception_ptr e, std::optional<ConnectionEnd> v) {
		assert(!e);
		dialer_end = v;
	});
	std::optional<drain_watch> held(std::in_place, fd_of(dialer));
	const auto start = std::chrono::steady_clock::now();
	l1.call([&] {
		for (int i = 0; i != count; ++i)
			dialer.send(std::string(size, char('a' + i % 26)));
		dialer.close();
	});
	assert(l1.until([&] { return dialer_end && acceptor_done; }, 60s));
	const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
	const auto held_ms = held->worst_ms();
	held.reset();
	stop = true;
	t2.join();
	return {got, ms, watch.worst_ms(), held_ms, *dialer_end, *acceptor_end, acceptor.keepalives_sent(),
			dialer.drained_expiries(), coarse_wakes};
}
// Both ends share this process, so a host stop silences the reader's keepalives too, which no
// two hosts do, and the host's network stack may withhold the bytes (macOS throttles a
// background process's sockets): a failed run the host stalled for a third of dead_after, or a
// coarse writer's run that ended stuck after its queue went half of dead_after without draining
// (it writes only after a wait, so the sampled queue shows every drain), runs again, at most
// twice. Only a whole run passes, and any other failure fails at once.
void read_slowly(const reader_load &load) {
	for (int attempt = 1;; ++attempt) {
		const auto r = read_slowly_once(load);
		std::printf("%s: %d of %d frames read in %lld ms against a %lld ms dead_after; dialer %d, acceptor %d; "
					"%llu keepalives from the reader; %llu write waits expired with the queue drained%s; "
					"the process stalled %lld ms at most; the queue held %lld ms at most without draining\n",
					load.name, r.got, load.frames, r.ms, (long long)load.dead_after.count(), int(r.dialer),
					int(r.acceptor), (unsigned long long)r.keepalives, (unsigned long long)r.drained_expiries,
					r.coarse_wakes ? " (wakes at an empty queue)" : "", r.stalled_ms, r.held_ms);
		if (r.got == load.frames && r.dialer == ConnectionEnd::closed && r.acceptor == ConnectionEnd::closed) {
			assert(r.ms >= load.frames * load.per_frame.count());
			assert(!r.coarse_wakes || r.drained_expiries > 0);  // the case reached the rule it tests
			return;
		}
		const bool stalled = r.stalled_ms >= load.dead_after.count() / 3;
		const bool held = load.coarse && r.dialer == ConnectionEnd::failed && r.held_ms >= load.dead_after.count() / 2;
		assert((stalled || held) && attempt < 3);
	}
}
// N2: a peer reading steadily, slower than the queue drains, is not stuck: 20 ms per 512 KiB
// frame, 32 MiB over 1.2 s against a 300 ms dead_after, with write progress every frame.
void slow_reader() { read_slowly({"slow_reader", 64, 512 * 1024, 20ms, 300ms}); }
// N2's keepalive: a reader whose loop is busy 400 ms per frame sends its overdue keepalive at
// each turn between frames. Held to the late rule's extension it would go every other frame,
// 800 ms apart, against a 600 ms dead_after.
void busy_reader() { read_slowly({"busy_reader", 6, 64 * 1024, 400ms, 600ms}); }
// The socket's queue draining keeps a write wait alive: a writer the kernel wakes only at an
// empty 1.5 MiB queue (the low-water mark where settable; Linux wakes at a third free) to a
// reader of 64 KiB every 125 ms or slower waits 3 s or more, against a 2 s dead_after. The queue
// drains about once a frame: it went 133 ms without draining at most here, 643 ms under
// background QoS, whose sockets macOS throttles.
void coarse_writer() { read_slowly({"coarse_writer", 48, 64 * 1024, 125ms, 2000ms, true}); }

// A loopback relay between a dialer (up) and its acceptor (down): down to up always flows, up
// to down stops once `forward_up` goes false, so the acceptor still talks but never reads again.
class Relay {
  public:
	Relay() : listener_(::socket(AF_INET, SOCK_STREAM, 0)) {
		sockaddr_in a{};
		a.sin_family = AF_INET;
		a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		assert(::bind(listener_, reinterpret_cast<sockaddr *>(&a), sizeof a) == 0 && ::listen(listener_, 1) == 0);
	}
	~Relay() {
		stop_ = true;
		if (thread_.joinable())
			thread_.join();
		for (int fd : {listener_, up_, down_})
			if (fd >= 0)
				::close(fd);
	}
	std::uint16_t port() const { return port_of(listener_); }
	// Accepts the dialer's connection and connects it to `to`, then relays.
	void join(std::uint16_t to) {
		up_ = ::accept(listener_, nullptr, nullptr);
		down_ = ::socket(AF_INET, SOCK_STREAM, 0);
		sockaddr_in a{};
		a.sin_family = AF_INET;
		a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
		a.sin_port = htons(to);
		assert(up_ >= 0 && ::connect(down_, reinterpret_cast<sockaddr *>(&a), sizeof a) == 0);
		thread_ = std::thread([this] { pump(); });
	}
	std::atomic<bool> forward_up{true};

  private:
	static bool copy(int from, int to) {
		char buffer[64 * 1024];
		const auto n = ::read(from, buffer, sizeof buffer);
		for (ssize_t at = 0; at < n;) {
			const auto w = ::write(to, buffer + at, std::size_t(n - at));
			if (w <= 0)
				return false;
			at += w;
		}
		return n > 0;
	}
	void pump() {
		while (!stop_) {
			const bool up = forward_up;
			pollfd fds[2] = {{down_, POLLIN, 0}, {up_, short(up ? POLLIN : 0), 0}};
			if (::poll(fds, 2, 10) <= 0)
				continue;
			if ((fds[0].revents & (POLLIN | POLLHUP)) && !copy(down_, up_))
				return;
			if (up && (fds[1].revents & (POLLIN | POLLHUP)) && !copy(up_, down_))
				return;
		}
	}
	int listener_, up_ = -1, down_ = -1;
	std::atomic<bool> stop_{false};
	std::thread thread_;
};

// Review F5, the stuck rule's safety half: a peer that keeps sending keepalives but never reads
// again ends the writer `failed` once its queue stops draining, not `dead`, and not kept open.
void nonreader() {
	Loop loop;
	Relay relay;
	ConnectionOptions dialing, accepting;
	dialing.keepalive = accepting.keepalive = 50ms;
	dialing.dead_after = 300ms;
	accepting.dead_after = 60s;  // the peer stays up and keeps talking
	tcp::acceptor listener(loop.io, tcp::endpoint(asio::ip::address_v4::loopback(), 0));
	tcp::socket a(loop.io), b(loop.io);
	a.connect(tcp::endpoint(asio::ip::address_v4::loopback(), relay.port()));
	relay.join(listener.local_endpoint().port());
	listener.accept(b);
	Pair p(loop, {std::move(a), std::move(b)}, context("one"), context("two"), dialing, accepting);
	p.establish(pins_of(1, "one"), pins_of(2, "two"));
	relay.forward_up = false;
	Ends ends;
	run_both(p, ends);
	const auto start = std::chrono::steady_clock::now();
	loop.call([&] {
		for (int i = 0; i != 64; ++i)
			p.dialer->send(std::string(512 * 1024, 'q'));
	});
	const bool ended = loop.until([&] { return ends.dialer.has_value(); }, 10s);
	const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
	std::printf("nonreader: the writer %s after %lld ms against a 300 ms dead_after, end %d; the peer %s and sent "
				"%llu keepalives; %llu write waits expired with the queue drained\n",
				ended ? "ended" : "is still open", (long long)ms, ended ? int(*ends.dialer) : -1,
				ends.acceptor ? "ended" : "is open", (unsigned long long)p.dialer->keepalives_received(),
				(unsigned long long)p.dialer->drained_expiries());
	loop.call([&] {
		p.dialer->abort();
		p.acceptor->abort();
	});
	assert(loop.until([&] { return ends.dialer && ends.acceptor; }, 3s));
	assert(ended && ms < 5000 && *ends.dialer == ConnectionEnd::failed);
}

// Loops of a host handshake at once with the one context its Links took: no handshake writes
// what another reads (run under TSan, which reports such a write in libcrypto).
void shared_context() {
	for (int round = 0; round != 32; ++round) {
		auto shared = context("one");
		auto one = pins_of(1, "one"), two = pins_of(2, "two");
		Loop a, b;
		Links taken(a.io.get_executor(), a.ref(), shared, one, id('K'), {});
		std::atomic<int> ready{0};
		auto handshake = [&](Loop &loop) {
			Pair p(loop, shared, context("two"));
			ready.fetch_add(1);
			while (ready.load() != 2)
				std::this_thread::yield();
			auto [d, x] = p.handshake(one, 2, two);
			assert(d.node == 2 && x.node == 1);
		};
		std::thread first([&] { handshake(a); }), second([&] { handshake(b); });
		first.join();
		second.join();
	}
	std::printf("shared_context: 32 rounds of two loops handshaking at once on one context\n");
}

struct Case {
	const char *name;
	void (*run)();
};
constexpr Case cases[] = {{"authenticate", authenticate}, {"wrong_node", wrong_node}, {"unpinned", unpinned},
						  {"no_certificate", no_certificate}, {"pins", pins},		 {"frames", frames},
						  {"keepalive", keepalive},	   {"dead_peer", dead_peer}, {"graceful_close", graceful_close},
						  {"abort_truncates", abort_truncates}, {"fd_reuse_reader", fd_reuse_reader},
						  {"fd_reuse_writer", fd_reuse_writer}, {"slow_reader", slow_reader},
						  {"busy_reader", busy_reader}, {"coarse_writer", coarse_writer}, {"nonreader", nonreader},
						  {"shared_context", shared_context}};
} // namespace

int main(int argc, char **argv) {
	assert(argc >= 2);
	fixture = argv[1];
	std::signal(SIGPIPE, SIG_IGN);  // the host's duty where SO_NOSIGPIPE is missing (connection.h)
	std::setvbuf(stdout, nullptr, _IOLBF, 0);  // a case's line reaches the log before an assert aborts
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
