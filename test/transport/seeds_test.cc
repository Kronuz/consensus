// Multicast seeds (docs/mechanisms/seeds.md) on this host, announcements kept on it (TTL 0):
//   codec        the datagram's round trip, limits and every fault
//   drops        malformed, oversized, wrong-version and other-cluster datagrams counted, never seeds;
//                several services per host, a listener seeds only those it asks for
//   processes    three processes find each other and report the same seed set (A1)
//   unavailable  no route to the group: reported once, nothing thrown, no seeds (A1)
//   forged       a forged seed whose TLS hello fails adds no member and backs off (A3)
//   forged_rotating  the same with a new sender per datagram, after a 256-sender flood (A3)
//   rate         the burst, then one announcement per interval plus jitter; options applied (A4)
//   recovers     a start() that fails is retried with backoff and seeds once it succeeds
//   receive_errors  a failing receive is counted, reported once and backed off
//   throws       an on_dial that throws releases its seed as a failed dial
//   require      where CI or CONSENSUS_SEEDS_REQUIRE is set, unavailable multicast fails
//   consensus_transport_seeds_test <fixture dir> <case>
// Announcements go out on loopback, or on the default interface where loopback carries no
// multicast; a host where neither reaches itself prints why and exits 77 (skipped), unless
// CI or CONSENSUS_SEEDS_REQUIRE is set.
#include "consensus/transport/connection.h"
#include "consensus/transport/seeds.h"
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <cassert>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <netinet/in.h>
#include <openssl/pem.h>
#include <atomic>
#include <spawn.h>
#include <string>
#include <thread>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;

using namespace kronuz::consensus::transport;
using namespace std::chrono_literals;
using kronuz::consensus::NodeId;
using Clock = std::chrono::steady_clock;

namespace {
std::filesystem::path fixture;
// The longest a thread that only sleeps 1 ms went without running, in ms (rpc_hosts.h's Lag): a
// runner that pauses the process delays every timer and read, which no seeds rule can.
struct Lag {
	std::atomic<bool> stopped{false};
	std::atomic<std::uint64_t> worst{0};
	std::thread sampler{[this] {
		auto last = Clock::now();
		while (!stopped) {
			std::this_thread::sleep_for(1ms);
			const auto now = Clock::now();
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
std::string interface_ = "127.0.0.1";  // where this run's announcements go: loopback, or "" the default

kronuz::consensus::Identity identity(char c, std::uint32_t n = 0) {
	kronuz::consensus::Identity r{};
	r.fill(c);
	if (n)
		std::memcpy(r.data(), &n, sizeof n);
	return r;
}
// A port and cluster of this run's own, so concurrent runs never hear each other.
std::uint16_t run_port() { return std::uint16_t(20000 + std::random_device{}() % 20000); }
SeedOptions local(std::uint16_t port) {
	SeedOptions o;
	o.port = port;
	o.interface = interface_;
	o.ttl = 0;
	o.interval = 50ms;
	o.jitter = 20ms;
	o.burst_spacing = 10ms;
	o.expire = 2s;
	return o;
}

struct Loop {
	asio::io_context io;
	task::asio_scheduler scheduler{io.get_executor()};
	task::scheduler_ref ref() { return task::scheduler_ref::of(scheduler); }
	template <class P> bool until(P done, std::chrono::milliseconds limit = 10s) {
		const auto stop = Clock::now() + limit;
		while (!done()) {
			if (Clock::now() >= stop)
				return false;
			io.restart();
			io.run_for(5ms);
		}
		return true;
	}
	void run_for(std::chrono::milliseconds d) {
		until([] { return false; }, d);
	}
	~Loop() {
		io.stop();
		scheduler.close();
		scheduler.destroy_roots();
	}
};
void stop(Loop &loop, Seeds &seeds) {
	seeds.stop();
	assert(loop.until([&] { return seeds.drained(); }));
}

// CI must run these cases, never skip them: a platform bug in Seeds would read as a skip.
bool required() { return std::getenv("CI") || std::getenv("CONSENSUS_SEEDS_REQUIRE"); }
[[noreturn]] void skip(const std::string &why) {
	std::fprintf(stderr, "multicast unavailable on this host: %s%s\n", why.c_str(),
				 required() ? " (required: CI or CONSENSUS_SEEDS_REQUIRE is set)" : "");
	std::exit(required() ? 1 : 77);
}
void require_multicast(const Seeds &seeds) {
	if (!seeds.unavailable().empty())
		skip(seeds.unavailable());
}
// Picks loopback, else the default interface: the first where one Seeds hears another.
void choose_interface() {
	std::string why;
	for (const char *candidate : {"127.0.0.1", ""}) {
		interface_ = candidate;
		Loop loop;
		const auto port = run_port();
		Seeds a(loop.io.get_executor(), identity('Q'), {{"probe", 1}}, {}, local(port));
		Seeds b(loop.io.get_executor(), identity('Q'), {}, {"probe"}, local(port));
		a.start();
		b.start();
		const bool heard = loop.until([&] { return !b.seeds().empty(); }, 2s);
		why += std::string(why.empty() ? "" : "; ") + (*candidate ? candidate : "default") + ": " +
			   (!a.unavailable().empty() ? a.unavailable() : !b.unavailable().empty() ? b.unavailable() : "nothing heard");
		stop(loop, a);
		stop(loop, b);
		if (heard)
			return;
	}
	skip(why);
}

void codec() {
	Announcement a{identity('C'), identity('S'), {{"node", 8890}, {"relay.1", 1}, {"voter_x-9", 65535}}};
	const auto bytes = encode_announcement(a);
	assert(bytes.size() == seed_header + 3 * 3 + 4 + 7 + 9 && bytes.substr(0, 8) == "DTSD0001");
	Announcement b;
	assert(decode_announcement(bytes, b) == SeedFault::none && b == a);
	// The largest valid announcement fits the limit with room.
	Announcement big{identity('C'), identity('S'), {}};
	for (int i = 0; i < 8; ++i)
		big.services.push_back({std::string(31, 'a') + char('0' + i), std::uint16_t(i + 1)});
	assert(encode_announcement(big).size() == 321 && decode_announcement(encode_announcement(big), b) == SeedFault::none);

	auto fault = [&](std::string d) { return decode_announcement(d, b); };
	assert(fault(std::string(seed_datagram_limit + 1, 'x')) == SeedFault::oversized);
	assert(fault("") == SeedFault::malformed && fault("DTSD000") == SeedFault::malformed);
	assert(fault("XTSD0001" + bytes.substr(8)) == SeedFault::malformed);
	assert(fault("DTSD00x1" + bytes.substr(8)) == SeedFault::malformed);
	assert(fault("DTSD0002" + bytes.substr(8)) == SeedFault::version);
	assert(fault(encode_announcement(a, 7)) == SeedFault::version);
	assert(fault(bytes.substr(0, seed_header - 1)) == SeedFault::malformed);
	for (std::size_t n = seed_header; n < bytes.size(); ++n)
		assert(fault(bytes.substr(0, n)) == SeedFault::malformed);  // every truncation
	assert(fault(bytes + '\0') == SeedFault::malformed);           // a trailing byte
	auto with = [&](std::size_t at, char c) {
		auto d = bytes;
		d[at] = c;
		return fault(d);
	};
	assert(with(40, 0) == SeedFault::malformed && with(40, 9) == SeedFault::malformed);  // count 0, 9
	assert(with(41, 0) == SeedFault::malformed && with(41, 40) == SeedFault::malformed); // name 0, overrun
	assert(with(42, 'N') == SeedFault::malformed);  // a name character outside [a-z0-9._-]
	{
		auto d = bytes;
		d[46] = d[47] = 0;  // port 0
		assert(fault(d) == SeedFault::malformed);
		d = bytes;
		std::fill(d.begin() + 8, d.begin() + 24, '\0');  // zero cluster
		assert(fault(d) == SeedFault::malformed);
		d = encode_announcement({identity('C'), identity('S'), {{"node", 1}, {"nodf", 2}}});
		d[seed_header + 1 + 4 + 2 + 4] = 'e';  // the second name becomes "node": repeated
		assert(fault(d) == SeedFault::malformed);
	}
	const Announcement invalid[] = {
		{identity('C'), identity('S'), {}},
		{{}, identity('S'), {{"n", 1}}},
		{identity('C'), identity('S'), {{"n", 0}}},
		{identity('C'), identity('S'), {{"N", 1}}},
		{identity('C'), identity('S'), {{std::string(33, 'n'), 1}}},
		{identity('C'), identity('S'), {{"node", 1}, {"node", 2}}},
	};
	for (const auto &bad : invalid) {
		bool threw = false;
		try {
			encode_announcement(bad);
		} catch (const std::invalid_argument &) {
			threw = true;
		}
		assert(threw);
	}
	std::puts("codec: ok");
}

// A raw socket that sends datagrams to the group on loopback, as a forger would.
struct Forger {
	asio::ip::udp::socket socket;
	asio::ip::udp::endpoint group;
	Forger(Loop &loop, std::uint16_t port)
		: socket(loop.io, asio::ip::udp::v4()), group(asio::ip::make_address("239.192.168.1"), port) {
		if (!interface_.empty())
			socket.set_option(asio::ip::multicast::outbound_interface(asio::ip::make_address_v4(interface_)));
		socket.set_option(asio::ip::multicast::hops(0));
		socket.set_option(asio::ip::multicast::enable_loopback(true));
	}
	void send(const std::string &d) { socket.send_to(asio::buffer(d), group); }
};

void drops() {
	Loop loop;
	const auto port = run_port();
	const auto cluster = identity('K');
	Seeds host(loop.io.get_executor(), cluster, {{"node", 1000}, {"relay", 1001}, {"voter", 1002}}, {}, local(port));
	Seeds voters(loop.io.get_executor(), cluster, {}, {"voter", "relay"}, local(port));
	Seeds other(loop.io.get_executor(), cluster, {}, {"elsewhere"}, local(port));
	voters.start();
	require_multicast(voters);
	other.start();
	Forger forger(loop, port);
	const auto good = encode_announcement({cluster, identity('F'), {{"voter", 7}}});
	forger.send(std::string(seed_datagram_limit + 100, 'x'));          // oversized
	forger.send(good.substr(0, good.size() - 1));                      // malformed
	forger.send("DTSD0002" + good.substr(8));                          // another version
	forger.send(encode_announcement({identity('X'), identity('G'), {{"voter", 8}}}));  // another cluster
	assert(loop.until([&] { return voters.stats().received == 4; }));
	const auto &s = voters.stats();
	assert(s.oversized == 1 && s.malformed == 1 && s.version == 1 && s.cluster == 1);
	assert(voters.seeds().empty());
	// A host announcing three services: each listener seeds only the ones it asks for.
	host.start();
	assert(loop.until([&] { return voters.seeds().size() == 1 && other.stats().unwanted > 0; }));
	const auto seed = voters.seeds().front();
	assert(seed.sender == host.sender());
	assert(interface_.empty() ? !seed.address.is_unspecified() : seed.address == asio::ip::make_address(interface_));
	assert((seed.services == std::vector<SeedService>{{"relay", 1001}, {"voter", 1002}}));
	assert(seed.port("voter") == 1002 && seed.port("node") == 0);
	assert(other.seeds().empty() && host.stats().own == 0 && host.stats().received == 0);  // it seeds nothing
	assert(voters.stats().oversized == 1 && voters.stats().malformed == 1 && voters.stats().version == 1);
	// One source address holds at most per_address entries: a flood of ports from it is counted, never seeded.
	auto o = local(port);
	o.per_address = 4;
	Seeds crowd(loop.io.get_executor(), cluster, {}, {"voter"}, o);
	crowd.start();
	for (std::uint16_t p = 2000; p < 2064; ++p)
		forger.send(encode_announcement({cluster, identity('F'), {{"voter", p}}}));
	assert(loop.until([&] { return crowd.stats().address_full >= 60; }));
	std::map<asio::ip::address, int> held;
	asio::ip::address flooder;
	for (const auto &entry : crowd.seeds()) {
		++held[entry.address];
		if (entry.port("voter") >= 2000)
			flooder = entry.address;
	}
	std::printf("drops: the flooding address holds %d entries, %llu refused\n", held[flooder],
				(unsigned long long)crowd.stats().address_full);
	assert(held[flooder] == 4 && crowd.stats().full == 0);
	stop(loop, host);
	stop(loop, voters);
	stop(loop, other);
	stop(loop, crowd);
	std::puts("drops: ok");
}

// One of three processes: announces "node" at `node_port`, prints the seed set it reached.
int child(std::uint16_t port, std::uint16_t node_port) {
	Loop loop;
	Seeds seeds(loop.io.get_executor(), identity('P'), {{"node", node_port}}, {"node"}, local(port));
	seeds.start();
	if (!seeds.unavailable().empty()) {
		std::printf("unavailable %s\n", seeds.unavailable().c_str());
		stop(loop, seeds);  // its retry is pending
		return 3;
	}
	loop.until([&] { return seeds.seeds().size() >= 2; }, 10s);
	loop.run_for(500ms);  // the others hear this one's announcements too
	std::set<std::uint16_t> ports{node_port};
	for (const auto &s : seeds.seeds())
		ports.insert(s.port("node"));
	for (auto p : ports)
		std::printf("%u ", unsigned(p));
	std::printf("own=%llu\n", (unsigned long long)seeds.stats().own);
	stop(loop, seeds);
	return 0;
}
void processes(const char *self) {
	const auto port = std::to_string(run_port());
	struct Child {
		pid_t pid;
		int out;
	};
	std::vector<Child> children;
	for (const char *node : {"9101", "9102", "9103"}) {
		int pipe_fds[2];
		assert(::pipe(pipe_fds) == 0);
		posix_spawn_file_actions_t actions;
		posix_spawn_file_actions_init(&actions);
		posix_spawn_file_actions_adddup2(&actions, pipe_fds[1], 1);
		posix_spawn_file_actions_addclose(&actions, pipe_fds[0]);
		std::vector<std::string> args{self, fixture.string(), "child", port, node, interface_};
		std::vector<char *> argv;
		for (auto &a : args)
			argv.push_back(a.data());
		argv.push_back(nullptr);
		pid_t pid;
		assert(posix_spawn(&pid, self, &actions, nullptr, argv.data(), environ) == 0);
		posix_spawn_file_actions_destroy(&actions);
		::close(pipe_fds[1]);
		children.push_back({pid, pipe_fds[0]});
	}
	std::vector<std::string> sets;
	for (auto &c : children) {
		std::string out;
		char buffer[256];
		for (ssize_t n; (n = ::read(c.out, buffer, sizeof buffer)) > 0;)
			out.append(buffer, std::size_t(n));
		::close(c.out);
		int status = 0;
		assert(::waitpid(c.pid, &status, 0) == c.pid);
		std::printf("child: %s", out.c_str());
		if (out.starts_with("unavailable"))
			skip(out.substr(12, out.find('\n') - 12));
		assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
		sets.push_back(out.substr(0, out.find("own=")));
		assert(out.find("own=0") == std::string::npos);  // each heard its own announcements and dropped them
	}
	for (auto &s : sets)
		assert(s == "9101 9102 9103 ");
	std::puts("processes: ok");
}

void unavailable() {
	Loop loop;
	auto o = local(run_port());
	o.interface = "192.0.2.1";  // TEST-NET-1: no interface of this host has it
	o.backoff = 50ms;
	o.backoff_max = 10s;
	Seeds seeds(loop.io.get_executor(), identity('U'), {{"node", 1}}, {"node"}, o);
	int reports = 0;
	std::string reason;
	seeds.on_unavailable = [&](const std::string &r) {
		++reports;
		reason = r;
	};
	seeds.start();  // never throws: the node goes on with its explicit seeds
	seeds.start();
	loop.run_for(300ms);
	std::printf("unavailable: %s\n", reason.c_str());
	assert(reports == 1 && !reason.empty() && seeds.unavailable() == reason);
	// Retried at 50 and 150 ms after the first: a few in 300 ms, never a spin.
	assert(seeds.stats().setup_failures >= 2 && seeds.stats().setup_failures <= 4);
	assert(seeds.seeds().empty() && seeds.stats().announced == 0 && seeds.stats().received == 0);
	stop(loop, seeds);
	std::puts("unavailable: ok");
}

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
// A TLS listener presenting `name`'s certificate; it authenticates whoever dials.
struct Server {
	asio::ip::tcp::acceptor acceptor;
	std::shared_ptr<asio::ssl::context> tls;
	NodePins pins;
	std::vector<std::shared_ptr<Connection>> accepted;
	Server(Loop &loop, NodeId node, const std::string &name)
		: acceptor(loop.io, asio::ip::tcp::endpoint(asio::ip::address_v4::loopback(), 0)), tls(context(name)),
		  pins(node, {{1, pin_of("one")}, {node, pin_of(name)}}, tls->native_handle()) {}
	std::uint16_t port() const { return acceptor.local_endpoint().port(); }
	task::task<void> serve(Loop &loop) {
		for (;;) {
			auto [error, socket] = co_await task::await_asio<void(asio::error_code, asio::ip::tcp::socket)>(
				[&](auto h) { acceptor.async_accept(std::move(h)); });
			if (error)
				co_return;
			auto c = std::make_shared<Connection>(loop.ref(), std::move(*socket), tls, false);
			accepted.push_back(c);
			task::spawn(loop.ref(), handshake(loop, c), [](std::exception_ptr, std::optional<bool>) {});
		}
	}
	task::task<bool> handshake(Loop &loop, std::shared_ptr<Connection> c) {
		late_deadline deadline(loop.ref(), 2s);
		co_await c->handshake(pins, 0, deadline.token());
		co_return true;
	}
};
// The dial a node makes to a seed: TCP, then TLS authenticated against its pins (any pinned node).
task::task<NodeId> dial(Loop &loop, std::shared_ptr<asio::ssl::context> tls, const NodePins &pins,
						asio::ip::tcp::endpoint to) {
	asio::ip::tcp::socket socket(loop.io);
	const auto error = co_await task::await_asio<void(asio::error_code)>([&](auto h) { socket.async_connect(to, std::move(h)); });
	if (error)
		throw std::system_error(error);
	auto c = std::make_shared<Connection>(loop.ref(), std::move(socket), tls, true);
	late_deadline deadline(loop.ref(), 2s);
	co_return co_await c->handshake(pins, 0, deadline.token());
}

// With `rotate`, the forger first floods 256 senders from its one address, then sends a new
// sender per datagram: the seed is the dial target, so neither skips the backoff or fills the set.
void forged(bool rotate) {
#ifndef SO_NOSIGPIPE
	// A failed hello's write() to a closed peer raises SIGPIPE here: the host ignores it (main).
	std::raise(SIGPIPE);
#endif
	Loop loop;
	const auto port = run_port();
	const auto cluster = identity('K');
	// The node dialing seeds pins nodes one and two; three's certificate is the CA's but unpinned.
	auto tls = context("one");
	NodePins pins(1, {{1, pin_of("one")}, {2, pin_of("two")}}, tls->native_handle());
	Server two(loop, 2, "two"), impostor(loop, 3, "three");
	task::spawn(loop.ref(), two.serve(loop), [](std::exception_ptr) {});
	task::spawn(loop.ref(), impostor.serve(loop), [](std::exception_ptr) {});

	auto o = local(port);
	o.backoff = 100ms;
	o.backoff_max = 10s;
	Seeds seeds(loop.io.get_executor(), cluster, {}, {"node"}, o);
	Seeds genuine(loop.io.get_executor(), cluster, {{"node", two.port()}}, {}, local(port));
	std::set<NodeId> members;
	std::map<std::uint16_t, int> dials;
	std::map<std::uint16_t, std::vector<Clock::time_point>> at;
	std::uint64_t genuine_id = 0;
	int joins = 0;
	seeds.on_dial = [&](const Seed &seed) {
		const auto service = seed.port("node");
		if (service == two.port())
			genuine_id = seed.id;
		++dials[service];
		at[service].push_back(Clock::now());
		task::spawn(loop.ref(), dial(loop, tls, pins, {seed.address, service}),
					[&, id = seed.id](std::exception_ptr e, std::optional<NodeId> node) {
						if (e || !node)
							return seeds.failed(id);
						members.insert(*node);
						++joins;
						seeds.joined(id);
					});
	};
	seeds.start();
	require_multicast(seeds);
	Forger forger(loop, port);
	std::uint32_t sender = 0;
	auto forged = [&] { return encode_announcement({cluster, identity('F', rotate ? ++sender : 0), {{"node", impostor.port()}}}); };
	if (rotate) {
		for (std::uint64_t b = 1; b <= 8; ++b) {  // in batches, so no socket buffer overflows
			for (int i = 0; i < 32; ++i)
				forger.send(forged());
			assert(loop.until([&] { return seeds.stats().received >= 32 * b; }));
		}
		assert(seeds.seeds().size() == 1 && seeds.stats().full == 0 && seeds.stats().address_full == 0);  // one target
	}
	genuine.start();
	// A forger announces the impostor's port every 10 ms, five times the genuine node's rate.
	asio::steady_timer pace(loop.io);
	std::function<void()> again = [&] {
		forger.send(forged());
		pace.expires_after(10ms);
		pace.async_wait([&](asio::error_code e) {
			if (!e)
				again();
		});
	};
	again();
	loop.run_for(2000ms);
	pace.cancel();
	std::printf("forged%s: %d dials of the impostor, %d of the genuine node, %llu failures, members %zu\n",
				rotate ? " (rotating)" : "", dials[impostor.port()], dials[two.port()], (unsigned long long)seeds.stats().failures, members.size());
	assert(members == std::set<NodeId>{2});  // the impostor's hello failed: never a member
	assert(dials[two.port()] == 1);          // joined: no further dials
	// Backoff 100, 200, 400, 800 ms: at most five dials in 2 s, each wait at least its backoff.
	const auto &times = at[impostor.port()];
	assert(dials[impostor.port()] >= 2 && dials[impostor.port()] <= 5);
	for (std::size_t i = 1; i < times.size(); ++i)
		assert(times[i] - times[i - 1] >= 100ms * (1 << (i - 1)));
	assert(seeds.stats().failures + 1 >= std::uint64_t(dials[impostor.port()]));  // the last may be in flight
	// A joined node reported lost is dialed again once its backoff passes, and joins again.
	seeds.failed(genuine_id);
	assert(loop.until([&] { return joins == 2; }, 3s));
	assert(dials[two.port()] == 2 && members == std::set<NodeId>{2});
	two.acceptor.close();
	impostor.acceptor.close();
	stop(loop, seeds);
	stop(loop, genuine);
	loop.run_for(100ms);
	std::puts(rotate ? "forged_rotating: ok" : "forged: ok");
}

void rate() {
	// Defaults: the Xapian-era group and port, three hops.
	SeedOptions d;
	assert(d.group == "239.192.168.1" && d.port == 58880 && d.ttl == 3 && d.interface.empty());
	for (auto bad : {[] { SeedOptions o; o.group = "10.0.0.1"; return o; }(), [] { SeedOptions o; o.port = 0; return o; }(),
					 [] { SeedOptions o; o.ttl = 256; return o; }(), [] { SeedOptions o; o.interval = 5ms; return o; }(),
					 [] { SeedOptions o; o.jitter = o.interval + 1ms; return o; }(), [] { SeedOptions o; o.burst = 17; return o; }(), [] { SeedOptions o; o.burst = 0; return o; }(),
					 [] { SeedOptions o; o.expire = o.interval; return o; }(), [] { SeedOptions o; o.seeds = 0; return o; }(),
					 [] { SeedOptions o; o.per_address = 0; return o; }(), [] { SeedOptions o; o.per_address = o.seeds + 1; return o; }(),
					 [] { SeedOptions o; o.backoff_max = o.backoff - 1ms; return o; }()}) {
		bool threw = false;
		try {
			validate_seed_options(bad);
		} catch (const std::invalid_argument &) {
			threw = true;
		}
		assert(threw);
	}
	Loop loop;
	const auto port = run_port();
	auto o = local(port);
	o.interval = 100ms;
	o.jitter = 50ms;
	o.burst = 3;
	o.burst_spacing = 10ms;
	// A raw listener timestamps each datagram as it arrives.
	asio::ip::udp::socket raw(loop.io, asio::ip::udp::v4());
	raw.set_option(asio::socket_base::reuse_address(true));
#ifdef SO_REUSEPORT
	const int one = 1;
	::setsockopt(raw.native_handle(), SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);
#endif
	raw.bind({asio::ip::make_address("239.192.168.1"), port});
	raw.set_option(asio::ip::multicast::join_group(
		asio::ip::make_address_v4("239.192.168.1"),
		interface_.empty() ? asio::ip::address_v4::any() : asio::ip::make_address_v4(interface_)));
	std::vector<Clock::time_point> heard;
	std::array<char, 1024> buffer;
	asio::ip::udp::endpoint from;
	std::function<void()> receive = [&] {
		raw.async_receive_from(asio::buffer(buffer), from, [&](asio::error_code e, std::size_t) {
			if (e)
				return;
			heard.push_back(Clock::now());
			receive();
		});
	};
	receive();
	Seeds seeds(loop.io.get_executor(), identity('R'), {{"node", 1}}, {}, o);
	seeds.start();
	require_multicast(seeds);
	// The options reached the sending socket: the interface and the TTL.
	int ttl = -1;
	socklen_t size = sizeof ttl;
	unsigned char ttl8 = 255;
	socklen_t size8 = sizeof ttl8;
	if (::getsockopt(seeds.sending_handle(), IPPROTO_IP, IP_MULTICAST_TTL, &ttl, &size) == 0 && size == sizeof ttl)
		assert(ttl == 0);
	else
		assert(::getsockopt(seeds.sending_handle(), IPPROTO_IP, IP_MULTICAST_TTL, &ttl8, &size8) == 0 && ttl8 == 0);
	in_addr interface{};
	socklen_t isize = sizeof interface;
	assert(::getsockopt(seeds.sending_handle(), IPPROTO_IP, IP_MULTICAST_IF, &interface, &isize) == 0);
	assert(interface_.empty() || interface.s_addr == htonl(INADDR_LOOPBACK));
	Lag lag;
	const auto start = Clock::now();
	loop.run_for(1500ms);
	const auto lagged = std::chrono::milliseconds(lag.stop());
	stop(loop, seeds);
	raw.close();
	loop.run_for(20ms);
	std::printf("rate: %zu datagrams in 1.5 s, %llu sent, the host's lag %lld ms\n", heard.size(),
				(unsigned long long)seeds.stats().announced, (long long)lagged.count());
	// The burst: three within 80 ms of the start (20 ms nominal; without it the third is past 200 ms).
	assert(heard.size() >= 3 && heard[2] - start < 80ms + lagged);
	// Then each gap is one interval plus at most its jitter, and the host's lag when it stalled one.
	for (std::size_t i = 3; i < heard.size(); ++i) {
		const auto gap = heard[i] - heard[i - 1];
		assert(gap >= 90ms && gap <= 150ms + lagged);
	}
	// 1.5 s holds at most 3 + 1500 / 100 datagrams; at least 3 + 1500 / 150 - 3, less one per 150 ms of lag.
	assert(heard.size() <= 18 && heard.size() + std::size_t(lagged / 150ms) >= 10);
	std::puts("rate: ok");
}
// A node started before its network is up seeds once it is: a failed start() is retried with
// backoff. No descriptor is left while it starts, so every socket it opens fails.
void recovers() {
	Loop loop;
	const auto port = run_port();
	Seeds node(loop.io.get_executor(), identity('V'), {{"node", 7000}}, {}, local(port));
	node.start();
	require_multicast(node);
	auto o = local(port);
	o.backoff = 50ms;
	o.backoff_max = 200ms;
	Seeds late(loop.io.get_executor(), identity('V'), {}, {"node"}, o);
	int reports = 0;
	late.on_unavailable = [&](const std::string &) { ++reports; };
	rlimit old{};
	assert(::getrlimit(RLIMIT_NOFILE, &old) == 0);
	rlimit low = old;
	low.rlim_cur = std::min<rlim_t>(old.rlim_cur, 256);
	assert(::setrlimit(RLIMIT_NOFILE, &low) == 0);
	std::vector<int> filler;
	for (int fd; filler.size() < 4096 && (fd = ::open("/dev/null", O_RDONLY)) >= 0;)
		filler.push_back(fd);
	late.start();
	loop.run_for(300ms);
	const auto failures = late.stats().setup_failures;
	const auto reason = late.unavailable();
	for (const int fd : filler)
		::close(fd);
	assert(::setrlimit(RLIMIT_NOFILE, &old) == 0);
	std::printf("recovers: %llu failed starts in 300 ms (%s), %d report\n", (unsigned long long)failures,
				reason.c_str(), reports);
	// Retries at 50 and 150 ms after the first: a few, never a spin, and one report.
	assert(failures >= 2 && failures <= 4 && reports == 1 && reason.starts_with("open: "));
	assert(loop.until([&] { return late.seeds().size() == 1; }, 3s));
	assert(late.seeds().front().port("node") == 7000 && late.unavailable().empty() && reports == 1);
	stop(loop, node);
	stop(loop, late);
	std::puts("recovers: ok");
}

// A receive that keeps failing is counted, reported once and waits backoff, doubling, before
// the next: the receiving descriptor becomes a pipe, so every receive fails with "not a socket".
void receive_errors() {
	Loop loop;
	const auto port = run_port();
	Seeds node(loop.io.get_executor(), identity('E'), {{"node", 7200}}, {}, local(port));
	auto o = local(port);
	o.backoff = 50ms;
	o.backoff_max = 10s;
	Seeds seeds(loop.io.get_executor(), identity('E'), {}, {"node"}, o);
	std::vector<std::string> reasons;
	seeds.on_unavailable = [&](const std::string &r) { reasons.push_back(r); };
	int pipe_fds[2];
	assert(::pipe(pipe_fds) == 0);
	Clock::time_point broken;
	seeds.on_dial = [&](const Seed &s) {
		seeds.joined(s.id);
		assert(::dup2(pipe_fds[0], seeds.receiving_handle()) >= 0);  // the receive this handler re-arms fails
		broken = Clock::now();
	};
	seeds.start();
	require_multicast(seeds);
	node.start();
	assert(loop.until([&] { return seeds.stats().receive_failures > 0; }));
	loop.run_for(1000ms);
	const auto failures = seeds.stats().receive_failures;
	std::printf("receive_errors: %llu failed receives in %lld ms, reported: %s\n", (unsigned long long)failures,
				(long long)std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - broken).count(),
				reasons.empty() ? "" : reasons.front().c_str());
	// Waits of 50, 100, 200, 400 ms: at most 6 in the first 1.5 s, at least 3 in its first second.
	assert(failures >= 3 && failures <= 6);
	assert(reasons.size() == 1 && reasons.front().starts_with("receive: ") && seeds.unavailable() == reasons.front());
	stop(loop, node);
	stop(loop, seeds);
	::close(pipe_fds[0]);
	::close(pipe_fds[1]);
	std::puts("receive_errors: ok");
}

// An on_dial that throws counts as a failed dial: the seed is offered again after its backoff,
// and the exception leaves the loop's run() while the source keeps listening.
void throws() {
	Loop loop;
	const auto port = run_port();
	Seeds node(loop.io.get_executor(), identity('T'), {{"node", 7100}}, {}, local(port));
	auto o = local(port);
	o.backoff = 100ms;
	Seeds seeds(loop.io.get_executor(), identity('T'), {}, {"node"}, o);
	std::vector<Clock::time_point> dials;
	seeds.on_dial = [&](const Seed &s) {
		dials.push_back(Clock::now());
		if (dials.size() == 1)
			throw std::runtime_error("a dialer's bug");
		seeds.joined(s.id);
	};
	seeds.start();
	require_multicast(seeds);
	node.start();
	bool threw = false;
	try {
		loop.until([&] { return !dials.empty(); });
	} catch (const std::runtime_error &) {
		threw = true;
	}
	assert(threw && dials.size() == 1 && seeds.stats().failures == 1);
	assert(loop.until([&] { return dials.size() == 2; }, 3s));
	assert(dials[1] - dials[0] >= 100ms);
	loop.run_for(300ms);
	std::printf("throws: %zu dials, %llu failures\n", dials.size(), (unsigned long long)seeds.stats().failures);
	assert(dials.size() == 2 && seeds.stats().failures == 1);  // joined the second time: no more
	stop(loop, node);
	stop(loop, seeds);
	std::puts("throws: ok");
}

// The exit status of this test's `skip` case under `env`, with CI and CONSENSUS_SEEDS_REQUIRE
// otherwise unset.
int skip_status(const char *self, std::vector<std::string> env) {
	for (char **e = environ; *e; ++e) {
		const std::string_view v(*e);
		if (!v.starts_with("CI=") && !v.starts_with("CONSENSUS_SEEDS_REQUIRE="))
			env.emplace_back(v);
	}
	std::vector<char *> envp;
	for (auto &e : env)
		envp.push_back(e.data());
	envp.push_back(nullptr);
	std::vector<std::string> args{self, fixture.string(), "skip"};
	std::vector<char *> argv;
	for (auto &a : args)
		argv.push_back(a.data());
	argv.push_back(nullptr);
	posix_spawn_file_actions_t actions;
	posix_spawn_file_actions_init(&actions);
	posix_spawn_file_actions_addopen(&actions, 2, "/dev/null", O_WRONLY, 0);
	pid_t pid;
	assert(posix_spawn(&pid, self, &actions, nullptr, argv.data(), envp.data()) == 0);
	posix_spawn_file_actions_destroy(&actions);
	int status = 0;
	assert(::waitpid(pid, &status, 0) == pid && WIFEXITED(status));
	return WEXITSTATUS(status);
}
void require(const char *self) {
	const int sandbox = skip_status(self, {}), ci = skip_status(self, {"CI=true"}),
			  forced = skip_status(self, {"CONSENSUS_SEEDS_REQUIRE=1"});
	std::printf("require: skip exits %d, %d under CI, %d with CONSENSUS_SEEDS_REQUIRE\n", sandbox, ci, forced);
	assert(sandbox == 77 && ci == 1 && forced == 1);
	std::puts("require: ok");
}
} // namespace

int main(int argc, char **argv) {
	if (argc < 3) {
		std::fprintf(stderr, "usage: %s FIXTURE_DIR CASE\n", argv[0]);
		return 2;
	}
#ifndef SO_NOSIGPIPE
	std::signal(SIGPIPE, SIG_IGN);  // the host's duty where SO_NOSIGPIPE is missing (connection.h)
#endif
	fixture = argv[1];
	const std::string name = argv[2];
	if (name == "child") {
		interface_ = argc > 5 ? argv[5] : "";
		return child(std::uint16_t(std::stoi(argv[3])), std::uint16_t(std::stoi(argv[4])));
	}
	if (name == "skip")
		skip("this case always skips");
	if (name == "require")
		return require(argv[0]), 0;
	if (name != "codec" && name != "unavailable")
		choose_interface();
	if (name == "codec")
		codec();
	else if (name == "drops")
		drops();
	else if (name == "processes")
		processes(argv[0]);
	else if (name == "unavailable")
		unavailable();
	else if (name == "forged")
		forged(false);
	else if (name == "forged_rotating")
		forged(true);
	else if (name == "rate")
		rate();
	else if (name == "recovers")
		recovers();
	else if (name == "receive_errors")
		receive_errors();
	else if (name == "throws")
		throws();
	else
		return 2;
	return 0;
}
