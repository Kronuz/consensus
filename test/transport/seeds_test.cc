// Multicast seeds (docs/mechanisms/seeds.md) on this host, announcements kept on it (TTL 0):
//   codec        the datagram's round trip, limits and every fault
//   drops        malformed, oversized, wrong-version and other-cluster datagrams counted, never seeds;
//                several services per host, a listener seeds only those it asks for
//   processes    three processes find each other and report the same seed set (A1)
//   unavailable  no route to the group: reported once, nothing thrown, no seeds (A1)
//   forged       a forged seed whose TLS hello fails adds no member and backs off (A3)
//   rate         the burst, then one announcement per interval plus jitter; options applied (A4)
//   consensus_transport_seeds_test <fixture dir> <case>
// Announcements go out on loopback, or on the default interface where loopback carries no
// multicast; a host where neither reaches itself prints why and exits 77 (skipped), unless
// CONSENSUS_SEEDS_REQUIRE is set.
#include "consensus/transport/connection.h"
#include "consensus/transport/seeds.h"
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <netinet/in.h>
#include <openssl/pem.h>
#include <spawn.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;

using namespace kronuz::consensus::transport;
using namespace std::chrono_literals;
using kronuz::consensus::NodeId;
using Clock = std::chrono::steady_clock;

namespace {
std::filesystem::path fixture;
std::string interface_ = "127.0.0.1";  // where this run's announcements go: loopback, or "" the default

kronuz::consensus::Identity identity(char c) {
	kronuz::consensus::Identity r{};
	r.fill(c);
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

[[noreturn]] void skip(const std::string &why) {
	std::fprintf(stderr, "multicast unavailable on this host: %s\n", why.c_str());
	std::exit(std::getenv("CONSENSUS_SEEDS_REQUIRE") ? 1 : 77);
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
	stop(loop, host);
	stop(loop, voters);
	stop(loop, other);
	std::puts("drops: ok");
}

// One of three processes: announces "node" at `node_port`, prints the seed set it reached.
int child(std::uint16_t port, std::uint16_t node_port) {
	Loop loop;
	Seeds seeds(loop.io.get_executor(), identity('P'), {{"node", node_port}}, {"node"}, local(port));
	seeds.start();
	if (!seeds.unavailable().empty()) {
		std::printf("unavailable %s\n", seeds.unavailable().c_str());
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
		if (out.starts_with("unavailable")) {
			std::fprintf(stderr, "multicast unavailable on this host: %s", out.c_str() + 12);
			std::exit(std::getenv("CONSENSUS_SEEDS_REQUIRE") ? 1 : 77);
		}
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

void forged() {
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
	seeds.on_dial = [&](const Seed &seed) {
		const auto service = seed.port("node");
		++dials[service];
		at[service].push_back(Clock::now());
		task::spawn(loop.ref(), dial(loop, tls, pins, {seed.address, service}),
					[&, id = seed.id](std::exception_ptr e, std::optional<NodeId> node) {
						if (e || !node)
							return seeds.failed(id);
						members.insert(*node);
						seeds.joined(id);
					});
	};
	seeds.start();
	require_multicast(seeds);
	genuine.start();
	// A forger announces the impostor's port every 10 ms, five times the genuine node's rate.
	Forger forger(loop, port);
	const auto forged = encode_announcement({cluster, identity('F'), {{"node", impostor.port()}}});
	asio::steady_timer pace(loop.io);
	std::function<void()> again = [&] {
		forger.send(forged);
		pace.expires_after(10ms);
		pace.async_wait([&](asio::error_code e) {
			if (!e)
				again();
		});
	};
	again();
	loop.run_for(2000ms);
	pace.cancel();
	std::printf("forged: %d dials of the impostor, %d of the genuine node, %llu failures, members %zu\n",
				dials[impostor.port()], dials[two.port()], (unsigned long long)seeds.stats().failures, members.size());
	assert(members == std::set<NodeId>{2});  // the impostor's hello failed: never a member
	assert(dials[two.port()] == 1);          // joined: no further dials
	// Backoff 100, 200, 400, 800 ms: at most five dials in 2 s, each wait at least its backoff.
	const auto &times = at[impostor.port()];
	assert(dials[impostor.port()] >= 3 && dials[impostor.port()] <= 5);
	for (std::size_t i = 1; i < times.size(); ++i)
		assert(times[i] - times[i - 1] >= 100ms * (1 << (i - 1)));
	assert(seeds.stats().failures + 1 >= std::uint64_t(dials[impostor.port()]));  // the last may be in flight
	two.acceptor.close();
	impostor.acceptor.close();
	stop(loop, seeds);
	stop(loop, genuine);
	loop.run_for(100ms);
	std::puts("forged: ok");
}

void rate() {
	// Defaults: the Xapian-era group and port, three hops.
	SeedOptions d;
	assert(d.group == "239.192.168.1" && d.port == 58880 && d.ttl == 3 && d.interface.empty());
	for (auto bad : {[] { SeedOptions o; o.group = "10.0.0.1"; return o; }(), [] { SeedOptions o; o.port = 0; return o; }(),
					 [] { SeedOptions o; o.ttl = 256; return o; }(), [] { SeedOptions o; o.interval = 5ms; return o; }(),
					 [] { SeedOptions o; o.jitter = o.interval + 1ms; return o; }(), [] { SeedOptions o; o.burst = 17; return o; }(),
					 [] { SeedOptions o; o.expire = o.interval; return o; }(), [] { SeedOptions o; o.seeds = 0; return o; }(),
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
	const auto start = Clock::now();
	loop.run_for(1500ms);
	stop(loop, seeds);
	raw.close();
	loop.run_for(20ms);
	std::printf("rate: %zu datagrams in 1.5 s, %llu sent\n", heard.size(), (unsigned long long)seeds.stats().announced);
	// The burst: three within 50 ms of the start.
	assert(heard.size() >= 3 && heard[2] - start < 50ms);
	// Then each gap is one interval plus at most its jitter (and the scheduler's slack).
	for (std::size_t i = 3; i < heard.size(); ++i) {
		const auto gap = heard[i] - heard[i - 1];
		assert(gap >= 90ms && gap <= 150ms + 60ms);
	}
	// 1.5 s holds at most 3 + 1500 / 100 datagrams, at least 3 + 1500 / 150 - 1.
	assert(heard.size() <= 18 && heard.size() >= 12);
	std::puts("rate: ok");
}
} // namespace

int main(int argc, char **argv) {
	if (argc < 3) {
		std::fprintf(stderr, "usage: %s FIXTURE_DIR CASE\n", argv[0]);
		return 2;
	}
	fixture = argv[1];
	const std::string name = argv[2];
	if (name == "child") {
		interface_ = argc > 5 ? argv[5] : "";
		return child(std::uint16_t(std::stoi(argv[3])), std::uint16_t(std::stoi(argv[4])));
	}
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
		forged();
	else if (name == "rate")
		rate();
	else
		return 2;
	return 0;
}
