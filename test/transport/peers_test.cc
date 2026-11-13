// Peers over real TLS sockets between three consensus Workers, with voters named by host:
// lookups through an injected resolver (answering, failing, throwing or hanging on cue) and
// through the system resolver, the resolve deadline and the cache, staggered dials over
// several addresses, the certificate pin refusing a name that reaches the wrong voter and the
// next dial starting past it, a slow resolver behind the cache, and the spacing of lookups
// (docs/mechanisms/peer-dns.md). The fetch lane: exact bytes, not-found and errors between
// voters, fetches alongside consensus through small budgets, and a peer freezing or closing
// mid-fetch (docs/mechanisms/peer-fetch.md). Peers is also instantiated over a minimal host.
#include "consensus/transport/peers.h"
#include "consensus/transport/profile.h"

#include <array>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <journal/journal.h>
#include <journal/memory_io.h>
#include <openssl/pem.h>
#include <openssl/x509v3.h>
#include <unistd.h>

using namespace kronuz::consensus;
using namespace kronuz::consensus::transport;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Tcp = asio::ip::tcp;

namespace {
struct MinimalHost {
	Worker &worker() { std::abort(); }
	SnapshotChannel &channel() { std::abort(); }
};

#define CHECK(condition)                                                                           \
	do {                                                                                           \
		if (!(condition)) {                                                                        \
			std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #condition);    \
			std::abort();                                                                          \
		}                                                                                          \
	} while (0)

long long ms(Clock::duration duration) {
	return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

// A CA and three EC voter leaves, written as PEM files for reactor::tls.
struct Certificates {
	std::filesystem::path directory;
	std::map<NodeId, Fingerprint> pins;
	static void extend(X509 *certificate, X509 *issuer, int nid, const char *value) {
		X509V3_CTX context;
		X509V3_set_ctx(&context, issuer, certificate, nullptr, nullptr, 0);
		std::unique_ptr<X509_EXTENSION, decltype(&X509_EXTENSION_free)> extension(
			X509V3_EXT_conf_nid(nullptr, &context, nid, value), X509_EXTENSION_free);
		CHECK(extension && X509_add_ext(certificate, extension.get(), -1) == 1);
	}
	static std::unique_ptr<X509, decltype(&X509_free)> make(const char *name, EVP_PKEY *key, X509 *issuer,
															EVP_PKEY *issuer_key, long serial) {
		std::unique_ptr<X509, decltype(&X509_free)> certificate(X509_new(), X509_free);
		CHECK(X509_set_version(certificate.get(), 2) == 1);
		ASN1_INTEGER_set(X509_get_serialNumber(certificate.get()), serial);
		X509_gmtime_adj(X509_getm_notBefore(certificate.get()), -60);
		X509_gmtime_adj(X509_getm_notAfter(certificate.get()), 86400);
		std::unique_ptr<X509_NAME, decltype(&X509_NAME_free)> subject(X509_NAME_new(), X509_NAME_free);
		CHECK(X509_NAME_add_entry_by_txt(subject.get(), "CN", MBSTRING_ASC,
										 reinterpret_cast<const unsigned char *>(name), -1, -1, 0) == 1);
		CHECK(X509_set_subject_name(certificate.get(), subject.get()) == 1);
		CHECK(X509_set_issuer_name(certificate.get(), issuer ? X509_get_subject_name(issuer) : subject.get()) == 1);
		CHECK(X509_set_pubkey(certificate.get(), key) == 1);
		if (issuer) {
			extend(certificate.get(), issuer, NID_basic_constraints, "critical,CA:FALSE");
			extend(certificate.get(), issuer, NID_ext_key_usage, "serverAuth,clientAuth");
		} else {
			extend(certificate.get(), certificate.get(), NID_basic_constraints, "critical,CA:TRUE");
			extend(certificate.get(), certificate.get(), NID_key_usage, "critical,keyCertSign,cRLSign");
			extend(certificate.get(), certificate.get(), NID_subject_key_identifier, "hash");
		}
		CHECK(X509_sign(certificate.get(), issuer_key, EVP_sha256()) > 0);
		return certificate;
	}
	static void write(const std::filesystem::path &path, X509 *certificate, EVP_PKEY *key) {
		std::unique_ptr<BIO, decltype(&BIO_free)> out(BIO_new_file(path.c_str(), "w"), BIO_free);
		CHECK(out);
		if (certificate)
			CHECK(PEM_write_bio_X509(out.get(), certificate) == 1);
		else
			CHECK(PEM_write_bio_PrivateKey(out.get(), key, nullptr, nullptr, 0, nullptr, nullptr) == 1);
	}
	explicit Certificates(std::filesystem::path path) : directory(std::move(path)) {
		std::filesystem::create_directories(directory);
		std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> authority_key(EVP_EC_gen("P-256"), EVP_PKEY_free);
		CHECK(authority_key);
		auto authority = make("Consensus-DNS-Test-CA", authority_key.get(), nullptr, authority_key.get(), 1);
		write(directory / "ca.pem", authority.get(), nullptr);
		for (NodeId node = 1; node <= 3; ++node) {
			std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(EVP_EC_gen("P-256"), EVP_PKEY_free);
			CHECK(key);
			const auto name = "voter-" + std::to_string(node);
			auto leaf = make(name.c_str(), key.get(), authority.get(), authority_key.get(), static_cast<long>(node + 1));
			write(directory / (name + ".key"), nullptr, key.get());
			write(directory / (name + ".pem"), leaf.get(), nullptr);
			pins.emplace(node, certificate_fingerprint(leaf.get()));
		}
	}
	reactor::tls::Options tls(unsigned index) const {
		const auto name = "voter-" + std::to_string(index + 1);
		return {(directory / (name + ".pem")).string(), (directory / (name + ".key")).string(),
				(directory / "ca.pem").string()};
	}
};

// A resolver scripted per name. Hung calls park until release(), slow ones answer after
// `slow`; `active` counts the calls still inside, so a scenario can wait for every lookup
// thread to return before it ends.
struct FakeDns {
	enum class Mode { Answer, Empty, Throw, Hang, Slow };
	Clock::duration slow{};
	std::mutex mutex;
	std::condition_variable wake;
	bool released = false;
	std::map<std::string, std::vector<asio::ip::address>> answers;
	std::map<std::string, Mode> modes;
	std::map<std::string, unsigned> calls;
	unsigned active = 0;
	std::vector<asio::ip::address> resolve(const std::string &host) {
		std::unique_lock lock(mutex);
		++calls[host];
		++active;
		struct Leave {
			unsigned &active;
			~Leave() { --active; }
		} leave{active};
		switch (modes[host]) {
		case Mode::Answer:
			return answers[host];
		case Mode::Empty:
			return {};
		case Mode::Throw:
			throw std::runtime_error("SERVFAIL");
		case Mode::Hang:
			wake.wait(lock, [&] { return released; });
			return answers[host];
		case Mode::Slow:
			wake.wait_for(lock, slow, [&] { return released; });
			return answers[host];
		}
		return {};
	}
	void set(const std::string &host, Mode mode, std::vector<asio::ip::address> found = {}) {
		std::lock_guard lock(mutex);
		modes[host] = mode;
		if (!found.empty())
			answers[host] = std::move(found);
	}
	unsigned count(const std::string &host) {
		std::lock_guard lock(mutex);
		return calls[host];
	}
	void quiesce() {
		{
			std::lock_guard lock(mutex);
			released = true;
			wake.notify_all();
		}
		for (;;) {
			{
				std::lock_guard lock(mutex);
				if (!active)
					return;
			}
			std::this_thread::sleep_for(1ms);
		}
	}
	static PeerResolver resolver(std::shared_ptr<FakeDns> self) {
		return [self](const std::string &host) { return self->resolve(host); };
	}
};

constexpr Timing compact{20, 40, 100, 200, 60000};
// For a test that loads the one test thread: an append round trip past rpc_timeout is never
// matched (consensus-late-append), and a slow runner under load exceeds compact's 40 ms.
constexpr Timing loaded{20, 200, 400, 800, 60000};
Limits limits() {
	Limits result;
	result.command_bytes = 1024;
	result.rpc_bytes = 2048;
	result.log_bytes = 4096;
	result.log_entries = 32;
	result.control_entries = 4;
	result.rpc_entries = 4;
	return result;
}
FixedConfiguration fixed(NodeId local) {
	FixedConfiguration result;
	result.cluster[0] = 'C';
	result.configuration[0] = 'V';
	result.local = local;
	result.voters = {1, 2, 3};
	return result;
}

// The transport's host: a Worker over in-memory storage and its snapshot channel.
struct Node {
	kronuz::journal::testing::Disk disk;
	kronuz::journal::testing::MemoryIO io{disk};
	Worker worker_;
	SnapshotChannel channel_;
	std::vector<std::string> commands;
	explicit Node(NodeId local, Timing timing = compact)
		: 
		  worker_(io, fixed(local), {{512 * 1024, 4096}, {16 * 1024, 3}, {128 * 1024, 4}, 8, 3}, limits(), timing,
				  {4, 1, false, false}),
		  channel_(worker_) {}
	Worker &worker() { return worker_; }
	SnapshotChannel &channel() { return channel_; }
};

const asio::ip::address loopback = asio::ip::make_address("127.0.0.1");
const std::array<std::string, 3> names{"one", "two", "three"};

bool bindable(asio::io_context &io, Tcp::endpoint endpoint) {
	asio::error_code error;
	Tcp::acceptor probe(io);
	probe.open(endpoint.protocol(), error);
	if (!error)
		probe.bind(endpoint, error);
	return !error;
}
// The second loopback address: 127.0.0.2 where the system routes all of 127/8, else ::1.
asio::ip::address alternate_loopback(asio::io_context &io) {
	for (const char *candidate : {"127.0.0.2", "::1"})
		if (bindable(io, {asio::ip::make_address(candidate), 0}))
			return asio::ip::make_address(candidate);
	std::fprintf(stderr, "no second loopback address\n");
	std::abort();
}
std::uint16_t free_port(asio::io_context &io, asio::ip::address address) {
	Tcp::acceptor acceptor(io, Tcp::endpoint(address, 0));
	return acceptor.local_endpoint().port();
}
// A port free on both addresses, so a voter can move between them and keep its port.
std::uint16_t shared_port(asio::io_context &io, asio::ip::address a, asio::ip::address b) {
	for (unsigned attempt = 0; attempt != 64; ++attempt) {
		const auto port = free_port(io, a);
		if (bindable(io, {b, port}))
			return port;
	}
	std::abort();
}

// Three Workers, each with its Peers. A scenario sets listen endpoints, peer addresses and
// resolvers, and may stop one voter's Peers and start another for the same Worker. A voter
// `aside` runs on its own context, which `frozen` stops polling, as if its process hung.
struct Cluster {
	asio::io_context &io;
	asio::io_context side;
	std::optional<unsigned> aside;
	bool frozen = false;
	const Certificates &certificates;
	std::array<std::shared_ptr<Node>, 3> nodes;
	std::array<std::shared_ptr<Peers<Node>>, 3> peers;
	std::array<PeerOptions, 3> options;
	std::array<bool, 3> running{};
	std::vector<std::shared_ptr<Peers<Node>>> retired;
	Clock::time_point origin = Clock::now();
	RequestId request = 0;
	Timing timing;

	Cluster(asio::io_context &context, const Certificates &certs, Timing timing = compact)
		: io(context), certificates(certs), timing(timing) {
		for (unsigned i = 0; i != 3; ++i) {
			nodes[i] = std::make_shared<Node>(i + 1, timing);
			Identity identity{};
			identity[0] = static_cast<char>('a' + i);
			nodes[i]->worker().create(identity);
			for (unsigned turn = 0; turn != 100 && !nodes[i]->worker().ready(); ++turn)
				nodes[i]->worker().run_one({0, 100});
			CHECK(nodes[i]->worker().ready());
			CHECK(nodes[i]->worker().try_submit(Start{}) == SubmitResult::Accepted);
			options[i].tls = certificates.tls(i);
			options[i].pins = certificates.pins;
			options[i].deadline = 2000ms;
			options[i].resolve_deadline = 200ms;
			options[i].stagger = 100ms;
		}
	}
	void name_peers(const std::array<std::uint16_t, 3> &ports, const std::array<std::string, 3> &hosts = names) {
		for (unsigned i = 0; i != 3; ++i) {
			options[i].addresses.clear();
			for (unsigned j = 0; j != 3; ++j)
				if (i != j)
					options[i].addresses[j + 1] = {hosts[j], ports[j]};
		}
	}
	void resolve_with(std::shared_ptr<FakeDns> dns, unsigned i) { options[i].resolver = FakeDns::resolver(std::move(dns)); }
	void start(unsigned i) {
		PeerConfiguration configuration{fixed(i + 1), limits(), {}, {}, {}, {}};
		configuration.cluster[0] = 'c';
		configuration.group[0] = 'g';
		configuration.epoch[0] = 'e';
		configuration.profile =
			profile_fingerprint("peers-dns", configuration.configuration, configuration.consensus);
		peers[i] = std::make_shared<Peers<Node>>(executor(i), nodes[i], configuration, options[i]);
		peers[i]->start();
		running[i] = true;
	}
	asio::io_context &context(unsigned i) { return aside == i ? side : io; }
	asio::any_io_executor executor(unsigned i) { return context(i).get_executor(); }
	void stop(unsigned i) {
		peers[i]->stop();
		retired.push_back(peers[i]);
		running[i] = false;
	}
	void restart(unsigned i) {
		stop(i);
		start(i);
	}
	void pump() {
		const auto now = static_cast<std::uint64_t>(ms(Clock::now() - origin));
		if (!frozen) {
			side.restart();
			side.poll();
		}
		for (unsigned i = 0; i != 3; ++i) {
			if (frozen && aside == i)
				continue;
			auto &node = *nodes[i];
			if (running[i])
				peers[i]->admit();
			const Tick tick{now, timing.election_min + i * (timing.election_max - timing.election_min) * 2 / 5};
			node.worker().run_one(tick);
			for (auto &action : node.worker().take_actions()) {
				if (auto send = std::get_if<Send>(&action)) {
					if (running[i])
						peers[i]->send(std::move(*send));
				} else if (auto committed = std::get_if<Committed>(&action)) {
					for (const auto &entry : committed->entries)
						if (entry.kind == EntryKind::Command)
							node.commands.push_back(entry.payload);
					node.worker().applied({committed->entries.back().index});
				}
			}
			node.channel().run_one(tick);
			if (running[i])
				peers[i]->pump();
		}
	}
	// Pumps every millisecond until done() or the limit; returns whether done() held.
	asio::awaitable<bool> until(std::function<bool()> done, Clock::duration limit) {
		const auto end = Clock::now() + limit;
		asio::steady_timer timer(io);
		while (Clock::now() < end) {
			pump();
			if (done())
				co_return true;
			timer.expires_after(1ms);
			co_await timer.async_wait(asio::use_awaitable);
		}
		if (verbose)
			report();
		co_return false;
	}
	bool verbose = true;
	// Each voter's state, printed when a wait times out.
	void report() const {
		for (unsigned i = 0; i != 3; ++i) {
			const auto &worker = nodes[i]->worker_;
			std::fprintf(stderr, "  voter %u: role %d term %llu committed %llu commands %zu", i + 1,
						 static_cast<int>(worker.role()), static_cast<unsigned long long>(worker.term()),
						 static_cast<unsigned long long>(worker.committed()), nodes[i]->commands.size());
			if (running[i]) {
				const auto &stats = peers[i]->stats();
				std::fprintf(stderr, " connections %zu admitted %llu rejected %llu reconnects %llu lookups %llu", connections(i),
							 static_cast<unsigned long long>(stats.admitted), static_cast<unsigned long long>(stats.rejected),
							 static_cast<unsigned long long>(stats.reconnects), static_cast<unsigned long long>(stats.lookups));
			}
			std::fprintf(stderr, "\n");
		}
	}
	asio::awaitable<void> idle(Clock::duration duration) {
		verbose = false;
		(void)co_await until([] { return false; }, duration);
		verbose = true;
	}
	std::optional<unsigned> leader() const {
		for (unsigned i = 0; i != 3; ++i)
			if (nodes[i]->worker().role() == Role::Leader && !(frozen && aside == i))
				return i;
		return std::nullopt;
	}
	bool has(unsigned i, const std::string &command) const {
		const auto &commands = nodes[i]->commands;
		return std::find(commands.begin(), commands.end(), command) != commands.end();
	}
	// Proposes `command` through whichever voter leads until each voter in `on` applied it.
	asio::awaitable<bool> commit(std::string command, std::vector<unsigned> on, Clock::duration limit) {
		std::optional<std::pair<unsigned, Term>> proposed;
		co_return co_await until(
			[&] {
				// A new leader or term may have lost the proposal: propose again.
				if (auto current = leader()) {
					const auto at = std::make_pair(*current, nodes[*current]->worker().term());
					if (proposed != at &&
						nodes[*current]->worker().try_submit(Propose{++request, command}) == SubmitResult::Accepted)
						proposed = at;
				}
				for (auto i : on)
					if (!has(i, command))
						return false;
				return true;
			},
			limit);
	}
	// A voter's authenticated connections: two lanes per connected peer.
	std::size_t connections(unsigned i) const {
		if (!running[i])
			return 0;
		const auto occupancy = peers[i]->occupancy();
		return occupancy.connections - occupancy.handshakes;
	}
	// Every voter's two lanes to each peer; a lane counts on each end as that end's handshake
	// completes, so a voter holding four does not mean the others do.
	asio::awaitable<bool> fully_connected(Clock::duration limit) {
		co_return co_await until(
			[&] { return connections(0) == 4 && connections(1) == 4 && connections(2) == 4; }, limit);
	}
	asio::awaitable<bool> stop_all(Clock::duration limit) {
		for (unsigned i = 0; i != 3; ++i)
			if (running[i])
				stop(i);
		co_return co_await until(
			[&] {
				for (auto &old : retired)
					if (!old->drained())
						return false;
				return true;
			},
			limit);
	}
};

using Scenario = asio::awaitable<void> (*)(asio::io_context &, const Certificates &);
void run(const char *name, const Certificates &certificates, Scenario scenario) {
	asio::io_context io;
	const auto start = Clock::now();
	std::exception_ptr failure;
	asio::co_spawn(io, scenario(io, certificates), [&](std::exception_ptr error) { failure = error; });
	io.run();
	if (failure)
		std::rethrow_exception(failure);
	std::printf("%s: ok (%lld ms)\n", name, ms(Clock::now() - start));
	std::fflush(stdout);
}

// Every voter resolves every name to `address` through its own FakeDns.
std::array<std::shared_ptr<FakeDns>, 3> answering(Cluster &cluster, asio::ip::address address) {
	std::array<std::shared_ptr<FakeDns>, 3> dns;
	for (unsigned i = 0; i != 3; ++i) {
		dns[i] = std::make_shared<FakeDns>();
		for (const auto &name : names)
			dns[i]->set(name, FakeDns::Mode::Answer, {address});
		cluster.resolve_with(dns[i], i);
	}
	return dns;
}

// A1: voters named by host find each other through the resolver and serve; a voter whose
// name moves to another address is redialed there.
asio::awaitable<void> names_and_moves(asio::io_context &io, const Certificates &certificates) {
	Cluster cluster(io, certificates);
	const auto other = alternate_loopback(io);
	const std::array<std::uint16_t, 3> ports{free_port(io, loopback), shared_port(io, loopback, other),
											 free_port(io, loopback)};
	cluster.name_peers(ports);
	auto dns = answering(cluster, loopback);
	for (unsigned i = 0; i != 3; ++i)
		cluster.options[i].listen = {loopback, ports[i]};
	for (unsigned i = 0; i != 3; ++i)
		cluster.start(i);
	CHECK(co_await cluster.commit("named", {0, 1, 2}, 5s));
	CHECK(co_await cluster.fully_connected(3s));
	CHECK(dns[0]->count("two") >= 1 && dns[0]->count("three") >= 1 && dns[1]->count("three") >= 1);
	CHECK(dns[1]->count("one") == 0 && dns[2]->count("one") == 0); // the lower id dials

	// Voter two moves to the other address and keeps its port; its name follows.
	for (auto &resolver : dns)
		resolver->set("two", FakeDns::Mode::Answer, {other});
	cluster.stop(1);
	cluster.options[1].listen = {other, ports[1]};
	cluster.start(1);
	const auto moved = Clock::now();
	CHECK(co_await cluster.until([&] { return cluster.connections(1) == 4; }, 5s));
	std::printf("  moved voter redialed at %s in %lld ms\n", other.to_string().c_str(), ms(Clock::now() - moved));
	CHECK(co_await cluster.commit("after move", {0, 1, 2}, 5s));
	CHECK(co_await cluster.stop_all(2s));
	for (auto &resolver : dns)
		resolver->quiesce();
}

// A voter listening on `::` serves an IPv4 peer and an IPv6 peer from one socket: voter one
// dials voter three at 127.0.0.1 and voter two dials it at ::1.
asio::awaitable<void> dual_stack(asio::io_context &io, const Certificates &certificates) {
	const auto v6 = asio::ip::make_address("::1");
	if (!bindable(io, {v6, 0})) {
		std::printf("dual-stack: skipped, no ::1\n");
		co_return;
	}
	Cluster cluster(io, certificates);
	const std::array<std::uint16_t, 3> ports{free_port(io, loopback), free_port(io, loopback),
											 free_port(io, asio::ip::address_v6::any())};
	cluster.name_peers(ports);
	auto dns = answering(cluster, loopback);
	dns[1]->set("three", FakeDns::Mode::Answer, {v6});
	cluster.options[0].listen = {loopback, ports[0]};
	cluster.options[1].listen = {loopback, ports[1]};
	cluster.options[2].listen = {asio::ip::address_v6::any(), ports[2]};
	for (unsigned i = 0; i != 3; ++i)
		cluster.start(i);
	CHECK(co_await cluster.commit("dual stack", {0, 1, 2}, 5s));
	CHECK(co_await cluster.fully_connected(3s));
	CHECK(co_await cluster.stop_all(2s));
	for (auto &resolver : dns)
		resolver->quiesce();
}

// A1 over the system resolver: every voter is "localhost".
asio::awaitable<void> system_localhost(asio::io_context &io, const Certificates &certificates) {
	Cluster cluster(io, certificates);
	const std::array<std::uint16_t, 3> ports{free_port(io, loopback), free_port(io, loopback),
											 free_port(io, loopback)};
	cluster.name_peers(ports, {"localhost", "localhost", "localhost"});
	for (unsigned i = 0; i != 3; ++i)
		cluster.options[i].listen = {loopback, ports[i]};
	for (unsigned i = 0; i != 3; ++i)
		cluster.start(i);
	CHECK(co_await cluster.commit("localhost", {0, 1, 2}, 5s));
	CHECK(co_await cluster.fully_connected(3s));
	CHECK(cluster.peers[0]->stats().lookups >= 1 && cluster.peers[1]->stats().lookups >= 1);
	CHECK(co_await cluster.stop_all(2s));
}

// A2: voter one's lookup of "two" never answers. The others connect, voter one keeps
// quorum and pays one parked lookup however often it redials, a hung lookup of a cached
// name redials the cached address within the resolve deadline, and stop() returns at once.
asio::awaitable<void> hung_resolver(asio::io_context &io, const Certificates &certificates) {
	Cluster cluster(io, certificates);
	const std::array<std::uint16_t, 3> ports{free_port(io, loopback), free_port(io, loopback),
											 free_port(io, loopback)};
	cluster.name_peers(ports);
	auto dns = answering(cluster, loopback);
	for (unsigned i = 0; i != 3; ++i)
		cluster.options[i].listen = {loopback, ports[i]};
	dns[0]->set("two", FakeDns::Mode::Hang);
	// One and three form quorum first, so that the leader reaches both (it is not two).
	cluster.start(0);
	cluster.start(2);
	CHECK(co_await cluster.commit("quorum", {0, 2}, 5s));
	cluster.start(1);
	const auto started = Clock::now();
	CHECK(co_await cluster.until([&] { return cluster.connections(1) == 2 && cluster.connections(2) == 4; }, 3s));
	std::printf("  with a hung lookup, the other peers connected in %lld ms\n", ms(Clock::now() - started));
	CHECK(cluster.connections(0) == 2);
	co_await cluster.idle(1s);
	CHECK(cluster.peers[0]->stats().reconnects >= 4);
	CHECK(dns[0]->count("two") == 1 && cluster.peers[0]->occupancy().lookups == 1);

	CHECK(cluster.peers[0]->stats().lookup_timeouts >= 4); // its uncached dials found it late

	// Voter one has "three" cached; its next lookup hangs, and voter three restarts.
	dns[0]->set("three", FakeDns::Mode::Hang);
	const auto lookups = dns[0]->count("three");
	cluster.restart(2);
	const auto bounced = Clock::now();
	CHECK(co_await cluster.until([&] { return cluster.connections(2) == 4; }, 3s));
	const auto redial = ms(Clock::now() - bounced);
	std::printf("  hung lookup of a cached name: redialed in %lld ms (resolve deadline 200 ms)\n", redial);
	CHECK(redial < 200 + 800);
	CHECK(dns[0]->count("three") == lookups + 1 && cluster.peers[0]->occupancy().lookups == 2);
	CHECK(co_await cluster.commit("still quorum", {0, 2}, 5s));

	const auto stopping = Clock::now();
	CHECK(co_await cluster.stop_all(100ms));
	std::printf("  stop() with two hung lookups drained in %lld ms\n", ms(Clock::now() - stopping));
	for (auto &resolver : dns)
		resolver->quiesce();
}

// A3: at startup "two" does not resolve on voter one, which retries with backoff while the
// others form quorum. Once the cluster formed, every lookup fails (NXDOMAIN, SERVFAIL,
// timeout): connections stay, and a restarted voter is redialed at its cached address.
asio::awaitable<void> outage(asio::io_context &io, const Certificates &certificates) {
	Cluster cluster(io, certificates);
	const std::array<std::uint16_t, 3> ports{free_port(io, loopback), free_port(io, loopback),
											 free_port(io, loopback)};
	cluster.name_peers(ports);
	auto dns = answering(cluster, loopback);
	for (unsigned i = 0; i != 3; ++i)
		cluster.options[i].listen = {loopback, ports[i]};
	dns[0]->set("two", FakeDns::Mode::Empty);
	const auto started = Clock::now();
	cluster.start(0);
	cluster.start(2);
	CHECK(co_await cluster.commit("without two's name", {0, 2}, 5s));
	cluster.start(1);
	co_await cluster.idle(1s);
	const auto elapsed = ms(Clock::now() - started);
	const auto attempts = dns[0]->count("two");
	std::printf("  NXDOMAIN at startup: %u lookups of two in %lld ms\n", attempts, elapsed);
	// Each of two lanes looks up once per round, after 100-199 ms of backoff.
	CHECK(attempts >= 4 && attempts <= 2 * (elapsed / 100 + 1));
	CHECK(cluster.connections(0) == 2 && cluster.peers[0]->stats().lookup_failures + 1 >= attempts);
	dns[0]->set("two", FakeDns::Mode::Answer);
	CHECK(co_await cluster.fully_connected(3s));

	// The outage: NXDOMAIN, SERVFAIL and timeouts for every name, on every voter.
	for (auto &resolver : dns) {
		resolver->set("one", FakeDns::Mode::Empty);
		resolver->set("two", FakeDns::Mode::Throw);
		resolver->set("three", FakeDns::Mode::Hang);
	}
	std::array<std::uint64_t, 3> admitted{};
	for (unsigned i = 0; i != 3; ++i)
		admitted[i] = cluster.peers[i]->stats().admitted;
	CHECK(co_await cluster.commit("during outage", {0, 1, 2}, 5s));
	co_await cluster.idle(500ms);
	bool steady = true;
	for (unsigned i = 0; i != 3; ++i)
		steady = steady && cluster.connections(i) == 4 && cluster.peers[i]->stats().admitted == admitted[i];
	if (!steady) {
		std::fprintf(stderr, "  admitted before the outage: %llu %llu %llu\n", (unsigned long long)admitted[0],
					 (unsigned long long)admitted[1], (unsigned long long)admitted[2]);
		cluster.report();
	}
	CHECK(steady);

	// Voter three restarts: one and two redial it through a hung lookup, at the cached address.
	cluster.restart(2);
	const auto bounced = Clock::now();
	CHECK(co_await cluster.until(
		[&] { return cluster.connections(0) == 4 && cluster.connections(1) == 4 && cluster.connections(2) == 4; },
		5s));
	std::printf("  outage: restarted voter redialed at its cached address in %lld ms\n",
				ms(Clock::now() - bounced));
	// Voter two restarts: voter one's lookup of "two" throws, and it dials the cached address.
	// The new Peers of voter two has no cache, so it cannot dial "three" until DNS returns.
	cluster.restart(1);
	CHECK(co_await cluster.until([&] { return cluster.connections(1) == 2; }, 5s));
	CHECK(co_await cluster.commit("after outage", {0, 2}, 5s));
	co_await cluster.idle(500ms);
	CHECK(cluster.connections(0) == 4 && cluster.connections(1) == 2);
	CHECK(co_await cluster.stop_all(500ms));
	for (auto &resolver : dns)
		resolver->quiesce();
}

// A4: on voter one, "two" resolves to voter three's address (same port, other address).
// The certificate pin refuses that dial at the handshake, voter three admits nothing from
// it, and voter one retries until the name is right.
asio::awaitable<void> wrong_node(asio::io_context &io, const Certificates &certificates) {
	Cluster cluster(io, certificates);
	const auto other = alternate_loopback(io);
	const auto shared = shared_port(io, loopback, other);
	const std::array<std::uint16_t, 3> ports{free_port(io, loopback), shared, shared};
	cluster.name_peers(ports);
	auto dns = answering(cluster, loopback);
	for (auto &resolver : dns)
		resolver->set("three", FakeDns::Mode::Answer, {other});
	dns[0]->set("two", FakeDns::Mode::Answer, {other});
	cluster.options[0].listen = {loopback, ports[0]};
	cluster.options[1].listen = {loopback, shared};
	cluster.options[2].listen = {other, shared};
	cluster.start(0);
	cluster.start(2);
	CHECK(co_await cluster.commit("despite a wrong name", {0, 2}, 5s));
	cluster.start(1);
	co_await cluster.idle(1s);
	CHECK(cluster.connections(0) == 2 && cluster.connections(1) == 2 && cluster.connections(2) == 4);
	CHECK(cluster.peers[0]->stats().reconnects >= 4 && cluster.peers[0]->stats().rejected >= 4);
	CHECK(cluster.peers[2]->stats().admitted == 4); // the true lanes of one and two only
	std::printf("  wrong node: %llu dials refused by the pin; voter three admitted %llu lanes\n",
				static_cast<unsigned long long>(cluster.peers[0]->stats().rejected),
				static_cast<unsigned long long>(cluster.peers[2]->stats().admitted));
	dns[0]->set("two", FakeDns::Mode::Answer, {loopback});
	CHECK(co_await cluster.until([&] { return cluster.connections(1) == 4; }, 3s));
	CHECK(co_await cluster.commit("right name", {0, 1, 2}, 5s));
	CHECK(co_await cluster.stop_all(500ms));
	for (auto &resolver : dns)
		resolver->quiesce();
}

// N1: on voter one, "two" answers {voter three's address, voter two's address}. Three's
// connects first and the pin refuses it; the next dial starts past it and reaches voter two.
asio::awaitable<void> refused_first(asio::io_context &io, const Certificates &certificates) {
	Cluster cluster(io, certificates);
	const auto other = alternate_loopback(io);
	const auto shared = shared_port(io, loopback, other);
	const std::array<std::uint16_t, 3> ports{free_port(io, loopback), shared, shared};
	cluster.name_peers(ports);
	auto dns = answering(cluster, loopback);
	for (auto &resolver : dns)
		resolver->set("three", FakeDns::Mode::Answer, {other});
	dns[0]->set("two", FakeDns::Mode::Answer, {other, loopback});
	cluster.options[0].listen = {loopback, ports[0]};
	cluster.options[1].listen = {loopback, shared};
	cluster.options[2].listen = {other, shared};
	for (unsigned i = 0; i != 3; ++i)
		cluster.start(i);
	const auto started = Clock::now();
	CHECK(co_await cluster.until([&] { return cluster.connections(1) == 4; }, 3s));
	const auto reached = ms(Clock::now() - started);
	std::printf("  wrong address first: reached voter two in %lld ms (%llu refusals)\n", reached,
				static_cast<unsigned long long>(cluster.peers[0]->stats().rejected));
	CHECK(cluster.peers[0]->stats().rejected >= 1 && reached < 1000); // within a few backoffs
	CHECK(co_await cluster.commit("past the wrong address", {0, 1, 2}, 5s));
	CHECK(co_await cluster.stop_all(500ms));
	for (auto &resolver : dns)
		resolver->quiesce();
}

// N3: every lookup of "three" answers after 1.5 s, past the 1 s resolve deadline. Once the
// name is cached, a redial goes to the cache at once and the slow lookup refreshes it behind.
asio::awaitable<void> slow_resolver(asio::io_context &io, const Certificates &certificates) {
	Cluster cluster(io, certificates);
	const std::array<std::uint16_t, 3> ports{free_port(io, loopback), free_port(io, loopback),
											 free_port(io, loopback)};
	cluster.name_peers(ports);
	auto dns = answering(cluster, loopback);
	for (unsigned i = 0; i != 3; ++i) {
		cluster.options[i].listen = {loopback, ports[i]};
		cluster.options[i].resolve_deadline = 1000ms;
		dns[i]->slow = 1500ms;
		dns[i]->set("three", FakeDns::Mode::Slow);
	}
	for (unsigned i = 0; i != 3; ++i)
		cluster.start(i);
	CHECK(co_await cluster.until([&] { return cluster.connections(2) == 4; }, 5s));
	CHECK(co_await cluster.commit("slow dns", {0, 1, 2}, 5s));
	co_await cluster.idle(1600ms); // past any lookup in flight, so the redial starts one
	const auto lookups = dns[0]->count("three") + dns[1]->count("three");
	cluster.restart(2);
	const auto bounced = Clock::now();
	CHECK(co_await cluster.until([&] { return cluster.connections(2) == 4; }, 3s));
	const auto redial = ms(Clock::now() - bounced);
	std::printf("  slow lookup (1500 ms) of a cached name: redialed in %lld ms (resolve deadline 1000 ms)\n", redial);
	CHECK(redial < 700);
	CHECK(dns[0]->count("three") + dns[1]->count("three") > lookups);
	CHECK(co_await cluster.stop_all(500ms));
	for (auto &resolver : dns)
		resolver->quiesce();
}

// N4: voter three is down but its name resolves. Voters one and two keep redialing it after
// 100-199 ms of backoff per lane, and look its name up at most once per resolve deadline.
asio::awaitable<void> lookup_spacing(asio::io_context &io, const Certificates &certificates) {
	Cluster cluster(io, certificates);
	const std::array<std::uint16_t, 3> ports{free_port(io, loopback), free_port(io, loopback),
											 free_port(io, loopback)};
	cluster.name_peers(ports);
	auto dns = answering(cluster, loopback);
	for (unsigned i = 0; i != 3; ++i)
		cluster.options[i].listen = {loopback, ports[i]};
	cluster.start(0);
	cluster.start(1);
	CHECK(co_await cluster.commit("without three", {0, 1}, 5s));
	const auto lookups = dns[0]->count("three");
	const auto reconnects = cluster.peers[0]->stats().reconnects;
	const auto begun = Clock::now();
	co_await cluster.idle(1200ms);
	const auto window = ms(Clock::now() - begun);
	const auto looked = dns[0]->count("three") - lookups;
	const auto redials = cluster.peers[0]->stats().reconnects - reconnects;
	std::printf("  down peer: %llu redials, %u lookups in %lld ms (resolve deadline 200 ms)\n",
				static_cast<unsigned long long>(redials), looked, window);
	CHECK(looked >= 2 && static_cast<long long>(looked) <= window / 200 + 1);
	CHECK(redials > looked);
	CHECK(co_await cluster.stop_all(500ms));
	for (auto &resolver : dns)
		resolver->quiesce();
}

// N6: the constructor takes addresses for exactly the other voters.
asio::awaitable<void> membership(asio::io_context &io, const Certificates &certificates) {
	Cluster cluster(io, certificates);
	cluster.name_peers({free_port(io, loopback), free_port(io, loopback), free_port(io, loopback)});
	cluster.options[0].listen = {loopback, 0};
	const auto rejects = [&](std::map<NodeId, PeerAddress> addresses) {
		auto options = cluster.options[0];
		options.addresses = std::move(addresses);
		PeerConfiguration configuration{fixed(1), limits(), {}, {}, {}, {}};
		try {
			Peers<Node> peers(io.get_executor(), cluster.nodes[0], configuration, options);
		} catch (const std::invalid_argument &) {
			return true;
		}
		return false;
	};
	const PeerAddress somewhere{"two", 1};
	CHECK(!rejects({{2, somewhere}, {3, somewhere}}));
	CHECK(rejects({{2, somewhere}, {4, somewhere}}));             // a non-voter for a voter
	CHECK(rejects({{2, somewhere}, {3, somewhere}, {4, somewhere}})); // a non-voter besides
	CHECK(rejects({{1, somewhere}, {2, somewhere}}));             // itself for a voter
	CHECK(rejects({{1, somewhere}, {2, somewhere}, {3, somewhere}}));
	co_return;
}

// Whether a connect to `endpoint` neither completes nor fails within 300 ms.
bool hangs(Tcp::endpoint endpoint) {
	asio::io_context probe;
	Tcp::socket socket(probe);
	bool done = false;
	socket.async_connect(endpoint, [&](asio::error_code) { done = true; });
	probe.run_for(300ms);
	return !done;
}

// A6: "two" resolves to an unroutable address first. The dial starts the next address a
// stagger later and connects then, not after the connect deadline. The cache keeps the
// whole set: when voter two moves to another address of the set during an outage, voter
// one finds it there.
asio::awaitable<void> several_addresses(asio::io_context &io, const Certificates &certificates) {
	const std::vector<asio::ip::address> mixed{
		asio::ip::make_address("2001:db8::1"), asio::ip::make_address("2001:db8::2"),
		asio::ip::make_address("192.0.2.1"), asio::ip::make_address("192.0.2.2"), asio::ip::make_address("192.0.2.3")};
	const std::vector<std::string> expected{"2001:db8::1", "192.0.2.1", "2001:db8::2", "192.0.2.2", "192.0.2.3"};
	const auto ordered = interleave(mixed, 7);
	CHECK(ordered.size() == expected.size());
	for (std::size_t i = 0; i != expected.size(); ++i)
		CHECK(ordered[i].address().to_string() == expected[i] && ordered[i].port() == 7);
	CHECK(interleave({}, 7).empty());

	Cluster cluster(io, certificates);
	const auto other = alternate_loopback(io);
	const auto port = shared_port(io, loopback, other);
	// The black hole: TEST-NET-1 where this host routes it into the void, else `other` with
	// a full accept queue at the voter's port (Linux drops the SYN).
	std::optional<Tcp::acceptor> full;
	auto blackhole = asio::ip::make_address("192.0.2.1");
	if (!hangs({blackhole, port})) {
		blackhole = other;
		full.emplace(io, Tcp::endpoint(other, port));
		full->listen(0);
		for (unsigned i = 0; i != 16 && !hangs({other, port}); ++i) {
		}
		CHECK(hangs({other, port}));
	}
	const std::array<std::uint16_t, 3> ports{free_port(io, loopback), port, free_port(io, loopback)};
	cluster.name_peers(ports);
	auto dns = std::make_shared<FakeDns>();
	dns->set("two", FakeDns::Mode::Answer, {blackhole, loopback});
	dns->set("three", FakeDns::Mode::Answer, {loopback});
	for (unsigned i = 0; i != 3; ++i) {
		cluster.resolve_with(dns, i);
		cluster.options[i].deadline = 5000ms;
		cluster.options[i].listen = {loopback, ports[i]};
	}
	// Voter three stays down, so voter two's lanes come from voter one only.
	cluster.start(0);
	cluster.start(1);
	const auto started = Clock::now();
	CHECK(co_await cluster.until([&] { return cluster.connections(1) == 2; }, 4s));
	const auto connected = ms(Clock::now() - started);
	std::printf("  unroutable %s first: connected in %lld ms (stagger 100 ms, deadline 5000 ms)\n",
				blackhole.to_string().c_str(), connected);
	CHECK(connected < 100 + 900);
	CHECK(co_await cluster.commit("two of three", {0, 1}, 5s));
	full.reset();

	// "two" now answers both loopback addresses; voter two restarts and is redialed, and the
	// lookup behind the redial (past the lookup spacing) caches the new set.
	dns->set("two", FakeDns::Mode::Answer, {loopback, other});
	co_await cluster.idle(250ms);
	const auto lookups = dns->count("two");
	cluster.restart(1);
	CHECK(co_await cluster.until(
		[&] {
			return cluster.connections(1) == 2 && dns->count("two") > lookups &&
				   cluster.peers[0]->occupancy().lookups == 0;
		},
		4s));
	// An outage, and voter two moves to the set's other address.
	dns->set("two", FakeDns::Mode::Empty);
	cluster.stop(1);
	cluster.options[1].listen = {other, port};
	cluster.start(1);
	const auto moved = Clock::now();
	CHECK(co_await cluster.until([&] { return cluster.connections(1) == 2; }, 4s));
	const auto redial = ms(Clock::now() - moved);
	std::printf("  cached set: found voter two at %s in %lld ms\n", other.to_string().c_str(), redial);
	CHECK(redial < 2000);
	CHECK(co_await cluster.stop_all(500ms));
	dns->quiesce();
}

// Deterministic bytes, distinct per seed, so a fetched copy compares exactly.
std::string pattern(std::size_t size, unsigned seed) {
	std::string bytes(size, '\0');
	std::uint64_t state = 0x9e3779b97f4a7c15ull * (seed + 1);
	for (auto &byte : bytes) {
		state ^= state << 13;
		state ^= state >> 7;
		state ^= state << 17;
		byte = static_cast<char>(state);
	}
	return bytes;
}
using Files = std::map<std::string, std::string, std::less<>>;
// A voter's reader over its files; the name "throws" throws, and `hook` sees every read.
FetchReader reader(std::shared_ptr<Files> files, std::function<void(std::string_view)> hook = {}) {
	return [files, hook](std::string_view name) -> std::optional<std::string> {
		if (hook)
			hook(name);
		if (name == "throws")
			throw std::runtime_error("unreadable");
		auto found = files->find(name);
		return found == files->end() ? std::nullopt : std::optional<std::string>(found->second);
	};
}
struct Fetched {
	bool done = false;
	std::error_code error;
	std::optional<std::string> bytes;
	Clock::time_point asked = Clock::now(), answered;
};
// Voter `from` asks `peer` for `name`. The completion must run once, on the voter's executor,
// and never within fetch().
std::shared_ptr<Fetched> fetch(Cluster &cluster, unsigned from, NodeId peer, std::string name) {
	auto result = std::make_shared<Fetched>();
	auto returned = std::make_shared<bool>(false);
	cluster.peers[from]->fetch(peer, std::move(name),
							   [result, returned, executor = cluster.context(from).get_executor()](std::error_code error,
																					  std::optional<std::string> bytes) {
								   CHECK(*returned && !result->done && executor.running_in_this_thread());
								   result->done = true;
								   result->error = error;
								   result->bytes = std::move(bytes);
								   result->answered = Clock::now();
							   });
	*returned = true;
	return result;
}
bool all_done(const std::vector<std::shared_ptr<Fetched>> &fetches) {
	return std::all_of(fetches.begin(), fetches.end(), [](const auto &fetched) { return fetched->done; });
}
// Voters at IP literals on loopback, each serving its files.
std::array<std::shared_ptr<Files>, 3> serving(Cluster &cluster, asio::io_context &io) {
	const std::array<std::uint16_t, 3> ports{free_port(io, loopback), free_port(io, loopback), free_port(io, loopback)};
	cluster.name_peers(ports, {"127.0.0.1", "127.0.0.1", "127.0.0.1"});
	std::array<std::shared_ptr<Files>, 3> files;
	for (unsigned i = 0; i != 3; ++i) {
		cluster.options[i].listen = {loopback, ports[i]};
		files[i] = std::make_shared<Files>();
		cluster.options[i].reader = reader(files[i]);
	}
	return files;
}

// F1: every voter asks both others, at once, for bytes they hold (larger than a lane frame),
// a name they lack, an empty file and one their reader cannot read; a fifth fetch past the
// bound, and one before any bulk lane, fail at once. Completions run on the owner executor.
asio::awaitable<void> fetch_exact(asio::io_context &io, const Certificates &certificates) {
	Cluster cluster(io, certificates);
	auto files = serving(cluster, io);
	for (unsigned i = 0; i != 3; ++i)
		*files[i] = {{"history/seal-7", pattern(3 * fetch_chunk_bytes + 5, i)}, {"history/empty", ""}};
	for (unsigned i = 0; i != 3; ++i)
		cluster.start(i);
	auto early = fetch(cluster, 0, 2, "history/seal-7");
	bool refused = false;
	try {
		cluster.peers[0]->fetch(2, "", [](std::error_code, std::optional<std::string>) {});
	} catch (const std::invalid_argument &) {
		refused = true;
	}
	CHECK(refused && !early->done);
	CHECK(co_await cluster.until([&] { return early->done; }, 1s));
	CHECK(early->error == std::errc::not_connected && !early->bytes);
	CHECK(co_await cluster.commit("serving", {0, 1, 2}, 5s));
	CHECK(co_await cluster.fully_connected(3s));

	const std::array<std::string, 4> names{"history/seal-7", "history/missing", "history/empty", "throws"};
	struct Asked {
		unsigned from;
		NodeId peer;
		std::string name;
		std::shared_ptr<Fetched> result;
	};
	std::vector<Asked> asked;
	std::vector<std::shared_ptr<Fetched>> busy;
	const auto started = Clock::now();
	for (unsigned i = 0; i != 3; ++i)
		for (NodeId peer = 1; peer <= 3; ++peer) {
			if (peer == i + 1)
				continue;
			for (const auto &name : names)
				asked.push_back({i, peer, name, fetch(cluster, i, peer, name)});
			busy.push_back(fetch(cluster, i, peer, "history/seal-7"));
		}
	std::vector<std::shared_ptr<Fetched>> results;
	for (const auto &fetched : asked)
		results.push_back(fetched.result);
	results.insert(results.end(), busy.begin(), busy.end());
	CHECK(co_await cluster.until([&] { return all_done(results); }, 3s));
	std::printf("  %zu concurrent fetches between three voters answered in %lld ms\n", asked.size(),
				ms(Clock::now() - started));
	for (const auto &[from, peer, name, result] : asked) {
		const auto &held = *files[peer - 1];
		if (name == "throws")
			CHECK(result->error == std::errc::io_error && !result->bytes);
		else if (auto found = held.find(name); found != held.end())
			CHECK(!result->error && result->bytes == found->second);
		else
			CHECK(!result->error && !result->bytes);
	}
	for (const auto &result : busy)
		CHECK(result->error == std::errc::resource_unavailable_try_again && !result->bytes);
	for (unsigned i = 0; i != 3; ++i) {
		const auto &stats = cluster.peers[i]->stats();
		const auto occupancy = cluster.peers[i]->occupancy();
		CHECK(stats.served == 8 && stats.fetched == 6 && stats.fetch_failures == 4 + (i == 0));
		CHECK(occupancy.fetches == 0 && occupancy.serving == 0 && cluster.connections(i) == 4);
	}
	CHECK(co_await cluster.stop_all(2s));
}

// The append round trips every voter observed since `before`: how many, how many timed out,
// and the bound of the slowest one's histogram bucket.
struct RoundTrips {
	std::uint64_t count = 0, timeouts = 0, slowest_ns = 0;
};
using AppendSnapshots = std::array<std::pair<kronuz::metrics::DurationSnapshot, kronuz::metrics::DurationSnapshot>, 3>;
AppendSnapshots appends(const Cluster &cluster) {
	AppendSnapshots result;
	for (unsigned i = 0; i != 3; ++i)
		result[i] = {cluster.peers[i]->stats().append.response.snapshot(),
					 cluster.peers[i]->stats().append.timeout.snapshot()};
	return result;
}
RoundTrips since(const Cluster &cluster, const AppendSnapshots &before) {
	RoundTrips result;
	const auto now = appends(cluster);
	for (unsigned i = 0; i != 3; ++i) {
		result.count += now[i].first.count - before[i].first.count;
		result.timeouts += now[i].second.count - before[i].second.count;
		for (std::size_t bucket = 0; bucket != now[i].first.buckets.size(); ++bucket)
			if (now[i].first.buckets[bucket] != before[i].first.buckets[bucket])
				result.slowest_ns = std::max(result.slowest_ns, bucket < kronuz::metrics::duration_bounds_ns.size()
																	? kronuz::metrics::duration_bounds_ns[bucket]
																	: ~std::uint64_t{0});
	}
	return result;
}

#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define CONSENSUS_TEST_TSAN 1
#endif
#endif
#if defined(__SANITIZE_THREAD__) && !defined(CONSENSUS_TEST_TSAN)
#define CONSENSUS_TEST_TSAN 1
#endif
// The slowest append round trip's bucket bound while fetching. TSan slows the executor several
// times over and reaches the 64 ms edge, so there the next bucket holds and the leader's
// unchanged term carries the election bound.
#ifdef CONSENSUS_TEST_TSAN
constexpr std::uint64_t round_trip_bound_ns = 256'000'000;
#else
constexpr std::uint64_t round_trip_bound_ns = 64'000'000;
#endif

// F2: rounds of four 8 MiB fetches at once, both ways between voters one and three, through 1 MiB
// byte budgets every lane shares, under the `loaded` timing (D92). Consensus keeps committing
// within 2 s each: no append round trip times out or lands past 64 ms (256 ms under TSan), and
// the leader keeps its term.
asio::awaitable<void> fetch_alongside_consensus(asio::io_context &io, const Certificates &certificates) {
	constexpr std::size_t size = 8 * 1024 * 1024, budget = 1024 * 1024;
	Cluster cluster(io, certificates, loaded);
	auto files = serving(cluster, io);
	for (unsigned i = 0; i != 3; ++i) {
		(*files[i])["history/table"] = pattern(size, 10 + i);
		cluster.options[i].incoming_bytes = cluster.options[i].outgoing_bytes = budget;
	}
	for (unsigned i = 0; i != 3; ++i)
		cluster.start(i);
	CHECK(co_await cluster.commit("before", {0, 1, 2}, 5s));
	CHECK(co_await cluster.fully_connected(3s));
	co_await cluster.idle(200ms);
	const auto leader = cluster.leader();
	CHECK(leader);
	const auto term = cluster.nodes[*leader]->worker().term();

	// Commits a command every 20 ms while `busy()` holds, at most eight (the log holds 32
	// entries), then waits out `busy()`; returns the commits and the slowest, in ms.
	unsigned command = 0;
	auto commits = [&](std::function<bool()> busy, Clock::duration limit) -> asio::awaitable<std::pair<unsigned, long long>> {
		unsigned count = 0;
		long long slowest = 0;
		const auto end = Clock::now() + limit;
		while (busy() && count != 8) {
			const auto begun = Clock::now();
			CHECK(co_await cluster.commit("command " + std::to_string(++command), {0, 1, 2}, 2s));
			slowest = std::max(slowest, ms(Clock::now() - begun));
			++count;
			cluster.verbose = false;
			(void)co_await cluster.until([&] { return !busy(); }, 20ms);
			cluster.verbose = true;
		}
		(void)co_await cluster.until([&] { return !busy(); }, end - Clock::now());
		co_return std::make_pair(count, slowest);
	};
	auto before = appends(cluster);
	const auto quiet_end = Clock::now() + 500ms;
	const auto [quiet_commits, quiet_slowest] = co_await commits([&] { return Clock::now() < quiet_end; }, 1s);
	const auto quiet = since(cluster, before);

	// Rounds of four fetches, each round once the last one's bytes checked out.
	before = appends(cluster);
	const auto started = Clock::now();
	const std::vector<std::pair<unsigned, NodeId>> pairs{{0, 2}, {0, 3}, {2, 1}, {1, 3}};
	constexpr unsigned rounds = 4;
	unsigned round = 0;
	std::vector<std::shared_ptr<Fetched>> fetches;
	auto fetching = [&] {
		if (!all_done(fetches))
			return true;
		for (std::size_t i = 0; i != fetches.size(); ++i)
			CHECK(!fetches[i]->error && fetches[i]->bytes == (*files[pairs[i].second - 1])["history/table"]);
		fetches.clear();
		if (round == rounds)
			return false;
		++round;
		for (const auto &[from, peer] : pairs)
			fetches.push_back(fetch(cluster, from, peer, "history/table"));
		return true;
	};
	const auto [busy_commits, busy_slowest] = co_await commits(fetching, 20s);
	const auto elapsed = ms(Clock::now() - started);
	const auto busy = since(cluster, before);
	CHECK(round == rounds && fetches.empty());
	std::size_t high_water = 0;
	for (unsigned i = 0; i != 3; ++i) {
		const auto occupancy = cluster.peers[i]->occupancy();
		high_water = std::max({high_water, occupancy.outgoing_high_water, occupancy.incoming_high_water});
	}
	std::printf("  %u rounds of %zu x 8 MiB fetched in %lld ms through 1 MiB budgets (high water %zu KiB)\n", rounds,
				pairs.size(), elapsed, high_water / 1024);
	std::printf("  quiet: %u commits, slowest %lld ms; %llu round trips, slowest bucket <= %llu us, %llu timeouts\n",
				quiet_commits, quiet_slowest, (unsigned long long)quiet.count,
				(unsigned long long)(quiet.slowest_ns / 1000), (unsigned long long)quiet.timeouts);
	std::printf("  fetching: %u commits, slowest %lld ms; %llu round trips, slowest bucket <= %llu us, %llu timeouts\n",
				busy_commits, busy_slowest, (unsigned long long)busy.count, (unsigned long long)(busy.slowest_ns / 1000),
				(unsigned long long)busy.timeouts);
	CHECK(busy_commits >= 1 && busy.count >= 10 && busy.timeouts == 0 && busy.slowest_ns <= round_trip_bound_ns);
	CHECK(cluster.leader() == leader && cluster.nodes[*leader]->worker().term() == term);
	CHECK(co_await cluster.stop_all(2s));
}

// F3: voter two hangs as its reply starts. Voter one's fetch fails at its deadline; voter two
// resumes within the next, the late reply drains into the failed fetch over the same lanes, and
// the next fetch succeeds. Then voter two closes as its reply starts: the fetch fails at once, and the other
// two keep committing.
asio::awaitable<void> fetch_failures(asio::io_context &io, const Certificates &certificates) {
	constexpr std::size_t size = 8 * 1024 * 1024;
	Cluster cluster(io, certificates);
	cluster.aside = 1;
	auto files = serving(cluster, io);
	(*files[1])["history/table"] = pattern(size, 20);
	enum class Then { Serve, Freeze, Close } then = Then::Serve;
	cluster.options[1].reader = reader(files[1], [&](std::string_view) {
		if (then == Then::Freeze) {
			cluster.frozen = true;
			cluster.side.stop();
		} else if (then == Then::Close)
			asio::post(io, [&] { cluster.stop(1); });
		then = Then::Serve;
	});
	cluster.options[0].fetch_deadline = 500ms;
	for (unsigned i = 0; i != 3; ++i)
		cluster.start(i);
	CHECK(co_await cluster.commit("before", {0, 1, 2}, 5s));
	CHECK(co_await cluster.fully_connected(3s));
	const auto admitted = cluster.peers[0]->stats().admitted;

	then = Then::Freeze;
	auto stalled = fetch(cluster, 0, 2, "history/table");
	CHECK(co_await cluster.until([&] { return stalled->done; }, 2s));
	const auto waited = ms(stalled->answered - stalled->asked);
	std::printf("  frozen peer: fetch failed after %lld ms (deadline 500 ms)\n", waited);
	CHECK(cluster.frozen && stalled->error == std::errc::timed_out && !stalled->bytes && waited >= 500);
	CHECK(cluster.peers[0]->occupancy().fetches == 1); // the failed fetch keeps its slot until its reply ends
	cluster.frozen = false;
	CHECK(co_await cluster.until([&] { return cluster.peers[0]->occupancy().fetches == 0; }, 3s));
	auto retried = fetch(cluster, 0, 2, "history/table");
	CHECK(co_await cluster.until([&] { return retried->done; }, 3s));
	CHECK(!retried->error && retried->bytes == (*files[1])["history/table"]);
	CHECK(cluster.peers[0]->stats().admitted == admitted && cluster.connections(0) == 4); // the same lanes
	CHECK(co_await cluster.commit("after the hang", {0, 1, 2}, 5s));

	then = Then::Close;
	auto closed = fetch(cluster, 0, 2, "history/table");
	CHECK(co_await cluster.until([&] { return closed->done; }, 2s));
	const auto failed = ms(closed->answered - closed->asked);
	std::printf("  closed peer: fetch failed after %lld ms\n", failed);
	CHECK(!cluster.running[1] && closed->error == std::errc::connection_aborted && !closed->bytes);
	CHECK(cluster.peers[0]->occupancy().fetches == 0);
	CHECK(co_await cluster.commit("without two", {0, 2}, 5s));
	cluster.start(1);
	CHECK(co_await cluster.fully_connected(3s));
	auto reopened = fetch(cluster, 0, 2, "history/table");
	CHECK(co_await cluster.until([&] { return reopened->done; }, 3s));
	CHECK(!reopened->error && reopened->bytes == (*files[1])["history/table"]);
	CHECK(co_await cluster.commit("with two again", {0, 1, 2}, 5s));
	CHECK(co_await cluster.stop_all(2s));
}

// F4: voter two hangs for good, and voter one allows one fetch per peer.
// The fetch fails at its deadline and holds the slot; a deadline later voter one closes the
// bulk lane, so the next fetch fails with not_connected rather than being refused for as long
// as the lane stays open. A local stop() fails a fetch it leaves pending. Once voter two
// resumes and voter one restarts, a fetch returns the bytes.
asio::awaitable<void> fetch_hung_peer(asio::io_context &io, const Certificates &certificates) {
	Cluster cluster(io, certificates);
	cluster.aside = 1;
	auto files = serving(cluster, io);
	(*files[1])["history/table"] = pattern(1024 * 1024, 30);
	cluster.options[0].fetches = 1;
	cluster.options[0].fetch_deadline = 300ms;
	for (unsigned i = 0; i != 3; ++i)
		cluster.start(i);
	CHECK(co_await cluster.commit("before", {0, 1, 2}, 5s));
	CHECK(co_await cluster.fully_connected(3s));

	// Voter two hangs before the request reaches it, so no part of a reply is on the wire (a
	// partial frame would meet the frame deadline instead).
	cluster.frozen = true;
	auto stalled = fetch(cluster, 0, 2, "history/table");
	CHECK(co_await cluster.until([&] { return stalled->done; }, 2s));
	CHECK(cluster.frozen && stalled->error == std::errc::timed_out);
	auto refused = fetch(cluster, 0, 2, "history/table");
	CHECK(co_await cluster.until([&] { return refused->done; }, 1s));
	CHECK(refused->error == std::errc::resource_unavailable_try_again); // within the second deadline
	CHECK(co_await cluster.until([&] { return cluster.peers[0]->occupancy().fetches == 0; }, 2s));
	const auto recycled = ms(Clock::now() - stalled->asked);
	auto later = fetch(cluster, 0, 2, "history/table");
	CHECK(co_await cluster.until([&] { return later->done; }, 1s));
	std::printf("  hung peer: fetch failed at %lld ms, lane closed at %lld ms (deadline 300 ms); later fetch: %s\n",
				ms(stalled->answered - stalled->asked), recycled, later->error.message().c_str());
	CHECK(later->error == std::errc::not_connected || later->error == std::errc::connection_aborted);
	CHECK(recycled >= 600);

	auto pending = fetch(cluster, 0, 3, "history/table");
	cluster.stop(0);
	CHECK(co_await cluster.until([&] { return pending->done && cluster.retired.back()->drained(); }, 2s));
	CHECK(pending->error == std::errc::connection_aborted && !pending->bytes);

	cluster.frozen = false;
	cluster.start(0);
	CHECK(co_await cluster.fully_connected(5s));
	auto resumed = fetch(cluster, 0, 2, "history/table");
	CHECK(co_await cluster.until([&] { return resumed->done; }, 3s));
	CHECK(!resumed->error && resumed->bytes == (*files[1])["history/table"]);
	CHECK(co_await cluster.stop_all(2s));
}
} // namespace

static_assert(PeerHost<MinimalHost> && PeerHost<Node>);
template class kronuz::consensus::transport::Peers<MinimalHost>;

int main(int argc, char **argv) {
	std::signal(SIGPIPE, SIG_IGN);
	const std::string only = argc > 1 ? argv[1] : "";
	Certificates certificates(std::filesystem::current_path() / ".scratch" /
							  ("peers-dns-certificates-" + std::to_string(::getpid())));
	struct Cleanup {
		std::filesystem::path path;
		~Cleanup() { std::filesystem::remove_all(path); }
	} cleanup{certificates.directory};
	const std::pair<const char *, Scenario> scenarios[] = {
		{"dual-stack", dual_stack},
		{"names-and-moves", names_and_moves}, {"system-localhost", system_localhost},
		{"hung-resolver", hung_resolver},     {"outage", outage},
		{"wrong-node", wrong_node},           {"several-addresses", several_addresses},
		{"refused-first", refused_first},     {"slow-resolver", slow_resolver},
		{"lookup-spacing", lookup_spacing},   {"membership", membership},
		{"fetch-exact", fetch_exact},         {"fetch-alongside-consensus", fetch_alongside_consensus},
		{"fetch-failures", fetch_failures},   {"fetch-hung-peer", fetch_hung_peer},
	};
	for (const auto &[name, scenario] : scenarios)
		if (only.empty() || only == name)
			run(name, certificates, scenario);
	std::printf("peers: ok\n");
	return EXIT_SUCCESS;
}
