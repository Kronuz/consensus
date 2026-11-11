// Peers over real TLS sockets between three consensus Workers, with voters named by host:
// lookups through an injected resolver (answering, failing, throwing or hanging on cue) and
// through the system resolver, the resolve deadline and the cache, staggered dials over
// several addresses, and the certificate pin refusing a name that reaches the wrong voter
// (docs/mechanisms/peer-dns.md). Peers is also instantiated over a minimal host.
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

// A resolver scripted per name. Hung calls park until release(); `active` counts the calls
// still inside, so a scenario can wait for every lookup thread to return before it ends.
struct FakeDns {
	enum class Mode { Answer, Empty, Throw, Hang };
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
	explicit Node(NodeId local)
		: 
		  worker_(io, fixed(local), {{512 * 1024, 4096}, {16 * 1024, 3}, {128 * 1024, 4}, 8, 3}, limits(), compact,
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
// resolvers, and may stop one voter's Peers and start another for the same Worker.
struct Cluster {
	asio::io_context &io;
	const Certificates &certificates;
	std::array<std::shared_ptr<Node>, 3> nodes;
	std::array<std::shared_ptr<Peers<Node>>, 3> peers;
	std::array<PeerOptions, 3> options;
	std::array<bool, 3> running{};
	std::vector<std::shared_ptr<Peers<Node>>> retired;
	Clock::time_point origin = Clock::now();
	RequestId request = 0;

	Cluster(asio::io_context &context, const Certificates &certs) : io(context), certificates(certs) {
		for (unsigned i = 0; i != 3; ++i) {
			nodes[i] = std::make_shared<Node>(i + 1);
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
		peers[i] = std::make_shared<Peers<Node>>(io.get_executor(), nodes[i], configuration, options[i]);
		peers[i]->start();
		running[i] = true;
	}
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
		for (unsigned i = 0; i != 3; ++i) {
			auto &node = *nodes[i];
			if (running[i])
				peers[i]->admit();
			const Tick tick{now, 100 + i * 40};
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
			if (nodes[i]->worker().role() == Role::Leader)
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
	CHECK(cluster.connections(0) == 4 && cluster.connections(1) == 4 && cluster.connections(2) == 4);
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
	CHECK(cluster.connections(0) == 4 && cluster.connections(1) == 4 && cluster.connections(2) == 4);
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

	// Voter one has "three" cached; its next lookup hangs, and voter three restarts.
	dns[0]->set("three", FakeDns::Mode::Hang);
	const auto timeouts = cluster.peers[0]->stats().lookup_timeouts;
	cluster.restart(2);
	const auto bounced = Clock::now();
	CHECK(co_await cluster.until([&] { return cluster.connections(2) == 4; }, 3s));
	const auto redial = ms(Clock::now() - bounced);
	std::printf("  hung lookup of a cached name: redialed in %lld ms (resolve deadline 200 ms)\n", redial);
	CHECK(redial < 200 + 800);
	CHECK(cluster.peers[0]->stats().lookup_timeouts > timeouts);
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
	CHECK(co_await cluster.until([&] { return cluster.connections(1) == 4; }, 3s));

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
	for (unsigned i = 0; i != 3; ++i)
		CHECK(cluster.connections(i) == 4 && cluster.peers[i]->stats().admitted == admitted[i]);

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

	// "two" now answers both loopback addresses; voter two restarts and is redialed.
	dns->set("two", FakeDns::Mode::Answer, {loopback, other});
	cluster.restart(1);
	CHECK(co_await cluster.until([&] { return cluster.connections(1) == 2; }, 4s));
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
		{"names-and-moves", names_and_moves}, {"system-localhost", system_localhost},
		{"hung-resolver", hung_resolver},     {"outage", outage},
		{"wrong-node", wrong_node},           {"several-addresses", several_addresses},
	};
	for (const auto &[name, scenario] : scenarios)
		if (only.empty() || only == name)
			run(name, certificates, scenario);
	std::printf("peers: ok\n");
	return EXIT_SUCCESS;
}
