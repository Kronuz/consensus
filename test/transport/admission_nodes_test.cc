// Nodes admitted into and removed from running host transports (docs/mechanisms/admission.md),
// over loopback with the admission fixture, every loop on its own thread. Each case but `policy`
// runs under both trust policies, `ca` (the default: the cluster CA, pins recorded at first join)
// and `pins` (pre-approved pins only), with the same steps and checks:
//   policy   the cluster CA's verdicts on the fixture's certificates, and the directory's rules
//   join     a node admitted at runtime is dialed, completes the hello against its pin, and calls
//   refused  a node never admitted, and one presenting another certificate, refused by name
//   remove   a node removed and admitted again 8 times while calls flow between two other nodes:
//            its links close, its dials are refused (removed), and no other call is lost
//   epoch    TLS completes, the node is removed, then its hellos run: a dial, and an accept moved
//            to its owner loop, become no link (the removal epoch)
//   issue    token v2 and the DTJN0002 exchange: a node with a saved key joins (D150), the
//            asynchronous hook names it, and two joins and a retry with the key get one
//            certificate; a version 1 token, another cluster's, another CA key's, a wrong and a
//            missing secret, a node the hook refuses and an impostor are refused by name
//   renew    a renewed certificate (the same key) is the node's, its pin unchanged (A4)
//   expired  an expired certificate is refused by accept and dial (cluster CA)
//   reload   a runtime reload of the certificate and CA bundle closes the links whose chain no
//            longer verifies; a renewal under the new CA links again, pins unchanged (cluster CA)
//   boot     a hello's boot checked against the directory's admitted one at each end, fetching
//            when behind; the refusal's reason reaches the dialer (A5)
//   redial   a call within the redial spacing after a failed dial dials once the spacing passes
//   flood    the join pool on the transport port: a stalled joiner cut at the slot deadline, the
//            address's cap, the rate, and a node's redials unharmed by the flood (A3)
//   save     a joined node's files: the key owner-only, a failed write throws naming its file,
//            and a save killed at any file leaves every file whole
//   consensus_transport_admission_nodes_test <fixture dir> case [ca|pins]
#include "consensus/transport/admission_join.h"
#include "consensus/transport/host_transport.h"
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <csignal>
#include <openssl/pem.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>

using namespace kronuz::consensus::transport;
using namespace kronuz::consensus;
using namespace std::chrono_literals;
using tcp = asio::ip::tcp;
using steady = std::chrono::steady_clock;

namespace {
std::filesystem::path fixture;
bool by_ca = true;  // the trust policy under test
constexpr const char *prefix = "spiffe://kronuz.test/node/";

std::unique_ptr<X509, decltype(&X509_free)> certificate(const std::string &name) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file((fixture / (name + ".pem")).c_str(), "r"), BIO_free);
	std::unique_ptr<X509, decltype(&X509_free)> c(PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
	assert(c);
	return c;
}
std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key_of(const std::string &name) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file((fixture / (name + ".key")).c_str(), "r"), BIO_free);
	std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> k(PEM_read_bio_PrivateKey(input.get(), nullptr, nullptr, nullptr),
														  EVP_PKEY_free);
	assert(k);
	return k;
}
Fingerprint pin_of(const std::string &name) { return key_pin(certificate(name).get()); }
std::shared_ptr<asio::ssl::context> context(const std::string &name, const std::string &ca = "ca") {
	auto c = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	reactor::tls::configure(*c, {(fixture / (name + ".pem")).string(), (fixture / (name + ".key")).string(),
								 (fixture / (ca + ".pem")).string()},
							reactor::tls::Role::Peer);
	return c;
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
bool ok = true;
void check(bool value, const char *what) {
	std::printf("  %s: %s\n", what, value ? "ok" : "FAILED");
	std::fflush(stdout);
	ok &= value;
}

struct Loop {
	asio::io_context io;
	task::asio_scheduler scheduler{io.get_executor()};
	asio::executor_work_guard<asio::io_context::executor_type> work = asio::make_work_guard(io);
	std::thread thread;
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

// What a call saw, at its caller.
struct Seen {
	std::atomic<bool> done{false};
	rpc::Status status;
	std::string received;
	RpcCall call;
	steady::time_point started = steady::now(), finished;
};
constexpr rpc::MethodId echo = 7, hang = 8;  // 7 answers its request; 8 never answers

// One node: its certificate `name`, `L` loops, the trust policy under test, and no peers until
// the test admits them.
struct Host {
	NodeId node;
	std::string name;
	std::vector<std::unique_ptr<Loop>> loops;
	tcp::acceptor acceptor;
	std::optional<tcp::acceptor> v6;  // a second listener, on ::1: another source on loopback
	std::shared_ptr<asio::ssl::context> tls;
	std::optional<NodePins> pins;
	std::unique_ptr<HostTransport> transport;
	bool running = false;
	Host(NodeId n, std::string certificate, std::uint32_t L, JoinHook join = {}, JoinOptions joins = {},
		 std::function<void(HostTransportOptions &)> tune = {})
		: node(n), name(std::move(certificate)), loops(make(L)), acceptor(loops[0]->io, {asio::ip::address_v4::loopback(), 0}) {
		tls = context(name);
		const std::map<NodeId, Fingerprint> own{{node, pin_of(name)}};
		if (by_ca)
			pins.emplace(node, own, tls->native_handle());  // the default: the cluster CA (D149)
		else
			pins.emplace(node, own, tls->native_handle(), pre_approved_pins());
		HostTransportOptions options;
		options.member_of = [](GroupId, NodeId) -> std::optional<Member> { return std::nullopt; };
		options.open_of = [](GroupId) {  // no group is stored here; the open is required, never sent
			GroupOpen o;
			o.cluster.fill('T');
			o.group.fill('G');
			o.epoch.fill('E');
			return o;
		};
		options.links.connection.handshake = 2000ms;
		options.redial = 50ms;
		options.join = std::move(join);
		options.joins = joins;
		if (tune)
			tune(options);
		std::vector<HostLoop> at;
		for (auto &l : loops)
			at.push_back({l->io.get_executor(), task::scheduler_ref::of(l->scheduler)});
		Identity cluster{};
		cluster.fill('K');
		transport = std::make_unique<HostTransport>(at, tls, *pins, cluster, std::map<NodeId, PeerAddress>{}, options);
		for (auto method : {echo, hang})
			transport->handle(method, [this, method](RpcCall c, const rpc::Incoming &) -> std::optional<rpc::Events> {
				auto body = std::make_shared<std::string>();
				rpc::Events e;
				e.body = [this, c, method, body](std::string_view bytes, bool end) {
					body->append(bytes);
					transport->consume(c, bytes.size());
					if (end && method == echo) {
						transport->total(c, body->size());
						transport->write(c, *body, true);
					}
				};
				e.done = [](const rpc::Status &) {};
				return e;
			});
	}
	static std::vector<std::unique_ptr<Loop>> make(std::uint32_t L) {
		std::vector<std::unique_ptr<Loop>> loops;
		for (std::uint32_t k = 0; k != L; ++k)
			loops.push_back(std::make_unique<Loop>());
		return loops;
	}
	PeerAddress address() const { return {"127.0.0.1", acceptor.local_endpoint().port()}; }
	tcp::endpoint endpoint6() const { return {asio::ip::address_v6::loopback(), v6->local_endpoint().port()}; }
	void start() {
		transport->serve(acceptor);
		if (v6)
			transport->serve(*v6);
		for (auto &l : loops)
			l->thread = std::thread([l = l.get()] { l->io.run(); });
		running = true;
	}
	template <class F> auto at(NodeId peer, F f) { return loops[transport->loop_of_node(peer)]->at(std::move(f)); }
	// Admits `peer` as the policy under test has it: its pin approved beforehand (pins), or
	// recorded at its first join (ca).
	void admit(const Host &peer, std::optional<PeerAddress> address = std::nullopt) {
		const auto where = address.value_or(peer.address());
		const auto pin = by_ca ? std::nullopt : std::optional(pin_of(peer.name));
		at(peer.node, [&] {
			transport->admit_node(peer.node, where, pin);
			return 0;
		});
	}
	void remove(NodeId peer) {
		at(peer, [&] {
			transport->remove_node(peer);
			return 0;
		});
	}
	std::shared_ptr<Seen> call(NodeId to, rpc::MethodId method, std::string request, std::chrono::milliseconds deadline) {
		auto seen = std::make_shared<Seen>();
		at(to, [&] {
			rpc::Events e;
			e.body = [this, seen](std::string_view bytes, bool) {
				seen->received.append(bytes);
				transport->consume(seen->call, bytes.size());
			};
			e.done = [seen](const rpc::Status &s) {
				seen->status = s;
				seen->finished = steady::now();
				seen->done = true;
			};
			const auto n = request.size();
			seen->call = transport->call(to, method, {.deadline = rpc::Clock::now() + deadline, .total = n}, std::move(request), e);
			return 0;
		});
		return seen;
	}
	// A completed echo of `bytes` from `to`.
	bool echoes(NodeId to, std::chrono::milliseconds deadline = 5000ms) {
		const std::string request = "from " + std::to_string(node) + " to " + std::to_string(to);
		auto seen = call(to, echo, request, deadline);
		return until([&] { return seen->done.load(); }, deadline + 2000ms) && seen->status.outcome == rpc::Outcome::ok &&
			   seen->received == request;
	}
	std::uint64_t established() const {
		std::uint64_t sum = 0;
		for (std::uint32_t k = 0; k != loops.size(); ++k)
			sum += transport->link_stats(k).established;
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
// Admits each host into the other.
void pair(Host &a, Host &b) {
	a.admit(b);
	b.admit(a);
}

void watchdog(std::chrono::seconds limit) {
	std::thread([limit] {
		std::this_thread::sleep_for(limit);
		std::printf("HUNG after %llds\n", (long long)limit.count());
		std::fflush(stdout);
		std::_Exit(3);
	}).detach();
}

// The cluster CA's verdict on each fixture certificate, then the directory's rules.
void policy_case() {
	const ClusterCa ca(certificate("ca").get(), prefix);
	struct Want {
		const char *name;
		NodeId node;
		PinFault fault;
	} wants[] = {{"n1", 1, PinFault::cluster},      {"n3", 3, PinFault::cluster},       {"n3b", 3, PinFault::cluster},
				 {"other", 0, PinFault::cluster},   {"bare", 0, PinFault::cluster},     {"twice", 0, PinFault::cluster},
				 {"padded", 0, PinFault::cluster},  {"rogue3", 0, PinFault::unpinned}};
	for (const auto &w : wants) {
		const auto got = ca(certificate(w.name).get(), nullptr);
		std::printf("  %-7s node %llu %s\n", w.name, (unsigned long long)got.node, got.node ? "" : pin_fault_name(got.fault));
		check(got.node == w.node && (got.node || got.fault == w.fault), w.name);
	}
	check(!pre_approved_pins(), "pre-approved pins are no policy: only pins admit");

	auto tls = context("n1");
	const ClusterCa derived(tls->native_handle(), 1);
	check(derived.prefix() == prefix && derived(certificate("n2").get(), nullptr).node == 2,
		  "the default takes its CA from the context and its prefix from the local certificate");
	auto bare = context("bare");
	const ClusterCa nameless(bare->native_handle(), 4);
	const auto none = nameless(certificate("n2").get(), nullptr);
	check(nameless.prefix().empty() && !none.node && none.fault == PinFault::unpinned,
		  "a local certificate naming no node leaves only pins admitting (unpinned)");
	NodePins pins(1, {{1, pin_of("n1")}}, tls->native_handle(), pre_approved_pins());
	auto copy = pins;
	auto throws = [](auto f, std::optional<PinFault> fault) {
		try {
			f();
		} catch (const PinRefused &e) {
			return fault && e.fault == *fault;
		} catch (const std::invalid_argument &) {
			return !fault;
		}
		return false;
	};
	pins.admit(2, pin_of("n2"));
	check(copy.contains(2) && copy.pin(2) == pin_of("n2"), "an admission through one copy holds for every copy");
	check(throws([&] { pins.admit(2, pin_of("n3")); }, PinFault::repinned), "a pinned node takes no other pin (repinned)");
	check(throws([&] { pins.admit(3, pin_of("n2")); }, std::nullopt), "a pin is one node's");
	check(throws([&] { pins.remove(1); }, std::nullopt), "the local node is never removed");
	const auto before = pins.epoch();
	check(pins.current(2, before), "a pinned node is current");
	pins.remove(2);
	check(pins.epoch() == before + 1 && !pins.contains(2) && !pins.current(2, before), "a removal raises the epoch");
	check(throws([&] { pins.admit(3, pin_of("n2")); }, std::nullopt), "a removed node's pin is still its own");
	pins.admit(2, pin_of("n3b"));
	check(pins.pin(2) == pin_of("n3b") && pins.current(2, pins.epoch()) && !pins.current(2, before),
		  "admitted again, a removed node may take another pin; a connection begun before the removal is not current");
	pins.admit(4, pin_of("n2"));
	check(pins.pin(4) == pin_of("n2"), "the old pin is free once its removed node took another");
	pins.remove(4);
	pins.admit(4);
	check(!pins.contains(4), "admitted again with no pin, a removed node waits for its first join");
}

void join_case() {
	Host one(1, "n1", 2), two(2, "n2", 2), three(3, "n3", 4);
	one.start();
	two.start();
	three.start();
	auto c = one.call(3, echo, "early", 1000ms);
	check(until([&] { return c->done.load(); }, 2000ms) && c->status.outcome == rpc::Outcome::not_connected,
		  "a node not admitted has no address: not_connected at once");
	pair(one, two);
	check(one.echoes(2) && two.echoes(1), "two nodes admitted at runtime call each other");
	const auto dialed = one.established();
	pair(one, three);
	check(one.echoes(3), "a third node admitted at runtime is dialed and answers");
	check(one.established() > dialed, "its link was established by this node's dial");
	check(three.echoes(1), "and calls back");
	check(one.pins->pin(3) == pin_of("n3") && three.pins->pin(1) == pin_of("n1"),
		  by_ca ? "each recorded the other's pin at its first join" : "each holds the other's pre-approved pin");
	one.stop();
	two.stop();
	three.stop();
}

void refused_case() {
	Host one(1, "n1", 2), three(3, "n3", 2), four(4, "bare", 2), rekeyed(3, "n3b", 2);
	for (auto *h : {&one, &three, &four, &rekeyed})
		h->start();
	pair(one, three);
	check(one.echoes(3), "node 3 joins");

	// Node 4's certificate names no node: node 1 never admitted it.
	const auto never = by_ca ? PinFault::cluster : PinFault::unpinned;
	const auto before = one.pins->refused(never);
	four.admit(one);
	auto c = four.call(1, echo, "let me in", 1500ms);
	check(until([&] { return c->done.load(); }, 4000ms) && c->status.outcome != rpc::Outcome::ok, "a node never admitted does not call");
	std::printf("  refused %s: %llu\n", pin_fault_name(never), (unsigned long long)(one.pins->refused(never) - before));
	check(one.pins->refused(never) > before, by_ca ? "its accept is refused: cluster" : "its accept is refused: unpinned");

	// Node 3 again on another address with another certificate: node 1 pinned its first. The
	// first node 3 stops, so no link of it carries the calls.
	three.stop();
	std::this_thread::sleep_for(300ms);
	const auto other = by_ca ? PinFault::repinned : PinFault::unpinned;
	const auto rekeyed_before = one.pins->refused(other);
	one.at(3, [&] {
		one.transport->admit_node(3, rekeyed.address());  // its address moves; its pin stays
		return 0;
	});
	auto d = one.call(3, echo, "who are you", 1500ms);
	check(until([&] { return d->done.load(); }, 4000ms) && d->status.outcome != rpc::Outcome::ok,
		  "a dial to a node presenting another certificate fails");
	rekeyed.admit(one);
	auto e = rekeyed.call(1, echo, "it is me", 1500ms);
	check(until([&] { return e->done.load(); }, 4000ms) && e->status.outcome != rpc::Outcome::ok,
		  "its own dial with that certificate fails");
	std::printf("  refused %s: %llu\n", pin_fault_name(other), (unsigned long long)(one.pins->refused(other) - rekeyed_before));
	check(one.pins->refused(other) >= rekeyed_before + 2, by_ca ? "both refused: repinned" : "both refused: unpinned");
	check(one.pins->pin(3) == pin_of("n3"), "node 3 keeps its first pin");
	Host back(3, "n3", 2);
	back.start();
	back.admit(one);
	one.at(3, [&] {
		one.transport->admit_node(3, back.address());
		return 0;
	});
	check(one.echoes(3), "with its first certificate again, node 3 answers");
	for (auto *h : {&one, &back, &four, &rekeyed})
		h->stop();
}

// A renewed certificate (n3r: node 3's key, a new certificate) is node 3's with no operator step:
// its pin is the key's. A new key under node 3's name (n3b) is refused in refused_case.
void renew_case() {
	Host one(1, "n1", 2), three(3, "n3", 2);
	one.start();
	three.start();
	pair(one, three);
	check(one.echoes(3) && one.pins->pin(3) == pin_of("n3"), "node 3 joins");
	three.stop();
	std::this_thread::sleep_for(300ms);
	check(pin_of("n3r") == pin_of("n3") && certificate_fingerprint(certificate("n3r").get()) !=
												   certificate_fingerprint(certificate("n3").get()),
		  "the renewed certificate is another certificate for the same key");
	Host renewed(3, "n3r", 2);
	renewed.start();
	renewed.admit(one);
	one.at(3, [&] {
		one.transport->admit_node(3, renewed.address());  // its address moves; nothing else changes
		return 0;
	});
	check(one.echoes(3) && renewed.echoes(1), "node 3 with its renewed certificate calls and answers");
	check(one.pins->pin(3) == pin_of("n3") && !one.pins->refused(PinFault::repinned) && !one.pins->refused(PinFault::unpinned),
		  "its pin is unchanged, and nothing was refused");
	one.stop();
	renewed.stop();
}

// A certificate that expired an hour ago, for `key`'s key, signed by the fixture's CA.
std::string expired_certificate(const std::string &key, NodeId node) {
	const auto ca = certificate("ca");
	auto ca_key = key_of("ca"), subject = key_of(key);
	std::unique_ptr<X509, decltype(&X509_free)> c(X509_new(), X509_free);
	const std::string uri = "URI:" + std::string(prefix) + std::to_string(node);
	bool ok = X509_set_version(c.get(), 2) == 1 && ASN1_INTEGER_set(X509_get_serialNumber(c.get()), 77) == 1 &&
			  X509_set_subject_name(c.get(), X509_get_subject_name(ca.get())) == 1 &&
			  X509_set_issuer_name(c.get(), X509_get_subject_name(ca.get())) == 1 &&
			  X509_gmtime_adj(X509_getm_notBefore(c.get()), -7200) && X509_gmtime_adj(X509_getm_notAfter(c.get()), -3600) &&
			  X509_set_pubkey(c.get(), subject.get()) == 1;
	X509V3_CTX v3;
	X509V3_set_ctx(&v3, ca.get(), c.get(), nullptr, nullptr, 0);
	for (const auto &[nid, value] : {std::pair{NID_basic_constraints, "critical,CA:FALSE"},
									 {NID_ext_key_usage, "serverAuth,clientAuth"},
									 {NID_subject_alt_name, uri.c_str()}}) {
		X509_EXTENSION *extension = ok ? X509V3_EXT_conf_nid(nullptr, &v3, nid, value) : nullptr;
		ok = extension && X509_add_ext(c.get(), extension, -1) == 1;
		X509_EXTENSION_free(extension);
	}
	assert(ok && X509_sign(c.get(), ca_key.get(), EVP_sha256()) > 0);
	const auto name = "expired" + std::to_string(node);
	std::unique_ptr<BIO, decltype(&BIO_free)> out(BIO_new_file((fixture / (name + ".pem")).c_str(), "w"), BIO_free);
	PEM_write_bio_X509(out.get(), c.get());
	std::filesystem::copy_file(fixture / (key + ".key"), fixture / (name + ".key"), std::filesystem::copy_options::overwrite_existing);
	return name;
}

// A call that arrives within the redial spacing after a failed dial, with nothing else waiting,
// dials once the spacing passes, not at its deadline: a peer whose certificate was just renewed
// is redialed. Node 9's address accepts and stays silent, so each dial fails at the handshake
// deadline (300 ms); the spacing is 1500 ms.
void redial_case() {
	Loop silent;
	tcp::acceptor listener(silent.io, {asio::ip::address_v4::loopback(), 0});
	std::vector<tcp::socket> held;
	std::atomic<int> dials{0};
	std::function<void()> next = [&] {
		listener.async_accept([&](asio::error_code error, tcp::socket socket) {
			if (error)
				return;
			held.push_back(std::move(socket));
			++dials;
			next();
		});
	};
	next();
	silent.thread = std::thread([&] { silent.io.run(); });
	Host one(1, "n1", 2, {}, {}, [](HostTransportOptions &o) {
		o.redial = 1500ms;
		o.links.connection.handshake = 300ms;
	});
	one.start();
	const PeerAddress nine{"127.0.0.1", listener.local_endpoint().port()};
	one.at(9, [&] {
		one.transport->admit_node(9, nine, std::nullopt);
		return 0;
	});
	auto first = one.call(9, echo, "first", 100ms);
	check(until([&] { return dials.load() == 1 && first->done.load(); }, 2000ms), "the first call dials once and ends");
	std::this_thread::sleep_for(700ms);  // past the dial's handshake deadline: its failure saw no call waiting
	const auto asked = steady::now();
	auto second = one.call(9, echo, "second", 6000ms);
	const bool redialed = until([&] { return dials.load() >= 2; }, 3000ms);
	std::printf("  the second call dialed %lld ms after it began\n",
				(long long)std::chrono::duration_cast<std::chrono::milliseconds>(steady::now() - asked).count());
	check(redialed && !second->done.load(), "a call within the redial spacing dials when the spacing passes, before its deadline");
	one.stop();
	silent.at([&] {
		listener.close();
		held.clear();
		return 0;
	});
	silent.join();
}

// The boot check (A5, M7): node 1 admits a peer's hello only for the boot its directory admitted,
// fetching first when the hello names a newer directory version; the refusal names the reason.
// Node 2 starts as a copy (boot C, while the directory admitted B): node 1 refuses it both ways,
// and node 2 hears why; node 2 as boot B links. Node 3's hello names directory version 8 while
// node 1 holds 5: node 1 fetches, finds boot D admitted, and links.
constexpr std::uint32_t duplicate = 1;
void boot_case() {
	const auto boot = [](char c) {
		Identity id{};
		id.fill(c);
		return id;
	};
	std::mutex directory_mutex;
	std::uint64_t version = 5;
	std::map<NodeId, Identity> admitted{{2, boot('B')}};
	std::atomic<int> fetches{0};
	const auto admits = [&](const NodeHello &peer, task::scheduler_ref loop) -> task::task<std::uint32_t> {
		bool behind = false;
		{
			std::lock_guard hold(directory_mutex);
			behind = peer.directory > version;
		}
		if (behind) {  // the fetch from the root, then the directory at the hello's version
			++fetches;
			late_deadline fetch(loop, 30ms);
			co_await fetch.wait();
			std::lock_guard hold(directory_mutex);
			version = peer.directory;
			admitted[3] = boot('D');
		}
		std::lock_guard hold(directory_mutex);
		const auto it = admitted.find(peer.node);
		co_return it != admitted.end() && it->second == peer.boot ? 0 : duplicate;
	};
	std::mutex heard_mutex;
	std::vector<std::pair<NodeId, std::uint32_t>> heard;
	Host one(1, "n1", 2, {}, {}, [&](HostTransportOptions &o) {
		o.links.boot = boot('A');
		o.links.directory = 5;
		o.links.check_boot = admits;
	});
	Host two(2, "n2", 2, {}, {}, [&](HostTransportOptions &o) {
		o.links.boot = boot('C');
		o.links.directory = 5;
		o.links.boot_refused = [&](NodeId peer, std::uint32_t reason) {
			std::lock_guard hold(heard_mutex);
			heard.emplace_back(peer, reason);
		};
	});
	one.start();
	two.start();
	pair(one, two);
	auto c = two.call(1, echo, "a copy", 1500ms);
	auto d = one.call(2, echo, "a copy", 1500ms);
	check(until([&] { return c->done.load() && d->done.load(); }, 4000ms) && c->status.outcome != rpc::Outcome::ok &&
			  d->status.outcome != rpc::Outcome::ok,
		  "a boot the directory did not admit neither calls nor answers");
	const auto refused_here = [&] {
		std::uint64_t sum = 0;
		for (std::uint32_t k = 0; k != one.loops.size(); ++k)
			sum += one.loops[k]->at([&] { return one.transport->link_stats(k).boot_refused; });
		return sum;
	};
	bool named = false;
	{
		std::lock_guard hold(heard_mutex);
		for (auto [peer, reason] : heard)
			named |= peer == 1 && reason == duplicate;
	}
	std::printf("  node 1 refused %llu hellos; node 2 heard %zu refusals\n", (unsigned long long)refused_here(), heard.size());
	// Node 2 has no check of its own: it starts node 1's dial at its reply, which node 1 then
	// closes on reading node 2's boot.
	check(!one.established(), "node 1 links to node 2 neither way");
	check(named, "node 2 hears node 1's refusal and its reason: duplicate");
	two.transport->boot(boot('B'), 5);
	check(two.echoes(1, 8000ms) && one.echoes(2), "as the admitted boot, node 2 links and calls");

	Host three(3, "n3", 2, {}, {}, [&](HostTransportOptions &o) {
		o.links.boot = boot('D');
		o.links.directory = 8;
	});
	three.start();
	pair(one, three);
	check(three.echoes(1, 8000ms) && fetches >= 1, "a hello naming a newer directory is checked after the fetch, and links");
	one.stop();
	two.stop();
	three.stop();
}

// An expired certificate is refused on the shared port, by accept and by dial, before any hello.
void expired_case() {
	Host one(1, "n1", 2), four(4, expired_certificate("n4", 4), 2);
	one.start();
	four.start();
	pair(one, four);
	const auto before = one.pins->refused(PinFault::unverified);
	auto in = four.call(1, echo, "expired", 1500ms);
	auto out = one.call(4, echo, "expired", 1500ms);
	check(until([&] { return in->done.load() && out->done.load(); }, 4000ms) && in->status.outcome != rpc::Outcome::ok &&
			  out->status.outcome != rpc::Outcome::ok,
		  "an expired certificate neither calls nor answers");
	std::printf("  established %llu, refused unverified %llu\n", (unsigned long long)one.established(),
				(unsigned long long)(one.pins->refused(PinFault::unverified) - before));
	check(!one.established() && !one.pins->pin(4), "no link, and no pin recorded");
	one.stop();
	four.stop();
}

// Reloading at runtime: node 1 trusts only ca2, with its own key's ca2 certificate (n1c2), and its
// link to node 2 (ca's) closes and stays closed; node 2 renewed under ca2 (n2c2) links again, both
// pins unchanged. A certificate of another key is refused.
void reload_case() {
	Host one(1, "n1", 2), two(2, "n2", 2);
	one.start();
	two.start();
	pair(one, two);
	check(one.echoes(2) && two.echoes(1), "nodes 1 and 2 link under the first CA");
	bool refused = false;
	try {
		one.transport->reload(context("n1k", "ca2"));
	} catch (const std::invalid_argument &e) {
		std::printf("  %s\n", e.what());
		refused = true;
	}
	check(refused, "a certificate of another key is refused");
	one.transport->reload(context("n1c2", "ca2"));
	const auto unverified = [&] {
		std::uint64_t sum = 0;
		for (std::uint32_t k = 0; k != one.loops.size(); ++k)
			sum += one.loops[k]->at([&] { return one.transport->link_stats(k).unverified; });
		return sum;
	};
	check(until([&] { return unverified() >= 1; }, 2000ms), "the reload closes node 2's links: their chain no longer verifies");
	auto c = one.call(2, echo, "after the reload", 1500ms);
	auto d = two.call(1, echo, "after the reload", 1500ms);
	check(until([&] { return c->done.load() && d->done.load(); }, 4000ms) && c->status.outcome != rpc::Outcome::ok &&
			  d->status.outcome != rpc::Outcome::ok,
		  "and neither node calls the other");
	two.transport->reload(context("n2c2", "ca2"));
	const bool relinked = one.echoes(2, 8000ms) && two.echoes(1, 8000ms);
	if (!relinked)
		for (auto *h : {&one, &two})
			for (std::uint32_t k = 0; k != h->loops.size(); ++k) {
				const auto st = h->loops[k]->at([&] { return h->transport->link_stats(k); });
				std::printf("  node %llu loop %u: established %llu refused %llu abandoned %llu rejected %llu unverified %llu; "
							"pins refused unverified %llu unpinned %llu\n",
							(unsigned long long)h->node, k, (unsigned long long)st.established, (unsigned long long)st.refused,
							(unsigned long long)st.abandoned, (unsigned long long)st.rejected,
							(unsigned long long)st.unverified, (unsigned long long)h->pins->refused(PinFault::unverified),
							(unsigned long long)h->pins->refused(PinFault::unpinned));
			}
	check(relinked, "node 2 renewed under the second CA links again");
	check(one.pins->pin(2) == pin_of("n2") && two.pins->pin(1) == pin_of("n1"), "both pins unchanged");
	one.stop();
	two.stop();
}

// The loop count for node 1 under which nodes 2 and 3 share a loop: removing 3 runs beside 2's links.
std::uint32_t shared_loop() {
	for (std::uint32_t L = 2; L != 16; ++L) {
		using namespace kronuz::journal::host_detail;
		if (loop(slot(2), L) == loop(slot(3), L))
			return L;
	}
	return 1;
}

void remove_case() {
	const auto L = shared_loop();
	Host one(1, "n1", L), two(2, "n2", 4), three(3, "n3", 4);
	std::printf("  node 1 runs %u loops; nodes 2 and 3 share loop %u\n", L, one.transport->loop_of_node(2));
	for (auto *h : {&one, &two, &three})
		h->start();
	pair(one, two);
	pair(one, three);
	check(one.echoes(2) && one.echoes(3) && three.echoes(1), "three nodes call each other");

	// Calls between 1 and 2, both ways, while node 3 is removed and admitted again.
	std::atomic<bool> stopping{false};
	std::mutex mutex;
	std::vector<std::shared_ptr<Seen>> flowing;
	std::uint64_t made = 0;
	std::thread traffic([&] {
		for (std::uint64_t i = 0; !stopping; ++i) {
			auto &from = i % 2 ? two : one;
			const std::string request = "call " + std::to_string(i);
			auto seen = from.call(i % 2 ? 1 : 2, echo, request, 10000ms);
			std::lock_guard hold(mutex);
			flowing.push_back(seen);
			++made;
			if (flowing.size() >= 64) {
				std::this_thread::sleep_for(1ms);
				std::erase_if(flowing, [](const auto &s) { return s->done && s->status.outcome == rpc::Outcome::ok; });
			}
		}
	});

	bool closed = true, refused = true, dial_refused = true, back = true;
	constexpr int cycles = 8;
	for (int cycle = 0; cycle != cycles; ++cycle) {
		auto out = one.call(3, hang, "out", 30000ms), in = three.call(1, hang, "in", 30000ms);
		std::this_thread::sleep_for(50ms);
		const auto before = one.pins->refused(PinFault::removed);
		const auto at = steady::now();
		one.remove(3);
		closed &= until([&] { return out->done && in->done; }, 3000ms) && out->status.outcome != rpc::Outcome::ok &&
				  in->status.outcome != rpc::Outcome::ok;
		const auto took = std::chrono::duration_cast<std::chrono::milliseconds>(steady::now() - at).count();
		auto again = three.call(1, echo, "again", 1000ms);
		refused &= until([&] { return again->done.load(); }, 3000ms) && again->status.outcome != rpc::Outcome::ok &&
				   one.pins->refused(PinFault::removed) > before;
		auto dial = one.call(3, echo, "dial", 1000ms);
		dial_refused &= until([&] { return dial->done.load(); }, 500ms) && dial->status.outcome == rpc::Outcome::not_connected;
		one.admit(three);
		const bool answered = one.echoes(3) && three.echoes(1);
		back &= answered;
		std::printf("  cycle %d: its calls ended in %lld ms (%d, %d); refused removed %llu; admitted again: %s\n", cycle,
					(long long)took, int(out->status.outcome), int(in->status.outcome),
					(unsigned long long)(one.pins->refused(PinFault::removed) - before), answered ? "answers" : "silent");
	}
	check(closed, "each removal ends the removed node's calls both ways at once: its links closed");
	check(refused, "its later dials are refused: removed");
	check(dial_refused, "this node does not dial it: not_connected");
	check(back, "admitted again, it answers");

	stopping = true;
	traffic.join();
	std::vector<std::shared_ptr<Seen>> rest;
	{
		std::lock_guard hold(mutex);
		rest = flowing;
	}
	const bool finished = until([&] {
		return std::all_of(rest.begin(), rest.end(), [](const auto &s) { return s->done.load(); });
	}, 15000ms);
	std::uint64_t failed = 0;
	for (const auto &s : rest)
		failed += !s->done || s->status.outcome != rpc::Outcome::ok;
	std::printf("  calls between 1 and 2: %llu made, %llu not ok\n", (unsigned long long)made, (unsigned long long)failed);
	check(finished && !failed && made > 100, "no call between the other two nodes is lost");
	for (auto *h : {&one, &two, &three})
		h->stop();
}
// A loop the test thread steps, so the epoch case forces its interleavings instead of racing them.
struct Stepped {
	asio::io_context io;
	task::asio_scheduler scheduler{io.get_executor()};
	task::scheduler_ref ref() { return task::scheduler_ref::of(scheduler); }
	~Stepped() {
		io.stop();
		scheduler.close();
		scheduler.destroy_roots();
	}
};
// Steps `loops` in turn until `done`, within `limit`.
template <class P> bool step(std::initializer_list<Stepped *> loops, P done, std::chrono::milliseconds limit = 10000ms) {
	const auto start = steady::now();
	while (!done()) {
		if (steady::now() - start > limit)
			return false;
		for (auto *l : loops) {
			l->io.restart();
			l->io.run_for(1ms);
		}
	}
	return true;
}
// Runs `f` on `loop`, as the transport runs remove_node and stop.
template <class F> bool on(Stepped &loop, F f) {
	bool done = false;
	asio::post(loop.io, [&] {
		f();
		done = true;
	});
	return step({&loop}, [&] { return done; });
}
template <class T> struct Result {
	std::optional<T> value;
	void spawn(Stepped &loop, task::task<T> t) {
		task::spawn(loop.ref(), std::move(t), [this](std::exception_ptr e, std::optional<T> v) {
			assert(!e);
			value = std::move(v);
		});
	}
};
// One node's Links on a stepped loop, under the policy under test.
struct Node {
	NodeId node;
	std::string name;
	Stepped &loop;
	tcp::acceptor acceptor;
	std::shared_ptr<asio::ssl::context> tls = context(name);
	std::optional<NodePins> pins;
	std::unique_ptr<Links> links;
	std::uint64_t established = 0;
	Node(NodeId n, std::string certificate, Stepped &l)
		: node(n), name(std::move(certificate)), loop(l), acceptor(l.io, {asio::ip::address_v4::loopback(), 0}) {
		const std::map<NodeId, Fingerprint> own{{node, pin_of(name)}};
		if (by_ca)
			pins.emplace(node, own, tls->native_handle());
		else
			pins.emplace(node, own, tls->native_handle(), pre_approved_pins());
		links = open(l);
	}
	// A Links of this node on `at`, sharing its pins.
	std::unique_ptr<Links> open(Stepped &at) {
		Identity cluster{};
		cluster.fill('K');
		auto made = std::make_unique<Links>(at.io.get_executor(), at.ref(), tls, *pins, cluster, std::map<NodeId, PeerAddress>{});
		made->on_established = [this](const Link &) { ++established; };
		return made;
	}
	void admit(const Node &peer) {
		links->pins().admit(peer.node, by_ca ? std::nullopt : std::optional(pin_of(peer.name)));
		links->route(peer.node, {"127.0.0.1", peer.acceptor.local_endpoint().port()});
	}
};
// Stops each Links on its loop and steps the loops until they drain.
bool stop_all(std::initializer_list<std::pair<Links *, Stepped *>> links, std::initializer_list<Stepped *> loops) {
	for (auto [l, at] : links)
		on(*at, [l] { l->stop(); });
	return step(loops, [&] { return std::all_of(links.begin(), links.end(), [](auto l) { return l.first->drained(); }); });
}

// R5: a connection whose TLS passed before remove_node must not become a link once its hellos run.
void epoch_case() {
	{
		// The dial: node 1's TLS to node 3 completes, node 3 is removed, then node 1 offers its hello.
		Stepped loop;
		Node one(1, "n1", loop), three(3, "n3", loop);
		one.admit(three);
		three.admit(one);
		task::spawn(loop.ref(), three.links->serve(three.acceptor));
		Result<std::shared_ptr<Connection>> tls;
		tls.spawn(loop, one.links->connect(3, LinkLane::Control));
		check(step({&loop}, [&] { return tls.value.has_value(); }) && *tls.value, "dial: node 1's TLS to node 3 completes");
		on(loop, [&] { one.links->remove(3); });
		Result<bool> offered;
		offered.spawn(loop, one.links->offer(3, LinkLane::Control, std::move(*tls.value)));
		tls.value.reset();
		check(step({&loop}, [&] { return offered.value.has_value(); }) && !*offered.value && !one.established &&
				  !one.links->find(3, LinkLane::Control),
			  "dial: removed before its hellos, it becomes no link");
		check(step({&loop}, [&] { return !three.links->find(1, LinkLane::Control); }), "dial: node 3's end of it ends");
		check(stop_all({{one.links.get(), &loop}, {three.links.get(), &loop}}, {&loop}), "dial: drained");
	}
	{
		// The accept: node 1 accepts on loop a and moves node 3's connection to its owner on loop b.
		// Loop b waits while the move is posted; node 3 is removed; then b reads node 3's hello.
		Stepped a, b, c;
		Node one(1, "n1", a), three(3, "n3", c);
		auto home = one.open(b);
		one.links->owner = [&](NodeId) { return home.get(); };
		home->owner = [&](NodeId) { return home.get(); };
		one.admit(three);
		three.admit(one);
		task::spawn(a.ref(), one.links->serve(one.acceptor));
		Result<bool> dialed;
		dialed.spawn(c, three.links->dial(1, LinkLane::Control));
		// An empty owner drains once its moved accept arrives: until then it is not drained.
		check(step({&a, &c}, [&] { return !home->drained(); }), "accept: node 3 authenticated on loop a, moved to loop b");
		home->remove(3);  // on loop b's thread, ahead of the moved accept queued there
		check(step({&a, &b, &c}, [&] { return dialed.value.has_value(); }) && !*dialed.value && !one.established &&
				  !home->find(3, LinkLane::Control) && !one.links->find(3, LinkLane::Control),
			  "accept: removed before its hellos, it becomes no link");
		check(!three.links->find(1, LinkLane::Control), "accept: node 3 has no link either");
		check(stop_all({{one.links.get(), &a}, {home.get(), &b}, {three.links.get(), &c}}, {&a, &b, &c}), "accept: drained");
	}
}
// Starts `t` on `loop`; its future holds its value, or what it threw.
template <class T> std::future<T> start(Loop &loop, task::task<T> t) {
	auto result = std::make_shared<std::promise<T>>();
	auto future = result->get_future();
	asio::post(loop.io, [&loop, result, t = std::move(t)]() mutable {
		task::spawn(task::scheduler_ref::of(loop.scheduler), std::move(t), [result](std::exception_ptr e, std::optional<T> v) {
			if (e)
				result->set_exception(e);
			else
				result->set_value(std::move(*v));
		});
	});
	return future;
}
// Runs `t` on `loop` and waits for its value, or rethrows what it threw.
template <class T> T run(Loop &loop, task::task<T> t) { return start(loop, std::move(t)).get(); }
// A host presenting `tls`'s leaf that offers `ca` as a member does, then counts what the joiner sends.
task::task<std::size_t> impostor(tcp::acceptor &listener, std::shared_ptr<asio::ssl::context> tls, X509 *ca, task::scheduler_ref loop) {
	auto [error, socket] = co_await task::await_asio<void(asio::error_code, tcp::socket)>(
		[&](auto h) { listener.async_accept(std::move(h)); });
	if (error || !socket)
		co_return 0;
	join_detail::Stream stream(std::move(*socket), *tls);
	SSL_set_verify(stream.native_handle(), SSL_VERIFY_NONE, nullptr);
	join_detail::Bounded bound(loop, 3000ms, stream);
	std::size_t received = 0;
	try {
		(void)co_await join_detail::read_bytes(stream.next_layer(), join_magic.size());
		co_await join_detail::handshake(stream, asio::ssl::stream_base::server);
		Identity cluster{};
		cluster.fill('K');
		co_await join_detail::write_frame(stream, encode_join_offer({cluster, {join_detail::der(ca)}}));
		char buffer[512];
		auto [ended, n] = co_await task::await_asio<void(asio::error_code, std::size_t)>(
			[&](auto h) { stream.async_read_some(asio::buffer(buffer), std::move(h)); });
		received = ended ? 0 : n;
	} catch (...) {
	}
	co_return received;
}
template <class F> std::optional<JoinFault> fault_of(F f) {
	try {
		f();
	} catch (const JoinRefused &e) {
		std::printf("  %s\n", e.what());
		return e.fault;
	}
	return std::nullopt;
}

// Token v2 and the DTJN0002 exchange (A1, A2), on the member's transport port.
void issue_case() {
	const auto ca = certificate("ca");
	Identity cluster{};
	cluster.fill('K');  // Host's
	const auto token = fresh_join_token(cluster, ca.get());
	const auto text = encode_join_token(token);
	check(decode_join_token(text) == token && text.size() == join_token_size && join_token_size == 168,
		  "the token prints as 168 characters and reads back");
	const std::string v1 = "DTJT" + text.substr(8 + 32, 128);  // version 1's shape: DTJT, the CA and the secret in hex
	check(fault_of([&] { decode_join_token(v1); }) == JoinFault::version, "a version 1 token is refused: version");
	check(fault_of([&] { decode_join_token("DTJT0003" + text.substr(8)); }) == JoinFault::version,
		  "a token of another version is refused: version");
	check(fault_of([&] { decode_join_token(text.substr(1)); }) == JoinFault::malformed &&
			  fault_of([&] { decode_join_token(text.substr(0, 20) + "zz" + text.substr(22)); }) == JoinFault::malformed,
		  "a cut or non-hex token is refused: malformed");

	// The root's side: the secret's digest, a node per key (idempotent), asynchronous.
	std::optional<task::scheduler_ref> loop0;
	const auto digest = join_secret_digest(token.secret);
	std::mutex assigned_mutex;
	std::map<Fingerprint, NodeId> assigned;
	std::atomic<int> assigns{0};
	Issuer issuer(ca.get(), key_of("ca").get(), ClusterCa(context("n1")->native_handle(), 1).prefix(),
				  [&](const JoinRequest &r) -> task::task<Issuer::Assignment> {
					  ++assigns;
					  late_deadline relay(*loop0, 20ms);  // the relay to the root's leader
					  co_await relay.wait();
					  if (!join_secret_matches(digest, r.secret))
						  co_return Issuer::Assignment{0, JoinFault::token, {}};
					  if (r.requested == 1)
						  co_return Issuer::Assignment{0, JoinFault::refused, "name"};
					  std::lock_guard hold(assigned_mutex);
					  const auto [it, fresh] = assigned.try_emplace(r.key, NodeId(5 + assigned.size()));
					  co_return Issuer::Assignment{it->second, JoinFault::refused, {}};
				  });
	// Node 1 serves joins on its transport port, beside its nodes.
	Host one(1, "n1", 2, [&](JoinRequest r) { return issuer.issue(std::move(r)); });
	loop0 = task::scheduler_ref::of(one.loops[0]->scheduler);
	one.start();
	const auto &service = *one.transport->joins();
	const tcp::endpoint endpoint = one.acceptor.local_endpoint();

	Loop joiner;
	joiner.thread = std::thread([&] { joiner.io.run(); });
	const auto key = fresh_join_key();
	const auto dir = fixture / "joined5";
	std::filesystem::remove_all(dir);
	save_join_key(dir, key);
	auto joining = [&](JoinToken t, std::string k, NodeId requested = 0) {
		return join(joiner.io.get_executor(), task::scheduler_ref::of(joiner.scheduler), endpoint, std::move(t), std::move(k),
					"detail", requested);
	};
	auto attempt = [&](JoinToken t, NodeId requested = 0) {
		return fault_of([&] { run(joiner, joining(std::move(t), key, requested)); });
	};
	auto wrong = token;
	wrong.secret[0] ^= 1;
	check(attempt(wrong) == JoinFault::token && service.refused(JoinFault::token) == 1, "a wrong secret is refused: token");
	check(attempt({cluster, token.ca, ""}) == JoinFault::token && service.refused(JoinFault::token) == 2,
		  "a missing secret is refused: token");
	auto elsewhere = token;
	elsewhere.cluster.fill('X');
	check(attempt(elsewhere) == JoinFault::cluster && service.refused(JoinFault::token) == 2 && assigns == 2,
		  "a token of another cluster is refused by the joiner before its secret goes out: cluster");
	check(attempt({cluster, key_pin(certificate("rogue").get()), token.secret}) == JoinFault::ca && assigns == 2,
		  "a token pinning another CA key is refused by the joiner before its secret goes out: ca");
	std::string reason;
	try {
		run(joiner, joining(token, key, 1));
	} catch (const JoinRefused &e) {
		std::printf("  %s\n", e.what());
		reason = e.fault == JoinFault::refused ? e.reason : "";
	}
	check(reason == "name" && service.refused(JoinFault::refused) == 1, "a node the hook refuses: refused, with its reason");
	{
		// The CA certificate is public: a host with another CA's leaf (rogue3) offers it.
		Loop fake;
		fake.thread = std::thread([&] { fake.io.run(); });
		tcp::acceptor listener(fake.io, {asio::ip::address_v4::loopback(), 0});
		auto received = start(fake, impostor(listener, context("rogue3"), ca.get(), task::scheduler_ref::of(fake.scheduler)));
		std::optional<JoinFault> fault;
		try {
			run(joiner, join(joiner.io.get_executor(), task::scheduler_ref::of(joiner.scheduler), listener.local_endpoint(), token, key));
		} catch (const JoinRefused &e) {
			std::printf("  %s\n", e.what());
			fault = e.fault;
		} catch (const std::exception &e) {
			std::printf("  impostor: %s\n", e.what());
		}
		const auto bytes = received.get();
		std::printf("  the impostor received %zu bytes\n", bytes);
		check(fault == JoinFault::ca && !bytes, "an impostor offering the CA certificate is refused before the secret goes out: ca");
		fake.join();
	}
	check(!issuer.signatures(), "nothing was signed");

	// Two joins with the saved key at once, then a retry with it: one node, one certificate.
	auto first = start(joiner, joining(token, key));
	auto second = start(joiner, joining(token, key));
	const auto a = first.get(), b = second.get();
	const auto retried = run(joiner, joining(token, key));
	std::printf("  nodes %llu %llu %llu, %llu signatures\n", (unsigned long long)a.node, (unsigned long long)b.node,
				(unsigned long long)retried.node, (unsigned long long)issuer.signatures());
	check(a.node == 5 && b.node == 5 && retried.node == 5 && a.certificate == b.certificate &&
			  a.certificate == retried.certificate && issuer.signatures() == 1,
		  "two joins with the same saved key and a retry get node 5 and one certificate");
	const auto six = run(joiner, joining(token, fresh_join_key()));
	check(six.node == 6 && issuer.signatures() == 2, "another key is another node");
	check(a.key == key, "the joined node keeps the key its caller saved");
	const auto files = a.save(dir);
	check(files.certificate_authority == (dir / "ca.pem").string() && files.private_key == (dir / "node.key").string(),
		  "it keeps the key, certificate and CA it was handed");
	{
		Host five(5, "joined5/node", 2);
		five.start();
		five.admit(one);
		check(five.echoes(1), "with no key or certificate configured by hand, it joins and calls");
		check(one.pins->pin(5) == pin_of("joined5/node"), "node 1 pinned its issued certificate at its first join");
		one.admit(five);
		check(one.echoes(5), "routed to it, node 1 calls it back");
		five.stop();
	}
	const auto joins = one.loops[0]->at([&] { return one.transport->link_stats(0).joins; });
	std::printf("  node 1's port took %llu joins\n", (unsigned long long)joins);
	// Two secrets, a cluster, a CA key, a name, one key three times, another key: 9 joins.
	check(joins == 9, "every join came through node 1's transport port");
	joiner.join();
	one.stop();
}

// The join pool on the transport port (A3, M4): joiners come from ::1, node 2 from 127.0.0.1. A
// joiner that stalls after its magic is cut at the slot deadline, one past its address's cap is
// refused, joins past the rate wait, and all the while node 2's links, dropped again and again,
// redial and finish their hellos within the handshake deadline.
void flood_case() {
	const auto ca = certificate("ca");
	std::optional<task::scheduler_ref> loop0;
	Issuer issuer(ca.get(), key_of("ca").get(), ClusterCa(context("n1")->native_handle(), 1).prefix(),
				  [&](const JoinRequest &) -> task::task<Issuer::Assignment> {
					  late_deadline relay(*loop0, 5ms);
					  co_await relay.wait();
					  co_return Issuer::Assignment{9, JoinFault::refused, {}};
				  });
	JoinOptions options;
	options.slot = 1000ms;  // the default's 10 s, shortened
	Host one(1, "n1", 2, [&](JoinRequest r) { return issuer.issue(std::move(r)); }, options), two(2, "n2", 2);
	one.v6.emplace(one.loops[0]->io, tcp::endpoint(asio::ip::address_v6::loopback(), 0));
	loop0 = task::scheduler_ref::of(one.loops[0]->scheduler);
	one.start();
	two.start();
	pair(one, two);
	check(one.echoes(2) && two.echoes(1), "nodes 1 and 2 link");
	const auto &joins = *one.transport->joins();
	Identity cluster{};
	cluster.fill('K');
	const auto token = fresh_join_token(cluster, ca.get());

	// 12 joiners from ::1 send the magic and stall: 8 take the address's places, 4 are refused.
	asio::io_context io;
	std::vector<tcp::socket> stalled;
	for (int i = 0; i != 12; ++i) {
		stalled.emplace_back(io);
		stalled.back().connect(one.endpoint6());
		asio::write(stalled.back(), asio::buffer(join_magic.data(), join_magic.size()));
	}
	check(until([&] { return joins.busy() == 4 && joins.serving() == 8; }, 2000ms), "8 stalled joiners served, 4 past the address's cap refused");
	const auto stalled_at = steady::now();
	check(until([&] { return joins.cut() == 8 && !joins.serving(); }, 3000ms), "the 8 stalled joiners are cut at the slot deadline");
	const auto freed = std::chrono::duration_cast<std::chrono::milliseconds>(steady::now() - stalled_at).count();
	std::printf("  freed in %lld ms\n", (long long)freed);
	check(freed <= 1500, "within the slot deadline");
	stalled.clear();

	// 8 joiners from ::1 join back to back for 3 s, past the rate, while node 2's links are dropped
	// every 200 ms: each redial's hello finishes within the handshake deadline.
	std::atomic<bool> flooding{true};
	std::atomic<int> joined{0}, failed{0};
	Loop joiner;
	joiner.thread = std::thread([&] { joiner.io.run(); });
	std::vector<std::thread> workers;
	for (int w = 0; w != 8; ++w)
		workers.emplace_back([&] {
			while (flooding) {
				try {
					run(joiner, join(joiner.io.get_executor(), task::scheduler_ref::of(joiner.scheduler), one.endpoint6(), token,
									 fresh_join_key()));
					++joined;
				} catch (...) {
					++failed;
				}
			}
		});
	long long slowest = 0;
	int answered = 0;
	for (int round = 0; round != 12; ++round) {
		std::this_thread::sleep_for(200ms);
		one.remove(2);
		one.admit(two);
		const auto asked = steady::now();
		answered += one.echoes(2, 2000ms);
		slowest = std::max<long long>(slowest, std::chrono::duration_cast<std::chrono::milliseconds>(steady::now() - asked).count());
	}
	flooding = false;
	for (auto &w : workers)
		w.join();
	std::printf("  %d joins, %d failed, %llu delayed, %llu refused at the caps; node 2's slowest echo %lld ms\n", joined.load(),
				failed.load(), (unsigned long long)joins.delayed(), (unsigned long long)joins.busy(), slowest);
	check(answered == 12, "under the flood, node 2 answers after each of 12 drops, within the handshake deadline");
	check(joins.delayed() > 0 && joined > 0, "joins past the address's rate wait and are served");
	check(until([&] { return !joins.serving(); }, 3000ms), "and the pool empties");
	joiner.join();
	one.stop();
	two.stop();
}

mode_t mode_of(const std::filesystem::path &path) {
	struct stat s {};
	return ::stat(path.c_str(), &s) == 0 ? s.st_mode & 0777 : 07777;
}
// Joined::save: the key is its owner's alone from its first byte on disk; a failed write throws.
void save_case() {
	const auto dir = fixture / "save";
	const Joined joined{5, std::string(4096, 'k') + "\n", "certificate\n", "ca\n"};
	const auto fresh = [&] {
		std::filesystem::remove_all(dir);
		std::filesystem::create_directories(dir);
	};
	const auto staged = [&] {
		for (const auto &e : std::filesystem::directory_iterator(dir))
			if (e.path().extension() == ".new")
				return true;
		return false;
	};
	::umask(022);
	fresh();
	joined.save(dir);
	std::printf("  modes: node.key %03o, node.pem %03o, ca.pem %03o\n", unsigned(mode_of(dir / "node.key")),
				unsigned(mode_of(dir / "node.pem")), unsigned(mode_of(dir / "ca.pem")));
	check(mode_of(dir / "node.key") == 0600 && mode_of(dir / "node.pem") == 0644 && mode_of(dir / "ca.pem") == 0644 && !staged(),
		  "under umask 022: the key 0600, the certificates 0644, nothing staged left");

	// A child stopped by SIGXFSZ at the key's write: whatever of the key reached the disk.
	fresh();
	std::fflush(stdout);
	const pid_t child = ::fork();
	if (!child) {
		::signal(SIGXFSZ, SIG_DFL);
		const rlimit none{0, 0}, small{1024, 1024};
		::setrlimit(RLIMIT_CORE, &none);
		::setrlimit(RLIMIT_FSIZE, &small);
		try {
			joined.save(dir);
		} catch (...) {
			::_exit(1);
		}
		::_exit(0);
	}
	int status = 0;
	::waitpid(child, &status, 0);
	bool key = false, owner_only = true;
	for (const auto &e : std::filesystem::directory_iterator(dir))
		if (e.path().filename().string().starts_with("node.key")) {
			std::printf("  stopped save left %s %03o\n", e.path().filename().c_str(), unsigned(mode_of(e.path())));
			key = true;
			owner_only &= !(mode_of(e.path()) & 077);
		}
	check(WIFSIGNALED(status) && WTERMSIG(status) == SIGXFSZ && key, "a save stopped by the file-size limit at its key's write");
	check(owner_only, "the key's bytes on disk are its owner's alone");

	// The same limit, SIGXFSZ ignored: the key's write fails with EFBIG.
	fresh();
	const auto previous = ::signal(SIGXFSZ, SIG_IGN);
	rlimit limit{};
	::getrlimit(RLIMIT_FSIZE, &limit);
	auto small = limit;
	small.rlim_cur = 1024;
	::setrlimit(RLIMIT_FSIZE, &small);
	std::string what;
	try {
		joined.save(dir);
	} catch (const std::system_error &e) {
		what = e.what();
	}
	::setrlimit(RLIMIT_FSIZE, &limit);
	::signal(SIGXFSZ, previous);
	std::printf("  a failed write: %s\n", what.empty() ? "returned" : what.c_str());
	check(what.find("write") != std::string::npos && what.find("node.key") != std::string::npos,
		  "a failed write throws, naming the key");
	check(!std::filesystem::exists(dir / "node.key") && !staged(), "and leaves no key behind");

	// A save over a whole one, the process killed (SIGXFSZ) while writing one file: every file
	// reads whole, the old bytes or the new, and the one being written reads old.
	const auto read = [&](const char *name) {
		std::ifstream in(dir / name, std::ios::binary);
		return std::string(std::istreambuf_iterator<char>(in), {});
	};
	const auto killed = [&](const Joined &next) {
		std::fflush(stdout);
		const pid_t child = ::fork();
		if (!child) {
			::signal(SIGXFSZ, SIG_DFL);
			const rlimit none{0, 0}, small{1024, 1024};
			::setrlimit(RLIMIT_CORE, &none);
			::setrlimit(RLIMIT_FSIZE, &small);
			try {
				next.save(dir);
			} catch (...) {
				::_exit(1);
			}
			::_exit(0);
		}
		int status = 0;
		::waitpid(child, &status, 0);
		return WIFSIGNALED(status) && WTERMSIG(status) == SIGXFSZ;
	};
	const Joined old{5, "old key\n", "old certificate\n", "old ca\n"};
	fresh();
	old.save(dir);
	check(killed({5, std::string(4096, 'k'), "new certificate\n", "new ca\n"}) && read("node.key") == old.key &&
			  read("ca.pem") == old.ca && read("node.pem") == old.certificate,
		  "killed writing the key: the old key, CA and certificate read whole");
	fresh();
	old.save(dir);
	check(killed({5, "new key\n", std::string(4096, 'c'), "new ca\n"}) && read("node.key") == "new key\n" &&
			  read("ca.pem") == "new ca\n" && read("node.pem") == old.certificate,
		  "killed writing the certificate, the last: the new key and CA, the old certificate whole");

	// A directory it cannot write: the first file's creation fails.
	if (::geteuid() == 0) {
		std::printf("  as root, every directory is writable: the unwritable directory is skipped\n");
		return;
	}
	fresh();
	std::filesystem::permissions(dir, std::filesystem::perms::owner_read | std::filesystem::perms::owner_exec);
	what.clear();
	try {
		joined.save(dir);
	} catch (const std::system_error &e) {
		what = e.what();
	}
	std::filesystem::permissions(dir, std::filesystem::perms::owner_all);
	std::printf("  an unwritable directory: %s\n", what.empty() ? "returned" : what.c_str());
	check(what.find("node.key") != std::string::npos, "an unwritable directory throws, naming node.key, the first");
	std::filesystem::remove_all(dir);
}
} // namespace

int main(int argc, char **argv) {
	if (argc < 3) {
		std::fprintf(stderr, "usage: %s <fixture dir> policy|join|refused|renew|remove|epoch|issue|save|expired|reload|flood|boot|redial [ca|pins]\n", argv[0]);
		return 2;
	}
	fixture = argv[1];
	const std::string which = argv[2];
	by_ca = argc < 4 || std::string(argv[3]) == "ca";
	const bool one_policy = which == "policy" || which == "issue" || which == "save" || which == "expired" || which == "reload" || which == "flood" || which == "boot" || which == "redial";
	std::printf("%s (%s)\n", which.c_str(), one_policy || by_ca ? "cluster CA" : "pre-approved pins");
	watchdog(120s);
	if (which == "policy")
		policy_case();
	else if (which == "join")
		join_case();
	else if (which == "refused")
		refused_case();
	else if (which == "remove")
		remove_case();
	else if (which == "epoch")
		epoch_case();
	else if (which == "issue")
		issue_case();
	else if (which == "save")
		save_case();
	else if (which == "renew")
		renew_case();
	else if (which == "expired")
		expired_case();
	else if (which == "reload")
		reload_case();
	else if (which == "flood")
		flood_case();
	else if (which == "boot")
		boot_case();
	else if (which == "redial")
		redial_case();
	else
		return 2;
	std::printf("%s\n", ok ? "PASS" : "FAIL");
	return ok ? 0 : 1;
}
