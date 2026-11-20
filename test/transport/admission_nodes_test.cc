// Nodes admitted into and removed from running host transports (docs/mechanisms/admission.md),
// over loopback with the admission fixture, every loop on its own thread. Each case but `policy`
// runs under both trust policies, `ca` (the default: the cluster CA, pins recorded at first join)
// and `pins` (pre-approved pins only), with the same steps and checks:
//   policy   the cluster CA's verdicts on the fixture's certificates, and the directory's rules
//   join     a node admitted at runtime is dialed, completes the hello against its pin, and calls
//   refused  a node never admitted, and one presenting another certificate, refused by name
//   remove   a node removed and admitted again 8 times while calls flow between two other nodes:
//            its links close, its dials are refused (removed), and no other call is lost
//   issue    a node with no key or certificate joins with the token (D150): the issuer signs its
//            key, the CA policy pins it at its first call; a wrong, a missing and another
//            cluster's token, and a node the hook refuses, are refused by name
//   consensus_transport_admission_nodes_test <fixture dir> case [ca|pins]
#include "consensus/transport/admission_join.h"
#include "consensus/transport/host_transport.h"
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <cassert>
#include <cstdio>
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
bool by_ca = true;  // the trust policy under test
constexpr const char *prefix = "spiffe://kronuz.test/node/";

std::unique_ptr<X509, decltype(&X509_free)> certificate(const std::string &name) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file((fixture / (name + ".pem")).c_str(), "r"), BIO_free);
	std::unique_ptr<X509, decltype(&X509_free)> c(PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
	assert(c);
	return c;
}
Fingerprint pin_of(const std::string &name) { return certificate_fingerprint(certificate(name).get()); }
std::shared_ptr<asio::ssl::context> context(const std::string &name) {
	auto c = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	reactor::tls::configure(*c, {(fixture / (name + ".pem")).string(), (fixture / (name + ".key")).string(),
								 (fixture / "ca.pem").string()},
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
	std::shared_ptr<asio::ssl::context> tls;
	std::optional<NodePins> pins;
	std::unique_ptr<HostTransport> transport;
	bool running = false;
	Host(NodeId n, std::string certificate, std::uint32_t L)
		: node(n), name(std::move(certificate)), loops(make(L)), acceptor(loops[0]->io, {asio::ip::address_v4::loopback(), 0}) {
		tls = context(name);
		const std::map<NodeId, Fingerprint> own{{node, pin_of(name)}};
		if (by_ca)
			pins.emplace(node, own, tls->native_handle());  // the default: the cluster CA (D149)
		else
			pins.emplace(node, own, tls->native_handle(), pre_approved_pins());
		HostTransportOptions options;
		options.member_of = [](GroupId, NodeId) -> std::optional<Member> { return std::nullopt; };
		options.links.connection.handshake = 2000ms;
		options.redial = 50ms;
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
	void start() {
		transport->serve(acceptor);
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
	check(!pre_approved_pins()(certificate("n1").get(), nullptr).node, "pre-approved pins admit no unpinned certificate");

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

	// Node 3 again on another address with another certificate: node 1 pinned its first.
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
	one.at(3, [&] {
		one.transport->admit_node(3, three.address());
		return 0;
	});
	check(one.echoes(3), "back at its own address, node 3 answers");
	for (auto *h : {&one, &three, &four, &rekeyed})
		h->stop();
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
// Runs `t` on `loop` and waits for its value, or rethrows what it threw.
template <class T> T run(Loop &loop, task::task<T> t) {
	std::promise<T> result;
	asio::post(loop.io, [&] {
		task::spawn(task::scheduler_ref::of(loop.scheduler), std::move(t), [&](std::exception_ptr e, std::optional<T> v) {
			if (e)
				result.set_exception(e);
			else
				result.set_value(std::move(*v));
		});
	});
	return result.get_future().get();
}
std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key_of(const std::string &name) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file((fixture / (name + ".key")).c_str(), "r"), BIO_free);
	std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> k(PEM_read_bio_PrivateKey(input.get(), nullptr, nullptr, nullptr),
														  EVP_PKEY_free);
	assert(k);
	return k;
}

void issue_case() {
	Host one(1, "n1", 2);
	one.start();
	const auto ca = certificate("ca");
	auto token = fresh_join_token(ca.get());
	const auto text = encode_join_token(token);
	check(decode_join_token(text) == token && text.size() == 132 && !decode_join_token(text.substr(1)),
		  "the token prints as 132 characters and reads back");
	std::atomic<std::uint64_t> issued{0};
	Issuer issuer(one.tls, ca.get(), key_of("ca").get(), ClusterCa(one.tls->native_handle(), 1).prefix(), token.secret,
				  {.assign = [](NodeId requested) { return requested >= 5 && requested <= 9 ? requested : NodeId{0}; },
				   .issued = [&](NodeId, X509 *) { ++issued; }});
	// The issuer serves on its own acceptor on node 1's loop 0.
	auto &at = *one.loops[0];
	tcp::acceptor acceptor(at.io, {asio::ip::address_v4::loopback(), 0});
	const tcp::endpoint endpoint = acceptor.local_endpoint();
	auto loop0 = task::scheduler_ref::of(at.scheduler);
	asio::post(at.io, [&] {
		task::spawn(loop0, [](tcp::acceptor &acceptor, Issuer &issuer, task::scheduler_ref loop) -> task::task<void> {
			for (;;) {
				auto [error, socket] = co_await task::await_asio<void(asio::error_code, tcp::socket)>(
					[&](auto h) { acceptor.async_accept(std::move(h)); });
				if (error || !socket)
					co_return;
				task::spawn(loop, issuer.serve(std::move(*socket), loop), [](std::exception_ptr, std::optional<NodeId>) {});
			}
		}(acceptor, issuer, loop0));
	});

	Loop joiner;
	joiner.thread = std::thread([&] { joiner.io.run(); });
	auto attempt = [&](JoinToken t, NodeId requested) -> std::optional<JoinFault> {
		try {
			run(joiner, join(joiner.io.get_executor(), task::scheduler_ref::of(joiner.scheduler), endpoint, std::move(t), requested));
		} catch (const JoinRefused &e) {
			std::printf("  %s\n", e.what());
			return e.fault;
		}
		return std::nullopt;
	};
	auto wrong = token;
	wrong.secret[0] ^= 1;
	check(attempt(wrong, 5) == JoinFault::token && issuer.refused(JoinFault::token) == 1, "a wrong token is refused: token");
	check(attempt({token.ca, ""}, 5) == JoinFault::token && issuer.refused(JoinFault::token) == 2, "a missing token is refused: token");
	check(attempt({pin_of("rogue"), token.secret}, 5) == JoinFault::ca && issuer.refused(JoinFault::token) == 2,
		  "a token of another CA is refused by the joiner before its secret goes out: ca");
	check(attempt(token, 1) == JoinFault::node && issuer.refused(JoinFault::node) == 1, "a node the hook refuses: node");
	check(!issued, "nothing was issued");

	const auto joined = run(joiner, join(joiner.io.get_executor(), task::scheduler_ref::of(joiner.scheduler), endpoint, token, 5));
	check(joined.node == 5 && issued == 1, "the token's holder is issued node 5's certificate");
	const auto files = joined.save(fixture / "joined5");
	check(files.certificate_authority == (fixture / "joined5" / "ca.pem").string(), "it keeps the key, certificate and CA it was handed");
	{
		Host five(5, "joined5/node", 2);
		five.start();
		five.admit(one);
		check(five.echoes(1) && one.echoes(5), "with no key or certificate configured by hand, it joins and calls");
		check(one.pins->pin(5) == pin_of("joined5/node"), "node 1 pinned its issued certificate at its first join");
		five.stop();
	}
	asio::post(at.io, [&] {
		asio::error_code ignored;
		acceptor.close(ignored);
	});
	joiner.join();
	one.stop();
}
} // namespace

int main(int argc, char **argv) {
	if (argc < 3) {
		std::fprintf(stderr, "usage: %s <fixture dir> policy|join|refused|remove|issue [ca|pins]\n", argv[0]);
		return 2;
	}
	fixture = argv[1];
	const std::string which = argv[2];
	by_ca = argc < 4 || std::string(argv[3]) == "ca";
	std::printf("%s (%s)\n", which.c_str(), which == "policy" || which == "issue" ? "cluster CA" : by_ca ? "cluster CA" : "pre-approved pins");
	watchdog(120s);
	if (which == "policy")
		policy_case();
	else if (which == "join")
		join_case();
	else if (which == "refused")
		refused_case();
	else if (which == "remove")
		remove_case();
	else if (which == "issue")
		issue_case();
	else
		return 2;
	std::printf("%s\n", ok ? "PASS" : "FAIL");
	return ok ? 0 : 1;
}
