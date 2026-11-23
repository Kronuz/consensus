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
		options.open_of = [](GroupId) {  // no group is stored here; the open is required, never sent
			GroupOpen o;
			o.cluster.fill('T');
			o.group.fill('G');
			o.epoch.fill('E');
			return o;
		};
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
std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key_of(const std::string &name) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file((fixture / (name + ".key")).c_str(), "r"), BIO_free);
	std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> k(PEM_read_bio_PrivateKey(input.get(), nullptr, nullptr, nullptr),
														  EVP_PKEY_free);
	assert(k);
	return k;
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

// Token v2 and the DTJN0002 exchange (A1, A2): the member serves joins on its own acceptor here.
void issue_case() {
	Host one(1, "n1", 2);
	one.start();
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
	auto loop0 = task::scheduler_ref::of(one.loops[0]->scheduler);
	const auto digest = join_secret_digest(token.secret);
	std::mutex assigned_mutex;
	std::map<Fingerprint, NodeId> assigned;
	std::atomic<int> assigns{0};
	Issuer issuer(ca.get(), key_of("ca").get(), ClusterCa(one.tls->native_handle(), 1).prefix(),
				  [&](const JoinRequest &r) -> task::task<Issuer::Assignment> {
					  ++assigns;
					  late_deadline relay(loop0, 20ms);  // the relay to the root's leader
					  co_await relay.wait();
					  if (!join_secret_matches(digest, r.secret))
						  co_return Issuer::Assignment{0, JoinFault::token, {}};
					  if (r.requested == 1)
						  co_return Issuer::Assignment{0, JoinFault::refused, "name"};
					  std::lock_guard hold(assigned_mutex);
					  const auto [it, fresh] = assigned.try_emplace(r.key, NodeId(5 + assigned.size()));
					  co_return Issuer::Assignment{it->second, JoinFault::refused, {}};
				  });
	JoinService service(one.tls, cluster, [&](JoinRequest r) { return issuer.issue(std::move(r)); });
	auto &at = *one.loops[0];
	tcp::acceptor acceptor(at.io, {asio::ip::address_v4::loopback(), 0});
	const tcp::endpoint endpoint = acceptor.local_endpoint();
	asio::post(at.io, [&] {
		task::spawn(loop0, [](tcp::acceptor &acceptor, JoinService &service, task::scheduler_ref loop) -> task::task<void> {
			for (;;) {
				auto [error, socket] = co_await task::await_asio<void(asio::error_code, tcp::socket)>(
					[&](auto h) { acceptor.async_accept(std::move(h)); });
				if (error || !socket)
					co_return;
				task::spawn(loop, service.serve(std::move(*socket), loop), [](std::exception_ptr, std::optional<NodeId>) {});
			}
		}(acceptor, service, loop0));
	});

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
	asio::post(at.io, [&] {
		asio::error_code ignored;
		acceptor.close(ignored);
	});
	joiner.join();
	one.stop();
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
		std::fprintf(stderr, "usage: %s <fixture dir> policy|join|refused|remove|epoch|issue|save [ca|pins]\n", argv[0]);
		return 2;
	}
	fixture = argv[1];
	const std::string which = argv[2];
	by_ca = argc < 4 || std::string(argv[3]) == "ca";
	const bool one_policy = which == "policy" || which == "issue" || which == "save";
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
	else
		return 2;
	std::printf("%s\n", ok ? "PASS" : "FAIL");
	return ok ? 0 : 1;
}
