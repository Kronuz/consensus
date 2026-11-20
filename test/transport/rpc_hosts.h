// Two or more hosts over loopback with the TLS fixture, every loop on its own thread: the harness
// of rpc_host_test and hello_test.
#pragma once
#include "consensus/transport/host_transport.h"
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <cassert>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
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

inline std::filesystem::path fixture;
inline const char *names[] = {"", "one", "two", "three", "four"};

inline Fingerprint pin_of(const std::string &name) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file((fixture / (name + ".pem")).c_str(), "r"), BIO_free);
	std::unique_ptr<X509, decltype(&X509_free)> leaf(PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
	assert(leaf);
	return certificate_fingerprint(leaf.get());
}
inline std::shared_ptr<asio::ssl::context> context(const std::string &name) {
	auto c = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	reactor::tls::configure(*c, {(fixture / (name + ".pem")).string(), (fixture / (name + ".key")).string(),
								 (fixture / "ca.pem").string()},
							reactor::tls::Role::Peer);
	return c;
}
inline std::uint64_t ms_since(steady::time_point t) {
	return std::uint64_t(std::chrono::duration_cast<std::chrono::milliseconds>(steady::now() - t).count());
}

struct Loop {
	asio::io_context io;
	task::asio_scheduler scheduler{io.get_executor()};
	asio::executor_work_guard<asio::io_context::executor_type> work = asio::make_work_guard(io);
	std::thread thread;
	void start() {
		thread = std::thread([this] { io.run(); });
	}
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

struct Host {
	NodeId node;
	std::vector<std::unique_ptr<Loop>> loops;
	tcp::acceptor acceptor;
	std::unique_ptr<HostTransport> transport;
	bool running = false;
	Host(NodeId n, std::uint32_t L) : node(n), loops(make(L)), acceptor(loops[0]->io, {asio::ip::address_v4::loopback(), 0}) {}
	static std::vector<std::unique_ptr<Loop>> make(std::uint32_t L) {
		std::vector<std::unique_ptr<Loop>> loops;
		for (std::uint32_t k = 0; k != L; ++k)
			loops.push_back(std::make_unique<Loop>());
		return loops;
	}
	std::uint16_t port() const { return acceptor.local_endpoint().port(); }
	void open(const std::map<NodeId, PeerAddress> &addresses, HostTransportOptions options = {}) {
		auto tls = context(names[node]);
		std::map<NodeId, Fingerprint> pins;
		for (NodeId n = 1; n <= 4; ++n)
			pins[n] = pin_of(names[n]);
		options.member_of = [](GroupId, NodeId incarnation) -> std::optional<Member> {
			if (incarnation < 1 || incarnation > 4)
				return std::nullopt;
			return Member{incarnation, incarnation, MemberRole::Voter};
		};
		std::vector<HostLoop> at;
		for (auto &l : loops)
			at.push_back({l->io.get_executor(), task::scheduler_ref::of(l->scheduler)});
		Identity cluster{};
		cluster.fill('K');
		transport = std::make_unique<HostTransport>(at, tls, NodePins(node, pins, tls->native_handle()), cluster, addresses, options);
	}
	void stores(const std::vector<GroupId> &groups) {
		GroupOpen o;
		o.cluster.fill('T');
		o.group.fill('G');
		o.epoch.fill('E');
		o.profile.fill(0x5A);
		for (auto g : groups)
			transport->store(g, node, o);
	}
	void start() {
		transport->serve(acceptor);
		for (auto &l : loops)
			l->start();
		running = true;
	}
	template <class F> void post(std::uint32_t k, F f) { asio::post(loops[k]->io, std::move(f)); }
	// Runs f on the loop of node `peer`'s connections and waits for it.
	template <class F> auto at(NodeId peer, F f) { return loops[transport->loop_of_node(peer)]->at(std::move(f)); }
	std::uint64_t total(std::atomic<std::uint64_t> HostTransportStats::*counter) const {
		std::uint64_t sum = 0;
		for (std::uint32_t k = 0; k != loops.size(); ++k)
			sum += transport->stats(k).*counter;
		return sum;
	}
	std::uint64_t lane(std::atomic<std::uint64_t> LaneStats::*counter) const {
		std::uint64_t sum = 0;
		for (std::uint32_t k = 0; k != loops.size(); ++k)
			sum += transport->lane_stats(k).*counter;
		return sum;
	}
	std::uint64_t ended(rpc::Outcome o) const {
		std::uint64_t sum = 0;
		for (std::uint32_t k = 0; k != loops.size(); ++k)
			sum += transport->lane_stats(k).ended(o);
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
inline std::map<NodeId, PeerAddress> addresses(std::initializer_list<const Host *> hosts) {
	std::map<NodeId, PeerAddress> r;
	for (auto *h : hosts)
		r[h->node] = {"127.0.0.1", h->port()};
	return r;
}
inline std::vector<GroupId> groups_on(std::uint32_t L, std::uint32_t k, std::size_t n) {
	std::vector<GroupId> r;
	for (GroupId g = 1000; r.size() < n; ++g)
		if (kronuz::journal::host_detail::loop(kronuz::journal::host_detail::slot(g), L) == k)
			r.push_back(g);
	return r;
}
template <class P> inline bool until(P done, std::chrono::milliseconds limit) {
	const auto start = steady::now();
	while (!done()) {
		if (steady::now() - start > limit)
			return false;
		std::this_thread::sleep_for(5ms);
	}
	return true;
}
struct Beats {
	steady::time_point last{};
	std::atomic<std::uint64_t> worst{0}, count{0};
	void beat() {
		const auto now = steady::now();
		if (last != steady::time_point{})
			worst = std::max<std::uint64_t>(worst, std::uint64_t(std::chrono::duration_cast<std::chrono::milliseconds>(now - last).count()));
		last = now;
		++count;
	}
};
