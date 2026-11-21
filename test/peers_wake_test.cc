// PeerOptions::wake (docs/mechanisms/peers-wake.md): Peers calls it once per event that leaves
// the owner work, before the owner can see that work, and never otherwise.
//   idle     a lone voter redialing dead peers, then three connected voters with no traffic
//   control  three voters electing and committing: one wake per Control frame, each with the
//            input retained, and one per Bulk session opened or lost
//   bulk     a scripted voter on the Bulk lane: a session opening, a fetch served (no wake), a
//            Begin the channel takes, its Accepted written, a corrupt frame, a peer hanging up,
//            and a fetch's second deadline closing the lane
//   consensus_transport_peers_wake_test <fixture dir> idle|control|bulk
#include "consensus/transport/peers.h"
#include "consensus/transport/profile.h"
#include "support/ports.h"

#include <cstdio>
#include <cstdlib>
#include <faultlab/faultfs.h>
#include <filesystem>
#include <journal/journal.h>
#include <openssl/pem.h>

using namespace kronuz::consensus;
using namespace kronuz::consensus::transport;
using namespace kronuz::consensus::testing;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using Tcp = asio::ip::tcp;

namespace {
#define CHECK(condition)                                                                           \
	do {                                                                                           \
		if (!(condition)) {                                                                        \
			std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #condition);    \
			std::abort();                                                                          \
		}                                                                                          \
	} while (0)

std::filesystem::path fixture;
const char *names[] = {"one", "two", "three"};
const asio::ip::address loopback = asio::ip::make_address("127.0.0.1");

Fingerprint pin_of(const std::string &name) {
	std::unique_ptr<BIO, decltype(&BIO_free)> input(BIO_new_file((fixture / (name + ".pem")).c_str(), "r"), BIO_free);
	std::unique_ptr<X509, decltype(&X509_free)> leaf(PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr), X509_free);
	CHECK(leaf);
	return certificate_fingerprint(leaf.get());
}
reactor::tls::Options tls(unsigned i) {
	return {(fixture / (std::string(names[i]) + ".pem")).string(), (fixture / (std::string(names[i]) + ".key")).string(),
			(fixture / "ca.pem").string()};
}
std::map<NodeId, Fingerprint> pins() { return {{1, pin_of("one")}, {2, pin_of("two")}, {3, pin_of("three")}}; }

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
PeerConfiguration peer_configuration(NodeId local) {
	PeerConfiguration configuration{fixed(local), limits(), {}, {}, {}, {}};
	configuration.cluster[0] = 'c';
	configuration.group[0] = 'g';
	configuration.epoch[0] = 'e';
	configuration.profile = profile_fingerprint("peers-wake", configuration.configuration, configuration.consensus);
	return configuration;
}

// A Worker over in-memory storage and its snapshot channel, started.
struct Node {
	kronuz::faultlab::filesystem disk;
	kronuz::faultlab::io io{disk};
	Worker worker_;
	SnapshotChannel channel_;
	std::vector<std::string> commands;
	explicit Node(NodeId local, std::optional<SnapshotLimits> snapshots = std::nullopt)
		: worker_(io, fixed(local), {{512 * 1024, 4096}, {16 * 1024, 3}, {128 * 1024, 4}, 8, 3}, limits(), compact,
				  {4, 1, false, false}, snapshots),
		  channel_(worker_) {
		Identity identity{};
		identity[0] = static_cast<char>('a' + local);
		worker_.create(identity);
		for (unsigned turn = 0; turn != 100 && !worker_.ready(); ++turn)
			worker_.run_one({0, 100});
		CHECK(worker_.ready());
		CHECK(worker_.try_submit(Start{}) == SubmitResult::Accepted);
	}
	Worker &worker() { return worker_; }
	SnapshotChannel &channel() { return channel_; }
};

PeerOptions options(unsigned i, const std::map<NodeId, std::uint16_t> &ports, std::uint16_t listen) {
	PeerOptions result;
	result.tls = tls(i);
	result.pins = pins();
	result.listen = {loopback, listen};
	for (const auto &[node, port] : ports)
		if (node != i + 1)
			result.addresses[node] = {"127.0.0.1", port};
	result.deadline = 2000ms;
	return result;
}

// Runs `turn` every millisecond until done() holds or the limit passes.
asio::awaitable<bool> until(std::function<void()> turn, std::function<bool()> done, Clock::duration limit) {
	auto executor = co_await asio::this_coro::executor;
	asio::steady_timer timer(executor);
	const auto end = Clock::now() + limit;
	while (Clock::now() < end) {
		turn();
		if (done())
			co_return true;
		timer.expires_after(1ms);
		co_await timer.async_wait(asio::use_awaitable);
	}
	co_return false;
}
asio::awaitable<void> settle(std::function<void()> turn, Clock::duration duration) {
	(void)co_await until(std::move(turn), [] { return false; }, duration);
}
std::size_t ready(const Peers<Node> &peers) {
	const auto occupancy = peers.occupancy();
	return occupancy.connections - occupancy.handshakes;
}

// A lone voter redials a dead peer and accepts nothing; then three voters connect and idle:
// the only wakes are the two Bulk sessions each opens, and none while they idle.
asio::awaitable<void> idle(asio::io_context &io) {
	std::map<NodeId, std::uint16_t> ports;
	for (NodeId node = 1; node <= 3; ++node)
		ports[node] = free_port(io, loopback);
	{
		unsigned wakes = 0;
		auto node = std::make_shared<Node>(2);
		auto lone_options = options(1, ports, ports[2]);
		lone_options.wake = [&] { ++wakes; };
		auto lone = std::make_shared<Peers<Node>>(io.get_executor(), node, peer_configuration(2), lone_options);
		lone->start();
		co_await settle([&] { lone->admit(); }, 1000ms);
		CHECK(lone->stats().reconnects >= 2);
		CHECK(wakes == 0);
		lone->stop();
		CHECK(co_await until([] {}, [&] { return lone->drained(); }, 5s));
		CHECK(wakes == 0);
	}
	std::array<unsigned, 3> wakes{};
	std::array<std::shared_ptr<Node>, 3> nodes;
	std::array<std::shared_ptr<Peers<Node>>, 3> peers;
	for (unsigned i = 0; i != 3; ++i) {
		nodes[i] = std::make_shared<Node>(i + 1);
		auto voter_options = options(i, ports, ports[i + 1]);
		voter_options.wake = [&wakes, i] { ++wakes[i]; };
		peers[i] = std::make_shared<Peers<Node>>(io.get_executor(), nodes[i], peer_configuration(i + 1), voter_options);
	}
	for (auto &voter : peers)
		voter->start();
	const auto turn = [&] {
		for (auto &voter : peers)
			voter->admit();
	};
	CHECK(co_await until(turn, [&] { return ready(*peers[0]) == 4 && ready(*peers[1]) == 4 && ready(*peers[2]) == 4; }, 10s));
	CHECK(co_await until(turn, [&] { return wakes == std::array<unsigned, 3>{2, 2, 2}; }, 2s));
	co_await settle(turn, 1000ms);
	for (unsigned i = 0; i != 3; ++i) {
		CHECK(wakes[i] == 2);
		CHECK(peers[i]->stats().received == 0);
	}
	for (auto &voter : peers)
		voter->stop();
	CHECK(co_await until([] {}, [&] { return peers[0]->drained() && peers[1]->drained() && peers[2]->drained(); }, 5s));
	CHECK(wakes == (std::array<unsigned, 3>{2, 2, 2}));
}

// Three voters elect and commit with every input admitted from the wake itself. At each wake a
// newly received Control frame is the only new one and its input is retained; at each owner turn
// every received frame has had its wake. Resting, the counts are received + 2 (Bulk sessions);
// one voter stopping costs each other voter one wake (its lost Bulk session) and itself none.
asio::awaitable<void> control(asio::io_context &io) {
	std::map<NodeId, std::uint16_t> ports;
	for (NodeId node = 1; node <= 3; ++node)
		ports[node] = free_port(io, loopback);
	std::array<std::shared_ptr<Node>, 3> nodes;
	std::array<std::shared_ptr<Peers<Node>>, 3> peers;
	std::array<std::uint64_t, 3> wakes{}, seen{}, framed{};
	std::array<bool, 3> running{true, true, true};
	for (unsigned i = 0; i != 3; ++i) {
		nodes[i] = std::make_shared<Node>(i + 1);
		auto voter_options = options(i, ports, ports[i + 1]);
		voter_options.wake = [&, i] {
			auto &voter = *peers[i];
			const auto received = voter.stats().received;
			if (received != seen[i]) {
				CHECK(received == seen[i] + 1);
				CHECK(voter.occupancy().retained_inputs >= 1);
				seen[i] = received;
				++framed[i];
			}
			++wakes[i];
			voter.admit();
		};
		peers[i] = std::make_shared<Peers<Node>>(io.get_executor(), nodes[i], peer_configuration(i + 1), voter_options);
	}
	for (auto &voter : peers)
		voter->start();
	const auto origin = Clock::now();
	bool sending = true;  // off: the Workers run, their messages go nowhere
	RequestId request = 0;
	const auto turn = [&] {
		const auto now = static_cast<std::uint64_t>(
			std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - origin).count());
		for (unsigned i = 0; i != 3; ++i) {
			if (!running[i])
				continue;
			CHECK(seen[i] == peers[i]->stats().received);
			peers[i]->admit();
			auto &node = *nodes[i];
			const Tick tick{now, compact.election_min + i * (compact.election_max - compact.election_min) * 2 / 5};
			node.worker().run_one(tick);
			for (auto &action : node.worker().take_actions()) {
				if (auto send = std::get_if<Send>(&action)) {
					if (sending)
						peers[i]->send(std::move(*send));
				}
				else if (auto committed = std::get_if<Committed>(&action)) {
					for (const auto &entry : committed->entries)
						if (entry.kind == EntryKind::Command)
							node.commands.push_back(entry.payload);
					node.worker().applied({committed->entries.back().index});
				}
			}
			node.channel().run_one(tick);
			peers[i]->pump();
		}
	};
	const auto applied = [&](const std::string &command) {
		for (auto &node : nodes)
			if (std::find(node->commands.begin(), node->commands.end(), command) == node->commands.end())
				return false;
		return true;
	};
	std::optional<std::pair<unsigned, Term>> proposed;
	CHECK(co_await until(turn,
						 [&] {
							 for (unsigned i = 0; i != 3; ++i) {
								 auto &worker = nodes[i]->worker();
								 if (worker.role() != Role::Leader)
									 continue;
								 const auto at = std::make_pair(i, worker.term());
								 if (proposed != at && worker.try_submit(Propose{++request, "wake"}) == SubmitResult::Accepted)
									 proposed = at;
							 }
							 return applied("wake");
						 },
						 20s));
	sending = false;
	co_await settle(turn, 500ms);
	for (unsigned i = 0; i != 3; ++i) {
		const auto &stats = peers[i]->stats();
		std::printf("voter %u: %llu wakes, %llu frames\n", i + 1, static_cast<unsigned long long>(wakes[i]),
					static_cast<unsigned long long>(stats.received));
		// Four lanes, each authenticated once: no Bulk session was lost and opened again. (A dial
		// the peer turned away past its handshake bound never opened one.)
		CHECK(stats.received > 0 && stats.admitted == 4 && ready(*peers[i]) == 4);
		CHECK(framed[i] == stats.received);
		CHECK(wakes[i] == stats.received + 2);
		CHECK(peers[i]->occupancy().retained_inputs == 0);
	}
	// Resting: no traffic, no wake.
	const auto rested = wakes;
	co_await settle(turn, 1000ms);
	CHECK(wakes == rested);
	peers[2]->stop();
	running[2] = false;
	CHECK(co_await until(turn, [&] { return peers[2]->drained() && ready(*peers[0]) == 2 && ready(*peers[1]) == 2; }, 5s));
	co_await settle(turn, 500ms);
	CHECK(wakes[0] == rested[0] + 1 && wakes[1] == rested[1] + 1 && wakes[2] == rested[2]);
	for (unsigned i = 0; i != 2; ++i)
		peers[i]->stop();
	CHECK(co_await until([] {}, [&] { return peers[0]->drained() && peers[1]->drained(); }, 5s));
	CHECK(wakes[0] == rested[0] + 1 && wakes[1] == rested[1] + 1);
}

// The Bulk lane against a scripted voter 1, frame by frame; voter 3 is a dead port.
struct Scripted {
	std::shared_ptr<asio::ssl::context> context = std::make_shared<asio::ssl::context>(asio::ssl::context::tls);
	std::shared_ptr<ByteBudget> budget = std::make_shared<ByteBudget>(16 * 1024 * 1024);
	Scripted() { reactor::tls::configure(*context, tls(0), reactor::tls::Role::Peer); }
	struct Lane {
		std::unique_ptr<Session> session;
		Hello local, remote;
	};
	asio::awaitable<Lane> open(Tcp::endpoint endpoint) {
		auto executor = co_await asio::this_coro::executor;
		Tcp::socket socket(executor);
		co_await socket.async_connect(endpoint, asio::use_awaitable);
		Lane lane;
		lane.session = std::make_unique<Session>(std::move(socket), context, budget, maximum_transfer_bytes, 2000ms);
		const auto configuration = peer_configuration(1);
		lane.local.node = 1;
		lane.local.lane = transport::Lane::Bulk;
		lane.local.cluster = configuration.cluster;
		lane.local.group = configuration.group;
		lane.local.epoch = configuration.epoch;
		lane.local.profile = configuration.profile;
		lane.local.connection = fresh_connection_identity();
		const auto all = pins();
		lane.remote = co_await lane.session->authenticate(false, lane.local, {{1, all.at(1)}, {2, all.at(2)}});
		co_return lane;
	}
	asio::awaitable<void> put(Session &session, const std::string &bytes) {
		Frame frame(budget, bytes.size());
		frame.assign(bytes);
		co_await session.send(std::move(frame));
	}
};

asio::awaitable<void> bulk(asio::io_context &io) {
	std::map<NodeId, std::uint16_t> ports;
	for (NodeId node = 1; node <= 3; ++node)
		ports[node] = free_port(io, loopback);
	auto node = std::make_shared<Node>(2, SnapshotLimits{7, 100000});
	auto voter_options = options(1, ports, ports[2]);
	voter_options.fetch_deadline = 300ms;
	std::shared_ptr<Peers<Node>> peers;
	unsigned wakes = 0;
	std::optional<SnapshotId> receiving;
	voter_options.wake = [&] {
		++wakes;
		if (!receiving)
			receiving = node->channel().incoming_snapshot();
		peers->pump();  // the owner's answer: hand the writer what the channel now has
	};
	peers = std::make_shared<Peers<Node>>(io.get_executor(), node, peer_configuration(2), voter_options);
	peers->start();
	const auto turn = [&] { peers->admit(); };
	const auto reaches = [&](unsigned count) { return until(turn, [&, count] { return wakes >= count; }, 5s); };
	const Tcp::endpoint endpoint{loopback, ports[2]};
	Scripted voter;

	// A session opens: one wake. A fetch request is served by Peers alone: none.
	auto lane = co_await voter.open(endpoint);
	CHECK(co_await reaches(1));
	co_await voter.put(*lane.session, encode_fetch({FetchKind::Request, 1, 0, "name"}));
	auto reply = co_await lane.session->receive();
	CHECK(decode_fetch(reply.bytes()).kind == FetchKind::Missing);
	co_await settle(turn, 200ms);
	CHECK(wakes == 1);

	// A Begin the channel takes: one wake, with the transfer already the channel's. The owner's
	// pump() hands the writer the Accepted, and the written frame is one more.
	const auto configuration = fixed(2);
	const std::string payload = "data";
	SnapshotDescriptor descriptor{configuration.cluster, configuration.configuration, 7, 1, 1, payload.size(),
								  kronuz::journal::crc32c(payload)};
	const TransferEnvelope envelope{configuration.cluster, configuration.configuration, lane.local.connection,
									lane.remote.connection, 1, 2, {1, 17, {1, 1}}};
	co_await voter.put(*lane.session, encode_transfer(TransferKind::Begin, envelope,
													   encode_snapshot(descriptor, *node->worker().snapshot_policy())));
	auto accepted = co_await lane.session->receive();
	CHECK(decode_transfer(accepted.bytes()).kind == TransferKind::Accepted);
	CHECK(co_await reaches(3));
	CHECK(receiving && node->channel().incoming_snapshot() == receiving);
	co_await settle(turn, 200ms);
	CHECK(wakes == 3);

	// A corrupt frame closes the session: one wake, none as the lane ends.
	co_await voter.put(*lane.session, std::string(16, 'X'));
	CHECK(co_await reaches(4));
	co_await settle(turn, 200ms);
	CHECK(wakes == 4 && ready(*peers) == 0);

	// A peer hanging up: one wake as the session opens, one as it is lost.
	auto second = co_await voter.open(endpoint);
	CHECK(co_await reaches(5));
	second.session->close();
	CHECK(co_await reaches(6));
	co_await settle(turn, 200ms);
	CHECK(wakes == 6 && ready(*peers) == 0);

	// A fetch the peer never answers: its first deadline fails it (the callback is the host's,
	// no wake); the second closes the lane, which loses the session: one wake.
	auto third = co_await voter.open(endpoint);
	CHECK(co_await reaches(7));
	std::optional<std::error_code> fetched;
	unsigned wakes_at_failure = 0;
	peers->fetch(1, "x", [&](std::error_code error, std::optional<std::string>) {
		fetched = error;
		wakes_at_failure = wakes;
	});
	auto request = co_await third.session->receive();
	CHECK(decode_fetch(request.bytes()).kind == FetchKind::Request);
	CHECK(co_await reaches(8));
	CHECK(fetched == std::make_error_code(std::errc::timed_out) && wakes_at_failure == 7);
	co_await settle(turn, 200ms);
	CHECK(wakes == 8 && ready(*peers) == 0);

	peers->stop();
	CHECK(co_await until([] {}, [&] { return peers->drained(); }, 5s));
	CHECK(wakes == 8);
	std::printf("bulk: %u wakes\n", wakes);
}
} // namespace

int main(int argc, char **argv) {
	if (argc != 3) {
		std::fprintf(stderr, "usage: %s <fixture dir> idle|control|bulk\n", argv[0]);
		return 2;
	}
	fixture = argv[1];
	const std::string which = argv[2];
	using Scenario = asio::awaitable<void> (*)(asio::io_context &);
	Scenario scenario = which == "idle" ? idle : which == "control" ? control : which == "bulk" ? bulk : nullptr;
	if (!scenario) {
		std::fprintf(stderr, "unknown case %s\n", argv[2]);
		return 2;
	}
	asio::io_context io;
	std::exception_ptr failure;
	asio::co_spawn(io, scenario(io), [&](std::exception_ptr error) { failure = error; });
	io.run();
	if (failure) {
		try {
			std::rethrow_exception(failure);
		} catch (const std::exception &error) {
			std::fprintf(stderr, "%s: threw: %s\n", which.c_str(), error.what());
		}
		return 1;
	}
	std::printf("%s: ok\n", which.c_str());
	return 0;
}
