// The node hello's limits and wire versions (docs/mechanisms/hello.md):
//   codec     the ten Limits and the wire range round-trip; a hello whose limits fail
//             validate_limits is refused; a peer outside the wire range is refused by name
//   leader    host 3's hello carries a 64 KiB command limit; host 1's transport reports it for
//             incarnation 3 before that source's first frame, and a leader fed the report holds
//             back a 512 KiB entry from 3 while 1 and 2 commit it (control: hello-limits-ignored)
//   version   a host speaking only wire version 2 never connects to one speaking 1: the acceptor
//             counts the refusal, and no frame crosses; a host whose frame cannot carry its own
//             rpc_bytes is refused at construction
//   consensus_transport_hello_test <fixture dir> case
#include "rpc_hosts.h"
#include "consensus/core.h"
#include <deque>

namespace {
template <class E, class F> bool refuses(F f, std::string_view expect) {
	try {
		f();
	} catch (const E &error) {
		if (std::string_view(error.what()).find(expect) != std::string_view::npos)
			return true;
		std::printf("expected '%.*s', got '%s'\n", int(expect.size()), expect.data(), error.what());
		return false;
	}
	std::printf("not refused: %.*s\n", int(expect.size()), expect.data());
	return false;
}
Identity id(char c) {
	Identity r{};
	r.fill(c);
	return r;
}
Limits low() {
	Limits l;
	l.command_bytes = 64 << 10;
	l.rpc_bytes = 256 << 10;
	return l;
}

bool codec() {
	NodeHello hello{2, LinkLane::Control, false, id('K'), id('c'), 1, 1, 1, 3, low()};
	const auto bytes = encode_node_hello(hello);
	bool ok = bytes.size() == node_hello_size && decode_node_hello(bytes) == hello;
	// rpc_entries (the seventh limit, at 72 + 6 * 8) set to 0: validate_limits refuses it.
	auto zero = bytes;
	zero.replace(72 + 6 * 8, 8, std::string(8, '\0'));
	ok &= refuses<kronuz::journal::Corruption>([&] { (void)decode_node_hello(zero); }, "invalid limits");
	auto inverted = bytes;  // command_bytes above rpc_bytes
	inverted.replace(72 + 8, 8, inverted.substr(72 + 5 * 8, 8));
	inverted[72 + 8] = char(inverted[72 + 8] + 1);
	ok &= refuses<kronuz::journal::Corruption>([&] { (void)decode_node_hello(inverted); }, "invalid limits");
	auto wire = bytes;  // wire_min 0
	wire.replace(64, 4, std::string(4, '\0'));
	ok &= refuses<kronuz::journal::Corruption>([&] { (void)decode_node_hello(wire); }, "invalid wire version range");
	auto bad = hello;
	bad.limits.members = 0;
	ok &= refuses<std::invalid_argument>([&] { (void)encode_node_hello(bad); }, "invalid limits");
	// Wire ranges: any overlap is accepted, none refused by name.
	NodeHello local{1, LinkLane::Control, false, id('K'), id('a'), 1, 1, 2, 4, Limits{}};
	auto remote = hello;
	check_node_hello(remote, local, 2, false);  // 1-3 against 2-4
	remote.wire_min = remote.wire_max = 5;
	ok &= refuses<WireVersionRefused>([&] { check_node_hello(remote, local, 2, false); }, "wire versions 5-5 share none with 2-4");
	remote.wire_min = remote.wire_max = 1;
	ok &= refuses<WireVersionRefused>([&] { check_node_hello(remote, local, 2, false); }, "share none");
	std::printf("codec: %zu-byte hello round-trips with its limits and wire range; zero and inverted limits, wire_min 0 and two disjoint ranges refused: %s\n",
				bytes.size(), ok ? "ok" : "FAILED");
	return ok;
}

// Three Cores in one process; messages cross one FIFO at once, disk and apply complete at once.
struct Cores {
	struct Node {
		std::unique_ptr<Core> core;
		std::vector<std::string> applied;
	};
	std::map<NodeId, Node> nodes;
	std::deque<std::tuple<NodeId, NodeId, Message>> wire;
	std::uint64_t now = 100, big_to_3 = 0;  // appends from 1 to 3 that carried an entry past 3's command limit
	static FixedConfiguration config(NodeId local) {
		FixedConfiguration c;
		c.cluster[0] = 'P';
		c.configuration[0] = 'R';
		c.local = local;
		c.voters = {1, 2, 3};
		return c;
	}
	Cores() {
		for (NodeId n : {1u, 2u, 3u}) {
			RecoveredState s;
			s.configuration = config(n);
			nodes[n].core = std::make_unique<Core>(config(n), std::move(s), Limits{}, Timing{20, 40, 100, 200, 60000});
			consume(n, nodes[n].core->step(Tick{now, 100 + 20 * n}));
			consume(n, nodes[n].core->step(Start{}));
		}
	}
	void consume(NodeId n, Actions actions) {
		auto &node = nodes.at(n);
		for (auto &a : actions) {
			if (auto p = std::get_if<Persist>(&a)) {
				consume(n, node.core->step(Persisted{p->token}));
			} else if (auto s = std::get_if<Send>(&a)) {
				if (auto *append = std::get_if<AppendRequest>(&s->message); append && n == 1 && s->peer == 3)
					for (auto &e : append->entries)
						big_to_3 += e.payload.size() > low().command_bytes;
				wire.emplace_back(n, s->peer, s->message);
			} else if (auto c = std::get_if<Committed>(&a)) {
				for (const auto &e : c->entries)
					if (e.kind == EntryKind::Command)
						node.applied.push_back(e.payload);
				consume(n, node.core->step(Applied{c->entries.back().index}));
			} else if (auto pc = std::get_if<PersistCheckpoint>(&a)) {
				consume(n, node.core->step(Persisted{pc->token}));
			}
		}
	}
	void run(std::uint64_t ms) {
		for (std::uint64_t t = 0; t < ms; t += 10) {
			now += 10;
			for (auto &[n, node] : nodes)
				consume(n, node.core->step(Tick{now, 100 + 20 * n}));
			for (std::size_t bound = 100000; !wire.empty() && bound--;) {
				auto [from, to, message] = std::move(wire.front());
				wire.pop_front();
				consume(to, nodes.at(to).core->step(Tick{now, 100 + 20 * to}));
				consume(to, nodes.at(to).core->step(Receive{from, std::move(message)}));
			}
		}
	}
	bool has(NodeId n, std::size_t bytes) const {
		const auto &a = nodes.at(n).applied;
		return std::any_of(a.begin(), a.end(), [&](const std::string &p) { return p.size() == bytes; });
	}
};

bool leader() {
	Host one(1, 2), three(3, 2);
	HostTransportOptions small;
	small.links.limits = low();
	one.open(addresses({&three}));
	three.open(addresses({&one}), small);
	const GroupId g = 1000;
	one.stores({g});
	three.stores({g});
	std::mutex mutex;
	std::vector<PeerLimits> reports;
	std::atomic<int> delivered{0};
	const auto k = one.transport->loop_of_group(g);
	one.transport->on_peer_limits(k, [&](GroupId group, const PeerLimits &report) {
		std::lock_guard hold(mutex);
		if (group == g)
			reports.push_back(report);
	});
	one.transport->on_deliver(k, [&](const Received &) {
		std::lock_guard hold(mutex);
		delivered += !reports.empty();  // the report came first
		return true;
	});
	one.start();
	three.start();
	const auto at = three.transport->loop_of_group(g);
	const bool arrived = until([&] {
		three.post(at, [&] { three.transport->send(at, LinkLane::Control, FrameKind::control, {g, 3, 1}, 1, "vote"); });
		return delivered > 0;
	}, 10s);
	std::this_thread::sleep_for(100ms);
	one.stop();
	three.stop();
	std::vector<PeerLimits> got;
	{
		std::lock_guard hold(mutex);
		got = reports;
	}
	const bool reported = arrived && got.size() == 1 && got[0].incarnation == 3 && got[0].limits == low();

	// The leader takes the report as its PeerLimits, then a 512 KiB command: 1 and 2 commit it,
	// and no append to 3 carries it.
	Cores cores;
	cores.consume(1, cores.nodes.at(1).core->step(Tick{cores.now, 100}));
	cores.run(400);
	auto &lead = *cores.nodes.at(1).core;
	bool ok = reported && lead.role() == Role::Leader;
	for (auto &report : got)
		cores.consume(1, lead.step(report));
	cores.consume(1, lead.step(Propose{1, std::string(512 << 10, 'x')}));
	cores.consume(1, lead.step(Propose{2, std::string(1000, 'y')}));
	cores.run(2000);
	ok &= cores.has(1, 512 << 10) && cores.has(2, 512 << 10) && !cores.has(3, 512 << 10) && !cores.big_to_3;
	std::printf("leader: host 1 reported %zu limits for incarnation %llu (command_bytes %zu); leader %s; the 512 KiB entry applied on 1: %s, 2: %s, 3: %s; appends to 3 carrying it: %llu %s\n",
				got.size(), (unsigned long long)(got.empty() ? 0 : got[0].incarnation), got.empty() ? 0 : got[0].limits.command_bytes,
				lead.role() == Role::Leader ? "1" : "none", cores.has(1, 512 << 10) ? "yes" : "no", cores.has(2, 512 << 10) ? "yes" : "no",
				cores.has(3, 512 << 10) ? "yes" : "no", (unsigned long long)cores.big_to_3, ok ? "ok" : "FAILED");
	return ok;
}

bool version() {
	Host one(1, 1), two(2, 1);
	HostTransportOptions newer;
	newer.links.wire_min = newer.links.wire_max = 2;
	one.open(addresses({&two}));
	two.open(addresses({&one}), newer);
	const GroupId g = 1000;
	one.stores({g});
	two.stores({g});
	std::atomic<int> delivered{0};
	one.transport->on_deliver(0, [&](const Received &) {
		++delivered;
		return true;
	});
	one.start();
	two.start();
	const bool refused = until([&] {
		two.post(0, [&] { two.transport->send(0, LinkLane::Control, FrameKind::control, {g, 2, 1}, 1, "vote"); });
		return one.loops[0]->at([&] { return one.transport->link_stats(0).version_refused; }) >= 2;
	}, 10s);
	const auto links = one.loops[0]->at([&] { return one.transport->link_stats(0).established; }) +
					   two.loops[0]->at([&] { return two.transport->link_stats(0).established; });
	one.stop();
	two.stop();
	HostTransportOptions tight;
	tight.links.connection.maximum_frame = 1 << 20;  // below a 2 MiB rpc_bytes frame
	Host three(3, 1);
	const bool small = refuses<std::invalid_argument>([&] { three.open(addresses({&one}), tight); }, "maximum_frame below");
	const bool ok = refused && !links && !delivered && small;
	std::printf("version: hellos refused by wire version on host 1: %s; links established %llu; frames delivered %d; a 1 MiB frame for 2 MiB rpc_bytes refused: %s %s\n",
				refused ? "yes" : "no", (unsigned long long)links, delivered.load(), small ? "yes" : "no", ok ? "ok" : "FAILED");
	return ok;
}
} // namespace

int main(int argc, char **argv) {
	if (argc != 3) {
		std::fprintf(stderr, "usage: %s <fixture dir> codec|leader|version\n", argv[0]);
		return 2;
	}
	fixture = argv[1];
	const std::string which = argv[2];
	bool ok;
	if (which == "codec")
		ok = codec();
	else if (which == "leader")
		ok = leader();
	else if (which == "version")
		ok = version();
	else
		return 2;
	std::fflush(stdout);
	return ok ? 0 : 1;
}
