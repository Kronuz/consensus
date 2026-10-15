/*
 * Copyright (c) 2026 Germán Méndez Bravo (Kronuz)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

// T2 over shared journals (Track C, C2): three nodes, each one disk with one turn log for
// every group on it, run three-voter groups through Workers. Messages are reordered and
// dropped; power is cut at a random I/O operation of a random node, which loses whatever
// that node's disk had not made durable, and the node later recovers every group from its
// turn log. Oracles:
//   - state machine safety: an index applied anywhere holds the same command everywhere;
//   - durability: everything any node applied is applied again by every node once the
//     cluster heals, in the same order;
//   - liveness: after healing, every group commits a new command on every node.
#include "consensus/worker.h"
#include "journal/memory_io.h"
#include <cstdlib>
#include <deque>
#include <iostream>
#include <map>
#include <random>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace jt = kronuz::journal::testing;

namespace {
int checks = 0, failures = 0;
std::uint64_t crashes = 0, applied_total = 0, published = 0, needs_install = 0;
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; if (failures < 20) { std::cerr << "FAIL: " << what << '\n'; } }
}
constexpr Timing compact{20, 40, 100, 200, 60000};
constexpr NodeId node_count = 3;
constexpr std::uint64_t group_count = 3;
const j::TurnLogOptions options{16 * 1024, 1024, 1, 4, 4096};
// The replacement pool holds a checkpoint bundle of the whole retained log.
const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 32, 3};

FixedConfiguration configuration(std::uint64_t group, NodeId local) {
	FixedConfiguration c; c.local = local; c.voters = {1, 2, 3}; c.cluster[0] = 'S'; c.configuration[0] = static_cast<char>('A' + group); return c;
}
// The retained log holds a whole run, so a follower that was down catches up by append:
// installs need the snapshot transport, which is C4's, not this test's.
Limits limits() { Limits l; l.command_bytes = 1024; l.rpc_bytes = 8192; l.log_bytes = 4u << 20; l.log_entries = 1u << 16; l.control_entries = 4; l.rpc_entries = 8; return l; }

// Election delays within the compact profile's [election_min, election_max]; Core ignores
// a Tick outside it.
std::uint64_t delay(NodeId node, std::uint64_t group) { return compact.election_min + (37 * node + 17 * group) % (compact.election_max - compact.election_min + 1); }

struct Message { std::uint64_t group; NodeId from; kronuz::consensus::Message message; };

struct Node {
	NodeId id;
	jt::Disk disk;
	std::unique_ptr<jt::MemoryIO> log_io;
	std::unique_ptr<j::TurnDriver> driver;
	std::map<std::uint64_t, std::unique_ptr<Worker>> workers;
	std::map<std::uint64_t, bool> started;
	std::deque<Message> inbox;
	bool up = false;
	std::uint64_t down_until = 0;
};

struct Cluster {
	std::mt19937_64 random;
	std::array<Node, node_count> nodes;
	std::map<std::uint64_t, std::map<Index, std::string>> chosen;  // group -> index -> command, from every node's applies
	std::map<std::pair<std::uint64_t, NodeId>, std::vector<std::pair<Index, std::string>>> applied;  // since the node's last restart
	std::map<std::pair<std::uint64_t, NodeId>, std::map<Index, Term>> terms;  // the term of each entry a node applied
	std::uint64_t now = 100;
	RequestId next_request = 1;
	bool proposing = true;

	explicit Cluster(std::uint64_t seed) : random(seed) {
		for (NodeId n = 1; n <= node_count; ++n) { nodes[n - 1].id = n; nodes[n - 1].disk.random.seed(seed * 13 + n); }
		for (auto& node : nodes) { open(node); }
	}
	Node& at(NodeId id) { return nodes[id - 1]; }

	void open(Node& node) {
		node.workers.clear(); node.driver.reset(); node.inbox.clear();
		node.log_io = std::make_unique<jt::MemoryIO>(node.disk, "log/");
		node.driver = std::make_unique<j::TurnDriver>(*node.log_io, j::Identity{'N', static_cast<char>(node.id)}, options);
		check(!node.driver->recovered().damaged && node.driver->recovered().lost.empty(), "power loss alone damages no turn log: node " + std::to_string(node.id) + " at " + std::to_string(now));
		for (std::uint64_t g = 1; g <= group_count; ++g) {
			auto io = std::make_shared<jt::MemoryIO>(node.disk, "g" + std::to_string(g) + "/");
			auto worker = std::make_unique<Worker>(io, *node.driver, g, configuration(g, node.id), admission, limits(), compact, WorkerLimits{16, 128});
			if (j::Journal::layout(*io) == j::Journal::Layout::Shared && node.driver->has_group(g)) {
				worker->recover([&](j::ArtifactReader&) {});
			} else {
				j::Identity identity{'G', static_cast<char>(node.id), static_cast<char>(g)};
				worker->create(identity);
			}
			node.workers[g] = std::move(worker); node.started[g] = false;
			applied[{g, node.id}].clear(); terms[{g, node.id}].clear();
		}
		node.up = true;
	}
	void crash(Node& node) {
		++crashes;
		node.workers.clear(); node.driver.reset(); node.log_io.reset(); node.inbox.clear();
		node.disk.power_loss();
		node.up = false; node.down_until = now + 200 + random() % 600;
	}
	void deliver(std::uint64_t group, NodeId from, const Send& send) {
		if (send.peer < 1 || send.peer > node_count || random() % 10 == 0) { return; }  // dropped
		auto& to = at(send.peer);
		if (!to.up || to.inbox.size() > 256) { return; }
		to.inbox.push_back({group, from, send.message});
		if (to.inbox.size() > 1 && random() % 4 == 0) { std::swap(to.inbox.back(), to.inbox[random() % to.inbox.size()]); }  // reordered
	}
	void pump(Node& node, std::uint64_t g) {
		auto& worker = *node.workers.at(g);
		worker.run_one(Tick{now, delay(node.id, g)});
		while (auto job = worker.take_io_operation()) { j::drive_synchronously(*job); worker.run_one(Tick{now, delay(node.id, g)}); }
		for (const auto& action : worker.take_actions()) {
			if (auto send = std::get_if<Send>(&action)) { deliver(g, node.id, *send); }
			else if (auto batch = std::get_if<Committed>(&action)) {
				for (const auto& entry : batch->entries) {
					terms[{g, node.id}][entry.index] = entry.term;
					if (entry.kind != EntryKind::Command) { continue; }
					auto [it, fresh] = chosen[g].try_emplace(entry.index, entry.payload);
					check(it->second == entry.payload, "group " + std::to_string(g) + " index " + std::to_string(entry.index) + " holds one command everywhere");
					applied[{g, node.id}].push_back({entry.index, entry.payload}); ++applied_total;
				}
				worker.applied({batch->entries.back().index});
			}
			published += std::holds_alternative<CheckpointPublished>(action);
			if (auto fenced = std::get_if<Fenced>(&action); fenced && !node.disk.dead()) { check(false, "node " + std::to_string(node.id) + " group " + std::to_string(g) + " fenced: " + fenced->reason); }
		}
	}
	// One step of one node: deliver a message per group, run each Worker, write a turn.
	void step(Node& node, bool chaos) {
		if (!node.up) { if (now >= node.down_until) { open(node); } return; }
		if (chaos && random() % 400 == 0) { node.disk.cut = node.disk.operations + 1 + random() % 40; }  // power fails soon
		try {
			for (std::size_t m = node.inbox.size(); m-- > 0 && !node.inbox.empty();) {
				auto message = std::move(node.inbox.front()); node.inbox.pop_front();
				auto result = node.workers.at(message.group)->try_submit(Receive{message.from, std::move(message.message)});
				if (result == SubmitResult::Busy || result == SubmitResult::Pressure) { node.inbox.push_back(std::move(message)); }
			}
			for (auto& [g, worker] : node.workers) {
				pump(node, g);
				if (!worker->ready()) { continue; }
				if (!node.started[g]) {
					node.started[g] = worker->try_submit(Start{}) == SubmitResult::Accepted;
					// The hello reports each peer's node limits.
					for (NodeId peer = 1; node.started[g] && peer <= node_count; ++peer) { if (peer != node.id) { worker->try_submit(PeerLimits{peer, limits()}); } }
					continue;
				}
				if (proposing && worker->role() == Role::Leader && random() % 3 == 0) {
					worker->try_submit(Propose{next_request++, "g" + std::to_string(g) + "-" + std::to_string(next_request)});
				}
				// Followers checkpoint (local compaction); a leader's would force installs.
				const auto& known = terms[{g, node.id}];
				if (worker->role() != Role::Leader && worker->applied_index() > worker->base_index() + 20 && known.contains(worker->applied_index()) && random() % 30 == 0) {
					if (auto id = worker->reserve_checkpoint(1024)) {
						worker->attach_capture(*id, {next_request++, worker->applied_index(), known.at(worker->applied_index()), worker->epoch()});
						worker->offer_application_chunk(*id, "", true);
					}
				}
			}
			while (!node.driver->idle()) {
				const auto& write = node.driver->begin();
				for (auto& [g, worker] : node.workers) { pump(node, g); }
				try {
					if (write.file->write_at(write.offset, write.bytes) != write.bytes.size()) { throw std::runtime_error("short turn write"); }
					write.file->sync_data();
				} catch (...) { node.driver->failed(std::current_exception()); throw; }
				node.driver->completed();
			}
		} catch (const jt::PowerLoss&) { crash(node); return; }
		// A cut that a storage operation turned into an error is still a power loss.
		if (node.disk.dead()) { crash(node); }
	}
	void run(unsigned steps, bool chaos) {
		for (unsigned s = 0; s < steps; ++s) {
			now += 10;
			const auto first = random() % node_count;
			for (NodeId k = 0; k < node_count; ++k) { step(nodes[(first + k) % node_count], chaos); }
		}
	}
};

void simulate(std::uint64_t seed, unsigned steps) {
	Cluster cluster(seed);
	const auto at = "seed " + std::to_string(seed);
	try {
		cluster.run(steps, true);
		// Heal: no more power cuts; every node comes back and catches up.
		for (auto& node : cluster.nodes) { node.disk.cut = std::numeric_limits<std::uint64_t>::max(); }
		cluster.now += 1000;
		cluster.run(600, false);
		std::map<std::uint64_t, Index> before;
		for (auto& [g, entries] : cluster.chosen) { before[g] = entries.empty() ? 0 : entries.rbegin()->first; }
		cluster.run(600, false);
		cluster.proposing = false;  // settle: everything committed reaches every node
		cluster.run(300, false);
		for (std::uint64_t g = 1; g <= group_count; ++g) {
			const auto& entries = cluster.chosen[g];
			const Index last = entries.empty() ? 0 : entries.rbegin()->first;
			check(last > before[g], "group " + std::to_string(g) + " commits after healing: " + at);
			for (NodeId n = 1; n <= node_count; ++n) {
				auto& node = cluster.at(n);
				check(node.up && !node.workers.at(g)->fenced(), "node " + std::to_string(n) + " group " + std::to_string(g) + " is up: " + at);
				// Every node applies every chosen command at or below what it has reached.
				const auto applied_index = node.workers.at(g)->applied_index();
				std::map<Index, std::string> seen(cluster.applied[{g, n}].begin(), cluster.applied[{g, n}].end());
				for (const auto& [index, command] : entries) {
					if (index > applied_index || index <= node.workers.at(g)->base_index()) { continue; }
					auto found = seen.find(index);
					check(found != seen.end() && found->second == command, "node " + std::to_string(n) + " group " + std::to_string(g) + " applies index " + std::to_string(index) + " again: " + at);
				}
				// A node behind another's checkpoint base needs an install, which is C4's transport.
				Index highest_base = 0;
				for (NodeId m = 1; m <= node_count; ++m) { highest_base = std::max(highest_base, cluster.at(m).workers.at(g)->base_index()); }
				if (applied_index < highest_base) { ++needs_install; continue; }
				check(applied_index >= last, "node " + std::to_string(n) + " group " + std::to_string(g) + " catches up: " + at);
			}
		}
	} catch (const std::exception& error) { check(false, at + ": " + error.what()); }
}
}  // namespace

int main(int argc, char** argv) {
	const std::uint64_t seeds = argc > 1 ? std::stoull(argv[1]) : 24;
	const unsigned steps = argc > 2 ? unsigned(std::stoul(argv[2])) : 3000;
	for (std::uint64_t seed = 1; seed <= seeds; ++seed) { simulate(seed, steps); }
	std::cout << seeds << " seeds, " << crashes << " power cuts, " << checks << " shared cluster checks, " << failures << " failures ("
		<< applied_total << " commands applied, " << published << " checkpoints published, " << needs_install << " catch-ups left to installs)\n";
	return failures ? 1 : 0;
}
