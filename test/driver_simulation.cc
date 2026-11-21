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

// The randomized simulation through the Driver (docs/DRIVER.md): three nodes, each a journal
// host of two to four loops on a memory disk, and eight to ten three-member groups placed on
// every host by slot, so each host has groups sharing a loop's turn log and groups on other
// loops' logs. Each loop is its own inline_scheduler, Driver and turn driver, with every IO
// primitive held in the loop's HeldCompletion, which the simulation completes in random order,
// one loop at a time. Messages are delayed, dropped, duplicated and partitioned; nodes lose
// power with IO in flight, recover, and sometimes reopen with another loop count, which
// re-partitions the host. Callers propose, await commits, read, transfer leadership, cancel,
// checkpoint their application's image (restored on recovery), and repair at and below the
// commit cursor (Track C, D10), below their latest checkpoint too. The oracles, per group:
//   - one leader per term (RoleChanged);
//   - every node applies a prefix of one sequence of entries;
//   - each call completes once, per its D8 row: propose's Placement holds its command once
//     that index commits in that term; committed() succeeds only if the entry at the placement
//     committed with its term, and is superseded only if another term's did; a read's index
//     covers every entry applied anywhere before it was issued; a cancelled call never yields
//     a value; every call completes by shutdown;
//   - a witness member votes but never applies: its Sink never receives a Committed range;
//   - a repair completes only at or above the node's latest checkpoint, and the application
//     replays from its boundary; once the network heals every repair completes (none pending);
//   - no group fences but by a power cut, and no loop spins, stopping or not.
// After the run the network heals and a new entry commits in every group on every data voter.
#include "consensus/driver.h"
#include "journal/memory_io.h"

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace kronuz::consensus::driver_simulation {

namespace j = kronuz::journal;
namespace jt = kronuz::journal::testing;

namespace {

std::uint64_t failures = 0, current_seed = 0;
void fail(const std::string& what) {
	if (failures++ < 20) { std::fprintf(stderr, "driver seed %llu: %s\n", (unsigned long long)current_seed, what.c_str()); }
}
// SIM_DRIVER_TRACE=1 logs each step's apply, delivery and fault events to stderr.
const bool tracing = std::getenv("SIM_DRIVER_TRACE") != nullptr;
std::uint64_t trace_step = 0;
void trace(const char* format, ...) {
	if (!tracing) { return; }
	std::fprintf(stderr, "[%llu] ", (unsigned long long)trace_step);
	va_list args; va_start(args, format); std::vfprintf(stderr, format, args); va_end(args);
	std::fputc('\n', stderr);
}

constexpr std::uint64_t ms = 1'000'000;
constexpr NodeId witness_node = 3;
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
// The replacement pool covers a checkpoint's peak (its bundle bound from log_bytes, plus the
// application cap): a smaller one never admits a checkpoint, which then blocks the group's calls.
const j::AdmissionLimits admission{{8u << 20, 4096}, {64 * 1024, 3}, {4u << 20, 4}, 16, 3};
constexpr std::uint64_t application_cap = 64 * 1024;
constexpr Timing timing{20, 40, 100, 200, 60000};

Limits limits() {
	Limits l;
	l.command_bytes = 1024; l.rpc_bytes = 4096; l.log_bytes = 1 << 20; l.log_entries = 4096; l.control_entries = 4; l.rpc_entries = 8;
	return l;
}

struct Simulation;
struct Node;

// A node's Sink, shared by its loops' Drivers: hands every notice to the simulation's oracles
// and network.
struct NodeSink {
	Simulation* sim;
	Node* node;
	void operator()(GroupId, Send notice);
	void operator()(GroupId, Committed notice);
	void operator()(GroupId, RoleChanged notice);
	void operator()(GroupId, SendTransfer notice);
	void operator()(GroupId, ValidateImage notice);
	void operator()(GroupId, ActivateImage notice);
	void operator()(GroupId, CheckpointPublished notice);
	void operator()(GroupId, RepairCompleted notice);
	void operator()(GroupId, SnapshotNeeded notice);
	void operator()(GroupId, SnapshotTransmit) {}
	void operator()(GroupId, SnapshotReleased) {}
	void operator()(GroupId, Fenced notice);
};
static_assert(sink<NodeSink>);
using SimDriver = Driver<NodeSink>;

// A caller's view of one call: how it completed.
struct Call {
	enum Kind { open, propose, committed, read, transfer, repair, checkpoint, close } kind;
	NodeId node = 0;
	GroupId group = 0;
	std::string command;
	Placement target;          // committed()
	LogBoundary boundary{};    // repair()
	Index floor = 0;           // read: the highest index of the group applied anywhere when it was issued
	std::uint64_t issued = 0;  // the simulated time (ms) it was issued
	task::cancel_source cancel;
	bool cancel_requested = false;
	int completions = 0;
	std::exception_ptr error;
	std::optional<Placement> placed;
	std::optional<ReadReady> ready;
	std::optional<RepairCompleted> repaired;
	std::optional<CheckpointPublished> published;
};

// An application image: the entries applied through its boundary, each kind, term, payload.
std::string encode_image(const std::vector<Entry>& applied) {
	std::string bytes;
	for (const auto& e : applied) {
		bytes.push_back(static_cast<char>(e.kind));
		bytes.append(reinterpret_cast<const char*>(&e.term), sizeof e.term);
		const auto size = static_cast<std::uint32_t>(e.payload.size());
		bytes.append(reinterpret_cast<const char*>(&size), sizeof size); bytes += e.payload;
	}
	return bytes;
}
std::vector<Entry> decode_image(std::string_view bytes) {
	std::vector<Entry> applied;
	while (!bytes.empty()) {
		Entry e; std::uint32_t size = 0;
		e.kind = static_cast<EntryKind>(bytes[0]); bytes.remove_prefix(1);
		std::memcpy(&e.term, bytes.data(), sizeof e.term); bytes.remove_prefix(sizeof e.term);
		std::memcpy(&size, bytes.data(), sizeof size); bytes.remove_prefix(sizeof size);
		e.payload = std::string(bytes.substr(0, size)); bytes.remove_prefix(size);
		e.index = applied.size() + 1;
		applied.push_back(std::move(e));
	}
	return applied;
}
// A checkpoint's capture of the application: its image through `through`, in the epoch it was taken.
struct Capture {
	std::string image; std::size_t at = 0; Index through; Term term; std::uint64_t epoch;
	CaptureMetadata metadata() const { return {0, through, term, epoch}; }
	std::size_t read(std::span<char> out) {
		auto n = std::min(out.size(), image.size() - at);
		std::copy_n(image.data() + at, n, out.data()); at += n; return n;
	}
};

// One reactor loop of a host: its scheduler, its IO, its Driver, and its turn driver's task.
struct Loop {
	std::uint32_t index = 0;
	j::HeldCompletion held;
	std::unique_ptr<task::inline_scheduler> scheduler;
	std::unique_ptr<SimDriver> driver;
	std::unique_ptr<task::cancel_source> stop;
	bool turns_done = true, driver_done = true;
	task::scheduler_ref ref() const { return task::scheduler_ref::of(*scheduler); }
	// Runs what is queued, failing (not hanging) if the loop never settles.
	void run(NodeId node) {
		std::size_t budget = 1'000'000;
		scheduler->run_until([&] { return --budget == 0; });
		if (!budget) {
			std::fprintf(stderr, "driver seed %llu: node %llu loop %u spins\n", (unsigned long long)current_seed, (unsigned long long)node, index);
			std::exit(1);
		}
	}
};

// A node's copy of one group, as its application sees it.
struct Replica {
	std::vector<Entry> applied;     // what the application applied since the last restart
	std::optional<Index> applying;  // the last Committed range, until the application acknowledges it
	std::optional<Index> repairing; // a repair's boundary, until it completes or is refused
	bool replayed = false;          // a range from the boundary came while repairing
	std::uint64_t committed_notices = 0, epoch = 0;
	Index base = 0;                 // the latest checkpoint's boundary: published, or restored
};

struct Node {
	NodeId id = 0;
	std::uint32_t loop_count = 1;  // what the next boot asks the host for
	jt::Disk disk;
	std::unique_ptr<j::Host> host;
	NodeSink sink;
	std::vector<std::unique_ptr<Loop>> loops;
	bool up = false, cutting = false;
	double rate = 1;
	std::map<GroupId, Replica> replicas;
	Loop& loop_of(GroupId g) { return *loops.at(host->loop(g)); }
	SimDriver& driver(GroupId g) { return *loop_of(g).driver; }
	std::size_t held() const { std::size_t n = 0; for (const auto& l : loops) { n += l->held.held(); } return n; }
	void run() { for (auto& l : loops) { l->run(id); } }
};

struct Envelope { NodeId from, to; GroupId group; Message message; std::uint64_t at; };

struct Simulation {
	Simulation(std::uint64_t seed) : random_(seed) {
		place_groups();
		for (NodeId id = 1; id <= 3; ++id) {
			auto& n = nodes_[id];
			n.id = id;
			n.sink = NodeSink{this, &n};
			n.rate = 0.8 + double(random_() % 451) / 1000.0;
			n.loop_count = 2 + std::uint32_t(random_() % 3);
			boot(n);
		}
	}
	~Simulation() {
		for (auto& [id, n] : nodes_) { if (n.up) { shut(n, false); } }
	}

	void run(std::size_t steps) {
		for (std::size_t step = 0; step < steps && !failures; ++step) {
			trace_step = step;
			advance(1 + random_() % 4);
			auto& n = std::next(nodes_.begin(), std::ptrdiff_t(random_() % nodes_.size()))->second;
			const auto g = groups_[random_() % groups_.size()];
			const auto pick = random_() % 1000;
			if (pick < 300) { for (std::size_t k = 0; k < groups_.size(); ++k) { deliver_one(); } }  // the network carries every group
			else if (pick < 550) { if (n.up) { complete_io(*n.loops[random_() % n.loops.size()], n.id); } }
			else if (pick < 650) { if (n.up) { apply(n, g); } }
			else if (pick < 760) { propose(leader_or(n, g), g); }
			else if (pick < 800) { await_commit(n, g); }
			else if (pick < 830) { read(leader_or(n, g), g); }
			else if (pick < 850) { cancel_one(); }
			else if (pick < 855) { transfer(g); }
			else if (pick < 858) { if (n.up && up_count() == 3) { crash(n); } }
			else if (pick < 870) { if (!n.up) { boot(n); } }
			else if (pick < 874) { toggle_partition(n.id); }
			else if (pick < 904) { checkpoint(n, g); }
			else if (pick < 919) { repair_somewhere(n, g); }
			else { for (auto& [id, m] : nodes_) { if (m.up) { for (auto& l : m.loops) { complete_io(*l, id); } for (auto h : groups_) { apply(m, h); } } } }
			check_calls();
		}
	}

	// Heals the network and every node, then commits a new entry in every group on every data voter.
	void heal() {
		isolated_.clear();
		for (auto& [id, n] : nodes_) { if (!n.up) { boot(n); } }
		std::map<GroupId, std::shared_ptr<Call>> last;
		auto healed = [&](GroupId g) { auto& c = last[g]; return c && c->placed && healed_everywhere(g, *c->placed); };
		// A leader that compacted past a peer's known match asks for an install the simulation
		// never sends, and probes again at its base after snapshot_timeout: healing outlasts that
		// twice on the slowest node's clock (a quiet round is 5 ms, 4 ms at a 0.8 rate).
		const int rounds = 4000 + int(2 * timing.snapshot_timeout / 4);
		for (int round = 0; round < rounds && !failures; ++round) {
			trace_step = 1'000'000 + std::uint64_t(round);
			quiet_round();
			bool all = true;
			for (auto g : groups_) {
				if (healed(g)) { continue; }
				all = false;
				auto& c = last[g];
				// Proposes again once the last proposal failed, or another term's entry took its
				// place, or a later term leads (a deposed leader placed it).
				auto took = c && c->placed ? sequences_[g].find(c->placed->index) : sequences_[g].end();
				bool superseded = took != sequences_[g].end() && took->second.term != c->placed->term;
				for (auto& [id, n] : nodes_) { auto st = state(n, g); superseded |= c && c->placed && st && st->role == Role::Leader && st->term > c->placed->term; }
				if (c && !superseded && !(c->completions && (c->error || !c->placed))) { continue; }
				for (auto& [id, n] : nodes_) {
					auto s = state(n, g);
					if (s && s->role == Role::Leader) { c = start_propose(n, g, "healed"); break; }
				}
			}
			if (all) { break; }
		}
		for (auto g : groups_) {
			if (healed(g)) { continue; }
			fail("group " + std::to_string(g) + " did not commit a new entry on every member (applied on every data voter) after healing; " + std::to_string(snapshots_needed[g]) + " SnapshotNeeded");
			if (auto& c = last[g]) {
				std::string why = "none";
				if (c->error) { try { std::rethrow_exception(c->error); } catch (const std::exception& e) { why = e.what(); } catch (...) { why = "?"; } }
				std::fprintf(stderr, "  last proposal on node %llu: %d completions, error %s, placed %llu/%llu\n", (unsigned long long)c->node, c->completions, why.c_str(), c->placed ? (unsigned long long)c->placed->term : 0ull, c->placed ? (unsigned long long)c->placed->index : 0ull);
			}
			for (auto& [id, n] : nodes_) {
				auto s = state(n, g);
				std::fprintf(stderr, "  node %llu loop %u up %d role %d term %llu committed %llu applied %llu (application %zu) held %zu\n", (unsigned long long)id, n.up ? n.host->loop(g) : 0u, n.up,
					s ? int(s->role) : -1, s ? (unsigned long long)s->term : 0ull, s ? (unsigned long long)s->committed : 0ull, s ? (unsigned long long)s->applied : 0ull, n.replicas[g].applied.size(), n.held());
			}
		}
		// A witness acknowledged its own ranges and its Sink never saw one.
		for (auto g : witnessed_) {
			auto& n = nodes_.at(witness_node);
			auto s = state(n, g);
			if (n.replicas[g].committed_notices) { fail("a witness member's Sink received a Committed range"); }
			if (!s || s->applied != s->committed || s->committed == 0) {
				fail("group " + std::to_string(g) + "'s witness member does not acknowledge its committed ranges itself: committed " + std::to_string(s ? s->committed : 0) + ", applied " + std::to_string(s ? s->applied : 0));
			}
		}
		// Repair liveness (F4): on the healed network every repair completes before shutdown.
		auto pending = [&] { std::size_t k = 0; for (const auto& c : calls_) { k += c->kind == Call::repair && !c->completions; } return k; };
		for (int round = 0; round < 1000 && pending() && !failures; ++round) { quiet_round(); }
		repairs_pending = pending();
		if (repairs_pending) { fail(std::to_string(repairs_pending) + " repairs still pending after healing"); }
		// Shutdown: every call completes.
		for (auto& [id, n] : nodes_) { shut(n, false); }
		for (const auto& call : calls_) {
			if (call->completions != 1) { fail("a call completed " + std::to_string(call->completions) + " times by shutdown"); }
		}
		check_final();
	}

	std::uint64_t proposals = 0, placed = 0, commits = 0, superseded_count = 0, reads = 0, cancelled = 0, crashes = 0, transfers = 0, elections = 0, repartitions = 0;
	std::uint64_t checkpoints = 0, restored = 0, repairs = 0, repairs_below_base = 0, repaired = 0, refused = 0, repairs_cancelled = 0, repairs_pending = 0;
	std::uint64_t repair_wait_total = 0, repair_wait_max = 0;  // simulated ms from issue to a completed repair
	std::uint64_t boots_by_loops[5] = {};
	std::map<GroupId, std::uint64_t> snapshots_needed;  // the simulation sends no installs

	void on_send(Node& n, GroupId g, Send notice) {
		network_.push_back({n.id, notice.peer, g, std::move(notice.message), now_ + random_() % 6});
		if (random_() % 50 == 0) { network_.push_back({n.id, notice.peer, g, network_.back().message, now_ + random_() % 30}); }  // duplicated
	}
	void on_committed(Node& n, GroupId g, Committed notice) {
		auto& r = n.replicas[g];
		++r.committed_notices;
		trace("node %llu group %llu Committed %llu..%llu (application had %zu)", (unsigned long long)n.id, (unsigned long long)g, (unsigned long long)notice.first,
			(unsigned long long)(notice.entries.empty() ? 0 : notice.entries.back().index), r.applied.size());
		if (n.id == witness_node && witnessed_.count(g)) { fail("a witness member's Sink received a Committed range"); }
		// The range after a repair replays from its boundary.
		if (r.repairing && notice.first == *r.repairing + 1) { r.applied.resize(std::min<std::size_t>(r.applied.size(), *r.repairing)); r.replayed = true; }
		if (notice.first != r.applied.size() + 1) {
			fail("node " + std::to_string(n.id) + " received a range of group " + std::to_string(g) + " from " + std::to_string(notice.first) + " after applying " + std::to_string(r.applied.size()));
		}
		auto& sequence = sequences_[g];
		for (auto& entry : notice.entries) {
			auto [it, fresh] = sequence.emplace(entry.index, entry);
			if (!fresh && (it->second.term != entry.term || it->second.payload != entry.payload || it->second.kind != entry.kind)) {
				fail("state machine safety: group " + std::to_string(g) + "'s index " + std::to_string(entry.index) + " differs between nodes");
			}
			r.applied.push_back(std::move(entry));
		}
		r.applying = r.applied.empty() ? 0 : r.applied.back().index;
	}
	void on_role(Node& n, GroupId g, RoleChanged notice) {
		trace("node %llu group %llu role %d term %llu", (unsigned long long)n.id, (unsigned long long)g, int(notice.role), (unsigned long long)notice.term);
		if (notice.role != Role::Leader) { return; }
		++elections;
		auto [it, fresh] = leaders_[g].emplace(notice.term, n.id);
		if (!fresh && it->second != n.id) { fail("two leaders of group " + std::to_string(g) + " in term " + std::to_string(notice.term)); }
	}
	bool witness(NodeId node, GroupId g) const { return node == witness_node && witnessed_.count(g); }
	// A repair rewinds the application to its boundary. The Sink hears it before the range
	// after it; a caller after (its resume is posted), so then the range has rewound it.
	void on_repaired(Node& n, GroupId g, RepairCompleted notice, bool sink = false) {
		auto& r = n.replicas[g];
		r.epoch = notice.epoch;
		if (notice.boundary.index < r.base) { fail("node " + std::to_string(n.id) + " repaired group " + std::to_string(g) + " to " + std::to_string(notice.boundary.index) + ", below its checkpoint at " + std::to_string(r.base)); }
		if (sink || !r.replayed) { r.applied.resize(std::min<std::size_t>(r.applied.size(), notice.boundary.index)); }
		r.repairing.reset(); r.replayed = false;
	}
	void on_published(Node& n, GroupId g, CheckpointPublished notice) {
		auto& r = n.replicas[g];
		r.base = std::max(r.base, notice.through);
	}
	void on_fenced(Node& n, GroupId g, const Fenced& notice) {
		if (!n.cutting) { fail("node " + std::to_string(n.id) + " fenced group " + std::to_string(g) + ": " + notice.reason); }
	}

private:
	// Eight groups, one in each eighth of the host's slots, so every host of two to four loops
	// has a loop with several groups and groups on several loops; then up to two anywhere.
	void place_groups() {
		std::set<GroupId> ids;
		for (std::uint32_t eighth = 0; eighth < 10; ++eighth) {
			if (eighth == 8 && random_() % 3 == 0) { break; }
			if (eighth == 9 && random_() % 2) { break; }
			for (GroupId id = 1 + random_() % (GroupId{1} << 40);; ++id) {
				if (ids.count(id)) { continue; }
				if (eighth < 8 && j::host_detail::slot(id) / 512 != eighth) { continue; }
				ids.insert(id); groups_.push_back(id); break;
			}
		}
		for (std::size_t k = 0; k < groups_.size(); ++k) { if ((current_seed + k) % 2) { witnessed_.insert(groups_[k]); } }
	}
	// Each host has a loop whose turn log several groups share and groups on another loop's log.
	void check_placement(Node& n) {
		std::map<std::uint32_t, std::size_t> per_loop;
		for (auto g : groups_) { ++per_loop[n.host->loop(g)]; }
		std::size_t most = 0;
		for (auto [loop, count] : per_loop) { most = std::max(most, count); }
		if (per_loop.size() < 2 || most < 2) { fail("node " + std::to_string(n.id) + "'s groups do not both share and not share a turn log"); }
	}

	void boot(Node& n) {
		// A restart sometimes asks for another loop count: the host re-partitions its turn logs.
		if (n.disk.operations) {
			if (random_() % 4 == 0) { const auto before = n.loop_count; n.loop_count = 2 + std::uint32_t(random_() % 3); repartitions += before != n.loop_count; }
		}
		n.host = std::make_unique<j::Host>(std::make_shared<jt::MemoryIO>(n.disk, "host/"), j::Identity{'N', static_cast<char>('0' + n.id)}, j::HostOptions{n.loop_count, turn_options});
		++boots_by_loops[n.host->loops()];
		check_placement(n);
		for (std::uint32_t index = 0; index < n.host->loops(); ++index) {
			auto& l = *n.loops.emplace_back(std::make_unique<Loop>());
			l.index = index;
			l.scheduler = std::make_unique<task::inline_scheduler>();
			l.scheduler->advance(now_ * ms);
			l.driver = std::make_unique<SimDriver>(Seams{l.ref(), j::completion_ref::of(l.held), *n.host, index, current_seed * 31 + n.id * 7 + index}, n.sink);
			l.stop = std::make_unique<task::cancel_source>();
			l.turns_done = l.driver_done = false;
			// A loop's turn driver or Driver ends only at shutdown, and only by a power cut with an error.
			auto ended = [&n, &l](const char* what, std::exception_ptr error) {
				if (!error || n.cutting) { return; }
				std::string why = "?";
				try { std::rethrow_exception(error); } catch (const std::exception& e) { why = e.what(); } catch (...) {}
				fail("node " + std::to_string(n.id) + " loop " + std::to_string(l.index) + "'s " + what + " failed: " + why);
			};
			task::spawn(l.ref(), n.host->driver(index).run(j::completion_ref::of(l.held), l.stop->token()), [&l, ended](std::exception_ptr error) { l.turns_done = true; ended("turn driver", error); });
			task::spawn(l.ref(), l.driver->run(l.stop->token()), [&l, ended](std::exception_ptr error) { l.driver_done = true; ended("Driver", error); });
		}
		n.replicas.clear();
		n.up = true;
		trace("node %llu boots with %u loops", (unsigned long long)n.id, n.host->loops());
		for (std::size_t k = 0; k < groups_.size(); ++k) {
			const auto g = groups_[k];
			GroupOptions o;
			o.configuration.local = n.id; o.configuration.voters = {1, 2, 3};
			o.configuration.cluster[0] = 'D'; o.configuration.configuration[0] = 'S';
			if (witnessed_.count(g)) { o.founding = Membership{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 4, 0}; }
			o.admission = admission; o.limits = limits(); o.timing = timing; o.scheduling = WorkerLimits{16, 128};
			auto& d = n.driver(g);
			d.open(g, std::move(o));
			auto call = track(n, g, Call::open);
			if (n.host->status(g) == j::GroupStatus::stored) { start(n, call, d.recover(g, [this, &n, g](j::ArtifactReader& reader) { restore(n, g, reader); }, call->cancel.token())); }
			else { start(n, call, d.create(g, j::Identity{'G', static_cast<char>('0' + n.id), static_cast<char>('a' + k)}, call->cancel.token())); }
		}
		n.run();
	}
	// Stops the node's loops. A crash first cuts its disk's power, so IO in flight fails and
	// unsynced writes may be lost. A clean shutdown follows DRIVER.md's order: its callers cancel,
	// it closes every group while the loops run and its IO completes, then it stops the loops.
	void shut(Node& n, bool power_cut) {
		if (power_cut) { n.disk.cut = n.disk.operations; n.cutting = true; }
		else { close_all(n); }
		for (auto& l : n.loops) { l->stop->request(task::cancel_reason::shutdown); }
		auto stopped = [&] { for (auto& l : n.loops) { if (!l->turns_done || !l->driver_done) { return false; } } return true; };
		for (int round = 0; round < 10000 && !(stopped() && !n.held()); ++round) {
			for (auto& l : n.loops) {
				l->run(n.id);
				for (auto ticket : l->held.tickets()) {
					if (power_cut) { l->held.fail(ticket, std::make_exception_ptr(jt::PowerLoss{})); } else { l->held.complete(ticket); }
					l->run(n.id);
				}
			}
		}
		if (!stopped()) { fail("node " + std::to_string(n.id) + "'s loops did not stop"); }
		for (auto& l : n.loops) { l->driver.reset(); }
		n.host.reset(); n.loops.clear();
		if (power_cut) { n.disk.power_loss(); }
		n.up = n.cutting = false;
	}
	void close_all(Node& n) {
		for (auto& c : calls_) { if (c->node == n.id && !c->completions && !c->cancel_requested) { c->cancel_requested = true; c->cancel.request(); } }
		std::vector<std::shared_ptr<Call>> closes;
		for (auto g : groups_) {
			auto& call = closes.emplace_back(track(n, g, Call::close));
			start(n, call, n.driver(g).close(g));
		}
		auto closed = [&] { for (auto& c : closes) { if (!c->completions) { return false; } } return true; };
		for (int round = 0; round < 10000 && !closed(); ++round) {
			for (auto& l : n.loops) { l->run(n.id); complete_io(*l, n.id, true); }
		}
		if (!closed()) { fail("node " + std::to_string(n.id) + "'s groups did not close"); }
		for (auto& c : closes) { if (c->error) { fail("node " + std::to_string(n.id) + "'s close of group " + std::to_string(c->group) + " failed"); } }
	}
	void crash(Node& n) { ++crashes; trace("node %llu crashes", (unsigned long long)n.id); shut(n, true); }

	std::optional<SimDriver::GroupState> state(Node& n, GroupId g) { return n.up ? n.driver(g).state(g) : std::nullopt; }
	void advance(std::uint64_t dt) {
		now_ += dt;
		for (auto& [id, n] : nodes_) {
			if (!n.up) { continue; }
			for (auto& l : n.loops) {
				l->scheduler->advance(static_cast<std::uint64_t>(double(dt) * n.rate * double(ms)));
				l->run(n.id);
			}
		}
	}
	void quiet_round() {
		advance(5);
		while (!network_.empty()) { deliver(network_.front()); network_.pop_front(); }
		for (auto& [id, n] : nodes_) {
			if (!n.up) { continue; }
			for (auto& l : n.loops) { complete_io(*l, n.id, true); }
			for (auto g : groups_) { apply(n, g); }
		}
	}
	std::size_t up_count() const { std::size_t up = 0; for (const auto& [id, n] : nodes_) { up += n.up; } return up; }
	Node& leader_or(Node& n, GroupId g) {
		for (auto& [id, m] : nodes_) {
			auto s = state(m, g);
			if (s && s->role == Role::Leader && random_() % 8) { return m; }
		}
		return n;
	}

	void deliver(const Envelope& e) {
		auto& to = nodes_.at(e.to);
		if (!to.up || isolated_.count(e.from) || isolated_.count(e.to)) { return; }
		auto& l = to.loop_of(e.group);
		l.driver->receive(e.group, Receive{e.from, e.message});
		l.run(to.id);
	}
	void deliver_one() {
		if (network_.empty()) { return; }
		auto pick = random_() % std::min<std::size_t>(network_.size(), 4);  // reordered among the first few
		auto e = std::move(network_[pick]);
		network_.erase(network_.begin() + std::ptrdiff_t(pick));
		if (e.at > now_ && random_() % 4) { network_.push_back(std::move(e)); return; }
		if (random_() % 25 == 0) { return; }  // dropped
		deliver(e);
	}
	// Completes some of one loop's IO in random order; the other loops' IO stays in flight.
	void complete_io(Loop& l, NodeId node, bool all = false) {
		for (int k = 0; l.held.held() && (all || k < 1 + int(random_() % 3)); ++k) {
			auto tickets = l.held.tickets();
			l.held.complete(tickets[random_() % tickets.size()]);
			l.run(node);
		}
	}
	void apply(Node& n, GroupId g) {
		auto& r = n.replicas[g];
		if (!r.applying) { return; }
		auto& l = n.loop_of(g);
		trace("node %llu group %llu applied(%llu)", (unsigned long long)n.id, (unsigned long long)g, (unsigned long long)*r.applying);
		l.driver->applied(g, Applied{*r.applying});
		r.applying.reset();
		l.run(n.id);
	}
	void toggle_partition(NodeId id) {
		if (isolated_.erase(id)) { return; }
		if (isolated_.empty()) { isolated_.insert(id); }
	}

	std::shared_ptr<Call> track(Node& n, GroupId g, Call::Kind kind) {
		auto call = std::make_shared<Call>();
		call->kind = kind; call->node = n.id; call->group = g;
		calls_.push_back(call);
		return call;
	}
	template <class T> void start(Node& n, std::shared_ptr<Call> call, task::task<T> work) {
		task::spawn(n.loop_of(call->group).ref(), std::move(work), [this, call](std::exception_ptr error, std::optional<T> value) {
			++call->completions; call->error = error;
			if constexpr (std::is_same_v<T, Placement>) {
				trace("node %llu propose %s: %s %llu:%llu", (unsigned long long)call->node, call->command.c_str(), error ? "error" : "placed",
					value ? (unsigned long long)value->term : 0ull, value ? (unsigned long long)value->index : 0ull);
			}
			if constexpr (std::is_same_v<T, Placement>) { call->placed = value; }
			if constexpr (std::is_same_v<T, ReadReady>) { call->ready = value; }
			if constexpr (std::is_same_v<T, RepairCompleted>) {
				call->repaired = value;
				if (value) { const auto wait = now_ - call->issued; repair_wait_total += wait; repair_wait_max = std::max(repair_wait_max, wait); on_repaired(nodes_.at(call->node), call->group, *value); }
			}
			if constexpr (std::is_same_v<T, CheckpointPublished>) { call->published = value; if (value) { on_published(nodes_.at(call->node), call->group, *value); } }
			// A refused repair rewinds nothing; a cancelled one may still complete (D8's row).
			if (error && call->kind == Call::repair && !call->cancel_requested) { auto& r = nodes_.at(call->node).replicas[call->group]; r.repairing.reset(); r.replayed = false; }
		});
	}
	void start(Node& n, std::shared_ptr<Call> call, task::task<void> work) {
		task::spawn(n.loop_of(call->group).ref(), std::move(work), [call](std::exception_ptr error) { ++call->completions; call->error = error; });
	}
	std::shared_ptr<Call> start_propose(Node& n, GroupId g, std::string command) {
		auto call = track(n, g, Call::propose);
		call->command = command;
		start(n, call, n.driver(g).propose(g, std::move(command), call->cancel.token()));
		n.loop_of(g).run(n.id);
		return call;
	}
	void propose(Node& n, GroupId g) {
		if (!n.up) { return; }
		++proposals;
		start_propose(n, g, "c" + std::to_string(++commands_));
	}
	void await_commit(Node& n, GroupId g) {
		if (!n.up) { return; }
		std::vector<std::shared_ptr<Call>> placed_calls;
		for (const auto& c : calls_) { if (c->placed && c->group == g) { placed_calls.push_back(c); } }
		for (auto c = checked_.rbegin(); c != checked_.rend() && placed_calls.size() < 16; ++c) { if ((*c)->placed && (*c)->group == g) { placed_calls.push_back(*c); } }
		if (placed_calls.empty()) { return; }
		auto source = placed_calls[random_() % placed_calls.size()];
		auto call = track(n, g, Call::committed);
		call->target = *source->placed;
		start(n, call, n.driver(g).committed(g, call->target, call->cancel.token()));
		n.loop_of(g).run(n.id);
	}
	void read(Node& n, GroupId g) {
		if (!n.up) { return; }
		auto call = track(n, g, Call::read);
		const auto& sequence = sequences_[g];
		call->floor = sequence.empty() ? 0 : sequence.rbegin()->first;
		start(n, call, n.driver(g).read(g, call->cancel.token()));
		n.loop_of(g).run(n.id);
	}
	// A recovering node's application restores the image its latest checkpoint captured.
	void restore(Node& n, GroupId g, j::ArtifactReader& reader) {
		std::string bytes(static_cast<std::size_t>(reader.descriptor().length), '\0');
		for (std::size_t at = 0; at < bytes.size();) { at += reader.read_at(at, std::span<char>(bytes.data() + at, bytes.size() - at)); }
		auto& r = n.replicas[g];
		r.applied = decode_image(bytes); r.base = r.applied.size(); ++restored;
		auto& sequence = sequences_[g];
		for (const auto& e : r.applied) {
			auto it = sequence.find(e.index);
			if (it == sequence.end() || it->second.term != e.term || it->second.payload != e.payload) { fail("node " + std::to_string(n.id) + " restored group " + std::to_string(g) + "'s index " + std::to_string(e.index) + " unlike the committed sequence"); break; }
		}
	}
	// A checkpoint of what the application applied, through an index every member's log
	// holds, so no member needs a snapshot install (the simulation sends none).
	void checkpoint(Node& n, GroupId g) {
		if (!n.up || witness(n.id, g)) { return; }
		auto& r = n.replicas[g];
		if (r.applying || r.repairing || r.applied.empty()) { return; }
		Index floor = r.applied.size();
		for (auto& [id, m] : nodes_) {
			if (witness(id, g)) { auto s = state(m, g); if (!s) { return; } floor = std::min<Index>(floor, s->committed); }
			else { floor = std::min<Index>(floor, m.replicas[g].applied.size()); }
		}
		if (floor != r.applied.size() || floor <= r.base) { return; }
		++checkpoints;
		auto call = track(n, g, Call::checkpoint);
		start(n, call, n.driver(g).checkpoint(g, application_cap, Capture{encode_image(r.applied), 0, floor, r.applied.back().term, r.epoch}, call->cancel.token()));
		n.loop_of(g).run(n.id);
	}
	// Track C, D10: the application restores a boundary at or below what it applied, often
	// below its latest checkpoint, which the Core must refuse.
	// Half the repairs go to a replica that has a checkpoint, so many land below it.
	void repair_somewhere(Node& n, GroupId g) {
		if (random_() % 2) {
			std::vector<std::pair<Node*, GroupId>> checkpointed;
			for (auto& [id, m] : nodes_) { for (auto& [h, r] : m.replicas) { if (m.up && r.base) { checkpointed.emplace_back(&m, h); } } }
			if (!checkpointed.empty()) { auto [m, h] = checkpointed[random_() % checkpointed.size()]; repair(*m, h); return; }
		}
		repair(n, g);
	}
	void repair(Node& n, GroupId g) {
		if (!n.up || witness(n.id, g)) { return; }
		auto& r = n.replicas[g];
		if (r.applying || r.repairing) { return; }
		const Index applied = r.applied.size();
		if (applied < r.base) { return; }
		const Index b = r.base && random_() % 2 ? random_() % r.base : r.base + random_() % (applied - r.base + 1);
		const Term term = b ? r.applied[b - 1].term : 0;
		++repairs; repairs_below_base += b < r.base;
		r.repairing = b;
		auto call = track(n, g, Call::repair);
		call->boundary = {b, term}; call->issued = now_;
		start(n, call, n.driver(g).repair(g, call->boundary, call->cancel.token()));
		n.loop_of(g).run(n.id);
	}
	void transfer(GroupId g) {
		for (auto& [id, n] : nodes_) {
			auto s = state(n, g);
			if (!s || s->role != Role::Leader) { continue; }
			NodeId target = 1 + random_() % 2;
			if (target == id) { target = target == 1 ? 2 : 1; }
			if (!witnessed_.count(g)) { target = 1 + random_() % 3; if (target == id) { target = 1 + target % 3; } }
			++transfers;
			trace("node %llu group %llu transfers to %llu", (unsigned long long)id, (unsigned long long)g, (unsigned long long)target);
			auto call = track(n, g, Call::transfer);
			start(n, call, n.driver(g).transfer_leadership(g, target, call->cancel.token()));
			n.loop_of(g).run(n.id);
			return;
		}
	}
	void cancel_one() {
		std::vector<std::shared_ptr<Call>> live;
		for (const auto& c : calls_) { if (!c->completions && !c->cancel_requested && c->kind != Call::open) { live.push_back(c); } }
		if (live.empty()) { return; }
		auto call = live[random_() % live.size()];
		call->cancel_requested = true; ++cancelled;
		trace("cancel a call of kind %d on node %llu", int(call->kind), (unsigned long long)call->node);
		call->cancel.request();
		auto& n = nodes_.at(call->node);
		if (n.up) { n.loop_of(call->group).run(n.id); }
	}

	// After each step: no call completed twice, and a cancelled call yields no value unless it
	// completed before its cancel.
	void check_calls() {
		for (const auto& c : calls_) {
			if (c->completions > 1) { fail("a call completed twice"); }
			if (c->completions && c->error && (c->placed || c->ready)) { fail("a call completed with both a value and an error"); }
		}
		auto c = calls_.begin();
		while (c != calls_.end() && (*c)->completions) { checked_.push_back(*c); c = calls_.erase(c); }
	}
	bool healed_everywhere(GroupId g, Placement p) {
		for (auto& [id, n] : nodes_) {
			// A witness commits the entry too, and acknowledges it itself.
			if (witness(id, g)) { auto st = state(n, g); if (!st || st->committed < p.index || st->applied != st->committed) { return false; } continue; }
			const auto& applied = n.replicas[g].applied;
			if (applied.size() < p.index || applied[p.index - 1].term != p.term || applied[p.index - 1].payload != "healed") { return false; }
		}
		return true;
	}
	// Each completed call against its group's one committed sequence.
	void check_final() {
		calls_.insert(calls_.end(), checked_.begin(), checked_.end());
		for (const auto& c : calls_) {
			const auto& sequence = sequences_[c->group];
			if (c->error) {
				try { std::rethrow_exception(c->error); }
				catch (const superseded&) {
					++superseded_count;
					auto e = sequence.find(c->target.index);
					if (e == sequence.end() || e->second.term == c->target.term) { fail("committed() superseded though its entry committed with its term"); }
				}
				catch (const task::cancelled&) { repairs_cancelled += c->kind == Call::repair; }
				catch (const std::exception&) { refused += c->kind == Call::repair; }
				continue;
			}
			if (c->kind == Call::propose && c->placed) {
				++placed;
				auto e = sequence.find(c->placed->index);
				if (e != sequence.end() && e->second.term == c->placed->term && e->second.payload != c->command) { fail("a Placement's entry committed with another command"); }
			} else if (c->kind == Call::committed) {
				++commits;
				auto e = sequence.find(c->target.index);
				if (e == sequence.end() || e->second.term != c->target.term) { fail("committed() completed though its entry did not commit with its term"); }
			} else if (c->kind == Call::repair && c->repaired) {
				++repaired;
			} else if (c->kind == Call::read && c->ready) {
				++reads;
				if (c->ready->index < c->floor) { fail("a read's index is below an entry applied before it was issued"); }
			}
		}
	}

	std::mt19937_64 random_;
	std::vector<GroupId> groups_;
	std::set<GroupId> witnessed_;  // groups whose third member is a witness
	std::map<NodeId, Node> nodes_;
	std::deque<Envelope> network_;
	std::set<NodeId> isolated_;
	std::uint64_t now_ = 1;
	std::uint64_t commands_ = 0;
	std::map<GroupId, std::map<Index, Entry>> sequences_;
	std::map<GroupId, std::map<Term, NodeId>> leaders_;
	std::vector<std::shared_ptr<Call>> calls_, checked_;
};

void NodeSink::operator()(GroupId g, Send notice) { sim->on_send(*node, g, std::move(notice)); }
void NodeSink::operator()(GroupId g, Committed notice) { sim->on_committed(*node, g, std::move(notice)); }
void NodeSink::operator()(GroupId g, RoleChanged notice) { sim->on_role(*node, g, notice); }
void NodeSink::operator()(GroupId g, CheckpointPublished notice) { sim->on_published(*node, g, notice); }
void NodeSink::operator()(GroupId g, RepairCompleted notice) { sim->on_repaired(*node, g, notice, true); }
void NodeSink::operator()(GroupId g, Fenced notice) { sim->on_fenced(*node, g, notice); }
void NodeSink::operator()(GroupId g, SnapshotNeeded notice) {
	++sim->snapshots_needed[g];
	trace("node %llu group %llu SnapshotNeeded for peer %llu", (unsigned long long)node->id, (unsigned long long)g, (unsigned long long)notice.peer);
}
void NodeSink::operator()(GroupId, SendTransfer) { fail("a transfer frame without snapshots"); }
void NodeSink::operator()(GroupId, ValidateImage) { fail("a validation without snapshots"); }
void NodeSink::operator()(GroupId g, ActivateImage) {
	if (sim->witness(node->id, g)) { fail("a witness member was asked to activate an image"); }
}

}  // namespace

// Seeds that once failed, each at its failing step count, run beside every range of seeds.
constexpr std::pair<std::uint64_t, std::uint64_t> regressions[] = {{399, 3000}};

// Runs `seeds` seeds of `steps` steps from `first`, then the regression seeds outside that range.
bool run(std::uint64_t seeds, std::uint64_t steps, std::uint64_t first) {
	std::uint64_t proposals = 0, placed = 0, commits = 0, superseded_count = 0, reads = 0, cancelled = 0, crashes = 0, transfers = 0, elections = 0, repartitions = 0;
	std::uint64_t checkpoints = 0, restored = 0, repairs = 0, below = 0, repaired = 0, refused = 0, repairs_cancelled = 0, pending = 0, wait_total = 0, wait_max = 0, boots[5] = {};
	auto one = [&](std::uint64_t seed, std::uint64_t length) {
		current_seed = seed;
		Simulation sim(seed);
		sim.run(length);
		if (!failures) { sim.heal(); }
		proposals += sim.proposals; placed += sim.placed; commits += sim.commits; superseded_count += sim.superseded_count;
		reads += sim.reads; cancelled += sim.cancelled; crashes += sim.crashes; transfers += sim.transfers; elections += sim.elections; repartitions += sim.repartitions;
		checkpoints += sim.checkpoints; restored += sim.restored; repairs += sim.repairs; below += sim.repairs_below_base; repaired += sim.repaired; refused += sim.refused;
		repairs_cancelled += sim.repairs_cancelled; pending += sim.repairs_pending; wait_total += sim.repair_wait_total; wait_max = std::max(wait_max, sim.repair_wait_max);
		for (int l = 0; l < 5; ++l) { boots[l] += sim.boots_by_loops[l]; }
	};
	for (std::uint64_t seed = first; seed < first + seeds && !failures; ++seed) { one(seed, steps); }
	std::uint64_t replayed = 0;
	for (auto [seed, length] : regressions) {
		if (failures || (seed >= first && seed < first + seeds && steps == length)) { continue; }
		one(seed, length); ++replayed;
	}
	std::printf("driver simulation: %llu seeds x %llu steps (+%llu regression seeds), hosts booted with 2/3/4 loops %llu/%llu/%llu times (%llu re-partitions), %llu elections, %llu crashes, %llu proposals (%llu placed), %llu commits awaited (%llu superseded), %llu reads, %llu transfers, %llu cancels: %s\n",
		(unsigned long long)seeds, (unsigned long long)steps, (unsigned long long)replayed, (unsigned long long)boots[2], (unsigned long long)boots[3], (unsigned long long)boots[4], (unsigned long long)repartitions,
		(unsigned long long)elections, (unsigned long long)crashes, (unsigned long long)proposals, (unsigned long long)placed,
		(unsigned long long)commits, (unsigned long long)superseded_count, (unsigned long long)reads, (unsigned long long)transfers, (unsigned long long)cancelled, failures ? "FAILED" : "ok");
	std::printf("  %llu checkpoints (%llu images restored), %llu repairs requested (%llu below the checkpoint): %llu completed, %llu refused, %llu cancelled, %llu pending at a seed's end; completed after %.1f ms on average, %llu ms at the longest\n",
		(unsigned long long)checkpoints, (unsigned long long)restored, (unsigned long long)repairs, (unsigned long long)below, (unsigned long long)repaired, (unsigned long long)refused,
		(unsigned long long)repairs_cancelled, (unsigned long long)pending, repaired ? double(wait_total) / double(repaired) : 0.0, (unsigned long long)wait_max);
	return failures == 0;
}

}  // namespace kronuz::consensus::driver_simulation
