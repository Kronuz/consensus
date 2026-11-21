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

// Randomized simulation of a consensus group (strata Track C, T2; C5's first version).
// Each node's storage is modeled as durable hard state and log plus at most one pending
// batch; a crash restarts the node and keeps or loses that batch at random. Messages are
// delayed, reordered, duplicated, dropped, and partitioned; each node's clock runs at its
// own rate (0.5x to 2x). Half the seeds quiesce (docs/mechanisms/quiescence.md): a group at
// rest stops its timers, and a node-pair detector raises LeaderGone at a node for every node it
// lost its connection to (down, isolated or cut). After every step the oracles check:
//   - election safety: at most one leader per term;
//   - leader completeness: a new leader's durable log holds every entry any node applied;
//   - log matching: two durable logs that agree on an index's term agree before it;
//   - state machine safety: every node's applied sequence is a prefix of one sequence,
//     also after a repair (Track C, D10) rewinds a node's application and replays it;
//   - one grant per voter per term, across restarts;
//   - durability when sent: a vote grant, or an append acknowledgement, is backed by the
//     sender's durable state at the moment it is sent.
// After the run the group heals, elects a leader, and commits a new entry. Then the same
// group runs through the Driver on journal hosts (driver_simulation.cc), a tenth of the seeds.
//   simulation_test [seeds] [steps] [first seed]   (defaults 200, 10000, 1; SIM_DRIVER_SEEDS
//   sets the Driver's seed count)
#include "consensus/core.h"
#include "consensus/membership.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

using namespace kronuz::consensus;

namespace {

std::uint64_t failures = 0;
std::uint64_t current_seed = 0;

void fail(const std::string& what) {
	if (failures++ < 20) { std::fprintf(stderr, "seed %llu: %s\n", (unsigned long long)current_seed, what.c_str()); }
}

FixedConfiguration config(NodeId local, std::size_t voters) {
	FixedConfiguration c;
	c.cluster[0] = 'S';
	c.configuration[0] = 'G';
	c.local = local;
	for (NodeId id = 1; id <= voters; ++id) { c.voters.push_back(id); }
	return c;
}

struct Durable {
	HardState hard;
	Index base = 0;  // the checkpoint's boundary; 0 without one
	Term base_term = 0;
	std::optional<Membership> membership;  // the checkpoint's base membership, as recorded
	std::vector<Entry> image;  // the application image: every entry through `base`
	std::vector<Entry> log;  // log[i] has index base + i + 1
	void apply(const StorageBatch& batch) {
		if (batch.hard) { hard = *batch.hard; }
		if (batch.log) {
			log.resize(std::size_t(batch.log->replace_from - base - 1));
			for (const auto& e : batch.log->entries) { log.push_back(e); }
		}
	}
	void install(const RecoveredState& state, std::vector<Entry> through) {
		hard = state.hard; base = state.base_index; base_term = state.base_term;
		membership = state.membership; log = state.entries; image = std::move(through);
	}
	Index last() const { return base + log.size(); }
	// The entry at `index`, from the image or the log; null past the end.
	const Entry* entry(Index index) const {
		if (index == 0 || index > last()) { return nullptr; }
		return index <= base ? &image.at(index - 1) : &log[index - base - 1];
	}
};

struct Node {
	NodeId id;
	bool joiner = false;  // started from a join record (Track C, D5), no membership
	Limits limits;  // this node's own limits; nodes differ in their command limit (R16)
	double rate;  // local clock rate
	double local = 0;  // local clock, advanced at `rate`
	std::unique_ptr<Core> core;
	Durable durable;
	std::optional<Persist> pending;
	std::optional<PersistCheckpoint> checkpointing;
	struct Install { PersistInstall persist; std::vector<Entry> image; NodeId from; SnapshotKey key; };
	std::optional<Install> installing;  // an install from `from`, with the image it brings
	std::optional<Committed> applying;
	std::vector<std::string> applied;  // commands applied since the last restart
	std::vector<Entry> applied_entries;  // every entry applied since the last restart
	std::optional<ClosedState> closed;  // the last record it closed with, kept even once stale
	bool up = true;
};

struct Envelope {
	NodeId from, to;
	Message message;
	std::uint64_t at;  // deliverable from this global time
};

class Simulation {
public:
	Simulation(std::uint64_t seed, std::size_t voters, bool membership, bool witness = false, bool quiesce = false) : random_(seed), voters_(voters), membership_changes_(membership) {
		timing_.quiesce = quiesce;
		// D9: a 2 + witness group, founded with its roles.
		if (witness && voters == 3) { founding_.members.back().role = MemberRole::Witness; witness_ = true; }
		// Small RPC batches leave replication partial across leader changes.
		limits_.rpc_entries = 1 + random_() % 4;
		for (NodeId id = 1; id <= voters; ++id) {
			auto& n = nodes_[id];
			n.id = id;
			n.rate = 0.5 + double(random_() % 1501) / 1000.0;
			n.limits = node_limits();
			restart(n);
		}
	}

	void run(std::size_t steps) {
		for (std::size_t step = 0; step < steps && !failures; ++step) {
			advance(1 + random_() % 5);
			const auto pick = random_() % 1000;
			auto pick_node = std::next(nodes_.begin(), std::ptrdiff_t(random_() % nodes_.size()));
			NodeId id = pick_node->first;
			auto& n = pick_node->second;
			// Every seed crashes, partitions, reads, checkpoints, and transfers; membership seeds
			// also change members and limits.
			if (pick < 80) {
				tick(n);
			} else if (pick < 180) {
				for (auto k = 1 + random_() % 3; k-- > 0;) { propose(leader_or(n)); }
			} else if (pick < 450) {
				deliver_one();
			} else if (pick < 600) {
				if (n.up && (n.pending || n.checkpointing || n.installing)) { complete_storage(n); }
			} else if (pick < 700) {
				if (n.up && n.applying) { complete_apply(n); }
			} else if (pick < 701) {
				repair(n);
			} else if (pick < 702) {
				crash(random_() % 2 ? leader_or(n) : n);
			} else if (pick < 707) {
				if (random_() % 2) { toggle_partition(random_() % 2 ? leader_or(n).id : id); }
			} else if (pick < 719) {
				if (membership_changes_) { change_membership(); }
			} else if (pick < 723) {
				transfer();
			} else if (pick < 735) {
				read(n);
			} else if (pick < 741) {
				checkpoint(n);
			} else if (pick < 760) {
				serve_snapshot();
			} else if (pick < 775) {
				close(n);
			} else {
				for (auto& [i, m] : nodes_) { tick(m); }
				if (!n.up && random_() % 8 == 0) { restart(n); }  // a crashed node comes back
			}
			if (membership_changes_ && step == forced_at_) { force_replaced_configuration(); }
			if (forced_leader_ && step == forced_at_ + 1500) { isolated_.erase(*forced_leader_); forced_leader_.reset(); }

			detect();
			if (step % 16 == 0) { check_logs(); check_memberships(); }
			bool any = false;
			for (auto& [i, m] : nodes_) { any |= m.up && m.core->role() == Role::Leader; rested_steps += m.up && m.core->resting(); }
			led_steps += any;
		}
		check_logs();
	}

	// Heals partitions and crashes; clocks keep their skew. Pre-vote (Track C, D7) keeps a
	// node with a stale log and a fast clock from disrupting elections: before it, seed 217
	// at 2,000 steps stayed leaderless while its vote requests reset slower voters' timers.
	void quiet_round() {
		advance(5);
		detect();
		for (auto& [id, n] : nodes_) {
			tick(n);
			if (n.up && (n.pending || n.checkpointing || n.installing)) { complete_storage(n); }
			if (n.up && n.applying) { complete_apply(n); }
			serve_snapshot();
		}
		while (!network_.empty()) { deliver_front(); }
	}
	void heal() {
		isolated_.clear();
		for (auto& [id, n] : nodes_) {
			if (!n.up) { restart(n); }
		}
		// T2's forced self-removal, once everything is up.
		for (int round = 0; round < 1000 && !failures && !force_self_removal(); ++round) { quiet_round(); }
		for (int round = 0; round < 300 && !failures; ++round) {
			quiet_round();
			if (!removal_leader_) { continue; }
			auto& l = nodes_.at(*removal_leader_);
			if (!l.up || votes_in(l.core->committed_membership(), l.id)) { continue; }
			// The removal committed: count what was still in flight.
			bool waiting = false;
			for (const auto& [id, m] : nodes_) { waiting |= l.core->awaiting_snapshot(id).has_value(); }
			removals_with_snapshot += waiting;
			removals_with_read += read_floors_.count(removal_read_) > 0;
			removal_leader_.reset();
		}
		removal_leader_.reset();
		hold_snapshots_ = false;
		cut_.clear();
		if (membership_changes_) { force_replacement(); }
		for (auto& [id, n] : nodes_) {
			if (!n.up) { restart(n); }  // the forced crash's node returns with the rest
		}
		for (int round = 0; round < 4000 && !failures; ++round) {
			quiet_round();
			if (round >= 2000 && round % 50 == 0 && applied_healed() < healed_quorum()) {  // until a majority applies it
				for (auto& [id, n] : nodes_) {
					if (n.up && n.core->role() == Role::Leader) { act(n, n.core->step(Propose{++request_, "healed"})); }
				}
			}
		}
		if (applied_healed() < healed_quorum()) {
			fail("healed group did not commit a new entry on a majority");
			for (auto& [id, n] : nodes_) {
				if (!n.up) { continue; }
				std::fprintf(stderr, "  node %llu: up %d role %d term %llu committed %llu applied %llu last %llu durable log %zu pending %d applying %d\n", (unsigned long long)id, n.up,
					int(n.core->role()), (unsigned long long)n.core->term(), (unsigned long long)n.core->committed(), (unsigned long long)n.core->applied(),
					(unsigned long long)n.core->last_index(), n.durable.log.size(), n.pending.has_value(), n.applying.has_value());
				std::fprintf(stderr, "    base %llu members:", (unsigned long long)n.durable.base);
				for (const auto& x : n.core->membership().members) { std::fprintf(stderr, " %llu/%d", (unsigned long long)x.incarnation, int(x.role)); }
				for (const auto& x : n.core->membership().members) { if (auto pr = n.core->progress(x.incarnation)) { std::fprintf(stderr, " p%llu=%llu", (unsigned long long)x.incarnation, (unsigned long long)*pr); } }
				std::fprintf(stderr, " snapshots queued %zu\n", snapshot_requests_.size());
				std::fprintf(stderr, "    node limits: command %zu rpc %zu entries %zu; group command %zu rpc %zu", n.limits.command_bytes, n.limits.rpc_bytes, n.limits.rpc_entries,
					n.core->group_limits().command_bytes, n.core->group_limits().rpc_bytes);
				if (n.core->role() == Role::Leader) {
					for (const auto& [mid, m] : nodes_) {
						if (mid == id || !m.up) { continue; }
						if (auto e = n.durable.entry(m.core->last_index() + 1)) { std::fprintf(stderr, "; next for %llu: kind %d %zu bytes", (unsigned long long)mid, int(e->kind), e->payload.size()); }
					}
				}
				std::fprintf(stderr, "\n");
			}
		}
		check_logs();
	}

	std::uint64_t restarts = 0, kept = 0, lost = 0, elections = 0, led_steps = 0, repairs = 0, transfers = 0, changes_proposed = 0, memberships_committed = 0;
	std::uint64_t reads = 0, checkpoints = 0, installs = 0, limit_changes = 0, replaced_configurations = 0, forced_removals = 0;
	std::uint64_t forced_replacements = 0, removals_with_snapshot = 0, removals_with_read = 0, delayed_timeout_nows = 0;
	std::uint64_t held_back = 0;  // empty appends to peers below their next entry (M8)
	std::uint64_t closes = 0, resumed = 0, stale = 0;  // closed at rest; reopens that resumed their term; records refused as stale
	std::uint64_t leader_gones = 0, rested_steps = 0;  // LeaderGone raised; node-steps spent at rest
	std::uint64_t held_back_total() const { auto total = held_back; for (const auto& [_, n] : nodes_) { if (n.core) { total += n.core->held_back(); } } return total; }
	std::map<int, std::uint64_t> reject_reasons;
	Term max_term() const {
		Term t = 0;
		for (const auto& [id, n] : nodes_) { if (n.up) { t = std::max(t, n.core->term()); } }
		return t;
	}

private:
	// The current leader, if any node believes it leads; else `n`.
	Node& leader_or(Node& n) {
		for (auto& [id, m] : nodes_) {
			if (m.up && m.core->role() == Role::Leader) { return m; }
		}
		return n;
	}
	std::size_t applied_healed() const {
		const Membership* latest = &founding_;
		if (!memberships_.empty()) { latest = &memberships_.rbegin()->second; }
		std::size_t done = 0;
		for (const auto& [id, n] : nodes_) {
			done += votes_in(*latest, id) && std::find(n.applied.begin(), n.applied.end(), "healed") != n.applied.end();
		}
		return done;
	}
	std::uint64_t local_time(const Node& n) const { return std::uint64_t(n.local); }
	void advance(std::uint64_t dt) {
		now_ += dt;
		for (auto& [id, n] : nodes_) { n.local += double(dt) * n.rate; }
	}

	FixedConfiguration node_config(const Node& n) const {
		auto c = config(n.id, voters_);
		if (n.joiner) { c.voters.clear(); }
		return c;
	}
	void restart(Node& n) {
		RecoveredState state;
		state.configuration = node_config(n);
		state.hard = n.durable.hard;
		state.base_index = state.applied_index = n.durable.base;
		state.base_term = n.durable.base_term;
		state.entries = n.durable.log;
		if (n.durable.base) { state.membership = n.durable.membership; }
		else if (n.joiner) { state.membership = Membership{}; }  // the join record: no membership yet
		else if (witness_) { state.membership = founding_; }
		bool resuming = false;
		try {
			n.core = std::make_unique<Core>(node_config(n), state, n.limits, timing_);
			// Half the restarts hand the core the last closed record, matching or stale.
			if (n.closed && random_() % 2) { resuming = n.core->reopen(*n.closed); resumed += resuming; stale += !resuming; }
		} catch (const std::exception& error) {
			std::string detail = "node " + std::to_string(n.id) + " cannot reopen: " + error.what() + "; hard term " + std::to_string(state.hard.term) + " base " +
				std::to_string(state.base_index) + " command limit " + std::to_string(n.limits.command_bytes) + "; entries";
			for (const auto& e : state.entries) { detail += " " + std::to_string(e.index) + "/" + std::to_string(e.term) + "/" + std::to_string(int(e.kind)) + ":" + std::to_string(e.payload.size()); }
			fail(detail);
			throw;
		}
		n.pending.reset();
		n.checkpointing.reset();
		n.installing.reset();
		n.applying.reset();
		n.applied_entries = n.durable.image;  // the application restores its image
		n.applied.clear();
		for (const auto& e : n.applied_entries) {
			if (e.kind == EntryKind::Command) { n.applied.push_back(e.payload); }
		}
		n.up = true;
		act(n, n.core->step(Tick{local_time(n), election_delay()}));
		const Term term = n.core->term();
		act(n, n.core->step(Start{}));
		if (resuming && n.core->term() != term) { fail("node " + std::to_string(n.id) + " reopened into another term"); }
		// The hello tells each side the other's node limits; the host keeps the last ones it
		// heard, so a restarted core learns a down peer's too.
		for (auto& [id, m] : nodes_) {
			if (id == n.id) { continue; }
			act(n, n.core->step(PeerLimits{id, m.limits}));
			if (m.up && m.core) { act(m, m.core->step(PeerLimits{n.id, n.limits})); }
		}
	}
	Limits node_limits() {
		Limits l = limits_;
		l.command_bytes = 16 + random_() % 49;
		// One node in four takes RPCs only a few commands wide, so entries are held back
		// from it at random and it catches up by install (M8, condition 22).
		// Never below a membership entry's encoding, which validate_limits refuses.
		if (random_() % 4 == 0) { l.rpc_bytes = membership_bytes(l.members) + random_() % 64; l.rpc_entries = 1 + random_() % l.rpc_entries; }
		return l;
	}
	// Track C's profile rules: a barrier here takes about 60 units (a persist completes at
	// random) and a delivery up to 60, so an election round (two barriers and a round trip)
	// takes about 240; elections are drawn from 600–1,200, the heartbeat is 60.
	std::uint64_t election_delay() { return timing_.election_min + random_() % (timing_.election_max - timing_.election_min + 1); }

	void crash(Node& n) {
		if (!n.up) {
			restart(n);
			return;
		}
		++restarts;
		if (n.pending || n.checkpointing || n.installing) {
			if (random_() % 2) {  // the operation reached the disk; its completion did not
				if (n.pending) { n.durable.apply(n.pending->batch); }
				if (n.checkpointing) { n.durable.install(n.checkpointing->state, image(n, n.checkpointing->state.base_index)); }
				if (n.installing) { n.durable.install(n.installing->persist.state, n.installing->image); }
				++kept;
			} else {
				++lost;
			}
		}
		// The transport tells a sender its stream broke.
		if (n.installing) {
			auto& from = nodes_.at(n.installing->from);
			if (from.up) { act(from, from.core->step(SnapshotTransferFailed{n.id, n.installing->key})); }
		}
		n.up = false;
		std::erase_if(lost_, [&](const auto& pair) { return pair.first == n.id; });  // its detector restarts with it
		held_back += n.core->held_back();
		n.core.reset();
		if (random_() % 2) { restart(n); }
	}

	// Quiescence's node-pair detector: an up node hears LeaderGone{b} once for each node b it
	// lost its connection to (down, isolated or cut), within a step of the loss.
	void detect() {
		if (!timing_.quiesce) { return; }
		for (auto& [a, n] : nodes_) {
			if (!n.up) { continue; }
			for (auto& [b, m] : nodes_) {
				if (a == b) { continue; }
				if (m.up && !isolated_.count(a) && !isolated_.count(b) && !cut_.count({a, b})) { lost_.erase({a, b}); continue; }
				if (lost_.insert({a, b}).second) { ++leader_gones; act(n, n.core->step(LeaderGone{b})); }
			}
		}
	}
	void tick(Node& n) {
		if (!n.up) { return; }
		act(n, n.core->step(Tick{local_time(n), election_delay()}));
	}
	void propose(Node& n) {
		if (!n.up) { return; }
		auto command = "c" + std::to_string(++request_);
		command.resize(command.size() + random_() % 56, '.');  // some exceed a member's command limit
		act(n, n.core->step(Propose{request_, std::move(command)}));
	}
	// The applied entries through `through`: what a checkpoint's application image holds.
	static std::vector<Entry> image(const Node& n, Index through) {
		return {n.applied_entries.begin(), n.applied_entries.begin() + std::ptrdiff_t(std::min<std::size_t>(through, n.applied_entries.size()))};
	}
	void complete_storage(Node& n) {
		if (n.pending) { complete_persist(n); return; }
		if (n.checkpointing) {
			auto c = std::move(*n.checkpointing); n.checkpointing.reset();
			n.durable.install(c.state, image(n, c.state.base_index));
			++checkpoints;
			act(n, n.core->step(Persisted{c.token}));
			return;
		}
		if (n.installing) {
			auto install = std::move(*n.installing); n.installing.reset();
			if (n.applying) { complete_apply(n); }  // activation follows the outstanding delivery
			n.durable.install(install.persist.state, install.image);
			auto published = n.core->step(Persisted{install.persist.token});
			const bool activate = std::any_of(published.begin(), published.end(), [](const Action& a) { return std::holds_alternative<ActivateInstall>(a); });
			act(n, std::move(published));
			if (!n.up || !activate) { return; }
			n.applied_entries = install.image;
			n.applied.clear();
			for (const auto& e : n.applied_entries) {
				if (e.kind == EntryKind::Command) { n.applied.push_back(e.payload); }
			}
			check_prefixes();
			auto completed = n.core->step(InstallActivated{install.persist.token});
			for (const auto& a : completed) {
				if (std::holds_alternative<InstallCompleted>(a)) {
					++installs;
					network_.push_back({n.id, install.from, SnapshotResponse{n.core->term(), install.key, SnapshotReply::Installed}, now_ + random_() % 60});
				}
			}
			act(n, std::move(completed));
		}
	}
	void complete_persist(Node& n) {
		auto p = std::move(*n.pending);
		n.pending.reset();
		const Index before = n.durable.hard.commit_index;
		if (p.batch.log) {
			for (Index i = p.batch.log->replace_from; i <= n.durable.last(); ++i) {
				auto old = n.durable.entry(i);
				if (old && old->kind == EntryKind::Configuration && (i - p.batch.log->replace_from >= p.batch.log->entries.size() || p.batch.log->entries[i - p.batch.log->replace_from] != *old)) {
					++replaced_configurations;
					break;
				}
			}
		}
		n.durable.apply(p.batch);
		if (n.durable.hard.commit_index > before && n.core->role() == Role::Leader) { check_commit(n, n.durable.hard.commit_index); }
		act(n, n.core->step(Persisted{p.token}));
	}
	void complete_apply(Node& n) {
		auto c = std::move(*n.applying);
		n.applying.reset();
		for (const auto& e : c.entries) {
			record_applied(e);
			if (e.kind == EntryKind::Configuration) { record_membership(e); }
			if (e.kind == EntryKind::Command) { n.applied.push_back(e.payload); }
			n.applied_entries.push_back(e);
		}
		check_prefixes();
		act(n, n.core->step(Applied{c.entries.back().index}));
	}

	// Track C, D1, D3: the leader adds a learner, promotes one (as a plain add or a
	// replacement whose leaving voter the core removes next), or removes a voter, itself
	// included; joiners start from a join record.
	void change_membership() {
		Node* leader = nullptr;
		for (auto& [id, m] : nodes_) {
			// Half the proposals wait for can_change_membership(); the rest only for an idle leader
			// with no configuration pending, leaving D3(a) to the core, so a mutant without it shows.
			const bool weak = random_() % 2;
			if (m.up && (weak ? m.core->role() == Role::Leader && !m.core->busy() && m.core->committed_membership() == m.core->membership()
							  : m.core->can_change_membership())) {
				leader = &m;
			}
		}
		if (!leader) { return; }
		Membership next = leader->core->membership();
		std::vector<NodeId> learners, voters;
		for (const auto& x : next.members) { (voting(x.role) ? voters : learners).push_back(x.incarnation); }
		const auto choice = random_() % 4;
		if (choice == 3) {
			// R16: lower or raise the group's limits beneath the node's.
			Limits l = leader->limits;
			l.reads = 1 + random_() % l.reads;
			l.uncommitted_entries = 16 + random_() % (l.uncommitted_entries - 15);
			l.command_bytes = 16 + random_() % (l.command_bytes - 15);
			next.limits = l;
			++limit_changes;
		} else if (choice == 0 && next.members.size() < 7) {
			const NodeId incarnation = next.next_incarnation++;
			next.members.push_back({incarnation, incarnation, MemberRole::Learner});
			auto& joiner = nodes_[incarnation];
			if (!joiner.id) {  // a new node, not a crashed one
				joiner.id = incarnation;
				joiner.joiner = true;
				joiner.rate = 0.5 + double(random_() % 1501) / 1000.0;
				joiner.limits = node_limits();
				joiner.local = leader->local;
				restart(joiner);
			}
		} else if (choice == 1 && !learners.empty() && !next.leaving) {
			const NodeId promoted = learners[random_() % learners.size()];
			if (auto p = leader->core->progress(promoted); !p || *p < leader->core->committed()) { return; }  // D3(c): caught up first
			for (auto& x : next.members) { if (x.incarnation == promoted) { x.role = MemberRole::Voter; } }
			if (voters.size() >= 5 || random_() % 2) { next.leaving = voters[random_() % voters.size()]; }  // a replacement
		} else if (choice == 2 && voters.size() > 3 && !next.leaving) {
			const NodeId removed = voters[random_() % voters.size()];
			std::erase_if(next.members, [&](const Member& x) { return x.incarnation == removed; });
		} else {
			return;
		}
		++changes_proposed;
		auto actions = leader->core->step(ChangeMembership{++request_, std::move(next)});
		for (const auto& a : actions) {
			if (auto r = std::get_if<Reject>(&a); r && r->request == request_ && std::getenv("SIM_DEBUG")) { ++reject_reasons[int(r->reason)]; }
		}
		act(*leader, std::move(actions));
	}
	// Track C, D6: a leader hands leadership to a random voter of its membership in force.
	void transfer() {
		for (auto& [id, m] : nodes_) {
			if (!m.up || m.core->role() != Role::Leader) { continue; }
			std::vector<NodeId> targets;
			for (const auto& x : m.core->membership().members) {
				if (x.role == MemberRole::Voter && x.incarnation != id) { targets.push_back(x.incarnation); }
			}
			if (targets.empty()) { return; }
			auto actions = m.core->step(TransferLeadership{++request_, targets[random_() % targets.size()]});
			transfers += !std::any_of(actions.begin(), actions.end(), [](const Action& a) { return std::holds_alternative<Reject>(a); });
			act(m, std::move(actions));
			return;
		}
	}
	// Track C, D10: the node restores its application at a boundary in [0, applied] and
	// replays from its own log; a leader of more than one voter or a busy node is refused.
	void repair(Node& n) {
		if (!n.up) { return; }
		const Index b = random_() % (n.applied_entries.size() + 1);
		const Term term = b ? n.applied_entries[b - 1].term : 0;
		auto actions = n.core->step(Repair{++request_, {b, term}});
		for (const auto& a : actions) {
			if (std::holds_alternative<RepairCompleted>(a)) {
				++repairs;
				n.applied_entries.resize(b);
				n.applied.clear();
				for (const auto& e : n.applied_entries) {
					if (e.kind == EntryKind::Command) { n.applied.push_back(e.payload); }
				}
			}
		}
		act(n, std::move(actions));
	}
	// A linearizable read: its index covers every commit any node knew when it was admitted.
	void read(Node& n) {
		auto& l = leader_or(n);
		if (!l.up || l.core->role() != Role::Leader) { return; }
		Index floor = 0;
		for (const auto& [id, m] : nodes_) { if (m.up) { floor = std::max(floor, m.core->committed()); } }
		read_floors_[++request_] = floor;
		act(l, l.core->step(Read{request_}));
	}
	void check_read(const ReadReady& ready) {
		auto it = read_floors_.find(ready.request);
		if (it == read_floors_.end()) { return; }
		++reads;
		if (ready.index < it->second) { fail("a read at index " + std::to_string(ready.index) + " misses commit " + std::to_string(it->second)); }
		read_floors_.erase(it);
	}
	// Closes a node at rest with its record and reopens it at once or later; a node with an
	// application gap checkpoints first.
	void close(Node& n) {
		if (!n.up || n.pending || n.checkpointing || n.installing || n.applying) { return; }
		auto closed = n.core->closed_state();
		if (!closed) { checkpoint(n); return; }
		const auto c = node_config(n);
		const ClosedIdentity id{c.cluster, c.configuration, 1, 1, n.id};
		n.closed = decode_closed_state(encode_closed_state(*closed, id), id);
		if (*n.closed != *closed) { fail("a closed record does not round-trip"); }
		++closes;
		held_back += n.core->held_back();
		n.core.reset(); n.up = false;
		if (random_() % 2) { restart(n); }
	}
	// A local checkpoint through what the node applied.
	void checkpoint(Node& n) {
		if (!n.up || n.applying) { return; }
		const Index through = n.core->applied();
		if (through <= n.durable.base || through > n.applied_entries.size()) { return; }
		const auto c = config(1, voters_);
		act(n, n.core->step(LocalCheckpoint{++request_, 1, through, n.applied_entries[through - 1].term, c.cluster, c.configuration, n.core->epoch()}));
	}
	// A leader's snapshot to a lagging peer: the image it checkpointed, with the membership
	// the snapshot carries for that peer; a transfer fails when the peer is unreachable.
	void serve_snapshot() {
		if (snapshot_requests_.empty() || hold_snapshots_) { return; }
		auto [from, peer, key] = snapshot_requests_.front();
		snapshot_requests_.pop_front();
		auto& l = nodes_.at(from);
		if (!l.up || l.core->role() != Role::Leader) { return; }
		auto ready = l.core->step(SnapshotSourceReady{peer, key});
		const bool transmit = std::any_of(ready.begin(), ready.end(), [](const Action& a) { return std::holds_alternative<SnapshotTransmit>(a); });
		act(l, std::move(ready));
		if (!l.up) { return; }
		if (!transmit) {
			// A busy leader answers nothing; a host keeps the request until the core takes the
			// source or drops the flight.
			if (auto waiting = l.core->awaiting_snapshot(peer); waiting && *waiting == key) { snapshot_requests_.push_back({from, peer, key}); }
			return;
		}
		auto it = nodes_.find(peer);
		if (it == nodes_.end() || !it->second.up || isolated_.count(from) || isolated_.count(peer) || random_() % 10 == 0) {
			act(l, l.core->step(SnapshotTransferFailed{peer, key}));
			return;
		}
		auto& t = it->second;
		const auto c = config(1, voters_);
		auto prepared = t.core->step(InstallPrepared{++request_, ++prepared_, from, key.leader_term, c.cluster, c.configuration, key.boundary, l.core->snapshot_membership(peer)});
		for (const auto& a : prepared) {
			if (auto install = std::get_if<PersistInstall>(&a)) { t.installing = Node::Install{*install, image(l, key.boundary.index), from, key}; }
			if (auto rejected = std::get_if<InstallRejected>(&a)) {
				const auto reply = rejected->reason == InstallRejectReason::CaughtUp ? SnapshotReply::CaughtUp : SnapshotReply::Rejected;
				network_.push_back({peer, from, SnapshotResponse{t.core->term(), key, reply}, now_ + random_() % 60});
			}
		}
		act(t, std::move(prepared));
	}
	// T2's forced schedule: a leader cut off from the rest appends a Configuration entry that
	// only it holds; the others elect a new leader and write past it, and once the cut heals
	// the entry is replaced, so its membership must roll back (D1).
	void force_replaced_configuration() {
		Node* leader = nullptr;
		for (auto& [id, m] : nodes_) { if (m.up && m.core->can_change_membership()) { leader = &m; } }
		if (!leader) { ++forced_at_; return; }  // try again next step
		Membership next = leader->core->membership();
		if (next.members.size() >= 7) { return; }
		const NodeId incarnation = next.next_incarnation++;
		next.members.push_back({incarnation, incarnation, MemberRole::Learner});
		auto& joiner = nodes_[incarnation];
		if (!joiner.id) {  // a new node, not a crashed one
			joiner.id = incarnation; joiner.joiner = true; joiner.rate = 1.0; joiner.local = leader->local; joiner.limits = node_limits();
			restart(joiner);
		}
		isolated_.insert(leader->id);
		forced_leader_ = leader->id;
		act(*leader, leader->core->step(ChangeMembership{++request_, std::move(next)}));
		if (leader->pending) { complete_storage(*leader); }
	}
	// T2's forced schedule: a leader removes itself while it reaches only one other voter, so
	// the removal must not commit until a quorum of the new membership holds it (D4).
	bool force_self_removal() {
		Node* leader = nullptr;
		for (auto& [id, m] : nodes_) { if (m.up && m.core->can_change_membership()) { leader = &m; } }
		if (!leader) { return false; }
		Membership next = leader->core->membership();
		std::vector<NodeId> others;
		for (const auto& x : next.members) { if (x.role == MemberRole::Voter && x.incarnation != leader->id) { others.push_back(x.incarnation); } }
		if (others.size() < 2 || next.leaving) { return true; }  // nothing to force here
		if (others.size() >= 4) {
			// Five voters: one voter is cut off and falls behind a checkpoint, so the removal
			// commits on the other three while a read and that voter's snapshot are in flight.
			const NodeId behind = others[random_() % others.size()];
			cut_.insert({leader->id, behind}); cut_.insert({behind, leader->id});
			const NodeId lid = leader->id;
			const Index target = leader->core->committed() + 3;
			for (int i = 0; i < 400 && !failures; ++i) {  // commit past what the cut voter holds
				auto& l = nodes_.at(lid);
				if (!l.up || l.core->role() != Role::Leader) { cut_.clear(); return false; }
				if (l.core->applied() >= target) { break; }
				if (i % 20 == 0) { propose(l); }
				quiet_round();
			}
			leader = &nodes_.at(lid);
			if (!leader->core->can_change_membership()) { cut_.clear(); return false; }
			checkpoint(*leader);
			if (leader->checkpointing) { complete_storage(*leader); }
			read(*leader);
			hold_snapshots_ = true;
			removal_leader_ = leader->id;
			removal_read_ = request_;
		} else {
			const NodeId reachable = others[random_() % others.size()];
			for (const auto& [id, m] : nodes_) {
				if (id != leader->id && id != reachable) { cut_.insert({leader->id, id}); cut_.insert({id, leader->id}); }
			}
		}
		std::erase_if(next.members, [&](const Member& x) { return x.incarnation == leader->id; });
		auto actions = leader->core->step(ChangeMembership{++request_, std::move(next)});
		if (std::any_of(actions.begin(), actions.end(), [](const Action& x) { return std::holds_alternative<Reject>(x); })) {
			if (std::getenv("SIM_DEBUG")) { for (const auto& x : actions) { if (auto r = std::get_if<Reject>(&x)) { std::fprintf(stderr, "forced removal refused: %d\n", int(r->reason)); } } }
			cut_.clear();
			return false;
		}
		++forced_removals;
		act(*leader, std::move(actions));
		return true;
	}
	// T2's forced schedule: a crash between a promotion and the removal it implies. The leader
	// promotes a caught-up learner in place of another voter, and fails once the promotion
	// commits, before the leaving voter's removal does.
	void force_replacement() {
		auto rounds = [&](int bound, auto&& done) {
			for (int i = 0; i < bound && !failures; ++i) { if (done()) { return true; } quiet_round(); }
			return done();
		};
		Node* leader = nullptr;
		if (!rounds(1000, [&] { for (auto& [id, m] : nodes_) { if (m.up && m.core->can_change_membership()) { leader = &m; return true; } } return false; })) { return; }
		Membership next = leader->core->membership();
		NodeId learner = 0;
		for (const auto& x : next.members) { if (x.role == MemberRole::Learner) { learner = x.incarnation; } }
		if (!learner) {
			if (next.members.size() >= 7) { return; }
			learner = next.next_incarnation++;
			next.members.push_back({learner, learner, MemberRole::Learner});
			auto& j = nodes_[learner];
			j.id = learner; j.joiner = true; j.rate = 1.0; j.local = leader->local; j.limits = node_limits();
			restart(j);
			act(*leader, leader->core->step(ChangeMembership{++request_, next}));
		}
		const NodeId lid = leader->id;
		auto& l = nodes_.at(lid);
		if (!rounds(1000, [&] { return l.up && l.core->can_change_membership() && l.core->progress(learner) && *l.core->progress(learner) >= l.core->committed(); })) { return; }
		Membership promote = l.core->membership();
		std::vector<NodeId> voters;
		for (const auto& x : promote.members) { if (x.role == MemberRole::Voter && x.incarnation != lid) { voters.push_back(x.incarnation); } }
		if (voters.empty() || promote.leaving) { return; }
		for (auto& x : promote.members) { if (x.incarnation == learner) { x.role = MemberRole::Voter; } }
		promote.leaving = voters[random_() % voters.size()];
		auto actions = l.core->step(ChangeMembership{++request_, promote});
		if (std::any_of(actions.begin(), actions.end(), [](const Action& x) { return std::holds_alternative<Reject>(x); })) { return; }
		act(l, std::move(actions));
		if (rounds(1000, [&] { return !l.up || l.core->committed_membership().leaving != 0; }) && l.up && l.core->committed_membership().leaving) {
			++forced_replacements;
			crash(l);
		}
	}
	// The membership in force on every quiet node is the one its durable state implies: the
	// base membership, then the newest Configuration entry in its log (D1).
	void check_memberships() {
		for (const auto& [id, n] : nodes_) {
			if (!n.up || n.pending || n.checkpointing || n.installing) { continue; }
			Membership m = n.durable.base && n.durable.membership ? *n.durable.membership : (n.joiner && !n.durable.base ? Membership{} : founding_);
			for (const auto& e : n.durable.log) {
				if (e.kind == EntryKind::Configuration) { m = decode_membership(e.payload); }
			}
			const auto& held = n.core->membership();
			if (held.members != m.members || held.leaving != m.leaving || held.next_incarnation != m.next_incarnation) {
				fail("node " + std::to_string(id) + "'s membership differs from its durable log's");
			}
		}
	}
	void toggle_partition(NodeId id) {
		if (!isolated_.erase(id) && isolated_.size() + 1 < nodes_.size()) { isolated_.insert(id); }
	}
	void deliver_one() {
		if (network_.empty()) { return; }
		const std::size_t at = random_() % network_.size();
		auto e = network_[at];
		if (e.at > now_) { return; }
		network_.erase(network_.begin() + std::ptrdiff_t(at));
		if (random_() % 10 == 0) { network_.push_back(e); }  // a duplicate later
		deliver(e);
	}
	void deliver_front() {
		auto e = std::move(network_.front());
		network_.pop_front();
		deliver(e);
	}
	void deliver(const Envelope& e) {
		auto& n = nodes_.at(e.to);
		if (!n.up || isolated_.count(e.from) || isolated_.count(e.to) || cut_.count({e.from, e.to})) { return; }
		auto out = n.core->step(Receive{e.from, e.message});
		if (std::getenv("SIM_TRACE") && n.id == NodeId(std::atoi(std::getenv("SIM_TRACE"))) && now_ > trace_after_) {
			for (const auto& x : out) {
				if (auto snd = std::get_if<Send>(&x)) { if (auto r = std::get_if<AppendResponse>(&snd->message)) { std::fprintf(stderr, "trace %llu -> %llu: success %d matched %llu hint %llu compacted %d term %llu\n", (unsigned long long)n.id, (unsigned long long)snd->peer, r->success, (unsigned long long)r->matched, (unsigned long long)r->next_hint, r->compacted.has_value(), (unsigned long long)r->term); } }
			}
			if (auto ar = std::get_if<AppendRequest>(&e.message)) { std::fprintf(stderr, "trace %llu <- %llu: append term %llu prev %llu/%llu n %zu commit %llu; out %zu actions\n", (unsigned long long)n.id, (unsigned long long)e.from, (unsigned long long)ar->term, (unsigned long long)ar->previous, (unsigned long long)ar->previous_term, ar->entries.size(), (unsigned long long)ar->commit, out.size()); }
		}
		act(n, std::move(out));
	}

	void act(Node& n, Actions actions) {
		for (auto& a : actions) {
			if (auto p = std::get_if<Persist>(&a)) {
				if (n.pending) { fail("two storage batches outstanding"); }
				n.pending = std::move(*p);
			} else if (auto s = std::get_if<Send>(&a)) {
				check_send(n, *s);
				if (std::holds_alternative<TimeoutNow>(s->message)) {  // T2's forced schedule: a delayed duplicate
					network_.push_back({n.id, s->peer, s->message, now_ + 600 + random_() % 600});
					++delayed_timeout_nows;
				}
				if (random_() % 20 == 0) { continue; }  // lost
				network_.push_back({n.id, s->peer, s->message, now_ + random_() % 60});
				while (network_.size() > 4096) { network_.pop_front(); }
			} else if (auto c = std::get_if<Committed>(&a)) {
				if (n.applying) { fail("two application batches outstanding"); }
				n.applying = std::move(*c);
			} else if (auto c = std::get_if<PersistCheckpoint>(&a)) {
				n.checkpointing = std::move(*c);
			} else if (auto ready = std::get_if<ReadReady>(&a)) {
				check_read(*ready);
			} else if (auto needed = std::get_if<SnapshotNeeded>(&a)) {
				snapshot_requests_.push_back({n.id, needed->peer, needed->key});
			} else if (auto r = std::get_if<RoleChanged>(&a)) {
				if (r->role == Role::Leader) { on_leader(n, r->term); }
			} else if (std::holds_alternative<Fenced>(a)) {
				fail("node " + std::to_string(n.id) + " fenced: " + std::get<Fenced>(a).reason);
			}
		}
	}

	// Oracles ---------------------------------------------------------------------------

	void on_leader(const Node& n, Term term) {
		++elections;
		auto [it, fresh] = leaders_.emplace(term, n.id);
		if (!fresh && it->second != n.id) { fail("two leaders in term " + std::to_string(term)); }
		// Leader completeness: the new leader holds every entry any node applied. Its no-op
		// is not durable yet, so compare against its durable log.
		// Leader completeness covers entries committed in earlier terms. A leader's RoleChanged
		// arrives once its no-op persists, by when a later term may have committed more.
		for (const auto& [index, entry] : committed_) {
			if (auto at = commit_terms_.find(index); at != commit_terms_.end() && at->second >= term) { continue; }
			if (auto held = n.durable.entry(index); !held || *held != entry) {
				fail("leader " + std::to_string(n.id) + " of term " + std::to_string(term) + " lacks applied entry " + std::to_string(index));
				if (std::getenv("SIM_DEBUG")) {
					for (const auto& [i, m] : nodes_) {
						std::fprintf(stderr, "  node %llu up %d joiner %d term %llu voted %llu base %llu last %llu commit %llu applied %zu cmd-limit %zu members:", (unsigned long long)i, m.up, m.joiner,
							(unsigned long long)m.durable.hard.term, (unsigned long long)m.durable.hard.voted_for.value_or(0), (unsigned long long)m.durable.base, (unsigned long long)m.durable.last(),
							(unsigned long long)m.durable.hard.commit_index, m.applied_entries.size(), m.limits.command_bytes);
						if (m.core) { for (const auto& x : m.core->membership().members) { std::fprintf(stderr, " %llu/%d", (unsigned long long)x.incarnation, int(x.role)); } }
						std::fprintf(stderr, "\n");
					}
				}
				return;
			}
		}
	}
	void record_applied(const Entry& e) {
		auto [it, fresh] = committed_.emplace(e.index, e);
		if (!fresh && it->second != e) { fail("two different entries applied at index " + std::to_string(e.index)); }
	}
	// Commit safety: when a leader commits an index, the entry there is already durable on a
	// quorum of the voters of the membership it uses, itself counted only while it is one.
	void check_commit(const Node& leader, Index index) {
		for (Index i = index; i > 0 && !commit_terms_.count(i); --i) { commit_terms_[i] = leader.durable.hard.term; }
		// The membership its durable log puts in force, not the core's own view of it.
		Membership m = leader.durable.base ? leader.durable.membership.value_or(founding_) : founding_;
		for (const auto& e : leader.durable.log) {
			if (e.kind == EntryKind::Configuration) { m = decode_membership(e.payload); }
		}
		if (m.members.empty()) { return; }
		const Entry& entry = *leader.durable.entry(index);
		std::size_t holding = 0;
		for (const auto& x : m.members) {
			if (!voting(x.role)) { continue; }
			auto it = nodes_.find(x.incarnation);
			if (it == nodes_.end()) { continue; }
			auto held = it->second.durable.entry(index);
			holding += held && *held == entry;
		}
		if (holding < voter_count(m) / 2 + 1) {
			fail("leader " + std::to_string(leader.id) + " committed index " + std::to_string(index) + " held by " + std::to_string(holding) + " of " +
				std::to_string(voter_count(m)) + " voters");
		}
	}
	// Every committed membership is a single-voter step from the one before (D3).
	void record_membership(const Entry& e) {
		auto [it, fresh] = memberships_.emplace(e.index, decode_membership(e.payload));
		if (!fresh) { return; }
		if (it != memberships_.begin()) {
			const auto& before = std::prev(it)->second;
			try {
				validate_change(before, it->second, limits_);
			} catch (const std::exception& error) {
				fail(std::string("committed membership at ") + std::to_string(e.index) + " is not a single step: " + error.what());
			}
		}
		++memberships_committed;
	}
	std::size_t healed_quorum() const {
		const Membership* latest = &founding_;
		if (!memberships_.empty()) { latest = &memberships_.rbegin()->second; }
		return voter_count(*latest) / 2 + 1;
	}
	void check_send(const Node& n, const Send& s) {
		if (auto v = std::get_if<VoteResponse>(&s.message); v && v->granted) {
			auto [it, fresh] = ballots_.emplace(std::pair{n.id, v->term}, s.peer);
			if (!fresh && it->second != s.peer) { fail("node " + std::to_string(n.id) + " granted two candidates in term " + std::to_string(v->term)); }
			if (n.durable.hard.term != v->term || n.durable.hard.voted_for != s.peer) {
				fail("node " + std::to_string(n.id) + " granted a vote before it was durable");
			}
		}
		if (auto a = std::get_if<AppendResponse>(&s.message); a && a->success) {
			if (a->matched > n.durable.last()) { fail("node " + std::to_string(n.id) + " acknowledged entries before they were durable"); }
		}
	}
	void check_prefixes() {
		const std::vector<std::string>* longest = nullptr;
		for (const auto& [id, n] : nodes_) {
			if (!longest || n.applied.size() > longest->size()) { longest = &n.applied; }
		}
		for (const auto& [id, n] : nodes_) {
			if (!std::equal(n.applied.begin(), n.applied.end(), longest->begin())) { fail("applied sequences diverge"); }
		}
	}
	void check_logs() {
		for (const auto& [a, x] : nodes_) {
			for (const auto& [b, y] : nodes_) {
				if (a >= b) { continue; }
				const auto& l = x.durable;
				const auto& r = y.durable;
				for (Index i = std::min(l.last(), r.last()); i > 0; --i) {
					if (l.entry(i)->term == r.entry(i)->term) {
						for (Index j = 1; j <= i; ++j) {
							if (*l.entry(j) != *r.entry(j)) { fail("log matching violated at index " + std::to_string(j)); break; }
						}
						break;
					}
				}
			}
		}
	}

	std::mt19937_64 random_;
	std::size_t voters_;
	bool membership_changes_;
	Membership founding_ = fixed_membership(config(1, voters_));
	bool witness_ = false;
	std::map<Index, Membership> memberships_;
	Limits limits_;
	Timing timing_{60, 300, 600, 1200, 3000};  // a lost snapshot reply times out well inside the heal
	std::map<NodeId, Node> nodes_;
	std::deque<Envelope> network_;
	std::set<NodeId> isolated_;
	std::uint64_t now_ = 0;
	RequestId request_ = 0;
	std::map<Term, NodeId> leaders_;
	std::map<Index, Entry> committed_;
	std::map<Index, Term> commit_terms_;  // the term in which each index was first committed
	std::map<std::pair<NodeId, Term>, NodeId> ballots_;
	std::map<RequestId, Index> read_floors_;
	struct SnapshotRequest { NodeId from, peer; SnapshotKey key; };
	std::deque<SnapshotRequest> snapshot_requests_;
	Token prepared_ = 0;
	std::size_t forced_at_ = 2000 + random_() % 4000;
	std::set<std::pair<NodeId, NodeId>> cut_;  // (from, to) links down
	std::set<std::pair<NodeId, NodeId>> lost_;  // (at, of): connections the detector reported lost
	std::optional<NodeId> forced_leader_;
	bool hold_snapshots_ = false;
	std::optional<NodeId> removal_leader_;
	RequestId removal_read_ = 0;
	std::uint64_t trace_after_ = std::getenv("SIM_TRACE_AFTER") ? std::strtoull(std::getenv("SIM_TRACE_AFTER"), nullptr, 10) : 0;
};

// Quiescence's closed state (docs/mechanisms/quiescence.md): a group closed at rest reopens in
// its closed term with no election, and a reopened lone voter's first write costs the same
// barriers as a write to an open group. Storage completes at once; each batch is one barrier.
namespace closing {

void expect(bool value, const std::string& what) { if (!value) { fail("closing: " + what); } }

struct Replica {
	std::unique_ptr<Core> core;
	Durable durable;
	std::size_t barriers = 0;  // storage batches written
	std::vector<StorageBatch> batches;
	std::vector<RoleChanged> roles;
	std::size_t vote_traffic = 0;  // pre-vote and vote requests sent
	std::size_t sends = 0;  // every message sent
	std::size_t granted = 0;  // votes granted
	Index applied = 0;
};

class Group {
public:
	explicit Group(std::size_t voters, bool quiesce = false) : voters_(voters) {
		timing_.quiesce = quiesce;
		for (NodeId id = 1; id <= voters; ++id) { open(id, std::nullopt); }
	}
	std::map<NodeId, Replica> replicas;
	std::map<Term, NodeId> leaders;  // every leader seen, per term
	static ClosedIdentity identity(NodeId local) { auto c = config(local, 1); return {c.cluster, c.configuration, 7, 3, local}; }

	// Recovers `id` from its durable state; with a record, hands it to Core::reopen first.
	bool open(NodeId id, const std::optional<std::string>& record) {
		auto& r = replicas[id];
		RecoveredState state{config(id, voters_), r.durable.hard, r.durable.base, r.durable.base_term, r.durable.log, r.durable.base, r.durable.membership};
		if (!r.durable.base) { state.membership.reset(); }
		r.core = std::make_unique<Core>(config(id, voters_), state, Limits{}, timing_);
		r.applied = r.durable.base;
		bool resumed = record && r.core->reopen(decode_closed_state(*record, identity(id)));
		act(id, r.core->step(Tick{now_, delay(id)}));
		act(id, r.core->step(Start{}));
		return resumed;
	}
	// Checkpoints at applied and returns the record, closing the replica.
	std::string close(NodeId id) {
		auto& r = replicas.at(id);
		const auto c = config(id, voters_);
		if (r.applied > r.durable.base) {
			act(id, r.core->step(LocalCheckpoint{++request_, 1, r.applied, *r.core->term_at(r.applied), c.cluster, c.configuration, r.core->epoch()}));
		}
		auto closed = r.core->closed_state();
		expect(closed.has_value(), "replica " + std::to_string(id) + " can close at rest");
		r.core.reset();
		return closed ? encode_closed_state(*closed, identity(id)) : std::string{};
	}
	void tick(std::uint64_t dt) {
		now_ += dt;
		for (auto& [id, r] : replicas) { if (r.core) { act(id, r.core->step(Tick{now_, delay(id)})); } }
		settle();
	}
	void settle() {
		for (int n = 0; !network_.empty() && n < 100000; ++n) {
			auto e = std::move(network_.front()); network_.pop_front();
			auto& r = replicas.at(e.to);
			if (r.core && replicas.at(e.from).core) { act(e.to, r.core->step(Receive{e.from, std::move(e.message)})); }
		}
	}
	// Proposes on `id` and settles; true once `id` applied it.
	bool write(NodeId id, const std::string& command) {
		auto& r = replicas.at(id);
		act(id, r.core->step(Propose{++request_, command}));
		settle();
		return r.applied == r.core->last_index() && r.durable.entry(r.applied) && r.durable.entry(r.applied)->payload == command;
	}
	NodeId leader() const {
		for (const auto& [id, r] : replicas) { if (r.core && r.core->role() == Role::Leader) { return id; } }
		return 0;
	}
	std::uint64_t now() const { return now_; }
	std::uint64_t election_max() const { return timing_.election_max; }
	void act(NodeId id, Actions actions) {
		auto& r = replicas.at(id);
		for (auto& a : actions) {
			if (auto p = std::get_if<Persist>(&a)) {
				++r.barriers; r.batches.push_back(p->batch); r.durable.apply(p->batch);
				act(id, r.core->step(Persisted{p->token}));
			} else if (auto c = std::get_if<PersistCheckpoint>(&a)) {
				r.durable.install(c->state, {}); act(id, r.core->step(Persisted{c->token}));
			} else if (auto c = std::get_if<Committed>(&a)) {
				r.applied = c->entries.back().index; act(id, r.core->step(Applied{r.applied}));
			} else if (auto s = std::get_if<Send>(&a)) {
				r.vote_traffic += std::holds_alternative<VoteRequest>(s->message) || std::holds_alternative<PreVoteRequest>(s->message); ++r.sends;
				if (auto v = std::get_if<VoteResponse>(&s->message); v && v->granted) { ++r.granted; }
				network_.push_back({id, s->peer, s->message, 0});
			} else if (auto role = std::get_if<RoleChanged>(&a)) {
				r.roles.push_back(*role);
				if (role->role == Role::Leader) {
					auto [it, fresh] = leaders.emplace(role->term, id);
					expect(fresh || it->second == id, "two leaders in term " + std::to_string(role->term));
				}
			} else if (auto f = std::get_if<Fenced>(&a)) {
				expect(false, "replica " + std::to_string(id) + " fenced: " + f->reason);
			}
		}
	}
private:
	// Distinct election delays, so three voters never split their votes forever.
	std::uint64_t delay(NodeId id) const { return timing_.election_min + 150 * id; }
	std::size_t voters_;
	Timing timing_{60, 300, 600, 1200, 3000};
	std::deque<Envelope> network_;
	std::uint64_t now_ = 1;
	RequestId request_ = 0;
};

// A1, X5/F15: a closed lone voter reopens leading its closed term, and its first write costs
// two barriers (the entry with the term record, then the commit), as on an open group; without
// the record it elects again and the same write costs five.
void lone_voter() {
	Group g(1);
	g.tick(1);
	expect(g.leader() == 1 && g.write(1, "a"), "a lone voter leads and commits");
	auto& r = g.replicas.at(1);
	auto before = r.barriers;
	expect(g.write(1, "b") && r.barriers - before == 2, "a write to an open lone voter costs two barriers");
	const Term term = r.core->term();
	const auto record = g.close(1);
	expect(record.size() == 192, "a lone voter's record is 192 bytes, not " + std::to_string(record.size()));
	const auto durable = r.durable;

	// Reopened with its record: leading at Start, same term, no campaign.
	r.roles.clear(); before = r.barriers; const auto batch = r.batches.size();
	expect(g.open(1, record), "the record matches the recovered state");
	expect(r.core->role() == Role::Leader && r.core->term() == term, "the reopen leads its closed term");
	expect(r.roles.size() == 1 && r.roles[0].role == Role::Leader && r.roles[0].term == term, "the only role change is Leader in the closed term");
	expect(r.barriers == before, "the reopen itself writes nothing");
	expect(g.write(1, "c"), "the reopened lone voter commits");
	expect(r.barriers - before == 2, "the first write after a reopen costs two barriers, not " + std::to_string(r.barriers - before));
	expect(r.batches[batch].hard && r.batches[batch].hard->term == term && r.batches[batch].log && r.batches[batch].log->entries.size() == 1,
		"the term record shares the first write's batch");
	expect(!r.batches[batch + 1].log && r.batches[batch + 1].hard, "the second batch is the commit index");
	before = r.barriers;
	expect(g.write(1, "d") && r.barriers - before == 2 && !r.batches[batch + 2].hard, "the second write carries no term record");

	// The stale record: the replica wrote after it, so the reopen elects as after any restart.
	r.core.reset(); r.roles.clear(); before = r.barriers;
	expect(!g.open(1, record), "a record the log moved past is stale");
	g.tick(1);
	expect(r.core->term() == term + 1 && !r.roles.empty() && r.roles[0].role == Role::Candidate, "a stale record's reopen elects");

	// The control: the same closed state reopened without its record elects, and its first
	// write costs five barriers (ballot, no-op, commit, entry, commit).
	r.core.reset(); r.durable = durable; before = r.barriers;
	expect(!g.open(1, std::nullopt), "no record");
	g.tick(1);
	expect(g.write(1, "c") && r.barriers - before == 5, "without the record the first write costs five barriers, not " + std::to_string(r.barriers - before));
	expect(r.core->term() == term + 1, "without the record the reopen raises the term");
}

// A1: a three-voter group closed at rest reopens with no election and no term change, on a
// request to its leader and on a frame to its followers; a reopened follower keeps its ballot.
void three_voters() {
	Group g(3);
	for (int n = 0; n < 200 && !g.leader(); ++n) { g.tick(97); }
	const NodeId leader = g.leader();
	expect(leader != 0 && g.write(leader, "a"), "three voters elect and commit");
	g.tick(70);  // a heartbeat carries the commit index to the followers
	const Term term = g.replicas.at(leader).core->term();
	std::map<NodeId, std::string> records;
	for (NodeId id = 1; id <= 3; ++id) {
		records[id] = g.close(id);
		expect(records[id].size() == 240, "a three-voter member's record is 240 bytes");
		expect(decode_closed_state(records[id], Group::identity(id)).leader == leader, "replica " + std::to_string(id) + " closes knowing the leader");
	}
	// The leader reopens first, on a request; each follower on the frame it sends.
	for (auto& [id, r] : g.replicas) { r.roles.clear(); r.vote_traffic = 0; }
	expect(g.open(leader, records[leader]), "the leader's record matches");
	for (NodeId id = 1; id <= 3; ++id) { if (id != leader) { expect(g.open(id, records[id]), "a follower's record matches"); } }
	expect(g.write(leader, "b"), "the reopened group commits a request");
	for (auto& [id, r] : g.replicas) {
		expect(r.core->term() == term, "replica " + std::to_string(id) + " stays in the closed term");
		expect(r.vote_traffic == 0, "replica " + std::to_string(id) + " sends no vote request");
		for (const auto& role : r.roles) { expect(role.role != Role::Candidate && role.term == term, "no campaign"); }
		expect(r.core->leader() == leader, "every replica follows the closed leader");
	}
	// A reopened follower never votes twice in its term: its ballot is the recovered one.
	NodeId follower = 0;
	for (auto& [id, r] : g.replicas) { if (id != leader && r.durable.hard.voted_for == leader) { follower = id; } }
	expect(follower != 0, "a follower voted for the leader");
	if (!follower) { return; }
	const NodeId other = 6 - leader - follower;
	auto& f = g.replicas.at(follower);
	const auto granted = f.granted;
	g.act(follower, f.core->step(Receive{other, VoteRequest{term, 100, term, true}}));
	expect(f.granted == granted, "a reopened follower refuses a second candidate in its term");
}

// The record's bytes: a round trip, and every refusal decoding names.
void record_bytes() {
	Group g(3);
	for (int n = 0; n < 200 && !g.leader(); ++n) { g.tick(97); }
	const NodeId leader = g.leader();
	g.write(leader, "a");
	const auto id = Group::identity(leader);
	const auto bytes = g.close(leader);
	const auto closed = decode_closed_state(bytes, id);
	expect(encode_closed_state(closed, id) == bytes, "the record round-trips");
	auto refused = [&](std::string b, const ClosedIdentity& expected, const std::string& what) {
		try { decode_closed_state(b, expected); } catch (const kronuz::journal::Corruption&) { return; }
		expect(false, "decoding refuses " + what);
	};
	for (std::size_t i = 0; i < bytes.size(); ++i) { auto b = bytes; b[i] ^= 0x10; refused(b, id, "a flipped byte at " + std::to_string(i)); }
	refused(bytes.substr(0, bytes.size() - 1), id, "a truncated record");
	refused(bytes + "x", id, "a trailing byte");
	for (auto change : {&ClosedIdentity::group, &ClosedIdentity::incarnation, &ClosedIdentity::local}) {
		auto other = id; ++(other.*change); refused(bytes, other, "another replica's record");
	}
	auto other = id; other.cluster[1] = 'x'; refused(bytes, other, "another cluster's record");
	// Fields the rules refuse, under a valid checksum.
	auto patched = [&](std::size_t offset, std::uint64_t value, unsigned width = 8) {
		auto b = bytes.substr(0, bytes.size() - 4);
		for (unsigned i = 0; i < width; ++i) { b[offset + i] = static_cast<char>(value >> (8 * i)); }
		kronuz::journal::put32(b, kronuz::journal::crc32c(b));
		return b;
	};
	refused(patched(84, closed.last.index + 1), id, "a commit index past the last index");
	refused(patched(100, closed.hard.term + 1), id, "a last term past the term");
	refused(patched(76, 0), id, "a leader without its ballot");
	refused(patched(116, closed.last.index + 1), id, "a membership index past the last index");
	refused(patched(8, bytes.size() + 8, 4), id, "a length that is not the record's");
	expect(decode_closed_state(patched(108, closed.leader), id) == closed, "the patch itself keeps a valid record");
	bool thrown = false;
	try { auto bad = closed; bad.hard.commit_index = bad.last.index + 1; encode_closed_state(bad, id); } catch (const std::invalid_argument&) { thrown = true; }
	expect(thrown, "encoding refuses a state the rules refuse");
}

bool run() {
	lone_voter();
	three_voters();
	record_bytes();
	std::printf("closing: lone voter, three voters and record bytes: %s\n", failures ? "FAILED" : "ok");
	return !failures;
}

}  // namespace closing

// Quiescence (docs/mechanisms/quiescence.md): a group at rest sends nothing and arms no timer;
// a request or a frame wakes it in its term; LeaderGone makes its followers elect within their
// timeout; a restarted member neither deposes a leader at rest nor is locked out by one.
namespace quiescence {

using closing::Group;
void expect(bool value, const std::string& what) { if (!value) { fail("quiescence: " + what); } }
constexpr auto never = std::numeric_limits<std::uint64_t>::max();

// Elects three voters, commits, and lets the heartbeat carry the commit: all at rest.
NodeId rest(Group& g, const std::string& command) {
	for (int n = 0; n < 200 && !g.leader(); ++n) { g.tick(97); }
	const NodeId leader = g.leader();
	expect(leader != 0 && g.write(leader, command), "three voters elect and commit " + command);
	g.tick(70);
	for (auto& [id, r] : g.replicas) {
		expect(r.core->resting() && r.core->next_deadline() == never, "replica " + std::to_string(id) + " rests with no timer armed");
	}
	return leader;
}

// At rest nothing is sent for a long while, and no term moves; a request wakes the leader in its
// term, and the group rests again once the commit is heard.
void at_rest() {
	Group g(3, true);
	const NodeId leader = rest(g, "a");
	const Term term = g.replicas.at(leader).core->term();
	for (auto& [id, r] : g.replicas) { r.sends = 0; }
	for (int n = 0; n < 1000; ++n) { g.tick(97); }  // 97,000 units: 80 election timeouts
	for (auto& [id, r] : g.replicas) {
		expect(r.sends == 0, "replica " + std::to_string(id) + " sends nothing at rest, not " + std::to_string(r.sends));
		expect(r.core->term() == term, "no term moves at rest");
	}
	expect(g.write(leader, "b"), "a request wakes the leader and commits");
	expect(g.leader() == leader && g.replicas.at(leader).core->term() == term, "in the same term: CheckQuorum's window starts at the wake");
	g.tick(70);
	for (auto& [id, r] : g.replicas) { expect(r.core->resting(), "replica " + std::to_string(id) + " rests again"); }
	// Without quiescence the same group keeps its timers: the control.
	Group control(3);
	for (int n = 0; n < 200 && !control.leader(); ++n) { control.tick(97); }
	expect(control.write(control.leader(), "a"), "the control commits");
	control.tick(70);
	for (auto& [id, r] : control.replicas) { expect(!r.core->resting() && r.core->next_deadline() != never, "without quiesce a replica keeps a timer"); }
}

// LeaderGone at both followers of a crashed leader at rest: they elect within the timeout. When
// one hears it, its pre-vote wakes the other, and they elect within two.
void leader_gone() {
	for (bool both : {true, false}) {
		Group g(3, true);
		const NodeId leader = rest(g, "a");
		const Term term = g.replicas.at(leader).core->term();
		g.replicas.at(leader).core.reset();  // the leader's node is gone
		std::vector<NodeId> followers;
		for (NodeId id = 1; id <= 3; ++id) { if (id != leader) { followers.push_back(id); } }
		for (auto id : followers) {
			if (both || id == followers[0]) { g.act(id, g.replicas.at(id).core->step(LeaderGone{leader})); }
		}
		const auto from = g.now();
		NodeId elected = 0;
		for (int n = 0; n < 100 && !elected; ++n) { g.tick(20); elected = g.leader(); }
		if (both) {
			expect(elected != 0 && elected != leader && g.replicas.at(elected).core->term() > term, "LeaderGone elects a new leader in a later term");
			expect(g.now() - from <= 2 * g.election_max(), "within the election timeout and a round: " + std::to_string(g.now() - from));
		} else {
			expect(elected != 0 && elected != leader, "one follower's pre-vote wakes the other, and they elect");
			expect(g.now() - from <= 3 * g.election_max(), "within two election timeouts and a round: " + std::to_string(g.now() - from));
		}
	}
}

// A restarted follower's campaign reaches the leader at rest, which wakes and heartbeats it; the
// other follower at rest refuses its pre-vote. No term moves. A restarted leader's own campaign
// is not refused by followers at rest under it: the group elects.
void restarts() {
	Group g(3, true);
	NodeId leader = rest(g, "a");
	Term term = g.replicas.at(leader).core->term();
	const NodeId follower = leader % 3 + 1;
	g.replicas.at(follower).core.reset();
	g.open(follower, std::nullopt);
	for (int n = 0; n < 100 && g.replicas.at(follower).core->leader() != leader; ++n) { g.tick(97); }
	expect(g.replicas.at(follower).core->leader() == leader && g.leader() == leader, "a restarted follower finds the leader at rest");
	for (auto& [id, r] : g.replicas) { expect(r.core->term() == term, "a restarted follower deposes no one"); }
	g.tick(70);
	g.replicas.at(leader).core.reset();
	g.open(leader, std::nullopt);
	NodeId elected = 0;
	for (int n = 0; n < 200 && !elected; ++n) { g.tick(97); elected = g.leader(); }
	expect(elected != 0 && g.replicas.at(elected).core->term() > term, "a restarted leader is not locked out: the group elects");
}

// A transfer to a follower at rest: the target's campaign ends its rest, so it leads with its
// timers and its term entry is on the old leader within the settle, the third voter down too.
void transfer() {
	for (bool down : {false, true}) {
		Group g(3, true);
		const NodeId leader = rest(g, "a");
		const Term term = g.replicas.at(leader).core->term();
		const NodeId target = leader % 3 + 1, third = 6 - leader - target;
		if (down) { g.replicas.at(third).core.reset(); }
		g.act(leader, g.replicas.at(leader).core->step(TransferLeadership{1000001, target}));
		g.settle();
		const auto& t = *g.replicas.at(target).core;
		const std::string at = down ? " (the third voter down)" : "";
		expect(t.role() == Role::Leader && t.term() == term + 1, "the target leads the next term" + at);
		expect(g.replicas.at(leader).core->last_index() == t.last_index(), "its term entry is on the old leader within the settle" + at);
	}
}

// A leader's replica fenced on a live node raises no LeaderGone; its Driver sends Gone, and the
// followers at rest elect within the timeout. Gone from a follower changes nothing.
void gone() {
	Group g(3, true);
	const NodeId leader = rest(g, "a");
	const Term term = g.replicas.at(leader).core->term();
	const NodeId a = leader % 3 + 1, b = 6 - leader - a;
	g.act(a, g.replicas.at(a).core->step(Receive{b, Gone{term}}));
	expect(g.replicas.at(a).core->leader() == leader && g.replicas.at(a).core->resting(), "Gone from a follower changes nothing");
	(void)g.replicas.at(leader).core->step(StorageFault{"quiescence: the leader's storage fails"});
	expect(g.replicas.at(leader).core->role() == Role::Fenced, "the leader fences, its node up");
	for (NodeId id : {a, b}) { g.act(id, g.replicas.at(id).core->step(Receive{leader, Gone{term}})); }
	const auto from = g.now();
	NodeId elected = 0;
	for (int n = 0; n < 100 && !elected; ++n) {
		g.tick(20);
		for (NodeId id : {a, b}) { if (g.replicas.at(id).core->role() == Role::Leader) { elected = id; } }
	}
	expect(elected && g.replicas.at(elected).core->term() > term, "Gone from the leader elects a new leader in a later term");
	expect(g.now() - from <= 2 * g.election_max(), "within the election timeout and a round: " + std::to_string(g.now() - from));
}

bool run() {
	at_rest();
	leader_gone();
	restarts();
	transfer();
	gone();
	std::printf("quiescence: at rest, LeaderGone, restarts, transfer and Gone: %s\n", failures ? "FAILED" : "ok");
	return !failures;
}

}  // namespace quiescence

}  // namespace

namespace kronuz::consensus::driver_simulation { bool run(std::uint64_t seeds, std::uint64_t steps, std::uint64_t first); }

int main(int argc, char** argv) {
	const std::uint64_t seeds = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 200;
	const std::uint64_t steps = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 10000;
	const std::uint64_t first = argc > 3 ? std::strtoull(argv[3], nullptr, 10) : 1;
	std::uint64_t restarts = 0, kept = 0, lost = 0, elections = 0, repairs = 0, transfers = 0, changes = 0, committed_changes = 0;
	std::uint64_t reads = 0, checkpoints = 0, installs = 0, limit_changes = 0, replaced = 0, removals = 0, replacements = 0, with_snapshot = 0, with_read = 0, duplicated = 0, held = 0;
	std::uint64_t closes = 0, resumed = 0, stale = 0, quiescent = 0, leader_gones = 0, rested = 0;
	if (!closing::run() || !quiescence::run()) { return EXIT_FAILURE; }
	for (std::uint64_t seed = first; seed < first + seeds && !failures; ++seed) {
		current_seed = seed;
		Simulation sim(seed, 3 + 2 * (seed % 3 == 0), seed % 2 == 0, seed % 5 == 1, seed % 4 >= 2);  // even seeds change membership; half quiesce
		quiescent += seed % 4 >= 2;
		sim.run(steps);
		if (std::getenv("SIM_DEBUG")) {
			for (auto [r, n] : sim.reject_reasons) { std::fprintf(stderr, "  reject %d: %llu\n", r, (unsigned long long)n); } std::fprintf(stderr, "seed %llu: elections %llu led %llu of %llu steps, max term %llu\n", (unsigned long long)seed, (unsigned long long)sim.elections, (unsigned long long)sim.led_steps, (unsigned long long)steps, (unsigned long long)sim.max_term()); }
		if (!failures) { sim.heal(); }
		restarts += sim.restarts;
		kept += sim.kept;
		lost += sim.lost;
		elections += sim.elections;
		repairs += sim.repairs;
		transfers += sim.transfers;
		changes += sim.changes_proposed;
		committed_changes += sim.memberships_committed;
		reads += sim.reads;
		checkpoints += sim.checkpoints;
		installs += sim.installs;
		limit_changes += sim.limit_changes;
		replaced += sim.replaced_configurations;
		removals += sim.forced_removals;
		replacements += sim.forced_replacements;
		with_snapshot += sim.removals_with_snapshot;
		with_read += sim.removals_with_read;
		duplicated += sim.delayed_timeout_nows;
		held += sim.held_back_total();
		closes += sim.closes; resumed += sim.resumed; stale += sim.stale; leader_gones += sim.leader_gones; rested += sim.rested_steps;
	}
	std::printf("simulation: %llu seeds x %llu steps, %llu elections, %llu restarts (%llu kept a pending batch, %llu lost one), %llu repairs, %llu transfers, %llu membership changes proposed, %llu committed memberships applied: %s\n",
		(unsigned long long)seeds, (unsigned long long)steps, (unsigned long long)elections, (unsigned long long)restarts, (unsigned long long)kept,
		(unsigned long long)lost, (unsigned long long)repairs, (unsigned long long)transfers, (unsigned long long)changes, (unsigned long long)committed_changes, failures ? "FAILED" : "ok");
	std::printf("  %llu linearizable reads, %llu checkpoints, %llu installs, %llu limit changes, %llu replaced configuration entries, %llu forced self-removals\n", (unsigned long long)reads,
		(unsigned long long)checkpoints, (unsigned long long)installs, (unsigned long long)limit_changes, (unsigned long long)replaced, (unsigned long long)removals);
	std::printf("  forced: %llu crashes between promotion and removal; removals committed with a snapshot in flight %llu, with a read in flight %llu; %llu delayed duplicate TimeoutNows; %llu appends held back from peers below their next entry\n",
		(unsigned long long)replacements, (unsigned long long)with_snapshot, (unsigned long long)with_read, (unsigned long long)duplicated, (unsigned long long)held);
	std::printf("  %llu closes at rest; %llu reopens resumed their closed term, %llu records refused as stale\n", (unsigned long long)closes, (unsigned long long)resumed, (unsigned long long)stale);
	std::printf("  %llu quiescent seeds: %llu LeaderGone raised, %llu node-steps at rest\n", (unsigned long long)quiescent, (unsigned long long)leader_gones, (unsigned long long)rested);
	if (failures) { return EXIT_FAILURE; }
	const auto* driver_seeds = std::getenv("SIM_DRIVER_SEEDS");  // the Driver's seeds alone, when set
	const bool driver = driver_simulation::run(driver_seeds ? std::strtoull(driver_seeds, nullptr, 10) : (seeds + 9) / 10, std::min<std::uint64_t>(steps, 3000), first);
	return driver ? EXIT_SUCCESS : EXIT_FAILURE;
}
