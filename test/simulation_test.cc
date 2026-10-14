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
// own rate (0.5x to 2x). After every step the oracles check:
//   - election safety: at most one leader per term;
//   - leader completeness: a new leader's durable log holds every entry any node applied;
//   - log matching: two durable logs that agree on an index's term agree before it;
//   - state machine safety: every node's applied sequence is a prefix of one sequence,
//     also after a repair (Track C, D10) rewinds a node's application and replays it;
//   - one grant per voter per term, across restarts;
//   - durability when sent: a vote grant, or an append acknowledgement, is backed by the
//     sender's durable state at the moment it is sent.
// After the run the group heals, elects a leader, and commits a new entry.
//   simulation_test [seeds] [steps] [first seed]   (defaults 200, 10000, 1)
#include "consensus/core.h"
#include "consensus/membership.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
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
	std::vector<Entry> log;  // log[i] has index i + 1
	void apply(const StorageBatch& batch) {
		if (batch.hard) { hard = *batch.hard; }
		if (batch.log) {
			log.resize(std::size_t(batch.log->replace_from - 1));
			for (const auto& e : batch.log->entries) { log.push_back(e); }
		}
	}
};

struct Node {
	NodeId id;
	bool joiner = false;  // started from a join record (Track C, D5), no membership
	double rate;  // local clock rate
	double local = 0;  // local clock, advanced at `rate`
	std::unique_ptr<Core> core;
	Durable durable;
	std::optional<Persist> pending;
	std::optional<Committed> applying;
	std::vector<std::string> applied;  // commands applied since the last restart
	std::vector<Entry> applied_entries;  // every entry applied since the last restart
	bool up = true;
};

struct Envelope {
	NodeId from, to;
	Message message;
	std::uint64_t at;  // deliverable from this global time
};

class Simulation {
public:
	Simulation(std::uint64_t seed, std::size_t voters, bool membership) : random_(seed), voters_(voters), membership_changes_(membership) {
		// Small RPC batches leave replication partial across leader changes.
		limits_.rpc_entries = 1 + random_() % 4;
		for (NodeId id = 1; id <= voters; ++id) {
			auto& n = nodes_[id];
			n.id = id;
			n.rate = 0.5 + double(random_() % 1501) / 1000.0;
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
			if (pick < 80) {
				tick(n);
			} else if (pick < 180) {
				for (auto k = 1 + random_() % 3; k-- > 0;) { propose(leader_or(n)); }
			} else if (pick < 450) {
				deliver_one();
			} else if (pick < 600) {
				if (n.up && n.pending) { complete_persist(n); }
			} else if (pick < 700) {
				if (n.up && n.applying) { complete_apply(n); }
			} else if (pick < 701) {
				repair(n);
			} else if (pick < 712 && membership_changes_) {
				change_membership();
			} else if (pick < 702) {
				crash(random_() % 2 ? leader_or(n) : n);
			} else if (pick < 706) {
				toggle_partition(random_() % 2 ? leader_or(n).id : id);
			} else if (pick >= 712 && pick < 716) {
				transfer();
			} else {
				for (auto& [i, m] : nodes_) { tick(m); }
			}
			if (step % 16 == 0) { check_logs(); }
			bool any = false;
			for (auto& [i, m] : nodes_) { any |= m.up && m.core->role() == Role::Leader; }
			led_steps += any;
		}
		check_logs();
	}

	// Heals partitions and crashes; clocks keep their skew. Pre-vote (Track C, D7) keeps a
	// node with a stale log and a fast clock from disrupting elections: before it, seed 217
	// at 2,000 steps stayed leaderless while its vote requests reset slower voters' timers.
	void heal() {
		isolated_.clear();
		for (auto& [id, n] : nodes_) {
			if (!n.up) { restart(n); }
		}
		for (int round = 0; round < 4000 && !failures; ++round) {
			advance(5);
			for (auto& [id, n] : nodes_) {
				tick(n);
				if (n.pending) { complete_persist(n); }
				if (n.applying) { complete_apply(n); }
			}
			while (!network_.empty()) { deliver_front(); }
			if (round >= 2000 && round % 50 == 0 && applied_healed() < healed_quorum()) {  // until a majority applies it
				for (auto& [id, n] : nodes_) {
					if (n.core->role() == Role::Leader) { act(n, n.core->step(Propose{++request_, "healed"})); }
				}
			}
		}
		if (applied_healed() < healed_quorum()) {
			fail("healed group did not commit a new entry on a majority");
			for (auto& [id, n] : nodes_) {
				std::fprintf(stderr, "  node %llu: up %d role %d term %llu committed %llu applied %llu last %llu durable log %zu pending %d applying %d\n", (unsigned long long)id, n.up,
					int(n.core->role()), (unsigned long long)n.core->term(), (unsigned long long)n.core->committed(), (unsigned long long)n.core->applied(),
					(unsigned long long)n.core->last_index(), n.durable.log.size(), n.pending.has_value(), n.applying.has_value());
			}
		}
		check_logs();
	}

	std::uint64_t restarts = 0, kept = 0, lost = 0, elections = 0, led_steps = 0, repairs = 0, transfers = 0, changes_proposed = 0, memberships_committed = 0;
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
		state.entries = n.durable.log;
		if (n.joiner) { state.membership = Membership{}; }  // the join record: no membership yet
		n.core = std::make_unique<Core>(node_config(n), std::move(state), limits_, timing_);
		n.pending.reset();
		n.applying.reset();
		n.applied.clear();
		n.applied_entries.clear();
		n.up = true;
		act(n, n.core->step(Tick{local_time(n), election_delay()}));
		act(n, n.core->step(Start{}));
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
		if (n.pending) {
			if (random_() % 2) {
				n.durable.apply(n.pending->batch);  // the batch reached the disk; its completion did not
				++kept;
			} else {
				++lost;
			}
		}
		n.up = false;
		n.core.reset();
		if (random_() % 2) { restart(n); }
	}

	void tick(Node& n) {
		if (!n.up) { return; }
		act(n, n.core->step(Tick{local_time(n), election_delay()}));
	}
	void propose(Node& n) {
		if (!n.up) { return; }
		act(n, n.core->step(Propose{++request_, "c" + std::to_string(request_)}));
	}
	void complete_persist(Node& n) {
		auto p = std::move(*n.pending);
		n.pending.reset();
		const Index before = n.durable.hard.commit_index;
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
		const auto choice = random_() % 3;
		if (choice == 0 && next.members.size() < 7) {
			const NodeId incarnation = next.next_incarnation++;
			next.members.push_back({incarnation, incarnation, MemberRole::Learner});
			auto& joiner = nodes_[incarnation];
			if (!joiner.core) {
				joiner.id = incarnation;
				joiner.joiner = true;
				joiner.rate = 0.5 + double(random_() % 1501) / 1000.0;
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
		if (!n.up || isolated_.count(e.from) || isolated_.count(e.to)) { return; }
		act(n, n.core->step(Receive{e.from, e.message}));
	}

	void act(Node& n, Actions actions) {
		for (auto& a : actions) {
			if (auto p = std::get_if<Persist>(&a)) {
				if (n.pending) { fail("two storage batches outstanding"); }
				n.pending = std::move(*p);
			} else if (auto s = std::get_if<Send>(&a)) {
				check_send(n, *s);
				if (random_() % 20 == 0) { continue; }  // lost
				network_.push_back({n.id, s->peer, s->message, now_ + random_() % 60});
				while (network_.size() > 4096) { network_.pop_front(); }
			} else if (auto c = std::get_if<Committed>(&a)) {
				if (n.applying) { fail("two application batches outstanding"); }
				n.applying = std::move(*c);
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
		for (const auto& [index, entry] : committed_) {
			if (index > n.durable.log.size() || n.durable.log[index - 1] != entry) {
				fail("leader " + std::to_string(n.id) + " of term " + std::to_string(term) + " lacks applied entry " + std::to_string(index));
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
		// The membership its durable log puts in force, not the core's own view of it.
		Membership m = founding_;  // every log here starts at index 1, over the founding membership
		for (const auto& e : leader.durable.log) {
			if (e.kind == EntryKind::Configuration) { m = decode_membership(e.payload, limits_); }
		}
		if (m.members.empty()) { return; }
		const Entry& entry = leader.durable.log.at(index - 1);
		std::size_t holding = 0;
		for (const auto& x : m.members) {
			if (!voting(x.role)) { continue; }
			auto it = nodes_.find(x.incarnation);
			if (it == nodes_.end()) { continue; }
			const auto& log = it->second.durable.log;
			holding += index <= log.size() && log[index - 1] == entry;
		}
		if (holding < voter_count(m) / 2 + 1) {
			fail("leader " + std::to_string(leader.id) + " committed index " + std::to_string(index) + " held by " + std::to_string(holding) + " of " +
				std::to_string(voter_count(m)) + " voters");
		}
	}
	// Every committed membership is a single-voter step from the one before (D3).
	void record_membership(const Entry& e) {
		auto [it, fresh] = memberships_.emplace(e.index, decode_membership(e.payload, limits_));
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
			if (a->matched > n.durable.log.size()) { fail("node " + std::to_string(n.id) + " acknowledged entries before they were durable"); }
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
				const auto& l = x.durable.log;
				const auto& r = y.durable.log;
				for (std::size_t i = std::min(l.size(), r.size()); i-- > 0;) {
					if (l[i].term == r[i].term) {
						if (!std::equal(l.begin(), l.begin() + std::ptrdiff_t(i + 1), r.begin())) { fail("log matching violated at index " + std::to_string(i + 1)); }
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
	std::map<Index, Membership> memberships_;
	Limits limits_;
	Timing timing_{60, 300, 600, 1200, 60000};
	std::map<NodeId, Node> nodes_;
	std::deque<Envelope> network_;
	std::set<NodeId> isolated_;
	std::uint64_t now_ = 0;
	RequestId request_ = 0;
	std::map<Term, NodeId> leaders_;
	std::map<Index, Entry> committed_;
	std::map<std::pair<NodeId, Term>, NodeId> ballots_;
};

}  // namespace

int main(int argc, char** argv) {
	const std::uint64_t seeds = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 200;
	const std::uint64_t steps = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 10000;
	const std::uint64_t first = argc > 3 ? std::strtoull(argv[3], nullptr, 10) : 1;
	std::uint64_t restarts = 0, kept = 0, lost = 0, elections = 0, repairs = 0, transfers = 0, changes = 0, committed_changes = 0;
	for (std::uint64_t seed = first; seed < first + seeds && !failures; ++seed) {
		current_seed = seed;
		Simulation sim(seed, 3 + 2 * (seed % 3 == 0), seed % 2 == 0);  // even seeds change membership
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
	}
	std::printf("simulation: %llu seeds x %llu steps, %llu elections, %llu restarts (%llu kept a pending batch, %llu lost one), %llu repairs, %llu transfers, %llu membership changes proposed, %llu committed memberships applied: %s\n",
		(unsigned long long)seeds, (unsigned long long)steps, (unsigned long long)elections, (unsigned long long)restarts, (unsigned long long)kept,
		(unsigned long long)lost, (unsigned long long)repairs, (unsigned long long)transfers, (unsigned long long)changes, (unsigned long long)committed_changes, failures ? "FAILED" : "ok");
	return failures ? EXIT_FAILURE : EXIT_SUCCESS;
}
