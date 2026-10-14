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

// Track C, C1 regressions from the architect's review (M1-M6, condition 5): a small
// deterministic cluster of Cores in which disk batches complete at once, applications apply
// at once, messages travel through one FIFO, and links can be cut in one direction.
#include "consensus/checkpoint.h"
#include "consensus/core.h"
#include "consensus/membership.h"
#include "consensus/snapshot.h"
#include "consensus/storage.h"

#include <algorithm>
#include <cstdio>
#include <deque>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

using namespace kronuz::consensus;

namespace {

int checks = 0, failures = 0;
void check(bool value, const char* what) {
	++checks;
	if (!value) { ++failures; std::fprintf(stderr, "FAIL: %s\n", what); }
}
constexpr Timing compact{20, 40, 100, 200, 60000};

template <class T> const T* first(const Actions& actions) {
	for (const auto& a : actions) { if (auto v = std::get_if<T>(&a)) { return v; } }
	return nullptr;
}
inline FixedConfiguration founding_config(NodeId local, std::size_t voters = 3) {
	FixedConfiguration c; c.cluster[0] = 'P'; c.configuration[0] = 'R'; c.local = local;
	for (NodeId v = 1; v <= voters; ++v) { c.voters.push_back(v); }
	return c;
}
inline FixedConfiguration joiner_config(NodeId local) { auto c = founding_config(local); c.voters.clear(); return c; }

struct ProbeNode {
	NodeId id;
	FixedConfiguration config;
	Limits limits;
	std::unique_ptr<Core> core;
	std::vector<std::string> applied;
	std::vector<Actions> log;  // every action batch, for inspection
	bool down = false;
	std::optional<PersistCheckpoint> last_checkpoint;
	std::optional<PersistInstall> last_install;
	std::vector<SnapshotNeeded> snapshots;
	std::uint64_t appends_received = 0;
};
struct ProbeEnvelope { NodeId from, to; Message message; };

class ProbeCluster {
public:
	std::map<NodeId, ProbeNode> nodes;
	std::deque<ProbeEnvelope> wire;
	std::set<std::pair<NodeId, NodeId>> cut;  // (from, to) dropped
	std::uint64_t now = 0;
	RequestId request = 1000;
	std::map<std::pair<NodeId, NodeId>, std::uint64_t> sent_appends;  // (from, to) count

	ProbeNode& add(NodeId id, FixedConfiguration config, RecoveredState state, Limits limits = {}) {
		auto& n = nodes[id];
		n.id = id; n.config = config; n.limits = limits;
		n.core = std::make_unique<Core>(config, std::move(state), limits, compact);
		consume(id, n.core->step(Tick{now, 100 + 20 * id}));
		consume(id, n.core->step(Start{}));
		return n;
	}
	ProbeNode& add_founding(NodeId id, std::size_t voters = 3, Limits limits = {}) {
		RecoveredState s; s.configuration = founding_config(id, voters);
		return add(id, founding_config(id, voters), std::move(s), limits);
	}
	ProbeNode& add_joiner(NodeId id, Limits limits = {}) {
		RecoveredState s; s.configuration = joiner_config(id); s.membership = Membership{};
		return add(id, joiner_config(id), std::move(s), limits);
	}
	Core& core(NodeId id) { return *nodes.at(id).core; }
	void consume(NodeId id, Actions actions) {
		auto& n = nodes.at(id);
		n.log.push_back(actions);
		for (auto& a : actions) {
			if (auto p = std::get_if<Persist>(&a)) {
				consume(id, n.core->step(Persisted{p->token}));
			} else if (auto s = std::get_if<Send>(&a)) {
				if (std::holds_alternative<AppendRequest>(s->message)) { ++sent_appends[{id, s->peer}]; }
				wire.push_back({id, s->peer, s->message});
			} else if (auto c = std::get_if<Committed>(&a)) {
				for (const auto& e : c->entries) { if (e.kind == EntryKind::Command) { n.applied.push_back(e.payload); } }
				consume(id, n.core->step(Applied{c->entries.back().index}));
			} else if (auto pc = std::get_if<PersistCheckpoint>(&a)) {
				n.last_checkpoint = *pc;
				consume(id, n.core->step(Persisted{pc->token}));
			} else if (auto pi = std::get_if<PersistInstall>(&a)) {
				n.last_install = *pi;
			} else if (auto sn = std::get_if<SnapshotNeeded>(&a)) {
				n.snapshots.push_back(*sn);
			}
		}
	}
	void pump(std::size_t bound = 100000) {
		while (!wire.empty() && bound--) {
			auto e = std::move(wire.front()); wire.pop_front();
			if (!nodes.contains(e.to)) { continue; }
			auto& n = nodes.at(e.to);
			if (n.down || nodes.at(e.from).down || cut.contains({e.from, e.to})) { continue; }
			if (std::holds_alternative<AppendRequest>(e.message)) { ++n.appends_received; }
			consume(e.to, n.core->step(Tick{now, 100 + 20 * e.to}));
			consume(e.to, n.core->step(Receive{e.from, std::move(e.message)}));
		}
	}
	void tick(std::uint64_t dt = 10) {
		now += dt;
		for (auto& [id, n] : nodes) { if (!n.down) { consume(id, n.core->step(Tick{now, 100 + 20 * id})); } }
		pump();
	}
	void run(std::uint64_t ms) { for (std::uint64_t t = 0; t < ms; t += 10) { tick(10); } }
	std::optional<NodeId> leader() {
		std::optional<NodeId> best;
		for (auto& [id, n] : nodes) {
			if (!n.down && n.core->role() == Role::Leader && (!best || n.core->term() > core(*best).term())) { best = id; }
		}
		return best;
	}
	// Proposes on the current leader and runs until it applies; true once applied.
	bool commit_one(const std::string& command, std::uint64_t ms = 5000) {
		for (std::uint64_t t = 0; t < ms; t += 10) {
			if (auto l = leader(); l && !core(*l).busy()) {
				auto& n = nodes.at(*l);
				if (std::find(n.applied.begin(), n.applied.end(), command) != n.applied.end()) { return true; }
				auto actions = n.core->step(Propose{++request, command});
				consume(*l, std::move(actions));
			}
			tick(10);
		}
		for (auto& [id, n] : nodes) { if (std::find(n.applied.begin(), n.applied.end(), command) != n.applied.end()) { return true; } }
		return false;
	}
	bool change(NodeId leader_id, Membership next, RejectReason* reason = nullptr) {
		auto actions = core(leader_id).step(ChangeMembership{++request, std::move(next)});
		auto r = first<Reject>(actions);
		if (r && reason) { *reason = r->reason; }
		consume(leader_id, std::move(actions));
		pump();
		return !r;
	}
};

// Leaders and transfers converge within this many simulated milliseconds.
ProbeCluster founded(std::size_t voters = 3, Limits limits = {}) {
	ProbeCluster c;
	for (NodeId id = 1; id <= voters; ++id) { c.add_founding(id, voters, limits); }
	c.now = 100; c.consume(1, c.core(1).step(Tick{c.now, 100}));  // node 1's timer fires first
	c.run(400);
	return c;
}

// M1: a promoted learner that does not yet hold its promotion still votes (Raft §4.1), so
// the survivors of its old leader elect one.
void promotion_window(bool only_two) {
	auto c = founded();
	c.add_joiner(4);
	check(c.leader() == 1u && c.commit_one("before"), "node 1 leads and commits");
	Membership with4 = c.core(1).membership();
	with4.members.push_back({4, 4, MemberRole::Learner}); with4.next_incarnation = 5;
	check(c.change(1, with4), "the leader adds learner 4");
	c.run(500);
	c.cut.insert({1, 4});
	if (only_two) { c.cut.insert({1, 3}); }
	Membership promoted = c.core(1).membership();
	for (auto& x : promoted.members) { if (x.incarnation == 4) { x.role = MemberRole::Voter; } }
	check(c.change(1, promoted), "the leader promotes 4");
	c.run(300);
	check(!votes_in(c.core(4).membership(), 4), "4 does not yet hold its promotion");
	c.nodes.at(1).down = true;
	c.cut.clear();
	c.run(5000);
	check(c.leader().has_value() && c.commit_one("after"), only_two ? "with the promotion uncommitted on 2, the survivors elect and commit" : "with the promotion committed, the survivors elect and commit");
}

// M2: a joiner installs only a snapshot that names its base membership, and reopens from it;
// it cannot checkpoint below the entry that names its membership.
bool reopens(const FixedConfiguration& config, const RecoveredState& state) {
	try {
		kronuz::journal::ArtifactDescriptor application{{'A'}, 0, kronuz::journal::crc32c("")};
		auto bundle = decode_checkpoint(encode_checkpoint(state, 1, application), config, 1, application);
		Core reopened(config, bundle.state, {}, compact);
		return true;
	} catch (const std::exception&) {
		return false;
	}
}
void joiner_install_and_checkpoint() {
	auto c = founded();
	for (int i = 0; i < 5; ++i) { c.commit_one("c" + std::to_string(i)); }
	auto& leader = c.core(1);
	c.consume(1, leader.step(LocalCheckpoint{1, 9, leader.applied(), leader.term(), founding_config(1).cluster, founding_config(1).configuration}));
	check(!leader.base_membership().has_value() && leader.snapshot_membership(4).has_value(), "a founding base is implicit for founding voters, explicit for anyone else");
	c.add_joiner(4);
	Membership with4 = leader.membership(); with4.members.push_back({4, 4, MemberRole::Learner}); with4.next_incarnation = 5;
	check(c.change(1, with4), "the leader adds learner 4");
	c.run(200);
	auto& needed = c.nodes.at(1).snapshots;
	auto request = std::find_if(needed.begin(), needed.end(), [](const SnapshotNeeded& n) { return n.peer == 4; });
	check(request != needed.end(), "the leader asks for a snapshot for learner 4");
	if (request == needed.end()) { return; }
	auto& joiner = c.core(4);
	auto unnamed = joiner.step(InstallPrepared{50, 76, 1, request->key.leader_term, founding_config(1).cluster, founding_config(1).configuration, request->key.boundary, std::nullopt});
	auto refused = first<InstallRejected>(unnamed);
	check(refused && refused->reason == InstallRejectReason::Invalid, "a joiner refuses an install that does not name its base membership");
	auto prepared = joiner.step(InstallPrepared{51, 77, 1, request->key.leader_term, founding_config(1).cluster, founding_config(1).configuration, request->key.boundary, leader.snapshot_membership(4)});
	auto install = first<PersistInstall>(prepared);
	check(install && install->state.membership && install->state.membership->members.size() == 3, "the install persists the named base membership");
	if (install) { check(reopens(joiner_config(4), install->state), "the joiner reopens from its install"); }

	auto d = founded();
	for (int i = 0; i < 3; ++i) { d.commit_one("c" + std::to_string(i)); }
	const Index before_add = d.core(1).last_index();
	d.add_joiner(4);
	Membership add4 = d.core(1).membership(); add4.members.push_back({4, 4, MemberRole::Learner}); add4.next_incarnation = 5;
	check(d.change(1, add4), "the leader adds learner 4 by append");
	d.run(500);
	auto& j = d.core(4);
	bool below = false;
	for (Term t = 1; t <= j.term(); ++t) {
		below |= first<PersistCheckpoint>(j.step(LocalCheckpoint{2, 9, before_add, t, founding_config(1).cluster, founding_config(1).configuration})) != nullptr;
	}
	check(!below, "a joiner cannot checkpoint below the entry that names its membership");
	auto above = j.step(LocalCheckpoint{3, 9, j.applied(), j.term(), founding_config(1).cluster, founding_config(1).configuration});
	auto persisted = first<PersistCheckpoint>(above);
	check(persisted && reopens(joiner_config(4), persisted->state), "a joiner checkpoints above it and reopens");
}

// M4: a membership is validated against its own limits; a group's limits stay within the
// node's; a command limit below a Configuration entry's size does not wedge the group.
void limits_and_reopen() {
	Limits node; node.members = 3;
	auto c = founded(3, node);
	c.commit_one("a");
	Membership raise = c.core(1).membership(); raise.limits = node; raise.limits->members = 5;
	check(!c.change(1, raise), "a group cannot raise members above its node's limit");
	Membership lower = c.core(1).membership(); lower.limits = node; lower.limits->command_bytes = 64;
	check(c.change(1, lower), "the group lowers command_bytes to 64");
	c.run(300);
	const Index lowered_at = c.core(1).last_index();
	Membership back = c.core(1).membership(); back.limits->command_bytes = 1 << 20;
	check(c.change(1, back), "the leader admits the next Configuration entry");
	c.run(1000);
	check(c.core(1).committed_membership().limits && c.core(1).committed_membership().limits->command_bytes == (1u << 20), "the raise commits");
	auto cp = c.core(1).step(LocalCheckpoint{2, 9, lowered_at, c.core(1).term(), founding_config(1).cluster, founding_config(1).configuration});
	auto persisted = first<PersistCheckpoint>(cp);
	check(persisted && reopens(founding_config(1), persisted->state), "the leader reopens its own log after the lowering");
}

// M5: a removed member is no longer replicated to and its snapshot is released; a leader
// that removes itself hands off by transfer.
void removal() {
	auto c = founded(4);
	c.commit_one("a");
	c.nodes.at(4).down = true;
	for (int i = 0; i < 4; ++i) { c.commit_one("b" + std::to_string(i)); }
	auto& leader = c.core(1);
	c.consume(1, leader.step(LocalCheckpoint{1, 9, leader.applied(), leader.term(), founding_config(1, 4).cluster, founding_config(1, 4).configuration}));
	c.nodes.at(4).down = false;
	c.run(300);
	auto& needed = c.nodes.at(1).snapshots;
	auto request = std::find_if(needed.begin(), needed.end(), [](const SnapshotNeeded& n) { return n.peer == 4; });
	check(request != needed.end(), "the leader asks for a snapshot for 4");
	if (request == needed.end()) { return; }
	auto key = request->key;
	c.consume(1, leader.step(SnapshotSourceReady{4, key}));
	c.nodes.at(4).down = true;
	Membership without4 = leader.membership();
	std::erase_if(without4.members, [](const Member& x) { return x.incarnation == 4; });
	check(c.change(1, without4), "the leader removes 4");
	c.run(300);
	bool released = false;
	for (const auto& batch : c.nodes.at(1).log) {
		for (const auto& a : batch) { if (auto r = std::get_if<SnapshotReleased>(&a); r && r->peer == 4 && r->reason == SnapshotReleaseReason::Removed) { released = true; } }
	}
	const auto before = c.sent_appends[{1, 4}];
	c.run(1000);
	check(released && c.sent_appends[{1, 4}] == before, "a removed member's snapshot is released and it gets no more appends");
	c.commit_one("c");
	check(first<PersistCheckpoint>(leader.step(LocalCheckpoint{2, 10, leader.applied(), leader.term(), founding_config(1, 4).cluster, founding_config(1, 4).configuration})) != nullptr, "the leader checkpoints again");

	auto d = founded();
	d.commit_one("a");
	auto& self = d.core(1);
	const Term term = self.term();
	Membership without1 = self.membership();
	std::erase_if(without1.members, [](const Member& x) { return x.incarnation == 1; });
	check(d.change(1, without1), "the leader removes itself");
	d.run(300);
	auto l = d.leader();
	check(l && *l != 1 && d.core(*l).term() == term + 1, "a self-removing leader hands off by transfer within one election");
}

// M6: a new member joins as a learner, never directly as a voter.
void direct_voter_refused() {
	auto c = founded();
	c.commit_one("a");
	Membership direct = c.core(1).membership(); direct.members.push_back({4, 4, MemberRole::Voter}); direct.next_incarnation = 5;
	RejectReason reason{};
	check(!c.change(1, direct, &reason) && reason == RejectReason::InvalidMembership, "a new member cannot join as a voter");
}

// Condition 5: a witness's truncation bound is the current leader's report, within what the
// leader committed; a lower report lowers it.
void witness_bound() {
	Membership founding{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 4, 0};
	RecoveredState state; state.configuration = founding_config(3); state.membership = founding;
	Core w(founding_config(3), state, {}, compact); w.step(Start{});
	std::vector<Entry> entries;
	for (Index i = 1; i <= 6; ++i) { entries.push_back({i, 1, i == 1 ? EntryKind::NoOp : EntryKind::Command, i == 1 ? "" : "x"}); }
	auto appended = w.step(Receive{1, AppendRequest{1, 9, 0, 0, 4, 0, entries, 5}});
	if (auto p = first<Persist>(appended)) {
		auto out = w.step(Persisted{p->token});
		if (auto committed = first<Committed>(out)) { w.step(Applied{committed->entries.back().index}); }
	}
	check(w.witness_bound() == 4, "the bound is clamped to the leader's commit");
	w.step(Receive{1, AppendRequest{1, 10, 6, 1, 6, 0, {}, 2}});
	check(w.witness_bound() == 2, "a later, lower report lowers the bound");
	check(!first<PersistCheckpoint>(w.step(LocalCheckpoint{1, 9, 4, 1, founding_config(3).cluster, founding_config(3).configuration})), "the witness refuses a checkpoint past it");
}


// T2's seeded fault "D3(a) off" (Ongaro, raft-dev, 2015): leader 1 promotes learner 5, held
// only by 1 and 5; 2, 3, and 4 elect 2, which removes 1 before committing an entry of its
// term, and commits that with 3 alone; healed, 1 wins with 4 and 5 under its promotion.
// With D3(a), 2's change waits for its term's commit, and the two never diverge.
void single_server_change() {
	auto c = founded(4);
	c.add_joiner(5);
	c.commit_one("a");
	Membership with5 = c.core(1).membership(); with5.members.push_back({5, 5, MemberRole::Learner}); with5.next_incarnation = 6;
	check(c.change(1, with5), "the leader adds learner 5");
	c.run(400);
	auto cut_off = [&](NodeId a, std::initializer_list<NodeId> others) {
		for (NodeId b : others) { c.cut.insert({a, b}); c.cut.insert({b, a}); }
	};
	cut_off(1, {2, 3, 4});
	Membership promoted = c.core(1).membership();
	for (auto& x : promoted.members) { if (x.incarnation == 5) { x.role = MemberRole::Voter; } }
	check(c.change(1, promoted), "the leader promotes 5; only 1 and 5 hold it");
	c.run(50);
	const Index at = c.core(1).last_index();
	cut_off(5, {2, 3, 4});
	c.nodes.at(1).down = true; c.nodes.at(5).down = true;
	// 3 and 4 have not heard from a leader for over election_min, but their timers have not
	// fired; 2's does, so it alone campaigns.
	c.now += 60;
	c.consume(2, c.core(2).step(Tick{c.now + 40, 100}));
	auto& two = c.core(2);
	for (int i = 0; i < 1000 && two.role() != Role::Leader && !c.wire.empty(); ++i) { c.pump(1); }
	check(two.role() == Role::Leader, "2 leads 2, 3, and 4");
	// 2's term never commits: from here 4 never hears from it.
	c.cut.insert({2, 4});
	std::erase_if(c.wire, [](const ProbeEnvelope& e) { return e.from == 2 && e.to == 4; });
	c.pump();
	Membership without1 = two.membership();
	std::erase_if(without1.members, [](const Member& x) { return x.incarnation == 1; });
	RejectReason reason{};
	const bool admitted = c.change(2, without1, &reason);
	check(!admitted && reason == RejectReason::NotReady, "2 cannot change the membership before committing an entry of its term (D3(a))");
	c.run(300);
	// 2 and 3 committed whatever they could; now only 1, 4, and 5 run.
	const Index two_committed = two.committed();
	c.nodes.at(2).down = true; c.nodes.at(3).down = true;
	c.nodes.at(1).down = false; c.nodes.at(5).down = false;
	c.cut.clear();
	c.now += 300; c.consume(1, c.core(1).step(Tick{c.now, 100}));
	c.run(1000);
	const bool diverged = c.core(1).committed() >= at && two_committed >= at && c.core(1).committed_membership() != c.core(2).committed_membership() &&
		find_member(c.core(1).committed_membership(), 1) && !find_member(c.core(2).committed_membership(), 1);
	check(!diverged, "no two members commit different memberships at one index");
}

}  // namespace

int main() {
	try {
		promotion_window(false);
		promotion_window(true);
		joiner_install_and_checkpoint();
		limits_and_reopen();
		removal();
		direct_voter_refused();
		witness_bound();
		single_server_change();
	} catch (const std::exception& error) {
		check(false, error.what());
	}
	std::printf("%d membership checks, %d failures\n", checks, failures);
	return failures ? 1 : 0;
}
