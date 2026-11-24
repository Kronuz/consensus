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

// Core::next_deadline() names every Tick that acts (docs/mechanisms/deadlines.md): a cluster of
// Cores whose messages travel without Ticks, so each case reaches a state a Receive or a
// Persisted left, reads the deadline, and checks that a Tick at it acts and one before it does not.
#include "consensus/core.h"
#include "consensus/membership.h"

#include <cstdio>
#include <deque>
#include <limits>
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
constexpr auto never = std::numeric_limits<std::uint64_t>::max();

template <class T> const T* first(const Actions& actions) {
	for (const auto& a : actions) { if (auto v = std::get_if<T>(&a)) { return v; } }
	return nullptr;
}
FixedConfiguration config(NodeId local, std::size_t voters) {
	FixedConfiguration c; c.cluster[0] = 'D'; c.configuration[0] = 'L'; c.local = local;
	for (NodeId v = 1; v <= voters; ++v) { c.voters.push_back(v); }
	return c;
}

// Persistence and application complete at once; messages wait on the wire until deliver(),
// which steps no Tick, so a core's clock moves only on the Ticks a case gives it.
class Net {
public:
	struct Node { std::unique_ptr<Core> core; std::uint64_t delay; bool hold = false; std::vector<Token> held; };
	std::map<NodeId, Node> nodes;
	std::deque<std::tuple<NodeId, NodeId, Message>> wire;
	std::set<std::pair<NodeId, NodeId>> cut;  // (from, to) dropped
	std::map<std::pair<NodeId, NodeId>, std::uint64_t> appended;  // (from, to): the last append's clock
	std::uint64_t now = 0;
	RequestId request = 1000;

	Core& add(NodeId id, FixedConfiguration c, RecoveredState state, std::uint64_t delay, Limits limits = {}) {
		auto& n = nodes[id];
		n.core = std::make_unique<Core>(std::move(c), std::move(state), limits, compact); n.delay = delay;
		step(id, Tick{now, delay}); step(id, Start{});
		return *n.core;
	}
	Core& founding(NodeId id, std::size_t voters, std::uint64_t delay, Limits limits = {}) {
		RecoveredState s; s.configuration = config(id, voters);
		return add(id, config(id, voters), std::move(s), delay, limits);
	}
	Core& joiner(NodeId id) {
		RecoveredState s; s.configuration = config(id, 3); s.configuration.voters.clear(); s.membership = Membership{};
		auto c = config(id, 3); c.voters.clear();
		return add(id, std::move(c), std::move(s), 200);
	}
	Core& core(NodeId id) { return *nodes.at(id).core; }
	Actions step(NodeId id, Event event) {
		auto actions = core(id).step(std::move(event));
		consume(id, actions);
		return actions;
	}
	void consume(NodeId id, const Actions& actions) {
		auto& n = nodes.at(id);
		for (const auto& a : actions) {
			if (auto p = std::get_if<Persist>(&a)) {
				if (n.hold) { n.held.push_back(p->token); } else { step(id, Persisted{p->token}); }
			} else if (auto pc = std::get_if<PersistCheckpoint>(&a)) {
				step(id, Persisted{pc->token});
			} else if (auto c = std::get_if<Committed>(&a)) {
				step(id, Applied{c->entries.back().index});
			} else if (auto s = std::get_if<Send>(&a)) {
				if (std::holds_alternative<AppendRequest>(s->message)) { appended[{id, s->peer}] = now; }
				if (!cut.contains({id, s->peer})) { wire.emplace_back(id, s->peer, s->message); }
			}
		}
	}
	void release(NodeId id) {
		auto& n = nodes.at(id); n.hold = false;
		for (auto token : std::exchange(n.held, {})) { step(id, Persisted{token}); }
	}
	void deliver() {
		for (std::size_t bound = 100000; !wire.empty() && bound--;) {
			auto [from, to, message] = std::move(wire.front()); wire.pop_front();
			if (nodes.contains(to)) { step(to, Receive{from, std::move(message)}); }
		}
	}
	Actions tick(NodeId id, std::uint64_t at) { now = at; return step(id, Tick{at, nodes.at(id).delay}); }
	void run_to(std::uint64_t until) {
		while (now + 10 <= until) {
			now += 10;
			for (auto& [id, n] : nodes) { step(id, Tick{now, n.delay}); }
			deliver();
		}
	}
	bool change(NodeId leader, Membership next) {
		auto actions = step(leader, ChangeMembership{++request, std::move(next)});
		return !first<Reject>(actions);
	}
	void propose(NodeId leader, std::string command) { step(leader, Propose{++request, std::move(command)}); }
};

// The invariant every case ends on: a Tick before next_deadline() changes nothing.
void quiet_before(Core& core, std::uint64_t at, std::uint64_t delay, const char* what) {
	const auto deadline = core.next_deadline();
	if (deadline <= at) { return; }
	const auto retired = core.retired_log_entries();
	auto actions = core.step(Tick{at, delay});
	check(actions.empty() && core.next_deadline() == deadline && core.retired_log_entries() == retired, what);
}

// Leader 1 of voters 1-3 with learner 4 caught up.
Net with_learner() {
	Net n;
	for (NodeId id = 1; id <= 3; ++id) { n.founding(id, 3, id == 1 ? 100 : 200); }
	n.run_to(300);
	n.joiner(4);
	Membership add = n.core(1).membership(); add.members.push_back({4, 4, MemberRole::Learner}); add.next_incarnation = 5;
	check(n.core(1).role() == Role::Leader && n.change(1, add), "leader 1 adds learner 4");
	n.deliver(); n.run_to(n.now + 200);
	return n;
}

// A1 (D3): once a replacement commits, the removal of its leaving member is due at once; a
// Tick at that deadline proposes it, and it commits.
void d3_removal() {
	auto n = with_learner();
	auto& leader = n.core(1);
	Membership replace = leader.membership();
	for (auto& x : replace.members) { if (x.incarnation == 4) { x.role = MemberRole::Voter; } }
	replace.leaving = 3;
	check(n.change(1, replace), "the leader replaces 3 by 4");
	n.deliver();  // replicates and commits it; no Tick reaches the leader
	auto due = leader.removal();
	check(due && leader.committed_membership() == replace, "the replacement commits and its removal is due");
	if (!due) { return; }
	check(leader.next_deadline() == n.now, "next_deadline() names the due removal");
	auto actions = n.tick(1, leader.next_deadline());
	auto persist = first<Persist>(actions);
	const bool appended = persist && persist->batch.log && persist->batch.log->entries.size() == 1 && persist->batch.log->entries[0].kind == EntryKind::Configuration;
	check(appended && decode_membership(persist->batch.log->entries[0].payload) == *due, "a Tick at that deadline proposes the removal");
	n.deliver();
	check(leader.committed_membership() == *due && !find_member(leader.committed_membership(), 3) && !leader.removal(), "the removal commits");
	quiet_before(leader, n.now, 100, "after the removal, a Tick before the deadline changes nothing");
}

// D3, at a full log: a removal that cannot be appended is not due (no Tick would place it); a
// compaction frees room, and the removal is due at once.
void d3_removal_at_full_log() {
	Limits small; small.log_entries = 24; small.control_entries = 2;
	Net n;
	n.founding(1, 3, 100, small);
	for (NodeId id = 2; id <= 3; ++id) { n.founding(id, 3, 200); }
	n.run_to(300);
	auto& leader = n.core(1);
	while (leader.last_index() < 22) { n.propose(1, "c"); n.deliver(); }
	n.joiner(4);
	Membership add = leader.membership(); add.members.push_back({4, 4, MemberRole::Learner}); add.next_incarnation = 5;
	check(n.change(1, add), "leader 1 adds learner 4");
	n.deliver(); n.run_to(n.now + 200);
	Membership replace = leader.membership();
	for (auto& x : replace.members) { if (x.incarnation == 4) { x.role = MemberRole::Voter; } }
	replace.leaving = 3;
	check(n.change(1, replace), "the leader replaces 3 by 4 with its last entry");
	n.deliver();
	check(leader.committed_membership() == replace && leader.last_index() == 24, "the replacement commits and fills the log");
	check(!leader.removal() && leader.next_deadline() > n.now, "a removal the log cannot take is not due");
	quiet_before(leader, n.now, 100, "a Tick at a full log changes nothing");
	n.step(1, LocalCheckpoint{++n.request, 7, leader.applied(), leader.term(), config(1, 3).cluster, config(1, 3).configuration});
	check(leader.retired_log_entries() > 0 && leader.next_deadline() == n.now, "after the compaction a Tick is due at once");
	auto actions = n.tick(1, n.now);
	auto persist = first<Persist>(actions);
	check(persist && persist->batch.log && persist->batch.log->entries.size() == 1 && persist->batch.log->entries[0].kind == EntryKind::Configuration &&
		!find_member(decode_membership(persist->batch.log->entries[0].payload), 3), "that Tick retires the compacted entries and proposes the removal");
}

// D9: a witness that leads hands off once a data voter answers after it took office. The
// answer comes in a Receive, which does not drive: next_deadline() names it at once.
void d9_witness_handoff() {
	Membership founding{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 4, 0};
	Net n;
	for (NodeId id : {1u, 2u, 3u}) {
		RecoveredState state; state.configuration = config(id, 3); state.membership = founding;
		n.add(id, config(id, 3), std::move(state), id == 3 ? 100 : 200);
	}
	n.now = 110;
	for (NodeId id : {1u, 2u, 3u}) { n.step(id, Tick{110, n.nodes.at(id).delay}); }
	n.deliver();  // the witness pre-votes, campaigns, wins and commits its entry, all at 110
	auto& witness = n.core(3);
	check(witness.role() == Role::Leader && witness.committed() == witness.last_index(), "the witness leads and commits at 110");
	const auto heartbeat = witness.next_deadline();
	check(heartbeat > 110, "its next deadline is its heartbeat");
	n.tick(3, heartbeat);  // no voter answered since it took office: no target yet
	n.deliver();           // they answer the heartbeat
	check(witness.role() == Role::Leader, "the witness still leads");
	check(witness.next_deadline() == heartbeat, "next_deadline() names the due hand-off");
	auto actions = n.tick(3, heartbeat);
	const Send* sent = nullptr;
	for (const auto& a : actions) {
		if (auto s = std::get_if<Send>(&a); s && std::holds_alternative<TimeoutNow>(s->message)) { sent = s; }
	}
	check(sent && sent->peer != 3 && std::get<TimeoutNow>(sent->message).term == witness.term(), "a Tick at that deadline sends TimeoutNow to a data voter");
	if (!sent) { return; }
	const auto target = sent->peer;
	n.deliver();
	check(n.core(target).role() == Role::Leader && witness.role() == Role::Follower, "the data voter takes over");
}

// D4: the drive that starts a removed leader's transfer handles its flights too, so a flight
// timeout due then is not left to a Tick no deadline names.
void d4_handoff_flights() {
	Net n;
	for (NodeId id = 1; id <= 4; ++id) { n.founding(id, 4, id == 1 ? 100 : 200); }
	n.run_to(300);
	auto& leader = n.core(1);
	check(leader.role() == Role::Leader, "1 leads voters 1-4");
	n.cut = {{1, 4}, {4, 1}, {4, 2}, {4, 3}};  // 4 hears nothing: its flights time out
	auto t = n.now;
	for (const auto start = n.appended[{1, 4}]; n.appended[{1, 4}] == start; ) { n.tick(1, ++t); n.deliver(); }
	const auto sent = n.appended[{1, 4}];  // a flight to 4 left at `sent`; it times out at sent + 40
	n.tick(1, sent + 25); n.deliver();     // a heartbeat at sent + 25: the next is due at sent + 45
	Membership without1 = leader.membership();
	std::erase_if(without1.members, [](const Member& x) { return x.incarnation == 1; });
	n.tick(1, sent + 30);
	check(n.change(1, without1), "1 removes itself");
	n.nodes.at(1).hold = true;
	n.deliver();  // 2 and 3 take it; the commit's persistence waits
	check(leader.busy(), "the commit persists");
	n.tick(1, sent + 40);  // while persisting: the clock only
	n.release(1);          // the commit's Persisted drives at sent + 40: the hand-off starts
	check(leader.committed_membership() == without1, "the removal commits");
	quiet_before(leader, sent + 40, 100, "after the hand-off's drive, a Tick at its clock changes nothing");
}

// Retirement: a compaction's entries retire 256 per step and count against the log until
// then; next_deadline() names a Tick at once while any remain.
void retirement() {
	Net n;
	for (NodeId id = 1; id <= 3; ++id) { n.founding(id, 3, id == 1 ? 100 : 200); }
	n.run_to(300);
	auto& leader = n.core(1);
	for (int i = 0; i < 600; ++i) { n.propose(1, "c"); n.deliver(); }
	check(leader.applied() > 600, "the leader applies 600 commands");
	n.step(1, LocalCheckpoint{++n.request, 7, leader.applied(), leader.term(), config(1, 3).cluster, config(1, 3).configuration});
	auto retired = leader.retired_log_entries();
	check(retired > 256, "the compaction leaves more than one step's retirement");
	for (int steps = 0; retired && steps < 8; ++steps) {
		check(leader.next_deadline() == n.now, "while entries remain to retire, a Tick is due at once");
		n.tick(1, leader.next_deadline());
		check(leader.retired_log_entries() < retired, "each Tick retires a slice");
		retired = leader.retired_log_entries();
	}
	check(!retired && leader.next_deadline() > n.now, "once retired, the leader sleeps to its heartbeat");
	quiet_before(leader, n.now, 100, "then a Tick before the deadline changes nothing");
}

}  // namespace

int main() {
	d3_removal();
	d3_removal_at_full_log();
	d9_witness_handoff();
	d4_handoff_flights();
	retirement();
	std::printf("%d checks, %d failures\n", checks, failures);
	return failures ? 1 : 0;
}
