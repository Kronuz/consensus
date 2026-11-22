// Wake orderings at rest (docs/mechanisms/quiescence.md, Rest): three voters at rest, and node 9
// standing for a removed member whose pre-votes (refused: it holds no log) wake whoever they
// reach. Each case drives one ordering by hand, then checks that the leader never rests for more
// than a heartbeat interval while a follower's election timer runs, that no term moves in ten
// election timeouts, and that the group rests again.
//   at_rest   an idle group sends nothing once at rest.
//   s1        the leader is woken and heartbeats its followers at rest; then the pre-vote wakes
//             both, and their acknowledgements reach an awake leader.
//   s2        the commit heartbeat that rests both followers is still persisting when the
//             pre-vote wakes them: their answers match the leader's flights, and only the
//             acknowledgements deferred behind the persistence keep the leader awake.
//   s2c       s2 with the pre-vote after the persistence: the control.
//   s3        every message delivered twice after a wake.
//   s4        s1, with each acknowledgement overtaking its follower's answer to the round.
//   s5        s1, with both acknowledgements lost.
//   s6        driver_replace_test 1 1080 quiesce: one follower's acknowledgement reaches the
//             leader while it is still awake for the other's answer to its round.
//   s7        a stale frame from the leader (b's append, delivered again once b committed) ends
//             a follower's rest, and the follower's answer to it is lost.
// usage: quiescence_test [case]
#include "consensus/core.h"

#include <cstdio>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

using namespace kronuz::consensus;

namespace {

int failures = 0;
void expect(bool value, const std::string& what) {
	if (!value) { std::printf("FAIL %s\n", what.c_str()); ++failures; }
}

FixedConfiguration config(NodeId local) {
	FixedConfiguration c; c.cluster[0] = 'Q'; c.configuration[0] = 'G'; c.local = local; c.voters = {1, 2, 3};
	return c;
}

struct Replica { std::unique_ptr<Core> core; bool hold = false; std::vector<Persist> held; std::size_t sends = 0; };
struct Envelope { NodeId from, to; Message message; };

// Three cores over a network that delivers in order and at once, unless a case takes the queue.
struct Group {
	Timing timing{60, 300, 600, 1200, 3000};
	std::map<NodeId, Replica> replicas;
	std::deque<Envelope> network;
	std::uint64_t now = 1;
	RequestId request = 0;
	bool duplicate = false;
	std::uint64_t exposed_since = 0, exposed_most = 0;
	std::size_t to_rest = 0;  // messages from rest()'s write until the group rests
	Group() {
		timing.quiesce = true;
		for (NodeId id = 1; id <= 3; ++id) {
			auto& r = replicas[id];
			r.core = std::make_unique<Core>(config(id), RecoveredState{config(id), HardState{}, 0, 0, {}, 0, std::nullopt}, Limits{}, timing);
			act(id, r.core->step(Tick{now, delay(id)}));
			act(id, r.core->step(Start{}));
		}
	}
	Core& core(NodeId id) { return *replicas.at(id).core; }
	std::uint64_t delay(NodeId id) const { return timing.election_min + 150 * id; }
	void act(NodeId id, Actions actions) {
		auto& r = replicas.at(id);
		for (auto& a : actions) {
			if (auto p = std::get_if<Persist>(&a)) { if (r.hold) { r.held.push_back(*p); } else { act(id, r.core->step(Persisted{p->token})); } }
			else if (auto c = std::get_if<Committed>(&a)) { if (!c->entries.empty()) { act(id, r.core->step(Applied{c->entries.back().index})); } }
			else if (auto s = std::get_if<Send>(&a)) { ++r.sends; network.push_back({id, s->peer, s->message}); }
			else if (auto f = std::get_if<Fenced>(&a)) { expect(false, "replica fenced: " + f->reason); }
		}
	}
	void release(NodeId id) {
		auto& r = replicas.at(id); r.hold = false;
		auto held = std::move(r.held); r.held.clear();
		for (auto& p : held) { act(id, r.core->step(Persisted{p.token})); }
	}
	void deliver(const Envelope& e) { if (replicas.count(e.to)) { act(e.to, core(e.to).step(Receive{e.from, e.message})); } }  // node 9 is no replica
	void settle() {
		for (int n = 0; !network.empty() && n < 100000; ++n) {
			auto e = std::move(network.front()); network.pop_front();
			deliver(e); if (duplicate) { deliver(e); }
		}
	}
	// Ticks every replica without delivering what the ticks send.
	void tick_only(std::uint64_t dt) { now += dt; for (auto& [id, r] : replicas) { act(id, r.core->step(Tick{now, delay(id)})); } }
	void tick(std::uint64_t dt) { tick_only(dt); settle(); observe(); }
	NodeId leader() { for (auto& [id, r] : replicas) { if (r.core->role() == Role::Leader) { return id; } } return 0; }
	std::vector<NodeId> followers(NodeId leader) const {
		std::vector<NodeId> out;
		for (NodeId id = 1; id <= 3; ++id) { if (id != leader) { out.push_back(id); } }
		return out;
	}
	// The leader rests while a follower of its term runs its election timer.
	bool exposed() {
		const NodeId l = leader();
		if (!l || !core(l).resting()) { return false; }
		for (auto id : followers(l)) {
			if (core(id).term() == core(l).term() && !core(id).resting()) { return true; }
		}
		return false;
	}
	void observe() {
		if (!exposed()) { exposed_since = 0; return; }
		if (!exposed_since) { exposed_since = now; }
		exposed_most = std::max(exposed_most, now - exposed_since);
	}
	// A removed member's pre-vote, refused everywhere.
	void prevote(NodeId to) { act(to, core(to).step(Receive{9, PreVoteRequest{core(to).term() + 1, 0, 0}})); }
	bool all_rest() { for (auto& [id, r] : replicas) { if (!r.core->resting()) { return false; } } return true; }
	// Elects, commits "a" and rests.
	NodeId rest() {
		for (int n = 0; n < 200 && !leader(); ++n) { tick(97); }
		const NodeId l = leader();
		for (auto& [id, r] : replicas) { r.sends = 0; }
		act(l, core(l).step(Propose{++request, "a"})); settle();
		tick(70);
		for (auto& [id, r] : replicas) { to_rest += r.sends; }
		expect(l && all_rest(), "three voters elect, commit and rest");
		return l;
	}
	// A heartbeat interval and two more, one unit at a time, then ten election timeouts: the
	// leader rests over a running timer for at most a heartbeat interval, no term moves, the
	// group rests again, and it sends at most `most` messages in those timeouts.
	void quiet(const std::string& name, Term term, std::size_t most) {
		observe();
		for (std::uint64_t n = 0; n < 3 * timing.heartbeat; ++n) { tick(1); }
		for (auto& [id, r] : replicas) { r.sends = 0; }
		for (int n = 0; n < 125; ++n) { tick(97); }
		std::size_t sends = 0; bool moved = false;
		for (auto& [id, r] : replicas) { sends += r.sends; moved |= r.core->term() != term; }
		expect(exposed_most <= timing.heartbeat, name + ": the leader rests over a running election timer for " + std::to_string(exposed_most) + " units, at most a heartbeat interval");
		expect(!moved, name + ": no term moves");
		expect(all_rest(), name + ": every replica rests again");
		expect(sends <= most, name + ": " + std::to_string(sends) + " messages in ten election timeouts at rest, at most " + std::to_string(most));
		std::printf("%s: exposed %llu units; %zu messages in ten election timeouts after\n", name.c_str(), (unsigned long long)exposed_most, sends);
	}
};

void at_rest() {
	Group g; const NodeId l = g.rest(); const Term term = g.core(l).term();
	for (auto& [id, r] : g.replicas) { r.sends = 0; }
	for (int n = 0; n < 1000; ++n) { g.tick(97); }
	std::size_t sends = 0; for (auto& [id, r] : g.replicas) { sends += r.sends; }
	expect(sends == 0 && g.core(l).term() == term && g.all_rest(), "at_rest: " + std::to_string(sends) + " messages in 80 election timeouts at rest, none expected");
	std::printf("at_rest: %zu messages from a write until rest, %zu in 80 election timeouts after\n", g.to_rest, sends);
}

void s1() {
	Group g; const NodeId l = g.rest(); const Term term = g.core(l).term();
	g.prevote(l); g.tick(1);  // the leader heartbeats; its followers answer at rest
	for (auto id : g.followers(l)) { g.prevote(id); }
	g.settle();
	expect(!g.core(l).resting(), "s1: the leader stays awake on its woken followers' acknowledgements");
	g.quiet("s1", term, 0);
}

void s2(bool control) {
	const std::string name = control ? "s2c" : "s2";
	Group g; const NodeId l = g.rest(); const Term term = g.core(l).term();
	g.act(l, g.core(l).step(Propose{++g.request, "b"})); g.settle();  // b committed at the leader
	for (auto id : g.followers(l)) { g.replicas.at(id).hold = true; }
	// The first frame with b's commit: each follower rests, persisting it, its answer deferred.
	auto held = [&] {
		bool all = true;
		for (auto id : g.followers(l)) { all &= !g.replicas.at(id).held.empty() && g.core(id).resting(); }
		return all;
	};
	for (int n = 0; n < 60 && !held(); ++n) { g.tick(1); }
	expect(held(), name + ": both followers rest with the commit persisting");
	if (!control) { for (NodeId id = 1; id <= 3; ++id) { g.prevote(id); } }
	for (auto id : g.followers(l)) { g.release(id); }
	g.settle();
	if (control) { for (NodeId id = 1; id <= 3; ++id) { g.prevote(id); } g.settle(); }
	expect(!g.exposed(), name + ": the leader does not rest while a follower's election timer runs");
	g.quiet(name, term, 0);
}

void s3() {
	Group g; const NodeId l = g.rest(); const Term term = g.core(l).term();
	g.duplicate = true;
	for (NodeId id = 1; id <= 3; ++id) { g.prevote(id); }
	g.settle();
	for (int n = 0; n < 20; ++n) { g.tick(60); }
	g.duplicate = false;
	g.tick(60); g.tick(60);
	expect(g.core(l).resting(), "s3: the leader rests within two heartbeats once duplication stops");
	g.exposed_most = 0;  // duplication itself keeps followers waking; measured from here
	g.quiet("s3", term, 0);
}

void s4(bool lose) {
	const std::string name = lose ? "s5" : "s4";
	Group g; const NodeId l = g.rest(); const Term term = g.core(l).term();
	g.prevote(l); g.tick_only(1);  // the leader's round, queued to both followers at rest
	auto round = std::move(g.network); g.network.clear();
	for (auto& e : round) { g.deliver(e); }  // answered at rest: queued
	auto answers = std::move(g.network); g.network.clear();
	for (auto id : g.followers(l)) { g.prevote(id); }
	auto acknowledgements = std::move(g.network); g.network.clear();
	std::size_t dropped = 0;
	for (auto& e : acknowledgements) {
		if (lose && std::holds_alternative<AppendResponse>(e.message)) { ++dropped; } else { g.network.push_back(e); }
	}
	for (auto& e : answers) { g.network.push_back(e); }
	g.settle();
	expect(!lose || dropped == 2, name + ": both acknowledgements dropped");
	if (!lose) { expect(!g.exposed(), name + ": the leader does not rest on answers its followers' acknowledgements overtook"); }
	g.quiet(name, term, 0);
}

void s6() {
	Group g; const NodeId l = g.rest(); const Term term = g.core(l).term();
	const NodeId a = l % 3 + 1, b = 6 - l - a;
	g.prevote(l); g.tick_only(1);  // the leader's round, queued to both followers at rest
	auto round = std::move(g.network); g.network.clear();
	for (auto& e : round) { g.deliver(e); }  // both answer at rest
	auto answers = std::move(g.network); g.network.clear();
	for (auto& e : answers) { if (e.from == a) { g.deliver(e); } }  // a's answer: told
	g.prevote(a); g.settle();  // a wakes; its acknowledgement reaches a leader awake for b
	for (auto& e : answers) { if (e.from == b) { g.deliver(e); } }
	g.settle();
	expect(!(g.core(l).resting() && !g.core(a).resting()), "s6: the leader does not rest while a's election timer runs");
	g.quiet("s6", term, 0);
}

void s7() {
	Group g; const NodeId l = g.rest(); const Term term = g.core(l).term();
	const NodeId f = l % 3 + 1;
	g.act(l, g.core(l).step(Propose{++g.request, "b"}));
	std::optional<Envelope> append;
	for (auto& e : g.network) { if (e.to == f && std::holds_alternative<AppendRequest>(e.message)) { append = e; } }
	expect(append.has_value(), "s7: b's append to a follower");
	g.settle();
	for (int n = 0; n < 10 && !g.all_rest(); ++n) { g.tick(60); }
	expect(g.all_rest(), "s7: the group rests with b committed");
	g.deliver(*append);  // the stale frame: nothing new, and the follower leaves rest
	expect(!g.core(f).resting(), "s7: the stale frame ends the follower's rest");
	std::erase_if(g.network, [&](const Envelope& e) { return e.from == f; });  // its answer, lost
	g.settle();
	g.quiet("s7", term, 0);
}

}  // namespace

int main(int argc, char** argv) {
	const std::string only = argc > 1 ? argv[1] : "";
	auto run = [&](const char* name, auto&& body) { if (only.empty() || only == name) { body(); } };
	run("at_rest", at_rest);
	run("s1", s1);
	run("s2", [] { s2(false); });
	run("s2c", [] { s2(true); });
	run("s3", s3);
	run("s4", [] { s4(false); });
	run("s5", [] { s4(true); });
	run("s6", s6);
	run("s7", s7);
	std::printf("quiescence: %s\n", failures ? "FAILED" : "ok");
	return failures ? 1 : 0;
}
