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

// Track C, D15: pipelined appends. A verified peer has up to `pipeline` Append RPCs and
// `window_bytes` in flight; an unverified one, or one whose flight failed or timed out,
// has one at its verified progress; and a group pipelining through loss and reordering
// commits the same log everywhere.
#include "consensus/core.h"
#include "consensus/membership.h"

#include <algorithm>
#include <cstdio>
#include <deque>
#include <map>
#include <memory>
#include <random>
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

FixedConfiguration config_of(NodeId local, std::size_t voters = 3) {
	FixedConfiguration c; c.cluster[0] = 'P'; c.configuration[0] = 'L'; c.local = local;
	for (NodeId v = 1; v <= voters; ++v) { c.voters.push_back(v); }
	return c;
}

struct Node {
	std::unique_ptr<Core> core;
	std::vector<std::string> applied;
};
struct Envelope { NodeId from, to; Message message; };

class Cluster {
public:
	std::map<NodeId, Node> nodes;
	std::deque<Envelope> wire;
	std::set<std::pair<NodeId, NodeId>> cut;  // (from, to) dropped
	std::uint64_t now = 0;
	RequestId request = 1000;
	std::mt19937_64 rng{1};
	double loss = 0;      // the chance a message is dropped
	bool reorder = false;  // deliver in a random order
	std::size_t drop_append_responses = 0;  // the next this many Append responses are lost
	std::map<std::pair<NodeId, NodeId>, std::size_t> appends;  // non-empty Append RPCs sent

	Cluster(std::size_t voters, Limits limits, Flow flow) {
		for (NodeId id = 1; id <= voters; ++id) {
			RecoveredState s; s.configuration = config_of(id, voters);
			nodes[id].core = std::make_unique<Core>(config_of(id, voters), std::move(s), limits, compact, flow);
			consume(id, nodes[id].core->step(Tick{now, 100 + 20 * id}));
			consume(id, nodes[id].core->step(Start{}));
		}
		now = 100; consume(1, core(1).step(Tick{now, 100}));
		run(400);
	}
	Core& core(NodeId id) { return *nodes.at(id).core; }
	void consume(NodeId id, Actions actions) {
		auto& n = nodes.at(id);
		for (auto& a : actions) {
			if (auto p = std::get_if<Persist>(&a)) {
				consume(id, n.core->step(Persisted{p->token}));
			} else if (auto s = std::get_if<Send>(&a)) {
				if (auto r = std::get_if<AppendRequest>(&s->message); r && !r->entries.empty()) { ++appends[{id, s->peer}]; }
				wire.push_back({id, s->peer, s->message});
			} else if (auto c = std::get_if<Committed>(&a)) {
				for (const auto& e : c->entries) { if (e.kind == EntryKind::Command) { n.applied.push_back(e.payload); } }
				consume(id, n.core->step(Applied{c->entries.back().index}));
			}
		}
	}
	void pump() {
		for (std::size_t bound = 100000; !wire.empty() && bound--;) {
			if (reorder && wire.size() > 1) { std::swap(wire.front(), wire[rng() % wire.size()]); }
			auto e = std::move(wire.front()); wire.pop_front();
			if (cut.contains({e.from, e.to}) || (loss > 0 && double(rng() % 1000) < loss * 1000)) { continue; }
			if (drop_append_responses && std::holds_alternative<AppendResponse>(e.message)) {
				--drop_append_responses;
				continue;
			}
			consume(e.to, core(e.to).step(Tick{now, 100 + 20 * e.to}));
			consume(e.to, core(e.to).step(Receive{e.from, std::move(e.message)}));
		}
	}
	void tick(std::uint64_t dt = 10) {
		now += dt;
		for (auto& [id, n] : nodes) { consume(id, n.core->step(Tick{now, 100 + 20 * id})); }
		pump();
	}
	void run(std::uint64_t ms) { for (std::uint64_t t = 0; t < ms; t += 10) { tick(10); } }
	std::optional<NodeId> leader() {
		std::optional<NodeId> best;
		for (auto& [id, n] : nodes) {
			if (n.core->role() == Role::Leader && (!best || n.core->term() > core(*best).term())) { best = id; }
		}
		return best;
	}
	// Proposes on the leader without delivering anything.
	void propose(const std::string& command) {
		const auto l = *leader();
		consume(l, core(l).step(Propose{++request, command}));
	}
};

// A verified peer whose responses are lost has `pipeline` RPCs out, never more; once
// they time out it falls back to one flight at its verified progress.
void depth() {
	Limits limits; limits.rpc_entries = 2;
	Cluster c(3, limits, Flow{4, 64 << 20});
	check(c.leader() == 1u, "node 1 leads");
	c.propose("warm"); c.run(100);  // node 2 answers: verified
	c.cut = {{2, 1}};
	c.appends.clear();
	for (int i = 0; i < 20; ++i) { c.propose("c" + std::to_string(i)); }
	c.pump();
	check(c.appends[{1, 2}] == 4, "four RPCs in flight to a verified peer");
	check(c.appends[{1, 3}] >= 5, "the answering peer is not held back");
	c.appends.clear();
	c.tick(compact.rpc_timeout + 10);
	check(c.appends[{1, 2}] == 1, "after a timeout, one flight at verified progress");
	c.cut.clear(); c.run(400);
	check(c.core(2).last_index() == c.core(1).last_index(), "the peer catches up once answers return");
}

// A success retires every older flight: losing the oldest response costs no timeout and
// no resend, since a later success proves the prefix.
void lost_response() {
	Limits limits; limits.rpc_entries = 1;
	Cluster c(2, limits, Flow{4, 64 << 20});
	c.propose("warm"); c.run(100);
	const auto before = c.appends[{1, 2}];
	for (int i = 0; i < 3; ++i) { c.propose("r" + std::to_string(i)); }
	c.drop_append_responses = 1;  // the first flight's answer
	c.pump();
	check(c.core(1).committed() == c.core(1).last_index(), "the later answers commit everything");
	// The lost answer's flight is retired with the rest: the whole pipeline is free.
	c.cut = {{2, 1}};
	const auto free_before = c.appends[{1, 2}];
	for (int i = 0; i < 8; ++i) { c.propose("f" + std::to_string(i)); }
	c.pump();
	check(c.appends[{1, 2}] - free_before == 4, "four new flights: no stale one holds a slot");
	(void)before;
}

// An unverified peer (a new leader's, before any success) has one flight.
void unverified() {
	Limits limits; limits.rpc_entries = 1;
	Cluster c(3, limits, Flow{4, 64 << 20});
	c.cut = {{2, 1}, {3, 1}};
	// Node 2 takes over: it has heard no success from anyone yet.
	c.consume(2, c.core(2).step(Tick{c.now, 0}));
	for (int t = 0; t < 400 && c.leader() != 2u; t += 10) {
		c.cut = {{1, 2}, {1, 3}, {3, 1}};
		c.tick(10);
	}
	if (c.leader() != 2u) { check(false, "node 2 leads"); return; }
	c.cut = {{1, 2}, {3, 2}};  // node 2 hears no one from here on
	c.appends.clear();
	for (int i = 0; i < 6; ++i) { c.propose("u" + std::to_string(i)); }
	c.pump();
	// Node 1 never answered node 2: its first flight is still out, and no second follows.
	// Node 3 answered the election's no-op, so it is verified and pipelines.
	check(c.appends[{2, 1}] == 0, "an unverified peer gets no second flight");
	check(c.appends[{2, 3}] == 4, "a verified one gets the pipeline");
}

// The window bounds bytes in flight: 1 MiB entries under a 2 MiB window go two at a time.
void window() {
	Limits limits; limits.rpc_entries = 1; limits.command_bytes = 2 << 20; limits.rpc_bytes = 4 << 20;
	Cluster c(3, limits, Flow{8, 2 << 20});
	c.propose("warm"); c.run(100);
	c.cut = {{2, 1}};
	c.appends.clear();
	for (int i = 0; i < 6; ++i) { c.propose(std::string(1 << 20, char('a' + i))); }
	c.pump();
	check(c.appends[{1, 2}] == 2, "two 1 MiB RPCs fill a 2 MiB window");
}

// Pipelining through loss and reordering converges: every node applies the same log.
void converge() {
	for (std::uint64_t seed = 1; seed <= 20; ++seed) {
		Limits limits; limits.rpc_entries = 3;
		Cluster c(3, limits, Flow{4, 1 << 20});
		c.rng.seed(seed); c.loss = 0.1; c.reorder = true;
		std::vector<std::string> want;
		for (int i = 0; i < 100; ++i) {
			if (auto l = c.leader(); l && !c.core(*l).busy()) {
				want.push_back("s" + std::to_string(seed) + "-" + std::to_string(i));
				c.consume(*l, c.core(*l).step(Propose{++c.request, want.back()}));
			}
			c.tick(10);
		}
		c.loss = 0; c.reorder = false; c.run(3000);
		const auto& first = c.nodes.at(1).applied;
		bool same = true;
		for (auto& [id, n] : c.nodes) { same = same && n.applied == first; }
		check(same, "every node applies the same commands");
		check(!first.empty() && std::is_sorted(first.begin(), first.end(), [&](const std::string& a, const std::string& b) {
			return std::find(want.begin(), want.end(), a) < std::find(want.begin(), want.end(), b);
		}), "in proposal order");
	}
}

}  // namespace

int main() {
	depth();
	lost_response();
	unverified();
	window();
	converge();
	std::printf("pipeline_test: %d checks, %d failures\n", checks, failures);
	return failures ? 1 : 0;
}
