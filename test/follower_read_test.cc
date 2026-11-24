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

// A follower's Read, forwarded to its leader (docs/mechanisms/follower-read.md): the cases
// below, and a randomized cluster whose reads at any member must each return an index at or
// above every commit made before the read began. test/follower_read_mutants.py seeds a fault
// per case; the plain test fails at each.
//   usage: follower_read_test [case] [seeds]
#include "consensus/core.h"
#include "consensus/storage.h"
#include "consensus/transport/codec.h"

#include <cstdio>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <vector>

using namespace kronuz::consensus;

namespace {

// Ticks of 10 to 20, as the unit suite: heartbeat 20, election 100 to 200.
constexpr Timing compact{20, 40, 100, 200, 60000};
int failures = 0;
void check(bool value, std::string_view what) {
	if (!value) { ++failures; std::printf("FAIL %.*s\n", int(what.size()), what.data()); }
}

FixedConfiguration config(NodeId local, std::size_t voters) {
	FixedConfiguration c; c.cluster[0] = 'F'; c.configuration[0] = 'R'; c.local = local;
	for (NodeId id = 1; id <= voters; ++id) { c.voters.push_back(id); }
	return c;
}

struct Host {
	NodeId id;
	std::size_t voters;
	Limits limits;
	Timing timing;
	Token nonce;
	std::unique_ptr<Core> core;
	std::vector<std::string> journal;
	std::optional<Persist> pending;
	std::optional<Committed> applying;
	bool auto_persist = true, auto_apply = true;
	std::map<RequestId, Index> ready;  // ReadReady by request
	std::map<RequestId, std::pair<RejectReason, std::uint64_t>> rejected;  // and when
	Host(NodeId local, std::size_t count, Limits l, Timing t, Token n) : id(local), voters(count), limits(l), timing(t), nonce(n) {
		RecoveredState state; state.configuration = config(local, count);
		core = std::make_unique<Core>(config(local, count), std::move(state), limits, timing);
		journal.push_back(encode_initialization(config(local, count)));
	}
	// A crash and a restart from the journal: a fresh Core, its token counter from 0, a new nonce.
	void restart(Token next) {
		Recovery recovery(config(id, voters), limits);
		for (std::size_t i = 0; i < journal.size(); ++i) { recovery.replay(i + 1, journal[i]); }
		auto state = recovery.finish(journal.size());
		core = std::make_unique<Core>(config(id, voters), std::move(state), limits, timing);
		nonce = next; pending.reset(); applying.reset(); ready.clear(); rejected.clear();
	}
};

struct Envelope { NodeId from, to; Message message; };

template <class M> const M* as(const Envelope& e) { return std::get_if<M>(&e.message); }

// Cores over a network that delivers in order, with a Tick at the receiver's clock before each
// frame as the Driver gives; `hold` keeps matching frames back until release().
struct Network {
	std::map<NodeId, std::unique_ptr<Host>> hosts;
	std::deque<Envelope> network, held;
	std::vector<Envelope> sent;  // every frame, in send order
	std::set<NodeId> isolated;
	std::set<std::pair<NodeId, NodeId>> cut;
	std::function<bool(const Envelope&)> hold;
	std::uint64_t now = 0;
	Network(std::size_t count, Limits limits = {}, Timing timing = compact) {
		for (NodeId id = 1; id <= count; ++id) { hosts.emplace(id, std::make_unique<Host>(id, count, limits, timing, 1000 + id)); }
	}
	Host& host(NodeId id) { return *hosts.at(id); }
	Core& core(NodeId id) { return *host(id).core; }
	std::uint64_t delay(NodeId id) const { return compact.election_min + 20 * (id - 1); }
	void consume(NodeId id, Actions actions) {
		auto& h = host(id);
		for (auto& action : actions) {
			if (auto persist = std::get_if<Persist>(&action)) {
				check(!h.pending, "one disk batch outstanding");
				h.pending = std::move(*persist);
				if (h.auto_persist) { complete_disk(id); }
			} else if (auto send = std::get_if<Send>(&action)) {
				sent.push_back(Envelope{id, send->peer, send->message});
				network.push_back(Envelope{id, send->peer, std::move(send->message)});
			} else if (auto committed = std::get_if<Committed>(&action)) {
				check(!h.applying, "one application batch outstanding");
				h.applying = std::move(*committed);
				if (h.auto_apply) { complete_apply(id); }
			} else if (auto ready = std::get_if<ReadReady>(&action)) {
				check(!h.ready.contains(ready->request), "a read completes once");
				check(h.core->applied() >= ready->index, "a read completes only at an applied index");
				h.ready[ready->request] = ready->index;
			} else if (auto reject = std::get_if<Reject>(&action)) {
				h.rejected[reject->request] = {reject->reason, now};
			} else if (auto fenced = std::get_if<Fenced>(&action)) {
				check(false, "a replica fenced: " + fenced->reason);
			}
		}
	}
	void complete_disk(NodeId id) {
		auto& h = host(id);
		auto pending = std::move(*h.pending); h.pending.reset();
		h.journal.push_back(encode_storage_batch(pending.batch));
		consume(id, h.core->step(Persisted{pending.token}));
	}
	void complete_apply(NodeId id) {
		auto& h = host(id);
		auto committed = std::move(*h.applying); h.applying.reset();
		consume(id, h.core->step(Applied{committed.entries.back().index}));
	}
	void apply_all(NodeId id) { while (host(id).applying) { complete_apply(id); } }
	void start() {
		for (auto& [id, h] : hosts) { start(id); }
	}
	void start(NodeId id) {
		consume(id, core(id).step(Tick{now, delay(id)}));
		consume(id, core(id).step(Start{host(id).nonce}));
	}
	void restart(NodeId id, Token nonce) {
		host(id).restart(nonce);
		start(id);
	}
	void tick(NodeId id) { consume(id, core(id).step(Tick{now, delay(id)})); }
	void tick_all() { for (auto& [id, h] : hosts) { tick(id); } }
	bool blocked(const Envelope& e) const {
		return isolated.contains(e.from) || isolated.contains(e.to) || cut.contains({e.from, e.to}) || !hosts.contains(e.to);
	}
	void deliver(Envelope e) {
		tick(e.to);
		consume(e.to, core(e.to).step(Receive{e.from, std::move(e.message)}));
	}
	void pump(std::size_t bound = 100000) {
		while (!network.empty() && bound--) {
			auto e = std::move(network.front()); network.pop_front();
			if (blocked(e)) { continue; }
			if (hold && hold(e)) { held.push_back(std::move(e)); continue; }
			deliver(std::move(e));
		}
		check(network.empty(), "delivery drains within its bound");
	}
	// Delivers the held frames, past `hold`; what they cause goes through it.
	void release() {
		auto frames = std::move(held); held.clear();
		for (auto& e : frames) { if (!blocked(e)) { deliver(std::move(e)); } }
	}
	// Time moves to each host's next deadline in turn, through `until`, as a Driver's wheel does.
	void run_until(std::uint64_t until) {
		for (;;) {
			pump();
			auto at = std::numeric_limits<std::uint64_t>::max();
			for (auto& [id, h] : hosts) { at = std::min(at, std::max(now, h->core->next_deadline())); }
			if (at > until) { now = until; return; }
			now = at;
			std::set<NodeId> ticked;
			for (auto& [id, h] : hosts) { if (h->core->next_deadline() <= now) { tick(id); ticked.insert(id); } }
			// A Tick at its deadline moves the deadline past it, or the host would spin.
			for (NodeId id : ticked) {
				if (core(id).next_deadline() <= now) { check(false, "a deadline stays due after its Tick"); now = until; return; }
			}
		}
	}
	// Ticks every host each `step` through `until`, delivering as it goes.
	void advance(std::uint64_t until, std::uint64_t step = 10) {
		while (now < until) { now = std::min(until, now + step); tick_all(); pump(); }
	}
	// Ticks every host each 5 until `done` holds or `limit` passes.
	void advance_until(const std::function<bool()>& done, std::uint64_t limit) {
		while (!done() && now < limit) { now += 5; tick_all(); pump(); }
	}
	void elect(NodeId id) {
		now += 200; tick(id); pump();
		advance(now + 40);  // the commit index reaches every follower
		check(core(id).role() == Role::Leader, "the leader is elected");
	}
	RequestId request = 100;
	void propose(NodeId leader, std::string command) {
		consume(leader, core(leader).step(Propose{++request, std::move(command)}));
		pump(); advance(now + 40);
	}
	// A Tick first, as the Driver steps every call after its turn's Tick.
	RequestId read(NodeId id) { tick(id); auto r = ++request; consume(id, core(id).step(Read{r})); return r; }
	template <class M> const M* last(NodeId from, NodeId to) const {
		for (auto it = sent.rbegin(); it != sent.rend(); ++it) {
			if (it->from == from && it->to == to) { if (auto m = as<M>(*it)) { return m; } }
		}
		return nullptr;
	}
	template <class M> std::size_t count(NodeId from, NodeId to) const {
		std::size_t n = 0;
		for (const auto& e : sent) { n += e.from == from && e.to == to && as<M>(e); }
		return n;
	}
};

// A3 reads at 2 under leader 1, and 3 nodes up: the common start.
Network three(Limits limits = {}, Timing timing = compact) {
	Network n(3, limits, timing);
	n.start(); n.elect(1);
	n.propose(1, "a"); n.propose(1, "b");
	return n;
}

// A1: the follower's read completes at the leader's commit index at its arrival, once a quorum
// confirmed it, and only once the follower applied that index.
void confirmed_after_apply() {
	auto n = three();
	n.host(2).auto_apply = false;
	n.propose(1, "c");
	const auto committed = n.core(1).committed();
	const auto r = n.read(2);
	const auto* request = n.last<ReadIndexRequest>(2, 1);
	check(request && request->term == n.core(2).term() && request->start == n.host(2).nonce, "the read goes to the leader with this start's nonce");
	n.pump();
	const auto* answer = n.last<ReadIndexResponse>(1, 2);
	check(answer && answer->result == ReadIndexReply::Ready && answer->index == committed, "the leader answers with its commit index");
	check(!n.host(2).ready.contains(r) && !n.host(2).rejected.contains(r) && n.core(2).applied() < committed, "the read waits for the follower's apply");
	n.apply_all(2);
	check(n.host(2).ready.contains(r) && n.host(2).ready[r] == committed, "the read completes once the follower applied the index");
}

// A1: a leader cut off with one follower in a minority confirms nothing; the majority commits
// meanwhile, and the follower's read is refused, never answered with the old commit index.
void stale_leader() {
	Network n(5);
	n.start(); n.elect(1); n.propose(1, "a");
	const auto old = n.core(1).committed();
	for (NodeId a : {1, 2}) { for (NodeId b : {3, 4, 5}) { n.cut.insert({a, b}); n.cut.insert({b, a}); } }
	n.now += 10;
	const auto r = n.read(2);
	const auto sent_at = n.now;
	n.pump();
	n.advance(sent_at + 180);  // 3 wins its pre-vote and its election before 1's CheckQuorum
	check(n.core(3).role() == Role::Leader && n.core(3).term() > n.core(1).term(), "the majority elects a new leader");
	n.propose(3, "majority");
	check(n.core(3).committed() > old, "the majority commits past the old leader's index");
	n.advance(sent_at + 260);
	check(!n.host(2).ready.contains(r), "the minority leader's follower never answers the read");
	check(n.host(2).rejected.contains(r) && n.host(2).rejected[r].first == RejectReason::NotLeader, "the read is refused NotLeader");
}

// A1: a follower that loses its leader in the same term (LeaderGone) refuses its read at once,
// and the old leader's answer, arriving after, completes nothing.
void leader_lost() {
	auto n = three();
	n.hold = [](const Envelope& e) { return as<ReadIndexRequest>(e) != nullptr; };
	const auto r = n.read(2);
	n.pump();
	check(!n.held.empty() && !n.host(2).rejected.contains(r), "the read is in flight");
	n.consume(2, n.core(2).step(LeaderGone{1}));
	check(n.host(2).rejected.contains(r) && n.host(2).rejected[r].first == RejectReason::NotLeader, "losing the leader refuses the read at once");
	n.hold = nullptr; n.release(); n.pump();
	check(!n.host(2).ready.contains(r), "the lost leader's answer completes nothing");
}

// A1: a follower whose term moves refuses its read at once: by its own campaign, or by a
// higher term heard from another member.
void term_change() {
	{
		auto n = three();
		n.isolated.insert(1);
		n.now += 10;
		const auto r = n.read(2);
		const auto term = n.core(2).term();
		n.advance(n.now + 150);  // 2 times out, wins 3's pre-vote and campaigns
		check(n.core(2).term() > term, "the follower campaigns");
		check(n.host(2).rejected.contains(r) && n.host(2).rejected[r].first == RejectReason::NotLeader && n.host(2).rejected[r].second < n.now, "its campaign refuses the read");
	}
	{
		auto n = three();
		n.hold = [](const Envelope& e) { return as<ReadIndexRequest>(e) != nullptr; };
		const auto r = n.read(2);
		n.pump();
		const auto term = n.core(2).term();
		n.consume(2, n.core(2).step(Receive{3, VoteRequest{term + 1, n.core(2).last_index(), n.core(2).term_at(n.core(2).last_index()).value_or(0), true}}));
		check(n.core(2).term() == term + 1, "a higher term reaches the follower");
		check(n.host(2).rejected.contains(r) && n.host(2).rejected[r].first == RejectReason::NotLeader, "a higher term refuses the read at once");
	}
}

// A1: only the leader asked, in the read's term, with this start's nonce and a pending token,
// completes a read.
void answer_identity() {
	Network n(3);
	n.start(); n.elect(1);
	n.isolated.insert(1);
	n.advance_until([&] { return n.core(2).role() == Role::Leader; }, n.now + 400);  // 2 leads term 2
	n.isolated.clear(); n.advance(n.now + 40);
	check(n.core(2).role() == Role::Leader && n.core(2).term() == 2 && n.core(3).leader() == 2, "2 leads term 2, 3 follows it");
	n.propose(2, "a");
	n.hold = [](const Envelope& e) { return as<ReadIndexRequest>(e) != nullptr; };
	const auto r = n.read(3);
	n.pump();
	const auto* request = n.last<ReadIndexRequest>(3, 2);
	if (!request) { check(false, "the read is forwarded"); return; }
	const auto start = request->start, token = request->token;
	const auto index = n.core(3).applied();
	n.consume(3, n.core(3).step(Receive{1, ReadIndexResponse{2, start, token, ReadIndexReply::Ready, index}}));
	check(!n.host(3).ready.contains(r), "an answer from a peer not asked is ignored");
	n.consume(3, n.core(3).step(Receive{2, ReadIndexResponse{1, start, token, ReadIndexReply::Ready, index}}));
	check(!n.host(3).ready.contains(r), "an answer at another term is ignored");
	n.consume(3, n.core(3).step(Receive{2, ReadIndexResponse{2, start + 1, token, ReadIndexReply::Ready, index}}));
	check(!n.host(3).ready.contains(r), "an answer for another start is ignored");
	n.consume(3, n.core(3).step(Receive{2, ReadIndexResponse{2, start, token + 1, ReadIndexReply::Ready, index}}));
	check(!n.host(3).ready.contains(r), "an answer for another token is ignored");
	check(!n.host(3).rejected.contains(r), "the read still waits");
	n.hold = nullptr; n.release(); n.pump();
	check(n.host(3).ready.contains(r) && n.host(3).ready[r] == n.core(2).committed(), "the leader's own answer completes it");
}

// A2 (review V1): a follower restarts in the same term while the leader holds its read
// unconfirmed. Its new read reuses the old token; a write commits before it. The leader's
// answer to the old read, at the old index, must not complete the new one.
void restart_same_term() {
	auto n = three();
	// The follower's token counter well past what a restart's catch-up takes.
	for (int i = 0; i < 30; ++i) { n.read(2); n.pump(); }
	// A write's appends go out before the read arrives, so their answers carry no probe of it.
	n.hold = [](const Envelope& e) { return as<AppendResponse>(e) && e.to == 1; };
	n.consume(1, n.core(1).step(Propose{++n.request, "w"}));
	n.pump();
	const auto before = n.core(1).committed();
	const auto old_read = n.read(2);
	const auto* request = n.last<ReadIndexRequest>(2, 1);
	if (!request) { check(false, "the old read is forwarded"); return; }
	const auto old_start = request->start, old_token = request->token;
	const auto answered = n.count<ReadIndexResponse>(1, 2);
	n.pump();  // the leader admits it at `before`, its probe waits behind the write's flights
	// The write's answers commit it; the probes the leader sends next never reach a quorum.
	n.hold = nullptr; n.release();
	n.hold = [](const Envelope& e) { const auto* a = as<AppendResponse>(e); return a && a->read_probe && e.to == 1; };
	n.pump();
	const auto committed = n.core(1).committed();
	check(committed > before && n.count<ReadIndexResponse>(1, 2) == answered, "the write commits; the old read is held unconfirmed");
	// The follower crashes and restarts in the same term; it learns the leader again.
	n.restart(2, 77);
	n.advance(n.now + 40);
	check(n.core(2).leader() == 1 && n.core(2).term() == n.core(1).term(), "the restarted follower follows the same leader in the same term");
	n.apply_all(2);
	// Reads until one reuses the old token; earlier ones never reach the leader.
	auto keep = n.hold;
	n.hold = [keep](const Envelope& e) { return as<ReadIndexRequest>(e) || keep(e); };
	RequestId new_read = 0;
	for (int i = 0; i < 60 && !new_read; ++i) {
		const auto r = n.read(2);
		const auto* again = n.last<ReadIndexRequest>(2, 1);
		if (again && again->token == old_token && again->start == 77) { new_read = r; }
		n.pump();
	}
	check(new_read != 0, "the restarted follower reuses the old token");
	std::erase_if(n.held, [&](const Envelope& e) { const auto* q = as<ReadIndexRequest>(e); return q && q->start == 77; });
	// The old read's probes reach the quorum now; the leader answers it with the old index.
	n.hold = [](const Envelope& e) { return as<ReadIndexRequest>(e) != nullptr; };
	n.release(); n.pump(); n.advance(n.now + 40);
	const auto* old_answer = n.last<ReadIndexResponse>(1, 2);
	check(old_answer && old_answer->start == old_start && old_answer->token == old_token && old_answer->result == ReadIndexReply::Ready && old_answer->index == before && before < committed,
		"the leader answers the old read at its admission's index, below the write");
	check(!n.host(2).ready.contains(new_read), "the old start's answer never completes the new read");
}

// A3 (review V4): one deadline, election_max from the send, covers the answer and the apply.
void deadline() {
	{
		auto n = three();
		n.host(2).auto_apply = false;
		n.propose(1, "c");
		const auto r = n.read(2);
		const auto sent_at = n.now;
		n.pump();
		check(n.last<ReadIndexResponse>(1, 2) && !n.host(2).ready.contains(r), "answered, the read waits for the apply");
		check(n.core(2).next_deadline() <= sent_at + compact.election_max, "its deadline is among the follower's");
		n.run_until(sent_at + compact.election_max - 1);
		check(!n.host(2).rejected.contains(r), "the read waits until its deadline");
		n.run_until(sent_at + compact.election_max);
		check(n.host(2).rejected.contains(r) && n.host(2).rejected[r].first == RejectReason::NotLeader, "an apply not reached by the deadline refuses the read");
		n.apply_all(2);
		check(!n.host(2).ready.contains(r), "a late apply completes nothing");
	}
	{
		// The leader keeps heartbeating but never answers, then goes silent: the follower refuses at
		// its deadline, not at the election timeout that the last heartbeat set.
		auto n = three();
		n.hold = [](const Envelope& e) { return as<ReadIndexRequest>(e) != nullptr; };
		const auto r = n.read(2);
		const auto sent_at = n.now;
		n.run_until(sent_at + 150);
		n.isolated.insert(1);
		n.run_until(sent_at + compact.election_max);
		check(n.host(2).rejected.contains(r) && n.host(2).rejected[r].second <= sent_at + compact.election_max, "an unanswered read is refused at its deadline");
	}
}

// A3 (review V5): a read that arrives while another's confirmation is in flight is confirmed by
// a later probe, never by the one in flight; at the follower, each answer completes its own read.
void in_flight() {
	auto n = three();
	n.hold = [](const Envelope& e) { return as<AppendResponse>(e) && e.to == 1; };
	const auto a = n.read(2);
	n.pump();  // a's probes are out; their answers held
	const auto b = n.read(2);
	n.pump();  // b arrives while a's probes fly
	check(n.count<ReadIndexResponse>(1, 2) == 0, "nothing is confirmed yet");
	n.release(); n.pump();  // a's probes answered; the probes for b held
	check(n.host(2).ready.contains(a), "the read whose probe was answered completes");
	check(!n.host(2).ready.contains(b) && n.count<ReadIndexResponse>(1, 2) == 1, "the read that arrived during it waits for its own probe");
	n.hold = nullptr; n.release(); n.pump();
	check(n.host(2).ready.contains(b), "its own probe completes it");
	// At the follower: an answer to the first of two reads completes the first alone.
	n.host(2).auto_apply = false;
	n.propose(1, "c");
	n.hold = [](const Envelope& e) { return as<ReadIndexRequest>(e) != nullptr; };
	const auto c = n.read(2), d = n.read(2);
	n.pump();
	if (n.held.size() != 2) { check(false, "both reads are forwarded"); return; }
	auto second = n.held.back(); n.held.pop_back();
	n.release(); n.pump();
	n.apply_all(2);
	check(n.host(2).ready.contains(c) && !n.host(2).ready.contains(d), "one answer completes only its own read");
	n.hold = nullptr; n.network.push_back(second); n.pump(); n.apply_all(2);
	check(n.host(2).ready.contains(d), "the second read completes on its own answer");
}

// Review V3: the leader's refusals reach the follower at once, each with its reason.
void refusals() {
	{
		// NotReady: a leader whose term has no committed entry yet.
		Network n(3);
		n.start(); n.elect(1);
		n.isolated.insert(1);
		n.hold = [](const Envelope& e) { return as<AppendResponse>(e) && e.to == 2; };
		n.advance_until([&] { return n.core(2).role() == Role::Leader && n.core(3).leader() == 2; }, n.now + 400);
		check(n.core(2).role() == Role::Leader && n.core(3).leader() == 2, "2 leads, its no-op uncommitted");
		const auto r = n.read(3);
		n.pump();
		check(n.host(3).rejected.contains(r) && n.host(3).rejected[r].first == RejectReason::NotReady, "NotReady reaches the follower at once");
	}
	{
		// Busy: the leader's reads are at limits.reads.
		Limits limits; limits.reads = 1;
		auto n = three(limits);
		n.hold = [](const Envelope& e) { return as<AppendResponse>(e) && e.to == 1; };
		n.read(1); n.pump();
		const auto r = n.read(2);
		n.pump();
		check(n.host(2).rejected.contains(r) && n.host(2).rejected[r].first == RejectReason::Busy, "Busy reaches the follower at once");
	}
	{
		// NotLeader: the leader steps down (CheckQuorum) with the read unconfirmed.
		auto n = three();
		n.hold = [](const Envelope& e) { return as<AppendResponse>(e) && e.to == 1; };
		n.now += 10;
		const auto r = n.read(2);
		const auto sent_at = n.now;
		n.pump();
		n.run_until(sent_at + compact.election_max - 1);
		check(n.core(1).role() != Role::Leader, "the leader steps down");
		check(n.host(2).rejected.contains(r) && n.host(2).rejected[r].first == RejectReason::NotLeader && n.host(2).rejected[r].second < sent_at + compact.election_max,
			"its step-down refuses the read before the follower's deadline");
	}
}

// Review V7: a follower holding a read never rests and its deadline stays armed; a resting
// leader wakes on the request.
void at_rest() {
	Timing timing = compact; timing.quiesce = true;
	{
		auto n = three({}, timing);
		n.run_until(n.now + 1000);
		check(n.core(1).resting() && n.core(2).resting(), "the group rests");
		const auto r = n.read(2);
		const auto sent_at = n.now;
		n.pump();
		check(n.host(2).ready.contains(r) && n.now == sent_at, "a read at a rested group completes at once");
		n.run_until(n.now + 1000);
		check(n.core(1).resting() && n.core(2).resting(), "the group rests again");
	}
	{
		auto n = three({}, timing);
		n.run_until(n.now + 1000);
		n.hold = [](const Envelope& e) { return as<ReadIndexResponse>(e) != nullptr; };
		const auto r = n.read(2);
		const auto sent_at = n.now;
		n.pump();
		check(!n.core(2).resting() && n.core(2).next_deadline() <= sent_at + compact.election_max, "a follower holding a read stays awake with its deadline armed");
		n.run_until(sent_at + compact.election_max);
		check(n.host(2).rejected.contains(r) && n.host(2).rejected[r].second <= sent_at + compact.election_max, "a read at a rested group refuses within election_max");
	}
}

// A host that gives no nonce keeps today's behavior: a follower's read is refused.
void no_nonce() {
	Network n(3);
	for (auto& [id, h] : n.hosts) { h->nonce = 0; }
	n.start(); n.elect(1); n.propose(1, "a");
	const auto r = n.read(2);
	check(n.host(2).rejected.contains(r) && n.host(2).rejected[r].first == RejectReason::NotLeader && n.count<ReadIndexRequest>(2, 1) == 0, "no nonce, no forwarded read");
	const auto local = n.read(1); n.pump();
	check(n.host(1).ready.contains(local), "the leader's reads are unchanged");
}

// A4: the frames round-trip the peer codec; malformed ones are refused.
void codec() {
	using kronuz::journal::Corruption;
	const std::vector<Message> frames{ReadIndexRequest{7, 0x1234567890abcdefULL, 11}, ReadIndexResponse{7, 3, 11, ReadIndexReply::Ready, 42},
		ReadIndexResponse{7, 3, 12, ReadIndexReply::NotReady}, ReadIndexResponse{7, 3, 13, ReadIndexReply::Busy}, ReadIndexResponse{7, 3, 14, ReadIndexReply::NotLeader}};
	for (const auto& frame : frames) {
		const auto bytes = transport::encode(frame);
		check(transport::encode(transport::decode(bytes)) == bytes && transport::decode(bytes).index() == frame.index(), "a read frame round-trips");
	}
	auto refused = [](const Message& m) { try { (void)transport::encode(m); return false; } catch (const std::invalid_argument&) { return true; } };
	check(refused(ReadIndexResponse{7, 3, 11, ReadIndexReply::Ready, 0}), "Ready without an index is not encoded");
	check(refused(ReadIndexResponse{7, 3, 11, ReadIndexReply::Busy, 5}), "a refusal with an index is not encoded");
	check(refused(ReadIndexResponse{7, 3, 11, static_cast<ReadIndexReply>(4), 0}), "an unknown reply is not encoded");
	auto bytes = transport::encode(ReadIndexResponse{7, 3, 11, ReadIndexReply::Busy});
	auto corrupt = [](std::string b) { try { (void)transport::decode(b); return false; } catch (const Corruption&) { return true; } };
	auto reply = bytes; reply[4 + 8 * 4] = 4;  // the reply word, little-endian
	check(corrupt(reply), "an unknown reply is refused");
	auto indexed = bytes; indexed[4 + 8 * 5] = 1;  // an index on a refusal
	check(corrupt(indexed), "a refusal with an index is refused");
	check(corrupt(bytes.substr(0, bytes.size() - 1)) && corrupt(bytes + '\0'), "a short or long frame is refused");
	auto request = transport::encode(ReadIndexRequest{7, 3, 11});
	check(corrupt(request.substr(0, request.size() - 8)), "a request without its token is refused");
}

// A1: random partitions, drops, restarts and leader changes over 3 or 5 voters, with reads at
// every member. Each completed read returns an index at or above every commit made anywhere
// before it began, at a replica that applied it.
void randomized(std::uint64_t seeds) {
	std::uint64_t reads = 0, refused = 0, forwarded = 0;  // completed, refused, completed at a follower
	for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
		std::mt19937_64 random(seed);
		const std::size_t count = seed % 2 ? 3 : 5;
		Network n(count);
		n.start();
		std::map<std::pair<NodeId, RequestId>, std::pair<Index, bool>> floors;  // the commit anywhere at each read's start; at a follower
		Token nonce = 5000;
		std::uint64_t ready_seen = 0;
		for (int step = 0; step < 1500; ++step) {
			const auto pick = random() % 1000;
			const NodeId id = 1 + random() % count;
			if (pick < 300) {
				n.now += 1 + random() % 15; n.tick_all();
			} else if (pick < 450) {
				for (auto& [i, h] : n.hosts) { if (h->core->role() == Role::Leader) { n.consume(i, h->core->step(Propose{++n.request, "x"})); } }
			} else if (pick < 700) {
				n.tick(id);  // read() ticks first; the floor is taken after it
				Index floor = 0;
				for (auto& [i, h] : n.hosts) { floor = std::max(floor, h->core->committed()); }
				const bool follower = n.core(id).role() == Role::Follower;
				floors[{id, n.read(id)}] = {floor, follower};
			} else if (pick < 760) {
				n.cut.clear();
				const NodeId a = 1 + random() % count, b = 1 + random() % count;
				if (a != b) { n.cut.insert({a, b}); n.cut.insert({b, a}); }
				if (random() % 2) { const NodeId c = 1 + random() % count; for (NodeId x = 1; x <= count; ++x) { if (x != c) { n.cut.insert({c, x}); n.cut.insert({x, c}); } } }
			} else if (pick < 780) {
				n.cut.clear();
			} else if (pick < 795) {
				if (!n.host(id).pending && !n.host(id).applying) {
					for (const auto& [r, index] : n.host(id).ready) { floors.erase({id, r}); }
					std::erase_if(floors, [&](const auto& f) { return f.first.first == id; });  // the crash abandons its reads
					n.restart(id, ++nonce);
				}
			}
			// Deliver part of the network, in order, dropping a few frames.
			for (std::size_t k = n.network.size() * (random() % 4) / 3; k && !n.network.empty(); --k) {
				auto e = std::move(n.network.front()); n.network.pop_front();
				if (n.blocked(e) || random() % 50 == 0) { continue; }
				n.deliver(std::move(e));
			}
			for (auto& [i, h] : n.hosts) {
				for (const auto& [r, index] : h->ready) {
					auto found = floors.find({i, r});
					if (found == floors.end()) { continue; }
					++ready_seen; forwarded += found->second.second;
					if (index < found->second.first) {
						check(false, "seed " + std::to_string(seed) + ": a read at " + std::to_string(i) + " returned " + std::to_string(index) + " below commit " + std::to_string(found->second.first));
					}
					floors.erase(found);
				}
				for (const auto& [r, reason] : h->rejected) { refused += floors.erase({i, r}); }
			}
		}
		reads += ready_seen;
	}
	std::printf("randomized: %llu seeds, %llu reads completed (%llu at a follower), %llu refused\n", (unsigned long long)seeds, (unsigned long long)reads,
		(unsigned long long)forwarded, (unsigned long long)refused);
	check(reads > seeds && forwarded > seeds, "the randomized clusters complete reads at followers");
}

} // namespace

int main(int argc, char** argv) {
	const std::string only = argc > 1 ? argv[1] : "";
	const std::uint64_t seeds = argc > 2 ? std::stoull(argv[2]) : 200;
	const std::pair<const char*, std::function<void()>> cases[] = {
		{"confirmed", confirmed_after_apply}, {"stale", stale_leader}, {"lost", leader_lost}, {"term", term_change},
		{"identity", answer_identity}, {"restart", restart_same_term}, {"deadline", deadline}, {"in-flight", in_flight},
		{"refusals", refusals}, {"rest", at_rest}, {"nonce", no_nonce}, {"codec", codec}, {"randomized", [&] { randomized(seeds); }},
	};
	bool ran = false;
	for (const auto& [name, run] : cases) {
		if (!only.empty() && only != "all" && only != name) { continue; }
		ran = true;
		const int before = failures;
		run();
		std::printf("%s: %s\n", name, failures == before ? "ok" : "FAILED");
	}
	if (!ran) { std::printf("no case %s\n", only.c_str()); return 2; }
	std::printf("follower_read: %s\n", failures ? "FAILED" : "ok");
	return failures ? 1 : 0;
}
