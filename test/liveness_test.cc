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

// Track C, T3 (liveness), in simulated milliseconds: a discrete-event cluster of Cores
// whose barriers are drawn with p99 = b and whose messages take up to r/2 each way, under
// R4's timing profile for (b, r). Rows: a healed partition elects and commits within 5 s
// at p99 (10 s worst); a partitioned node never raises the healthy group's term; a
// transfer to a reachable voter completes within 500 ms at p99; a leader that cannot
// receive steps down within election_max and a new leader commits within 5 s.
//   liveness_test [seeds]
#include "consensus/core.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <queue>
#include <random>
#include <set>
#include <string>
#include <vector>

using namespace kronuz::consensus;

namespace {

int failures = 0;
void check(bool value, const std::string& what) {
	if (!value && failures++ < 20) { std::fprintf(stderr, "FAIL: %s\n", what.c_str()); }
}

struct Profile {
	const char* name;
	double b, r;  // barrier p99 and round trip, ms
	Timing timing;
};
// R4: RPC timeout >= max(4(r + b), 100); election_min >= 3(2b + r); election_max twice it;
// heartbeat a tenth of election_min.
Profile derive(const char* name, double b, double r) {
	Timing t;
	t.election_min = std::uint64_t(std::ceil(3 * (2 * b + r)));
	t.election_max = 2 * t.election_min;
	t.heartbeat = std::max<std::uint64_t>(1, t.election_min / 10);
	t.rpc_timeout = std::max<std::uint64_t>(100, std::uint64_t(std::ceil(4 * (r + b))));
	return {name, b, r, t};
}

class Cluster {
public:
	// witness: the last voter founds the group as a witness (2 + witness, D9).
	Cluster(std::uint64_t seed, std::size_t voters, const Profile& profile, bool witness = false) : random_(seed), profile_(profile) {
		tick_ = std::max<std::uint64_t>(1, profile.timing.heartbeat / 5);
		for (NodeId id = 1; id <= voters; ++id) {
			FixedConfiguration c; c.cluster[0] = 'L'; c.configuration[0] = 'T'; c.local = id;
			for (NodeId v = 1; v <= voters; ++v) { c.voters.push_back(v); }
			RecoveredState state; state.configuration = c;
			if (witness) {
				Membership m = fixed_membership(c); m.members.back().role = MemberRole::Witness;
				state.membership = m;
			}
			nodes_[id].core = std::make_unique<Core>(c, std::move(state), Limits{}, profile.timing);
			step(id, Tick{0, delay()});
			step(id, Start{});
		}
		for (auto& [id, n] : nodes_) { at(tick_ + random_() % tick_, [this, id] { tick(id); }); }
	}
	std::uint64_t now() const { return now_; }
	std::function<void()> observe;  // called after every event
	// A node joining from a join record (D5): no voters, no membership.
	void add_joiner(NodeId id) {
		FixedConfiguration c; c.cluster[0] = 'L'; c.configuration[0] = 'T'; c.local = id;
		RecoveredState state; state.configuration = c; state.membership = Membership{};
		nodes_[id].core = std::make_unique<Core>(c, std::move(state), Limits{}, profile_.timing);
		step(id, Tick{now_, delay()});
		step(id, Start{});
		at(now_ + tick_, [this, id] { tick(id); });
	}
	// Proposes a membership change on `leader` until it is placed; true once placed.
	bool change(NodeId leader, const Membership& next, std::uint64_t until) {
		while (now_ < until) {
			if (core(leader).role() != Role::Leader) { return false; }
			auto actions = step(leader, ChangeMembership{++request_, next});
			if (!std::any_of(actions.begin(), actions.end(), [](const Action& a) { return std::holds_alternative<Reject>(a); })) { return true; }
			run_until(now_ + tick_);
		}
		return false;
	}
	void run_until(std::uint64_t until, const std::function<bool()>& done = {}) {
		while (!events_.empty() && events_.top().at <= until) {
			auto event = events_.top(); events_.pop();
			now_ = event.at;
			event.run();
			if (observe) { observe(); }
			if (done && done()) { return; }
		}
		now_ = until;
	}
	// Links are directional: cut(a, b) drops what a sends to b.
	void cut(NodeId from, NodeId to) { cut_.insert({from, to}); }
	void heal() { cut_.clear(); }
	void isolate(const std::set<NodeId>& side) {
		for (auto& [a, x] : nodes_) {
			for (auto& [b, y] : nodes_) { if (a != b && side.contains(a) != side.contains(b)) { cut(a, b); } }
		}
	}
	std::vector<NodeId> ids() const { std::vector<NodeId> out; for (auto& [id, n] : nodes_) { out.push_back(id); } return out; }
	Core& core(NodeId id) { return *nodes_.at(id).core; }
	std::optional<NodeId> leader(const std::set<NodeId>& among = {}) {
		std::optional<NodeId> best;
		for (auto& [id, n] : nodes_) {
			if ((among.empty() || among.contains(id)) && n.core->role() == Role::Leader && (!best || n.core->term() > core(*best).term())) { best = id; }
		}
		return best;
	}
	Term max_term(const std::set<NodeId>& among) {
		Term t = 0;
		for (NodeId id : among) { t = std::max(t, core(id).term()); }
		return t;
	}
	// Proposes on the leader among `among` until one commits; true once committed.
	bool commit_one(const std::set<NodeId>& among, std::uint64_t until) {
		std::optional<std::pair<NodeId, RequestId>> asked;  // the leader asked, and the request
		auto placed = [&]() -> std::optional<Index> {
			if (!asked) { return std::nullopt; }
			auto found = placed_.find(asked->second);
			return found == placed_.end() ? std::nullopt : std::optional<Index>(found->second);
		};
		auto committed = [&] { return placed() && core(asked->first).role() == Role::Leader && core(asked->first).committed() >= *placed(); };
		while (now_ < until) {
			if (!asked || core(asked->first).role() != Role::Leader) {
				asked.reset();
				if (auto l = leader(among); l && !core(*l).busy()) {
					auto actions = step(*l, Propose{++request_, "p" + std::to_string(request_)});
					if (!std::any_of(actions.begin(), actions.end(), [](const Action& a) { return std::holds_alternative<Reject>(a); })) { asked = {{*l, request_}}; }
				}
			}
			run_until(std::min(until, now_ + tick_), committed);
			if (committed()) { return true; }
		}
		return false;
	}
	Actions step(NodeId id, Event event) {
		auto actions = nodes_.at(id).core->step(std::move(event));
		consume(id, actions);
		return actions;
	}

private:
	struct Node {
		std::unique_ptr<Core> core;
		std::vector<std::pair<NodeId, Message>> inbox;  // held while the core persists, as a Worker's caller holds them
	};
	struct Scheduled {
		std::uint64_t at, order;
		std::function<void()> run;
		bool operator<(const Scheduled& o) const { return at != o.at ? at > o.at : order > o.order; }
	};
	void at(std::uint64_t when, std::function<void()> run) { events_.push({when, ++order_, std::move(run)}); }
	std::uint64_t delay() {
		const auto& t = profile_.timing;
		return t.election_min + random_() % (t.election_max - t.election_min + 1);
	}
	// 99% of barriers in [b/10, b], 1% in [b, 3b], so p99 is b.
	std::uint64_t barrier() {
		const double b = profile_.b;
		std::uniform_real_distribution<double> body(b / 10, b), tail(b, 3 * b);
		return std::uint64_t(std::ceil(random_() % 100 ? body(random_) : tail(random_)));
	}
	std::uint64_t one_way() {
		std::uniform_real_distribution<double> d(profile_.r / 4, profile_.r / 2);
		return std::max<std::uint64_t>(1, std::uint64_t(std::ceil(d(random_))));
	}
	void tick(NodeId id) {
		step(id, Tick{now_, delay()});
		at(now_ + tick_, [this, id] { tick(id); });
	}
	void consume(NodeId id, Actions& actions) {
		for (auto& action : actions) {
			if (auto persist = std::get_if<Persist>(&action)) {
				const Token token = persist->token;
				at(now_ + barrier(), [this, id, token] {
					step(id, Tick{now_, delay()}); step(id, Persisted{token});
					auto& n = nodes_.at(id);
					while (!n.inbox.empty() && !n.core->busy()) {
						auto [from, message] = std::move(n.inbox.front()); n.inbox.erase(n.inbox.begin());
						step(id, Receive{from, std::move(message)});
					}
				});
			} else if (auto send = std::get_if<Send>(&action)) {
				const NodeId to = send->peer;
				if (cut_.contains({id, to})) { continue; }
				auto message = send->message;
				at(now_ + one_way(), [this, id, to, message] {
					if (cut_.contains({id, to})) { return; }
					step(to, Tick{now_, delay()});
					auto& n = nodes_.at(to);
					if (n.core->busy() || !n.inbox.empty()) { n.inbox.emplace_back(id, message); return; }
					step(to, Receive{id, message});
				});
			} else if (auto p = std::get_if<ProposalPlaced>(&action)) {
				placed_[p->request] = p->index;
			} else if (auto committed = std::get_if<Committed>(&action)) {
				const Index through = committed->entries.back().index;
				at(now_, [this, id, through] { step(id, Applied{through}); });
			} else if (std::holds_alternative<Fenced>(action)) {
				check(false, "a node fenced");
			}
		}
	}

	std::mt19937_64 random_;
	Profile profile_;
	std::map<NodeId, Node> nodes_;
	std::priority_queue<Scheduled> events_;
	std::set<std::pair<NodeId, NodeId>> cut_;
	std::map<RequestId, Index> placed_;
	std::uint64_t now_ = 0, order_ = 0, tick_ = 1;
	RequestId request_ = 0;
};

double percentile(std::vector<double> v, double p) {
	if (v.empty()) { return 0; }
	std::sort(v.begin(), v.end());
	return v[std::min(v.size() - 1, std::size_t(p * double(v.size() - 1) + 0.5))];
}

void run(const Profile& profile, std::uint64_t seeds) {
	std::vector<double> heal, transfer, stepdown, recover, handoff, survive, promotion, replacement;
	std::uint64_t unpromoted = 0, unreplaced = 0, removed_raised = 0, removed_runs = 0;
	std::uint64_t unsurvived = 0, unhanded = 0, raised = 0, lone_raised = 0, unhealed = 0, untransferred = 0, unrecovered = 0;
	const auto& t = profile.timing;
	for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
		std::mt19937_64 pick(seed * 7919);
		const std::size_t voters = seed % 2 ? 3 : 5;
		std::set<NodeId> all;
		for (NodeId id = 1; id <= voters; ++id) { all.insert(id); }

		// A healed partition elects and commits.
		{
			Cluster c(seed, voters, profile);
			c.commit_one(all, 20000);
			std::set<NodeId> side;
			const auto size = 1 + pick() % (voters / 2);
			auto l = c.leader();
			if (l && pick() % 2) { side.insert(*l); }
			while (side.size() < size) { side.insert(1 + NodeId(pick() % voters)); }
			c.isolate(side);
			c.run_until(c.now() + 2000 + pick() % 8000);
			c.heal();
			const auto healed = c.now();
			if (c.commit_one(all, healed + 30000)) { heal.push_back(double(c.now() - healed)); } else { ++unhealed; }
		}
		// A partitioned follower never raises the healthy group's term.
		{
			Cluster c(seed + 1000000, voters, profile);
			c.commit_one(all, 20000);
			auto l = c.leader();
			if (!l) { ++raised; continue; }
			NodeId lone = *l;
			while (lone == *l) { lone = 1 + NodeId(pick() % voters); }
			std::set<NodeId> healthy = all; healthy.erase(lone);
			const Term before = c.max_term(healthy);
			c.isolate({lone});
			c.run_until(c.now() + 20000);
			c.heal();
			c.run_until(c.now() + 5000);
			if (c.max_term(healthy) != before) { ++raised; if (std::getenv("LIVENESS_DEBUG")) { std::fprintf(stderr, "%s raised: seed %llu voters %zu lone %llu term %llu -> %llu\n", profile.name, (unsigned long long)seed, voters, (unsigned long long)lone, (unsigned long long)before, (unsigned long long)c.max_term(healthy)); } }
			if (c.core(lone).term() != before) { ++lone_raised; }
		}
		// A transfer to a reachable voter completes.
		{
			Cluster c(seed + 2000000, voters, profile);
			c.commit_one(all, 20000);
			auto l = c.leader();
			NodeId target = *l;
			while (target == *l) { target = 1 + NodeId(pick() % voters); }
			const auto started = c.now();
			// The host retries a transfer refused while the leader persists.
			for (RequestId r = 1; c.now() < started + 10000; ++r) {
				auto actions = c.step(*l, TransferLeadership{r, target});
				if (!std::any_of(actions.begin(), actions.end(), [](const Action& a) { return std::holds_alternative<Reject>(a); })) { break; }
				c.run_until(c.now() + 1);
			}
			c.run_until(started + 10000, [&] { return c.core(target).role() == Role::Leader; });
			if (c.core(target).role() == Role::Leader) { transfer.push_back(double(c.now() - started)); } else {
				++untransferred;
				if (std::getenv("LIVENESS_DEBUG")) { std::fprintf(stderr, "%s transfer failed: seed %llu voters %zu leader %llu target %llu, leader now %llu term %llu\n", profile.name, (unsigned long long)seed, voters, (unsigned long long)*l, (unsigned long long)target, (unsigned long long)c.leader().value_or(0), (unsigned long long)c.core(target).term()); }
			}
		}
		// T4: a 2 + witness group survives any single failure, and a witness that leads hands
		// off to a data voter within election_max.
		{
			const std::set<NodeId> three{1, 2, 3};
			for (NodeId failed : {NodeId(1), NodeId(2), NodeId(3)}) {
				Cluster c(seed + 4000000 + failed, 3, profile, true);
				c.commit_one(three, 20000);
				std::optional<std::uint64_t> led;
				c.observe = [&] {
					if (!led && c.core(3).role() == Role::Leader) { led = c.now(); }
					if (led && c.core(3).role() != Role::Leader) { handoff.push_back(double(c.now() - *led)); led.reset(); }
				};
				c.isolate({failed});
				const auto started = c.now();
				std::set<NodeId> rest = three; rest.erase(failed);
				// The witness refuses proposals, so a commit means a data voter leads.
				if (c.commit_one(rest, started + 30000)) { survive.push_back(double(c.now() - started)); } else { ++unsurvived; }
				c.run_until(c.now() + 5000);
				if (led) { ++unhanded; }
			}
		}
		// Condition 13: the leader fails in a promotion's window, and in a replacement's (after
		// the promotion commits, before the leaving voter's removal does).
		for (bool replace : {false, true}) {
			const std::set<NodeId> three{1, 2, 3};
			Cluster c(seed + 5000000 + replace, 3, profile);
			c.commit_one(three, 20000);
			auto l = c.leader(three);
			if (!l) { ++(replace ? unreplaced : unpromoted); continue; }
			c.add_joiner(4);
			Membership with4 = c.core(*l).membership(); with4.members.push_back({4, 4, MemberRole::Learner}); with4.next_incarnation = 5;
			c.change(*l, with4, c.now() + 10000);
			c.run_until(c.now() + 20000, [&] { auto p = c.core(*l).progress(4); return c.core(*l).can_change_membership() && p && *p >= c.core(*l).committed(); });
			Membership promoted = c.core(*l).membership();
			for (auto& x : promoted.members) { if (x.incarnation == 4) { x.role = MemberRole::Voter; } }
			NodeId leaving = 0;
			if (replace) { for (NodeId id : three) { if (id != *l) { leaving = id; break; } } promoted.leaving = leaving; }
			if (!c.change(*l, promoted, c.now() + 10000)) { ++(replace ? unreplaced : unpromoted); continue; }
			if (replace) { c.run_until(c.now() + 20000, [&] { return c.core(*l).committed_membership().leaving != 0; }); }
			for (NodeId id = 1; id <= 4; ++id) { if (id != *l) { c.cut(*l, id); c.cut(id, *l); } }
			const auto failed = c.now();
			std::set<NodeId> rest{1, 2, 3, 4}; rest.erase(*l);
			if (c.commit_one(rest, failed + 30000)) { (replace ? replacement : promotion).push_back(double(c.now() - failed)); } else { ++(replace ? unreplaced : unpromoted); }
		}
		// Condition 13: a removed voter, cut off before it learns of its removal, never raises
		// the remaining group's term once it returns.
		{
			const std::set<NodeId> three{1, 2, 3};
			Cluster c(seed + 6000000, 3, profile);
			c.commit_one(three, 20000);
			auto l = c.leader(three);
			if (l) {
				NodeId gone = *l == 3 ? 2 : 3;
				c.cut(*l, gone); c.cut(gone, *l);
				for (NodeId id : three) { if (id != *l && id != gone) { c.cut(id, gone); c.cut(gone, id); } }
				Membership without = c.core(*l).membership();
				std::erase_if(without.members, [&](const Member& x) { return x.incarnation == gone; });
				std::set<NodeId> kept = three; kept.erase(gone);
				if (c.change(*l, without, c.now() + 10000) && c.commit_one(kept, c.now() + 20000)) {
					++removed_runs;
					const Term before = c.max_term(kept);
					c.heal();
					c.run_until(c.now() + 20000);
					removed_raised += c.max_term(kept) != before;
				}
			}
		}
		// A leader that cannot receive steps down, and a new leader commits.
		{
			Cluster c(seed + 3000000, voters, profile);
			c.commit_one(all, 20000);
			auto l = c.leader();
			const NodeId deaf = *l;
			for (NodeId id : all) { if (id != deaf) { c.cut(id, deaf); } }
			const auto started = c.now();
			c.run_until(started + 20000, [&] { return c.core(deaf).role() != Role::Leader; });
			stepdown.push_back(double(c.now() - started));
			std::set<NodeId> rest = all; rest.erase(deaf);
			if (c.commit_one(rest, started + 30000)) { recover.push_back(double(c.now() - started)); } else { ++unrecovered; }
		}
	}
	const auto heal99 = percentile(heal, 0.99), heal_max = percentile(heal, 1.0);
	const auto transfer99 = percentile(transfer, 0.99), down_max = percentile(stepdown, 1.0), recover99 = percentile(recover, 0.99);
	std::printf("%-26s b=%3.0f r=%2.0f hb=%4llu rpc=%4llu elect=%llu-%llu  heal p99 %6.0f max %6.0f  transfer p99 %5.0f  step-down max %5.0f  recover p99 %6.0f  term raised %llu\n",
		profile.name, profile.b, profile.r, (unsigned long long)t.heartbeat, (unsigned long long)t.rpc_timeout,
		(unsigned long long)t.election_min, (unsigned long long)t.election_max, heal99, heal_max, transfer99, down_max, recover99, (unsigned long long)raised);
	std::printf("%26s samples: heal %zu (%llu failed), transfer %zu (%llu failed), recover %zu (%llu failed), isolated node's own term raised %llu\n", "", heal.size(), (unsigned long long)unhealed,
		transfer.size(), (unsigned long long)untransferred, recover.size(), (unsigned long long)unrecovered, (unsigned long long)lone_raised);
	check(!unhealed && heal99 <= 5000 && heal_max <= 10000, std::string(profile.name) + ": a healed partition commits within 5 s at p99, 10 s worst");
	check(!raised, std::string(profile.name) + ": a partitioned node never raises the healthy group's term");
	check(!untransferred && transfer99 <= 500, std::string(profile.name) + ": a transfer completes within 500 ms at p99");
	check(down_max <= double(t.election_max) + double(t.heartbeat), std::string(profile.name) + ": a deaf leader steps down within election_max");
	check(!unrecovered && recover99 <= 5000, std::string(profile.name) + ": a new leader commits within 5 s of a deaf leader");
	const auto survive99 = percentile(survive, 0.99), handoff_max = percentile(handoff, 1.0);
	std::printf("%26s 2 + witness: single failure recovered p99 %6.0f (%zu, %llu failed); witness hand-offs %zu, max %5.0f, unfinished %llu\n", "",
		survive99, survive.size(), (unsigned long long)unsurvived, handoff.size(), handoff_max, (unsigned long long)unhanded);
	check(!unsurvived && survive99 <= 5000, std::string(profile.name) + ": 2 + witness survives any single failure");
	check(!unhanded && handoff_max <= double(t.election_max), std::string(profile.name) + ": a witness leader hands off within election_max");
	const auto promotion99 = percentile(promotion, 0.99), replacement99 = percentile(replacement, 0.99);
	std::printf("%26s membership: leader fails in a promotion's window, new leader commits p99 %6.0f (%zu, %llu failed); in a replacement's window p99 %6.0f (%zu, %llu failed); removed voter raised the term %llu of %llu\n", "",
		promotion99, promotion.size(), (unsigned long long)unpromoted, replacement99, replacement.size(), (unsigned long long)unreplaced, (unsigned long long)removed_raised, (unsigned long long)removed_runs);
	check(!unpromoted && promotion99 <= 5000, std::string(profile.name) + ": a leader failing in a promotion's window is replaced within 5 s");
	check(!unreplaced && replacement99 <= 5000, std::string(profile.name) + ": a leader failing in a replacement's window is replaced within 5 s");
	check(!removed_raised && removed_runs, std::string(profile.name) + ": a removed voter never raises the group's term");
}

}  // namespace

int main(int argc, char** argv) {
	const std::uint64_t seeds = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 100;
	// The default profile (R4: Detent's authority profile, macOS x86_64 APFS at b = 80 ms),
	// and profiles derived from barrier p99 estimates per platform (docs/perf/commit-barriers.md
	// in strata; the journal's own p99 per platform is still to be measured).
	Timing detent; detent.heartbeat = 100; detent.rpc_timeout = 500; detent.election_min = 1000; detent.election_max = 2000;
	const Profile profiles[] = {
		{"default (macOS x86_64)", 80, 2, detent},
		derive("macOS arm64 APFS", 20, 2),
		derive("Linux ext4", 30, 2),
		derive("FreeBSD UFS", 15, 2),
	};
	for (const auto& p : profiles) { run(p, seeds); }
	std::printf("%d failures\n", failures);
	return failures ? 1 : 0;
}
