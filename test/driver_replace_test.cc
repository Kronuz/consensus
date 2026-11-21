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

// A replacement through the Driver (Track C, D3): three founding members on their own journal
// hosts, a joiner on a fourth, each one loop on an inline_scheduler with its IO held and
// completed in random order. The leader adds the joiner as a learner, promotes it beside a
// leaving member (any voting one, the leader included; a witness's place goes to a witness),
// and the core removes the leaving member by itself. Messages are delayed, reordered, dropped
// and duplicated; leadership moves now and then; a client write is outstanding throughout.
// The oracles, per seed:
//   - no group fences (a Persist the Worker did not fund fences it);
//   - one leader per term, and every node applies a prefix of one sequence of entries;
//   - the removal commits: every data member of the final membership applies it, and a
//     command proposed after it commits there too;
//   - after settling, every member holds the replacement's membership by value, exactly one
//     removal (of three configurations) committed, the removed member does not lead, and
//     every committed client write holds its value on every data member.
// `driver_replace_test [seeds] [first]`; SIM_REPLACE_TRACE=1 logs each node's notices.
#include "consensus/driver.h"
#include "faultlab/faultfs.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace fl = kronuz::faultlab;

namespace {

constexpr std::uint64_t ms = 1'000'000;
constexpr GroupId group = 7;
constexpr std::size_t step_bound = 40000;
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{8u << 20, 4096}, {64 * 1024, 3}, {4u << 20, 4}, 16, 3};
constexpr Timing timing{20, 40, 100, 200, 60000};
Limits limits() {
	Limits l;
	l.command_bytes = 1024; l.rpc_bytes = 4096; l.log_bytes = 1 << 20; l.log_entries = 4096; l.control_entries = 4; l.rpc_entries = 8;
	return l;
}

std::uint64_t failures = 0, current_seed = 0;
void fail(const std::string& what) {
	if (failures++ < 20) { std::fprintf(stderr, "replace seed %llu: %s\n", (unsigned long long)current_seed, what.c_str()); }
}
const bool tracing = std::getenv("SIM_REPLACE_TRACE") != nullptr;
std::uint64_t writes_committed = 0, elections_mid_removal = 0;

std::shared_ptr<j::IO> host_io(fl::filesystem& disk) {
	const auto names = disk.list();
	if (std::find(names.begin(), names.end(), "host") == names.end()) { disk.make_directory("host"); }
	return std::make_shared<fl::io>(disk, "host");
}

struct World;
struct NodeSink {
	World* world = nullptr;
	NodeId node = 0;
	void operator()(GroupId, Send notice);
	void operator()(GroupId, Committed notice);
	void operator()(GroupId, RoleChanged notice);
	void operator()(GroupId, Fenced notice);
	void operator()(GroupId, ActivateInstall) {}
	void operator()(GroupId, CheckpointPublished) {}
	void operator()(GroupId, RepairCompleted) {}
	void operator()(GroupId, SnapshotNeeded) {}
	void operator()(GroupId, SnapshotTransmit) {}
	void operator()(GroupId, SnapshotReleased) {}
};
static_assert(sink<NodeSink>);
using ReplaceDriver = Driver<NodeSink>;

// One node: a journal host of one loop, its Driver, and the application's copy of the group.
struct Node {
	NodeId id = 0;
	fl::filesystem disk;
	std::unique_ptr<j::Host> host;
	j::HeldCompletion held;
	task::inline_scheduler scheduler;
	NodeSink sink;
	std::unique_ptr<ReplaceDriver> driver;
	task::cancel_source stop;
	bool turns_done = false, driver_done = false;
	std::vector<Entry> applied;
	std::optional<Index> applying;
	task::scheduler_ref ref() { return task::scheduler_ref::of(scheduler); }
	void run() {
		std::size_t budget = 1'000'000;
		scheduler.run_until([&] { return --budget == 0; });
		if (!budget) { std::fprintf(stderr, "replace seed %llu: node %llu's loop spins\n", (unsigned long long)current_seed, (unsigned long long)id); std::exit(1); }
	}
	std::optional<ReplaceDriver::GroupState> state() const { return driver->state(group); }
};

// A call the test awaits: how it completed.
struct Call { bool done = false; std::exception_ptr error; std::optional<Placement> placed; };

struct Envelope { NodeId from, to; Message message; std::uint64_t at; };

struct World {
	explicit World(std::uint64_t seed) : random_(seed) {
		// Odd seeds found the group with a witness: its replacement is a witness too, or a data
		// voter's is a voter, beside the witness.
		witness_ = seed % 2;
		for (NodeId id = 1; id <= 4; ++id) { boot(id); }
	}
	~World() { for (auto& [id, n] : nodes_) { shut(*n); } }

	// Runs the replacement to its end; true once every oracle holds.
	bool run() {
		for (step_ = 0; step_ < step_bound && !failures; ++step_) {
			advance(1 + random_() % 4);
			const auto pick = random_() % 1000;
			if (pick < 400) { for (int k = 0; k < 4; ++k) { deliver_one(); } }
			else if (pick < 750) { auto& n = node(1 + random_() % 4); complete_io(n); }
			else if (pick < 900) { for (auto& [id, n] : nodes_) { apply(*n); } }
			else if (pick < 902 && phase_ >= promoted) { transfer(); }
			write();
			drive();
			if (phase_ == done) { break; }
		}
		if (failures) { return false; }
		for (const auto& call : opens_) { if (call->error) { fail("a node's create failed"); } }
		if (phase_ != done) { report("the replacement did not complete in " + std::to_string(step_bound) + " steps"); return false; }
		settle_and_check();
		return !failures;
	}
	std::uint64_t leader_changes = 0, transfers = 0, changes = 0;
	bool leader_left = false;  // the leaving member led when its replacement was proposed
	bool witness_left() const { return witness_ && leaving_ == 3; }

	void on_send(NodeId from, Send notice) {
		network_.push_back({from, notice.peer, notice.message, now_ + random_() % 6});
		if (random_() % 50 == 0) { network_.push_back({from, notice.peer, network_.back().message, now_ + random_() % 30}); }  // duplicated
	}
	void on_committed(NodeId id, Committed notice) {
		auto& n = node(id);
		if (tracing) { std::fprintf(stderr, "[%zu] node %llu Committed %llu..%zu\n", step_, (unsigned long long)id, (unsigned long long)notice.first, notice.entries.size()); }
		if (notice.first != n.applied.size() + 1) { fail("node " + std::to_string(id) + " received a range from " + std::to_string(notice.first) + " after applying " + std::to_string(n.applied.size())); }
		for (auto& entry : notice.entries) {
			auto [it, fresh] = sequence_.emplace(entry.index, entry);
			if (!fresh && it->second != entry) { fail("state machine safety: index " + std::to_string(entry.index) + " differs between nodes"); }
			n.applied.push_back(std::move(entry));
		}
		n.applying = n.applied.empty() ? 0 : n.applied.back().index;
	}
	void on_role(NodeId id, RoleChanged notice) {
		if (notice.role == Role::Leader && phase_ == promoted) { ++elections_mid_removal; }
		if (tracing) { std::fprintf(stderr, "[%zu] node %llu role %d term %llu\n", step_, (unsigned long long)id, int(notice.role), (unsigned long long)notice.term); }
		if (notice.role != Role::Leader) { return; }
		++leader_changes;
		auto [it, fresh] = leaders_.emplace(notice.term, id);
		if (!fresh && it->second != id) { fail("two leaders in term " + std::to_string(notice.term)); }
	}
	void on_fenced(NodeId id, const Fenced& notice) { fail("node " + std::to_string(id) + " fenced: " + notice.reason); }

private:
	// One outstanding client write on the leader at a time, through the replacement.
	void write() {
		if (write_ && !write_->done) { return; }
		if (write_ && write_->placed) { placed_.push_back({*write_->placed, write_payload_}); }
		write_.reset();
		if (phase_ == done) { return; }
		auto* l = leader(); if (!l) { return; }
		write_payload_ = "w" + std::to_string(writes_issued_++);
		start(*l, write_ = std::make_shared<Call>(), l->driver->propose(group, write_payload_));
		l->run();
	}
	// Runs on with no new changes, then checks the final state by value.
	void settle_and_check() {
		for (std::size_t k = 0; k < 3000 && !failures; ++k) {
			advance(1 + random_() % 4);
			for (int d = 0; d < 4; ++d) { deliver_one(); }
			complete_io(node(1 + random_() % 4));
			for (auto& [id, n] : nodes_) { apply(*n); }
			write();
		}
		if (failures) { return; }
		// The final membership: the founding one without the leaving member, the joiner in its role.
		std::set<std::pair<NodeId, int>> expected, got;
		const MemberRole leaving_role = witness_ && leaving_ == 3 ? MemberRole::Witness : MemberRole::Voter;
		for (NodeId id = 1; id <= 3; ++id) { if (id != leaving_) { expected.insert({id, int(witness_ && id == 3 ? MemberRole::Witness : MemberRole::Voter)}); } }
		expected.insert({4, int(leaving_role)});
		for (const auto& x : removal_.members) { got.insert({x.incarnation, int(x.role)}); }
		if (got != expected || removal_.leaving != 0) { report("the final membership is not the replacement's"); return; }
		// Every member node holds exactly that membership in force and committed through it.
		for (const auto& x : removal_.members) {
			auto s = node(x.incarnation).state();
			if (!s || !(s->membership == removal_)) { report("member " + std::to_string(x.incarnation) + " holds another membership"); return; }
		}
		// The removal committed once: one Configuration entry drops the leaving member.
		std::size_t configs = 0, removals = 0;
		for (const auto& [index, e] : sequence_) {
			if (e.kind != EntryKind::Configuration) { continue; }
			++configs; auto m = decode_membership(e.payload);
			removals += !find_member(m, leaving_) && find_member(m, 4) != nullptr;
		}
		if (removals != 1) { report("" + std::to_string(removals) + " committed removals"); return; }
		if (configs != 3) { report("" + std::to_string(configs) + " committed configurations, expected add, promote and remove"); return; }
		// The removed member leads no term the group follows.
		if (auto* l = leader(); !l || l->id == leaving_) { report(l ? "the removed member leads" : "no leader after settling"); return; }
		// Every write whose placement committed holds its payload, on every data member.
		Index last = 0;
		for (const auto& [p, payload] : placed_) {
			auto took = sequence_.find(p.index);
			if (took == sequence_.end() || took->second.term != p.term) { continue; }
			if (took->second.payload != payload) { report("write " + payload + " at " + std::to_string(p.index) + " holds " + took->second.payload); return; }
			++writes_committed; last = std::max(last, p.index);
		}
		for (auto* n : data_members()) {
			if (n->applied.size() < last) { report("node " + std::to_string(n->id) + " did not apply through write index " + std::to_string(last)); return; }
		}
	}
	std::shared_ptr<Call> write_;
	std::string write_payload_;
	std::uint64_t writes_issued_ = 0;
	std::vector<std::pair<Placement, std::string>> placed_;
	enum Phase { adding, promoting, promoted, removed, done };

	Node& node(NodeId id) { return *nodes_.at(id); }
	void boot(NodeId id) {
		auto& n = *nodes_.emplace(id, std::make_unique<Node>()).first->second;
		n.id = id; n.sink = NodeSink{this, id};
		n.host = std::make_unique<j::Host>(host_io(n.disk), j::Identity{'N', static_cast<char>('0' + id)}, j::HostOptions{1, turn_options});
		n.driver = std::make_unique<ReplaceDriver>(Seams{n.ref(), j::completion_ref::of(n.held), *n.host, 0, current_seed * 31 + id}, n.sink);
		task::spawn(n.ref(), n.host->driver(0).run(j::completion_ref::of(n.held), n.stop.token()), [&n](std::exception_ptr) { n.turns_done = true; });
		task::spawn(n.ref(), n.driver->run(n.stop.token()), [&n](std::exception_ptr) { n.driver_done = true; });
		GroupOptions o;
		o.configuration.local = id;
		if (id <= 3) { o.configuration.voters = {1, 2, 3}; }  // node 4 joins
		o.configuration.cluster[0] = 'R'; o.configuration.configuration[0] = 'P';
		if (witness_ && id <= 3) { o.founding = Membership{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 4, 0}; }
		o.admission = admission; o.limits = limits(); o.timing = timing; o.scheduling = WorkerLimits{16, 128};
		n.driver->open(group, std::move(o));
		start(n, opens_.emplace_back(std::make_shared<Call>()), n.driver->create(group, j::Identity{'G', static_cast<char>('0' + id)}));
		n.run();
	}
	void shut(Node& n) {
		n.stop.request(task::cancel_reason::shutdown);
		for (int round = 0; round < 10000 && !(n.turns_done && n.driver_done && !n.held.held()); ++round) {
			n.run();
			for (auto ticket : n.held.tickets()) { n.held.complete(ticket); n.run(); }
		}
		n.driver.reset(); n.host.reset();
	}

	template <class T> void start(Node& n, std::shared_ptr<Call> call, task::task<T> work) {
		task::spawn(n.ref(), std::move(work), [call](std::exception_ptr error, std::optional<T> value) {
			call->done = true; call->error = error;
			if constexpr (std::is_same_v<T, Placement>) { call->placed = value; }
		});
	}
	void start(Node& n, std::shared_ptr<Call> call, task::task<void> work) {
		task::spawn(n.ref(), std::move(work), [call](std::exception_ptr error) { call->done = true; call->error = error; });
	}

	void advance(std::uint64_t dt) {
		now_ += dt;
		for (auto& [id, n] : nodes_) { n->scheduler.advance(dt * ms); n->run(); }
	}
	void deliver_one() {
		if (network_.empty()) { return; }
		auto pick = random_() % std::min<std::size_t>(network_.size(), 4);  // reordered among the first few
		auto e = std::move(network_[pick]);
		network_.erase(network_.begin() + std::ptrdiff_t(pick));
		if (e.at > now_ && random_() % 4) { network_.push_back(std::move(e)); return; }
		if (random_() % 25 == 0) { return; }  // dropped
		auto& to = node(e.to);
		to.driver->receive(group, Receive{e.from, std::move(e.message)});
		to.run();
	}
	// Completes some of the node's IO in random order.
	void complete_io(Node& n) {
		for (int k = 0; n.held.held() && k < 1 + int(random_() % 3); ++k) {
			auto tickets = n.held.tickets();
			n.held.complete(tickets[random_() % tickets.size()]);
			n.run();
		}
	}
	void apply(Node& n) {
		if (!n.applying) { return; }
		n.driver->applied(group, Applied{*n.applying});
		n.applying.reset();
		n.run();
	}

	// The node that leads the highest term, if any.
	Node* leader() {
		Node* best = nullptr; Term term = 0;
		for (auto& [id, n] : nodes_) {
			auto s = n->state();
			if (s && s->role == Role::Leader && s->term >= term) { best = n.get(); term = s->term; }
		}
		return best;
	}
	// Moves leadership to another voting member of the membership in force.
	void transfer() {
		auto* l = leader();
		if (!l || (transfer_ && !transfer_->done)) { return; }
		auto s = l->state();
		std::vector<NodeId> targets;
		for (const auto& m : s->membership.members) { if (m.role == MemberRole::Voter && m.incarnation != l->id) { targets.push_back(m.incarnation); } }
		if (targets.empty()) { return; }
		++transfers;
		start(*l, transfer_ = std::make_shared<Call>(), l->driver->transfer_leadership(group, targets[random_() % targets.size()]));
		l->run();
	}
	// Issues the next change on the leader when no change is in flight, and judges the phase by
	// the leader's membership in force, so a change a deposed leader lost is issued again.
	void drive() {
		if (change_ && !change_->done) { return; }
		auto* l = leader();
		if (!l) { return; }
		auto s = l->state();
		const auto& m = s->membership;
		const auto* joiner = find_member(m, 4);
		auto issue = [&](Membership next) {
			if (change_ && step_ - issued_ < 25) { return; }  // a refused change retries a little later
			++changes; issued_ = step_;
			start(*l, change_ = std::make_shared<Call>(), l->driver->change_membership(group, std::move(next)));
			l->run();
		};
		if (phase_ < promoted && !joiner && s->committed >= 1) {
			Membership next = m; next.members.push_back({4, 4, MemberRole::Learner}); next.next_incarnation = 5;
			issue(next);
		} else if (joiner && joiner->role == MemberRole::Learner) {
			phase_ = promoting;
			if (!leaving_) {
				// Any voting member leaves, the leader included; a witness's place goes to a witness.
				std::vector<NodeId> voting;
				for (const auto& x : m.members) { if (x.incarnation != 4 && kronuz::consensus::voting(x.role)) { voting.push_back(x.incarnation); } }
				leaving_ = voting[random_() % voting.size()]; leader_left = leaving_ == l->id;
			}
			Membership next = m;
			const auto leaving_role = find_member(m, leaving_)->role;
			for (auto& x : next.members) { if (x.incarnation == 4) { x.role = leaving_role; } }
			next.leaving = leaving_;
			issue(next);
		} else if (joiner && m.leaving) {
			phase_ = promoted;
		} else if (joiner && leaving_ && !find_member(m, leaving_)) {
			if (phase_ < removed) { phase_ = removed; removal_ = m; }
			if (removed_everywhere() && !probe_) { start(*l, probe_ = std::make_shared<Call>(), l->driver->propose(group, "after")); l->run(); }
			if (probe_ && probe_->done && (probe_->error || !probe_->placed || superseded(*probe_->placed, l))) { probe_.reset(); }  // proposes again
			if (probe_ && probe_->placed && applied_everywhere(*probe_->placed)) { phase_ = done; }
		}
	}
	// Another term's entry took the placement, or a later term leads (a deposed leader placed it).
	bool superseded(const Placement& p, Node* l) {
		auto took = sequence_.find(p.index);
		return (took != sequence_.end() && took->second.term != p.term) || (!applied_everywhere(p) && l->state()->term > p.term);
	}
	// The data members of the final membership: the ones that apply.
	std::vector<Node*> data_members() {
		std::vector<Node*> out;
		for (const auto& x : removal_.members) { if (x.role == MemberRole::Voter) { out.push_back(&node(x.incarnation)); } }
		return out;
	}
	bool removed_everywhere() {
		for (auto* n : data_members()) {
			bool seen = false;
			for (const auto& e : n->applied) { seen |= e.kind == EntryKind::Configuration && decode_membership(e.payload) == removal_; }
			if (!seen) { return false; }
		}
		return true;
	}
	bool applied_everywhere(const Placement& p) {
		for (auto* n : data_members()) {
			if (n->applied.size() < p.index || n->applied[p.index - 1].term != p.term || n->applied[p.index - 1].payload != "after") { return false; }
		}
		return true;
	}
	void report(const std::string& what) {
		fail(what + " (phase " + std::to_string(int(phase_)) + ", leaving " + std::to_string(leaving_) + (witness_ ? ", witness group" : "") + ")");
		for (auto& [id, n] : nodes_) {
			auto s = n->state();
			std::string members;
			if (s) { for (const auto& x : s->membership.members) { members += " " + std::to_string(x.incarnation) + ":" + std::to_string(int(x.role)); } }
			std::fprintf(stderr, "  node %llu role %d term %llu committed %llu applied %llu, members%s leaving %llu, held %zu\n", (unsigned long long)id,
				s ? int(s->role) : -1, s ? (unsigned long long)s->term : 0ull, s ? (unsigned long long)s->committed : 0ull, s ? (unsigned long long)s->applied : 0ull,
				members.c_str(), s ? (unsigned long long)s->membership.leaving : 0ull, n->held.held());
		}
	}

	std::mt19937_64 random_;
	bool witness_ = false;
	std::map<NodeId, std::unique_ptr<Node>> nodes_;
	std::deque<Envelope> network_;
	std::map<Index, Entry> sequence_;
	std::map<Term, NodeId> leaders_;
	std::vector<std::shared_ptr<Call>> opens_;
	std::shared_ptr<Call> change_, transfer_, probe_;
	Phase phase_ = adding;
	NodeId leaving_ = 0;
	Membership removal_;
	std::uint64_t now_ = 0;
	std::size_t step_ = 0, issued_ = 0;
};

void NodeSink::operator()(GroupId, Send notice) { world->on_send(node, std::move(notice)); }
void NodeSink::operator()(GroupId, Committed notice) { world->on_committed(node, std::move(notice)); }
void NodeSink::operator()(GroupId, RoleChanged notice) { world->on_role(node, notice); }
void NodeSink::operator()(GroupId, Fenced notice) { world->on_fenced(node, notice); }

}  // namespace

int main(int argc, char** argv) {
	const std::uint64_t seeds = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 100;
	const std::uint64_t first = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 1;
	std::uint64_t completed = 0, leader_left = 0, witness_left = 0, leader_changes = 0, transfers = 0, changes = 0;
	for (std::uint64_t seed = first; seed < first + seeds && !failures; ++seed) {
		current_seed = seed;
		World world(seed);
		if (!world.run()) { break; }
		++completed;
		leader_changes += world.leader_changes; transfers += world.transfers; changes += world.changes;
		witness_left += world.witness_left(); leader_left += world.leader_left;
	}
	std::printf("  %llu committed client writes checked, %llu elections between promotion and removal\n", (unsigned long long)writes_committed, (unsigned long long)elections_mid_removal);
	std::printf("driver replacement: %llu of %llu seeds completed (%llu replaced their leader, %llu a witness), %llu elections, %llu transfers, %llu changes issued: %s\n",
		(unsigned long long)completed, (unsigned long long)seeds, (unsigned long long)leader_left, (unsigned long long)witness_left, (unsigned long long)leader_changes,
		(unsigned long long)transfers, (unsigned long long)changes, failures ? "FAIL" : "ok");
	return failures ? 1 : 0;
}
