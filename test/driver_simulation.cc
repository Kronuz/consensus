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
// host on memory disks, a Driver on its own inline_scheduler, and every IO primitive held in a
// HeldCompletion the simulation completes in random order. Messages are delayed, dropped,
// duplicated and partitioned; nodes lose power with IO in flight and recover; callers propose,
// await commits, read, transfer leadership, and cancel. The oracles:
//   - one leader per term (RoleChanged);
//   - every node applies a prefix of one sequence of entries;
//   - each call completes once, per its D8 row: propose's Placement holds its command once
//     that index commits in that term; committed() succeeds only if the entry at the placement
//     committed with its term, and is superseded only if another term's did; a read's index
//     covers every entry applied anywhere before it was issued; a cancelled call never yields
//     a value; every call completes by shutdown;
//   - a witness member votes but never applies: its Sink never receives a Committed range.
// After the run the network heals and a new entry commits on every data voter.
#include "consensus/driver.h"
#include "journal/memory_io.h"

#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <map>
#include <random>
#include <set>
#include <string>
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

constexpr GroupId group = 7;
constexpr std::uint64_t ms = 1'000'000;
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{4u << 20, 4096}, {64 * 1024, 3}, {256 * 1024, 4}, 16, 3};
constexpr Timing timing{20, 40, 100, 200, 60000};

Limits limits() {
	Limits l;
	l.command_bytes = 1024; l.rpc_bytes = 4096; l.log_bytes = 1 << 20; l.log_entries = 4096; l.control_entries = 4; l.rpc_entries = 8;
	return l;
}

struct Simulation;
struct Node;

// A node's Sink: hands every notice to the simulation's oracles and network.
struct NodeSink {
	Simulation* sim;
	Node* node;
	void operator()(GroupId, Send notice);
	void operator()(GroupId, Committed notice);
	void operator()(GroupId, RoleChanged notice);
	void operator()(GroupId, ActivateInstall notice);
	void operator()(GroupId, CheckpointPublished) {}
	void operator()(GroupId, RepairCompleted) {}
	void operator()(GroupId, SnapshotNeeded) {}
	void operator()(GroupId, SnapshotTransmit) {}
	void operator()(GroupId, SnapshotReleased) {}
	void operator()(GroupId, Fenced) {}
};
static_assert(sink<NodeSink>);

// A caller's view of one call: how it completed.
struct Call {
	enum Kind { open, propose, committed, read, transfer } kind;
	NodeId node = 0;
	std::string command;
	Placement target;          // committed()
	Index floor = 0;           // read: the highest index applied anywhere when it was issued
	task::cancel_source cancel;
	bool cancel_requested = false;
	int completions = 0;
	std::exception_ptr error;
	std::optional<Placement> placed;
	std::optional<ReadReady> ready;
};

struct Node {
	NodeId id = 0;
	bool witness = false;
	jt::Disk disk;
	j::HeldCompletion held;
	std::unique_ptr<task::inline_scheduler> scheduler;
	std::unique_ptr<j::Host> host;
	NodeSink sink;
	std::unique_ptr<Driver<NodeSink>> driver;
	std::unique_ptr<task::cancel_source> stop;
	bool up = false, turns_done = true, driver_done = true;
	double rate = 1;
	std::vector<Entry> applied;  // what the application applied since the last restart
	std::optional<Index> applying;  // the last Committed range, until the application acknowledges it
	std::uint64_t committed_notices = 0;
	task::scheduler_ref ref() const { return task::scheduler_ref::of(*scheduler); }
	// Runs what is queued, failing (not hanging) if the loop never settles.
	void run() {
		if (!scheduler) { return; }
		std::size_t budget = 1'000'000;
		scheduler->run_until([&] { return --budget == 0; });
		if (!budget) { std::fprintf(stderr, "driver seed %llu: node %llu's loop spins\n", (unsigned long long)current_seed, (unsigned long long)id); std::exit(1); }
	}
};

struct Envelope { NodeId from, to; Message message; std::uint64_t at; };

struct Simulation {
	Simulation(std::uint64_t seed, bool witness) : random_(seed), witness_(witness) {
		for (NodeId id = 1; id <= 3; ++id) {
			auto& n = nodes_[id];
			n.id = id; n.witness = witness && id == 3;
			n.sink = NodeSink{this, &n};
			n.rate = 0.8 + double(random_() % 451) / 1000.0;
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
			const auto pick = random_() % 1000;
			if (pick < 300) { deliver_one(); }
			else if (pick < 550) { if (n.up) { complete_io(n); } }
			else if (pick < 650) { if (n.up) { apply(n); } }
			else if (pick < 760) { propose(leader_or(n)); }
			else if (pick < 800) { await_commit(n); }
			else if (pick < 830) { read(leader_or(n)); }
			else if (pick < 850) { cancel_one(); }
			else if (pick < 855) { transfer(); }
			else if (pick < 858) { if (n.up && up_count() == 3) { crash(n); } }
			else if (pick < 870) { if (!n.up) { boot(n); } }
			else if (pick < 874) { toggle_partition(n.id); }
			else { for (auto& [id, m] : nodes_) { if (m.up) { complete_io(m); apply(m); } } }
			check_calls();
		}
	}

	// Heals the network and every node, then commits a new entry on every data voter.
	void heal() {
		isolated_.clear();
		for (auto& [id, n] : nodes_) { if (!n.up) { boot(n); } }
		std::shared_ptr<Call> last;
		for (int round = 0; round < 4000 && !failures; ++round) {
			trace_step = 1'000'000 + std::uint64_t(round);
			quiet_round();
			if (!last || (last->completions && (last->error || !last->placed))) {
				for (auto& [id, n] : nodes_) {
					auto s = n.up ? n.driver->state(group) : std::nullopt;
					if (s && s->role == Role::Leader) { last = start_propose(n, "healed"); break; }
				}
			}
			if (last && last->placed && healed_everywhere(*last->placed)) { break; }
		}
		if (!last || !last->placed || !healed_everywhere(*last->placed)) {
			fail("the healed group did not apply a new entry on every data voter");
			if (last) {
				std::fprintf(stderr, "  last healed proposal on node %llu: completions %d error %d placed %llu:%llu\n", (unsigned long long)last->node, last->completions, bool(last->error),
					last->placed ? (unsigned long long)last->placed->term : 0ull, last->placed ? (unsigned long long)last->placed->index : 0ull);
				if (last->placed) {
					auto e = sequence_.find(last->placed->index);
					if (e != sequence_.end()) { std::fprintf(stderr, "  committed at %llu: term %llu payload %s\n", (unsigned long long)e->first, (unsigned long long)e->second.term, e->second.payload.c_str()); }
				}
			}
			for (auto& [id, n] : nodes_) {
				auto s = n.up ? n.driver->state(group) : std::nullopt;
				std::fprintf(stderr, "  node %llu up %d role %d term %llu committed %llu applied %llu (application %zu) held %zu\n", (unsigned long long)id, n.up, s ? int(s->role) : -1,
					s ? (unsigned long long)s->term : 0ull, s ? (unsigned long long)s->committed : 0ull, s ? (unsigned long long)s->applied : 0ull, n.applied.size(), n.held.held());
			}
		}
		// The witness acknowledged its own ranges and the Sink never saw one.
		for (auto& [id, n] : nodes_) {
			if (!n.witness) { continue; }
			auto s = n.driver->state(group);
			if (n.committed_notices) { fail("a witness member's Sink received a Committed range"); }
			if (!s || s->applied != s->committed || s->committed == 0) { fail("a witness member acknowledges its committed ranges itself"); }
		}
		// Shutdown: every call completes.
		for (auto& [id, n] : nodes_) { shut(n, false); }
		for (const auto& call : calls_) {
			if (call->completions != 1) { fail("a call completed " + std::to_string(call->completions) + " times by shutdown"); }
		}
		check_final();
	}

	std::uint64_t proposals = 0, placed = 0, commits = 0, superseded_count = 0, reads = 0, cancelled = 0, crashes = 0, transfers = 0, elections = 0;

	void on_send(Node& n, Send notice) {
		network_.push_back({n.id, notice.peer, std::move(notice.message), now_ + random_() % 6});
		if (random_() % 50 == 0) { network_.push_back({n.id, notice.peer, network_.back().message, now_ + random_() % 30}); }  // duplicated
	}
	void on_committed(Node& n, Committed notice) {
		++n.committed_notices;
		trace("node %llu Committed %llu..%llu (application had %zu)", (unsigned long long)n.id, (unsigned long long)notice.first,
			(unsigned long long)(notice.entries.empty() ? 0 : notice.entries.back().index), n.applied.size());
		if (n.witness) { fail("a witness member's Sink received a Committed range"); }
		if (notice.first != n.applied.size() + 1) { fail("node " + std::to_string(n.id) + " received a range from " + std::to_string(notice.first) + " after applying " + std::to_string(n.applied.size())); }
		for (auto& entry : notice.entries) {
			auto [it, fresh] = sequence_.emplace(entry.index, entry);
			if (!fresh && (it->second.term != entry.term || it->second.payload != entry.payload || it->second.kind != entry.kind)) {
				fail("state machine safety: index " + std::to_string(entry.index) + " differs between nodes");
			}
			n.applied.push_back(std::move(entry));
		}
		n.applying = n.applied.empty() ? 0 : n.applied.back().index;
	}
	void on_role(Node& n, RoleChanged notice) {
		trace("node %llu role %d term %llu", (unsigned long long)n.id, int(notice.role), (unsigned long long)notice.term);
		if (notice.role != Role::Leader) { return; }
		++elections;
		auto [it, fresh] = leaders_.emplace(notice.term, n.id);
		if (!fresh && it->second != n.id) { fail("two leaders in term " + std::to_string(notice.term)); }
	}

private:
	void boot(Node& n) {
		n.scheduler = std::make_unique<task::inline_scheduler>();
		n.scheduler->advance(now_ * ms);
		n.host = std::make_unique<j::Host>(std::make_shared<jt::MemoryIO>(n.disk, "host/"), j::Identity{'N', static_cast<char>('0' + n.id)}, j::HostOptions{1, turn_options});
		n.driver = std::make_unique<Driver<NodeSink>>(Seams{n.ref(), j::completion_ref::of(n.held), *n.host, 0, current_seed * 31 + n.id}, n.sink);
		n.stop = std::make_unique<task::cancel_source>();
		n.turns_done = n.driver_done = false;
		task::spawn(n.ref(), n.host->driver(0).run(j::completion_ref::of(n.held), n.stop->token()), [&n](std::exception_ptr) { n.turns_done = true; });
		task::spawn(n.ref(), n.driver->run(n.stop->token()), [&n](std::exception_ptr) { n.driver_done = true; });
		n.applied.clear(); n.applying.reset();
		n.up = true;
		trace("node %llu boots", (unsigned long long)n.id);
		GroupOptions o;
		o.configuration.local = n.id; o.configuration.voters = {1, 2, 3};
		o.configuration.cluster[0] = 'D'; o.configuration.configuration[0] = 'S';
		if (witness_) { o.founding = Membership{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 4, 0}; }
		o.admission = admission; o.limits = limits(); o.timing = timing; o.scheduling = WorkerLimits{16, 128};
		n.driver->open(group, std::move(o));
		auto call = track(n, Call::open);
		const auto status = n.host->status(group);
		if (status == j::GroupStatus::stored) { start(n, call, n.driver->recover(group, [](j::ArtifactReader&) {}, call->cancel.token())); }
		else { start(n, call, n.driver->create(group, j::Identity{'G', static_cast<char>('0' + n.id)}, call->cancel.token())); }
		n.run();
	}
	// Stops the node's loop. A crash first cuts its disk's power, so IO in flight fails and
	// unsynced writes may be lost; a clean shutdown lets its IO complete.
	void shut(Node& n, bool power_cut) {
		if (power_cut) { n.disk.cut = n.disk.operations; }
		n.stop->request(task::cancel_reason::shutdown);
		for (int round = 0; round < 10000 && !(n.turns_done && n.driver_done && !n.held.held()); ++round) {
			n.run();
			for (auto ticket : n.held.tickets()) {
				if (power_cut) { n.held.fail(ticket, std::make_exception_ptr(jt::PowerLoss{})); } else { n.held.complete(ticket); }
				n.run();
			}
		}
		if (!(n.turns_done && n.driver_done)) { fail("node " + std::to_string(n.id) + "'s loop did not stop"); }
		n.driver.reset(); n.host.reset(); n.scheduler.reset(); n.stop.reset();
		if (power_cut) { n.disk.power_loss(); }
		n.up = false;
	}
	void crash(Node& n) { ++crashes; trace("node %llu crashes", (unsigned long long)n.id); shut(n, true); }

	void advance(std::uint64_t dt) {
		now_ += dt;
		for (auto& [id, n] : nodes_) {
			if (!n.up) { continue; }
			n.scheduler->advance(static_cast<std::uint64_t>(double(dt) * n.rate * double(ms)));
			n.run();
		}
	}
	void quiet_round() {
		advance(5);
		while (!network_.empty()) { deliver(network_.front()); network_.pop_front(); }
		for (auto& [id, n] : nodes_) { if (n.up) { complete_io(n, true); apply(n); } }
	}
	std::size_t up_count() const { std::size_t up = 0; for (const auto& [id, n] : nodes_) { up += n.up; } return up; }
	Node& leader_or(Node& n) {
		for (auto& [id, m] : nodes_) {
			auto s = m.up ? m.driver->state(group) : std::nullopt;
			if (s && s->role == Role::Leader && random_() % 8) { return m; }
		}
		return n;
	}

	void deliver(const Envelope& e) {
		auto& to = nodes_.at(e.to);
		if (!to.up || isolated_.count(e.from) || isolated_.count(e.to)) { return; }
		to.driver->receive(group, Receive{e.from, e.message});
		to.run();
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
	void complete_io(Node& n, bool all = false) {
		for (int k = 0; n.held.held() && (all || k < 1 + int(random_() % 3)); ++k) {
			auto tickets = n.held.tickets();
			n.held.complete(tickets[random_() % tickets.size()]);
			n.run();
		}
	}
	void apply(Node& n) {
		if (!n.applying) { return; }
		trace("node %llu applied(%llu)", (unsigned long long)n.id, (unsigned long long)*n.applying);
		n.driver->applied(group, Applied{*n.applying});
		n.applying.reset();
		n.run();
	}
	void toggle_partition(NodeId id) {
		if (isolated_.erase(id)) { return; }
		if (isolated_.empty()) { isolated_.insert(id); }
	}

	std::shared_ptr<Call> track(Node& n, Call::Kind kind) {
		auto call = std::make_shared<Call>();
		call->kind = kind; call->node = n.id;
		calls_.push_back(call);
		return call;
	}
	template <class T> void start(Node& n, std::shared_ptr<Call> call, task::task<T> work) {
		task::spawn(n.ref(), std::move(work), [call](std::exception_ptr error, std::optional<T> value) {
			++call->completions; call->error = error;
			if constexpr (std::is_same_v<T, Placement>) {
				trace("node %llu propose %s: %s %llu:%llu", (unsigned long long)call->node, call->command.c_str(), error ? "error" : "placed",
					value ? (unsigned long long)value->term : 0ull, value ? (unsigned long long)value->index : 0ull);
			}
			if constexpr (std::is_same_v<T, Placement>) { call->placed = value; }
			if constexpr (std::is_same_v<T, ReadReady>) { call->ready = value; }
		});
	}
	void start(Node& n, std::shared_ptr<Call> call, task::task<void> work) {
		task::spawn(n.ref(), std::move(work), [call](std::exception_ptr error) { ++call->completions; call->error = error; });
	}
	std::shared_ptr<Call> start_propose(Node& n, std::string command) {
		auto call = track(n, Call::propose);
		call->command = command;
		start(n, call, n.driver->propose(group, std::move(command), call->cancel.token()));
		n.run();
		return call;
	}
	void propose(Node& n) {
		if (!n.up) { return; }
		++proposals;
		start_propose(n, "c" + std::to_string(++commands_));
	}
	void await_commit(Node& n) {
		if (!n.up) { return; }
		std::vector<std::shared_ptr<Call>> placed_calls;
		for (const auto& c : calls_) { if (c->placed) { placed_calls.push_back(c); } }
		if (placed_calls.empty()) { return; }
		auto source = placed_calls[random_() % placed_calls.size()];
		auto call = track(n, Call::committed);
		call->target = *source->placed;
		start(n, call, n.driver->committed(group, call->target, call->cancel.token()));
		n.run();
	}
	void read(Node& n) {
		if (!n.up) { return; }
		auto call = track(n, Call::read);
		call->floor = sequence_.empty() ? 0 : sequence_.rbegin()->first;
		start(n, call, n.driver->read(group, call->cancel.token()));
		n.run();
	}
	void transfer() {
		for (auto& [id, n] : nodes_) {
			auto s = n.up ? n.driver->state(group) : std::nullopt;
			if (!s || s->role != Role::Leader) { continue; }
			NodeId target = 1 + random_() % 2;
			if (target == id) { target = target == 1 ? 2 : 1; }
			if (!witness_) { target = 1 + random_() % 3; if (target == id) { target = 1 + target % 3; } }
			++transfers;
			trace("node %llu transfers to %llu", (unsigned long long)id, (unsigned long long)target);
			auto call = track(n, Call::transfer);
			start(n, call, n.driver->transfer_leadership(group, target, call->cancel.token()));
			n.run();
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
		nodes_.at(call->node).run();
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
	bool healed_everywhere(Placement p) const {
		for (const auto& [id, n] : nodes_) {
			if (n.witness) { continue; }
			if (n.applied.size() < p.index || n.applied[p.index - 1].term != p.term || n.applied[p.index - 1].payload != "healed") { return false; }
		}
		return true;
	}
	// Each completed call against the one committed sequence.
	void check_final() {
		calls_.insert(calls_.end(), checked_.begin(), checked_.end());
		for (const auto& c : calls_) {
			if (c->error) {
				try { std::rethrow_exception(c->error); }
				catch (const superseded&) {
					++superseded_count;
					auto e = sequence_.find(c->target.index);
					if (e == sequence_.end() || e->second.term == c->target.term) { fail("committed() superseded though its entry committed with its term"); }
				}
				catch (const task::cancelled&) {}
				catch (const std::exception&) {}
				continue;
			}
			if (c->kind == Call::propose && c->placed) {
				++placed;
				auto e = sequence_.find(c->placed->index);
				if (e != sequence_.end() && e->second.term == c->placed->term && e->second.payload != c->command) { fail("a Placement's entry committed with another command"); }
			} else if (c->kind == Call::committed) {
				++commits;
				auto e = sequence_.find(c->target.index);
				if (e == sequence_.end() || e->second.term != c->target.term) { fail("committed() completed though its entry did not commit with its term"); }
			} else if (c->kind == Call::read && c->ready) {
				++reads;
				if (c->ready->index < c->floor) { fail("a read's index is below an entry applied before it was issued"); }
			}
		}
	}

	std::mt19937_64 random_;
	bool witness_;
	std::map<NodeId, Node> nodes_;
	std::deque<Envelope> network_;
	std::set<NodeId> isolated_;
	std::uint64_t now_ = 1;
	std::uint64_t commands_ = 0;
	std::map<Index, Entry> sequence_;
	std::map<Term, NodeId> leaders_;
	std::vector<std::shared_ptr<Call>> calls_, checked_;
};

void NodeSink::operator()(GroupId, Send notice) { sim->on_send(*node, std::move(notice)); }
void NodeSink::operator()(GroupId, Committed notice) { sim->on_committed(*node, std::move(notice)); }
void NodeSink::operator()(GroupId, RoleChanged notice) { sim->on_role(*node, notice); }
void NodeSink::operator()(GroupId, ActivateInstall) {
	if (node->witness) { fail("a witness member was asked to activate an image"); }
}

}  // namespace

// Runs `seeds` seeds of `steps` steps from `first`; odd seeds found a 2 + witness group.
bool run(std::uint64_t seeds, std::uint64_t steps, std::uint64_t first) {
	std::uint64_t proposals = 0, placed = 0, commits = 0, superseded_count = 0, reads = 0, cancelled = 0, crashes = 0, transfers = 0, elections = 0;
	for (std::uint64_t seed = first; seed < first + seeds && !failures; ++seed) {
		current_seed = seed;
		Simulation sim(seed, seed % 2 == 1);
		sim.run(steps);
		if (!failures) { sim.heal(); }
		proposals += sim.proposals; placed += sim.placed; commits += sim.commits; superseded_count += sim.superseded_count;
		reads += sim.reads; cancelled += sim.cancelled; crashes += sim.crashes; transfers += sim.transfers; elections += sim.elections;
	}
	std::printf("driver simulation: %llu seeds x %llu steps, %llu elections, %llu crashes, %llu proposals (%llu placed), %llu commits awaited (%llu superseded), %llu reads, %llu transfers, %llu cancels: %s\n",
		(unsigned long long)seeds, (unsigned long long)steps, (unsigned long long)elections, (unsigned long long)crashes, (unsigned long long)proposals, (unsigned long long)placed,
		(unsigned long long)commits, (unsigned long long)superseded_count, (unsigned long long)reads, (unsigned long long)transfers, (unsigned long long)cancelled, failures ? "FAILED" : "ok");
	return failures == 0;
}

}  // namespace kronuz::consensus::driver_simulation
