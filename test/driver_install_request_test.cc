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

// A follower's install request (docs/mechanisms/install-request.md), through the Driver on three
// nodes (each a host on a memory disk with one loop on an inline_scheduler, frames carried in
// process) and on a bare Core. `driver_install_request_test [case]`:
//   lost_state     a current follower whose state is lost gets one image at the leader's base and
//                  re-delivers its own committed entries past it (A1); also on a bare Core
//   behind         a follower whose log lags the leader's base gets one install (A1)
//   current        a voter whose base is above the leader's asks 100 times: no image (A2)
//   after_install  a damaged replica at the leader's base gets exactly one image (A2)
//   unrequested    an image at or below the commit index with no request is CaughtUp (A2)
//   leader_lost    leadership moves before the image and mid-transfer: NotLeader, never two
//                  images; the new leader then ships one (A3)
//   second_request a second call is Busy; resent frames get InFlight; one transfer (A3)
//   non_leader     a follower answers NotLeader with its leader; LeaderMustTransfer, NotLeader,
//                  InvalidRepair for a witness and a non-member, NotLeader when unanswered (A3)
//   witness_bound  Driver::witness_bound is Core's; a witness truncates through it, keeping the
//                  records above, and not one past it (A4)
#include "consensus/driver.h"
#include "journal/memory_io.h"

#include <cstring>
#include <deque>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <variant>
#include <vector>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace jt = kronuz::journal::testing;

namespace {

int checks = 0, failures = 0;
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; if (failures < 40) { std::cerr << "FAIL: " << what << '\n'; } }
}

constexpr std::uint64_t ms = 1'000'000;
constexpr GroupId group = 43;
constexpr Timing timing{20, 40, 100, 200, 3000};
const j::TurnLogOptions turn_options{64 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 32, 3};
constexpr std::uint32_t format = 7;
bool witnessed = false;  // node 3 founds the group as a witness

Limits limits() { Limits l; l.command_bytes = 1024; l.rpc_bytes = 4096; l.log_bytes = 64 * 1024; l.log_entries = 64; l.control_entries = 4; l.rpc_entries = 8; return l; }

// The application's state: the payloads it applied, in order; its image is that list, then
// 150,000 bytes of filler, so a transfer spans three chunks.
constexpr std::size_t filler = 150000;
std::string encode_image(const std::vector<std::string>& applied) {
	std::string bytes(4, '\0');
	for (const auto& p : applied) { const auto n = static_cast<std::uint32_t>(p.size()); bytes.append(reinterpret_cast<const char*>(&n), 4); bytes += p; }
	const auto length = static_cast<std::uint32_t>(bytes.size() - 4);
	std::memcpy(bytes.data(), &length, 4);
	bytes.append(filler, 'f');
	return bytes;
}
std::vector<std::string> decode_image(std::string_view bytes) {
	std::vector<std::string> applied;
	std::uint32_t length = 0; std::memcpy(&length, bytes.data(), 4);
	bytes = bytes.substr(4, length);
	while (bytes.size() >= 4) {
		std::uint32_t n = 0; std::memcpy(&n, bytes.data(), 4); bytes.remove_prefix(4);
		applied.emplace_back(bytes.substr(0, n)); bytes.remove_prefix(n);
	}
	return applied;
}
std::string read_all(j::ArtifactReader& image) {
	std::string bytes(static_cast<std::size_t>(image.descriptor().length), '\0');
	for (std::size_t at = 0; at < bytes.size();) {
		at += image.read_at(at, std::span<char>(bytes.data() + at, std::min<std::size_t>(65536, bytes.size() - at)));
	}
	return bytes;
}
struct Capture {
	std::string image; std::size_t at = 0; Index through; Term term; std::uint64_t epoch;
	CaptureMetadata metadata() const { return {0, through, term, epoch}; }
	std::size_t read(std::span<char> out) {
		auto n = std::min(out.size(), image.size() - at);
		std::copy_n(image.data() + at, n, out.data()); at += n; return n;
	}
};

struct Cluster;
struct Node;

struct Sink {
	Cluster* cluster = nullptr;
	Node* node = nullptr;
	void operator()(GroupId, Send);
	void operator()(GroupId, Committed);
	void operator()(GroupId, RoleChanged) {}
	void operator()(GroupId, SendTransfer);
	void operator()(GroupId, ValidateImage);
	void operator()(GroupId, ActivateImage);
	void operator()(GroupId, CheckpointPublished) {}
	void operator()(GroupId, RepairCompleted) {}
	void operator()(GroupId, SnapshotNeeded) { ++needed; }
	void operator()(GroupId, SnapshotTransmit) {}
	void operator()(GroupId, SnapshotReleased) {}
	void operator()(GroupId, Fenced f) { fenced = f.reason; }
	int needed = 0;
	std::string fenced;
};
static_assert(sink<Sink>);

// A call's outcome, filled when its task completes.
struct Outcome { bool done = false; std::optional<RepairCompleted> result; std::optional<RejectReason> reason; std::string error; };

struct Node {
	NodeId id = 0;
	jt::Disk disk;
	task::inline_scheduler scheduler;
	j::InlineCompletion io;
	std::unique_ptr<j::Host> host;
	Sink sink;
	std::unique_ptr<Driver<Sink>> driver;
	std::unique_ptr<task::cancel_source> stop;
	bool turns_done = true, driver_done = true;
	std::vector<std::string> applied;
	std::optional<Index> applying;
	std::optional<std::pair<Token, std::string>> validation;
	std::optional<Token> activation;
	int validations = 0, activations = 0;
	std::uint64_t epoch = 0;
	Index redelivered = 0;  // the first index delivered after an activation
	bool activated = false;

	void boot(Cluster& c, std::uint64_t now);
	void run() {
		std::size_t budget = 1'000'000;
		scheduler.run_until([&] { return --budget == 0; });
		if (!budget) { check(false, "node " + std::to_string(id) + "'s loop spins"); std::exit(1); }
	}
	void shut() {
		if (!driver) { return; }
		bool closed = false;
		task::spawn(task::scheduler_ref::of(scheduler), driver->close(group), [&](std::exception_ptr) { closed = true; });
		run();
		stop->request(task::cancel_reason::shutdown);
		run();
		check(closed && turns_done && driver_done, "node " + std::to_string(id) + " closes and stops");
		driver.reset(); host.reset();
	}
	std::optional<Driver<Sink>::GroupState> state() const { return driver ? driver->state(group) : std::nullopt; }
};

struct Packet { NodeId from, to; std::variant<Message, std::string> body; };
struct Answer { NodeId from, to; InstallRequestResponse response; };

struct Cluster {
	std::map<NodeId, Node> nodes;
	std::deque<Packet> network;
	std::set<NodeId> isolated;
	std::function<bool(const Packet&)> drop;
	std::vector<Answer> answers;                 // every InstallRequestResponse sent
	std::map<std::pair<NodeId, NodeId>, int> frames;  // transfer frames, by (from, to)
	std::uint64_t now = 1, transfer_frames = 0;
	std::map<Index, Term> terms;
	Cluster() {
		for (NodeId id = 1; id <= 3; ++id) { auto& n = nodes[id]; n.id = id; n.boot(*this, now); }
		for (auto& [id, n] : nodes) { create(n); }
	}
	~Cluster() { for (auto& [id, n] : nodes) { n.shut(); } }
	static GroupOptions options(NodeId local) {
		GroupOptions o;
		o.configuration.local = local; o.configuration.voters = {1, 2, 3};
		o.configuration.cluster[0] = 'R'; o.configuration.configuration[0] = 'Q';
		o.admission = admission; o.limits = limits(); o.timing = timing; o.scheduling = WorkerLimits{16, 128};
		o.snapshots = SnapshotLimits{format, 1 << 20, 2};
		if (witnessed) { o.founding = Membership{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 4, 0}; }
		return o;
	}
	void create(Node& n) {
		n.driver->open(group, options(n.id));
		bool done = false; std::exception_ptr error;
		task::spawn(task::scheduler_ref::of(n.scheduler), n.driver->create(group, j::Identity{'R', static_cast<char>('0' + n.id)}), [&](std::exception_ptr e) { done = true; error = e; });
		n.run();
		check(done && !error, "node " + std::to_string(n.id) + " creates its group");
	}
	// Cuts a node off; the others' detector tells their groups it is gone.
	void isolate(NodeId id) {
		isolated.insert(id);
		for (auto& [other, n] : nodes) { if (other != id && n.driver) { n.driver->leader_gone(id); n.run(); } }
	}
	bool cut(NodeId from, NodeId to) const { return isolated.count(from) || isolated.count(to); }
	void round(std::uint64_t dt = 5) {
		now += dt;
		for (auto& [id, n] : nodes) { n.scheduler.advance(dt * ms); n.run(); }
		for (std::size_t k = network.size(); k; --k) {
			auto p = std::move(network.front()); network.pop_front();
			auto& to = nodes.at(p.to);
			if (cut(p.from, p.to) || !to.driver || (drop && drop(p))) { continue; }
			if (auto* m = std::get_if<Message>(&p.body)) { to.driver->receive(group, Receive{p.from, std::move(*m)}); }
			else { to.driver->transfer(group, p.from, std::move(std::get<std::string>(p.body))); }
			to.run();
		}
		for (auto& [id, n] : nodes) { answer(n); }
	}
	void answer(Node& n) {
		if (!n.driver) { return; }
		if (n.applying) { n.driver->applied(group, Applied{*n.applying}); n.applying.reset(); n.run(); }
		if (n.validation) { n.driver->validated(group, std::exchange(n.validation, std::nullopt)->first); n.run(); }
		if (n.activation) { n.driver->activated(group, *std::exchange(n.activation, std::nullopt)); n.run(); }
	}
	Node* leader() {
		for (auto& [id, n] : nodes) { auto s = n.state(); if (s && s->role == Role::Leader && !isolated.count(id)) { return &n; } }
		return nullptr;
	}
	Node* follower(Node* except = nullptr) {
		for (auto& [id, n] : nodes) {
			auto s = n.state();
			if (&n != except && s && s->role == Role::Follower && !isolated.count(id) && (!witnessed || id != 3)) { return &n; }
		}
		return nullptr;
	}
	template <class P> bool until(P done, int rounds = 4000) {
		for (int r = 0; r < rounds; ++r) { if (done()) { return true; } round(); }
		return done();
	}
	Placement commit(const std::string& command) {
		std::optional<Placement> placed;
		for (int attempt = 0; attempt < 20 && !placed; ++attempt) {
			until([&] { auto* l = leader(); return l && (!witnessed || l->id != 3); });
			auto* l = leader();
			if (!l) { continue; }
			bool done = false;
			task::spawn(task::scheduler_ref::of(l->scheduler), l->driver->propose(group, command), [&](std::exception_ptr e, std::optional<Placement> p) { done = true; if (!e) { placed = p; } });
			l->run();
			until([&] { return done; });
		}
		check(placed.has_value(), "the leader places " + command);
		if (!placed) { return {}; }
		until([&] {
			for (auto& [id, n] : nodes) {
				if (isolated.count(id)) { continue; }
				auto s = n.state();
				if (witnessed && id == 3 ? (!s || s->applied < placed->index) : n.applied.size() < placed->index) { return false; }
			}
			return true;
		});
		return *placed;
	}
	bool checkpoint(Node& n) {
		bool done = false; std::exception_ptr error;
		const Index through = n.applied.size();
		task::spawn(task::scheduler_ref::of(n.scheduler), n.driver->checkpoint(group, 1 << 20, Capture{encode_image(n.applied), 0, through, terms[through], n.epoch}),
			[&](std::exception_ptr e, std::optional<CheckpointPublished>) { done = true; error = e; });
		n.run();
		until([&] { return done; });
		return done && !error;
	}
	std::shared_ptr<Outcome> reinstall(Node& n) {
		auto outcome = std::make_shared<Outcome>();
		task::spawn(task::scheduler_ref::of(n.scheduler), n.driver->reinstall(group), [outcome, &n](std::exception_ptr e, std::optional<RepairCompleted> r) {
			outcome->done = true;
			if (r) { outcome->result = r; n.epoch = r->epoch; }
			if (e) {
				try { std::rethrow_exception(e); }
				catch (const rejected& x) { outcome->reason = x.reason; }
				catch (const std::exception& x) { outcome->error = x.what(); }
			}
		});
		n.run();
		return outcome;
	}
	// Waits for a call; its reason, or nothing on success.
	std::shared_ptr<Outcome> finish(std::shared_ptr<Outcome> outcome, int rounds = 6000) {
		until([&] { return outcome->done; }, rounds);
		check(outcome->done, "the reinstall call completes");
		if (!outcome->error.empty()) { check(false, "reinstall failed: " + outcome->error); }
		return outcome;
	}
};

void Node::boot(Cluster& c, std::uint64_t now) {
	host = std::make_unique<j::Host>(std::make_shared<jt::MemoryIO>(disk, "host/"), j::Identity{'H', static_cast<char>('0' + id)}, j::HostOptions{1, turn_options});
	sink = Sink{&c, this};
	scheduler.advance(now * ms - std::min(now * ms, scheduler.now()));
	driver = std::make_unique<Driver<Sink>>(Seams{task::scheduler_ref::of(scheduler), j::completion_ref::of(io), *host, 0, 17 + id}, sink);
	stop = std::make_unique<task::cancel_source>();
	turns_done = driver_done = false;
	auto ref = task::scheduler_ref::of(scheduler);
	task::spawn(ref, host->driver(0).run(j::completion_ref::of(io), stop->token()), [this](std::exception_ptr) { turns_done = true; });
	task::spawn(ref, driver->run(stop->token()), [this](std::exception_ptr) { driver_done = true; });
	run();
}

void Sink::operator()(GroupId, Send s) {
	if (auto r = std::get_if<InstallRequestResponse>(&s.message)) { cluster->answers.push_back({node->id, s.peer, *r}); }
	cluster->network.push_back({node->id, s.peer, std::move(s.message)});
}
void Sink::operator()(GroupId, SendTransfer t) {
	++cluster->transfer_frames; ++cluster->frames[{node->id, t.peer}];
	cluster->network.push_back({node->id, t.peer, std::move(t.frame)});
}
void Sink::operator()(GroupId, Committed c) {
	check(c.first == node->applied.size() + 1, "node " + std::to_string(node->id) + " gets the range after what it applied: " + std::to_string(c.first) + " after " + std::to_string(node->applied.size()));
	if (node->activated && !node->redelivered) { node->redelivered = c.first; }
	for (auto& e : c.entries) { node->applied.push_back(e.payload); cluster->terms[e.index] = e.term; }
	node->applying = c.entries.back().index;
}
void Sink::operator()(GroupId, ValidateImage v) {
	++node->validations;
	node->validation.emplace(v.token, read_all(v.image));
}
void Sink::operator()(GroupId, ActivateImage a) {
	++node->activations;
	auto image = decode_image(read_all(a.image));
	check(image.size() == a.boundary.index, "the activated image holds the state through its boundary");
	node->applied = std::move(image);
	node->activated = true; node->redelivered = 0;
	node->activation = a.token;
}

std::string name(const std::optional<RejectReason>& reason) { return reason ? std::to_string(static_cast<int>(*reason)) : std::string("none"); }

// A leader and a follower whose base is below the leader's, with committed entries above the
// leader's base: the follower checkpoints at 3, the leader at 9, and the group commits to 11.
struct Setup { Node* leader; Node* follower; Index base; };
Setup below(Cluster& c, bool follower_checkpoints = true) {
	for (int k = 0; k < 3; ++k) { c.commit("a" + std::to_string(k)); }
	auto* l = c.leader(); auto* f = c.follower();
	check(l && f, "a leader and a follower");
	if (!l || !f) { std::exit(1); }
	if (follower_checkpoints) { check(c.checkpoint(*f), "the follower checkpoints"); }
	for (int k = 0; k < 6; ++k) { c.commit("b" + std::to_string(k)); }
	const Index base = l->applied.size();
	check(c.checkpoint(*l), "the leader checkpoints");
	for (int k = 0; k < 2; ++k) { c.commit("c" + std::to_string(k)); }
	check(c.leader() == l, "the leader stays");
	return {l, f, base};
}

void lost_state_driver() {
	Cluster c;
	auto [l, f, base] = below(c);
	const auto committed = f->state()->committed;
	f->applied.assign(f->applied.size(), "lost");  // the application's state is gone
	const int needed = l->sink.needed, activations = f->activations;
	auto o = c.finish(c.reinstall(*f));
	check(o->result && o->result->boundary.index == base, "the call completes at the leader's base " + std::to_string(base) + ": reason " + name(o->reason));
	check(o->result && o->result->epoch == 1, "the epoch advances");
	check(f->activations == activations + 1 && f->validations == 1, "one validation and one activation");
	check(l->sink.needed == needed + 1, "the leader raised one SnapshotNeeded");
	check(f->state()->committed >= committed, "the follower's commit index never went back");
	check(f->redelivered == base + 1, "the follower re-delivered from the boundary on, from its own log: " + std::to_string(f->redelivered));
	c.commit("after");
	check(f->applied == l->applied, "the follower's state equals the leader's");
	check(f->sink.fenced.empty() && l->sink.fenced.empty(), "nothing fences");
	check(c.checkpoint(*f), "the restored follower checkpoints at its new epoch");
}

// A bare follower Core of leader 2, holding 1..5 committed through 4, applied.
FixedConfiguration configuration(NodeId local) {
	FixedConfiguration c; c.cluster[0] = 'R'; c.configuration[0] = 'Q'; c.local = local; c.voters = {1, 2, 3}; return c;
}
template <class T> const T* first(const Actions& actions) {
	for (const auto& a : actions) { if (auto v = std::get_if<T>(&a)) { return v; } }
	return nullptr;
}
template <class T> const T* first(Actions&&) = delete;  // the pointer would outlive the batch
std::unique_ptr<Core> follower_core(std::optional<Membership> membership = std::nullopt, NodeId leader = 2) {
	RecoveredState state; state.configuration = configuration(1); state.hard = HardState{1, std::nullopt, 4};
	for (Index i = 1; i <= 5; ++i) { state.entries.push_back({i, 1, i == 1 ? EntryKind::NoOp : EntryKind::Command, i == 1 ? "" : "x" + std::to_string(i)}); }
	state.membership = membership;
	auto core = std::make_unique<Core>(configuration(1), std::move(state));
	auto started = core->step(Start{});
	if (auto committed = first<Committed>(started)) { core->step(Applied{committed->entries.back().index}); }
	if (leader) { core->step(Receive{leader, AppendRequest{1, 90, 5, 1, 4, 0, {}}}); }
	return core;
}
InstallPrepared image(NodeId from, LogBoundary boundary) {
	return InstallPrepared{30, 31, from, 1, configuration(1).cluster, configuration(1).configuration, boundary};
}

void lost_state_core() {
	auto core = follower_core();
	auto asked = core->step(RequestInstall{7});
	auto send = first<Send>(asked);
	auto frame = send ? std::get_if<InstallRequest>(&send->message) : nullptr;
	check(frame && send->peer == 2 && frame->term == 1 && frame->base == LogBoundary{0, 0}, "the request goes to the leader with this replica's base");
	auto staged = core->step(image(2, {2, 1}));
	auto persist = first<PersistInstall>(staged);
	check(persist && persist->state.base_index == 2 && persist->state.hard.commit_index == 4 && persist->state.entries.size() == 3,
		"a requested image below the commit index keeps the log above it and the commit index");
	if (!persist) { return; }
	auto published = core->step(Persisted{persist->token});
	auto activate = first<ActivateInstall>(published);
	check(activate != nullptr, "the image activates");
	if (!activate) { return; }
	auto done = core->step(InstallActivated{activate->token});
	auto repaired = first<RepairCompleted>(done);
	auto committed = first<Committed>(done);
	check(first<InstallCompleted>(done) && repaired && repaired->request == 7 && repaired->boundary == LogBoundary{2, 1} && repaired->epoch == 1 && core->epoch() == 1,
		"activation completes the request as a repair, its epoch advanced");
	check(committed && committed->first == 3 && committed->entries.back().index == 4, "the committed entries past the boundary are delivered from its own log");
	check(core->committed() == 4 && core->last_index() == 5 && core->base_index() == 2, "commit index and log above the boundary stay");
}

void lost_state() { lost_state_driver(); lost_state_core(); }

void behind() {
	Cluster c;
	for (int k = 0; k < 3; ++k) { c.commit("a" + std::to_string(k)); }
	auto& three = c.nodes.at(3);
	if (c.leader() == &three) { std::cerr << "node 3 leads; case needs a follower\n"; check(false, "node 3 follows"); return; }
	check(c.checkpoint(three), "node 3 checkpoints before it is cut off");
	c.isolated.insert(3);
	for (int k = 0; k < 12; ++k) { c.commit("b" + std::to_string(k)); }
	auto* l = c.leader();
	const Index base = l->applied.size();
	for (auto& [id, n] : c.nodes) { if (id != 3) { check(c.checkpoint(n), "node " + std::to_string(id) + " checkpoints"); } }
	check(three.state()->leader == l->id, "node 3 still knows its leader");
	c.isolated.clear();
	auto o = c.finish(c.reinstall(three));
	check(o->result && o->result->boundary.index == base, "the lagging follower installs the leader's base: reason " + name(o->reason));
	check(three.activations == 1, "one install");
	c.commit("after");
	check(three.applied == c.leader()->applied, "it catches up by appends");
}

void current() {
	Cluster c;
	for (int k = 0; k < 3; ++k) { c.commit("a" + std::to_string(k)); }
	auto* l = c.leader(); auto* f = c.follower();
	check(c.checkpoint(*l), "the leader checkpoints at 3");
	for (int k = 0; k < 3; ++k) { c.commit("b" + std::to_string(k)); }
	check(c.checkpoint(*f), "the voter checkpoints at 6, above the leader's base");
	const int needed = l->sink.needed; const auto frames = c.transfer_frames;
	int refused = 0;
	for (int k = 0; k < 100; ++k) {
		auto o = c.finish(c.reinstall(*f), 200);
		refused += o->reason == RejectReason::Current;
	}
	check(refused == 100, "every request is refused as Current: " + std::to_string(refused));
	check(l->sink.needed == needed && c.transfer_frames == frames && f->activations == 0, "the leader ships nothing: " + std::to_string(l->sink.needed - needed) + " SnapshotNeeded");
}

void after_install() {
	Cluster c;
	for (int k = 0; k < 3; ++k) { c.commit("a" + std::to_string(k)); }
	auto* l = c.leader(); auto* f = c.follower();
	const Index base = l->applied.size();
	check(f->applied.size() == base && c.checkpoint(*l) && c.checkpoint(*f), "the leader and the replica checkpoint at the same base");
	c.commit("b");
	const int needed = l->sink.needed;
	auto o = c.finish(c.reinstall(*f));
	check(o->result && o->result->boundary.index == base, "the damaged replica at the leader's base " + std::to_string(base) + " gets its image: reason " + name(o->reason) +
		(o->result ? ", boundary " + std::to_string(o->result->boundary.index) : std::string()));
	int refused = 0;
	for (int k = 0; k < 20; ++k) { refused += c.finish(c.reinstall(*f), 200)->reason == RejectReason::Current; }
	check(refused == 20, "every later request is Current: " + std::to_string(refused));
	check(f->activations == 1 && l->sink.needed == needed + 1, "exactly one image: " + std::to_string(f->activations) + " activations, " + std::to_string(l->sink.needed - needed) + " SnapshotNeeded");
}

void unrequested() {
	auto core = follower_core();
	auto out = core->step(image(2, {2, 1}));
	auto rejected = first<InstallRejected>(out);
	check(rejected && rejected->reason == InstallRejectReason::CaughtUp && !first<PersistInstall>(out), "an unrequested image below the commit index is CaughtUp");
	auto other = follower_core();
	other->step(RequestInstall{7});
	auto stranger = other->step(image(3, {2, 1}));
	auto refused = first<InstallRejected>(stranger);
	check(refused && refused->reason == InstallRejectReason::CaughtUp, "an image from another than the leader asked is CaughtUp");
}

// Leadership moves once the leader raised SnapshotNeeded (`mid`: once its transfer is under way).
void leader_lost_at(bool mid) {
	Cluster c;
	auto [l, f, base] = below(c, false);
	(void)base;
	const NodeId gone = l->id;
	auto* other = c.follower(f);
	check(other && c.checkpoint(*other), "the other follower checkpoints, so as the next leader it holds an image");
	if (!other) { return; }
	auto o = c.reinstall(*f);
	const int needed = l->sink.needed;
	check(c.until([&] { return mid ? c.frames[{gone, f->id}] >= 3 : l->sink.needed > needed; }), "the leader starts the image");
	// The requester never campaigns, so the other follower leads next.
	const NodeId requester = f->id;
	c.drop = [requester](const Packet& p) {
		auto m = std::get_if<Message>(&p.body);
		return p.from == requester && m && (std::holds_alternative<PreVoteRequest>(*m) || std::holds_alternative<VoteRequest>(*m));
	};
	c.isolate(gone);
	c.finish(o);
	check(o->reason == RejectReason::NotLeader, std::string(mid ? "mid-transfer" : "before the transfer") + ", the call fails NotLeader: reason " + name(o->reason));
	check(f->activations == 0, "no image was activated");
	check(c.until([&] { auto* x = c.leader(); return x == other && f->state()->leader == other->id; }), "the other follower leads, and the requester knows it");
	auto again = c.finish(c.reinstall(*f));
	check(again->result.has_value(), "the new leader ships the image: reason " + name(again->reason));
	check(f->activations == 1, "one image in all");
	c.commit("after");
	check(f->applied == c.leader()->applied, "the follower's state equals the new leader's");
}
void leader_lost() { leader_lost_at(false); leader_lost_at(true); }

void second_request() {
	Cluster c;
	auto [l, f, base] = below(c);
	const int needed = l->sink.needed;
	int dropped = 0;
	c.drop = [&](const Packet& p) {
		auto m = std::get_if<Message>(&p.body);
		auto r = m ? std::get_if<InstallRequestResponse>(m) : nullptr;
		return r && r->result == InstallRequestReply::Started && dropped++ == 0;
	};
	auto one = c.reinstall(*f), two = c.reinstall(*f);
	c.finish(two, 200);
	check(two->reason == RejectReason::Busy, "a second call while one is pending is Busy: reason " + name(two->reason));
	c.finish(one);
	check(one->result && one->result->boundary.index == base, "the first completes: reason " + name(one->reason));
	int in_flight = 0;
	for (const auto& a : c.answers) { in_flight += a.response.result == InstallRequestReply::InFlight; }
	check(dropped == 1 && in_flight >= 1, "the resent frame is answered InFlight: " + std::to_string(in_flight));
	check(l->sink.needed == needed + 1 && f->activations == 1, "one transfer");
}

void non_leader() {
	{
		Cluster c;
		for (int k = 0; k < 2; ++k) { c.commit("a" + std::to_string(k)); }
		auto* l = c.leader(); auto* f = c.follower();
		const auto term = f->state()->term;
		f->driver->receive(group, Receive{c.follower(f)->id, InstallRequest{term, 77, {0, 0}}}); f->run();
		bool redirected = false;
		for (const auto& a : c.answers) { redirected |= a.from == f->id && a.response.request == 77 && a.response.result == InstallRequestReply::NotLeader && a.response.leader == l->id; }
		check(redirected, "a follower answers NotLeader, naming its leader");
		check(c.finish(c.reinstall(*l), 50)->reason == RejectReason::LeaderMustTransfer, "the leader's own call is LeaderMustTransfer");
	}
	{
		witnessed = true;
		Cluster c;
		for (int k = 0; k < 2; ++k) { c.commit("a" + std::to_string(k)); }
		auto* l = c.leader();
		auto& three = c.nodes.at(3);
		check(l && l->id != 3, "a data voter leads");
		check(c.finish(c.reinstall(three), 50)->reason == RejectReason::InvalidRepair, "a witness's call is InvalidRepair");
		l->driver->receive(group, Receive{3, InstallRequest{l->state()->term, 78, {0, 0}}}); l->run();
		bool refused = false;
		for (const auto& a : c.answers) { refused |= a.from == l->id && a.response.request == 78 && a.response.result == InstallRequestReply::NotMember; }
		check(refused, "the leader answers a witness NotMember");
		witnessed = false;
	}
	auto lonely = follower_core(std::nullopt, 0);
	const auto alone = lonely->step(RequestInstall{5});
	auto none = first<Reject>(alone);
	check(none && none->reason == RejectReason::NotLeader, "with no leader known the call is NotLeader");
	auto removed = follower_core();
	const auto asked = removed->step(RequestInstall{5});
	auto ask = first<Send>(asked);
	auto frame = ask ? std::get_if<InstallRequest>(&ask->message) : nullptr;
	check(frame != nullptr, "the request goes out");
	const auto answered = removed->step(Receive{2, InstallRequestResponse{1, frame ? frame->request : 0, InstallRequestReply::NotMember}});
	auto member = first<Reject>(answered);
	check(member && member->reason == RejectReason::InvalidRepair, "a removed incarnation's call is InvalidRepair");
	// Unanswered: resent each heartbeat while the leader keeps heartbeating, NotLeader after an
	// election timeout.
	auto silent = follower_core();
	silent->step(RequestInstall{6});
	int resent = 0; std::optional<RejectReason> reason;
	const Timing t;
	for (std::uint64_t now = 50; now <= t.election_max + 200 && !reason; now += 50) {
		auto out = silent->step(Tick{now, t.election_min});
		for (const auto& a : out) {
			if (auto s = std::get_if<Send>(&a); s && std::holds_alternative<InstallRequest>(s->message)) { ++resent; }
			if (auto r = std::get_if<Reject>(&a); r && r->request == 6) { reason = r->reason; }
		}
		silent->step(Receive{2, AppendRequest{1, 100 + now, 5, 1, 4, 0, {}}});
	}
	check(reason == RejectReason::NotLeader && silent->role() == Role::Follower && silent->leader() == 2, "an unanswered request fails NotLeader under its leader: reason " + name(reason));
	check(resent >= int(t.election_max / t.heartbeat) - 2, "it was resent each heartbeat: " + std::to_string(resent));
}

void witness_bound() {
	{
		witnessed = true;
		Cluster c;
		for (int k = 0; k < 4; ++k) { c.commit("a" + std::to_string(k)); }
		auto& three = c.nodes.at(3);
		c.until([&] { return false; }, 100);
		auto bound = three.driver->witness_bound(group);
		check(bound && *bound == three.state()->committed && *bound > 0, "Driver::witness_bound is the witness's bound: " + std::to_string(bound.value_or(0)));
		check(!three.driver->witness_bound(group + 1), "absent for a group not open");
		witnessed = false;
	}
	Membership founding{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 4, 0};
	RecoveredState state; state.configuration = configuration(3); state.membership = founding;
	Core w(configuration(3), state);
	w.step(Start{});
	std::vector<Entry> entries;
	for (Index i = 1; i <= 6; ++i) { entries.push_back({i, 1, i == 1 ? EntryKind::NoOp : EntryKind::Command, i == 1 ? "" : "x"}); }
	auto appended = w.step(Receive{1, AppendRequest{1, 9, 0, 0, 6, 0, entries, 4}});
	if (auto p = first<Persist>(appended)) {
		auto out = w.step(Persisted{p->token});
		if (auto committed = first<Committed>(out)) { w.step(Applied{committed->entries.back().index}); }
	}
	check(w.witness_bound() == 4 && w.applied() == 6, "the witness's bound is what the data voters matched, below what it applied");
	const auto& id = configuration(3);
	const auto beyond = w.step(LocalCheckpoint{1, 9, 5, 1, id.cluster, id.configuration});
	auto past = first<Reject>(beyond);
	check(past && past->reason == RejectReason::InvalidCheckpoint, "one past the bound is InvalidCheckpoint");
	const auto bounded = w.step(LocalCheckpoint{2, 10, 4, 1, id.cluster, id.configuration});
	auto through = first<PersistCheckpoint>(bounded);
	check(through != nullptr, "a checkpoint through the bound is accepted");
	if (through) { w.step(Persisted{through->token}); }
	check(w.base_index() == 4 && w.last_index() == 6 && w.term_at(5) == Term{1} && w.term_at(6) == Term{1}, "it keeps every record above the bound");
}

const std::map<std::string, void (*)()> cases{
	{"lost_state", &lost_state},
	{"behind", &behind},
	{"current", &current},
	{"after_install", &after_install},
	{"unrequested", &unrequested},
	{"leader_lost", &leader_lost},
	{"second_request", &second_request},
	{"non_leader", &non_leader},
	{"witness_bound", &witness_bound},
};

}  // namespace

int main(int argc, char** argv) {
	if (argc > 1) {
		auto found = cases.find(argv[1]);
		if (found == cases.end()) { std::cerr << "unknown case " << argv[1] << '\n'; return 2; }
		std::cerr << "case " << argv[1] << '\n';
		found->second();
	} else {
		for (const auto& [name, run] : cases) { std::cerr << "case " << name << '\n'; run(); }
	}
	std::cout << checks << " install request checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
