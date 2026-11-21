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

// A Driver group's install (docs/DRIVER.md, Install): three nodes, each a host on a memory disk
// with one loop on an inline_scheduler, their Sinks' Send and SendTransfer carried in process.
// Node 3 is cut off while the leader commits and checkpoints past it; healed, it installs the
// leader's image and applies entries past its boundary. `driver_install_test [case]`:
//   lagging   the image installs, validated and activated, and node 3 applies past it (A1)
//   rejected  validation_failed keeps node 3's state; the leader's retry, validated, installs (A2)
//   stale     an answer with a stale token, and a transfer frame cut short, are dropped and counted
//   restart   node 3 restarts between validation and its answer; the leader's retry installs
//   crash     node 3's disk loses power at each of its operations from the heal through the install
//             (`crash STRIDE`: every STRIDE-th): it recovers the old replica or the complete new one,
//             never an image its application activated that consensus did not persist (A3)
//   orphan    quiescent: the leader's transfer to node 3 stops after its first chunk and the
//             leader goes; at rest, node 3 drops the image at its snapshot_timeout, then repairs
#include "consensus/driver.h"
#include "journal/memory_io.h"

#include <cstdlib>
#include <cstring>
#include <limits>
#include <deque>
#include <functional>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <variant>
#include <vector>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace jt = kronuz::journal::testing;

namespace {

int checks = 0, failures = 0;
bool quiesce = false;  // the groups rest (docs/mechanisms/quiescence.md)
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; if (failures < 40) { std::cerr << "FAIL: " << what << '\n'; } }
}

constexpr std::uint64_t ms = 1'000'000;
constexpr GroupId group = 41;
constexpr Timing timing{20, 40, 100, 200, 3000};
const j::TurnLogOptions turn_options{64 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 32, 3};
constexpr std::uint32_t format = 7;

Limits limits() { Limits l; l.command_bytes = 1024; l.rpc_bytes = 4096; l.log_bytes = 64 * 1024; l.log_entries = 64; l.control_entries = 4; l.rpc_entries = 8; return l; }

// The application's state: the payloads it applied, in index order. Its image is that list, its
// length first, then 150,000 bytes of filler, so a transfer spans three chunks.
constexpr std::size_t filler = 150000;
std::string encode_image(const std::vector<std::string>& applied) {
	std::string bytes(4, '\0');
	for (const auto& p : applied) { const auto n = static_cast<std::uint32_t>(p.size()); bytes.append(reinterpret_cast<const char*>(&n), 4); bytes += p; }
	const auto length = static_cast<std::uint32_t>(bytes.size() - 4);
	std::memcpy(bytes.data(), &length, 4);
	for (std::size_t k = 0; k < filler; ++k) { bytes.push_back(static_cast<char>('a' + k % 26)); }
	return bytes;
}
std::vector<std::string> decode_image(std::string_view bytes) {
	std::vector<std::string> applied;
	std::uint32_t length = 0; std::memcpy(&length, bytes.data(), 4);
	check(bytes.size() == 4 + length + filler, "an image of its entries and the filler");
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
	std::string image; std::size_t at = 0; Index through; Term term;
	CaptureMetadata metadata() const { return {0, through, term, 0}; }
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
	void operator()(GroupId, SnapshotReleased) { ++released; }
	void operator()(GroupId, Fenced f) { fenced = f.reason; }
	int needed = 0, released = 0;
	std::string fenced;
};
static_assert(sink<Sink>);

// A node: its host, one loop, its Driver and its application.
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
	// The application.
	std::vector<std::string> applied;             // payloads through index applied.size()
	std::optional<Index> applying;                // a delivered range, until applied()
	std::optional<std::pair<Token, std::string>> validation;  // a ValidateImage, until answered
	std::optional<Token> activation;              // an ActivateImage, until answered
	std::vector<std::string> validated_image;     // what validation read
	int validations = 0, activations = 0, restores = 0;
	bool hold = false;                            // leaves a ValidateImage unanswered
	// The application's own durable state, which a power cut keeps: the boundary of the image it
	// staged at validation, and of the one it activated (strata's stage and activation).
	Index staged = 0, active = 0;
	std::function<bool(const std::vector<std::string>&)> accept = [](const auto&) { return true; };

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
		check(closed, "node " + std::to_string(id) + "'s group closes");
		stop->request(task::cancel_reason::shutdown);
		run();
		check(turns_done && driver_done, "node " + std::to_string(id) + "'s loop stops");
		driver.reset(); host.reset();
	}
	std::optional<Driver<Sink>::GroupState> state() const { return driver ? driver->state(group) : std::nullopt; }
};

struct Packet { NodeId from, to; std::variant<Message, std::string> body; };

struct Cluster {
	std::map<NodeId, Node> nodes;
	std::deque<Packet> network;
	std::set<NodeId> isolated;
	std::function<bool(const Packet&)> drop;  // a packet the network loses
	std::uint64_t now = 1, transfer_frames = 0, transfer_bytes = 0;
	Cluster() {
		for (NodeId id = 1; id <= 3; ++id) { auto& n = nodes[id]; n.id = id; n.boot(*this, now); }
		for (auto& [id, n] : nodes) { create(n); }
	}
	~Cluster() { for (auto& [id, n] : nodes) { n.shut(); } }
	static GroupOptions options(NodeId local) {
		GroupOptions o;
		o.configuration.local = local; o.configuration.voters = {1, 2, 3};
		o.configuration.cluster[0] = 'I'; o.configuration.configuration[0] = 'N';
		o.admission = admission; o.limits = limits(); o.timing = timing; o.scheduling = WorkerLimits{16, 128};
		o.snapshots = SnapshotLimits{format, 1 << 20, 2};
		o.timing.quiesce = quiesce;
		return o;
	}
	void create(Node& n) {
		n.driver->open(group, options(n.id));
		bool done = false; std::exception_ptr error;
		task::spawn(task::scheduler_ref::of(n.scheduler), n.driver->create(group, j::Identity{'I', static_cast<char>('0' + n.id)}), [&](std::exception_ptr e) { done = true; error = e; });
		n.run();
		check(done && !error, "node " + std::to_string(n.id) + " creates its group");
	}
	// Restarts a node from its disk, after a clean shutdown or a power cut: the application
	// restores its latest checkpoint's image, and finishes an activation it staged for it.
	void restart(Node& n, bool power_cut = false) {
		if (power_cut) {
			n.disk.cut = std::min(n.disk.cut, n.disk.operations);
			n.stop->request(task::cancel_reason::shutdown);
			n.run();
			check(n.turns_done && n.driver_done, "node " + std::to_string(n.id) + "'s loop stops after a power cut");
			n.driver.reset(); n.host.reset();
			n.disk.power_loss();
			n.disk.cut = std::numeric_limits<std::uint64_t>::max();
		} else {
			n.shut();
		}
		n.boot(*this, now);
		n.applied.clear(); n.applying.reset(); n.validation.reset(); n.activation.reset(); n.sink.fenced.clear();
		n.driver->open(group, options(n.id));
		bool done = false; std::exception_ptr error;
		task::spawn(task::scheduler_ref::of(n.scheduler), n.driver->recover(group, [&n](j::ArtifactReader& image) {
				n.applied = decode_image(read_all(image)); ++n.restores;
				const Index base = n.applied.size();
				if (n.staged == base) { n.active = base; }
				check(n.active <= base, "node " + std::to_string(n.id) + "'s application activated an image at " + std::to_string(n.active) + " that consensus did not persist (restored " + std::to_string(base) + ")");
				if (n.staged <= base) { n.staged = 0; }
			}),
			[&](std::exception_ptr e) { done = true; error = e; });
		n.run();
		check(done && !error, "node " + std::to_string(n.id) + " recovers its group");
	}
	// Cuts a node off; the others' detector tells their groups it is gone (a resting follower
	// elects only on that).
	void isolate(NodeId id) {
		isolated.insert(id);
		for (auto& [other, n] : nodes) { if (other != id && n.driver) { n.driver->leader_gone(id); n.run(); } }
	}
	bool cut(NodeId from, NodeId to) const { return isolated.count(from) || isolated.count(to); }
	// One round: the clock moves, the network delivers, the applications answer.
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
		if (n.validation && !n.hold) {
			auto [token, bytes] = *std::exchange(n.validation, std::nullopt);
			auto image = decode_image(bytes);
			if (n.accept(image)) { n.validated_image = image; n.driver->validated(group, token); } else { n.driver->validation_failed(group, token); }
			n.run();
		}
		if (n.activation) { n.driver->activated(group, *std::exchange(n.activation, std::nullopt)); n.run(); }
	}
	Node* leader() {
		for (auto& [id, n] : nodes) { auto s = n.state(); if (s && s->role == Role::Leader && !isolated.count(id)) { return &n; } }
		return nullptr;
	}
	template <class P> bool until(P done, int rounds = 4000) {
		for (int r = 0; r < rounds; ++r) { if (done()) { return true; } round(); }
		return done();
	}
	// Proposes on the leader and waits for every reachable member to apply it.
	Placement commit(const std::string& command) {
		std::optional<Placement> placed;
		for (int attempt = 0; attempt < 20 && !placed; ++attempt) {
			until([&] { return leader() != nullptr; });
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
			for (auto& [id, n] : nodes) { if (!isolated.count(id) && n.applied.size() < placed->index) { return false; } }
			return true;
		});
		return *placed;
	}
	// The node checkpoints what its application applied.
	bool checkpoint(Node& n) {
		auto s = n.state();
		if (!s) { return false; }
		bool done = false; std::exception_ptr error;
		const Index through = n.applied.size();
		task::spawn(task::scheduler_ref::of(n.scheduler), n.driver->checkpoint(group, 1 << 20, Capture{encode_image(n.applied), 0, through, *term_at(n, through)}),
			[&](std::exception_ptr e, std::optional<CheckpointPublished>) { done = true; error = e; });
		n.run();
		until([&] { return done; });
		if (error) { try { std::rethrow_exception(error); } catch (const std::exception& e) { std::cerr << "node " << n.id << "'s checkpoint: " << e.what() << '\n'; } }
		else if (!done) { std::cerr << "node " << n.id << "'s checkpoint did not complete\n"; }
		return done && !error;
	}
	std::optional<Term> term_at(Node& n, Index index) { (void)n; auto it = terms.find(index); return it == terms.end() ? std::nullopt : std::optional<Term>(it->second); }
	std::map<Index, Term> terms;  // each committed index's term, as the applications saw it
};

void Node::boot(Cluster& c, std::uint64_t now) {
	host = std::make_unique<j::Host>(std::make_shared<jt::MemoryIO>(disk, "host/"), j::Identity{'N', static_cast<char>('0' + id)}, j::HostOptions{1, turn_options});
	sink = Sink{&c, this};
	scheduler.advance(now * ms - std::min(now * ms, scheduler.now()));
	driver = std::make_unique<Driver<Sink>>(Seams{task::scheduler_ref::of(scheduler), j::completion_ref::of(io), *host, 0, 11 + id}, sink);
	stop = std::make_unique<task::cancel_source>();
	turns_done = driver_done = false;
	auto ref = task::scheduler_ref::of(scheduler);
	task::spawn(ref, host->driver(0).run(j::completion_ref::of(io), stop->token()), [this](std::exception_ptr) { turns_done = true; });
	task::spawn(ref, driver->run(stop->token()), [this](std::exception_ptr) { driver_done = true; });
	run();
}

void Sink::operator()(GroupId, Send s) { cluster->network.push_back({node->id, s.peer, std::move(s.message)}); }
void Sink::operator()(GroupId, SendTransfer t) {
	++cluster->transfer_frames; cluster->transfer_bytes += t.frame.size();
	cluster->network.push_back({node->id, t.peer, std::move(t.frame)});
}
void Sink::operator()(GroupId, Committed c) {
	check(c.first == node->applied.size() + 1, "node " + std::to_string(node->id) + " gets the range after what it applied: " + std::to_string(c.first) + " after " + std::to_string(node->applied.size()));
	for (auto& e : c.entries) { node->applied.push_back(e.payload); cluster->terms[e.index] = e.term; }
	node->applying = c.entries.back().index;
}
void Sink::operator()(GroupId, ValidateImage v) {
	++node->validations;
	check(v.source != node->id && v.boundary.index != 0, "a validation names its source and boundary");
	node->validation.emplace(v.token, read_all(v.image));
	node->staged = v.boundary.index;
}
void Sink::operator()(GroupId, ActivateImage a) {
	++node->activations;
	auto image = decode_image(read_all(a.image));
	check(image.size() == a.boundary.index, "the activated image holds the state through its boundary");
	check(image == node->validated_image, "the activated image is the one validated");
	node->applied = std::move(image);
	node->active = a.boundary.index;
	node->activation = a.token;
}

std::vector<std::string> prefix(const std::vector<std::string>& v, std::size_t n) { return {v.begin(), v.begin() + std::ptrdiff_t(std::min(n, v.size()))}; }

// Commits, cuts node 3 off, commits and checkpoints past it on 1 and 2: node 3 is behind the
// leader's checkpoint, which compacted the log it would need. Returns the checkpoint's boundary.
Index lag(Cluster& c) {
	for (int k = 0; k < 3; ++k) { c.commit("before" + std::to_string(k)); }
	check(c.checkpoint(c.nodes.at(3)), "node 3 checkpoints before it is cut off");
	c.isolated.insert(3);
	for (int k = 0; k < 12; ++k) { c.commit("during" + std::to_string(k)); }
	auto* l = c.leader();
	check(l && l->id != 3, "a leader among 1 and 2");
	if (!l) { return 0; }
	for (auto& [id, n] : c.nodes) { if (id != 3) { check(c.checkpoint(n), "node " + std::to_string(id) + " checkpoints"); } }
	return l->applied.size();
}

void lagging() {
	Cluster c;
	const Index boundary = lag(c);
	auto& three = c.nodes.at(3);
	const auto before = three.applied;
	check(before.size() < boundary, "node 3 is behind the checkpoint");
	c.isolated.clear();
	const bool installed = c.until([&] { return three.activations > 0; });
	check(installed, "node 3 installs the leader's image");
	check(three.validations == 1 && three.activations == 1, "one validation and one activation");
	const auto placed = c.commit("after");
	auto s = three.state();
	check(s && s->applied >= placed.index, "node 3 applies entries past the boundary");
	auto* l = c.leader();
	check(l && three.applied == l->applied, "node 3's application state equals the leader's");
	check(three.applied.size() > boundary && three.applied.back() == "after", "the entry past the boundary reached node 3's application");
	check(three.sink.fenced.empty(), "node 3 never fences");
	check(c.transfer_frames >= 4, "the image went as transfer frames: Begin, Accepted, a Chunk, a Credit, a Result");
	std::cerr << "lagging: boundary " << boundary << ", " << c.transfer_frames << " transfer frames, " << c.transfer_bytes << " bytes\n";
}

void rejected() {
	Cluster c;
	const Index boundary = lag(c);
	auto& three = c.nodes.at(3);
	const auto before = three.applied;
	const auto base = three.state()->applied;
	int refusals = 0;
	bool refuse = true;
	three.accept = [&](const std::vector<std::string>& image) { check(image.size() == boundary, "the validated image is the checkpoint's"); refusals += refuse; return !refuse; };
	c.isolated.clear();
	check(c.until([&] { return refusals == 2; }), "node 3 validates two transfers and refuses both");
	auto s = three.state();
	check(s && s->applied == base && three.applied == before && three.activations == 0, "a refused image keeps node 3's state: applied " + std::to_string(s ? s->applied : 0));
	refuse = false;
	check(c.until([&] { return three.activations > 0; }, 6000), "the leader's retry, validated, installs");
	check(three.validations >= 2 && three.activations == 1, "two validations, one activation");
	c.commit("after");
	auto* l = c.leader();
	check(l && three.applied == l->applied, "node 3's application state equals the leader's after the retry");
}

void stale() {
	Cluster c;
	lag(c);
	auto& three = c.nodes.at(3);
	auto dropped = three.driver->dropped(group);
	three.driver->validated(group, 999); three.driver->transfer(group, 1, std::string("RFTXFR01\x10", 9)); three.run();
	check(three.driver->dropped(group) == dropped + 2, "a validation with no transfer, and a frame cut short, are dropped and counted");
	three.hold = true;
	c.isolated.clear();
	check(c.until([&] { return three.validation.has_value(); }), "node 3 validates");
	if (!three.validation) { return; }
	dropped = three.driver->dropped(group);
	three.driver->validated(group, three.validation->first + 1); three.run();
	check(three.driver->dropped(group) == dropped + 1 && three.activations == 0, "a stale token is dropped and counted");
	three.hold = false;
	check(c.until([&] { return three.activations > 0; }), "the current token installs");
}

void restart() {
	Cluster c;
	const Index boundary = lag(c);
	auto& three = c.nodes.at(3);
	three.hold = true;
	c.isolated.clear();
	check(c.until([&] { return three.validation.has_value(); }), "node 3 validates");
	c.restart(three);
	check(three.state() && three.state()->applied < boundary, "restarted between validation and its answer, node 3 holds the old state");
	three.hold = false;
	check(c.until([&] { return three.activations > 0; }, 6000), "the leader's retry installs on the restarted node");
	c.commit("after");
	auto* l = c.leader();
	check(l && three.applied == l->applied, "node 3's application state equals the leader's");
}

// A3: node 3's disk fails at its k-th operation after the heal, for every k through a whole
// install (or every stride-th); the node then loses power, restarts and recovers.
std::uint64_t stride = 1;
void crash() {
	std::uint64_t span = 0;
	{
		Cluster c;
		lag(c);
		auto& three = c.nodes.at(3);
		const auto from = three.disk.operations;
		c.isolated.clear();
		check(c.until([&] { return three.activations > 0; }), "the uncut install");
		c.commit("after");
		span = three.disk.operations - from;
	}
	std::uint64_t cuts = 0, old_replica = 0, new_replica = 0, resumed = 0;
	for (std::uint64_t k = 1; k <= span && !failures; k += stride) {
		Cluster c;
		const Index boundary = lag(c);
		auto& three = c.nodes.at(3);
		const Index before = three.state()->applied;  // node 3's checkpoint, which it restores
		three.disk.cut = three.disk.operations + k;
		c.isolated.clear();
		c.until([&] { return three.disk.dead() || (three.activations > 0 && three.state() && three.state()->applied > boundary); });
		if (!three.disk.dead()) { continue; }  // the install finished first
		++cuts;
		const bool activated = three.active == boundary;
		c.restart(three, true);
		auto s = three.state();
		const Index restored = s ? s->applied : 0;
		check(restored == before || restored == boundary, "a cut at operation " + std::to_string(k) + " recovers the old replica (" + std::to_string(before) + ") or the new one (" + std::to_string(boundary) + "), not " + std::to_string(restored));
		old_replica += restored == before; new_replica += restored == boundary;
		resumed += restored == boundary && !activated;  // durable, not yet activated: recovery activates it
		c.commit("after");
		auto* l = c.leader();
		check(l && three.applied == l->applied, "after the cut at operation " + std::to_string(k) + " node 3's application state equals the leader's");
	}
	check(cuts > 0 && old_replica > 0 && new_replica > 0 && resumed > 0, "cuts land before the install is durable, after it, and between its publish and activated()");
	std::cerr << "crash: " << span << " operations, " << cuts << " cuts: " << old_replica << " old replicas, " << new_replica << " new (" << resumed << " activated at recovery)\n";
}

// R1: a resting follower's orphaned image ends at its snapshot_timeout, with no frame or call to
// step its group.
void orphan() {
	quiesce = true;
	Cluster c;
	auto& three = c.nodes.at(3);
	for (int k = 0; k < 3; ++k) { c.commit("before" + std::to_string(k)); }
	check(c.checkpoint(three), "node 3 checkpoints before it is cut off");
	c.isolate(3);
	for (int k = 0; k < 12; ++k) { c.commit("during" + std::to_string(k)); }
	auto* l = c.leader();
	check(l && l->id != 3, "a leader among 1 and 2");
	if (!l) { return; }
	const NodeId gone = l->id;
	check(c.checkpoint(*l), "the leader checkpoints; the other member keeps its log");
	int sent = 0;
	c.drop = [&](const Packet& p) { return p.from == gone && p.to == 3 && std::holds_alternative<std::string>(p.body) && ++sent > 2; };
	c.isolated.clear();
	check(c.until([&] { return sent > 2; }), "the leader's transfer reaches node 3 through its first chunk");
	c.isolate(gone);
	c.until([&] { auto* x = c.leader(); return x != nullptr; });
	const auto placed = c.commit("after");
	check(three.applied.size() >= placed.index, "node 3 catches up from the new leader's log");
	c.isolated.clear();
	const bool rest = c.until([&] { for (auto& [id, n] : c.nodes) { auto s = n.state(); if (!s || !s->resting) { return false; } } return true; });
	check(rest, "the group rests on all three");
	check(three.driver->armed_group_timers() == 1, "resting, node 3 arms its incoming image's receive timeout");
	const auto from = c.now;
	check(c.until([&] { return three.driver->armed_group_timers() == 0; }, int(timing.snapshot_timeout / 5) + 200), "node 3 drops the orphaned image and rests with no timer");
	check(c.now - from <= timing.snapshot_timeout + 50, "within its snapshot_timeout: " + std::to_string(c.now - from) + " ms");
	const LogBoundary b{three.applied.size(), c.terms[three.applied.size()]};
	bool repaired = false; std::exception_ptr error;
	task::spawn(task::scheduler_ref::of(three.scheduler), three.driver->repair(group, b), [&](std::exception_ptr e, std::optional<RepairCompleted>) { repaired = true; error = e; });
	three.run();
	check(c.until([&] { return repaired; }, 20) && !error, "node 3 then repairs at once");
	quiesce = false;
}

const std::map<std::string, void (*)()> cases{
	{"lagging", &lagging},
	{"rejected", &rejected},
	{"stale", &stale},
	{"restart", &restart},
	{"crash", &crash},
	{"orphan", &orphan},
};

}  // namespace

int main(int argc, char** argv) {
	if (argc > 1) {
		auto found = cases.find(argv[1]);
		if (found == cases.end()) { std::cerr << "unknown case " << argv[1] << '\n'; return 2; }
		std::cerr << "case " << argv[1] << '\n';
		if (argc > 2) { stride = std::max<std::uint64_t>(1, std::strtoull(argv[2], nullptr, 10)); }
		found->second();
	} else {
		for (const auto& [name, run] : cases) { std::cerr << "case " << name << '\n'; run(); }
	}
	std::cout << checks << " install checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
