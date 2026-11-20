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

// The consensus Driver (docs/DRIVER.md): one per reactor loop, stepping each group placed on
// the loop only when it has work (an input, a completed IO, a due timer on the loop's wheel).
// Callers await task calls; what no caller awaits goes to the host's Sink. consensus::run is
// the blocking entry over the same Driver.

#pragma once

#include "worker.h"
#include "journal/blocking.h"

#include <atomic>
#include <concepts>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_map>
#include <variant>
#include <vector>

namespace kronuz::consensus {

using GroupId = std::uint64_t;

struct Placement {
	Term term = 0;
	Index index = 0;
	bool operator==(const Placement&) const = default;
};

struct closed : std::runtime_error { closed() : std::runtime_error("consensus group closed") {} };
struct fenced : std::runtime_error { using std::runtime_error::runtime_error; };
struct rejected : std::runtime_error {
	explicit rejected(RejectReason why) : std::runtime_error("consensus call rejected"), reason(why) {}
	RejectReason reason;
};
struct superseded : std::runtime_error { superseded() : std::runtime_error("another term's entry committed at the placement") {} };
struct compacted : std::runtime_error { compacted() : std::runtime_error("the placement is below the retained log") {} };
struct transfer_failed : std::runtime_error { transfer_failed() : std::runtime_error("leadership transfer failed") {} };
struct needs_peer : std::runtime_error { needs_peer() : std::runtime_error("the call needs another voting member") {} };
struct close_in_doubt : std::runtime_error { using std::runtime_error::runtime_error; };
using stalled = kronuz::journal::stalled;

struct GroupOptions {
	FixedConfiguration configuration;
	std::optional<Membership> founding{};
	kronuz::journal::AdmissionLimits admission;
	Limits limits{};
	Timing timing{};
	WorkerLimits scheduling{};
	std::optional<SnapshotLimits> snapshots{};
	std::size_t inbox = 256;
};

struct Seams {
	task::scheduler_ref scheduler;
	kronuz::journal::completion_ref io;
	kronuz::journal::Host& host;
	std::uint32_t loop = 0;
	std::uint64_t seed = 0;
};

template <class S>
concept sink = requires(S& s, GroupId g) {
	s(g, std::declval<Send>());
	s(g, std::declval<Committed>());
	s(g, std::declval<RoleChanged>());
	s(g, std::declval<ActivateInstall>());
	s(g, std::declval<CheckpointPublished>());
	s(g, std::declval<RepairCompleted>());
	s(g, std::declval<SnapshotNeeded>());
	s(g, std::declval<SnapshotTransmit>());
	s(g, std::declval<SnapshotReleased>());
	s(g, std::declval<Fenced>());
};

template <class C>
concept capture = requires(C& c, std::span<char> chunk) {
	{ c.metadata() } -> std::convertible_to<CaptureMetadata>;
	{ c.read(chunk) } -> std::convertible_to<std::size_t>;
};

namespace driver_detail {

enum class Kind : std::uint8_t { open, propose, committed, read, transfer, repair, checkpoint, close };

// A caller suspended on a group, in the caller's frame (docs/DRIVER.md, "Waiters"). The loop
// links it and completes it; a token's callback, on any thread, claims it and hops to the loop,
// which unlinks it before the resume. Only the claim's winner posts the caller's resume.
struct Waiter {
	enum : int { linked = 0, completed = 1, cancelled = 2 };
	bool claim(int outcome) noexcept { int expected = linked; return state.compare_exchange_strong(expected, outcome, std::memory_order_acq_rel); }
	std::atomic<int> state{linked};
	std::coroutine_handle<> handle;
	task::scheduler_ref home;
	task::scheduler_ref owner_scheduler;  // the loop: where a cancel hops; set before the callback registers
	void* owner = nullptr;      // the Group while linked
	void (*hop_to)(void* owner, Waiter&) noexcept = nullptr;  // the Driver's cancel on the loop
	Waiter *prev = nullptr, *next = nullptr;
	Kind kind = Kind::open;
	bool admitted = false;      // given to the Worker
	RequestId request = 0;
	std::optional<Event> event;  // what admission submits
	Placement placement;        // propose's result; committed's target
	NodeId target = 0;          // transfer
	std::optional<Index> read_index;  // read: confirmed, waiting for the apply
	std::optional<RepairCompleted> repaired;
	std::optional<CheckpointPublished> published;
	std::exception_ptr error;
	// checkpoint: the capture, type-erased, and the chunk read ahead
	void* capture = nullptr;
	CaptureMetadata (*metadata)(void*) = nullptr;
	std::size_t (*read)(void*, std::span<char>) = nullptr;
	std::uint64_t cap = 0;
	std::optional<CheckpointId> checkpoint;
	std::vector<char> chunk, ahead;
	bool chunk_final = false, offered_final = false, chunk_loaded = false;
};

}  // namespace driver_detail

template <class Sink>
class Driver;

template <class Sink, class T>
T run(Driver<Sink>& driver, task::blocking_scheduler& scheduler, task::task<T> work);

template <class Sink>
class Driver {
	using Waiter = driver_detail::Waiter;
	using Kind = driver_detail::Kind;

public:
	struct GroupState { Role role; Term term; NodeId leader; Index committed, applied; Membership membership; };

	Driver(Seams seams, Sink& sink)
		: scheduler_(seams.scheduler), io_(seams.io), host_(seams.host), turns_(seams.host.driver(seams.loop)), loop_(seams.loop), sink_(sink),
		  random_(seams.seed ? seams.seed : std::random_device{}()) {
		if (!scheduler_ || !io_) { throw std::invalid_argument("a Driver needs a scheduler and a completion driver"); }
	}
	Driver(const Driver&) = delete;
	Driver& operator=(const Driver&) = delete;
	~Driver() {
		for (auto& [id, group] : groups_) { disarm(*group); }
	}

	// The one long-lived frame (D1): steps ready groups, a bounded number per resume, then
	// yields. On `stop`, shuts the loop down (D8 step 1) and returns once no IO is in flight.
	task::task<void> run(task::cancel_token stop = {}) {
		if (holder_ != Holder::none) { throw std::logic_error("the Driver is already running"); }
		holder_ = Holder::reactor;
		struct Release { Driver& d; ~Release() { d.holder_ = Holder::none; } } release{*this};
		co_await loop(std::move(stop));
	}

	// Plain calls, on the loop.
	void open(GroupId id, GroupOptions options) {
		on_loop();
		if (groups_.contains(id)) { throw std::logic_error("the group is open on this Driver"); }
		if (host_.loop(id) != loop_) { throw std::invalid_argument("the group is placed on another loop"); }
		auto group = std::make_unique<Group>();
		group->driver = this; group->id = id; group->fire = &Driver::fired;
		group->timing = options.timing; group->inbox_limit = options.inbox;
		group->worker = std::make_unique<Worker>(host_, id, std::move(options.configuration), options.admission, options.limits, options.timing, options.scheduling, options.snapshots);
		if (options.founding) { group->founding = std::make_unique<Membership>(std::move(*options.founding)); }
		groups_.emplace(id, std::move(group));
	}
	void receive(GroupId id, Receive message) {
		on_loop();
		auto* g = live(id);
		if (!g || g->closing) { count_drop(id); return; }
		if (g->inbox.size() >= g->inbox_limit) { ++g->dropped; return; }
		g->inbox.push_back(std::move(message)); mark_ready(*g);
	}
	void applied(GroupId id, Applied completion) {
		on_loop();
		auto* g = live(id);
		if (!g) { count_drop(id); return; }
		g->applied = completion; mark_ready(*g);
	}
	void application_failed(GroupId id, Index through, std::string reason) {
		on_loop();
		auto* g = live(id);
		if (!g) { count_drop(id); return; }
		g->application_failure.emplace(through, std::move(reason)); mark_ready(*g);
	}
	void activated(GroupId id, Token token) {
		on_loop();
		auto* g = live(id);
		if (!g) { count_drop(id); return; }
		g->activation.emplace(token, std::nullopt); mark_ready(*g);
	}
	void activation_failed(GroupId id, Token token, std::string reason) {
		on_loop();
		auto* g = live(id);
		if (!g) { count_drop(id); return; }
		g->activation.emplace(token, std::move(reason)); mark_ready(*g);
	}
	void peer_limits(GroupId id, PeerLimits limits) {
		on_loop();
		auto* g = live(id);
		if (!g || g->closing) { count_drop(id); return; }
		g->limits.push_back(limits); mark_ready(*g);
	}
	// An open group's own count; an id not open (closed, never opened) reads the count of plain
	// calls dropped on every id not open.
	std::uint64_t dropped(GroupId id) const {
		auto found = groups_.find(id);
		return found == groups_.end() ? dropped_unknown_ : found->second->dropped;
	}
	std::optional<GroupState> state(GroupId id) const {
		auto found = groups_.find(id);
		if (found == groups_.end()) { return std::nullopt; }
		const auto& w = *found->second->worker;
		return GroupState{w.role(), w.term(), w.leader(), w.committed(), w.applied_index(), w.membership().value_or(Membership{})};
	}
	// The groups whose wheel entry is armed: group timers, which the blocking entry never
	// counts as a reason to wait (D5 step 4).
	std::size_t armed_group_timers() const {
		std::size_t armed = 0;
		for (const auto& [id, group] : groups_) { armed += group->armed(); }
		return armed;
	}
	// The groups that hold a waiter, in order: what a stalled blocking entry names.
	std::vector<std::uint64_t> waiting_groups() const {
		std::vector<std::uint64_t> ids;
		for (const auto& [id, group] : groups_) { if (group->head) { ids.push_back(id); } }
		std::sort(ids.begin(), ids.end());
		return ids;
	}
	// The rows the Driver holds: each open, created or closing group, and none once closed.
	std::size_t groups() const noexcept { return groups_.size(); }
	// IO tasks and turn watches in flight: the only frames a group holds besides its callers'.
	std::size_t in_flight() const noexcept { return in_flight_; }
	task::scheduler_ref scheduler() const noexcept { return scheduler_; }
	kronuz::journal::TurnDriver& turns() noexcept { return turns_; }

	// Task calls.
	task::task<void> create(GroupId id, kronuz::journal::Identity identity, task::cancel_token token = {}) {
		Call call(*this, Kind::open, std::move(token));
		co_await call.enter();
		if (call.begin(id, false) && !call.resume_open()) {
			try {
				if (call.group->founding) { call.group->worker->found(std::move(*call.group->founding)); call.group->founding.reset(); }
				call.group->worker->create(identity); call.group->opened = true;
			}
			catch (...) { call.fail(opening_error(id)); }
		}
		co_await call;
	}
	template <class Restore>
	task::task<void> recover(GroupId id, Restore restore, task::cancel_token token = {}) {
		Call call(*this, Kind::open, std::move(token));
		co_await call.enter();
		if (call.begin(id, false) && !call.resume_open()) {
			try { call.group->worker->recover(std::move(restore)); call.group->opened = true; }
			catch (...) { call.fail(opening_error(id)); }
		}
		co_await call;
	}
	task::task<Placement> propose(GroupId id, std::string command, task::cancel_token token = {}) {
		Call call(*this, Kind::propose, std::move(token));
		co_await call.enter();
		if (call.begin(id, true)) { call.waiter.event = Propose{call.waiter.request, std::move(command)}; }
		co_await call;
		co_return call.waiter.placement;
	}
	task::task<void> committed(GroupId id, Placement placement, task::cancel_token token = {}) {
		Call call(*this, Kind::committed, std::move(token));
		co_await call.enter();
		call.waiter.placement = placement;
		call.waiter.admitted = true;  // nothing to submit: the commit index decides
		call.begin(id, true);
		co_await call;
	}
	task::task<Placement> change_membership(GroupId id, Membership next, task::cancel_token token = {}) {
		Call call(*this, Kind::propose, std::move(token));
		co_await call.enter();
		if (call.begin(id, true)) { call.waiter.event = ChangeMembership{call.waiter.request, std::move(next)}; }
		co_await call;
		co_return call.waiter.placement;
	}
	task::task<void> transfer_leadership(GroupId id, NodeId target, task::cancel_token token = {}) {
		Call call(*this, Kind::transfer, std::move(token));
		co_await call.enter();
		if (call.begin(id, true)) { call.waiter.target = target; call.waiter.event = TransferLeadership{call.waiter.request, target}; }
		co_await call;
	}
	task::task<RepairCompleted> repair(GroupId id, LogBoundary boundary, task::cancel_token token = {}) {
		Call call(*this, Kind::repair, std::move(token));
		co_await call.enter();
		if (call.begin(id, false)) { call.waiter.event = Repair{call.waiter.request, boundary}; }
		co_await call;
		co_return *call.waiter.repaired;
	}
	template <capture Capture>
	task::task<CheckpointPublished> checkpoint(GroupId id, std::uint64_t application_cap, Capture source, task::cancel_token token = {}) {
		Call call(*this, Kind::checkpoint, std::move(token));
		co_await call.enter();
		auto& w = call.waiter;
		w.capture = &source; w.cap = application_cap;
		w.metadata = [](void* c) -> CaptureMetadata { return static_cast<Capture*>(c)->metadata(); };
		w.read = [](void* c, std::span<char> out) -> std::size_t { return static_cast<Capture*>(c)->read(out); };
		call.begin(id, false);
		co_await call;
		co_return *w.published;
	}
	task::task<ReadReady> read(GroupId id, task::cancel_token token = {}) {
		Call call(*this, Kind::read, std::move(token));
		co_await call.enter();
		if (call.begin(id, true)) { call.waiter.event = Read{call.waiter.request}; }
		co_await call;
		co_return ReadReady{call.waiter.request, *call.waiter.read_index};
	}
	task::task<void> close(GroupId id) {
		Call call(*this, Kind::close, {});
		co_await call.enter();
		// After shutdown nothing steps the loop: close completes as every other call does.
		if (stopping_) { call.fail(std::make_exception_ptr(task::cancelled{})); co_await call; co_return; }
		auto found = groups_.find(id);
		if (found == groups_.end()) { co_await call.leave(); co_return; }
		auto& g = *found->second;
		call.waiter.admitted = true;
		call.link(g);
		if (!g.closing) { begin_close(g); }
		mark_ready(g);
		co_await call;
	}

private:
	template <class S, class T> friend T consensus::run(Driver<S>&, task::blocking_scheduler&, task::task<T>);
	enum class Holder : std::uint8_t { none, reactor, entry };

	struct Group : wheel::timer_entry {
		Driver* driver = nullptr;
		GroupId id = 0;
		std::unique_ptr<Worker> worker;
		// Rare state lives off the idle entry (Y9): create's founding roles, the install announced to the Sink.
		std::unique_ptr<Membership> founding;
		Timing timing{};
		Waiter *head = nullptr, *tail = nullptr;
		std::vector<Receive> inbox;
		std::size_t inbox_limit = 256;
		std::optional<Applied> applied;
		std::optional<std::pair<Index, std::string>> application_failure;
		std::optional<std::pair<Token, std::optional<std::string>>> activation;
		std::unique_ptr<SnapshotActivation> activating;
		std::vector<PeerLimits> limits;
		std::string fence_reason;
		Group* ready_next = nullptr;
		std::uint64_t dropped = 0;
		unsigned io = 0;  // IO tasks and turn watches in flight
		bool queued = false, opened = false, started = false, closing = false, retiring = false, closed = false, watching = false, fence_seen = false;
		bool witnessed = false;  // acknowledged a range as a witness's own: acknowledges every later one
	};

	// A call's frame state: its waiter, its token's callback, and the hops between the caller's
	// scheduler and the loop.
	struct Call {
		Call(Driver& d, Kind kind, task::cancel_token t) : driver(d), token(std::move(t)) { waiter.kind = kind; waiter.home = task::current(); }
		Call(const Call&) = delete;
		Call& operator=(const Call&) = delete;
		~Call() {
			callback.reset();  // waits for a callback running on another thread
			if (waiter.owner) { driver.unlink(*static_cast<Group*>(waiter.owner), waiter); }
		}
		// To the loop, unless already there.
		struct Enter {
			Call& call;
			bool await_ready() const noexcept { return task::current() == call.driver.scheduler_; }
			void await_suspend(std::coroutine_handle<> h) const { call.driver.scheduler_.post(h); }
			void await_resume() const {
				if (!call.waiter.home) { throw std::logic_error("a Driver call needs a running scheduler"); }
			}
		};
		Enter enter() { return {*this}; }
		// Back to the caller's scheduler, for a call that ends before it links.
		struct Leave {
			Call& call;
			bool await_ready() const noexcept { return call.waiter.home == call.driver.scheduler_; }
			void await_suspend(std::coroutine_handle<> h) const { call.waiter.home.post(h); }
			void await_resume() const noexcept {}
		};
		Leave leave() { return {*this}; }
		// On the loop: finds the group and links the waiter; false (and the waiter already
		// failed) when the call ends here.
		bool begin(GroupId id, bool needs_peer_check) {
			if (token.requested()) { return fail(std::make_exception_ptr(task::cancelled{})); }
			if (driver.stopping_) { return fail(std::make_exception_ptr(task::cancelled{})); }
			auto* g = driver.live(id);
			if (!g || g->closing) { return fail(std::make_exception_ptr(closed{})); }
			if (g->worker->fenced()) { return fail(std::make_exception_ptr(fenced(driver.fence_reason(*g)))); }
			if (needs_peer_check && driver.holder_ == Holder::entry && driver.has_peer(*g)) { return fail(std::make_exception_ptr(needs_peer{})); }
			if (waiter.kind != Kind::open && waiter.kind != Kind::committed) { waiter.request = driver.next_request(); }
			link(*g);
			return true;
		}
		// create/recover on a group already opened (X15): waits for it to be ready, or ends now.
		bool resume_open() {
			if (!group->opened) { return false; }
			if (group->started) { fail({}); }
			return true;
		}
		bool fail(std::exception_ptr error) {
			waiter.error = std::move(error);
			if (waiter.owner) { driver.unlink(*static_cast<Group*>(waiter.owner), waiter); }
			failed = true;
			return false;
		}
		void link(Group& g) { group = &g; driver.link(g, waiter); driver.mark_ready(g); }

		bool await_ready() const noexcept { return false; }
		bool await_suspend(std::coroutine_handle<> h) {
			if (failed) {
				// Back to the caller's scheduler, then rethrow there.
				if (waiter.home == driver.scheduler_) { return false; }
				waiter.home.post(h);
				return true;
			}
			waiter.handle = h;
			callback.emplace(token, Cancel{&waiter});
			return true;
		}
		void await_resume() {
			if (waiter.state.load(std::memory_order_acquire) == Waiter::cancelled) { throw task::cancelled{}; }
			if (waiter.error) { std::rethrow_exception(waiter.error); }
		}

		struct Cancel {
			Waiter* waiter;
			void operator()() const noexcept {
				if (!waiter->claim(Waiter::cancelled)) { return; }
				// The waiter's owner is read on the loop, in the hop: only the loop writes it.
				if (!driver_of(waiter).post_call(&hop, waiter)) { hop(waiter); }
			}
			static task::scheduler_ref driver_of(Waiter* w) noexcept { return w->owner_scheduler; }
			static void hop(void* raw) {
				auto* w = static_cast<Waiter*>(raw);
				if (w->owner) { w->hop_to(w->owner, *w); }
				w->home.post(w->handle);
			}
		};

		Driver& driver;
		task::cancel_token token;
		Waiter waiter;
		Group* group = nullptr;
		bool failed = false;
		std::optional<task::cancel_callback<Cancel>> callback;
	};

	// The run frame's wait for a ready group, with the same claim protocol.
	struct Idle {
		Driver& driver;
		task::cancel_token stop;
		Waiter waiter;
		std::optional<task::cancel_callback<typename Call::Cancel>> callback;
		Idle(Driver& d, task::cancel_token s) : driver(d), stop(std::move(s)) { waiter.home = d.scheduler_; waiter.owner_scheduler = d.scheduler_; }
		~Idle() {
			callback.reset();
			if (driver.sleeper_ == &waiter) { driver.sleeper_ = nullptr; }
		}
		bool await_ready() const noexcept { return driver.ready_head_ || stop.requested(); }
		void await_suspend(std::coroutine_handle<> h) {
			waiter.handle = h; waiter.owner = &driver; waiter.hop_to = [](void* owner, Waiter& w) noexcept {
				auto& d = *static_cast<Driver*>(owner);
				if (d.sleeper_ == &w) { d.sleeper_ = nullptr; }
				w.owner = nullptr;
			};
			driver.sleeper_ = &waiter;
			callback.emplace(stop, typename Call::Cancel{&waiter});
		}
		void await_resume() const noexcept {}
	};

	// A throw that ends the loop (one step() does not catch, as a power cut's) ends it for its
	// callers as a stop does: each completes with task::cancelled, none is left suspended.
	task::task<void> loop(task::cancel_token stop) {
		try {
			for (;;) {
				if (stop.requested()) {
					shutdown();
					if (!in_flight_) { co_return; }
				}
				if (ready_head_) {
					pump();
					co_await task::yield();
					continue;
				}
				// Shutting down, only landing IO wakes the loop.
				co_await Idle(*this, stopping_ ? task::cancel_token{} : stop);
			}
		} catch (...) { shutdown(); throw; }
	}

	void on_loop() const {
		assert((!task::current() || task::current() == scheduler_) && "a Driver's plain calls run on its loop");
	}
	Group* live(GroupId id) {
		auto found = groups_.find(id);
		return found == groups_.end() ? nullptr : found->second.get();
	}
	void count_drop(GroupId id) {
		auto found = groups_.find(id);
		if (found != groups_.end()) { ++found->second->dropped; } else { ++dropped_unknown_; }
	}
	RequestId next_request() noexcept { return ++requests_; }
	std::string fence_reason(const Group& g) const { return g.fence_reason.empty() ? "consensus group fenced" : g.fence_reason; }
	// D5: a voting member (voter or witness) other than this node.
	bool has_peer(const Group& g) const {
		const auto& w = *g.worker;
		auto membership = w.membership();
		if (!membership) {
			for (auto voter : w.configuration().voters) { if (voter != w.configuration().local) { return true; } }
			return false;
		}
		for (const auto& member : membership->members) {
			if (voting(member.role) && member.incarnation != w.configuration().local) { return true; }
		}
		return false;
	}

	// This node's role in the membership in force at `index`; none when it is not a member.
	std::optional<MemberRole> role_at(const Group& g, Index index) const {
		const auto& w = *g.worker;
		auto membership = w.membership_at(index);
		const auto* self = membership ? find_member(*membership, w.configuration().local) : nullptr;
		return self ? std::optional<MemberRole>(self->role) : std::nullopt;
	}
	// Whether a Committed range is a witness's own (D9): this node is a witness at the range's last
	// index (committed, so no term truncates it), or was one at the apply index and has left since.
	// A witness incarnation never applies again, so every range after its first is its own too.
	bool witness_range(Group& g, const Committed& range) {
		if (g.witnessed) { return true; }
		auto role = role_at(g, range.entries.back().index);
		if (!role) { role = role_at(g, range.first - 1); }
		g.witnessed = role == MemberRole::Witness;
		return g.witnessed;
	}

	void link(Group& g, Waiter& w) noexcept {
		w.owner = &g; w.owner_scheduler = scheduler_; w.hop_to = &Driver::cancelled_on_loop;
		w.prev = g.tail; w.next = nullptr;
		(g.tail ? g.tail->next : g.head) = &w;
		g.tail = &w;
	}
	void unlink(Group& g, Waiter& w) noexcept {
		(w.prev ? w.prev->next : g.head) = w.next;
		(w.next ? w.next->prev : g.tail) = w.prev;
		w.owner = nullptr; w.prev = w.next = nullptr;
	}
	// Completion, on the loop: unlinks, then claims; the winner posts the caller's resume.
	void complete(Group& g, Waiter& w, std::exception_ptr error = {}) noexcept {
		unlink(g, w);
		if (!w.claim(Waiter::completed)) { return; }
		w.error = std::move(error);
		w.home.post(w.handle);
	}
	// A cancel's hop, on the loop, while the waiter is still linked.
	static void cancelled_on_loop(void* owner, Waiter& w) noexcept {
		auto& g = *static_cast<Group*>(owner);
		auto& d = *g.driver;
		if (w.kind == Kind::checkpoint && w.checkpoint && g.worker) {
			// D8: before publication it is discarded; after, it finishes and the Sink hears.
			try { (void)g.worker->cancel_checkpoint(*w.checkpoint); } catch (...) {}
		}
		d.unlink(g, w);
		d.mark_ready(g);
	}

	void mark_ready(Group& g) noexcept {
		if (!g.queued) {
			g.queued = true; g.ready_next = nullptr;
			(ready_tail_ ? ready_tail_->ready_next : ready_head_) = &g;
			ready_tail_ = &g;
		}
		if (sleeper_ && sleeper_->claim(Waiter::completed)) {
			auto* s = std::exchange(sleeper_, nullptr);
			s->owner = nullptr;
			scheduler_.post(s->handle);
		}
	}
	static void fired(wheel::timer_entry& entry, bool expired) noexcept {
		if (!expired) { return; }
		auto& g = static_cast<Group&>(entry);
		g.driver->mark_ready(g);
	}
	void disarm(Group& g) noexcept { if (g.armed()) { scheduler_.cancel(g); } }

	// The ticks Core sees: milliseconds on the loop's clock, and a fresh election jitter.
	Tick tick(const Group& g) {
		const auto span = g.timing.election_max - g.timing.election_min + 1;
		return Tick{scheduler_.now() / 1'000'000, g.timing.election_min + random_() % span};
	}

	void pump() {
		for (unsigned steps = 0; ready_head_ && steps < group_budget; ++steps) {
			auto& g = *ready_head_;
			ready_head_ = g.ready_next;
			if (!ready_head_) { ready_tail_ = nullptr; }
			g.queued = false; g.ready_next = nullptr;
			step(g);
			if (g.closed) { erase(g); host_.settle(loop_); }
		}
	}
	// create's or recover's failure. The host's hold refuses a group a failed close left in
	// doubt with EIO before any IO, holding nothing; an EIO from later IO leaves the hold held.
	std::exception_ptr opening_error(GroupId id) {
		auto error = std::current_exception();
		try { std::rethrow_exception(error); }
		catch (const std::system_error& e) {
			if (e.code() == std::errc::io_error && !host_.holding(id)) { return std::make_exception_ptr(close_in_doubt(e.what())); }
		} catch (...) {}
		return error;
	}
	// A closed group's node goes once nothing refers to it: close_step disarmed it, and it has
	// no waiter or IO; only the ready list can still link it. Its Worker is gone, so its hold
	// on the group ended: settle() releases what the host still keeps of it.
	void erase(Group& g) {
		if (g.queued) {
			Group* before = nullptr;
			for (auto* x = ready_head_; x != &g; x = x->ready_next) { before = x; }
			(before ? before->ready_next : ready_head_) = g.ready_next;
			if (ready_tail_ == &g) { ready_tail_ = before; }
		}
		groups_.erase(g.id);
	}

	// One group's step: delivers its inputs and admits its calls, runs the Worker until it
	// reports no progress (a bounded number of turns, then the group is queued again), hands
	// any IO to the completion seam, dispatches actions, resolves waiters, and re-arms its
	// wheel entry at Core's next deadline.
	void step(Group& g) {
		if (g.closed || !g.worker) { return; }
		if (!g.opened) {  // created or recovered by neither call yet: only close moves it
			if (g.closing) { close_step(g); }
			return;
		}
		auto& w = *g.worker;
		bool exhausted = true;
		try {
			for (unsigned turn = 0; turn < turn_budget; ++turn) {
				bool progress = deliver(g);
				const auto result = w.run_one(tick(g));
				progress |= result == TurnResult::Inventory || result == TurnResult::Stored || result == TurnResult::Completed || result == TurnResult::Maintenance;
				progress |= result == TurnResult::Idle && w.runnable();  // the Tick left work behind it
				progress |= drain(g);
				// What admission submits answers in the same turn (a Reject before settle()).
				if (admit(g)) { progress = true; drain(g); }
				const bool was_retiring = g.retiring;
				settle(g);
				if (g.closed) { return; }
				progress |= g.retiring && !was_retiring;  // the retirement turns follow at once
				if (!progress) { exhausted = false; break; }
			}
		} catch (const std::exception& error) {
			// A throw mid-step (a Sink's included) leaves its batch half delivered: the group fences.
			if (g.fence_reason.empty()) { g.fence_reason = error.what(); }
			w.fence(g.fence_reason);
			fail_all(g, std::make_exception_ptr(fenced(fence_reason(g))));
		}
		if (g.closed) { return; }
		if (w.awaiting_turn() && !g.watching && !stopping_) { watch(g); }  // after shutdown, never again: D8
		if (exhausted) { mark_ready(g); return; }
		arm(g);
	}

	// The Worker's actions to their waiters or the Sink, and its IO to the completion seam.
	bool drain(Group& g) {
		auto& w = *g.worker;
		auto actions = w.take_actions();
		bool progress = !actions.empty();
		dispatch(g, std::move(actions));
		while (auto operation = w.take_io_operation()) { launch(g, std::move(operation)); progress = true; }
		return progress;
	}

	// The group's next wake on the wheel: Core's deadline, or a short retry while inputs or
	// calls wait for an admission nothing in flight will free.
	void arm(Group& g) {
		auto& w = *g.worker;
		std::uint64_t at = w.next_deadline();
		at = at > max_ms ? never : at * 1'000'000;
		const bool waiting = !g.inbox.empty() || !g.limits.empty() || unadmitted(g) || g.retiring;
		if (waiting && !g.io && !g.watching) { at = std::min(at, scheduler_.now() + retry_ns); }
		if (at == never) { disarm(g); return; }
		scheduler_.arm(g, at);
	}
	bool unadmitted(const Group& g) const {
		for (auto* w = g.head; w; w = w->next) { if (!w->admitted && w->kind != Kind::open) { return true; } }
		return false;
	}

	// The plain calls' inputs, in arrival order of kind; true if any reached the Worker.
	bool deliver(Group& g) {
		auto& w = *g.worker;
		bool progress = false;
		if (g.applied) { w.applied(*g.applied); g.applied.reset(); progress = true; }
		if (g.application_failure) { w.application_failed(g.application_failure->first, std::move(g.application_failure->second)); g.application_failure.reset(); progress = true; }
		if (g.activation && g.activating) {
			auto [token, failure] = std::move(*g.activation); g.activation.reset();
			if (token == g.activating->token) {
				if (failure) { w.snapshot_activation_failed(g.activating->snapshot, token); } else { w.snapshot_activated(g.activating->snapshot, token); }
				g.activating.reset();
			} else { ++g.dropped; }
			progress = true;
		} else if (g.activation) { g.activation.reset(); ++g.dropped; }
		return progress;
	}

	// Submits what waits for admission: received messages first, then calls in order. Stops
	// at the first the Worker refuses for now.
	bool admit(Group& g) {
		auto& w = *g.worker;
		if (!g.started) {
			if (!g.opened || !w.ready()) { return false; }
			// D5/X5: a Tick first, so Start sees the loop's clock, then Start.
			if (w.try_submit(tick(g)) != SubmitResult::Accepted || w.try_submit(Start{}) != SubmitResult::Accepted) { return false; }
			g.started = true;
			for (auto* x = g.head; x;) { auto* next = x->next; if (x->kind == Kind::open) { complete(g, *x); } x = next; }
			return true;
		}
		bool progress = false;
		while (!g.closing && !g.limits.empty()) {
			Event event = g.limits.front();
			if (w.try_submit_retained(event) != SubmitResult::Accepted) { break; }
			g.limits.erase(g.limits.begin()); progress = true;
		}
		std::size_t taken = 0;
		while (!g.closing && taken < g.inbox.size()) {
			Event event = std::move(g.inbox[taken]);
			SubmitResult result;
			try { result = w.try_submit_retained(event); }
			catch (const std::exception&) { ++g.dropped; ++taken; continue; }  // a malformed message
			if (result == SubmitResult::Busy || result == SubmitResult::Pressure) { g.inbox[taken] = std::move(std::get<Receive>(event)); break; }
			++taken; progress = true;
		}
		g.inbox.erase(g.inbox.begin(), g.inbox.begin() + static_cast<std::ptrdiff_t>(taken));
		if (g.inbox.empty()) { std::vector<Receive>().swap(g.inbox); }  // an idle group holds no buffer
		for (auto* x = g.head; x && !g.closing;) {
			auto* next = x->next;
			// A waiter its token claimed is unlinked by the hop on its way: never submitted.
			if (x->admitted || x->kind == Kind::open || x->kind == Kind::close || x->state.load(std::memory_order_acquire) != Waiter::linked) { x = next; continue; }
			if (x->kind == Kind::checkpoint) {
				if (!reserve(g, *x)) { break; }
				progress = true; x = next; continue;
			}
			if (holder_ == Holder::entry && x->kind != Kind::repair && has_peer(g)) { complete(g, *x, std::make_exception_ptr(needs_peer{})); x = next; continue; }
			SubmitResult result;
			try { result = w.try_submit_retained(*x->event); }
			catch (...) { complete(g, *x, std::current_exception()); x = next; continue; }
			if (result == SubmitResult::Busy || result == SubmitResult::Pressure) { break; }
			if (result == SubmitResult::Fenced) { break; }  // settle() fails every waiter
			x->admitted = true; x->event.reset(); progress = true;
			x = next;
		}
		for (auto* x = g.head; x; x = x->next) {
			if (x->kind == Kind::checkpoint && x->checkpoint) { progress |= offer(g, *x); }
		}
		return progress;
	}

	// checkpoint: reserves, attaches the capture's metadata, and starts reading its chunks.
	bool reserve(Group& g, Waiter& x) {
		auto& w = *g.worker;
		std::optional<CheckpointId> id;
		try { id = w.reserve_checkpoint(x.cap); } catch (...) { complete(g, x, std::current_exception()); return true; }
		if (!id) { return false; }
		try {
			auto metadata = x.metadata(x.capture);
			metadata.request = x.request;
			w.attach_capture(*id, metadata);
		} catch (...) {
			try { (void)w.cancel_checkpoint(*id); } catch (...) {}
			complete(g, x, std::current_exception()); return true;
		}
		x.checkpoint = id; x.admitted = true;
		x.chunk.resize(chunk_bytes); x.ahead.resize(chunk_bytes);
		return true;
	}
	// Offers the next chunk: it reads one ahead to know which is final.
	bool offer(Group& g, Waiter& x) {
		auto& w = *g.worker;
		if (x.offered_final || !w.checkpoint_active(*x.checkpoint)) { return false; }
		try {
			if (!x.chunk_loaded) {
				const auto first = x.read(x.capture, std::span<char>(x.chunk.data(), x.chunk.size()));
				x.chunk.resize(first); x.chunk_loaded = true;
				const auto next = first ? x.read(x.capture, std::span<char>(x.ahead.data(), x.ahead.size())) : 0;
				x.ahead.resize(next); x.chunk_final = next == 0;
			}
			const auto result = w.offer_application_chunk(*x.checkpoint, std::string_view(x.chunk.data(), x.chunk.size()), x.chunk_final);
			if (result != SubmitResult::Accepted) { return false; }
			if (x.chunk_final) { x.offered_final = true; std::vector<char>().swap(x.chunk); std::vector<char>().swap(x.ahead); return true; }
			std::swap(x.chunk, x.ahead);
			x.ahead.resize(chunk_bytes);
			const auto next = x.read(x.capture, std::span<char>(x.ahead.data(), x.ahead.size()));
			x.ahead.resize(next); x.chunk_final = next == 0;
			return true;
		} catch (...) {
			try { (void)w.cancel_checkpoint(*x.checkpoint); } catch (...) {}
			complete(g, x, std::current_exception());
			return true;
		}
	}

	Waiter* find(Group& g, RequestId request) {
		if (!request) { return nullptr; }
		for (auto* x = g.head; x; x = x->next) { if (x->request == request && x->admitted) { return x; } }
		return nullptr;
	}

	void dispatch(Group& g, Actions actions) {
		for (auto& action : actions) {
			std::visit([&](auto&& value) {
				using T = std::decay_t<decltype(value)>;
				if constexpr (std::is_same_v<T, ProposalPlaced>) {
					if (auto* x = find(g, value.request)) { x->placement = {value.term, value.index}; complete(g, *x); }
				} else if constexpr (std::is_same_v<T, Reject>) {
					if (auto* x = find(g, value.request)) { complete(g, *x, std::make_exception_ptr(rejected(value.reason))); }
				} else if constexpr (std::is_same_v<T, ReadReady>) {
					if (auto* x = find(g, value.request)) { x->read_index = value.index; }
				} else if constexpr (std::is_same_v<T, RepairCompleted>) {
					if (auto* x = find(g, value.request)) { x->repaired = value; complete(g, *x); } else { sink_(g.id, std::move(value)); }
				} else if constexpr (std::is_same_v<T, CheckpointPublished>) {
					if (auto* x = find(g, value.request)) { x->published = value; complete(g, *x); } else { sink_(g.id, std::move(value)); }
				} else if constexpr (std::is_same_v<T, Fenced>) {
					g.fence_reason = value.reason;
					if (!g.fence_seen) { g.fence_seen = true; sink_(g.id, std::move(value)); }
				} else if constexpr (std::is_same_v<T, Committed>) {
					// A witness keeps the log but no application state: it acknowledges its own ranges.
					if (witness_range(g, value)) { g.applied = Applied{value.entries.back().index}; mark_ready(g); } else { sink_(g.id, std::move(value)); }
				} else if constexpr (std::is_same_v<T, Send> || std::is_same_v<T, RoleChanged> ||
					std::is_same_v<T, SnapshotNeeded> || std::is_same_v<T, SnapshotTransmit> || std::is_same_v<T, SnapshotReleased>) {
					sink_(g.id, std::move(value));
				}
			}, std::move(action));
		}
	}

	// Resolves the waiters the group's state now decides, and moves a closing group on.
	void settle(Group& g) {
		auto& w = *g.worker;
		if (w.fenced()) {
			if (!g.fence_seen) { g.fence_seen = true; sink_(g.id, Fenced{fence_reason(g)}); }
			fail_all(g, std::make_exception_ptr(fenced(fence_reason(g))));
			if (g.closed) { return; }
		}
		if (auto activation = w.snapshot_activation(); activation && (!g.activating || g.activating->token != activation->token)) {
			g.activating = std::make_unique<SnapshotActivation>(*activation);
			sink_(g.id, ActivateInstall{activation->token, 0, {activation->descriptor.through, activation->descriptor.term}});
		}
		for (auto* x = g.head; x;) {
			auto* next = x->next;
			if (x->kind == Kind::committed && w.committed() >= x->placement.index) {
				const auto term = w.term_at(x->placement.index);
				if (!term) { complete(g, *x, std::make_exception_ptr(compacted{})); }
				else if (*term != x->placement.term) { complete(g, *x, std::make_exception_ptr(superseded{})); }
				else { complete(g, *x); }
			} else if (x->kind == Kind::read && x->read_index && w.applied_index() >= *x->read_index) {
				complete(g, *x);
			} else if (x->kind == Kind::transfer && x->admitted) {
				if (w.role() != Role::Leader && w.leader() == x->target) { complete(g, *x); }
				else if ((w.role() != Role::Leader && w.leader() != 0) || (w.role() == Role::Leader && !w.transferring())) {
					complete(g, *x, std::make_exception_ptr(transfer_failed{}));
				}
			} else if (x->kind == Kind::checkpoint && x->checkpoint && !w.checkpoint_active(*x->checkpoint) && !x->published) {
				// Discarded without publishing (a capture the Core rejected, or a cancel).
				complete(g, *x, std::make_exception_ptr(rejected(RejectReason::InvalidCheckpoint)));
			}
			x = next;
		}
		if (g.closing) { close_step(g); }
	}

	void fail_all(Group& g, std::exception_ptr error) {
		for (auto* x = g.head; x;) {
			auto* next = x->next;
			if (x->kind != Kind::close) { complete(g, *x, error); }
			x = next;
		}
		if (g.closing) { close_step(g); }
	}

	// D8, close: refuses new calls and resolves with `closed` every waiter that holds no staged
	// record; the rest drain as they complete.
	void begin_close(Group& g) {
		g.closing = true;
		g.inbox.clear(); std::vector<Receive>().swap(g.inbox); g.limits.clear();
		for (auto* x = g.head; x;) {
			auto* next = x->next;
			if (!holds_record(g, *x)) { complete(g, *x, std::make_exception_ptr(closed{})); }
			x = next;
		}
	}
	bool holds_record(Group& g, Waiter& x) {
		switch (x.kind) {
		case Kind::close: return true;
		case Kind::propose: return x.admitted;
		case Kind::open: return g.io || g.watching || (g.worker && g.worker->awaiting_turn());  // its durable step
		case Kind::checkpoint: {
			if (!x.checkpoint || !g.worker) { return false; }
			CancelResult result = CancelResult::Stale;
			try { result = g.worker->cancel_checkpoint(*x.checkpoint); } catch (...) {}
			return result == CancelResult::TooLate;  // a started publication finishes
		}
		default: return false;
		}
	}
	void close_step(Group& g) {
		bool others = false;
		for (auto* x = g.head; x;) {
			auto* next = x->next;
			if (x->kind == Kind::open && !holds_record(g, *x)) { complete(g, *x, std::make_exception_ptr(closed{})); }
			else if (x->kind != Kind::close) { others = true; }
			x = next;
		}
		if (others) { return; }
		auto& w = *g.worker;
		if (!g.retiring) { g.retiring = true; w.close(); }
		if (!w.drained() || g.io || g.watching) { return; }
		disarm(g);
		g.worker.reset(); g.closed = true;
		for (auto* x = g.head; x;) { auto* next = x->next; complete(g, *x); x = next; }
	}

	// IO the machine issued for itself runs under the loop's shutdown token only (D8).
	void launch(Group& g, std::shared_ptr<kronuz::journal::IOOperation> operation) {
		++g.io; ++in_flight_;
		auto ran = io_.run(operation, shutdown_.token());
		task::spawn(scheduler_, std::move(ran), [this, &g, operation](std::exception_ptr error) {
			--g.io; --in_flight_;
			if (error && g.worker && !operation->done()) {
				std::string reason = "storage operation failed";
				try { std::rethrow_exception(error); } catch (const std::exception& e) { reason = e.what(); } catch (...) {}
				g.worker->storage_operation_failed(operation, std::move(reason));
			}
			mark_ready(g);
		});
	}
	// A shared append or publication completes at its turn's barrier: wait for the group's
	// next record to be durable there. Shutdown cancels the watch and step never starts
	// another, so the loop neither spins on it nor waits for a turn no one may write (D8).
	// A group reopened from the host's table numbers its records past what its closed
	// incarnation retired, which the log no longer counts.
	void watch(Group& g) {
		g.watching = true; ++g.io; ++in_flight_;
		const auto next = std::max(turns_.sequence(g.id), turns_.retired(g.id)) + 1;
		task::spawn(scheduler_, turns_.durable(g.id, next, shutdown_.token()), [this, &g](std::exception_ptr) {
			g.watching = false; --g.io; --in_flight_;
			mark_ready(g);
		});
	}

	// After a blocking entry's own stop: the Driver takes calls again.
	void rearm() { stopping_ = false; shutdown_ = task::cancel_source{}; }

	// D8 step 1: every waiter completes with task::cancelled (cancel_reason::shutdown); new
	// calls complete the same way; staged records are still written.
	void shutdown() {
		if (!stopping_) {
			stopping_ = true;
			shutdown_.request(task::cancel_reason::shutdown);
		}
		for (auto& [id, group] : groups_) {
			auto& g = *group;
			for (auto* x = g.head; x;) {
				auto* next = x->next;
				unlink(g, *x);
				if (x->claim(Waiter::cancelled)) { x->home.post(x->handle); }
				x = next;
			}
		}
	}

	static constexpr unsigned group_budget = 64, turn_budget = 16;
	static constexpr std::size_t chunk_bytes = 65536;
	static constexpr std::uint64_t never = wheel::timer_wheel::never, max_ms = (never - 1) / 1'000'000, retry_ns = 1'000'000;

	task::scheduler_ref scheduler_;
	kronuz::journal::completion_ref io_;
	kronuz::journal::Host& host_;
	kronuz::journal::TurnDriver& turns_;
	std::uint32_t loop_;
	Sink& sink_;
	std::mt19937_64 random_;
	std::unordered_map<GroupId, std::unique_ptr<Group>> groups_;
	Group *ready_head_ = nullptr, *ready_tail_ = nullptr;
	Waiter* sleeper_ = nullptr;
	task::cancel_source shutdown_;
	RequestId requests_ = 0;
	std::uint64_t dropped_unknown_ = 0;
	std::size_t in_flight_ = 0;
	Holder holder_ = Holder::none;
	bool stopping_ = false;
};

// The blocking entry (D5, docs/DRIVER.md): runs `work` and the Driver's loop on the calling
// thread over `scheduler`, holding the loop's turn driver, committing each staged turn inline.
template <class Sink, class T>
T run(Driver<Sink>& driver, task::blocking_scheduler& scheduler, task::task<T> work) {
	namespace jd = kronuz::journal::detail;
	if (!(driver.scheduler() == task::scheduler_ref::of(scheduler))) { throw std::logic_error("the Driver runs on another scheduler"); }
	if (driver.holder_ != Driver<Sink>::Holder::none) { throw std::logic_error("the Driver is already running"); }
	auto& turns = driver.turns();
	kronuz::journal::TurnDriver::Hold hold(turns, kronuz::journal::TurnDriver::Holder::entry, task::scheduler_ref::of(scheduler));
	driver.holder_ = Driver<Sink>::Holder::entry;
	struct Release { Driver<Sink>& d; ~Release() { d.holder_ = Driver<Sink>::Holder::none; } } release{driver};
	task::cancel_source stop;
	jd::EntryResult<void> loop_result;
	auto loop_root = jd::entry_root(driver.loop(stop.token()), loop_result);
	jd::EntryResult<T> result;
	auto root = jd::entry_root(std::move(work), result);
	struct Destroy { std::coroutine_handle<> handle; ~Destroy() { handle.destroy(); } } destroy_loop{loop_root.handle}, destroy{root.handle};
	auto ref = task::scheduler_ref::of(scheduler);
	ref.post(loop_root.handle);
	ref.post(root.handle);
	std::exception_ptr turn_error;
	// Waits only toward an operation deadline: group timers never count (step 4).
	auto may_wait = [&] { return scheduler.armed_timers() > driver.armed_group_timers(); };
	for (;;) {
		const bool settled = scheduler.drive([&] { return result.finished || !turns.idle(); }, may_wait);
		if (!turns.idle()) {
			try { turns.commit(); } catch (...) { if (!turn_error) { turn_error = std::current_exception(); } }
			continue;
		}
		if (result.finished) { break; }
		if (!settled) { throw stalled(driver.waiting_groups()); }
	}
	// The work is done and nothing is staged: the Driver's loop stops once its IO lands.
	stop.request(task::cancel_reason::shutdown);
	for (;;) {
		scheduler.drive([&] { return loop_result.finished || !turns.idle(); }, [] { return false; });
		if (!turns.idle()) {
			try { turns.commit(); } catch (...) { if (!turn_error) { turn_error = std::current_exception(); } }
			continue;
		}
		break;
	}
	// A throw that ended the loop is the entry's: the Driver stays stopped.
	if (loop_result.error) { std::rethrow_exception(loop_result.error); }
	driver.rearm();  // the entry's stop ends this run only: the next entry starts afresh
	if (result.error) { std::rethrow_exception(result.error); }
	if (turn_error) { std::rethrow_exception(turn_error); }
	if constexpr (!std::is_void_v<T>) { return std::move(*result.value); }
}

}  // namespace kronuz::consensus
