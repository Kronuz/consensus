#pragma once
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <journal/codec.h>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

// The RPC lane (docs/mechanisms/rpc-lane.md): request/response calls with streamed bodies under
// the lane's own credit. The codec checks a frame alone (rules 1-5); a Lane is one connection's
// call table (rules 6-10), on that connection's loop; ByteBudget pools are host-wide and
// thread-safe.
namespace kronuz::consensus::transport::rpc {

inline constexpr std::size_t header_bytes = 24;
inline constexpr std::size_t chunk_bytes = 65536;
inline constexpr std::size_t text_bytes = 1024;
inline constexpr std::size_t frame_bytes = header_bytes + 24 + chunk_bytes;  // the largest: a Call, a full chunk inline
inline constexpr std::uint64_t unknown_total = ~std::uint64_t{0};

enum class Kind : std::uint8_t { call = 1, body, credit, cancel, error, drain };
// Error carries 1-6 or a method's code from 256; Cancel carries 3, 4, 5 or source_failed.
enum class Code : std::uint32_t {
	unknown_method = 1,
	overloaded,
	deadline_exceeded,
	cancelled,
	message_size,
	handler_failed,
	source_failed,
};
using MethodId = std::uint32_t;

struct Frame {
	Kind kind = Kind::call;
	bool end = false;                         // Call, Body
	std::uint32_t word = 0;                   // Call: method; Cancel, Error: code
	std::uint64_t call = 0;                   // Drain: the highest Call id accepted, or 0
	std::uint64_t timeout_ms = 0, window = 0; // Call
	std::uint64_t offset = 0, total = 0;      // Body; Call: total
	std::uint64_t limit = 0;                  // Credit
	std::string_view bytes;                   // Call, Body: data; Cancel, Error: text
};

inline bool error_code(std::uint32_t code) noexcept { return (code >= 1 && code <= 6) || code >= 256; }
inline bool cancel_code(std::uint32_t code) noexcept { return code >= 3 && code <= 7 && code != 6; }

// The first of D3 rules 2-5 that a decoded frame breaks, or nullptr.
inline const char *violation(const Frame &f) noexcept {
	if (f.kind < Kind::call || f.kind > Kind::drain)
		return "rule 2: kind";
	if (f.end && f.kind != Kind::call && f.kind != Kind::body)
		return "rule 2: end flag";
	const bool coded = f.kind == Kind::call || f.kind == Kind::cancel || f.kind == Kind::error;
	if (coded != (f.word != 0))
		return "rule 3: word";
	if ((f.kind == Kind::error && !error_code(f.word)) || (f.kind == Kind::cancel && !cancel_code(f.word)))
		return "rule 3: code";
	const std::size_t most = f.kind == Kind::call || f.kind == Kind::body ? chunk_bytes
		: f.kind == Kind::cancel || f.kind == Kind::error                 ? text_bytes
																		  : 0;
	if (f.bytes.size() > most)
		return "rule 4: length";
	if (!f.call && f.kind != Kind::drain)
		return "rule 5: call id";
	const std::uint64_t n = f.bytes.size();
	if (f.kind == Kind::call) {
		if (!f.timeout_ms || !f.window)
			return "rule 5: timeout or window";
		if (n > f.total)
			return "rule 5: inline past total";
		if (f.total != unknown_total && f.end != (n == f.total))
			return "rule 5: end";
	}
	if (f.kind == Kind::body) {
		if (!n && !f.end)
			return "rule 7.2: empty body";
		if (f.offset > f.total || n > f.total - f.offset)
			return "rule 7.3: past total";
		if (f.total != unknown_total && f.end != (n == f.total - f.offset))
			return "rule 7.5: end";
	}
	return nullptr;
}

inline std::string magic(unsigned version) {
	if (!version || version > 9999)
		throw std::invalid_argument("rpc version");
	std::string m = "DTRP0000";
	for (int i = 7; version; --i, version /= 10)
		m[i] = static_cast<char>('0' + version % 10);
	return m;
}

inline std::string encode(const Frame &f, unsigned version = 1) {
	if (auto why = violation(f))
		throw std::invalid_argument(std::string("invalid rpc frame: ") + why);
	std::string out = magic(version);
	out.reserve(header_bytes + 24 + f.bytes.size());
	out.push_back(static_cast<char>(f.kind));
	out.push_back(static_cast<char>(f.end));
	out.append(2, '\0');
	journal::put32(out, f.word);
	journal::put64(out, f.call);
	if (f.kind == Kind::call) {
		journal::put64(out, f.timeout_ms);
		journal::put64(out, f.window);
		journal::put64(out, f.total);
	} else if (f.kind == Kind::body) {
		journal::put64(out, f.offset);
		journal::put64(out, f.total);
	} else if (f.kind == Kind::credit) {
		journal::put64(out, f.limit);
	}
	out.append(f.bytes);
	return out;
}

// The frame borrows `in` for its bytes. Throws journal::Corruption naming the rule it breaks.
inline Frame decode(std::string_view in, unsigned version = 1) {
	using journal::Corruption;
	if (in.size() < header_bytes || in.substr(0, 8) != magic(version))
		throw Corruption("rpc frame: rule 1: magic");
	const auto kind = static_cast<std::uint8_t>(in[8]), flags = static_cast<std::uint8_t>(in[9]);
	if (!kind || kind > 6)
		throw Corruption("rpc frame: rule 2: kind");
	if ((flags & ~1u) || in[10] || in[11])
		throw Corruption("rpc frame: rule 2: reserved bits");
	Frame f;
	f.kind = static_cast<Kind>(kind);
	f.end = flags & 1;
	in.remove_prefix(12);
	f.word = journal::get32(in);
	f.call = journal::get64(in);
	const std::size_t fixed = f.kind == Kind::call ? 24 : f.kind == Kind::body ? 16 : f.kind == Kind::credit ? 8 : 0;
	if (in.size() < fixed)
		throw Corruption("rpc frame: rule 4: length");
	if (f.kind == Kind::call) {
		f.timeout_ms = journal::get64(in);
		f.window = journal::get64(in);
		f.total = journal::get64(in);
	} else if (f.kind == Kind::body) {
		f.offset = journal::get64(in);
		f.total = journal::get64(in);
	} else if (f.kind == Kind::credit) {
		f.limit = journal::get64(in);
	}
	f.bytes = in;
	if (auto why = violation(f))
		throw Corruption(std::string("rpc frame: ") + why);
	return f;
}

// A host-wide pool of lane bytes, shared by every loop. A holder takes bytes before it grants
// or queues them; waits are first in first out, and a wake runs on the releasing thread.
class ByteBudget {
  public:
	using Wake = std::function<void(std::size_t granted)>;
	explicit ByteBudget(std::size_t limit) : limit_(limit) {}
	ByteBudget(const ByteBudget &) = delete;
	std::size_t limit() const noexcept { return limit_; }
	std::size_t used() const {
		std::lock_guard lock(mutex_);
		return used_;
	}
	std::size_t high_water() const {
		std::lock_guard lock(mutex_);
		return high_;
	}
	// Between `least` and `most` bytes now, or 0 when fewer are free or another waits first.
	std::size_t try_acquire(std::size_t least, std::size_t most) {
		std::lock_guard lock(mutex_);
		return waiters_.empty() ? take(least, most) : 0;
	}
	// As try_acquire; on 0 it queues `wake` under `ticket` until the bytes free.
	std::size_t acquire(std::size_t least, std::size_t most, Wake wake, std::uint64_t &ticket) {
		std::lock_guard lock(mutex_);
		ticket = 0;
		if (waiters_.empty())
			if (auto n = take(least, most))
				return n;
		ticket = ++tickets_;
		waiters_.push_back({least, most, std::move(wake), ticket});
		return 0;
	}
	// Withdraws a wait; false when its grant is already on the way to its wake.
	bool cancel(std::uint64_t ticket) {
		Wakes wakes;
		bool found = false;
		{
			std::lock_guard lock(mutex_);
			auto it = std::find_if(waiters_.begin(), waiters_.end(), [&](auto &w) { return w.ticket == ticket; });
			if (it == waiters_.end())
				return false;
			found = it == waiters_.begin();
			waiters_.erase(it);
			if (found)
				serve(wakes);
		}
		run(wakes);
		return true;
	}
	void release(std::size_t n) { renew(n, 0); }
	// Takes `n` now, past the limit if it must: for frames that answer the peer and cannot wait.
	// Their own bound is elsewhere (LaneOptions::queue_bytes); until they go, nobody else takes.
	void charge(std::size_t n) {
		std::lock_guard lock(mutex_);
		used_ += n;
		high_ = std::max(high_, used_);
	}
	// Gives back a holder's `returned` bytes and takes up to `wanted` back in one step, its own
	// first: the result, the new holding, is at least min(wanted, returned). It grows past
	// `returned` only while nobody waits.
	std::size_t renew(std::size_t returned, std::size_t wanted) {
		Wakes wakes;
		std::size_t held;
		{
			std::lock_guard lock(mutex_);
			used_ -= returned;
			held = std::min(wanted, returned);  // its own bytes, whatever another holder took past the limit
			used_ += held;
			if (waiters_.empty())
				held += take(0, wanted - held);
			serve(wakes);
		}
		run(wakes);
		return held;
	}

  private:
	using Wakes = std::vector<std::function<void()>>;
	struct Waiter {
		std::size_t least, most;
		Wake wake;
		std::uint64_t ticket;
	};
	std::size_t take(std::size_t least, std::size_t most) {
		const std::size_t n = used_ < limit_ ? std::min(most, limit_ - used_) : 0;
		if (n < least || !n)
			return 0;
		used_ += n;
		high_ = std::max(high_, used_);
		return n;
	}
	void serve(Wakes &wakes) {
		while (!waiters_.empty()) {
			auto n = take(waiters_.front().least, waiters_.front().most);
			if (!n)
				break;
			wakes.push_back([wake = std::move(waiters_.front().wake), n] { wake(n); });
			waiters_.pop_front();
		}
	}
	static void run(Wakes &wakes) {
		for (auto &w : wakes)
			w();
	}
	mutable std::mutex mutex_;
	std::size_t limit_, used_ = 0, high_ = 0;
	std::uint64_t tickets_ = 0;
	std::deque<Waiter> waiters_;
};

// Bytes held in a pool until the holder lets them go.
class Charge {
  public:
	Charge() = default;
	Charge(ByteBudget &pool, std::size_t n) : pool_(&pool), n_(n) {}
	Charge(Charge &&o) noexcept : pool_(o.pool_), n_(std::exchange(o.n_, 0)) {}
	Charge &operator=(Charge &&o) noexcept {
		std::swap(pool_, o.pool_);
		std::swap(n_, o.n_);
		return *this;
	}
	~Charge() {
		if (n_)
			pool_->release(n_);
	}

  private:
	ByteBudget *pool_ = nullptr;
	std::size_t n_ = 0;
};

struct ProtocolError : std::runtime_error {
	using std::runtime_error::runtime_error;
};

enum class Outcome : std::uint8_t { ok, error, cancel, not_processed, connection_aborted, timed_out, not_connected, protocol_not_supported };
inline constexpr std::size_t outcomes = 8;
// How a call ended at this end: `error` and `cancel` name the frame and carry its code and text.
struct Status {
	Outcome outcome = Outcome::ok;
	std::uint32_t code = 0;
	std::string text;
};
using Handle = std::uint64_t;
// One call's callbacks, on the lane's loop. `body` hands over the peer's stream in order; the
// consumer calls Lane::consume as it takes the bytes, which renews credit. `done` runs once.
struct Events {
	std::function<void(std::string_view bytes, bool end)> body;
	std::function<void()> credit;
	std::function<void(const Status &)> done;
};
struct Incoming {
	Handle handle;
	std::uint64_t id;
	MethodId method;
	std::uint64_t total;
};
struct Pools {
	ByteBudget &serving, &calling, &outgoing;
};
using Clock = std::chrono::steady_clock;
struct LaneOptions {
	std::size_t window = 4 << 20; // a served request's credit window
	std::size_t serving = 256;    // served calls live at once
	// Bytes of frames the table queues unsent (A3, R2): Call and Body frames wait for room past it,
	// so a peer that stops reading holds at most this much of rpc_outgoing_bytes. One frame always fits.
	std::size_t queue_bytes = 4 * (header_bytes + 24 + chunk_bytes);
	std::chrono::milliseconds max_call_deadline = std::chrono::hours(1);
	unsigned version = 1;
	// The time a call whose deadline passed `late` ago gets again, once (T7: the transport's
	// late-fire rule, deadline.h); none: a call ends at its deadline.
	std::function<Clock::duration(Clock::duration late, Clock::duration timeout)> late;
};
struct CallOptions {
	Clock::time_point deadline;
	std::size_t window = 4 << 20;
	std::optional<std::uint64_t> total; // the request's length (unknown_total when unknown); none: inline only
};
// A frame ready for the socket; its charge returns when the write completes.
struct Outgoing {
	std::string bytes;
	Charge charge;
};

// One connection's call table. Not thread-safe: it lives on the connection's loop; `post` runs
// work there, for pool wakes from other loops. `ready` runs when next() has a frame. A throw from
// receive() is a protocol violation: the connection closes, then close(). A throw from this end's
// own code (accept, a call's events) fails that call alone, counted in failed(). A callback may
// call into the table, close() included, which then runs once the outermost call returns; none
// may destroy it.
class Lane {
  public:
	using Accept = std::function<std::optional<Events>(const Incoming &)>; // none: unknown_method
	using Post = std::function<void(std::function<void()>)>;
	Lane(bool dialer, Pools pools, LaneOptions options, Accept accept, Post post, std::function<void()> ready,
		 std::function<Clock::time_point()> now = Clock::now)
		: own_(dialer ? 1 : 2), next_id_(own_), pools_(pools), options_(options), accept_(std::move(accept)),
		  post_(std::move(post)), ready_(std::move(ready)), now_(std::move(now)) {}
	Lane(const Lane &) = delete;
	~Lane() { close(); }

	// Starts a call: it waits for `calling` credit and `outgoing` bytes, then queues its Call.
	// On a closed or draining lane `done` runs at once, within call().
	Handle call(MethodId method, CallOptions options, std::string request, Events events) {
		Busy busy(*this);
		const std::uint64_t total = options.total.value_or(request.size());
		if (!method || request.size() > chunk_bytes || !options.window || request.size() > total)
			throw std::invalid_argument("rpc call");
		const Handle h = ++handles_;
		auto &c = calls_[h];
		c.events = std::make_shared<Events>(std::move(events));
		c.method = method;
		c.deadline = options.deadline;
		c.timeout = options.deadline - now_();
		c.in = {.pool = &pools_.calling, .window = options.window};
		c.out.total = total;
		c.out.limit = c.out.sent = request.size();
		const bool end = request.size() == total;
		c.out.pending.push_back({std::move(request), end});
		if (closed_ || drain_sent_ || peer_drained_) {
			finish(h, {closed_ ? Outcome::not_connected : Outcome::not_processed});
			return h;
		}
		const std::size_t want = options.window;
		if (auto n = pools_.calling.acquire(std::min(want, chunk_bytes), want, wake(h, pools_.calling), c.ticket))
			started(h, n);
		else
			c.ticket_pool = &pools_.calling;
		return h;
	}
	// Bytes of this end's stream the peer's credit allows now (at most one chunk).
	std::size_t sendable(Handle h) const {
		auto it = calls_.find(h);
		if (it == calls_.end() || it->second.out.ended())
			return 0;
		auto &o = it->second.out;
		return std::min<std::uint64_t>(chunk_bytes, o.limit - o.sent);
	}
	// Queues the next chunk of this end's stream, within sendable(); `end` ends it.
	void send(Handle h, std::string bytes, bool end) {
		Busy busy(*this);
		auto it = calls_.find(h);
		if (it == calls_.end())
			return;
		auto &o = it->second.out;
		if (o.ended() || bytes.size() > sendable(h) || (!bytes.size() && !end) ||
			(o.total != unknown_total && (bytes.size() > o.total - o.sent || end != (o.sent + bytes.size() == o.total))))
			throw std::logic_error("rpc send");
		o.sent += bytes.size();
		o.pending.push_back({std::move(bytes), end});
		if (!it->second.ticket && it->second.called)
			pump(h);
	}
	// The consumer took `n` bytes of the peer's stream: renews credit, or finishes the stream.
	void consume(Handle h, std::size_t n) {
		Busy busy(*this);
		auto it = calls_.find(h);
		if (it == calls_.end())
			return;
		auto &c = it->second;
		auto &in = c.in;
		if (n > in.received - in.consumed)
			throw std::logic_error("rpc consume");
		in.consumed += n;
		if (in.ended && in.consumed == in.received) {
			in.pool->release(std::exchange(in.held, 0));
			if (!c.serving)
				return finish(h, {Outcome::ok});
		} else {
			regrant(c, in.consumed);
		}
	}
	// A served call's response length, before its first chunk; unknown_total by default.
	void total(Handle h, std::uint64_t total) {
		auto &o = calls_.at(h).out;
		if (!calls_.at(h).serving || o.sent)
			throw std::logic_error("rpc total");
		o.total = total;
	}
	// Ends a call early: Cancel from its caller, Error from its server.
	void stop(Handle h, Code code, std::string_view text = {}) {
		Busy busy(*this);
		auto it = calls_.find(h);
		if (it == calls_.end())
			return;
		auto &c = it->second;
		const auto word = static_cast<std::uint32_t>(code);
		if (c.serving ? !error_code(word) : !cancel_code(word))
			throw std::invalid_argument("rpc stop code");
		text = text.substr(0, text_bytes);
		if (c.called)
			small({.kind = c.serving ? Kind::error : Kind::cancel, .word = word, .call = c.id, .bytes = text}, &c);
		const auto outcome = c.serving ? Outcome::error
			: c.called || code != Code::deadline_exceeded ? Outcome::cancel
														  : Outcome::timed_out;
		finish(h, {outcome, word, std::string(text)});
	}
	// Sends Drain: no new calls either way; the connection may close once drained().
	void drain() {
		Busy busy(*this);
		if (drain_sent_ || closed_)
			return;
		drain_sent_ = true;
		small({.kind = Kind::drain, .call = high_peer_});
		not_processed(~std::uint64_t{0});
	}
	bool drained() const noexcept { return drain_sent_ && calls_.empty(); }
	// Either end sent Drain: the lane starts no more calls.
	bool draining() const noexcept { return drain_sent_ || peer_drained_; }
	// The connection failed: every call ends, every byte returns.
	void close() {
		if (depth_) {
			close_wanted_ = true;
			return;
		}
		if (closed_)
			return;
		closed_ = true;
		small_.clear();
		small_taken_ = small_queued_;
		serving_ -= std::exchange(slots_, 0);
		chunks_.clear();
		roomless_.clear();
		queued_ = refusals_ = 0;
		std::vector<Handle> live;
		for (auto &[h, c] : calls_)
			live.push_back(h);
		for (auto h : live)
			if (calls_.count(h))
				finish(h, {calls_[h].called ? Outcome::connection_aborted : Outcome::not_processed});
	}
	// Ends the calls whose deadline has passed, each first given the late rule's time once;
	// next_deadline() says when to call it again.
	void expire() {
		Busy busy(*this);
		const auto now = now_();
		std::vector<Handle> due;
		for (auto &[h, c] : calls_) {
			if (c.deadline <= now && options_.late && !c.extended)
				if (const auto more = options_.late(now - c.deadline, c.timeout); more > Clock::duration::zero()) {
					c.deadline = now + more;
					c.extended = true;
				}
			if (c.deadline <= now)
				due.push_back(h);
		}
		for (auto h : due)
			if (calls_.count(h))
				stop(h, Code::deadline_exceeded);
	}
	std::optional<Clock::time_point> next_deadline() const {
		std::optional<Clock::time_point> first;
		for (auto &[h, c] : calls_)
			if (!first || c.deadline < *first)
				first = c.deadline;
		return first;
	}
	// The next frame to write: small frames first, in order, then chunks round-robin by call.
	std::optional<Outgoing> next() {
		Busy busy(*this);
		std::optional<Outgoing> out;
		if (!small_.empty()) {
			auto &front = small_.front();
			refusals_ -= front.refusal;
			if (front.slot) {
				--serving_;
				--slots_;
			}
			out = std::move(front.out);
			small_.pop_front();
			++small_taken_;
		} else if (!chunks_.empty()) {
			auto it = chunks_.upper_bound(cursor_);
			if (it == chunks_.end())
				it = chunks_.begin();
			cursor_ = it->first;
			out = std::move(it->second.front());
			it->second.pop_front();
			if (it->second.empty())
				chunks_.erase(it);
		}
		if (out) {
			queued_ -= out->bytes.size();
			room();
		}
		return out;
	}
	void receive(std::string_view bytes) {
		Busy busy(*this);
		if (closed_)
			return;
		const Frame f = decode(bytes, options_.version);
		if (f.kind == Kind::drain)
			return drained_by_peer(f.call);
		const bool ours = (f.call & 1) == (own_ & 1);
		// Rule 6: a caller sends Call, request Body, Cancel and response Credit; a server the rest.
		if (ours ? f.kind == Kind::call || f.kind == Kind::cancel : f.kind == Kind::error)
			throw ProtocolError("rpc: rule 6: direction");
		if (f.kind == Kind::call) {
			called(f);
			if (refusals_ > options_.serving)
				throw ProtocolError("rpc: queue: the peer does not read its refusals");
			return;
		}
		if (ours ? f.call > last_id_ : f.call > high_peer_)
			throw ProtocolError("rpc: rule 6: id not allocated");
		auto at = index_.find(f.call);
		if (at == index_.end()) {
			++late_; // rule 9: a retired id
			return;
		}
		const Handle h = at->second;
		auto &c = calls_.at(h);
		switch (f.kind) {
		case Kind::body: {
			auto &in = c.in;
			const std::uint64_t n = f.bytes.size();
			if (in.ended)
				throw ProtocolError("rpc: rule 7.7: after end");
			if (f.offset != in.received)
				throw ProtocolError("rpc: rule 7.1: offset");
			if (in.known ? f.total != in.total : false)
				throw ProtocolError("rpc: rule 7.4: total");
			if (n > in.limit - f.offset)
				throw ProtocolError("rpc: rule 7.6: past credit");
			in.known = true;
			in.total = f.total;
			in.received += n;
			in.ended = f.end;
			if (auto keep = c.events; keep->body && !local(h, [&] { keep->body(f.bytes, f.end); }))
				return;
			if (f.end)
				consume(h, 0); // an end the consumer already took all of finishes the stream
			return;
		}
		case Kind::credit:
			if (f.limit <= c.out.limit)
				throw ProtocolError("rpc: rule 8: credit");
			c.out.limit = f.limit;
			if (auto keep = c.events; keep->credit)
				local(h, [&] { keep->credit(); });
			return;
		default: // Cancel at the server, Error at the caller
			return finish(h, {f.kind == Kind::cancel ? Outcome::cancel : Outcome::error, f.word, std::string(f.bytes)});
		}
	}
	std::uint64_t late() const noexcept { return late_; }
	std::uint64_t failed() const noexcept { return failed_; }
	std::size_t live() const noexcept { return calls_.size(); }
	// What next() would return: a small frame, or else a chunk.
	bool small_ready() const noexcept { return !small_.empty(); }
	bool chunk_ready() const noexcept { return !chunks_.empty(); }
	// Bytes of frames queued and not yet taken by next().
	std::size_t queued() const noexcept { return queued_; }

  private:
	struct Pending {
		std::string bytes;
		bool end;
	};
	struct Inbound { // the peer's stream
		ByteBudget *pool = nullptr;
		std::size_t window = 0, held = 0; // held covers [max(consumed, base), limit)
		std::uint64_t base = 0, limit = 0, received = 0, consumed = 0, total = unknown_total;
		bool known = false, ended = false;
	};
	struct Outbound { // this end's stream
		std::uint64_t total = unknown_total, limit = 0, sent = 0, queued = 0;
		bool end = false;
		std::deque<Pending> pending;
		bool ended() const noexcept { return end || (!pending.empty() && pending.back().end); }
	};
	struct Call {
		std::uint64_t id = 0;
		bool serving = false, called = false; // called: its Call is queued
		MethodId method = 0;
		Clock::time_point deadline;
		Clock::duration timeout{};      // the deadline's span when set, for the late rule
		bool extended = false;          // the late rule gave it more time
		std::shared_ptr<Events> events; // kept alive by each callback while it runs
		Inbound in;
		Outbound out;
		std::uint64_t ticket = 0; // a wait in ticket_pool
		ByteBudget *ticket_pool = nullptr;
		std::size_t reserved = 0; // the connection's room its outgoing wait holds
		bool roomless = false;    // waits for room on the connection
		std::uint64_t small = 0;  // its last small frame's sequence number (unsent while > small_taken_)
	};
	// A queued frame; a refusal answers a Call the table never took; `slot`: an ended served
	// call's serving slot, which returns when next() takes it.
	struct Queued {
		Outgoing out;
		bool refusal = false, slot = false;
	};
	// Holds the table for a callback's duration: a close() inside runs when the outermost returns.
	struct Busy {
		Lane &lane;
		explicit Busy(Lane &l) : lane(l) { ++lane.depth_; }
		~Busy() {
			if (!--lane.depth_ && std::exchange(lane.close_wanted_, false))
				lane.close();
		}
	};

	ByteBudget::Wake wake(Handle h, ByteBudget &pool) {
		return [this, h, &pool, post = post_, alive = std::weak_ptr<int>(alive_)](std::size_t n) {
			post([this, h, &pool, alive, n] {
				if (alive.expired())
					return pool.release(n);
				granted(h, pool, n);
			});
		};
	}
	void granted(Handle h, ByteBudget &pool, std::size_t n) {
		Busy busy(*this);
		auto it = calls_.find(h);
		if (it == calls_.end() || it->second.ticket_pool != &pool || !it->second.ticket)
			return pool.release(n);
		it->second.ticket = 0;
		it->second.ticket_pool = nullptr;
		if (&pool == &pools_.calling) {
			started(h, n);
		} else {
			reserved_ -= std::exchange(it->second.reserved, 0);
			chunk(h, Charge(pool, n));
			pump(h);
		}
	}
	// The caller's first response window is reserved: its Call can queue.
	void started(Handle h, std::size_t n) {
		auto &c = calls_.at(h);
		c.in.held = n;
		c.in.limit = n;
		pump(h);
	}
	// Moves pending chunks into the queue as the connection's room, then `outgoing`, admits
	// them; each frame is charged whole (A3). A call without room waits, in order, for next().
	void pump(Handle h) {
		auto it = calls_.find(h);
		while (it != calls_.end() && !it->second.ticket && !it->second.roomless && !it->second.out.pending.empty()) {
			auto &c = it->second;
			const std::size_t size = head_size(c);
			if (!fits(size)) {
				c.roomless = true;
				roomless_.push_back(h);
				return;
			}
			if (!pools_.outgoing.acquire(size, size, wake(h, pools_.outgoing), c.ticket)) {
				c.ticket_pool = &pools_.outgoing;
				reserved_ += c.reserved = size;
				return;
			}
			chunk(h, Charge(pools_.outgoing, size));
			it = calls_.find(h);
		}
	}
	// The encoded size of a call's next frame: its Call, or a Body.
	static std::size_t head_size(const Call &c) {
		return c.out.pending.front().bytes.size() + header_bytes + (c.called ? 16 : 24);
	}
	bool fits(std::size_t size) const noexcept {
		return !(queued_ + reserved_) || queued_ + reserved_ + size <= options_.queue_bytes;
	}
	// Frames left the queue: the calls waiting for room pump again, first come first, while the
	// first one's frame fits.
	void room() {
		while (!roomless_.empty()) {
			const auto h = roomless_.front();
			auto it = calls_.find(h);
			const bool waiting = it != calls_.end() && it->second.roomless;
			if (waiting && !fits(head_size(it->second)))
				return;
			roomless_.pop_front();
			if (waiting) {
				it->second.roomless = false;
				pump(h);
			}
		}
	}
	// D4: an id is allocated when its Call is queued, after every wait, so ids reach the peer in order.
	void allocate(Call &c, Handle h) {
		c.id = next_id_;
		next_id_ += 2;
		last_id_ = c.id;
		index_[c.id] = h;
	}
	// Queues the head pending chunk, charged: the Call frame first.
	void chunk(Handle h, Charge charge) {
		auto &c = calls_.at(h);
		auto p = std::move(c.out.pending.front());
		c.out.pending.pop_front();
		if (!c.called) {
			const auto left = std::chrono::ceil<std::chrono::milliseconds>(c.deadline - now_()).count();
			if (left <= 0)
				return finish(h, {Outcome::timed_out});
			c.called = true;
			allocate(c, h);
			const Frame f{.kind = Kind::call, .end = p.end, .word = c.method, .call = c.id, .timeout_ms = std::uint64_t(left),
						  .window = c.in.limit, .total = c.out.total, .bytes = p.bytes};
			small_.push_back({{encode(f, options_.version), std::move(charge)}});
			queued_ += small_.back().out.bytes.size();
		} else {
			const Frame f{.kind = Kind::body, .end = p.end, .call = c.id, .offset = c.out.queued, .total = c.out.total, .bytes = p.bytes};
			auto frame = encode(f, options_.version);  // before the queue: a throw leaves no empty one
			queued_ += frame.size();
			chunks_[c.id].push_back({std::move(frame), std::move(charge)});
		}
		c.out.end = p.end;
		c.out.queued += p.bytes.size();
		ready_();
		if (c.serving && p.end)
			finish(h, {Outcome::ok}); // the response is queued; an unread request is discarded
	}
	// D5: after the consumer took half a window, the consumed bytes move into the next grant.
	void regrant(Call &c, std::uint64_t from) {
		auto &in = c.in;
		if (from < in.base || in.limit - from > in.window / 2 || in.ended)
			return;
		const std::uint64_t wanted = in.known && in.total != unknown_total ? std::min<std::uint64_t>(in.window, in.total - from) : in.window;
		const std::size_t held = in.pool->renew(in.held, wanted);
		in.held = held;
		if (from + held > in.limit) {
			in.limit = from + held;
			small({.kind = Kind::credit, .call = c.id, .limit = in.limit}, &c);
		}
	}
	void called(const Frame &f) {
		if (f.call <= high_peer_)
			throw ProtocolError("rpc: rule 6: Call id did not increase");
		high_peer_ = f.call;
		if (drain_sent_)
			return; // D4: discarded unanswered; the Drain told the peer
		const std::uint64_t base = f.bytes.size();
		const std::uint64_t need = f.end ? 0 : f.total - base;
		const std::size_t want = std::min<std::uint64_t>(options_.window, need);
		std::size_t held = 0;
		if (serving_ >= options_.serving || (want && !(held = pools_.serving.try_acquire(std::min(want, chunk_bytes), want))))
			return small({.kind = Kind::error, .word = std::uint32_t(Code::overloaded), .call = f.call}, nullptr, true);
		const Handle h = ++handles_;
		std::optional<Events> events;
		try {
			events = accept_ ? accept_({h, f.call, f.word, f.total}) : std::nullopt;
		} catch (...) {
			++failed_;
			pools_.serving.release(held);
			return small({.kind = Kind::error, .word = std::uint32_t(Code::handler_failed), .call = f.call}, nullptr, true);
		}
		if (!events) {
			pools_.serving.release(held);
			return small({.kind = Kind::error, .word = std::uint32_t(Code::unknown_method), .call = f.call}, nullptr, true);
		}
		auto &c = calls_[h];
		c.id = f.call;
		c.serving = c.called = true;
		c.method = f.word;
		c.events = std::make_shared<Events>(std::move(*events));
		const auto ms = std::min<std::uint64_t>(f.timeout_ms, options_.max_call_deadline.count());
		c.timeout = std::chrono::milliseconds(ms);
		c.deadline = now_() + c.timeout;
		c.in = {.pool = &pools_.serving, .window = options_.window, .held = held, .base = base, .limit = base + held,
				.received = base, .total = f.total, .known = true, .ended = f.end};
		c.out.limit = f.window;
		index_[c.id] = h;
		++serving_;
		if (want)
			small({.kind = Kind::credit, .call = c.id, .limit = c.in.limit}, &c);
		if (auto keep = c.events; (base || f.end) && keep->body)
			local(h, [&] { keep->body(f.bytes, f.end); });
	}
	// Runs this end's callback for call `h`: a throw is this end's fault, not the peer's, so it
	// fails that call alone (Error handler_failed from a server, Cancel cancelled from a caller).
	template <class F> bool local(Handle h, F f) {
		try {
			f();
			return true;
		} catch (const std::exception &e) {
			fail(h, e.what());
		} catch (...) {
			fail(h, "unknown exception");
		}
		return false;
	}
	void fail(Handle h, std::string_view what) {
		++failed_;
		if (auto it = calls_.find(h); it != calls_.end())
			stop(h, it->second.serving ? Code::handler_failed : Code::cancelled, what);
	}
	void drained_by_peer(std::uint64_t id) {
		if (peer_drained_)
			throw ProtocolError("rpc: rule 10: second Drain");
		if (id > last_id_ || (id && (id & 1) != (own_ & 1)))
			throw ProtocolError("rpc: rule 10: Drain id");
		peer_drained_ = true;
		not_processed(id);
	}
	// Own calls above `above`, and those not yet queued, never started at the peer.
	void not_processed(std::uint64_t above) {
		std::vector<Handle> stopped;
		for (auto &[h, c] : calls_)
			if (!c.serving && (!c.called || c.id > above))
				stopped.push_back(h);
		for (auto h : stopped)
			if (calls_.count(h))
				finish(h, {Outcome::not_processed});
	}
	// A small frame answers or tells the peer, so it never waits: it takes rpc_outgoing_bytes at
	// once. Each is bounded (N1, F1): a served call that ends keeps its slot until its last small
	// frame is taken, and a peer that never reads past `serving` refusals is closed (receive).
	void small(const Frame &f, Call *c = nullptr, bool refusal = false) {
		auto bytes = encode(f, options_.version);
		pools_.outgoing.charge(bytes.size());
		const auto n = bytes.size();
		small_.push_back({{std::move(bytes), Charge(pools_.outgoing, n)}, refusal});
		queued_ += n;
		refusals_ += refusal;
		const auto seq = ++small_queued_;
		if (c)
			c->small = seq;
		ready_();
	}
	// The call's last small frame while it is unsent.
	Queued *unsent(const Call *c) {
		if (!c || c->small <= small_taken_)
			return nullptr;
		return &small_[c->small - small_taken_ - 1];
	}
	// A call ends here exactly once: its bytes return, its id retires, `done` runs last.
	void finish(Handle h, Status status) {
		auto it = calls_.find(h);
		if (it == calls_.end())
			return;
		Call c = std::move(it->second);
		calls_.erase(it);
		if (c.id)
			index_.erase(c.id);
		if (c.ticket)
			c.ticket_pool->cancel(c.ticket); // a grant already on its way is released by granted()
		reserved_ -= c.reserved;
		if (c.in.held)
			c.in.pool->release(c.in.held);
		if (auto q = chunks_.find(c.id); q != chunks_.end() && !(c.serving && status.outcome == Outcome::ok)) {
			for (auto &o : q->second)
				queued_ -= o.bytes.size();
			chunks_.erase(q);
			room();
		}
		if (Queued *last = c.serving ? unsent(&c) : nullptr) {
			last->slot = true; // the slot returns when next() takes it
			++slots_;
		} else if (c.serving) {
			--serving_;
		}
		if (c.events->done) {
			try {
				c.events->done(status);
			} catch (...) {
				++failed_;  // the call has ended; the rest of the table goes on
			}
		}
	}

	const std::uint64_t own_;
	std::uint64_t next_id_, last_id_ = 0, high_peer_ = 0, late_ = 0, failed_ = 0, cursor_ = 0;
	Pools pools_;
	LaneOptions options_;
	Accept accept_;
	Post post_;
	std::function<void()> ready_;
	std::function<Clock::time_point()> now_;
	Handle handles_ = 0;
	std::size_t serving_ = 0, slots_ = 0; // served calls live or ending; ended ones whose last small frame is unsent
	std::uint64_t small_queued_ = 0, small_taken_ = 0; // small frames queued and taken, ever
	bool closed_ = false, drain_sent_ = false, peer_drained_ = false;
	std::map<Handle, Call> calls_;
	std::unordered_map<std::uint64_t, Handle> index_;
	std::deque<Queued> small_;
	std::map<std::uint64_t, std::deque<Outgoing>> chunks_;
	std::deque<Handle> roomless_;                     // calls waiting for room, in order
	std::size_t queued_ = 0, reserved_ = 0, refusals_ = 0; // unsent bytes; room outgoing waits hold; refusals unsent
	int depth_ = 0;                                    // callbacks running (Busy)
	bool close_wanted_ = false;                        // a close() asked for inside one
	std::shared_ptr<int> alive_ = std::make_shared<int>();
};
} // namespace kronuz::consensus::transport::rpc
