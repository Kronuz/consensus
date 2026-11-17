#pragma once
#include <algorithm>
#include <cassert>
#include <chrono>
#include <coroutine>
#include <cstdint>
#include <stdexcept>
#include <task.hh>

// The one rule for a transport deadline that fires late (docs/mechanisms/host-connection.md):
// a fire more than one wheel tick past its time means the loop did not run, as when the host
// stopped or descheduled the process, so the peer gets that time again, once, capped at one
// timeout. Everything here runs on the loop that owns the deadline.
namespace kronuz::consensus::transport {

class wait_list;

// A deadline on the loop's timer wheel. Its token is requested (cancel_reason::deadline) at
// expiry, for IO awaited with it; wait() ends at a wake() or at expiry. The object is its
// wheel entry, so the wait owns its timer: destroying it unfiles the entry and leaves any
// wait_list it joined. With `extend_late` false a late fire expires at once: for a wait that
// times this end's own duty (the keepalive interval), not the peer's time.
class late_deadline : wheel::timer_entry {
  public:
	enum class outcome : std::uint8_t { pending, woken, expired };

	late_deadline(task::scheduler_ref loop, std::chrono::nanoseconds timeout, bool extend_late = true)
		: loop_(loop), timeout_(std::uint64_t(std::max<std::int64_t>(1, timeout.count()))),
		  at_(loop.now() + timeout_), extend_late_(extend_late) {
		fire = &on_wheel;
		if (!loop_.arm(*this, at_))
			throw std::logic_error("late_deadline needs a scheduler with timers");
	}
	late_deadline(const late_deadline &) = delete;
	late_deadline &operator=(const late_deadline &) = delete;
	~late_deadline() {
		assert(!waiter_ && "a late_deadline outlives its wait");
		if (armed())
			loop_.cancel(*this);
		leave();
	}

	task::cancel_token token() const noexcept { return source_.token(); }
	outcome state() const noexcept { return state_; }
	// The extension a late fire granted, 0 when none.
	std::chrono::nanoseconds extended() const noexcept { return std::chrono::nanoseconds(extended_); }

	// What the deadline waits for happened: the wait ends now, whether or not a late fire
	// already armed the extension. Idempotent; nothing after expiry.
	void wake() noexcept {
		if (state_ != outcome::pending)
			return;
		state_ = outcome::woken;
		if (armed())
			loop_.cancel(*this);
		resume();
	}

	// Suspends until a wake or expiry; never resumes inside the call that ends it.
	auto wait() noexcept {
		struct awaiter {
			late_deadline &d;
			bool await_ready() const noexcept { return d.state_ != outcome::pending; }
			void await_suspend(std::coroutine_handle<> h) noexcept { d.waiter_ = h; }
			outcome await_resume() const noexcept { return d.state_; }
		};
		return awaiter{*this};
	}

	// Wakes it with every other member of `list` (wait_list::wake_all), until destroyed.
	void join(wait_list &list) noexcept;

  private:
	friend class wait_list;
	static void on_wheel(wheel::timer_entry &entry, bool expired) noexcept {
		auto &self = static_cast<late_deadline &>(entry);
		if (!expired || self.state_ != outcome::pending)
			return;  // the wheel is dying, or a wake already ended it
		const std::uint64_t now = self.loop_.now();
		const std::uint64_t late = now > self.at_ ? now - self.at_ : 0;
		if (self.extend_late_ && !self.extended_ && late > wheel::timer_wheel::tick) {
			self.extended_ = std::min(late, self.timeout_);
			self.at_ = now + self.extended_;
			self.loop_.arm(self, self.at_);
			return;
		}
		self.state_ = outcome::expired;
		self.source_.request(task::cancel_reason::deadline);
		self.resume();
	}
	void resume() noexcept {
		if (auto h = std::exchange(waiter_, nullptr))
			loop_.post(h);
	}
	void leave() noexcept;

	task::scheduler_ref loop_;
	std::uint64_t timeout_, at_, extended_ = 0;
	bool extend_late_;
	outcome state_ = outcome::pending;
	std::coroutine_handle<> waiter_;
	task::cancel_source source_;
	wait_list *list_ = nullptr;
	late_deadline *prev_ = nullptr, *next_ = nullptr;
};

// The deadlines waiting for one event (a lookup's answer, a slot freeing): wake_all() wakes
// each. A deadline leaves the list when destroyed, so the list never holds a dead one.
class wait_list {
  public:
	wait_list() = default;
	wait_list(const wait_list &) = delete;
	wait_list &operator=(const wait_list &) = delete;
	~wait_list() {
		while (head_)
			head_->leave();
	}
	bool empty() const noexcept { return !head_; }
	void wake_all() noexcept {
		while (late_deadline *d = head_) {
			d->leave();
			d->wake();
		}
	}

  private:
	friend class late_deadline;
	late_deadline *head_ = nullptr;
};

inline void late_deadline::join(wait_list &list) noexcept {
	leave();
	list_ = &list;
	next_ = list.head_;
	if (next_)
		next_->prev_ = this;
	list.head_ = this;
}
inline void late_deadline::leave() noexcept {
	if (!list_)
		return;
	(prev_ ? prev_->next_ : list_->head_) = next_;
	if (next_)
		next_->prev_ = prev_;
	list_ = nullptr;
	prev_ = next_ = nullptr;
}
} // namespace kronuz::consensus::transport
