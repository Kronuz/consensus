#pragma once
#include <chrono>
#include <consensus/types.h>
#include <limits>
#include <metrics.hh>
#include <optional>

namespace kronuz::consensus::transport {
struct AppendTimings {
	kronuz::metrics::DurationHistogram<> response, higher_term_response, timeout, superseded, write_failure,
		disconnected;
	kronuz::metrics::Counter attempts, observations, duplicate_attempts, unmatched, successful, rejected;
};
// One record belongs to one authenticated control connection. These timestamps
// describe transport observations, independently of subsequent Core admission.
class AppendObservation {
  public:
	using Clock = std::chrono::steady_clock;
	using Time = Clock::time_point;
	std::uint64_t emit(const kronuz::consensus::AppendRequest &request, Time now, AppendTimings &timings) {
		timings.attempts.add();
		if (flight_ && flight_->term == request.term && flight_->rpc == request.rpc &&
			flight_->probe == request.read_probe) {
			timings.duplicate_attempts.add();
			return flight_->generation;
		}
		settle(now, timings.superseded);
		if (generation_ == std::numeric_limits<std::uint64_t>::max())
			return 0;
		timings.observations.add();
		flight_ = Flight{request.term, request.rpc, request.read_probe, ++generation_, now};
		return generation_;
	}
	bool receive(const kronuz::consensus::AppendResponse &response, Time now, AppendTimings &timings) {
		if (!flight_ || response.rpc != flight_->rpc || response.read_probe != flight_->probe ||
			response.term < flight_->term) {
			timings.unmatched.add();
			return false;
		}
		(response.success ? timings.successful : timings.rejected).add();
		settle(now, response.term == flight_->term ? timings.response : timings.higher_term_response);
		return true;
	}
	void expire(Time now, Clock::duration limit, AppendTimings &timings) {
		if (flight_ && now - flight_->started >= limit)
			settle(now, timings.timeout);
	}
	void abandon(Time now, AppendTimings &timings) { settle(now, timings.disconnected); }
	void write_failed(std::uint64_t generation, Time now, AppendTimings &timings) {
		if (flight_ && flight_->generation == generation)
			settle(now, timings.write_failure);
	}
	bool pending() const noexcept { return flight_.has_value(); }

  private:
	struct Flight {
		kronuz::consensus::Term term;
		kronuz::consensus::Token rpc, probe;
		std::uint64_t generation;
		Time started;
	};
	void settle(Time now, kronuz::metrics::DurationHistogram<> &histogram) {
		if (!flight_)
			return;
		auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(now - flight_->started).count();
		histogram.observe(elapsed > 0 ? static_cast<std::uint64_t>(elapsed) : 0);
		flight_.reset();
	}
	std::optional<Flight> flight_;
	std::uint64_t generation_ = 0;
};
} // namespace kronuz::consensus::transport
