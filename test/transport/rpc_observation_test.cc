#include "consensus/transport/rpc_observation.h"
#include <cassert>
#include <chrono>
using namespace kronuz::consensus::transport;
using namespace kronuz::consensus;
using namespace std::chrono_literals;
int main() {
	AppendObservation observation;
	AppendTimings timings;
	const auto start = AppendObservation::Time{};
	AppendRequest request{2, 10, 0, 0, 0, 7, {}};
	auto generation = observation.emit(request, start, timings);
	assert(observation.emit(request, start + 1ms, timings) == generation);
	assert(!observation.receive({1, 10, false, 0, 1, 7}, start + 2ms, timings));
	assert(!observation.receive({2, 10, false, 0, 1, 8}, start + 2ms, timings));
	assert(observation.receive({3, 10, false, 0, 1, 7}, start + 3ms, timings));
	observation.write_failed(generation, start + 4ms, timings);
	assert(timings.higher_term_response.snapshot().count == 1 && timings.write_failure.snapshot().count == 0);
	request.rpc = 11;
	auto old = observation.emit(request, start + 5ms, timings);
	request.rpc = 12;
	generation = observation.emit(request, start + 6ms, timings);
	observation.write_failed(old, start + 7ms, timings);
	assert(observation.pending() && timings.superseded.snapshot().count == 1);
	observation.expire(start + 8ms, 3ms, timings);
	assert(observation.pending());
	observation.expire(start + 9ms, 3ms, timings);
	assert(!observation.pending() && timings.timeout.snapshot().count == 1);
	assert(!observation.receive({2, 12, true, 0, 1, 7}, start + 10ms, timings));
	request.rpc = 13;
	generation = observation.emit(request, start + 11ms, timings);
	assert(observation.receive({2, 13, true, 0, 1, 7}, start + 12ms, timings));
	observation.write_failed(generation, start + 13ms, timings);
	assert(timings.response.snapshot().count == 1 && timings.response.snapshot().maximum_ns == 1000000);
	request.rpc = 14;
	observation.emit(request, start + 14ms, timings);
	observation.abandon(start + 15ms, timings);
	observation.abandon(start + 16ms, timings);
	assert(timings.disconnected.snapshot().count == 1);
	request.rpc = 15;
	generation = observation.emit(request, start + 17ms, timings);
	observation.write_failed(generation, start + 18ms, timings);
	assert(timings.write_failure.snapshot().count == 1 && !observation.pending());
	assert(timings.attempts.get() == 7 && timings.observations.get() == 6 &&
		   timings.duplicate_attempts.get() == 1);
}
