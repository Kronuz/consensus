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

// A full rpc_bytes window over a slow path (docs/mechanisms/append-pacing.md): three Cores at
// default Timing and Flow, joined by links that deliver each direction's messages in order at a
// fixed byte rate after 20 ms, as TCP does under netem loss or reordering (cubic at 1% loss and
// 40 ms round trip moves about 0.5 MB/s). The leader keeps 32 commands of 64 KiB uncommitted, a
// full 2 MiB append. A heartbeat or vote waits behind any append already on the link, so an
// append that takes longer than the election timeout to cross starves the followers.
#include "consensus/core.h"
#include "consensus/membership.h"

#include <cstdio>
#include <deque>
#include <map>
#include <memory>
#include <string>

using namespace kronuz::consensus;

namespace {

int checks = 0, failures = 0;
void check(bool value, const char* what) {
	++checks;
	if (!value) { ++failures; std::fprintf(stderr, "FAIL: %s\n", what); }
}

constexpr std::uint64_t one_way = 20;  // ms
constexpr std::size_t command = 64 << 10, in_flight = 32;

FixedConfiguration config_of(NodeId local) {
	FixedConfiguration c; c.cluster[0] = 'W'; c.configuration[0] = 'S'; c.local = local;
	c.voters = {1, 2, 3};
	return c;
}
std::size_t wire_bytes(const Message& m) {
	std::size_t bytes = 80;
	if (auto r = std::get_if<AppendRequest>(&m)) { for (const auto& e : r->entries) { bytes += 32 + e.payload.size(); } }
	return bytes;
}

struct Cluster {
	struct Arrival { std::uint64_t at; NodeId from; Message message; };
	std::map<NodeId, std::unique_ptr<Core>> cores;
	std::map<std::pair<NodeId, NodeId>, std::deque<Arrival>> links;  // (from, to), in arrival order
	std::map<std::pair<NodeId, NodeId>, std::uint64_t> busy_until;   // when each link's last byte is out
	std::uint64_t rate;  // bytes per ms on each link
	std::uint64_t now = 0;
	RequestId request = 0;
	std::size_t elections = 0, largest = 0;  // leaders elected since the load began; largest append payload

	explicit Cluster(std::uint64_t bytes_per_ms) : rate(bytes_per_ms) {
		for (NodeId id = 1; id <= 3; ++id) {
			RecoveredState s; s.configuration = config_of(id);
			cores[id] = std::make_unique<Core>(config_of(id), std::move(s), limits(), Timing{});
			consume(id, cores[id]->step(Tick{0, delay(id)}));
			consume(id, cores[id]->step(Start{}));
		}
	}
	static std::uint64_t delay(NodeId id) { return 1000 + 300 * id; }
	// The defaults, with room for a run's whole log: nothing checkpoints here.
	static Limits limits() { Limits l; l.log_bytes = std::size_t{256} << 20; return l; }
	void consume(NodeId id, Actions actions) {
		for (auto& a : actions) {
			if (auto p = std::get_if<Persist>(&a)) {
				consume(id, cores[id]->step(Persisted{p->token}));
			} else if (auto s = std::get_if<Send>(&a)) {
				if (auto r = std::get_if<AppendRequest>(&s->message)) {
					std::size_t bytes = 0; for (const auto& e : r->entries) { bytes += e.payload.size(); }
					largest = std::max(largest, bytes);
				}
				auto& free = busy_until[{id, s->peer}];
				free = std::max(free, now) + wire_bytes(s->message) / rate;
				links[{id, s->peer}].push_back({free + one_way, id, std::move(s->message)});
			} else if (auto c = std::get_if<Committed>(&a)) {
				consume(id, cores[id]->step(Applied{c->entries.back().index}));
			} else if (auto r = std::get_if<RoleChanged>(&a)) {
				elections += r->role == Role::Leader;
			}
		}
	}
	NodeId leader() const {
		for (const auto& [id, core] : cores) { if (core->role() == Role::Leader) { return id; } }
		return 0;
	}
	// One millisecond: every node ticks, due messages arrive, and the leader tops its log up to
	// `load` uncommitted commands.
	void step(std::size_t load) {
		++now;
		for (auto& [id, core] : cores) { consume(id, core->step(Tick{now, delay(id)})); }
		for (auto& [key, queue] : links) {
			while (!queue.empty() && queue.front().at <= now) {
				auto arrival = std::move(queue.front()); queue.pop_front();
				consume(key.second, cores[key.second]->step(Receive{arrival.from, std::move(arrival.message)}));
			}
		}
		if (auto l = leader(); l && load) {
			auto& core = *cores[l];
			while (core.last_index() - core.committed() < load && !core.busy()) {
				const auto last = core.last_index();
				consume(l, core.step(Propose{++request, std::string(command, 'x')}));
				if (core.last_index() == last) { break; }  // refused
			}
		}
	}
	void run(std::uint64_t ms, std::size_t load) { for (std::uint64_t t = 0; t < ms; ++t) { step(load); } }
	Index committed() const { auto l = leader(); return l ? cores.at(l)->committed() : 0; }
};

// 600 bytes per ms: a 2 MiB append takes 3.5 s to cross, beyond election_max. The leader keeps
// its followers and commits every second of a 30 s load, near the path's rate.
void slow_path() {
	Cluster c(600);
	c.run(3000, 0);
	check(c.leader() != 0, "a leader is elected");
	c.elections = 0;
	Index before = c.committed();
	bool steady = true;
	for (int second = 0; second < 30; ++second) {
		c.run(1000, in_flight);
		const auto now = c.committed();
		steady = steady && now > before;
		before = now;
	}
	const double per_s = double(c.committed()) / 30;
	std::printf("slow path: %zu elections, %.1f commits/s (the path carries %.1f), largest append %zu KiB\n",
		c.elections, per_s, 600.0 * 1000 / command, c.largest >> 10);
	check(c.elections == 0, "no election under load");
	check(steady, "the commit index advances every second");
	check(per_s >= 0.6 * 600 * 1000 / command, "commits at least 60% of the path's rate");
}

// A fast path keeps large appends: pacing grows the frame toward rpc_bytes.
void fast_path() {
	Cluster c(50000);
	c.run(3000, 0);
	c.elections = 0; c.largest = 0;
	c.run(2000, in_flight);
	std::printf("fast path: %zu elections, largest append %zu KiB\n", c.elections, c.largest >> 10);
	check(c.elections == 0, "no election on a fast path");
	check(c.largest >= Limits{}.rpc_bytes / 2, "a fast path paces up to rpc_bytes appends");
}

// A path that slows under a grown frame: 1 MiB at 1,500 bytes per ms takes 0.7 s, past
// rpc_timeout, so such a flight times out while its first copy still crosses, and its late
// answer, for a superseded flight, paces nothing. The timeout paces the peer from the floor
// again, and the group commits near the slower path's rate.
void slowed_path() {
	Cluster c(50000);
	c.run(3000, 0);
	c.run(1000, in_flight);
	check(c.largest >= Limits{}.rpc_bytes / 2, "the frame grew on the fast path");
	c.rate = 1500; c.elections = 0;
	c.run(2000, in_flight);
	const auto before = c.committed();
	c.run(10000, in_flight);
	const double per_s = double(c.committed() - before) / 10;
	std::printf("slowed path: %zu elections, %.1f commits/s (the path carries %.1f)\n", c.elections, per_s, 1500.0 * 1000 / command);
	check(c.elections == 0, "no election once the path slows");
	check(per_s >= 0.6 * 1500 * 1000 / command, "commits at least 60% of the slower path's rate");
}

}  // namespace

int main() {
	slow_path();
	fast_path();
	slowed_path();
	std::printf("full_window_stall_test: %d checks, %d failures\n", checks, failures);
	return failures ? 1 : 0;
}
