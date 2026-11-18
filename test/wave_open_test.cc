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

// Many lone voters opened at once on one host's loop: each opens, creates, proposes one
// command and applies it, all of a wave started before the loop runs, as a host opening
// its groups does. Every group must commit its command with no fence.
//   wave_open_test [groups [wave]]   (64 groups, all in one wave, by default)
#include "consensus/driver.h"
#include "journal/memory_io.h"

#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace jt = kronuz::journal::testing;

namespace {

int checks = 0, failures = 0;
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; if (failures < 20) { std::fprintf(stderr, "FAIL: %s\n", what.c_str()); } }
}
std::string what(std::exception_ptr error) {
	if (!error) { return "no error"; }
	try { std::rethrow_exception(error); } catch (const std::exception& e) { return e.what(); } catch (...) {}
	return "unknown";
}

// The test fixtures' turn log (16 KiB segments of 1 KiB blocks), as strata's group tests use.
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{256u << 20, 8192}, {256 * 1024, 3}, {64u << 20, 4}, 32, 3};
const j::Identity host_identity{'W', 'A', 'V', 'E'};

GroupOptions options() {
	GroupOptions o;
	o.configuration.local = 1; o.configuration.voters = {1};
	o.configuration.cluster[0] = 'C'; o.configuration.configuration[0] = 'G';
	o.admission = admission; o.timing = Timing{20, 40, 100, 200, 60000}; o.scheduling = WorkerLimits{16, 128};
	o.limits.command_bytes = 4096; o.limits.rpc_bytes = 16384; o.limits.log_bytes = 64 * 1024;
	o.limits.log_entries = 64; o.limits.control_entries = 4; o.limits.rpc_entries = 8;
	return o;
}

struct Sink {
	std::map<GroupId, Index> committed;
	std::map<GroupId, std::vector<std::string>> commands;
	std::map<GroupId, std::string> fenced;
	void operator()(GroupId, Send) {}
	void operator()(GroupId g, Committed n) {
		if (n.entries.empty()) { return; }
		committed[g] = n.entries.back().index;
		for (const auto& e : n.entries) { if (e.kind == EntryKind::Command) { commands[g].push_back(e.payload); } }
	}
	void operator()(GroupId, RoleChanged) {}
	void operator()(GroupId, ActivateInstall) {}
	void operator()(GroupId, CheckpointPublished) {}
	void operator()(GroupId, RepairCompleted) {}
	void operator()(GroupId g, Fenced n) { fenced.emplace(g, std::move(n.reason)); }
	void operator()(GroupId, SnapshotNeeded) {}
	void operator()(GroupId, SnapshotTransmit) {}
	void operator()(GroupId, SnapshotReleased) {}
};
static_assert(sink<Sink>);

struct Row { bool done = false; std::exception_ptr error; };

task::task<void> open_row(Driver<Sink>& driver, GroupId id, j::Identity identity) {
	co_await driver.create(id, identity);
	co_await driver.propose(id, "P|" + std::to_string(id));
}

}  // namespace

int main(int argc, char** argv) {
	const std::size_t groups = argc > 1 ? std::stoull(argv[1]) : 64;
	const std::size_t wave = argc > 2 ? std::stoull(argv[2]) : groups;
	if (!groups || !wave) { std::fprintf(stderr, "groups and wave must be positive\n"); return 2; }

	jt::Disk disk;
	task::inline_scheduler scheduler;
	auto ref = task::scheduler_ref::of(scheduler);
	j::InlineCompletion io;
	j::Host host(std::make_shared<jt::MemoryIO>(disk, "host/"), host_identity, j::HostOptions{1, turn_options});
	Sink sink;
	Driver<Sink> driver(Seams{ref, j::completion_ref::of(io), host, 0, 7}, sink);
	task::cancel_source stop;
	bool turns_done = false, driver_done = false;
	std::exception_ptr turns_error;
	task::spawn(ref, host.driver(0).run(j::completion_ref::of(io), stop.token()), [&](std::exception_ptr e) { turns_done = true; turns_error = e; });
	task::spawn(ref, driver.run(stop.token()), [&](std::exception_ptr) { driver_done = true; });
	const auto run = [&] {
		std::size_t budget = 10'000'000;
		scheduler.run_until([&] { return --budget == 0; });
		if (!budget) { check(false, "the loop never settles"); }
	};
	run();

	std::map<GroupId, Row> rows;
	for (std::size_t first = 0; first < groups; first += wave) {
		const std::size_t last = std::min(groups, first + wave);
		for (std::size_t i = first; i < last; ++i) {
			const GroupId id = 100 + i;
			driver.open(id, options());
			j::Identity identity{};
			for (unsigned b = 0; b < 8; ++b) { identity[b] = char(id >> (8 * b)); }
			identity[15] = 'W';
			auto& row = rows[id];
			task::spawn(ref, open_row(driver, id, identity), [&row](std::exception_ptr e) { row.done = true; row.error = e; });
		}
		// The application applies each committed range once the loop settles.
		for (bool more = true; more;) {
			run();
			more = false;
			for (std::size_t i = first; i < last; ++i) {
				const GroupId id = 100 + i;
				auto state = driver.state(id);
				auto found = sink.committed.find(id);
				if (found != sink.committed.end() && state && state->applied < found->second && !sink.fenced.contains(id)) {
					driver.applied(id, Applied{found->second}); more = true;
				}
			}
		}
	}

	std::size_t opened = 0, fenced = 0;
	std::string first_error, first_fence;
	for (const auto& [id, row] : rows) {
		const auto& commands = sink.commands[id];
		const bool good = row.done && !row.error && commands.size() == 1 && commands[0] == "P|" + std::to_string(id);
		opened += good;
		if (row.error && first_error.empty()) { first_error = "group " + std::to_string(id) + ": " + what(row.error); }
	}
	for (const auto& [id, reason] : sink.fenced) {
		++fenced;
		if (first_fence.empty()) { first_fence = "group " + std::to_string(id) + ": " + reason; }
	}
	std::printf("%zu groups in waves of %zu: %zu opened and committed, %zu fenced; turn driver %s\n", groups, wave, opened, fenced,
		host.driver(0).failed() ? ("failed: " + what(turns_error)).c_str() : "healthy");
	if (!first_error.empty()) { std::printf("first error: %s\n", first_error.c_str()); }
	if (!first_fence.empty()) { std::printf("first fence: %s\n", first_fence.c_str()); }
	check(opened == groups, "every group opens, creates and commits its one command");
	check(fenced == 0, "no group fences");
	check(!host.driver(0).failed(), "the turn driver stays healthy");

	stop.request(task::cancel_reason::shutdown);
	run();
	check(turns_done && driver_done, "the turn driver's and the Driver's runs end at shutdown");
	std::printf("%d wave checks, %d failures\n", checks, failures);
	return failures ? 1 : 0;
}
