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

// A group recovered with its store above `hard - control_pool` reclaims before Start, then starts
// or fails its open by name (docs/mechanisms/deadlines.md, "A recovered store over its control
// headroom"). A lone voter publishes a checkpoint of a 512 KiB image; the group is recovered in a
// new Host on the same disk with `hard` lowered so the control pool cannot fund the pre-Start
// Tick's campaign pack. `reclaims`: the store also holds the staged part of a 1 MiB image, from a
// checkpoint the process was killed in (a disk snapshot taken from the capture's read); reclaiming
// it funds the pool and the group leads. `refuses`: nothing is reclaimable and the open fails with
// fenced(over_headroom). At the code before the rule, neither open completes.
// `recovered_over_limit_test`.
#include "consensus/driver.h"
#include "faultlab/faultfs.h"

#include <algorithm>
#include <cstdio>
#include <functional>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace fl = kronuz::faultlab;

namespace {

int checks = 0, failures = 0;
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; if (failures < 40) { std::cerr << "FAIL: " << what << '\n'; } }
}
std::string what(std::exception_ptr error) {
	if (!error) { return "no error"; }
	try { std::rethrow_exception(error); } catch (const std::exception& e) { return e.what(); } catch (...) {}
	return "unknown";
}

std::shared_ptr<j::IO> host_io(fl::filesystem& disk) {
	const auto names = disk.list();
	if (std::find(names.begin(), names.end(), "host") == names.end()) { disk.make_directory("host"); }
	return std::make_shared<fl::io>(disk, "host");
}

constexpr Timing compact{20, 40, 100, 200, 60000};
const j::TurnLogOptions turn_options{16 * 1024, 1024, 1, 4, 4096};
const j::Identity host_identity{'H', 'O', 'S', 'T'};
constexpr GroupId g = 41;
constexpr std::uint64_t control_bytes = 256 * 1024, replacement_bytes = 4096, normal_bytes = 64 * 1024;

Limits limits() { Limits l; l.command_bytes = 1024; l.rpc_bytes = 4096; l.log_bytes = 64 * 1024; l.log_entries = 64; l.control_entries = 4; l.rpc_entries = 8; return l; }
const WorkerLimits scheduling{16, 128};

GroupOptions options(j::AdmissionLimits admission) {
	GroupOptions o;
	o.configuration.local = 1; o.configuration.voters = {1};
	o.configuration.cluster[0] = 'C'; o.configuration.configuration[0] = 'G';
	o.admission = admission; o.limits = limits(); o.timing = compact; o.scheduling = scheduling;
	return o;
}
// The first run's limits, far from the store's size.
const j::AdmissionLimits roomy{{256u << 20, 8192}, {control_bytes, 3}, {64u << 20, 4}, 5, 3};
// The recovery's: `hard` bytes as given, the control pool as before, a token replacement pool.
j::AdmissionLimits lowered(std::uint64_t hard) { return {{hard, 8192}, {control_bytes, 3}, {replacement_bytes, 1}, 5, 3}; }

// The bytes a shared group's pre-Start Tick reserves from its control pool: its campaign pack,
// each append's payload bound plus one (journal's append_plan for a turn-log batch).
std::uint64_t tick_pack() {
	const auto plan = plan_event(Tick{}, false, limits(), scheduling.retained);
	std::uint64_t bytes = 0;
	for (std::size_t i = 0; i < plan.count; ++i) { bytes += plan.appends[i].encoded_bytes + 1; }
	return bytes;
}

struct Sink {
	std::map<GroupId, Index> committed;
	std::vector<std::pair<GroupId, Fenced>> fenced;
	void operator()(GroupId, Send) {}
	void operator()(GroupId id, Committed n) { if (!n.entries.empty()) { committed[id] = std::max(committed[id], n.entries.back().index); } }
	void operator()(GroupId, RoleChanged) {}
	void operator()(GroupId, SendTransfer) {}
	void operator()(GroupId, ValidateImage) {}
	void operator()(GroupId, ActivateImage) {}
	void operator()(GroupId, CheckpointPublished) {}
	void operator()(GroupId, RepairCompleted) {}
	void operator()(GroupId id, Fenced n) { fenced.emplace_back(id, std::move(n)); }
	void operator()(GroupId, SnapshotNeeded) {}
	void operator()(GroupId, SnapshotTransmit) {}
	void operator()(GroupId, SnapshotReleased) {}
};
static_assert(sink<Sink>);

// One host and one loop on an inline_scheduler, its turns and IO completed inline.
struct Loop {
	fl::filesystem& disk;
	task::inline_scheduler scheduler;
	j::InlineCompletion io;
	std::unique_ptr<j::Host> host;
	Sink sink;
	std::unique_ptr<Driver<Sink>> driver;
	task::cancel_source stop;
	bool turns_done = false, driver_done = false;
	explicit Loop(fl::filesystem& d) : disk(d) {
		host = std::make_unique<j::Host>(host_io(disk), host_identity, j::HostOptions{1, turn_options});
		driver = std::make_unique<Driver<Sink>>(Seams{task::scheduler_ref::of(scheduler), j::completion_ref::of(io), *host, 0, 7}, sink);
		auto ref = task::scheduler_ref::of(scheduler);
		task::spawn(ref, host->driver(0).run(j::completion_ref::of(io), stop.token()), [this](std::exception_ptr) { turns_done = true; });
		task::spawn(ref, driver->run(stop.token()), [this](std::exception_ptr) { driver_done = true; });
		run();
	}
	~Loop() {
		stop.request(task::cancel_reason::shutdown); run();
		check(turns_done && driver_done, "the turn driver's and the Driver's runs end at shutdown");
		driver.reset(); host.reset();
	}
	// Runs what is queued, failing (not hanging) if the loop never settles.
	void run() {
		std::size_t budget = 1'000'000;
		scheduler.run_until([&] { return --budget == 0; });
		if (!budget) { check(false, "the loop spins"); std::exit(1); }
	}
	// Moves the clock through `span` ns in 1 ms passes, each run to rest.
	void pass(std::uint64_t span) { for (std::uint64_t at = 0; at < span; at += 1'000'000) { scheduler.advance(1'000'000); run(); } }
	struct Started { bool done = false; std::exception_ptr error; };
	std::shared_ptr<Started> start(task::task<void> work) {
		auto state = std::make_shared<Started>();
		task::spawn(task::scheduler_ref::of(scheduler), std::move(work), [state](std::exception_ptr e) { state->done = true; state->error = e; });
		run();
		return state;
	}
	template <class T> std::optional<T> settle(task::task<T> work, const std::string& name) {
		struct Result { bool done = false; std::exception_ptr error; std::optional<T> value; };
		auto result = std::make_shared<Result>();
		task::spawn(task::scheduler_ref::of(scheduler), std::move(work), [result](std::exception_ptr e, std::optional<T> v) { result->done = true; result->error = e; result->value = std::move(v); });
		run();
		check(result->done && !result->error, name + ": " + (result->done ? what(result->error) : "not done"));
		return result->value;
	}
	void settle(task::task<void> work, const std::string& name) {
		auto state = start(std::move(work));
		check(state->done && !state->error, name + ": " + (state->done ? what(state->error) : "not done"));
	}
	// A command placed, committed and applied: Core delivers one committed range per Applied.
	void write(const std::string& command) {
		auto placed = settle(driver->propose(g, command), "propose " + command);
		for (Index done = 0; sink.committed[g] > done;) { done = sink.committed[g]; driver->applied(g, Applied{done}); run(); }
		check(placed && sink.committed[g] >= placed->index, command + " commits");
	}
};

// A capture of `size` bytes; `at_read` runs once the bytes read pass `trigger`.
struct Capture {
	std::size_t size, at = 0; Index through; Term term;
	std::size_t trigger = ~std::size_t(0);
	std::function<void()> at_read{};
	CaptureMetadata metadata() const { return {0, through, term, 0}; }
	std::size_t read(std::span<char> out) {
		if (at >= trigger && at_read) { std::exchange(at_read, nullptr)(); }
		auto n = std::min(out.size(), size - at);
		std::fill_n(out.data(), n, char('a' + at % 26)); at += n; return n;
	}
};
task::task<CheckpointPublished> checkpoint(Loop& loop, Capture capture) {
	capture.through = loop.sink.committed[g]; capture.term = loop.driver->state(g)->term;
	return loop.driver->checkpoint(g, 1u << 20, std::move(capture));
}

// The group's artifacts on `disk` and their bytes.
std::map<std::string, std::uint64_t> artifacts(fl::filesystem& disk) {
	char name[32]; std::snprintf(name, sizeof name, "g%016llx-artifact-", static_cast<unsigned long long>(g));
	std::map<std::string, std::uint64_t> out;
	for (const auto& n : disk.list("host")) { if (n.starts_with(name)) { out[n] = disk.view("host/" + n).size(); } }
	return out;
}
std::uint64_t bytes(const std::map<std::string, std::uint64_t>& files) {
	std::uint64_t total = 0;
	for (const auto& [n, b] : files) { total += b; }
	return total;
}

// The first run: a lone voter commits a command and publishes a checkpoint of a 512 KiB image.
void seed(Loop& loop) {
	loop.driver->open(g, options(roomy));
	loop.settle(loop.driver->create(g, j::Identity{'O', 'L'}), "create");
	loop.write("a");
	loop.settle(checkpoint(loop, Capture{512 * 1024}), "the first checkpoint");
}

// Recovers g in a new Host on `disk` with `hard` bytes, for `span` ns of 1 ms passes; the open's outcome.
std::shared_ptr<Loop::Started> recover(Loop& loop, std::uint64_t hard, std::uint64_t span) {
	loop.driver->open(g, options(lowered(hard)));
	auto restore = [](j::ArtifactReader&) {};
	auto opened = loop.start(loop.driver->recover(g, restore));
	loop.pass(span);
	return opened;
}

// A kill mid-checkpoint leaves part of a 1 MiB image staged; the recovery is above its control
// headroom only by it. Reclaiming it before Start funds the pool: the open completes, the group leads and
// commits, and the staged image is gone while the published checkpoint stays.
void reclaims() {
	fl::filesystem disk;
	std::optional<fl::filesystem> killed;
	std::map<std::string, std::uint64_t> published;
	{
		Loop loop(disk);
		seed(loop);
		published = artifacts(disk);
		loop.write("b");
		Capture staged{1u << 20}; staged.trigger = 768 * 1024; staged.at_read = [&] { killed.emplace(disk.snapshot()); };
		loop.settle(checkpoint(loop, std::move(staged)), "the second checkpoint");
	}
	check(bool(killed), "the capture's read took the disk's snapshot");
	if (!killed) { return; }
	const auto before = artifacts(*killed);
	const auto kept = bytes(published), staged = bytes(before) - kept;
	const auto hard = kept + control_bytes + replacement_bytes + normal_bytes;  // room for a command once reclaimed
	check(published.size() == 2 && before.size() == 3, "the snapshot holds the published bundle and image and a staged image: " + std::to_string(before.size()));
	check(bytes(before) > hard - tick_pack(), "the staged image puts the store above its control headroom: staged " + std::to_string(staged));
	std::printf("reclaims: published %llu bytes, staged %llu, hard %llu, pre-Start pack %llu\n", (unsigned long long)kept, (unsigned long long)staged, (unsigned long long)hard, (unsigned long long)tick_pack());
	Loop again(*killed);
	auto opened = recover(again, hard, 200'000'000);
	check(opened->done && !opened->error, "the open completes: " + (opened->done ? what(opened->error) : "never, within 200 ms"));
	if (!opened->done || opened->error) { return; }
	check(artifacts(*killed) == published, "reclaim removed the staged image and kept the published checkpoint");
	auto state = again.driver->state(g);
	check(state && state->role == Role::Leader, "the lone voter leads");
	again.write("c");
}

// Nothing is reclaimable: a whole reclaim pass leaves the pool short of the pre-Start Tick, and
// the open fails with fenced(over_headroom), the Sink hearing the same reason. The disk keeps
// the checkpoint.
void refuses() {
	fl::filesystem disk;
	{ Loop loop(disk); seed(loop); }
	const auto before = artifacts(disk);
	const auto used = bytes(before), pack = tick_pack();
	check(before.size() == 2 && used > control_bytes + replacement_bytes, "the published checkpoint outweighs the pools: " + std::to_string(used));
	Loop again(disk);
	auto opened = recover(again, used + pack - 1, 200'000'000);
	check(opened->done, "the open completes within 200 ms");
	check(what(opened->error) == over_headroom, "it fails with fenced(over_headroom): " + what(opened->error));
	bool typed = false;
	try { if (opened->error) { std::rethrow_exception(opened->error); } } catch (const fenced&) { typed = true; } catch (...) {}
	check(typed, "the error is fenced");
	check(again.sink.fenced.size() == 1 && again.sink.fenced[0].first == g && again.sink.fenced[0].second.reason == over_headroom, "the Sink hears the fence once, by name");
	check(artifacts(disk) == before, "the store keeps its checkpoint");
}

}  // namespace

int main(int argc, char** argv) {
	const std::map<std::string, void (*)()> cases{{"reclaims", reclaims}, {"refuses", refuses}};
	if (argc > 1) {
		auto found = cases.find(argv[1]);
		if (found == cases.end()) { std::cerr << "unknown case " << argv[1] << '\n'; return 2; }
		found->second();
	} else {
		for (const auto& [name, run] : cases) { std::cerr << "case " << name << '\n'; run(); }
	}
	std::cout << checks << " recovered over-limit checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
