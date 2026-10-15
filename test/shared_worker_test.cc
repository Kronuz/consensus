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

// D11 through the Worker: single-voter groups on one core persist through one turn log
// (journal's shared Store), proposing and checkpointing at random. Power is lost after
// every I/O operation of a run, on one in-memory disk that holds the turn log and every
// group's artifacts: each command a group delivered as Committed comes back, in order,
// from its checkpoint image and its replayed suffix, and the groups carry on.
#include "consensus/worker.h"
#include "journal/memory_io.h"
#include <iostream>
#include <map>
#include <random>

using namespace kronuz::consensus;
namespace j = kronuz::journal;
namespace jt = kronuz::journal::testing;

namespace {
int checks = 0, failures = 0;
std::uint64_t delivered_commands = 0, published_checkpoints = 0, restored_images = 0, migrated_groups = 0;
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; if (failures < 20) { std::cerr << "FAIL: " << what << '\n'; } }
}
constexpr Timing compact{20, 40, 100, 200, 60000};
constexpr std::uint64_t group_count = 4;
const j::Identity log_identity{'T', 'U', 'R', 'N', 'S'};
const j::TurnLogOptions options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits admission{{4u << 20, 4096}, {64 * 1024, 3}, {256 * 1024, 4}, 16, 3};

FixedConfiguration configuration(std::uint64_t group) {
	FixedConfiguration c; c.local = 1; c.voters = {1}; c.cluster[0] = 'C'; c.configuration[0] = static_cast<char>('A' + group); return c;
}
Limits limits() { Limits l; l.command_bytes = 1024; l.rpc_bytes = 4096; l.log_bytes = 64 * 1024; l.log_entries = 64; l.control_entries = 4; l.rpc_entries = 8; return l; }

// The application: the commands applied, in index order, and their terms.
struct Application {
	std::map<Index, std::pair<Term, std::string>> applied;
	std::string image(Index through) const {
		std::string bytes;
		for (const auto& [index, entry] : applied) {
			if (index > through) { break; }
			j::put64(bytes, index); j::put64(bytes, entry.first); j::put32(bytes, static_cast<std::uint32_t>(entry.second.size())); bytes += entry.second;
		}
		return bytes;
	}
	void restore(std::string_view bytes) {
		applied.clear();
		while (!bytes.empty()) {
			auto index = j::get64(bytes); auto term = j::get64(bytes); auto size = j::get32(bytes);
			applied[index] = {term, std::string(bytes.substr(0, size))}; bytes.remove_prefix(size);
		}
	}
	std::vector<std::string> commands() const {
		std::vector<std::string> out;
		for (const auto& [index, entry] : applied) { if (!entry.second.empty()) { out.push_back(entry.second); } }
		return out;
	}
};

struct Group {
	std::unique_ptr<Worker> worker;
	Application application;
	std::vector<std::string> acknowledged;  // commands delivered as Committed, in order
	std::optional<CheckpointId> checkpoint;
	std::string image;
	bool offered = false, started = false;
	Index acknowledged_through = 0;  // redelivered entries are not acknowledged again
};

struct Node {
	jt::Disk& disk;
	jt::MemoryIO log_io;
	std::unique_ptr<j::TurnDriver> driver;
	std::map<std::uint64_t, Group> groups;
	std::uint64_t now = 100;
	RequestId next_request = 1;
	explicit Node(jt::Disk& d) : disk(d), log_io(d, "log/") {}

	void open() {
		driver = std::make_unique<j::TurnDriver>(log_io, log_identity, options);
		for (std::uint64_t g = 1; g <= group_count; ++g) {
			auto& group = groups[g];
			group.worker.reset(); group.checkpoint.reset(); group.offered = false;
			auto io = std::make_shared<jt::MemoryIO>(disk, "g" + std::to_string(g) + "/");
			group.worker = std::make_unique<Worker>(io, *driver, g, configuration(g), admission, limits(), compact, WorkerLimits{16, 128});
			const auto layout = j::Journal::layout(*io);
			if (layout == j::Journal::Layout::Legacy || (layout == j::Journal::Layout::Shared && driver->has_group(g))) {
				group.application = {};
				group.worker->recover([&](j::ArtifactReader& reader) {
					std::string bytes(static_cast<std::size_t>(reader.descriptor().length), '\0');
					for (std::size_t at = 0; at < bytes.size();) { at += reader.read_at(at, std::span<char>(bytes.data() + at, bytes.size() - at)); }
					group.application.restore(bytes); ++restored_images;
				});
			} else {
				check(group.acknowledged.empty(), "a group without records delivered nothing");
				group.application = {};
				j::Identity identity{'W'}; identity[1] = static_cast<char>(g);
				group.worker->create(identity);
			}
			group.started = false;
		}
		// R20: migrated groups' records reach a barrier, then the turn log is authoritative.
		while (!driver->idle()) { driver->commit(); }
		for (auto& [g, group] : groups) { if (group.worker->migrating()) { group.worker->finish_migration(); ++migrated_groups; } }
	}
	void pump(std::uint64_t g) {
		auto& group = groups.at(g); auto& worker = *group.worker;
		worker.run_one(Tick{now, 100 + 7 * g});
		while (auto job = worker.take_io_operation()) { j::drive_synchronously(*job); worker.run_one(Tick{now, 100 + 7 * g}); }
		for (const auto& action : worker.take_actions()) {
			if (auto batch = std::get_if<Committed>(&action)) {
				for (const auto& entry : batch->entries) {
					group.application.applied[entry.index] = {entry.term, entry.kind == EntryKind::Command ? entry.payload : std::string()};
					if (entry.kind == EntryKind::Command && entry.index > group.acknowledged_through) { group.acknowledged.push_back(entry.payload); ++delivered_commands; }
					group.acknowledged_through = std::max(group.acknowledged_through, entry.index);
				}
				worker.applied({batch->entries.back().index});
			}
			published_checkpoints += std::holds_alternative<CheckpointPublished>(action);
		}
	}
	void round(std::mt19937_64& random) {
		now += 10;
		for (auto& [g, group] : groups) {
			auto& worker = *group.worker;
			pump(g);
			if (!worker.ready()) { continue; }
			if (!group.started) { group.started = worker.try_submit(Start{}) == SubmitResult::Accepted; continue; }
			if (group.checkpoint && !worker.checkpoint_active(*group.checkpoint)) { group.checkpoint.reset(); }
			if (!group.checkpoint && worker.applied_index() >= 2 && random() % 10 == 0) {
				const auto through = worker.applied_index();
				const auto found = group.application.applied.find(through);
				if (found != group.application.applied.end()) {
					group.checkpoint = worker.reserve_checkpoint(64 * 1024);
					if (group.checkpoint) {
						worker.attach_capture(*group.checkpoint, {next_request++, through, found->second.first, worker.epoch()});
						group.image = group.application.image(through); group.offered = false;
					}
				}
			}
			if (group.checkpoint && !group.offered && worker.checkpoint_active(*group.checkpoint)) {
				group.offered = worker.offer_application_chunk(*group.checkpoint, group.image, true) == SubmitResult::Accepted;
			}
			if (worker.role() == Role::Leader && random() % 2) {
				worker.try_submit(Propose{next_request++, "c" + std::to_string(g) + "-" + std::to_string(random() % 1000000)});
			}
			pump(g);
		}
		// As an asynchronous host does: the Workers keep running while the turn is written.
		while (!driver->idle()) {
			const auto& write = driver->begin();
			for (auto& [g, group] : groups) { pump(g); }
			try {
				if (write.file->write_at(write.offset, write.bytes) != write.bytes.size()) { throw std::runtime_error("short turn write"); }
				for (int i = 0; i < 4; ++i) { for (auto& [g, group] : groups) { pump(g); } }
				write.file->sync_data();
			} catch (...) { driver->failed(std::current_exception()); throw; }
			driver->completed();
		}
	}
};

// Legacy groups: Workers on per-group journals (the synchronous backend) commit commands
// before any power cut; each command delivered there is acknowledged.
void legacy(jt::Disk& disk, std::mt19937_64& random, std::map<std::uint64_t, Group>& out) {
	for (std::uint64_t g = 1; g <= group_count; ++g) {
		jt::MemoryIO io(disk, "g" + std::to_string(g) + "/");
		Worker worker(io, configuration(g), admission, limits(), compact, WorkerLimits{16, 128});
		j::Identity identity{'L'}; identity[1] = static_cast<char>(g);
		worker.create(identity);
		auto& group = out[g];
		bool started = false; std::uint64_t now = 100; RequestId request = 1000 * g;
		for (unsigned turn = 0; turn < 400 && group.acknowledged.size() < 5 + random() % 10; ++turn) {
			now += 10;
			worker.run_one(Tick{now, 100 + 7 * g});
			for (const auto& action : worker.take_actions()) {
				if (auto batch = std::get_if<Committed>(&action)) {
					for (const auto& entry : batch->entries) { if (entry.kind == EntryKind::Command) { group.acknowledged.push_back(entry.payload); } }
					group.acknowledged_through = batch->entries.back().index;
					worker.applied({batch->entries.back().index});
				}
			}
			if (!worker.ready()) { continue; }
			if (!started) { started = worker.try_submit(Start{}) == SubmitResult::Accepted; continue; }
			if (worker.role() == Role::Leader && !worker.busy()) { worker.try_submit(Propose{request++, "legacy-" + std::to_string(g) + "-" + std::to_string(random() % 1000000)}); }
		}
		check(!group.acknowledged.empty(), "a legacy group commits commands");
	}
}

std::uint64_t run_migration(std::uint64_t seed, std::uint64_t cut) {
	jt::Disk disk; disk.random.seed(seed * 7 + cut);
	std::mt19937_64 random(seed);
	Node node(disk);
	legacy(disk, random, node.groups);
	try {
		disk.cut = cut;
		node.open();
		for (unsigned r = 0; r < 30; ++r) { node.round(random); }
	} catch (const jt::PowerLoss&) {}
	const auto operations = disk.operations;
	disk.power_loss();
	const auto at = "migration seed " + std::to_string(seed) + " cut " + std::to_string(cut);
	try {
		Node again(disk); again.now = node.now + 1000; again.next_request = node.next_request;
		for (auto& [g, group] : node.groups) { again.groups[g].acknowledged = group.acknowledged; }
		again.open();
		for (unsigned r = 0; r < 60; ++r) { again.round(random); }
		for (auto& [g, group] : again.groups) {
			const auto& before = node.groups[g].acknowledged;
			auto recovered = group.application.commands();
			check(recovered.size() >= before.size() && std::equal(before.begin(), before.end(), recovered.begin()), "group " + std::to_string(g) + " keeps its legacy and later commands: " + at);
			check(j::Journal::layout(*std::make_shared<jt::MemoryIO>(disk, "g" + std::to_string(g) + "/")) == j::Journal::Layout::Shared, "a reopened group is shared: " + at);
		}
	} catch (const std::exception& error) { check(false, at + ": " + error.what()); }
	return operations;
}

std::uint64_t run(std::uint64_t seed, std::uint64_t cut, unsigned rounds) {
	jt::Disk disk; disk.random.seed(seed * 31 + cut);
	std::mt19937_64 random(seed);
	Node node(disk);
	try {
		disk.cut = cut;
		node.open();
		for (unsigned r = 0; r < rounds; ++r) { node.round(random); }
	} catch (const jt::PowerLoss&) {}
	const auto operations = disk.operations;
	disk.power_loss();
	const auto at = "seed " + std::to_string(seed) + " cut " + std::to_string(cut);
	try {
		Node again(disk); again.now = node.now; again.next_request = node.next_request;
		for (auto& [g, group] : node.groups) { again.groups[g].acknowledged = group.acknowledged; }
		again.open();
		check(!again.driver->recovered().damaged, "power loss alone damages nothing: " + at);
		for (unsigned r = 0; r < 60; ++r) { again.round(random); }
		for (auto& [g, group] : again.groups) {
			const auto& before = node.groups[g].acknowledged;
			auto recovered = group.application.commands();
			bool kept = recovered.size() >= before.size() && std::equal(before.begin(), before.end(), recovered.begin());
			check(kept, "group " + std::to_string(g) + " keeps every delivered command in order: " + at);
			check(!group.worker->fenced() && group.worker->committed() > 0, "group " + std::to_string(g) + " carries on: " + at);
		}
	} catch (const std::exception& error) { check(false, at + ": " + error.what()); }
	return operations;
}
}  // namespace

int main(int argc, char** argv) {
	const std::uint64_t seeds = argc > 1 ? std::stoull(argv[1]) : 1;
	const std::uint64_t stride = argc > 2 ? std::stoull(argv[2]) : 1;
	std::uint64_t points = 0;
	for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
		const auto total = run(seed, std::numeric_limits<std::uint64_t>::max(), 150);
		for (std::uint64_t cut = 1; cut <= total; cut += stride, ++points) { run(seed, cut, 150); }
	}
	for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
		const auto total = run_migration(seed, std::numeric_limits<std::uint64_t>::max());
		for (std::uint64_t cut = 1; cut <= total; cut += stride, ++points) { run_migration(seed, cut); }
	}
	std::cout << points << " crash points, " << checks << " shared worker checks, " << failures << " failures (" << delivered_commands
		<< " commands delivered, " << published_checkpoints << " checkpoints published, " << restored_images << " images restored, " << migrated_groups << " groups migrated)\n";
	return failures ? 1 : 0;
}
