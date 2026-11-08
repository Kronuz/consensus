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

// Stores on the shared backend: several groups' batches and checkpoint records in one
// turn log, their artifacts in directories of their own, all on one in-memory disk.
// Power is cut after every I/O operation of a randomized run: every acknowledged batch
// and checkpoint survives, nothing unstaged appears, and the groups carry on. Checkpoints
// cross every publication step (artifacts durable, record staged, barrier, reclamation),
// which is Condition 25's ordering under T9.
#include "journal/memory_io.h"
#include "journal/store.h"
#include <iostream>
#include <map>
#include <random>

namespace j = kronuz::journal;
namespace jt = kronuz::journal::testing;

namespace {
int checks = 0, failures = 0;
std::uint64_t published = 0, acknowledged_batches = 0, reclaimed = 0, recovered_checkpoints = 0, migrated = 0;
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; if (failures < 20) { std::cerr << "FAIL: " << what << '\n'; } }
}
const j::Identity log_identity{'S', 'H', 'A', 'R', 'E', 'D'};
const j::TurnLogOptions options{16 * 1024, 1024, 1, 4, 4096};
const j::AdmissionLimits limits{{64u << 20, 4096}, {1u << 20, 8}, {8u << 20, 8}, 64, 3};
constexpr std::uint64_t group_count = 4;

j::Identity identity(std::uint64_t group) { j::Identity id{'G'}; id[1] = static_cast<char>(group); return id; }

std::string encode(const std::vector<std::string>& entries) {
	std::string bytes;
	for (const auto& entry : entries) { j::put32(bytes, static_cast<std::uint32_t>(entry.size())); bytes += entry; }
	return bytes;
}
std::vector<std::string> decode(std::string_view bytes) {
	std::vector<std::string> entries;
	while (!bytes.empty()) { auto size = j::get32(bytes); entries.emplace_back(bytes.substr(0, size)); bytes.remove_prefix(size); }
	return entries;
}

struct Group {
	std::shared_ptr<jt::MemoryIO> io;
	std::unique_ptr<j::Store> store;
	std::vector<std::string> attempted, acknowledged;  // every batch staged; those completed
};

struct Node {
	jt::Disk& disk;
	jt::MemoryIO log_io;
	std::unique_ptr<j::TurnDriver> driver;
	std::map<std::uint64_t, Group> groups;
	std::vector<std::pair<std::uint64_t, std::shared_ptr<j::StoreAppend>>> appends;
	explicit Node(jt::Disk& d) : disk(d), log_io(d, "log/") {}

	// Opens the driver and every group: recovers it, or creates it when it has no records.
	void open() {
		driver = std::make_unique<j::TurnDriver>(log_io, log_identity, options);
		for (std::uint64_t g = 1; g <= group_count; ++g) {
			auto& group = groups[g];
			group.store.reset();
			group.io = std::make_shared<jt::MemoryIO>(disk, "g" + std::to_string(g) + "/");
			group.store = std::make_unique<j::Store>(group.io, *driver, g, limits);
			const auto layout = j::Journal::layout(*group.io);
			if (layout == j::Journal::Layout::Legacy) {
				// R20: a per-group journal moves into the turn log; its batches are acknowledged.
				std::vector<std::string> recovered;
				group.store->migrate([&](std::uint64_t, std::string_view bytes) { recovered.emplace_back(bytes); },
					[&](const j::Frontier&, j::ArtifactReader& bundle, std::span<j::ArtifactReader>) {
						std::string bytes(static_cast<std::size_t>(bundle.descriptor().length), '\0');
						for (std::size_t at = 0; at < bytes.size();) { at += bundle.read_at(at, std::span<char>(bytes.data() + at, bytes.size() - at)); }
						recovered = decode(bytes);
					});
				while (!driver->idle()) { driver->commit(); }
				group.store->finish_migration();
				++migrated;
				group.acknowledged = recovered; group.attempted = recovered;
			} else if (layout == j::Journal::Layout::Shared && driver->has_group(g)) {
				std::vector<std::string> recovered;
				group.store->recover([&](std::uint64_t sequence, std::string_view bytes) {
					check(sequence == recovered.size() + 1, "batches replay in sequence");
					recovered.emplace_back(bytes);
				}, [&](const j::Frontier& frontier, j::ArtifactReader& bundle, std::span<j::ArtifactReader>) {
					std::string bytes(static_cast<std::size_t>(bundle.descriptor().length), '\0');
					for (std::size_t at = 0; at < bytes.size();) { at += bundle.read_at(at, std::span<char>(bytes.data() + at, bytes.size() - at)); }
					recovered = decode(bytes); ++recovered_checkpoints;
					check(recovered.size() == frontier.base_sequence, "a checkpoint holds the batches it covers");
				});
				group.acknowledged = recovered; group.attempted = recovered;
			} else {
				check(group.acknowledged.empty() && !driver->has_group(g), "a group without records acknowledged nothing");
				group.store->create(identity(g));
				group.acknowledged.clear(); group.attempted.clear();
			}
			while (!group.store->inventory_step(64).complete) {}
		}
	}

	// Every append that reported success is acknowledged, whenever power is lost after.
	void collect() {
		for (auto& [g, append] : appends) {
			if (!append->done()) { continue; }
			try { append->result(); } catch (...) { continue; }
			auto& group = groups[g];
			group.acknowledged.push_back(group.attempted[group.acknowledged.size()]); ++acknowledged_batches;
		}
		appends.clear();
	}
	void round(std::mt19937_64& random) {
		std::vector<std::pair<std::uint64_t, std::shared_ptr<j::StorePublication>>> publications;
		for (auto& [g, group] : groups) {
			auto& store = *group.store;
			if (random() % 8 == 0) {
				// A checkpoint of the acknowledged batches: artifacts first, then the record.
				auto id = store.reserve_replacement(1024, 1u << 20);
				if (id) {
					store.begin_artifact(*id, j::ArtifactPart::Application); store.write_chunk(*id, "application"); store.finish_artifact(*id);
					store.begin_artifact(*id, j::ArtifactPart::Bundle);
					auto bytes = encode(group.acknowledged);
					for (std::size_t at = 0; at < bytes.size(); at += 65536) { store.write_chunk(*id, std::string_view(bytes).substr(at, 65536)); }
					store.finish_artifact(*id);
					publications.emplace_back(g, store.begin_publication(*id, store.frontier().sequence));
					continue;
				}
			}
			if (random() % 4 != 0) {
				std::string batch = "g" + std::to_string(g) + "-" + std::to_string(group.attempted.size() + 1) + std::string(random() % 300, 'x');
				auto permit = store.reserve_append(j::AdmissionClass::Normal, batch.size());
				if (!permit) { continue; }
				group.attempted.push_back(batch);
				appends.emplace_back(g, store.begin_append(std::move(*permit), batch));
			}
		}
		while (!driver->idle()) { driver->commit(); }
		for (auto& [g, append] : appends) { check(append->done(), "an append completes at its turn's barrier"); append->result(); }
		collect();
		for (auto& [g, publication] : publications) {
			check(publication->done(), "a publication completes at its turn's barrier");
			publication->result(); ++published;
		}
		for (auto& [g, group] : groups) { if (random() % 4 == 0) { reclaimed += group.store->reclaim_step(16).removed; } }
	}
};

// Legacy groups: per-group journals with batches and a checkpoint, written without power
// cuts; the cuts start with the migration.
void legacy(jt::Disk& disk, std::mt19937_64& random, std::map<std::uint64_t, std::vector<std::string>>& written) {
	for (std::uint64_t g = 1; g <= group_count; ++g) {
		auto io = std::make_shared<jt::MemoryIO>(disk, "g" + std::to_string(g) + "/");
		j::Store store(io, limits);
		store.create(identity(g));
		while (!store.inventory_step(64).complete) {}
		auto& batches = written[g];
		for (int b = 0, n = int(random() % 30); b < n; ++b) {
			if (b == 10) {
				auto id = store.reserve_replacement(1024, 1u << 20);
				store.begin_artifact(*id, j::ArtifactPart::Application); store.write_chunk(*id, "application"); store.finish_artifact(*id);
				store.begin_artifact(*id, j::ArtifactPart::Bundle); store.write_chunk(*id, encode(batches)); store.finish_artifact(*id);
				store.publish(*id, store.frontier().sequence);
			}
			std::string batch = "legacy-" + std::to_string(g) + "-" + std::to_string(b);
			auto permit = store.reserve_append(j::AdmissionClass::Normal, batch.size());
			store.append(*permit, batch); batches.push_back(batch);
		}
	}
}

std::uint64_t run_migration(std::uint64_t seed, std::uint64_t cut) {
	jt::Disk disk; disk.random.seed(seed * 104729 + cut);
	std::mt19937_64 random(seed);
	std::map<std::uint64_t, std::vector<std::string>> written;
	legacy(disk, random, written);
	Node node(disk);
	for (auto& [g, batches] : written) { node.groups[g].acknowledged = batches; node.groups[g].attempted = batches; }
	try {
		disk.cut = cut;
		node.open();
		for (unsigned r = 0; r < 10; ++r) { node.round(random); }
	} catch (const jt::PowerLoss&) {}
	node.collect();
	const auto operations = disk.operations;
	disk.power_loss();
	const auto at = "migration seed " + std::to_string(seed) + " cut " + std::to_string(cut);
	try {
		Node again(disk);
		for (auto& [g, group] : node.groups) { again.groups[g].acknowledged = group.acknowledged; }
		again.open();
		for (auto& [g, group] : again.groups) {
			const auto& before = node.groups[g];
			bool kept = group.acknowledged.size() >= before.acknowledged.size() && group.acknowledged.size() <= before.attempted.size() &&
				std::equal(before.acknowledged.begin(), before.acknowledged.end(), group.acknowledged.begin());
			check(kept, "group " + std::to_string(g) + " keeps its legacy and migrated batches: " + at);
			check(j::Journal::layout(*group.io) == j::Journal::Layout::Shared, "a reopened group is shared: " + at);
		}
		for (unsigned r = 0; r < 6; ++r) { again.round(random); }
	} catch (const std::exception& error) { check(false, at + ": " + error.what()); }
	return operations;
}

std::uint64_t run(std::uint64_t seed, std::uint64_t cut, unsigned rounds) {
	jt::Disk disk; disk.random.seed(seed * 7919 + cut);
	std::mt19937_64 random(seed);
	Node node(disk);
	try {
		disk.cut = cut;
		node.open();
		for (unsigned r = 0; r < rounds; ++r) { node.round(random); }
	} catch (const jt::PowerLoss&) {}
	node.collect();
	const auto operations = disk.operations;
	disk.power_loss();
	const auto at = "seed " + std::to_string(seed) + " cut " + std::to_string(cut);
	try {
		Node again(disk);
		for (auto& [g, group] : node.groups) { again.groups[g].acknowledged = group.acknowledged; }
		again.open();
		check(!again.driver->recovered().damaged, "power loss alone damages nothing: " + at);
		for (auto& [g, group] : again.groups) {
			const auto& before = node.groups[g];
			bool kept = group.acknowledged.size() >= before.acknowledged.size() && group.acknowledged.size() <= before.attempted.size() &&
				std::equal(group.acknowledged.begin(), group.acknowledged.end(), before.attempted.begin());
			check(kept, "group " + std::to_string(g) + " keeps every acknowledged batch and invents none: " + at);
		}
		for (unsigned r = 0; r < 12; ++r) { again.round(random); }
		for (auto& [g, group] : again.groups) { check(!group.store->fenced(), "the groups carry on after recovery: " + at); }
	} catch (const std::exception& error) { check(false, at + ": " + error.what()); }
	return operations;
}
}  // namespace

int main(int argc, char** argv) {
	const std::uint64_t seeds = argc > 1 ? std::stoull(argv[1]) : 3;
	std::uint64_t points = 0;
	for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
		const auto total = run(seed, std::numeric_limits<std::uint64_t>::max(), 40);
		for (std::uint64_t cut = 1; cut <= total; ++cut, ++points) { run(seed, cut, 40); }
	}
	for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
		const auto total = run_migration(seed, std::numeric_limits<std::uint64_t>::max());
		for (std::uint64_t cut = 1; cut <= total; ++cut, ++points) { run_migration(seed, cut); }
	}
	std::cout << points << " crash points, " << checks << " shared store checks, " << failures << " failures (" << acknowledged_batches << " batches and "
		<< published << " checkpoints acknowledged, " << recovered_checkpoints << " checkpoints recovered, " << reclaimed << " artifacts reclaimed, " << migrated << " groups migrated)\n";
	return failures ? 1 : 0;
}
