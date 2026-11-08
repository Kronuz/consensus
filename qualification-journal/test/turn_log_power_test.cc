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

// T9 (strata Track C): RFTW0001 under power loss. A randomized multi-group workload over
// an in-memory filesystem whose unsynced 512-byte sectors each persist or not, and whose
// namespace changes persist only after a directory sync. Power is cut after every I/O
// operation of each run, and at every third point again during the recovery that follows.
// Segments recycle, so stale blocks from an earlier use are present (condition 10).
#include "journal/memory_io.h"
#include "journal/turn_log.h"
#include <cstdlib>
#include <iostream>
#include <map>
#include <set>
#include <random>

namespace j = kronuz::journal;

namespace {
int checks = 0, failures = 0;
void check(bool value, const std::string& what) {
	++checks;
	if (!value) { ++failures; if (failures < 20) { std::cerr << "FAIL: " << what << '\n'; } }
}
using namespace j::testing;

const j::Identity identity{'P', 'O', 'W', 'E', 'R'};
using Model = std::map<std::uint64_t, std::vector<j::TurnRecord>>;
j::TurnRecord record(std::uint64_t group, std::uint64_t sequence, std::size_t size, j::TurnRecordKind kind) {
	std::string payload(size, '\0');
	for (std::size_t i = 0; i < size; ++i) { payload[i] = static_cast<char>('a' + (group * 7 + sequence * 3 + i) % 26); }
	return {group, sequence, kind, payload};
}
struct Workload {
	std::mt19937_64 random; Model acknowledged; std::vector<j::TurnRecord> in_flight;
	explicit Workload(std::uint64_t seed) : random(seed) {}
	// One turn: one to three groups of six, records up to 700 bytes, one in five a checkpoint;
	// group 9 writes once and then idles, so its record must move forward.
	std::vector<j::TurnRecord> turn(const j::TurnLog& log) {
		std::vector<j::TurnRecord> batch; std::map<std::uint64_t, std::uint64_t> next;
		if (!log.sequence(9) && !acknowledged.contains(9)) { batch.push_back(record(9, 1, 100, j::TurnRecordKind::Batch)); return batch; }
		for (auto n = random() % 3 + 1; n; --n) {
			auto group = random() % 6 + 1;
			auto& sequence = next.try_emplace(group, log.sequence(group)).first->second;
			batch.push_back(record(group, ++sequence, random() % 700 + 1, random() % 5 ? j::TurnRecordKind::Batch : j::TurnRecordKind::Checkpoint));
		}
		return batch;
	}
	void acknowledge(const std::vector<j::TurnRecord>& batch) {
		for (const auto& r : batch) { if (r.kind == j::TurnRecordKind::Checkpoint) { acknowledged[r.group].clear(); } acknowledged[r.group].push_back(r); }
	}
	// Recovery holds every acknowledged record, and the in-flight turn whole or not at all.
	bool matches(const j::TurnRecovery& recovered) {
		if (recovered.groups == acknowledged) { return true; }
		auto with = acknowledged; for (const auto& r : in_flight) { if (r.kind == j::TurnRecordKind::Checkpoint) { with[r.group].clear(); } with[r.group].push_back(r); }
		if (recovered.groups == with) { acknowledge(in_flight); return true; }
		return false;
	}
};
const j::TurnLogOptions options{8 * 512, 512, 1, 3, 600};

// Runs `turns` turns, cutting power at operation `cut` (and at `second` during the
// recovery that follows), then reopens and continues for `after` more turns.
void run(std::uint64_t seed, std::uint64_t cut, std::uint64_t second, unsigned turns, unsigned after) {
	Disk disk; disk.random.seed(seed * 7919 + cut); MemoryIO io(disk); Workload work(seed);
	j::TurnRecovery recovered; bool lost_power = false;
	try {
		disk.cut = cut;
		j::TurnLog log(io, identity, recovered, options);
		for (unsigned t = 0; t < turns; ++t) {
			work.in_flight = work.turn(log); log.append(work.in_flight);
			work.acknowledge(work.in_flight); work.in_flight.clear();
			log.prepare();
		}
	} catch (const PowerLoss&) { lost_power = true; }
	disk.power_loss();
	if (second) {
		disk.cut = disk.operations + second;
		try { j::TurnLog log(io, identity, recovered, options); } catch (const PowerLoss&) {}
		disk.power_loss();
	}
	try {
		j::TurnLog log(io, identity, recovered, options);
		const auto at = "seed " + std::to_string(seed) + " cut " + std::to_string(cut) + " second " + std::to_string(second);
		check(!recovered.damaged && recovered.lost.empty(), "power loss alone damages nothing and loses no group: " + at);
		check(work.matches(recovered), "every acknowledged record survives, and the in-flight turn whole or not at all: " + at);
		work.in_flight.clear();
		for (unsigned t = 0; t < after; ++t) { auto batch = work.turn(log); log.append(batch); work.acknowledge(batch); }
	} catch (const std::exception& error) {
		check(false, "seed " + std::to_string(seed) + " cut " + std::to_string(cut) + ": " + error.what());
	}
	(void)lost_power;
}

// The blocks each record's fragments occupy, on a clean disk: what a damaged block may cost.
std::map<std::pair<std::uint64_t, std::uint64_t>, std::set<std::uint64_t>> groups_by_block(MemoryIO& io) {
	namespace d = j::turn_log_detail;
	std::map<std::pair<std::uint64_t, std::uint64_t>, std::set<std::uint64_t>> result;
	auto cursor = io.scan_directory();
	while (auto name = cursor->next()) {
		auto number = d::segment_number(*name); if (!number) { continue; }
		auto file = io.open_existing(*name);
		std::vector<d::Item> items; d::Manifests manifests; d::scan_segment(*file, identity, options, *number, items, manifests);
		for (const auto& item : items) {
			if (item.gap || !item.group) { continue; }
			std::string header(d::fragment_header, '\0');
			for (std::uint64_t position = item.offset;;) {
				if (position % options.block_bytes == 0) { std::string c(2, '\0'); file->read_at(position + 56, c); position += d::header_bytes(d::get16(c)); }
				auto room = options.block_bytes - position % options.block_bytes;
				if (room < d::fragment_header + 1) { position += room; continue; }
				file->read_at(position, header);
				result[{*number, position / options.block_bytes}].insert(item.group);
				auto type = static_cast<std::uint8_t>(header[6]); position += d::fragment_header + d::get16(std::string_view(header).substr(4));
				if (type == d::Full || type == d::Last) { break; }
			}
		}
	}
	return result;
}

// One corrupted block costs only the groups with records in it: every other group's
// acknowledged records survive, and the damage is reported.
void corrupted_blocks(std::uint64_t seed, std::uint64_t& blocks) {
	Disk clean; MemoryIO clean_io(clean); Workload work(seed); j::TurnRecovery recovered;
	{ j::TurnLog log(clean_io, identity, recovered, options); for (unsigned t = 0; t < 60; ++t) { auto b = work.turn(log); log.append(b); work.acknowledge(b); log.prepare(); } }
	clean.power_loss();  // everything acknowledged is durable
	auto truth = groups_by_block(clean_io);
	if (truth.empty()) { return; }
	// Damage to the last turn reads as a torn tail: recovery cannot tell it from a turn
	// never acknowledged, so only blocks before the last turn's first are corrupted.
	std::pair<std::uint64_t, std::uint64_t> last{std::numeric_limits<std::uint64_t>::max(), 0};
	{
		namespace d = j::turn_log_detail;
		std::uint64_t newest = 0; std::vector<std::pair<std::pair<std::uint64_t, std::uint64_t>, std::uint64_t>> firsts;
		auto cursor = clean_io.scan_directory();
		while (auto name = cursor->next()) {
			auto number = d::segment_number(*name); if (!number) { continue; }
			auto file = clean_io.open_existing(*name);
			std::vector<d::Item> items; d::Manifests manifests; d::scan_segment(*file, identity, options, *number, items, manifests);
			for (const auto& item : items) {
				if (item.gap || item.kind == d::open_record) { continue; }
				newest = std::max(newest, item.turn); firsts.push_back({{*number, item.offset / options.block_bytes}, item.turn});
			}
		}
		for (const auto& [where, turn] : firsts) { if (turn == newest) { last = std::min(last, where); } }
	}
	for (const auto& [where, groups] : truth) {
		if (where >= last) { continue; }
		Disk disk = clean; MemoryIO io(disk); ++blocks;
		auto name = j::turn_log_detail::segment_name(where.first);
		auto& inode = disk.inodes.at(disk.names.at(name));
		std::mt19937_64 random(seed * 131 + where.second);
		const auto offset = where.second * options.block_bytes + random() % options.block_bytes;
		inode.durable[offset] = static_cast<char>(inode.durable[offset] ^ 0x5a);
		const auto at = "seed " + std::to_string(seed) + " segment " + std::to_string(where.first) + " block " + std::to_string(where.second);
		try {
			j::TurnLog log(io, identity, recovered, options);
			bool only = std::all_of(recovered.lost.begin(), recovered.lost.end(), [&](std::uint64_t g) { return groups.contains(g); });
			bool kept = true;
			for (const auto& [group, records] : work.acknowledged) { if (!recovered.lost.contains(group) && recovered.groups[group] != records) { kept = false; } }
			check(only, "a corrupted block loses only groups with records in it: " + at);
			check(kept, "every other group keeps its acknowledged records: " + at);
			if (!kept && std::getenv("T9_DEBUG")) {
				std::cerr << "  damaged " << recovered.damaged << " lost:"; for (auto g : recovered.lost) std::cerr << ' ' << g;
				std::cerr << " truth:"; for (auto g : groups) std::cerr << ' ' << g; std::cerr << '\n';
				for (const auto& [group, records] : work.acknowledged) {
					if (recovered.lost.contains(group) || recovered.groups[group] == records) { continue; }
					auto& got = recovered.groups[group];
					std::cerr << "  group " << group << " acked " << records.size() << " [" << records.front().sequence << ".." << records.back().sequence << "] got " << got.size();
					if (!got.empty()) { std::cerr << " [" << got.front().sequence << ".." << got.back().sequence << "]"; }
					std::cerr << '\n';
				}
			}
		} catch (const std::exception& error) { check(false, at + ": " + error.what()); }
	}
}
}  // namespace

int main(int argc, char** argv) {
	const std::uint64_t seeds = argc > 1 ? std::stoull(argv[1]) : 8;
	std::uint64_t points = 0, seconds = 0;
	for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
		// How many operations an uninterrupted run makes.
		Disk disk; MemoryIO io(disk); Workload work(seed); j::TurnRecovery recovered;
		{
			j::TurnLog log(io, identity, recovered, options);
			for (unsigned t = 0; t < 60; ++t) { auto b = work.turn(log); log.append(b); work.acknowledge(b); log.prepare(); }
			check(log.stats().segments_recycled > 0 && log.stats().forwarded_records > 0, "the run recycles segments and moves records forward (condition 10)");
		}
		for (std::uint64_t cut = 1; cut <= disk.operations; ++cut, ++points) {
			const std::uint64_t second = cut % 3 == 0 ? cut % 7 + 1 : 0;
			seconds += second != 0;
			run(seed, cut, second, 60, 5);
		}
	}
	std::uint64_t blocks = 0;
	for (std::uint64_t seed = 1; seed <= seeds; ++seed) { corrupted_blocks(seed, blocks); }
	std::cout << blocks << " corrupted blocks; ";
	std::cout << points << " crash points (" << seconds << " with a second loss in recovery), " << checks << " checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
