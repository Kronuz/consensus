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

// RFTW0001 (turn_log.h): turns round-trip across blocks, segments, and epochs; the torn
// tail is dropped; damage before a later turn loses only the groups it may have held;
// checkpoints supersede; stale bytes of a recycled file never read as current.
#include "journal/turn_log.h"
#include "completion/posix.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>

namespace j = kronuz::journal;

namespace {
int checks = 0, failures = 0;
void check(bool value, const char* what) {
	++checks;
	if (!value) { ++failures; std::cerr << "FAIL: " << what << '\n'; }
}
template <class F> bool throws(F&& f) {
	try { f(); return false; } catch (const std::exception&) { return true; }
}
struct Directory {
	std::filesystem::path path;
	explicit Directory(const std::string& name) : path(std::filesystem::current_path() / ".scratch" / ("turn-log-" + name)) {
		std::filesystem::remove_all(path); std::filesystem::create_directories(path);
		std::filesystem::permissions(path, std::filesystem::perms::owner_all);
	}
	~Directory() { std::filesystem::remove_all(path); }
	std::filesystem::path segment(std::uint64_t number) const { return path / j::turn_log_detail::segment_name(number); }
	void flip(std::uint64_t number, std::uint64_t offset) const {
		std::fstream file(segment(number), std::ios::in | std::ios::out | std::ios::binary);
		file.seekg(static_cast<std::streamoff>(offset)); char c; file.get(c);
		file.seekp(static_cast<std::streamoff>(offset)); file.put(static_cast<char>(c ^ 0x5a));
	}
	void zero(std::uint64_t number, std::uint64_t offset, std::uint64_t count) const {
		std::fstream file(segment(number), std::ios::in | std::ios::out | std::ios::binary);
		file.seekp(static_cast<std::streamoff>(offset)); file << std::string(count, '\0');
	}
};
const j::Identity identity{'T', 'U', 'R', 'N'};
const j::TurnLogOptions small{8 * 512, 512, 1};  // 512-byte blocks, 4 KiB segments

using Model = std::map<std::uint64_t, std::vector<j::TurnRecord>>;
j::TurnRecord record(std::uint64_t group, std::uint64_t sequence, std::size_t size, j::TurnRecordKind kind = j::TurnRecordKind::Batch) {
	std::string payload(size, '\0');
	for (std::size_t i = 0; i < size; ++i) { payload[i] = static_cast<char>('a' + (group * 7 + sequence * 3 + i) % 26); }
	return {group, sequence, kind, payload};
}
// Appends turns of random groups and sizes, keeping the model of what each group holds.
std::uint64_t workload(j::TurnLog& log, Model& model, std::mt19937_64& random, unsigned turns, std::size_t largest) {
	std::uint64_t last = 0;
	for (unsigned t = 0; t < turns; ++t) {
		std::vector<j::TurnRecord> batch; std::map<std::uint64_t, std::uint64_t> next;
		for (unsigned n = random() % 4 + 1; n; --n) {
			auto group = random() % 5 + 1;
			auto& sequence = next.try_emplace(group, log.sequence(group)).first->second;
			batch.push_back(record(group, ++sequence, random() % largest));
		}
		last = log.append(batch);
		for (auto& r : batch) { model[r.group].push_back(r); }
	}
	return last;
}

void round_trip() {
	Directory directory("round-trip"); j::PosixIO io(directory.path); auto owner = io.acquire_owner(true);
	j::TurnRecovery recovered; Model model; std::mt19937_64 random(1);
	std::uint64_t last = 0;
	{
		j::TurnLog log(io, identity, recovered, small);
		check(recovered.epoch == 1 && recovered.last_turn == 0 && recovered.groups.empty(), "an empty directory opens the first epoch");
		last = workload(log, model, random, 60, 900);  // records cross blocks and turns cross segments
		check(log.segment() > 3 && log.stats().barriers == 61 && log.stats().turns == 60, "one barrier per turn, plus the open record's");
	}
	for (std::uint64_t epoch = 2; epoch <= 3; ++epoch) {
		j::TurnLog log(io, identity, recovered, small);
		check(recovered.epoch == epoch && recovered.last_turn == last && !recovered.damaged && recovered.lost.empty(), "reopening keeps every turn");
		check(recovered.groups == model, "every group's records read back in sequence");
		last = workload(log, model, random, 10, 300);
	}
}

void torn_tail() {
	Directory directory("torn"); j::PosixIO io(directory.path); auto owner = io.acquire_owner(true);
	j::TurnRecovery recovered; Model model; std::mt19937_64 random(2);
	std::uint64_t segment = 0, end = 0, before = 0;
	{
		j::TurnLog log(io, identity, recovered, small);
		workload(log, model, random, 5, 100);
		before = log.last_turn();
		auto start = log.stats().bytes_written;
		log.append(std::vector{record(9, 1, 300)});
		segment = log.segment(); end = log.stats().bytes_written - start;
	}
	// The last turn's tail never reached the disk: power was lost before its barrier.
	auto size = std::filesystem::file_size(directory.segment(segment));
	std::uint64_t written = 0;
	{
		std::ifstream file(directory.segment(segment), std::ios::binary); std::string bytes(size, '\0'); file.read(bytes.data(), static_cast<std::streamsize>(size));
		written = bytes.find_last_not_of('\0') + 1;
	}
	directory.zero(segment, written - 20, 20);
	j::TurnLog log(io, identity, recovered, small);
	check(recovered.last_turn == before && !recovered.groups.contains(9) && !recovered.damaged && recovered.groups == model, "a torn last turn is dropped whole");
	check(end > 20, "the torn turn spans more than the cut");
}

// An earlier epoch's torn last turn whose next block's header reached the disk: that
// header's manifest names the torn record, which no open kept. It names nothing
// acknowledged, so later opens, past later epochs' turns, lose no group over it.
void torn_turn_in_earlier_epoch() {
	Directory directory("torn-earlier"); j::PosixIO io(directory.path); auto owner = io.acquire_owner(true);
	j::TurnRecovery recovered; Model model; std::mt19937_64 random(8);
	std::uint64_t kept = 0;
	{
		j::TurnLog log(io, identity, recovered, small);
		for (std::uint64_t s = 1; s <= 3; ++s) { auto r = record(1, s, 40); log.append(std::vector{r}); model[1].push_back(r); }
		kept = log.sequence(1);
		// The turn crosses into the next block; only the bytes from that block on persist.
		auto write = log.stage(std::vector{record(1, kept + 1, 700)});
		const auto boundary = (write.offset / small.block_bytes + 1) * small.block_bytes;
		check(boundary < write.offset + write.bytes.size(), "the torn turn crosses a block boundary");
		auto tail = write.bytes.substr(static_cast<std::size_t>(boundary - write.offset));
		check(write.file->write_at(boundary, tail) == tail.size(), "the torn turn's tail is written");
		write.file->sync_data();
	}
	{
		j::TurnLog log(io, identity, recovered, small);
		check(!recovered.damaged && recovered.lost.empty() && recovered.groups == model, "the torn last turn is dropped whole");
		for (std::uint64_t s = 1; s <= 3; ++s) { auto r = record(2, s, 40); log.append(std::vector{r}); model[2].push_back(r); }
	}
	j::TurnLog log(io, identity, recovered, small);
	check(!recovered.damaged && recovered.lost.empty() && recovered.groups == model, "an earlier epoch's torn turn costs no group later");
}

void damage() {
	Directory directory("damage"); j::PosixIO io(directory.path); auto owner = io.acquire_owner(true);
	j::TurnRecovery recovered; std::uint64_t at = 0;
	{
		j::TurnLog log(io, identity, recovered, small);
		log.append(std::vector{record(4, 1, 20)});                   // intact, but nothing later
		log.append(std::vector{record(6, 1, 700)});                  // ends in the damaged block
		at = log.position();
		log.append(std::vector{record(1, 1, 300), record(2, 1, 300)});  // damaged below
		log.append(std::vector{record(3, 1, 300)});
		log.append(std::vector{record(1, 2, 300)});
	}
	directory.flip(1, at + 10);  // inside turn 2's first fragment header
	j::TurnLog log(io, identity, recovered, small);
	check(recovered.damaged && recovered.lost.contains(1), "the groups with records in the damaged block are lost");
	check(recovered.lost.contains(2) || recovered.groups[2] == std::vector{record(2, 1, 300)}, "a record of the damaged turn that reads back whole is kept");
	check(recovered.groups.contains(4) && !recovered.lost.contains(4), "a group without records in it is kept: the next block's manifest names the damaged block's groups");
	check(recovered.lost.contains(6), "a group whose record ends in the damaged block is lost with it");
	check(recovered.groups.contains(3) && recovered.groups[3] == std::vector{record(3, 1, 300)}, "a group whose records follow the damage is kept");
	check(throws([&] { log.append(std::vector{record(1, 3, 10)}); }), "a lost group rejoins under a new id");
}

void checkpoints_and_guards() {
	Directory directory("checkpoints"); j::PosixIO io(directory.path); auto owner = io.acquire_owner(true);
	j::TurnRecovery recovered;
	{
		j::TurnLog log(io, identity, recovered, small);
		log.append(std::vector{record(1, 1, 10), record(1, 2, 10)});
		log.append(std::vector{record(1, 3, 10, j::TurnRecordKind::Checkpoint), record(1, 4, 10, j::TurnRecordKind::HardState)});
		log.append(std::vector{record(7, 40, 10, j::TurnRecordKind::Checkpoint)});  // a group starting from a checkpoint
		check(throws([&] { log.append(std::vector{record(1, 6, 10)}); }), "a group's sequence is contiguous");
		check(throws([&] { log.append(std::vector{record(0, 1, 10)}); }), "group 0 is reserved");
		check(throws([&] { log.append(std::vector{record(8, 3, 10)}); }), "a new group starts at 1 or with a checkpoint");
		check(throws([&] { log.append(std::vector{record(2, 1, 5000)}); }) && log.last_turn() == 3, "a turn larger than a segment is refused before any write");
	}
	j::TurnLog log(io, identity, recovered, small);
	check(recovered.groups[1] == std::vector{record(1, 3, 10, j::TurnRecordKind::Checkpoint), record(1, 4, 10, j::TurnRecordKind::HardState)},
		"a checkpoint supersedes what precedes it");
	check(recovered.groups[7].size() == 1 && log.sequence(7) == 40, "a group may start from a checkpoint");
	check(throws([] { j::TurnLogOptions bad{1000, 512, 1}; Directory d("bad"); j::PosixIO io(d.path); auto o = io.acquire_owner(true); j::TurnRecovery r; j::TurnLog l(io, identity, r, bad); }),
		"a segment is a whole number of blocks");
}

void stale_bytes() {
	Directory directory("stale"); j::PosixIO io(directory.path); auto owner = io.acquire_owner(true);
	j::TurnRecovery recovered; Model model; std::mt19937_64 random(3);
	{
		j::TurnLog log(io, identity, recovered, small);
		workload(log, model, random, 8, 200);
	}
	// A recycled file keeps its earlier bytes under a later name: its blocks name another segment.
	auto spare = std::filesystem::path(directory.path);
	std::uint64_t highest = 0;
	for (auto& entry : std::filesystem::directory_iterator(directory.path)) {
		if (auto n = j::turn_log_detail::segment_number(entry.path().filename().string())) { highest = std::max(highest, *n); }
	}
	std::filesystem::copy_file(directory.segment(1), directory.segment(highest + 1));
	j::TurnLog log(io, identity, recovered, small);
	check(!recovered.damaged && recovered.groups == model, "a recycled file's earlier bytes are not current");
}

// Groups that checkpoint let their segments go; an idle group's record moves forward
// instead of pinning the oldest segment; reopening after reclamation reads the same state.
void reclamation(std::uint64_t seed) {
	Directory directory("reclaim"); j::PosixIO io(directory.path); auto owner = io.acquire_owner(true);
	j::TurnRecovery recovered; Model model; std::mt19937_64 random(seed);
	j::TurnLogOptions options = small; options.retain_segments = 3; options.forward_bytes = 600;
	auto run = [&](j::TurnLog& log, unsigned turns) {
		for (unsigned t = 0; t < turns; ++t) {
			std::vector<j::TurnRecord> batch;
			auto group = random() % 4 + 1; auto sequence = log.sequence(group) + 1;
			auto kind = random() % 5 == 0 ? j::TurnRecordKind::Checkpoint : j::TurnRecordKind::Batch;
			batch.push_back(record(group, sequence, random() % 400 + 1, kind));
			log.append(batch);
			if (kind == j::TurnRecordKind::Checkpoint) { model[group].clear(); }
			model[group].push_back(batch.back());
		}
	};
	{
		j::TurnLog log(io, identity, recovered, options);
		log.append(std::vector{record(9, 1, 100)}); model[9].push_back(record(9, 1, 100));  // never writes again
		run(log, 150);
		check(log.data_segments().size() <= options.retain_segments + 1, "segments stay within the retention bound");
		check(*log.data_segments().begin() > 1 && log.stats().forwarded_records > 0, "the idle group's record moved forward and the first segment went");
		check(log.stats().segments_recycled > 0, "released segments are recycled into the spare pool");
	}
	for (unsigned round = 0; round < 3; ++round) {
		j::TurnLog log(io, identity, recovered, options);
		check(!recovered.damaged && recovered.lost.empty() && recovered.groups == model, "reopening after reclamation reads every group's live records");
		run(log, 60);
	}
	std::size_t files = 0;
	for (auto& entry : std::filesystem::directory_iterator(directory.path)) { files += j::turn_log_detail::segment_number(entry.path().filename().string()).has_value(); }
	check(files <= options.retain_segments + 1 + options.spare_segments + 1, "segment files stay bounded across reopenings");
}

// The two-step append a completion driver uses: one turn in flight; nothing visible until
// its barrier returns; a failed write fails the log.
void staged_turns() {
	Directory directory("staged"); j::PosixIO io(directory.path); auto owner = io.acquire_owner(true);
	j::TurnRecovery recovered;
	{
		j::TurnLog log(io, identity, recovered, small);
		auto write = log.stage(std::vector{record(1, 1, 100)});
		check(log.sequence(1) == 0 && log.last_turn() == 0 && write.turn == 1, "a staged turn changes nothing visible");
		check(throws([&] { log.stage(std::vector{record(2, 1, 10)}); }), "one turn is in flight at a time (condition 7)");
		check(write.file->write_at(write.offset, write.bytes) == write.bytes.size(), "the host writes the staged bytes");
		write.file->sync_data(); log.completed();
		check(log.sequence(1) == 1 && log.last_turn() == 1 && throws([&] { log.completed(); }), "completion makes the turn visible, once");
		log.stage(std::vector{record(1, 2, 10)}); log.failed();
		check(throws([&] { log.append(std::vector{record(1, 2, 10)}); }), "a failed write fails the log");
	}
	j::TurnLog log(io, identity, recovered, small);
	check(recovered.groups[1] == std::vector{record(1, 1, 100)}, "the completed turn recovers; the failed one does not");
}
}  // namespace

int main() {
	try { round_trip(); torn_tail(); torn_turn_in_earlier_epoch(); damage(); checkpoints_and_guards(); stale_bytes(); staged_turns(); for (std::uint64_t seed = 4; seed < 7; ++seed) { reclamation(seed); } }
	catch (const std::exception& error) { check(false, error.what()); }
	std::cout << checks << " turn log checks, " << failures << " failures\n";
	return failures ? 1 : 0;
}
