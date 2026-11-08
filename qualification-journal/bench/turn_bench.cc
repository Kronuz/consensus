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

// D14's journal half (strata Track C): one journal of turns per core (RFTW0001) against one
// preallocated file per group whose turn is made durable by IO::sync_files, the measured
// alternative. Each core is a thread with its own directory; each turn carries one record
// from each group active in it. Reports turns per second per core, the turn's p50 and
// p99 latency (write and barrier), and bytes written per byte appended.
//   journal_turn_bench DIR [cores] [groups] [record_bytes] [active_percent] [seconds]
#include "journal/turn_log.h"
#include "completion/posix.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

namespace j = kronuz::journal;
using clock_type = std::chrono::steady_clock;

namespace {
struct Result { std::uint64_t turns = 0, records = 0, appended = 0, written = 0; std::vector<double> latency; };

std::filesystem::path fresh(const std::filesystem::path& path) {
	std::filesystem::remove_all(path); std::filesystem::create_directories(path);
	std::filesystem::permissions(path, std::filesystem::perms::owner_all);
	return path;
}
std::vector<std::uint64_t> active(std::mt19937_64& random, std::uint64_t groups, unsigned percent) {
	std::vector<std::uint64_t> result;
	for (std::uint64_t g = 1; g <= groups; ++g) { if (random() % 100 < percent) { result.push_back(g); } }
	if (result.empty()) { result.push_back(random() % groups + 1); }
	return result;
}

Result shared(const std::filesystem::path& dir, std::uint64_t groups, std::size_t bytes, unsigned percent, double seconds, std::uint64_t seed) {
	j::PosixIO io(fresh(dir)); auto owner = io.acquire_owner(true);
	j::TurnRecovery recovered; j::TurnLog log(io, {'B', 'E', 'N', 'C', 'H'}, recovered);
	log.prepare();
	Result r; std::mt19937_64 random(seed); const std::string payload(bytes, 'x');
	const auto start = clock_type::now(); const auto written = log.stats().bytes_written;
	std::vector<j::TurnRecord> batch;
	while (std::chrono::duration<double>(clock_type::now() - start).count() < seconds) {
		batch.clear();
		// Each group checkpoints every 64 of its records, so segments go and recycle.
		for (auto g : active(random, groups, percent)) {
			auto sequence = log.sequence(g) + 1;
			batch.push_back({g, sequence, sequence % 64 ? j::TurnRecordKind::Batch : j::TurnRecordKind::Checkpoint, payload});
		}
		const auto t0 = clock_type::now();
		log.append(batch);
		r.latency.push_back(std::chrono::duration<double, std::micro>(clock_type::now() - t0).count());
		++r.turns; r.records += batch.size(); r.appended += batch.size() * bytes;
		log.prepare();  // off the measured turn
	}
	r.written = log.stats().bytes_written - written;
	return r;
}

// One file per group, preallocated; a turn writes each active group's record into its file
// and makes them durable together with IO::sync_files.
Result per_group(const std::filesystem::path& dir, std::uint64_t groups, std::size_t bytes, unsigned percent, double seconds, std::uint64_t seed) {
	j::PosixIO io(fresh(dir)); auto owner = io.acquire_owner(true);
	const std::uint64_t file_bytes = std::uint64_t{4} << 20;
	std::vector<std::unique_ptr<j::File>> files; std::vector<std::uint64_t> offsets(groups + 1, 0);
	const std::string zeros(file_bytes, '\0');
	files.push_back(nullptr);
	for (std::uint64_t g = 1; g <= groups; ++g) {
		auto file = io.create_exclusive("group-" + std::to_string(g)); file->write_at(0, zeros); file->sync(); files.push_back(std::move(file));
	}
	io.sync_directory();
	Result r; std::mt19937_64 random(seed);
	std::string record(bytes + 24, 'x');  // the payload and a frame header
	const auto start = clock_type::now(); std::vector<j::File*> dirty;
	while (std::chrono::duration<double>(clock_type::now() - start).count() < seconds) {
		auto groups_now = active(random, groups, percent);
		const auto t0 = clock_type::now();
		dirty.clear();
		for (auto g : groups_now) {
			if (offsets[g] + record.size() > file_bytes) { offsets[g] = 0; }  // reuse, as recycling would
			files[g]->write_at(offsets[g], record); offsets[g] += record.size(); dirty.push_back(files[g].get());
			r.written += record.size();
		}
		io.sync_files(dirty);
		r.latency.push_back(std::chrono::duration<double, std::micro>(clock_type::now() - t0).count());
		++r.turns; r.records += groups_now.size(); r.appended += groups_now.size() * bytes;
	}
	return r;
}
double percentile(std::vector<double> values, double p) {
	if (values.empty()) { return 0; }
	std::sort(values.begin(), values.end());
	return values[std::min(values.size() - 1, static_cast<std::size_t>(p * static_cast<double>(values.size())))];
}
}  // namespace

int main(int argc, char** argv) {
	if (argc < 2) { std::cerr << "usage: journal_turn_bench DIR [cores] [groups] [record_bytes] [active_percent] [seconds]\n"; return 2; }
	const std::filesystem::path root(argv[1]);
	const unsigned cores = argc > 2 ? static_cast<unsigned>(std::stoul(argv[2])) : 1;
	const std::uint64_t groups = argc > 3 ? std::stoull(argv[3]) : 16;
	const std::size_t bytes = argc > 4 ? std::stoull(argv[4]) : 256;
	const unsigned percent = argc > 5 ? static_cast<unsigned>(std::stoul(argv[5])) : 50;
	const double seconds = argc > 6 ? std::stod(argv[6]) : 10;
	for (bool turn_log : {true, false}) {
		std::vector<Result> results(cores); std::vector<std::thread> threads;
		for (unsigned c = 0; c < cores; ++c) {
			threads.emplace_back([&, c] {
				auto dir = root / ((turn_log ? "shared-" : "per-group-") + std::to_string(c));
				results[c] = turn_log ? shared(dir, groups, bytes, percent, seconds, c + 1) : per_group(dir, groups, bytes, percent, seconds, c + 1);
			});
		}
		for (auto& t : threads) { t.join(); }
		Result all; for (auto& r : results) { all.turns += r.turns; all.records += r.records; all.appended += r.appended; all.written += r.written; all.latency.insert(all.latency.end(), r.latency.begin(), r.latency.end()); }
		std::cout << (turn_log ? "shared   " : "per-group") << " cores " << cores << " groups " << groups << " bytes " << bytes << " active " << percent << "%: "
			<< static_cast<double>(all.turns) / seconds / cores << " turns/s/core, " << static_cast<double>(all.records) / seconds << " records/s, p50 "
			<< percentile(all.latency, 0.5) << " us, p99 " << percentile(all.latency, 0.99) << " us, written/appended "
			<< (all.appended ? static_cast<double>(all.written) / static_cast<double>(all.appended) : 0) << '\n';
	}
	std::filesystem::remove_all(root);
	return 0;
}
