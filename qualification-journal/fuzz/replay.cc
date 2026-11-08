// Runs the fuzz target over seeded inputs without libFuzzer, as a test: valid turn-log
// segments and checkpoint records from real workloads, and their single-byte corruptions.
#include "journal/memory_io.h"
#include "journal/store.h"

#include <cstdio>
#include <random>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size);

namespace j = kronuz::journal;
namespace jt = kronuz::journal::testing;

static void run(char mode, const std::string& bytes) {
	std::string input(1, mode); input += bytes;
	LLVMFuzzerTestOneInput(reinterpret_cast<const std::uint8_t*>(input.data()), input.size());
}

int main() {
	std::mt19937_64 random(5);
	std::size_t inputs = 0;
	for (int seed = 0; seed < 6; ++seed) {
		// A real segment: a few turns of several groups.
		jt::Disk disk; jt::MemoryIO io(disk, "log/");
		{
			j::TurnRecovery recovered; j::TurnLog log(io, {'F', 'U', 'Z', 'Z'}, recovered, {8 * 512, 512, 1, 4, 1024});
			std::map<std::uint64_t, std::uint64_t> next;
			for (int t = 0; t < 6; ++t) {
				std::vector<j::TurnRecord> turn;
				for (std::uint64_t g = 1; g <= 3; ++g) { turn.push_back({g, ++next[g], j::TurnRecordKind::Batch, std::string(random() % 300, char('a' + g))}); }
				log.append(turn);
			}
		}
		auto file = io.open_existing(j::turn_log_detail::segment_name(1));
		std::string bytes(static_cast<std::size_t>(file->size()), '\0'); file->read_at(0, std::span<char>(bytes.data(), bytes.size()));
		run(0, bytes); ++inputs;
		for (int k = 0; k < 200; ++k) { auto bad = bytes; bad[random() % bad.size()] ^= char(1 + random() % 255); run(0, bad); ++inputs; }
	}
	// Checkpoint records: a group's first record (a v1 manifest) and corruptions of it.
	j::Frontier genesis; genesis.identity = {'G'};
	std::string manifest;  // built through a shared Store's create
	{
		jt::Disk disk; jt::MemoryIO log_io(disk, "log/"); j::TurnDriver driver(log_io, {'F', 'U', 'Z', 'Z'}, {8 * 512, 512, 1, 4, 1024});
		auto io = std::make_shared<jt::MemoryIO>(disk, "g1/");
		j::Store store(io, driver, 1, {{64u << 20, 4096}, {1u << 20, 8}, {8u << 20, 8}, 64, 3});
		store.create(genesis.identity); driver.commit();
		j::TurnDriver reopened(log_io, {'F', 'U', 'Z', 'Z'}, {8 * 512, 512, 1, 4, 1024});
		manifest = reopened.records(1).front().payload;
	}
	run(1, manifest); ++inputs;
	for (std::size_t n = 0; n < 8; ++n) { run(1, manifest.substr(0, n)); ++inputs; }  // shorter than a checksum
	for (int k = 0; k < 2000; ++k) { auto bad = manifest; bad[random() % bad.size()] ^= char(1 + random() % 255); run(1, bad); ++inputs; }
	for (int k = 0; k < 200; ++k) { std::string junk(random() % 400, '\0'); for (auto& c : junk) { c = char(random()); } run(1, junk); run(0, junk); inputs += 2; }
	std::printf("turn log fuzz replay: %zu inputs\n", inputs);
	return 0;
}
