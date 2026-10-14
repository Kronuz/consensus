// Replays a fuzz target's checked-in corpus as a test: every file in the directory goes
// through LLVMFuzzerTestOneInput once, without libFuzzer.
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size);

int main(int argc, char** argv) {
	if (argc != 2) {
		std::fprintf(stderr, "usage: %s CORPUS_DIR\n", argv[0]);
		return 2;
	}
	std::vector<std::filesystem::path> files;
	for (const auto& e : std::filesystem::directory_iterator(argv[1])) {
		if (e.is_regular_file()) { files.push_back(e.path()); }
	}
	std::sort(files.begin(), files.end());
	for (const auto& f : files) {
		std::ifstream in(f, std::ios::binary);
		const std::string bytes((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
		LLVMFuzzerTestOneInput(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size());
	}
	std::printf("%s: %zu inputs replayed\n", argv[1], files.size());
	return files.empty() ? 1 : 0;
}
