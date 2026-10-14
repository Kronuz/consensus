// Built against consensus b7073bb: every format Track C added must be refused, never misread.
#include "consensus/checkpoint.h"
#include "consensus/snapshot.h"
#include "consensus/storage.h"
#include "consensus/transport/codec.h"
#include <cstdio>
#include <fstream>
#include <iterator>
#include <string>
using namespace kronuz::consensus;
static std::string read(const std::string& path) { std::ifstream in(path, std::ios::binary); return {std::istreambuf_iterator<char>(in), {}}; }
int main(int argc, char** argv) {
	std::string dir = argv[1];
	FixedConfiguration c; c.cluster[0] = 'T'; c.cluster[1] = '1'; c.configuration[0] = 'O'; c.local = 1; c.voters = {1, 2, 3};
	int failures = 0;
	auto refuses = [&](const char* what, auto&& f) {
		bool threw = false;
		try { f(); } catch (const std::exception&) { threw = true; }
		std::printf("  %s: %s\n", what, threw ? "refused" : "ACCEPTED");
		failures += !threw;
	};
	refuses("RFTINIT2 (founding roles)", [&] { Recovery r(c); r.replay(1, read(dir + "/init-v2")); });
	refuses("RFTJOIN1 (join record)", [&] { FixedConfiguration j = c; j.local = 7; Recovery r(c); r.replay(1, read(dir + "/join")); });
	refuses("RFTBTCH2 (batch with a Configuration entry)", [&] { Recovery r(c); r.replay(1, encode_initialization(c)); r.replay(2, read(dir + "/batch-configuration")); });
	refuses("checkpoint bundle v3 (base membership)", [&] {
		kronuz::journal::ArtifactDescriptor application{{'A', 'P'}, 5, kronuz::journal::crc32c("image")};
		(void)decode_checkpoint(read(dir + "/checkpoint-v3"), c, 4, application);
	});
	refuses("RFTSNP02 (snapshot with membership)", [&] { (void)decode_snapshot(read(dir + "/snapshot-v2"), SnapshotPolicy{c.cluster, c.configuration, 7, 1 << 20}); });
	refuses("DTC2 frame (TimeoutNow)", [&] { (void)transport::decode(read(dir + "/frame-timeoutnow")); });
	std::printf("b7073bb reader: %d new formats accepted\n", failures);
	return failures ? 1 : 0;
}
