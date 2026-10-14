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

// Track C, T1: stores written at consensus b7073bb (Detent's pin) recover unchanged. The
// fixtures in test/fixtures/b7073bb come from that commit's encoders
// (fixtures/b7073bb/old_writer.cc.txt). With --write DIR, writes the formats Track C added,
// which tools/compat.sh feeds to b7073bb's reader, expecting each refused.
#include "consensus/checkpoint.h"
#include "consensus/membership.h"
#include "consensus/snapshot.h"
#include "consensus/storage.h"
#include "consensus/transport/codec.h"

#include <cstdio>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>

using namespace kronuz::consensus;

namespace {

int checks = 0, failures = 0;
void check(bool value, const char* what) {
	++checks;
	if (!value) { ++failures; std::fprintf(stderr, "FAIL: %s\n", what); }
}
std::string read(const std::string& path) {
	std::ifstream in(path, std::ios::binary);
	return {std::istreambuf_iterator<char>(in), {}};
}
void write(const std::string& path, const std::string& bytes) { std::ofstream(path, std::ios::binary) << bytes; }
FixedConfiguration fixture_config() {
	FixedConfiguration c; c.cluster[0] = 'T'; c.cluster[1] = '1'; c.configuration[0] = 'O'; c.local = 1; c.voters = {1, 2, 3};
	return c;
}

void old_stores(const std::string& dir) {
	const auto c = fixture_config();
	Recovery recovery(c);
	recovery.replay(1, read(dir + "/init"));
	recovery.replay(2, read(dir + "/batch-2"));
	recovery.replay(3, read(dir + "/batch-3"));
	recovery.replay(4, read(dir + "/batch-4"));
	auto state = recovery.finish(4);
	check(state.hard == HardState{2, 2, 2} && state.entries.size() == 3 && state.entries[1].payload == "alpha" && state.entries[2].payload == std::string("b\0c", 3) &&
		!state.membership, "a b7073bb store recovers its hard state and log, with the fixed voters");
	check(read(dir + "/init") == encode_initialization(c), "an all-voter founding still writes b7073bb's initialization record byte for byte");
	kronuz::journal::ArtifactDescriptor application{{'A', 'P'}, 5, kronuz::journal::crc32c("image")};
	for (const char* name : {"/checkpoint-v1", "/checkpoint-v2"}) {
		auto bundle = decode_checkpoint(read(dir + name), c, 4, application);
		check(bundle.state.base_index == 2 && bundle.state.entries.size() == 1 && !bundle.state.membership, "a b7073bb checkpoint bundle decodes");
	}
	check(read(dir + "/checkpoint-v2") == encode_checkpoint(decode_checkpoint(read(dir + "/checkpoint-v2"), c, 4, application), {}), "a fixed group's bundle re-encodes to b7073bb's bytes");
	SnapshotPolicy policy{c.cluster, c.configuration, 7, 1 << 20};
	auto snapshot = decode_snapshot(read(dir + "/snapshot-v1"), policy);
	check(snapshot.through == 2 && !snapshot.membership && encode_snapshot(snapshot, policy) == read(dir + "/snapshot-v1"), "a b7073bb snapshot descriptor decodes and re-encodes unchanged");
}

void write_new(const std::string& dir) {
	const auto c = fixture_config();
	Membership witness{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 4, 0};
	write(dir + "/init-v2", encode_initialization(c, witness));
	FixedConfiguration j = c; j.local = 7; j.voters.clear();
	write(dir + "/join", encode_join(j));
	Membership with4 = fixed_membership(c); with4.members.push_back({4, 4, MemberRole::Learner}); with4.next_incarnation = 5;
	write(dir + "/batch-configuration", encode_storage_batch(StorageBatch{std::nullopt, LogMutation{1, {{1, 1, EntryKind::Configuration, encode_membership(with4)}}}}));
	RecoveredState state; state.configuration = c; state.hard = HardState{2, 2, 3}; state.base_index = 2; state.base_term = 1; state.applied_index = 2;
	state.entries = {{3, 2, EntryKind::Command, "x"}}; state.membership = with4;
	kronuz::journal::ArtifactDescriptor application{{'A', 'P'}, 5, kronuz::journal::crc32c("image")};
	write(dir + "/checkpoint-v3", encode_checkpoint(state, 4, application));
	SnapshotPolicy policy{c.cluster, c.configuration, 7, 1 << 20};
	write(dir + "/frame-timeoutnow", transport::encode(TimeoutNow{7, 11, 21}));
	write(dir + "/snapshot-v2", encode_snapshot(SnapshotDescriptor{c.cluster, c.configuration, 7, 2, 1, 5, kronuz::journal::crc32c("image"), with4}, policy));
}

}  // namespace

int main(int argc, char** argv) {
	if (argc == 3 && std::string_view(argv[1]) == "--write") { write_new(argv[2]); return 0; }
	const std::string dir = argc > 1 ? argv[1] : "test/fixtures/b7073bb";
	try { old_stores(dir); } catch (const std::exception& error) { check(false, error.what()); }
	std::printf("%d compatibility checks, %d failures\n", checks, failures);
	return failures ? 1 : 0;
}
