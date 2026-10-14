// Writes fuzz_decoders' seed corpus: one valid input per decoder, prefixed by its selector.
#include "consensus/checkpoint.h"
#include "consensus/membership.h"
#include "consensus/storage.h"
#include "consensus/transport/codec.h"

#include <cstdio>
#include <fstream>
#include <string>

using namespace kronuz::consensus;

int main(int argc, char** argv) {
	if (argc != 2) { std::fprintf(stderr, "usage: %s CORPUS_DIR\n", argv[0]); return 2; }
	FixedConfiguration c; c.cluster[0] = 'F'; c.configuration[0] = 'Z'; c.local = 1; c.voters = {1, 2, 3};
	FixedConfiguration j = c; j.local = 7; j.voters.clear();
	Membership witness{{{1, 1, MemberRole::Voter}, {2, 2, MemberRole::Voter}, {3, 3, MemberRole::Witness}}, 4, 0};
	Membership limited = witness; limited.limits = Limits{};
	int n = 0;
	auto write = [&](char selector, const std::string& bytes) {
		std::ofstream(std::string(argv[1]) + "/seed-" + std::to_string(n++), std::ios::binary) << selector << bytes;
	};
	write(0, transport::encode(TimeoutNow{7, 11, 21}));
	write(0, transport::encode(AppendRequest{7, 11, 9, 6, 8, 13, {{10, 7, EntryKind::Configuration, encode_membership(limited)}}, 9}));
	write(0, transport::encode(VoteRequest{8, 9, 6, true}));
	write(1, encode_membership(witness));
	write(1, encode_membership(limited));
	write(2, encode_initialization(c));
	write(2, encode_initialization(c, witness));
	write(3, encode_join(j));
	write(4, encode_storage_batch(StorageBatch{HardState{2, 1, 1}, LogMutation{1, {{1, 1, EntryKind::NoOp, {}}, {2, 2, EntryKind::Configuration, encode_membership(limited)}}}}));
	RecoveredState state; state.configuration = c; state.hard = HardState{1, std::nullopt, 2};
	state.base_index = 1; state.base_term = 1; state.applied_index = 1; state.membership = limited;
	state.entries = {{2, 1, EntryKind::Command, "x"}};
	write(5, encode_checkpoint(state, 1, kronuz::journal::ArtifactDescriptor{}));
	std::printf("%d seeds\n", n);
	return 0;
}
