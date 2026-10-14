#include "consensus/transport/codec.h"
#include "consensus/transport/hello.h"
#include "consensus/transport/profile.h"
#include "consensus/transport/tls.h"
#include <cassert>
#include <iostream>
using namespace kronuz::consensus;
int main() {
	FixedConfiguration configuration;
	configuration.cluster[0] = 'c';
	configuration.configuration[0] = 'f';
	configuration.local = 1;
	configuration.voters = {1, 2, 3};
	auto profile = kronuz::consensus::transport::profile_fingerprint({}, configuration, {});
	configuration.local = 2;
	configuration.voters = {3, 1, 2};
	assert(kronuz::consensus::transport::profile_fingerprint({}, configuration, {}) == profile);
	configuration.voters = {1, 2, 4};
	assert(kronuz::consensus::transport::profile_fingerprint({}, configuration, {}) != profile);
	std::vector<Message> messages{
		VoteRequest{7, 9, 6}, VoteResponse{7, true},
		AppendRequest{7,
					  11,
					  9,
					  6,
					  8,
					  13,
					  {{10, 7, EntryKind::Command, std::string("a\0b", 3)}, {11, 7, EntryKind::NoOp, {}}}},
		AppendResponse{7, 11, true, 11, 12, 13, LogBoundary{8, 6}},
		SnapshotResponse{7, {7, 19, {11, 7}}, SnapshotReply::Installed},
		VoteRequest{8, 9, 6, true}, PreVoteRequest{8, 9, 6}, PreVoteResponse{7, 8, true}, TimeoutNow{7, 11, 21}};
	kronuz::consensus::transport::Hello local;
	local.cluster[0] = 'c';
	local.group[0] = 'g';
	local.epoch[0] = 'e';
	local.node = 1;
	local.connection = kronuz::consensus::transport::fresh_connection_identity();
	auto remote = local;
	remote.node = 2;
	remote.connection = kronuz::consensus::transport::fresh_connection_identity();
	assert(kronuz::consensus::transport::encode_hello(remote).size() == kronuz::consensus::transport::hello_size);
	kronuz::consensus::transport::validate_hello(
		kronuz::consensus::transport::decode_hello(kronuz::consensus::transport::encode_hello(remote)), local, 2,
		kronuz::consensus::transport::Lane::Control);
	for (unsigned change = 0; change != 6; ++change) {
		auto bad = remote;
		switch (change) {
		case 0:
			bad.node = 3;
			break;
		case 1:
			bad.profile[0] = 1;
			break;
		case 2:
			bad.epoch[0] = 'x';
			break;
		case 3:
			bad.connection = local.connection;
			break;
		case 4:
			bad.lane = kronuz::consensus::transport::Lane::Bulk;
			break;
		case 5:
			bad.group[0] = 'x';
			break;
		}
		try {
			kronuz::consensus::transport::validate_hello(bad, local, 2, kronuz::consensus::transport::Lane::Control);
			assert(false);
		} catch (const std::invalid_argument &) {
		}
	}
	unsigned rejected = 0;
	for (const auto &message : messages) {
		auto bytes = kronuz::consensus::transport::encode(message);
		assert(kronuz::consensus::transport::encode(kronuz::consensus::transport::decode(bytes)) == bytes);
		for (std::size_t size = 0; size < bytes.size(); ++size) {
			try {
				(void)kronuz::consensus::transport::decode(std::string_view(bytes).substr(0, size));
				assert(false);
			} catch (const kronuz::journal::Corruption &) {
				++rejected;
			}
		}
		try {
			(void)kronuz::consensus::transport::decode(bytes + "x");
			assert(false);
		} catch (const kronuz::journal::Corruption &) {
			++rejected;
		}
	}
	auto bad = kronuz::consensus::transport::encode(messages[2]);
	// Entry count follows magic and eight u64 fields. Reject before reserving entries.
	for (unsigned i = 0; i < 8; ++i)
		bad[60 + i] = static_cast<char>(0xff);
	try {
		(void)kronuz::consensus::transport::decode(bad);
		assert(false);
	} catch (const kronuz::journal::Corruption &) {
		++rejected;
	}
	Limits small;
	small.command_bytes = small.rpc_bytes = 3;
	auto full = messages[2];
	std::get<AppendRequest>(full).entries[0].payload = "abc";
	assert(kronuz::consensus::transport::encode(kronuz::consensus::transport::decode(kronuz::consensus::transport::encode(full, small), small),
									 small) == kronuz::consensus::transport::encode(full, small));
	std::get<AppendRequest>(full).entries[1].payload = "x";
	std::get<AppendRequest>(full).entries[1].kind = EntryKind::Command;
	try {
		(void)kronuz::consensus::transport::encode(full, small);
		assert(false);
	} catch (const std::invalid_argument &) {
		++rejected;
	}
	Limits maximum;
	auto largest = std::get<AppendRequest>(messages[2]);
	for (auto &entry : largest.entries) {
		entry.kind = EntryKind::Command;
		entry.payload.assign(maximum.command_bytes, 'x');
	}
	auto largest_bytes = kronuz::consensus::transport::encode(largest, maximum);
	assert(largest_bytes.size() == 68 + 32 * largest.entries.size() + maximum.rpc_bytes);
	assert(kronuz::consensus::transport::encode(kronuz::consensus::transport::decode(largest_bytes, maximum), maximum) ==
		   largest_bytes);
	for (auto bad : {kronuz::consensus::transport::encode(messages[1]), kronuz::consensus::transport::encode(messages[2]),
					 kronuz::consensus::transport::encode(messages[0])}) {
		if (bad.size() == 28)
			bad[20] = 2;
		else if (bad.size() > 100)
			bad[84] = 2;
		else
			bad[4] = 6;
		try {
			(void)kronuz::consensus::transport::decode(bad);
			assert(false);
		} catch (const kronuz::journal::Corruption &) {
			++rejected;
		}
	}
	std::cout << "bounded control codec: " << rejected << " malformed frames rejected\n";
}
