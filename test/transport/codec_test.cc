#include "consensus/transport/codec.h"
#include "consensus/admission.h"
#include "consensus/checkpoint.h"
#include "consensus/core.h"
#include "consensus/transport/hello.h"
#include "consensus/transport/profile.h"
#include "consensus/transport/tls.h"
#include <cassert>
#include <iostream>
using namespace kronuz::consensus;
namespace {
void configuration_entry_roundtrip() {
	auto require = [](bool value, const char* message) { if (!value) { throw std::runtime_error(message); } };
	FixedConfiguration fixed; fixed.local = 1; fixed.voters = {1, 2, 3};
	Limits limits; limits.command_bytes = 64; limits.rpc_bytes = 4096; limits.log_bytes = 8192;
	limits.log_entries = 32; limits.control_entries = 1; limits.rpc_entries = 4;
	auto membership = fixed_membership(fixed); membership.members.push_back({4, 4, MemberRole::Learner});
	membership.next_incarnation = 5; membership.limits = limits;
	auto payload = encode_membership(membership);
	require(payload.size() > limits.command_bytes && payload.size() < limits.rpc_bytes, "configuration exceeds command bound within RPC capacity");
	AppendRequest append{1, 1, 0, 0, 2, 0, {{1, 1, EntryKind::NoOp, {}}, {2, 1, EntryKind::Configuration, payload}}};
	auto wire = kronuz::consensus::transport::encode(append, limits);
	auto decoded = kronuz::consensus::transport::decode(wire, limits);
	require(std::get<AppendRequest>(decoded).entries == append.entries, "configuration transport preserves exact entries");
	Receive receive{2, decoded};
	require(plan_event(receive, false, limits).count == 1, "configuration transport enters one storage admission pack");
	RecoveredState initial; initial.configuration = fixed;
	Core core(fixed, initial, limits); core.step(Start{});
	auto actions = core.step(std::move(receive)); std::optional<StorageBatch> durable;
	for (const auto& action : actions) {
		if (auto persist = std::get_if<Persist>(&action)) {
			durable = persist->batch;
			core.step(Persisted{persist->token});
		}
	}
	require(durable && core.committed() == 2 && core.membership().members.size() == 4, "configuration reaches durable Core membership");
	Recovery recovery(fixed, limits); recovery.replay(1, encode_initialization(fixed));
	recovery.replay(2, encode_storage_batch(*durable));
	auto state = recovery.finish(2);
	require(state.entries == append.entries, "configuration storage replay preserves exact payloads");
	state.base_index = state.applied_index = 1; state.base_term = 1; state.entries.erase(state.entries.begin());
	kronuz::journal::ArtifactDescriptor application{{}, 1, 1};
	auto checkpoint = encode_checkpoint(state, 7, application, limits);
	auto reopened = decode_checkpoint(checkpoint, fixed, 7, application, limits);
	require(reopened.state.entries == state.entries, "configuration checkpoint preserves payload beyond command bound");
	append.term = 2; append.entries.back().payload = "bad";
	bool malformed = false;
	try { (void)kronuz::consensus::transport::decode(kronuz::consensus::transport::encode(append, limits), limits); }
	catch (const kronuz::journal::Corruption&) { malformed = true; }
	require(malformed, "transport rejects malformed membership payload");
	auto term = core.term(); core.step(Receive{2, append});
	require(core.term() == term && !core.busy(), "malformed transported membership rejects before term mutation");
	state.entries.back().payload = "bad";
	bool rejected = false;
	try { (void)encode_checkpoint(state, 7, application, limits); }
	catch (const std::exception&) { rejected = true; }
	require(rejected, "checkpoint rejects malformed membership payload");
}
}
int main() {
	configuration_entry_roundtrip();
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
	assert(largest_bytes.size() == 76 + 32 * largest.entries.size() + maximum.rpc_bytes);
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
