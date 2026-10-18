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

#include "consensus/checkpoint.h"
#include "consensus/core.h"
#include "consensus/admission.h"
#include <cstdlib>
#include <iostream>
#include <new>

namespace {
bool allocation_guard = false;
std::size_t allocations = 0, allocated_bytes = 0;
}
void* operator new(std::size_t size) {
	if (allocation_guard) {
		if (allocations >= 16 || size > 16384 - allocated_bytes) { throw std::bad_alloc(); }
		++allocations; allocated_bytes += size;
	}
	if (auto* result = std::malloc(size ? size : 1)) { return result; }
	throw std::bad_alloc();
}
void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete(void* pointer, std::size_t) noexcept { std::free(pointer); }

using namespace kronuz::consensus;
namespace {
unsigned checks = 0;
void require(bool value, const char* message) {
	++checks; if (!value) { throw std::runtime_error(message); }
}
template<class F> bool corrupt(F&& function) {
	try { function(); } catch (const kronuz::journal::Corruption&) { return true; }
	return false;
}
FixedConfiguration configuration() {
	FixedConfiguration result; result.local = 1; result.voters = {1}; return result;
}
RecoveredState fixture(std::size_t count, std::size_t bytes, bool checkpoint = true) {
	RecoveredState state; state.configuration = configuration();
	state.base_index = state.applied_index = checkpoint ? 1 : 0;
	state.base_term = checkpoint ? 1 : 0;
	state.hard = {1, {}, state.base_index + count};
	for (std::size_t i = 0; i < count; ++i) {
		state.entries.push_back({state.base_index + i + 1, 1, EntryKind::Command, std::string(bytes, 'x')});
	}
	return state;
}
void lowered_limits() {
	RetainedStatePolicy policy{2048, 16384, 16, 8192, 8};
	Limits wide; wide.command_bytes = 2048; wide.rpc_bytes = 8192; wide.log_bytes = 16384;
	wide.log_entries = 16; wide.control_entries = 1; wide.rpc_entries = 8;
	Limits narrow = wide; narrow.command_bytes = 64; narrow.rpc_bytes = narrow.log_bytes = 4096;
	narrow.log_entries = narrow.rpc_entries = 4;
	validate_limits(wide); validate_limits(narrow);
	auto state = fixture(8, 1024);
	kronuz::journal::ArtifactDescriptor application{{}, 1, 1};
	auto encoded = encode_checkpoint(state, 7, application, wide, {}, policy);
	auto decoded = decode_checkpoint(encoded, state.configuration, 7, application, narrow, policy);
	Core core(state.configuration, decoded.state, narrow, {}, {}, policy);
	require(core.retained_log_bytes() == 8192, "retained resources remain independent of operating limits");
	require(core.retained_suffix({6, 1}) == RetainedUsage{3, 3072}, "suffix query uses the exact retained range");
	require(core.retained_suffix({6, 2}) == RetainedUsage{}, "a mismatched snapshot boundary retains no suffix");
	require(!core.retained_suffix({0, 0}), "invalid boundaries are not funded captures");
	auto first = core.step(Start{});
	require(!first.empty(), "committed historical state produces a delivery");
	const auto* delivered = std::get_if<Committed>(&first.back());
	require(delivered && delivered->entries.size() == 4, "historical delivery respects the current RPC envelope");
	require(encode_checkpoint(decoded.state, 7, application, narrow, {}, policy) == encoded,
		"re-encoding lower operating limits preserves bytes");
	auto baseline = wide; baseline.rpc_bytes = 4096;
	std::array<Limits, 4> independent{baseline, baseline, baseline, baseline};
	independent[0].command_bytes = 64;
	independent[1].rpc_bytes = 2048;
	independent[2].log_bytes = 4096;
	independent[3].log_entries = 4;
	for (const auto& limits : independent) {
		validate_limits(limits);
		auto restored = decode_checkpoint(encoded, state.configuration, 7, application, limits, policy);
		Core reopened(state.configuration, restored.state, limits, {}, {}, policy);
		require(reopened.retained_log_bytes() == 8192, "each independent operating reduction preserves historical payloads");
		require(encode_checkpoint(restored.state, 7, application, limits, {}, policy) == encoded,
			"each independent operating reduction preserves checkpoint bytes");
	}
	auto small = policy; small.log_entries = 7;
	require(corrupt([&] { decode_checkpoint(encoded, state.configuration, 7, application, narrow, small); }),
		"suffix counts above policy are rejected");
	small = policy; small.log_bytes = 8191;
	require(corrupt([&] { decode_checkpoint(encoded, state.configuration, 7, application, narrow, small); }),
		"aggregate payload above policy is rejected");
}
void invalid_typed_state() {
	RetainedStatePolicy policy{8, 8192, 4096, 4096, 64};
	Limits limits; limits.command_bytes = 8; limits.rpc_bytes = 4096; limits.log_bytes = 8192;
	limits.log_entries = 4096; limits.rpc_entries = 64; limits.members = limits.voters = 1;
	for (bool count : {false, true}) {
		auto state = fixture(count ? 4097 : 4096, 1, false);
		if (!count) { state.entries.back().payload = std::string(9, 'x'); }
		auto expected = state.configuration;
		bool rejected = false, allocated = false;
		allocations = allocated_bytes = 0; allocation_guard = true;
		try { Core core(std::move(expected), std::move(state), limits, {}, {}, policy); }
		catch (const std::invalid_argument&) { rejected = true; }
		catch (const std::bad_alloc&) { allocated = true; }
		allocation_guard = false;
		require(rejected && !allocated, "invalid typed history is rejected before per-entry allocation");
	}
}
void historical_membership() {
	auto state = fixture(1, 1);
	state.membership = fixed_membership(state.configuration);
	state.membership->members.push_back({2, 2, MemberRole::Learner});
	state.membership->next_incarnation = 3;
	Limits limits; limits.members = limits.voters = 1;
	Core core(state.configuration, state, limits);
	require(core.retained_log_bytes() == 1, "historical membership without group limits uses ceilings");
	state.membership->limits = limits;
	bool rejected = false;
	try { Core invalid(state.configuration, state, limits); }
	catch (const std::invalid_argument&) { rejected = true; }
	require(rejected, "historical membership still obeys its recorded group limits");
}
void replay_bounds() {
	Limits limits; limits.command_bytes = limits.rpc_bytes = 4096; limits.log_bytes = 8192;
	limits.log_entries = 4; limits.control_entries = 1; limits.rpc_entries = 1;
	RetainedStatePolicy policy{4096, 8192, 4, 4096, 1};
	Recovery recovery(configuration(), limits, policy);
	recovery.replay(1, encode_initialization(configuration()));
	for (Index index = 1; index <= 3; ++index) {
		StorageBatch batch{HardState{1, {}, index}, LogMutation{index, {{index, 1, EntryKind::Command, std::string(4096, 'x')}}}};
		auto encoded = encode_storage_batch(batch);
		if (index < 3) { recovery.replay(index + 1, encoded); }
		else { require(corrupt([&] { recovery.replay(index + 1, encoded); }), "replay enforces aggregate retained capacity"); }
	}
}
void follower_resource_debt() {
	Limits limits; limits.command_bytes = 64; limits.rpc_bytes = 2048; limits.log_bytes = 8192;
	limits.log_entries = 32; limits.control_entries = 1; limits.rpc_entries = 4;
	RetainedStatePolicy policy{1024, 8192, 32, 2048, 8};
	auto state = fixture(32, 256, false); state.configuration.voters = {1, 2, 3}; state.hard.commit_index = 0;
	Core core(state.configuration, state, limits, {}, {}, policy); core.step(Start{});
	auto bounded = [&] {
		require(core.last_index() - core.base_index() + core.retired_log_entries() <= policy.log_entries + policy.batch_entries,
			"merged replacements retain only one batch of entry debt");
		require(core.retained_log_bytes() + core.retired_log_bytes() <= policy.log_bytes + policy.batch_bytes,
			"merged replacements retain only one batch of payload debt");
	};
	std::optional<Receive> retry;
	for (Term term = 2; term < 40; ++term) {
		Receive receive{2, AppendRequest{term, term, 0, 0, 0, 0, {}}};
		auto& first = std::get<AppendRequest>(receive.message);
		for (Index index = 1; index <= 4; ++index) { first.entries.push_back({index, term, EntryKind::Command, std::string(64, 'a')}); }
		receive.more.push_back(AppendRequest{term, term + 100, 4, term, 0, 0, {}});
		for (Index index = 5; index <= 8; ++index) { receive.more.back().entries.push_back({index, term, EntryKind::Command, std::string(64, 'b')}); }
		auto plan = plan_event(receive, false, limits, policy);
		require(plan.count == 1, "merged replacement shares one admission pack");
		retry = receive;
		auto output = core.step(std::move(receive)); bounded();
		for (const auto& action : output) { if (auto persist = std::get_if<Persist>(&action)) { core.step(Persisted{persist->token}); bounded(); } }
	}
	for (unsigned turn = 0; turn < 100 && core.retired_log_entries(); ++turn) { core.retire_log(); bounded(); }
	auto resumed = core.step(std::move(*retry)); bounded();
	for (const auto& action : resumed) { if (auto persist = std::get_if<Persist>(&action)) { core.step(Persisted{persist->token}); bounded(); } }
	for (unsigned turn = 0; turn < 100 && core.retired_log_entries(); ++turn) { core.retire_log(); bounded(); }
	require(!core.retired_log_entries() && core.last_index() == 8 && !core.busy(),
		"merged replacement resumes after finite historical retirement");
	Receive empty{2, AppendRequest{100, 1, 0, 0, 0, 0, {}}};
	empty.more.resize(policy.batch_entries - 1, std::get<AppendRequest>(empty.message));
	require(bool(retained_detail::receive_usage(empty, policy)), "empty merged requests fit the container allowance");
	empty.more.push_back(std::get<AppendRequest>(empty.message));
	require(!retained_detail::receive_usage(empty, policy), "empty merged requests cannot bypass container bounds");
	bool rejected = false;
	try { plan_event(empty, false, limits, policy); } catch (const std::length_error&) { rejected = true; }
	require(rejected, "admission rejects over-bound empty merges before protocol mutation");
	auto term = core.term(); core.step(std::move(empty));
	require(core.term() == term, "Core rejects over-bound empty merges before term mutation");
}
}
int main() {
	try { lowered_limits(); invalid_typed_state(); historical_membership(); replay_bounds(); follower_resource_debt(); }
	catch (const std::exception& error) { allocation_guard = false; std::cerr << error.what() << '\n'; return 1; }
	std::cout << checks << " retained-state checks passed\n";
}
