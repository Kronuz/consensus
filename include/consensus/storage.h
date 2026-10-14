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

#pragma once

#include "types.h"
#include "membership.h"
#include "journal/codec.h"
#include <algorithm>
#include <limits>
#include <set>
#include <stdexcept>

namespace kronuz::consensus {

namespace storage_detail {
using kronuz::journal::Corruption;
using kronuz::journal::get32;
using kronuz::journal::get64;
using kronuz::journal::put32;
using kronuz::journal::put64;
constexpr std::uint64_t initial_magic = 0x3154494e49544652ull;
// Track C, D1, D9: a founding membership with roles ("RFTINIT2"), and a joiner's record
// ("RFTJOIN1": identities and its incarnation, no membership).
constexpr std::uint64_t initial_magic_v2 = 0x3254494e49544652ull;
constexpr std::uint64_t join_magic = 0x314e494f4a544652ull;
constexpr std::uint64_t batch_magic = 0x3148435442544652ull;
// A batch holding a Configuration entry (Track C, D1), so a reader before it refuses it.
constexpr std::uint64_t membership_batch_magic = 0x3248435442544652ull;
inline void identity(std::string& bytes, Identity value) { bytes.append(value.data(), value.size()); }
inline Identity identity(std::string_view& bytes) {
	if (bytes.size() < 16) { throw Corruption("truncated consensus identity"); }
	Identity result{};
	std::copy_n(bytes.begin(), result.size(), result.begin()); bytes.remove_prefix(result.size()); return result;
}
inline bool flag(std::string_view& bytes) {
	if (bytes.empty() || static_cast<unsigned char>(bytes[0]) > 1) { throw Corruption("invalid consensus flag"); }
	bool result = bytes[0] != 0; bytes.remove_prefix(1); return result;
}
inline void end(std::string_view bytes) { if (!bytes.empty()) { throw Corruption("trailing consensus storage bytes"); } }
} // namespace storage_detail

// The founding record. Roles are written (RFTINIT2) only when a founding member is not a
// voter, so an all-voter group's record stays the one older readers know.
inline std::string encode_initialization(const FixedConfiguration& configuration, const std::optional<Membership>& founding = {}) {
	using namespace storage_detail;
	if (configuration.voters.size() > 31) { throw std::length_error("too many voters"); }
	const bool roles = founding && std::any_of(founding->members.begin(), founding->members.end(), [](const Member& x) { return x.role != MemberRole::Voter; });
	std::string result;
	put64(result, roles ? initial_magic_v2 : initial_magic); identity(result, configuration.cluster); identity(result, configuration.configuration);
	put64(result, configuration.local); put32(result, static_cast<std::uint32_t>(configuration.voters.size()));
	for (auto voter : configuration.voters) { put64(result, voter); }
	if (roles) {
		auto bytes = encode_membership(*founding);
		put32(result, static_cast<std::uint32_t>(bytes.size())); result += bytes;
	}
	return result;
}
inline std::string encode_join(const FixedConfiguration& configuration) {
	using namespace storage_detail;
	if (!configuration.voters.empty() || configuration.local == 0) { throw std::invalid_argument("a join record names only its own incarnation"); }
	std::string result;
	put64(result, join_magic); identity(result, configuration.cluster); identity(result, configuration.configuration);
	put64(result, configuration.local);
	return result;
}

// The framing bound is shared by encoding and pre-event storage admission.
inline std::size_t storage_batch_size(bool hard, bool log, std::size_t entries = 0, std::size_t payload_bytes = 0) {
	if ((!log && (entries || payload_bytes)) || (!entries && payload_bytes) ||
		entries > std::numeric_limits<std::uint32_t>::max()) {
		throw std::length_error("invalid storage batch shape");
	}
	std::size_t result = 10 + (hard ? 24 : 0) + (log ? 12 : 0);
	auto maximum = std::numeric_limits<std::size_t>::max();
	if (entries > (maximum - result) / 21) { throw std::length_error("storage batch size overflow"); }
	result += 21 * entries;
	if (payload_bytes > maximum - result) { throw std::length_error("storage batch size overflow"); }
	return result + payload_bytes;
}

inline std::size_t storage_batch_size(const StorageBatch& batch) {
	std::size_t result = storage_batch_size(batch.hard.has_value(), batch.log.has_value(), batch.log ? batch.log->entries.size() : 0);
	if (batch.log) {
		for (const auto& entry : batch.log->entries) {
			if (entry.payload.size() > std::numeric_limits<std::uint32_t>::max() ||
				entry.payload.size() > std::numeric_limits<std::size_t>::max() - result) {
				throw std::length_error("storage payload overflow");
			}
			result += entry.payload.size();
		}
	}
	return result;
}

inline std::string encode_storage_batch(const StorageBatch& batch) {
	using namespace storage_detail;
	std::string result; result.reserve(storage_batch_size(batch));
	const bool membership = batch.log && std::any_of(batch.log->entries.begin(), batch.log->entries.end(), [](const Entry& e) { return e.kind == EntryKind::Configuration; });
	put64(result, membership ? membership_batch_magic : batch_magic);
	result.push_back(batch.hard ? 1 : 0);
	if (batch.hard) {
		put64(result, batch.hard->term); put64(result, batch.hard->voted_for.value_or(0)); put64(result, batch.hard->commit_index);
	}
	result.push_back(batch.log ? 1 : 0);
	if (batch.log) {
		put64(result, batch.log->replace_from); put32(result, static_cast<std::uint32_t>(batch.log->entries.size()));
		for (const auto& entry : batch.log->entries) {
			put64(result, entry.index); put64(result, entry.term); result.push_back(static_cast<char>(entry.kind));
			put32(result, static_cast<std::uint32_t>(entry.payload.size())); result.append(entry.payload);
		}
	}
	return result;
}

// Replay into unpublished state. Call finish only after Journal::recover has
// successfully returned the exact verified storage sequence.
class Recovery {
public:
	Recovery(FixedConfiguration configuration, Limits limits = {}) : expected_(std::move(configuration)), limits_(limits) {
		std::set<NodeId> voters(expected_.voters.begin(), expected_.voters.end());
		// No voters: a joiner, recovered from its join record (D5).
		if (expected_.local == 0 || voters.contains(0) || (!voters.empty() && !voters.contains(expected_.local)) ||
			voters.size() != expected_.voters.size() || voters.size() > limits.voters || limits.voters > 31 ||
			limits.command_bytes == 0 || limits.rpc_entries == 0 || limits.rpc_bytes < limits.command_bytes ||
			limits.log_bytes < limits.rpc_bytes || limits.log_entries == 0) { throw std::invalid_argument("invalid storage recovery configuration or limits"); }
		state_.configuration = expected_;
	}
	// Pure semantic validation, shared by encoding, decoding, and restore.
	// Return aggregate payload bytes without copying the retained suffix.
	void validate_checkpoint_metadata(const RecoveredState& state, std::size_t entry_count) const {
		if (state.configuration != expected_ || state.base_index == 0 ||
			state.base_index >= std::numeric_limits<Index>::max() || state.base_term == 0 || state.base_term > state.hard.term ||
			state.applied_index != state.base_index || state.hard.commit_index < state.base_index ||
			entry_count > ceilings.log_entries || entry_count >= std::numeric_limits<Index>::max() - state.base_index ||
			state.hard.commit_index > state.base_index + entry_count ||
			(state.hard.voted_for && *state.hard.voted_for == 0)) {
			throw storage_detail::Corruption("invalid consensus checkpoint state");
		}
	}
	std::size_t validate_checkpoint_state(const RecoveredState& state) const {
		validate_checkpoint_metadata(state, state.entries.size());
		Term previous = state.base_term; std::size_t bytes = 0;
		for (std::size_t i = 0; i < state.entries.size(); ++i) {
			const auto& entry = state.entries[i];
			if (entry.index != state.base_index + i + 1 || entry.term == 0 || entry.term < previous || entry.term > state.hard.term ||
				entry.payload.size() > ceilings.command_bytes || entry.payload.size() > ceilings.log_bytes - bytes ||
				static_cast<unsigned>(entry.kind) > static_cast<unsigned>(EntryKind::Configuration) || (entry.kind == EntryKind::NoOp && !entry.payload.empty())) {
				throw storage_detail::Corruption("invalid consensus checkpoint suffix");
			}
			if (entry.kind == EntryKind::Configuration) { (void)decode_membership(entry.payload); }  // throws Corruption
			previous = entry.term; bytes += entry.payload.size();
		}
		return bytes;
	}
	// Only the checkpoint adapter supplies this state, after validating the
	// bundle's storage sequence and explicit application dependency.
	void restore(RecoveredState state, std::uint64_t covered_sequence) {
		if (finished_ || failed_ || sequence_ != 0) { throw std::logic_error("checkpoint restoration unavailable"); }
		try {
			if (covered_sequence == 0) { throw storage_detail::Corruption("invalid checkpoint storage sequence"); }
			auto bytes = validate_checkpoint_state(state);
			state_ = std::move(state); bytes_ = bytes; sequence_ = covered_sequence;
		} catch (...) { failed_ = true; throw; }
	}
	void replay(std::uint64_t sequence, std::string_view bytes) {
		using namespace storage_detail;
		if (finished_ || failed_) { throw std::logic_error("recovery unavailable"); }
		try {
			if (sequence_ == std::numeric_limits<std::uint64_t>::max() || sequence != sequence_ + 1) { throw Corruption("storage sequence mismatch"); }
			if (sequence == 1) { initialization(bytes); }
			else { apply(decode(bytes)); }
			sequence_ = sequence;
		} catch (...) { failed_ = true; throw; }
	}
	RecoveredState finish(std::uint64_t verified_sequence) {
		if (failed_ || finished_ || sequence_ == 0 || verified_sequence != sequence_) { throw std::logic_error("recovery has no verified initialized frontier"); }
		finished_ = true; return std::move(state_);
	}

private:
	void initialization(std::string_view bytes) {
		using namespace storage_detail;
		const auto bound = 52 + 31 * 8 + 4 + 36 + 24 * std::size_t(ceilings.members);  // RFTINIT2 with its largest membership
		if (bytes.size() > bound) { throw Corruption("invalid consensus initialization"); }
		const auto magic = get64(bytes);
		if (magic != initial_magic && magic != initial_magic_v2 && magic != join_magic) { throw Corruption("invalid consensus initialization"); }
		FixedConfiguration configuration;
		configuration.cluster = identity(bytes); configuration.configuration = identity(bytes); configuration.local = get64(bytes);
		if (magic == join_magic) {
			end(bytes);
			if (configuration != expected_) { throw Corruption("stored join record does not match"); }
			state_.membership = Membership{};
			return;
		}
		if (expected_.voters.empty()) { throw Corruption("a joiner's store holds a founding record"); }
		auto count = get32(bytes);
		if (count > limits_.voters || count > bytes.size() / 8) { throw Corruption("initialization voter count overflow"); }
		for (std::uint32_t i = 0; i < count; ++i) { configuration.voters.push_back(get64(bytes)); }
		if (magic == initial_magic_v2) {
			auto size = get32(bytes);
			if (size > bytes.size()) { throw Corruption("truncated founding membership"); }
			auto m = decode_membership(bytes.substr(0, size)); bytes.remove_prefix(size);
			// The roles name exactly the founding voters, each its own node.
			std::set<NodeId> voting, founding(configuration.voters.begin(), configuration.voters.end());
			for (const auto& x : m.members) {
				if (x.node != x.incarnation || x.role == MemberRole::Learner) { throw Corruption("founding membership member"); }
				voting.insert(x.incarnation);
			}
			if (voting != founding || m.leaving) { throw Corruption("founding membership does not match its voters"); }
			// RFTINIT2 is written only for a non-voter, so each founding has one encoding.
			if (std::all_of(m.members.begin(), m.members.end(), [](const Member& x) { return x.role == MemberRole::Voter; })) { throw Corruption("founding roles without a non-voter"); }
			state_.membership = std::move(m);
		}
		end(bytes);
		if (configuration != expected_) { throw Corruption("stored fixed configuration does not match"); }
	}
	// Bounded by the ceilings (R16); the core checks each entry by position.
	StorageBatch decode(std::string_view bytes) {
		using namespace storage_detail;
		// Metadata has its own bound; payload accounting below is independent.
		if (ceilings.rpc_entries > (std::numeric_limits<std::size_t>::max() - 46) / 21 ||
			ceilings.rpc_bytes > std::numeric_limits<std::size_t>::max() - 46 - ceilings.rpc_entries * 21 ||
			bytes.size() > 46 + ceilings.rpc_entries * 21 + ceilings.rpc_bytes) {
			throw Corruption("invalid consensus storage batch size or version");
		}
		const auto magic = get64(bytes);
		if (magic != batch_magic && magic != membership_batch_magic) { throw Corruption("invalid consensus storage batch size or version"); }
		const auto highest_kind = magic == batch_magic ? EntryKind::NoOp : EntryKind::Configuration;
		StorageBatch batch;
		if (flag(bytes)) {
			HardState hard; hard.term = get64(bytes); auto vote = get64(bytes); hard.commit_index = get64(bytes);
			if (vote) { hard.voted_for = vote; } batch.hard = hard;
		}
		if (flag(bytes)) {
			LogMutation mutation; mutation.replace_from = get64(bytes); auto count = get32(bytes);
			if (count > ceilings.rpc_entries || count > bytes.size() / 21) { throw Corruption("storage entry count exceeds bound"); }
			std::size_t total = 0;
			for (std::uint32_t i = 0; i < count; ++i) {
				Entry entry; entry.index = get64(bytes); entry.term = get64(bytes);
				if (bytes.empty() || static_cast<unsigned char>(bytes[0]) > static_cast<unsigned char>(highest_kind)) { throw Corruption("invalid storage entry kind"); }
				entry.kind = static_cast<EntryKind>(bytes[0]); bytes.remove_prefix(1);
				auto length = get32(bytes);
				if (length > ceilings.command_bytes || length > ceilings.rpc_bytes - total || length > bytes.size()) { throw Corruption("storage payload exceeds bound"); }
				entry.payload.assign(bytes.substr(0, length)); bytes.remove_prefix(length); total += length;
				if (entry.kind == EntryKind::Configuration) { (void)decode_membership(entry.payload); }
				mutation.entries.push_back(std::move(entry));
			}
			batch.log = std::move(mutation);
		}
		end(bytes);
		if (!batch.hard && !batch.log) { throw Corruption("empty consensus storage operation"); }
		return batch;
	}
	void apply(StorageBatch batch) {
		using storage_detail::Corruption;
		auto hard = batch.hard.value_or(state_.hard);
		if (hard.term < state_.hard.term || hard.commit_index < state_.hard.commit_index ||
			(hard.voted_for && *hard.voted_for == 0) ||  // a ballot may name any incarnation (Track C, R2)
			(hard.term == 0 && hard.voted_for) ||
			(hard.term == state_.hard.term && state_.hard.voted_for && hard.voted_for != state_.hard.voted_for)) {
			throw Corruption("invalid durable hard-state transition");
		}
		auto new_count = state_.entries.size(); auto new_bytes = bytes_;
		if (batch.log) {
			auto from = batch.log->replace_from;
			if (from == 0 || from <= state_.hard.commit_index || from <= state_.base_index || from - state_.base_index - 1 > state_.entries.size() ||
				batch.log->entries.size() > ceilings.log_entries - static_cast<std::size_t>(from - state_.base_index - 1)) { throw Corruption("invalid logical log replacement"); }
			new_count = static_cast<std::size_t>(from - state_.base_index - 1) + batch.log->entries.size();
			for (std::size_t i = static_cast<std::size_t>(from - state_.base_index - 1); i < state_.entries.size(); ++i) { new_bytes -= state_.entries[i].payload.size(); }
			Term term = from == state_.base_index + 1 ? state_.base_term : state_.entries[from - state_.base_index - 2].term;
			Index index = from;
			for (const auto& entry : batch.log->entries) {
				if (entry.index != index++ || entry.index >= std::numeric_limits<Index>::max() || entry.term == 0 || entry.term < term ||
					entry.term > hard.term || (entry.kind == EntryKind::NoOp && !entry.payload.empty()) ||
					entry.payload.size() > ceilings.log_bytes - new_bytes) { throw Corruption("invalid logical replacement entry"); }
				if (entry.index <= state_.base_index + state_.entries.size() && state_.entries[entry.index - state_.base_index - 1].term == entry.term && state_.entries[entry.index - state_.base_index - 1] != entry) {
					throw Corruption("same-index same-term storage content conflict");
				}
				new_bytes += entry.payload.size(); term = entry.term;
			}
		}
		if (hard.commit_index > state_.base_index + new_count) { throw Corruption("commit beyond recovered log"); }
		if (batch.log) {
			state_.entries.resize(static_cast<std::size_t>(batch.log->replace_from - state_.base_index - 1));
			for (auto& entry : batch.log->entries) { state_.entries.push_back(std::move(entry)); }
		}
		state_.hard = hard; bytes_ = new_bytes;
	}

	FixedConfiguration expected_;
	Limits limits_;
	RecoveredState state_;
	std::uint64_t sequence_ = 0;
	std::size_t bytes_ = 0;
	bool failed_ = false, finished_ = false;
};

} // namespace kronuz::consensus
