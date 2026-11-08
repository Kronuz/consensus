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

#include "membership.h"

namespace kronuz::consensus {

struct RetainedStatePolicy {
	std::size_t command_bytes = ceilings.command_bytes;
	std::size_t log_bytes = ceilings.log_bytes;
	std::size_t log_entries = ceilings.log_entries;
	std::size_t batch_bytes = ceilings.rpc_bytes;
	std::size_t batch_entries = ceilings.rpc_entries;
	bool operator==(const RetainedStatePolicy&) const = default;
};
struct RetainedUsage {
	std::size_t entries = 0;
	std::size_t payload_bytes = 0;
	bool operator==(const RetainedUsage&) const = default;
};
namespace retained_detail {
inline void validate_policy(const RetainedStatePolicy& p) {
	if (!p.command_bytes || !p.log_bytes || !p.log_entries || !p.batch_bytes || !p.batch_entries ||
		p.command_bytes > ceilings.command_bytes || p.log_bytes > ceilings.log_bytes ||
		p.log_entries > ceilings.log_entries || p.batch_bytes > ceilings.rpc_bytes || p.batch_entries > ceilings.rpc_entries) {
		throw std::invalid_argument("invalid retained-state resource policy");
	}
}
inline void validate_policy(const RetainedStatePolicy& p, const Limits& operating) {
	validate_policy(p);
	if (operating.command_bytes > p.command_bytes || operating.log_bytes > p.log_bytes ||
		operating.log_entries > p.log_entries || operating.rpc_bytes > p.batch_bytes || operating.rpc_entries > p.batch_entries) {
		throw std::invalid_argument("operating limits exceed retained-state resources");
	}
}
inline bool payload_fits(EntryKind kind, std::size_t bytes, std::size_t command_cap) noexcept {
	switch (kind) {
	case EntryKind::Command: return bytes <= command_cap;
	case EntryKind::NoOp: return bytes == 0;
	case EntryKind::Configuration: return bytes <= maximum_membership_bytes;
	}
	return false;
}
inline bool payload_valid(EntryKind kind, std::string_view payload, std::size_t command_cap) {
	if (!payload_fits(kind, payload.size(), command_cap)) { return false; }
	if (kind == EntryKind::Configuration) {
		try { (void)decode_membership(payload); }
		catch (const kronuz::journal::Corruption&) { return false; }
	}
	return true;
}
inline std::optional<RetainedUsage> receive_usage(const Receive& receive, const RetainedStatePolicy& policy) {
	const auto* first = std::get_if<AppendRequest>(&receive.message);
	if (!first) { return receive.more.empty() ? std::optional<RetainedUsage>{RetainedUsage{}} : std::nullopt; }
	if (receive.more.size() >= policy.batch_entries) { return std::nullopt; }
	RetainedUsage usage;
	auto append = [&](const AppendRequest& request) {
		if (request.entries.size() > policy.batch_entries - usage.entries) { return false; }
		for (const auto& entry : request.entries) {
			if (!payload_fits(entry.kind, entry.payload.size(), policy.command_bytes) ||
				entry.payload.size() > policy.batch_bytes - usage.payload_bytes) { return false; }
			usage.payload_bytes += entry.payload.size();
		}
		usage.entries += request.entries.size();
		return true;
	};
	if (!append(*first)) { return std::nullopt; }
	for (const auto& more : receive.more) { if (!append(more)) { return std::nullopt; } }
	return usage;
}
inline bool metadata_valid(const RecoveredState& state, std::size_t count, const RetainedStatePolicy& p) {
	const auto maximum = std::numeric_limits<Index>::max();
	if (state.configuration.voters.size() > ceilings.voters ||
		state.base_index >= maximum || ((state.base_index == 0) != (state.base_term == 0)) ||
		state.base_term > state.hard.term || state.applied_index != state.base_index ||
		count > p.log_entries || count >= maximum - state.base_index ||
		state.hard.commit_index < state.base_index || state.hard.commit_index > state.base_index + count ||
		(state.hard.voted_for && (*state.hard.voted_for == 0 || state.hard.term == 0))) { return false; }
	if (state.membership) {
		if (state.membership->members.empty()) {
			if (!state.configuration.voters.empty() || state.base_index != 0) { return false; }
		} else {
			try { validate_membership(*state.membership, ceilings); }
			catch (const std::invalid_argument&) { return false; }
		}
	}
	return true;
}
class Cursor {
public:
	Cursor(Index base, Term previous, Term hard_term, RetainedStatePolicy policy)
		: next_(base + 1), previous_(previous), hard_term_(hard_term), policy_(policy) {}
	bool accept(Index index, Term term, EntryKind kind, std::string_view payload) {
		if (index != next_ || index == std::numeric_limits<Index>::max() || !term || term < previous_ || term > hard_term_ ||
			usage_.entries >= policy_.log_entries || usage_.payload_bytes > policy_.log_bytes ||
			payload.size() > policy_.log_bytes - usage_.payload_bytes || !payload_valid(kind, payload, policy_.command_bytes)) { return false; }
		++next_; previous_ = term; ++usage_.entries; usage_.payload_bytes += payload.size();
		return true;
	}
	RetainedUsage usage() const noexcept { return usage_; }
private:
	Index next_;
	Term previous_, hard_term_;
	RetainedStatePolicy policy_;
	RetainedUsage usage_;
};
} // namespace retained_detail
} // namespace kronuz::consensus
