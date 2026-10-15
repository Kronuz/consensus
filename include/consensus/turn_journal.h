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

// D11: every group on a core persists through one turn log (journal's RFTW0001). A group's
// first record is its founding or join record; each Persist batch is one record after it.
// The batches the groups staged since the last turn are written as one turn with one data
// barrier, after which each group gets its Persisted. Recovery replays a group's records
// through Recovery, as the per-group journal's replay does.

#pragma once

#include "storage.h"
#include "journal/turn_log.h"
#include <map>
#include <optional>
#include <set>
#include <vector>

namespace kronuz::consensus {

class TurnJournal {
public:
	TurnJournal(kronuz::journal::IO& io, kronuz::journal::Identity identity, kronuz::journal::TurnLogOptions options = {})
		: log_(io, identity, recovered_, options) {}

	// The group's recovered state, or none when it has no records. A lost group (damage) is
	// none as well and rejoins as a new incarnation (D5).
	std::optional<RecoveredState> recover(std::uint64_t group, const FixedConfiguration& configuration, const Limits& limits) const {
		auto found = recovered_.groups.find(group);
		if (found == recovered_.groups.end() || recovered_.lost.contains(group)) { return std::nullopt; }
		Recovery recovery(configuration, limits);
		for (const auto& record : found->second) { recovery.replay(record.sequence, record.payload); }
		return recovery.finish(found->second.back().sequence);
	}
	bool lost(std::uint64_t group) const { return recovered_.lost.contains(group); }
	bool damaged() const noexcept { return recovered_.damaged; }
	const std::map<std::uint64_t, std::vector<kronuz::journal::TurnRecord>>& groups() const noexcept { return recovered_.groups; }

	// Stages a new group's first record: its founding record, or a joiner's join record.
	void create(std::uint64_t group, const FixedConfiguration& configuration, std::optional<Membership> founding = {}) {
		if (log_.sequence(group) || pending_sequence(group)) { throw std::logic_error("group already has records"); }
		stage_bytes(group, configuration.voters.empty() ? encode_join(configuration) : encode_initialization(configuration, founding), {});
	}
	// Stages a group's Persist batch; its token is released by the turn that writes it.
	void stage(std::uint64_t group, const Persist& persist) { stage_bytes(group, encode_storage_batch(persist.batch), persist.token); }
	bool idle() const noexcept { return pending_.empty(); }

	// Writes the staged turn and makes it durable; returns each staged Persist's group and
	// token, in staging order, for the host to deliver as Persisted.
	std::vector<std::pair<std::uint64_t, Token>> commit() {
		auto released = begin();
		auto& write = *write_;
		try {
			if (write.file->write_at(write.offset, write.bytes) != write.bytes.size()) { throw std::runtime_error("short turn write"); }
			write.file->sync_data();
		} catch (...) { failed(); throw; }
		completed();
		return released;
	}
	// The completion-driven form: begin() stages the turn and exposes its write; after the
	// host's write and File::sync_data, completed() publishes it.
	std::vector<std::pair<std::uint64_t, Token>> begin() {
		if (write_) { throw std::logic_error("a turn is already in flight"); }
		std::vector<kronuz::journal::TurnRecord> records;
		std::vector<std::pair<std::uint64_t, Token>> released;
		for (auto& item : pending_) {
			records.push_back(std::move(item.record));
			if (item.token) { released.emplace_back(item.group, item.token); }
		}
		pending_.clear(); next_.clear();
		write_ = log_.stage(records);
		return released;
	}
	const kronuz::journal::TurnLog::TurnWrite& write() const { if (!write_) { throw std::logic_error("no turn in flight"); } return *write_; }
	void completed() { log_.completed(); write_.reset(); }
	void failed() noexcept { log_.failed(); write_.reset(); }

	kronuz::journal::TurnLog& log() noexcept { return log_; }

private:
	struct Pending { std::uint64_t group; kronuz::journal::TurnRecord record; Token token; };
	std::uint64_t pending_sequence(std::uint64_t group) const { auto found = next_.find(group); return found == next_.end() ? 0 : found->second; }
	void stage_bytes(std::uint64_t group, std::string bytes, Token token) {
		auto& next = next_.try_emplace(group, log_.sequence(group)).first->second;
		pending_.push_back({group, {group, ++next, kronuz::journal::TurnRecordKind::Batch, std::move(bytes)}, token});
	}

	kronuz::journal::TurnRecovery recovered_;
	kronuz::journal::TurnLog log_;
	std::vector<Pending> pending_;
	std::map<std::uint64_t, std::uint64_t> next_;
	std::optional<kronuz::journal::TurnLog::TurnWrite> write_;
};

}  // namespace kronuz::consensus
