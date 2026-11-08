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

// One core's turn driver: the groups' Stores (shared backend) stage their records here,
// and each turn writes them with one data barrier. A record staged with an operation
// completes that operation at its turn's barrier, or fails it with the turn.

#pragma once

#include "turn_log.h"
#include <memory>
#include <vector>

namespace kronuz::journal {

class TurnDriver {
public:
	TurnDriver(IO& io, Identity identity, TurnLogOptions options = {}) : log_(io, identity, recovered_, options) {}
	TurnDriver(const TurnDriver&) = delete;
	TurnDriver& operator=(const TurnDriver&) = delete;

	// What recovery kept, per group, from its latest checkpoint record.
	const TurnRecovery& recovered() const noexcept { return recovered_; }
	bool has_group(std::uint64_t group) const { auto found = recovered_.groups.find(group); return found != recovered_.groups.end() && !found->second.empty(); }
	bool lost(std::uint64_t group) const { return recovered_.lost.contains(group); }
	// A group's recovered records, released once its Store has replayed them.
	const std::vector<TurnRecord>& records(std::uint64_t group) const {
		static const std::vector<TurnRecord> none;
		auto found = recovered_.groups.find(group); return found == recovered_.groups.end() ? none : found->second;
	}
	void release(std::uint64_t group) { recovered_.groups.erase(group); }
	// The group's last durable record sequence, 0 for none.
	std::uint64_t sequence(std::uint64_t group) const { return log_.sequence(group); }
	bool failed() const noexcept { return log_failed_; }

	// Stages a record for the next turn; its operation, if any, completes at that turn's
	// barrier with the token it was staged with.
	void stage(TurnRecord record, std::shared_ptr<IOOperation> operation = {}, MutationToken token = {}) {
		if (log_failed_) { throw std::logic_error("turn driver failed"); }
		pending_.push_back({std::move(record), std::move(operation), token});
	}
	bool idle() const noexcept { return pending_.empty(); }
	bool in_flight() const noexcept { return write_.has_value(); }

	// Stages the pending records as one turn and submits their operations; the host writes
	// write().bytes at write().offset, calls File::sync_data, then completed() or failed().
	const TurnLog::TurnWrite& begin() {
		if (write_) { throw std::logic_error("a turn is already in flight"); }
		if (pending_.empty()) { throw std::logic_error("no records to write"); }
		std::vector<TurnRecord> records; records.reserve(pending_.size());
		for (auto& item : pending_) { records.push_back(std::move(item.record)); }
		flight_ = std::move(pending_); pending_.clear();
		try { write_ = log_.stage(records); }
		catch (...) { fail(std::current_exception()); throw; }
		for (auto& item : flight_) { if (item.operation) { item.operation->submitted(); item.submitted = true; } }
		return *write_;
	}
	const TurnLog::TurnWrite& write() const { if (!write_) { throw std::logic_error("no turn in flight"); } return *write_; }
	void completed() {
		if (!write_) { throw std::logic_error("no turn in flight"); }
		// Reclamation after the barrier can fail; the operations then fail too, which is
		// safe: a durable record reported failed is merely unacknowledged.
		try { log_.completed(); } catch (...) { log_.failed(); write_.reset(); fail(std::current_exception()); throw; }
		write_.reset();
		auto flight = std::move(flight_); flight_.clear();
		for (auto& item : flight) { finish(item, nullptr); }
	}
	// The turn's write or barrier failed: the log fails, and so does every operation
	// staged with it or after it.
	void failed(std::exception_ptr error) noexcept { log_.failed(); write_.reset(); fail(error); }

	// Writes and syncs the pending turn synchronously.
	void commit() {
		const auto& write = begin();
		try {
			if (write.file->write_at(write.offset, write.bytes) != write.bytes.size()) { throw std::runtime_error("short turn write"); }
			write.file->sync_data();
		} catch (...) { failed(std::current_exception()); throw; }
		completed();
	}

	TurnLog& log() noexcept { return log_; }

private:
	struct Pending { TurnRecord record; std::shared_ptr<IOOperation> operation; MutationToken token; bool submitted = false; };
	static void finish(Pending& item, std::exception_ptr error) noexcept {
		if (!item.operation) { return; }
		MutationCompletion completion; completion.token = item.token; completion.error = error;
		item.operation->complete(std::move(completion));
	}
	void fail(std::exception_ptr error) noexcept {
		log_failed_ = true;
		auto flight = std::move(flight_); flight_.clear();
		for (auto& item : pending_) { flight.push_back(std::move(item)); }
		pending_.clear();
		// An operation not yet submitted is submitted first, so failing it settles it.
		for (auto& item : flight) {
			if (!item.operation) { continue; }
			if (!item.submitted) { try { item.operation->submitted(); } catch (...) { continue; } }
			finish(item, error);
		}
	}

	TurnRecovery recovered_;
	TurnLog log_;
	std::vector<Pending> pending_, flight_;
	std::optional<TurnLog::TurnWrite> write_;
	bool log_failed_ = false;
};

}  // namespace kronuz::journal
