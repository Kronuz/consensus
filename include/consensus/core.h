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
#include "retained_log.h"
#include "retirement.h"
#include <algorithm>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace kronuz::consensus {

// One owning executor. No I/O, clock reads, entropy, or application callbacks.
// Persist changes are staged internally; all dependent actions are deferred.
class Core {
	struct CaptureOwner { const Core* core; };
public:
	Core(FixedConfiguration configuration, RecoveredState recovered, Limits limits = {}, Timing timing = {})
		: configuration_(std::move(configuration)), hard_(recovered.hard), durable_(hard_),
		entries_(std::move(recovered.entries)), base_index_(recovered.base_index), base_term_(recovered.base_term), limits_(limits), timing_(timing), election_delay_(timing.election_min), applied_(recovered.applied_index) {
		if (configuration_ != recovered.configuration || configuration_.local == 0 || limits.voters > 31) {
			throw std::invalid_argument("invalid or mismatched fixed configuration");
		}
		if (recovered.membership) {
			// A joining node (Track C, D5) starts with no membership; it learns one from the log.
			base_membership_ = *recovered.membership;
			if (!base_membership_.members.empty()) { validate_membership(base_membership_, limits); }
		} else {
			std::set<NodeId> voters(configuration_.voters.begin(), configuration_.voters.end());
			if (configuration_.voters.empty() || configuration_.voters.size() > limits.voters || voters.size() != configuration_.voters.size() ||
				voters.contains(0) || !voters.contains(configuration_.local)) {
				throw std::invalid_argument("invalid voters");
			}
			base_membership_ = fixed_membership(configuration_);
		}
		if (limits.command_bytes == 0 || limits.rpc_entries == 0 || limits.log_entries == 0 || limits.reads == 0 || limits.uncommitted_entries == 0 ||
			limits.control_entries == 0 || limits.control_entries > limits.log_entries ||
			limits.command_bytes > limits.rpc_bytes || limits.rpc_bytes > limits.log_bytes ||
			timing.heartbeat == 0 || timing.rpc_timeout == 0 || timing.snapshot_timeout == 0 || timing.election_min <= timing.heartbeat ||
			timing.election_max < timing.election_min) { throw std::invalid_argument("invalid consensus limits or timing"); }
		if (base_index_ >= maximum || ((base_index_ == 0) != (base_term_ == 0)) || base_term_ > hard_.term ||
			entries_.size() >= maximum - base_index_ || recovered.applied_index != base_index_ || hard_.commit_index < base_index_) {
			throw std::invalid_argument("invalid recovered checkpoint boundary");
		}
		if (hard_.voted_for && *hard_.voted_for == 0) { throw std::invalid_argument("recovered ballot names no incarnation"); }
		if (hard_.term == 0 && hard_.voted_for) { throw std::invalid_argument("term zero cannot contain a ballot"); }
		if (entries_.size() > ceilings.log_entries || hard_.commit_index > last_index()) { throw std::invalid_argument("invalid recovered log bounds"); }
		Term previous_term = base_term_;
		for (std::size_t i = 0; i < entries_.size(); ++i) {
			const auto& entry = entries_[i];
			if (entry.index != base_index_ + i + 1 || !valid_entry(entry) || entry.term > hard_.term || entry.term < previous_term ||
				entry.payload.size() > limits_at(entry.index).command_bytes || entry.payload.size() > ceilings.log_bytes - log_bytes_) { throw std::invalid_argument("invalid recovered entry"); }
			log_bytes_ += entry.payload.size(); previous_term = entry.term;
			note_append(entry);
		}
	}
	~Core() { capture_owner_->core = nullptr; }
	Core(const Core&) = delete;
	Core& operator=(const Core&) = delete;
	Role role() const noexcept { return role_; }
	Term term() const noexcept { return hard_.term; }
	Index committed() const noexcept { return durable_.commit_index; }
	Index applied() const noexcept { return applied_; }
	// The application epoch: Repair advances it, and a LocalCheckpoint from an older one is
	// rejected, so a capture of damaged state never becomes the base.
	std::uint64_t epoch() const noexcept { return epoch_; }
	std::size_t retained_log_bytes() const noexcept { return log_bytes_; }
	std::size_t retired_log_bytes() const noexcept { return entries_.retired_bytes(); }
	std::size_t retired_log_entries() const noexcept { return entries_.retired_entries(); }
	void retire_log() noexcept { entries_.retire(256, limits_.command_bytes); }
	// Terminal host operation: accepted storage and external capture consumers
	// must settle first. This abandons protocol interest, never acknowledges IO.
	void begin_retirement() noexcept {
		if (retiring_) { return; }
		retiring_ = true; capture_owner_->core = nullptr;
		entries_.clear(); log_bytes_ = 0;
	}
	bool retirement_step(detail::RetirementBudget& budget) noexcept {
		if (!retiring_) { return false; }
		if (pending_) {
			if (!detail::retire_actions(pending_->deferred, budget)) { return false; }
			pending_.reset();
		}
		if (install_) {
			if (!detail::retire_actions(install_->deferred, budget)) { return false; }
			install_.reset();
		}
		if (captured_) {
			if (!detail::retire_entries(captured_->state_.entries, budget)) { return false; }
			captured_.reset();
		}
		while (!reads_.empty()) {
			if (!budget.consume()) { return false; }
			reads_.pop_back();
		}
		while (!peers_.empty()) {
			if (!budget.consume()) { return false; }
			peers_.erase(peers_.begin());
		}
		while (!grants_.empty()) {
			if (!budget.consume()) { return false; }
			grants_.erase(grants_.begin());
		}
		auto before = entries_.retired_bytes();
		budget.units -= entries_.retire(budget.units, budget.bytes);
		budget.bytes -= before - entries_.retired_bytes();
		return retirement_drained();
	}
	bool retirement_drained() const noexcept {
		return retiring_ && !pending_ && !install_ && !captured_ && reads_.empty() &&
			peers_.empty() && grants_.empty() && !entries_.charged_entries();
	}
	Index last_index() const noexcept { return base_index_ + static_cast<Index>(entries_.size()); }
	Index base_index() const noexcept { return base_index_; }
	LogBoundary base_boundary() const noexcept { return {base_index_, base_term_}; }
	std::optional<SnapshotKey> awaiting_snapshot(NodeId peer) const {
		auto found = peers_.find(peer);
		if (retiring_ || role_ != Role::Leader || found == peers_.end() || !found->second.snapshot || found->second.snapshot->phase != SnapshotPhase::AwaitSource) { return std::nullopt; }
		return found->second.snapshot->key;
	}
	bool snapshot_active(NodeId peer, const SnapshotKey& key) const {
		auto found = peers_.find(peer);
		return !retiring_ && role_ == Role::Leader && hard_.term == durable_.term && found != peers_.end() && found->second.snapshot && found->second.snapshot->key == key;
	}
	bool snapshot_sending(NodeId peer, const SnapshotKey& key) const {
		auto found = peers_.find(peer);
		return snapshot_active(peer, key) && found->second.snapshot->phase == SnapshotPhase::Sending;
	}
	bool busy() const noexcept { return pending_.has_value() || install_.has_value(); }
	bool transferring() const noexcept { return transfer_.has_value(); }
	// The limits the group applies to its next entry (R16).
	const Limits& group_limits() const { return limits_in_force(); }
	// The membership in force at the base index, as a snapshot carries it; absent for a
	// fixed group's founding membership.
	std::optional<Membership> base_membership() const { return retain_base_membership(base_membership_); }
	Index witness_bound() const noexcept { return witness_bound_; }
	const HardState& durable_hard_state() const noexcept { return durable_; }
	const FixedConfiguration& configuration() const noexcept { return configuration_; }
	// The membership in force (the newest Configuration entry in the log, committed or not)
	// and the newest committed one (Track C, D1).
	const Membership& membership() const noexcept { return configs_.empty() ? base_membership_ : configs_.rbegin()->second; }
	const Membership& committed_membership() const noexcept { return membership_at(durable_.commit_index); }
	// Whether a ChangeMembership would pass D3's preconditions now: a leader, not busy, that
	// committed an entry of its term, with no configuration above its commit index.
	bool can_change_membership() const noexcept {
		return started_ && role_ == Role::Leader && !busy() && durable_.commit_index > base_index_ &&
			log_term(durable_.commit_index) == hard_.term && (configs_.empty() || configs_.rbegin()->first <= durable_.commit_index);
	}
	// A member's matched index, as the leader knows it (D8).
	std::optional<Index> progress(NodeId incarnation) const {
		if (role_ != Role::Leader) { return std::nullopt; }
		if (incarnation == configuration_.local) { return last_index(); }
		auto found = peers_.find(incarnation);
		return found == peers_.end() ? std::nullopt : std::optional<Index>(found->second.matched);
	}

	class FrozenCheckpoint {
	public:
		const RecoveredState& metadata() const { validate(); return state_; }
		std::size_t size() const { validate(); return count_; }
		const Entry& entry(std::size_t offset) const {
			validate(); if (offset >= count_) { throw std::out_of_range("frozen checkpoint offset"); }
			return owner_->core->log_entry(state_.base_index + offset + 1);
		}
		Token token() const noexcept { return token_; }
	private:
		friend class Core;
		FrozenCheckpoint(const Core& owner, Token token, RecoveredState state, std::size_t count)
			: owner_(owner.capture_owner_), token_(token), state_(std::move(state)), count_(count) {}
		void validate() const {
			auto* core = owner_ ? owner_->core : nullptr;
			if (!core || core->role_ == Role::Fenced ||
				!((core->pending_ && core->pending_->token == token_ && core->pending_->checkpoint) ||
				(core->install_ && core->install_->token == token_ && !core->install_->published))) {
				throw std::logic_error("stale frozen checkpoint capability");
			}
		}
		std::shared_ptr<CaptureOwner> owner_;
		Token token_;
		RecoveredState state_;
		std::size_t count_;
	};
	struct CapturedActions { Actions actions; std::optional<FrozenCheckpoint> capture; };
	// Trusted Worker path: the same transition retains a validated range rather
	// than materializing its entries into the legacy persistence action.
	CapturedActions step_capture(Event event) {
		if (!std::holds_alternative<LocalCheckpoint>(event) && !std::holds_alternative<InstallPrepared>(event)) {
			throw std::invalid_argument("frozen capture requires checkpoint transition");
		}
		capture_mode_ = true;
		struct ModeReset { bool& mode; ~ModeReset() { mode = false; } } reset{capture_mode_};
		auto actions = step(std::move(event));
		auto capture = std::move(captured_); captured_.reset();
		if (role_ == Role::Fenced) { capture.reset(); }
		return {std::move(actions), std::move(capture)};
	}

	Actions step(Event event) {
		if (retiring_) { return {}; }
		retire_log();
		Actions output;
		if (role_ == Role::Fenced) { return output; }
		try { std::visit([&](auto&& value) { handle(std::move(value), output); }, std::move(event)); }
		catch (const std::exception& error) { fence(error.what(), output); }
		return output;
	}

private:
	struct Pending { Token token; HardState hard; Actions deferred; bool elected = false; std::optional<std::pair<Index, Term>> checkpoint; };
	struct PendingInstall {
		Token token, prepared; RequestId request; LogBoundary boundary; HardState hard;
		Actions deferred; bool retain_suffix, published = false;
		std::optional<Membership> membership{};
	};
	struct Flight { Token rpc; Index previous, through; Token read_probe; std::uint64_t sent; bool keepalive = false; };
	enum class SnapshotPhase { AwaitSource, Sending };
	struct SnapshotFlight { SnapshotKey key; SnapshotPhase phase; std::uint64_t started; };
	struct Peer { Index next = 1, matched = 0; std::optional<Flight> flight; std::uint64_t retry_after = 0; std::optional<SnapshotFlight> snapshot{}; std::optional<std::uint64_t> snapshot_retry_clock{}; };
	struct PendingRead { RequestId request; Token probe; Term term; Index index; std::set<NodeId> acknowledgements; };
	struct Transfer { NodeId target; Token token; std::uint64_t until; bool sent = false; };
	static constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
	const Membership& membership_at(Index index) const {
		auto it = configs_.upper_bound(index);
		return it == configs_.begin() ? base_membership_ : std::prev(it)->second;
	}
	bool is_voter(NodeId incarnation) const { return votes_in(membership(), incarnation); }
	std::vector<NodeId> voters() const {
		std::vector<NodeId> out;
		for (const auto& x : membership().members) { if (voting(x.role)) { out.push_back(x.incarnation); } }
		return out;
	}
	std::size_t quorum() const { return voter_count(membership()) / 2 + 1; }
	// The incarnations a leader replicates to: every member in force, and those of the
	// committed membership until their removal commits (D4).
	std::set<NodeId> replicas() const {
		std::set<NodeId> out;
		for (const auto& x : membership().members) { out.insert(x.incarnation); }
		for (const auto& x : committed_membership().members) { out.insert(x.incarnation); }
		out.erase(configuration_.local);
		return out;
	}
	// What a checkpoint or install records as its base membership: nothing for a fixed
	// configuration's founding membership, so a fixed group's bytes stay as they were.
	std::optional<Membership> retain_base_membership(std::optional<Membership> m) const {
		if (!m || *m == fixed_membership(configuration_)) { return std::nullopt; }
		return m;
	}
	// A Configuration entry without limits keeps those before it (R16).
	void note_append(const Entry& entry) {
		if (entry.kind != EntryKind::Configuration) { return; }
		auto m = decode_membership(entry.payload);
		if (!m.limits) { m.limits = membership_at(entry.index - 1).limits; }
		configs_[entry.index] = std::move(m);
	}
	// The limits a group applies: its newest configuration's, else the node's (R16).
	const Limits& limits_of(const Membership& m) const { return m.limits ? *m.limits : limits_; }
	const Limits& limits_in_force() const { return limits_of(membership()); }
	// The limits an entry at `index` is checked against: those in force just before it.
	const Limits& limits_at(Index index) const { return limits_of(membership_at(index - 1)); }
	void note_truncate(Index from) { configs_.erase(configs_.lower_bound(from), configs_.end()); }
	void note_base(Index through) {
		base_membership_ = membership_at(through);
		configs_.erase(configs_.begin(), configs_.upper_bound(through));
	}
	Token needed_probe(NodeId peer) const {
		for (const auto& read : reads_) {
			if (acknowledged(read) < quorum() && !read.acknowledgements.contains(peer) && is_voter(peer)) { return read.probe; }
		}
		return 0;
	}
	// A read counts acknowledgements only from voters of the membership in force (D4).
	std::size_t acknowledged(const PendingRead& read) const {
		return std::size_t(std::count_if(read.acknowledgements.begin(), read.acknowledgements.end(), [&](NodeId id) { return is_voter(id); }));
	}
	std::size_t log_offset(Index index) const {
		if (index <= base_index_ || index > last_index()) { throw std::out_of_range("log index outside retained suffix"); }
		return static_cast<std::size_t>(index - base_index_ - 1);
	}
	const Entry& log_entry(Index index) const { return entries_.at(log_offset(index)); }
	Term log_term(Index index) const { return index == base_index_ ? base_term_ : log_entry(index).term; }
	bool valid_entry(const Entry& entry) const {
		if (entry.index == 0 || entry.index == maximum || entry.term == 0 || entry.payload.size() > ceilings.command_bytes) { return false; }
		if (entry.kind == EntryKind::NoOp) { return entry.payload.empty(); }
		if (entry.kind == EntryKind::Configuration) {
			try {
				(void)decode_membership(entry.payload);
			} catch (const std::exception&) {
				return false;
			}
			return true;
		}
		return entry.kind == EntryKind::Command;
	}
	std::uint64_t deadline(std::uint64_t delay) const { return delay > maximum - now_ ? maximum : now_ + delay; }
	void reset_election() { election_deadline_ = deadline(election_delay_); }
	Token token() {
		if (next_token_ == maximum) { throw std::overflow_error("consensus token space exhausted; restart required"); }
		return ++next_token_;
	}
	void fence(std::string reason, Actions& output) {
		output.clear(); release_snapshots(SnapshotReleaseReason::Fenced, output);
		role_ = Role::Fenced; pending_.reset(); install_.reset(); peers_.clear(); reads_.clear();
		output.emplace_back(Fenced{std::move(reason)});
	}
	void follower(NodeId leader, Actions& deferred) {
		if (role_ != Role::Follower || leader_ != leader) { deferred.emplace_back(RoleChanged{Role::Follower, hard_.term, leader}); }
		release_snapshots(SnapshotReleaseReason::LeadershipLost, deferred);
		role_ = Role::Follower; leader_ = leader; peers_.clear(); grants_.clear(); prevote_.reset(); prevotes_.clear(); answered_.clear(); transfer_.reset();
		for (const auto& read : reads_) { deferred.emplace_back(Reject{read.request, RejectReason::NotLeader}); }
		reads_.clear(); reset_election();
	}
	void persist(StorageBatch batch, Actions deferred, Actions& output, bool elected = false) {
		if (busy()) { throw std::logic_error("overlapping persistence"); }
		auto id = token();
		pending_.emplace(Pending{id, hard_, std::move(deferred), elected, std::nullopt});
		output.emplace_back(Persist{id, std::move(batch)});
	}
	void handle(Start, Actions& output) {
		if (started_) { return; }
		started_ = true; reset_election(); deliver(output);
	}
	void handle(Tick tick, Actions& output) {
		if (tick.now < now_ || tick.election_delay < timing_.election_min || tick.election_delay > timing_.election_max) { return; }
		now_ = tick.now; election_delay_ = tick.election_delay;
		if (started_ && !busy()) { drive(output, true); }
	}
	void handle(Persisted completion, Actions& output) {
		if (install_) {
			if (install_->published || completion.token != install_->token) { return; }
			auto& operation = *install_;
			if (operation.retain_suffix) {
				note_base(operation.boundary.index);
				auto count = static_cast<std::size_t>(operation.boundary.index - base_index_);
				log_bytes_ = entries_.suffix_bytes(count);
				entries_.erase_prefix(count);
			} else {
				entries_.clear(); log_bytes_ = 0; configs_.clear();
				if (operation.membership) { base_membership_ = *operation.membership; }
			}
			base_index_ = operation.boundary.index; base_term_ = operation.boundary.term;
			hard_ = durable_ = operation.hard; operation.published = true;
			output = std::move(operation.deferred);
			output.emplace_back(ActivateInstall{operation.token, operation.prepared, operation.boundary});
			return; // Application activation still owns the exclusive busy gate.
		}
		if (!pending_ || completion.token != pending_->token) { return; }
		auto completed = std::move(*pending_); pending_.reset(); durable_ = completed.hard;
		if (completed.checkpoint) {
			auto [through, term] = *completed.checkpoint;
			note_base(through);
			auto count = static_cast<std::size_t>(through - base_index_);
			log_bytes_ = entries_.suffix_bytes(count);
			entries_.erase_prefix(count);
			base_index_ = through; base_term_ = term;
		}
		output = std::move(completed.deferred);
		if (completed.elected) { become_leader(output); }
		if (!pending_) {
			advance_commit(output);
			if (!pending_) { deliver(output); ready_reads(output); drive(output, false); }
		}
	}
	void handle(StorageFault failure, Actions& output) { fence(std::move(failure.error), output); }
	void handle(InstallPrepared request, Actions& output) {
		auto reject = [&](InstallRejectReason reason) { output.emplace_back(InstallRejected{request.request, request.prepared, reason}); };
		if (!started_ || busy()) { reject(InstallRejectReason::Busy); return; }
		if (request.request == 0 || request.prepared == 0 || request.authenticated_peer == configuration_.local || request.authenticated_peer == 0 ||
			request.cluster != configuration_.cluster || request.configuration != configuration_.configuration ||
			request.leader_term == 0 || request.boundary.index == 0 || request.boundary.index == maximum ||
			request.boundary.term == 0 || request.boundary.term > request.leader_term) { reject(InstallRejectReason::Invalid); return; }
		if (request.leader_term < hard_.term) { reject(InstallRejectReason::StaleTerm); return; }
		auto next_hard = durable_;
		if (request.leader_term > next_hard.term) { next_hard.term = request.leader_term; next_hard.voted_for.reset(); }
		if (request.boundary.index <= durable_.commit_index) {
			Actions deferred; hard_ = next_hard; follower(request.authenticated_peer, deferred); leader_contact_ = now_;
			deferred.emplace_back(InstallRejected{request.request, request.prepared, InstallRejectReason::CaughtUp});
			if (hard_ != durable_) { persist(StorageBatch{hard_, std::nullopt}, std::move(deferred), output); }
			else { output = std::move(deferred); }
			return;
		}
		bool retain = request.boundary.index <= last_index() && log_term(request.boundary.index) == request.boundary.term;
		next_hard.commit_index = request.boundary.index;
		RecoveredState state{configuration_, next_hard, request.boundary.index, request.boundary.term, {}, request.boundary.index};
		state.membership = retain_base_membership(retain ? std::optional<Membership>(membership_at(request.boundary.index)) : request.membership);
		if (retain) {
			auto first = static_cast<std::size_t>(request.boundary.index - base_index_);
			if (!capture_mode_) {
			state.entries.reserve(entries_.size() - first);
			for (std::size_t i = first; i < entries_.size(); ++i) { state.entries.push_back(entries_[i]); }
		}
		}
		Actions deferred; hard_ = next_hard; follower(request.authenticated_peer, deferred); leader_contact_ = now_;
		auto id = token(); install_.emplace(PendingInstall{id, request.prepared, request.request, request.boundary, next_hard, std::move(deferred), retain});
		install_->membership = request.membership;
		if (capture_mode_) { captured_ = FrozenCheckpoint(*this, id, state, retain ? entries_.size() - static_cast<std::size_t>(request.boundary.index - base_index_) : 0); }
		output.emplace_back(PersistInstall{id, request.prepared, std::move(state)});
	}
	void handle(InstallActivated completion, Actions& output) {
		if (!install_ || !install_->published || completion.token != install_->token) { return; }
		if (delivered_through_) { fence("snapshot activation precedes outstanding application completion", output); return; }
		auto operation = std::move(*install_); install_.reset(); applied_ = operation.boundary.index; reset_election();
		output.emplace_back(InstallCompleted{operation.request, operation.prepared, operation.boundary});
	}
	void handle(InstallActivationFailed failure, Actions& output) {
		if (install_ && install_->published && failure.token == install_->token) { fence(std::move(failure.error), output); }
	}
	void release_snapshots(SnapshotReleaseReason reason, Actions& output) {
		for (const auto& [id, peer] : peers_) { if (peer.snapshot) { output.emplace_back(SnapshotReleased{id, peer.snapshot->key, reason}); } }
	}
	void release_snapshot(NodeId id, Peer& peer, SnapshotReleaseReason reason, Actions& output, bool probe = false) {
		auto key = peer.snapshot->key; output.emplace_back(SnapshotReleased{id, key, reason}); peer.snapshot.reset();
		if (peer.flight && peer.flight->keepalive) { peer.flight.reset(); }
		peer.retry_after = deadline(timing_.rpc_timeout); peer.snapshot_retry_clock = now_;
		if (probe) { peer.next = std::max(peer.matched + 1, key.boundary.index + 1); }
	}
	void handle(SnapshotSourceReady ready, Actions& output) {
		if (busy() || role_ != Role::Leader || !valid_snapshot_key(ready.key)) { return; }
		auto found = peers_.find(ready.peer); if (found == peers_.end()) { return; } auto& peer = found->second;
		if (!peer.snapshot || peer.snapshot->key != ready.key || peer.snapshot->phase != SnapshotPhase::AwaitSource || ready.key.leader_term != durable_.term) { return; }
		if (ready.key.boundary != LogBoundary{base_index_, base_term_}) { release_snapshot(ready.peer, peer, SnapshotReleaseReason::Stale, output); return; }
		peer.snapshot->phase = SnapshotPhase::Sending; peer.snapshot->started = now_; output.emplace_back(SnapshotTransmit{ready.peer, ready.key});
	}
	void handle(SnapshotTransferFailed failure, Actions& output) {
		if (busy() || role_ != Role::Leader || !valid_snapshot_key(failure.key)) { return; }
		auto found = peers_.find(failure.peer); if (found == peers_.end() || !found->second.snapshot || found->second.snapshot->key != failure.key) { return; }
		bool probe = found->second.snapshot->phase == SnapshotPhase::Sending;
		release_snapshot(failure.peer, found->second, SnapshotReleaseReason::Unavailable, output, probe);
	}

	void handle(LocalCheckpoint request, Actions& output) {
		if (!started_ || busy()) { output.emplace_back(Reject{request.request, RejectReason::Busy}); return; }
		if (request.capture == 0 || request.epoch != epoch_ || request.cluster != configuration_.cluster || request.configuration != configuration_.configuration ||
			request.through <= base_index_ || request.through > applied_ || applied_ > durable_.commit_index ||
			(witness_self() && request.through > witness_bound_) ||
			log_term(request.through) != request.term) {
			output.emplace_back(Reject{request.request, RejectReason::InvalidCheckpoint}); return;
		}
		for (const auto& [id, peer] : peers_) {
			if (peer.snapshot && peer.snapshot->phase == SnapshotPhase::Sending && request.through > peer.snapshot->key.boundary.index) { output.emplace_back(Reject{request.request, RejectReason::Busy}); return; }
		}
		RecoveredState state{configuration_, durable_, request.through, request.term, {}, request.through};
		state.membership = retain_base_membership(membership_at(request.through));
		auto first = static_cast<std::size_t>(request.through - base_index_);
		if (!capture_mode_) {
			state.entries.reserve(entries_.size() - first);
			for (std::size_t i = first; i < entries_.size(); ++i) { state.entries.push_back(entries_[i]); }
		}
		auto id = token();
		Actions deferred; deferred.emplace_back(CheckpointPublished{request.request, request.through});
		pending_.emplace(Pending{id, durable_, std::move(deferred), false, std::pair{request.through, request.term}});
		if (capture_mode_) { captured_ = FrozenCheckpoint(*this, id, state, entries_.size() - first); }
		output.emplace_back(PersistCheckpoint{id, request.capture, std::move(state)});
	}
	void handle(Failed failure, Actions& output) {
		if (failure.source == FailureSource::Storage) {
			if (install_) { if (install_->published || failure.token != install_->token) { return; } }
			else if (!pending_ || failure.token != pending_->token) { return; }
		} else if (failure.token != delivered_through_ || delivered_through_ == 0) { return; }
		fence(std::move(failure.error), output);
	}
	void handle(Repair request, Actions& output) {
		auto reject = [&](RejectReason reason) { output.emplace_back(Reject{request.request, reason}); };
		if (!started_ || busy() || delivered_through_) { reject(RejectReason::Busy); return; }
		if (role_ == Role::Leader && voter_count(membership()) > 1) { reject(RejectReason::LeaderMustTransfer); return; }
		const auto b = request.boundary;
		if (b.index < base_index_ || b.index > durable_.commit_index || (b.index == base_index_ ? b.term != base_term_ : log_term(b.index) != b.term)) {
			reject(RejectReason::InvalidRepair);
			return;
		}
		applied_ = b.index; ++epoch_;
		output.emplace_back(RepairCompleted{request.request, b, epoch_});
		deliver(output);
	}
	void handle(Applied completion, Actions& output) {
		if (completion.through <= applied_) { return; }
		if (completion.through != delivered_through_ || completion.through > durable_.commit_index) {
			fence("invalid application completion", output); return;
		}
		applied_ = completion.through; delivered_through_ = 0;
		if (!busy()) { deliver(output); ready_reads(output); }
	}
	void handle(ChangeMembership change, Actions& output) {
		auto reject = [&](RejectReason reason) { output.emplace_back(Reject{change.request, reason}); };
		if (busy()) { reject(RejectReason::Busy); return; }
		if (!started_ || role_ != Role::Leader || witness_self()) { reject(RejectReason::NotLeader); return; }
		if (transfer_) { reject(RejectReason::Busy); return; }
		const auto& current = membership();
		// D3: (a) the leader committed an entry of its term; (b) no configuration above the
		// commit index; (c) a promoted learner has matched the commit index.
		if (durable_.commit_index == 0 || log_term(durable_.commit_index) != hard_.term ||
			(!configs_.empty() && configs_.rbegin()->first > durable_.commit_index)) { reject(RejectReason::NotReady); return; }
		try {
			validate_change(current, change.next, limits_in_force());
		} catch (const std::invalid_argument&) {
			reject(RejectReason::InvalidMembership); return;
		}
		// A group's RPC limits stay within what this node's transport carries.
		if (change.next.limits && (change.next.limits->command_bytes > limits_.rpc_bytes || change.next.limits->rpc_bytes > limits_.rpc_bytes ||
			change.next.limits->rpc_entries > limits_.rpc_entries)) { reject(RejectReason::InvalidMembership); return; }
		for (const auto& x : change.next.members) {
			auto old = find_member(current, x.incarnation);
			if (old && !voting(old->role) && voting(x.role)) {
				auto progress = this->progress(x.incarnation);
				if (!progress || *progress < durable_.commit_index) { reject(RejectReason::NotReady); return; }
			}
		}
		propose_membership(change.request, change.next, output);
	}
	void propose_membership(RequestId request, const Membership& next, Actions& output) {
		if (last_index() >= maximum - 1 || entries_.charged_entries() >= limits_in_force().log_entries) {
			if (request) { output.emplace_back(Reject{request, RejectReason::LogFull}); }
			return;
		}
		Entry entry{last_index() + 1, hard_.term, EntryKind::Configuration, encode_membership(next)};
		log_bytes_ += entry.payload.size(); entries_.push_back(entry); note_append(entry);
		for (NodeId replica : replicas()) {
			if (!peers_.contains(replica)) { peers_.emplace(replica, Peer{last_index(), 0, {}}); }
		}
		Actions deferred;
		if (request) { deferred.emplace_back(ProposalPlaced{request, hard_.term, entry.index}); }
		persist(StorageBatch{std::nullopt, LogMutation{entry.index, {std::move(entry)}}}, std::move(deferred), output);
	}
	// D6: the target must be a voter, not a witness or learner, of the membership in force.
	void handle(TransferLeadership request, Actions& output) {
		auto reject = [&](RejectReason reason) { output.emplace_back(Reject{request.request, reason}); };
		if (busy() || transfer_) { reject(RejectReason::Busy); return; }
		if (!started_ || role_ != Role::Leader) { reject(RejectReason::NotLeader); return; }
		auto target = find_member(membership(), request.target);
		if (request.target == configuration_.local || !target || target->role != MemberRole::Voter || !peers_.contains(request.target)) {
			reject(RejectReason::InvalidTarget); return;
		}
		start_transfer(request.target, output);
	}
	void start_transfer(NodeId target, Actions& output) {
		transfer_ = Transfer{target, token(), deadline(timing_.election_max)};
		auto& peer = peers_.at(target);
		if (peer.next <= last_index()) { replicate(target, peer, output, needed_probe(target)); }
		timeout_now(output);
	}
	bool witness_self() const {
		auto self = find_member(membership(), configuration_.local);
		return self && self->role == MemberRole::Witness;
	}
	// Sends TimeoutNow once the target has matched the leader's last index.
	void timeout_now(Actions& output) {
		if (!transfer_ || transfer_->sent || role_ != Role::Leader) { return; }
		auto found = peers_.find(transfer_->target);
		if (found == peers_.end() || found->second.matched != last_index()) { return; }
		transfer_->sent = true;
		output.emplace_back(Send{transfer_->target, TimeoutNow{hard_.term, last_index(), transfer_->token}});
	}
	void handle(Propose proposal, Actions& output) {
		if (busy()) { output.emplace_back(Reject{proposal.request, RejectReason::Busy}); return; }
		if (!started_ || role_ != Role::Leader) { output.emplace_back(Reject{proposal.request, RejectReason::NotLeader}); return; }
		if (witness_self()) { output.emplace_back(Reject{proposal.request, RejectReason::NotLeader}); return; }  // D9
		if (transfer_) { output.emplace_back(Reject{proposal.request, RejectReason::Busy}); return; }
		const auto& limits = limits_in_force();
		if (proposal.command.size() > limits.command_bytes) { output.emplace_back(Reject{proposal.request, RejectReason::TooLarge}); return; }
		if (last_index() - durable_.commit_index >= limits.uncommitted_entries) {
			output.emplace_back(Reject{proposal.request, RejectReason::Busy}); return;
		}
		// A lowered retained-log limit blocks proposals until checkpoints bring usage under it.
		if (last_index() >= maximum - 1 || entries_.charged_entries() >= limits.log_entries - limits.control_entries || entries_.charged_bytes() > limits.log_bytes || proposal.command.size() > limits.log_bytes - entries_.charged_bytes()) {
			output.emplace_back(Reject{proposal.request, RejectReason::LogFull}); return;
		}
		Entry entry{last_index() + 1, hard_.term, EntryKind::Command, std::move(proposal.command)};
		log_bytes_ += entry.payload.size(); entries_.push_back(entry); note_append(entry);
		Actions deferred; deferred.emplace_back(ProposalPlaced{proposal.request, hard_.term, entry.index});
		persist(StorageBatch{std::nullopt, LogMutation{entry.index, {std::move(entry)}}}, std::move(deferred), output);
	}
	void handle(Read read, Actions& output) {
		if (busy()) { output.emplace_back(Reject{read.request, RejectReason::Busy}); return; }
		if (!started_ || role_ != Role::Leader || witness_self()) { output.emplace_back(Reject{read.request, RejectReason::NotLeader}); return; }
		if (durable_.commit_index == 0 || log_term(durable_.commit_index) != hard_.term) {
			output.emplace_back(Reject{read.request, RejectReason::NotReady}); return;
		}
		if (reads_.size() >= limits_in_force().reads) { output.emplace_back(Reject{read.request, RejectReason::Busy}); return; }
		for (const auto& pending : reads_) {
			if (pending.request == read.request) { output.emplace_back(Reject{read.request, RejectReason::DuplicateRequest}); return; }
		}
		auto probe = token();
		reads_.push_back(PendingRead{read.request, probe, hard_.term, durable_.commit_index, {configuration_.local}});  // counted while a voter
		// Preserve data/probe correlation. Later reads queue behind the oldest
		// pending probe; each quorum observation must follow its admission.
		for (auto& [id, peer] : peers_) {
			auto needed = needed_probe(id);
			if (!peer.flight && needed) { replicate(id, peer, output, needed); }
		}
		ready_reads(output);
	}
	void handle(Receive receive, Actions& output) {
		// Any authenticated incarnation of the group (D5): a follower can lag the membership
		// that made its sender leader; terms and the log comparison decide.
		if (!started_ || busy() || receive.authenticated_peer == configuration_.local || receive.authenticated_peer == 0) { return; }
		if (!std::visit([&](const auto& message) { return valid(message); }, receive.message)) { return; }
		// A pre-vote request names the term its sender would campaign at: answered without
		// changing this node's term or ballot (Track C, D7).
		if (auto request = std::get_if<PreVoteRequest>(&receive.message)) {
			bool grant = request->term > hard_.term && is_voter(configuration_.local) && !heard_from_leader() && log_up_to_date(request->last_index, request->last_term);
			output.emplace_back(Send{receive.authenticated_peer, PreVoteResponse{hard_.term, request->term, grant}}); return;
		}
		// The leader-contact guard: a voter that heard from a current leader within
		// election_min ignores a vote request unless it comes from a leadership transfer.
		if (auto request = std::get_if<VoteRequest>(&receive.message); request && !request->transfer && heard_from_leader()) { return; }
		auto incoming_term = std::visit([](const auto& message) { return message.term; }, receive.message);
		bool changed = incoming_term > hard_.term;
		Actions deferred;
		if (changed) {
			hard_.term = incoming_term; hard_.voted_for.reset(); follower(0, deferred);
		}
		StorageBatch batch;
		std::visit([&](auto&& message) { receive_message(receive.authenticated_peer, std::move(message), batch, deferred); }, std::move(receive.message));
		if (changed || hard_ != durable_) { batch.hard = hard_; }
		if (batch.hard || batch.log) { persist(std::move(batch), std::move(deferred), output); }
		else {
			output = std::move(deferred); advance_commit(output);
			if (!pending_) { deliver(output); ready_reads(output); }
		}
		if (prevote_ && prevotes_.size() >= quorum() && role_ != Role::Leader && !busy()) { campaign(output); }
		if (std::exchange(timed_out_, false) && role_ == Role::Follower && !busy()) { campaign(output, true); }
	}
	bool valid(const VoteRequest& request) const { return request.term > 0 && request.last_index < maximum && request.last_term <= request.term && ((request.last_index == 0) == (request.last_term == 0)); }
	bool valid(const PreVoteRequest& request) const { return valid(VoteRequest{request.term, request.last_index, request.last_term}); }
	bool valid(const PreVoteResponse& response) const { return response.request_term > 0; }
	bool valid(const TimeoutNow& request) const { return request.term > 0 && request.token > 0 && request.last_index < maximum; }
	bool log_up_to_date(Index index, Term term) const {
		return term > log_term(last_index()) || (term == log_term(last_index()) && index >= last_index());
	}
	bool heard_from_leader() const { return role_ == Role::Follower && leader_ != 0 && now_ - leader_contact_ < timing_.election_min; }
	bool valid(const VoteResponse&) const { return true; }
	bool valid(const SnapshotResponse& response) const { return valid_snapshot_response(response); }
	bool valid(const AppendResponse& response) const {
		return response.rpc != 0 && response.matched < maximum && response.next_hint > 0 &&
			(!response.compacted || (!response.success && response.compacted->index > 0 && response.compacted->index < maximum &&
				response.compacted->term > 0 && response.compacted->term <= response.term && response.next_hint == response.compacted->index + 1));
	}
	bool valid(const AppendRequest& request) const {
		if (request.term == 0 || request.rpc == 0 || request.previous >= maximum || request.previous_term > request.term ||
			((request.previous == 0) != (request.previous_term == 0)) || request.entries.size() > ceilings.rpc_entries ||
			request.entries.size() >= maximum - request.previous) { return false; }
		Index index = request.previous; Term term = request.previous_term; std::size_t bytes = 0;
		for (const auto& entry : request.entries) {
			if (!valid_entry(entry) || entry.index != ++index || entry.term < term || entry.term > request.term ||
				entry.payload.size() > ceilings.rpc_bytes - bytes) { return false; }
			bytes += entry.payload.size(); term = entry.term;
		}
		return true;
	}
	void receive_message(NodeId source, VoteRequest request, StorageBatch&, Actions& output) {
		bool grant = request.term == hard_.term && is_voter(configuration_.local) && (!hard_.voted_for || *hard_.voted_for == source) && log_up_to_date(request.last_index, request.last_term);
		if (grant) { hard_.voted_for = source; reset_election(); }
		output.emplace_back(Send{source, VoteResponse{hard_.term, grant}});
	}
	void receive_message(NodeId, PreVoteRequest, StorageBatch&, Actions&) {}  // answered in handle(Receive)
	// D6: act once per token, only as the sender's caught-up follower in its term, and only
	// as a voter (never a witness or learner); handle(Receive) campaigns after settling.
	void receive_message(NodeId source, TimeoutNow request, StorageBatch&, Actions&) {
		auto self = find_member(membership(), configuration_.local);
		if (request.term != hard_.term || role_ != Role::Follower || leader_ != source || request.last_index != last_index() ||
			!self || self->role != MemberRole::Voter || acted_timeout_ == std::pair{source, request.token}) { return; }
		acted_timeout_ = {source, request.token}; timed_out_ = true;
	}
	void receive_message(NodeId source, PreVoteResponse response, StorageBatch&, Actions& output) {
		if (!prevote_ || response.request_term != *prevote_ || !response.granted || role_ == Role::Leader || !is_voter(source)) { return; }
		prevotes_.insert(source);  // handle(Receive) campaigns once the quorum is in and nothing persists
	}
	void receive_message(NodeId source, VoteResponse response, StorageBatch&, Actions& output) {
		if (response.term != hard_.term || role_ != Role::Candidate || !response.granted || !is_voter(source)) { return; }
		grants_.insert(source);
		if (grants_.size() >= quorum()) { become_leader(output); }
	}
	void receive_message(NodeId source, AppendRequest request, StorageBatch& batch, Actions& output) {
		if (request.term < hard_.term) {
			output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, false, 0, last_index() + 1, request.read_probe}}); return;
		}
		follower(source, output); leader_contact_ = now_;
		if (base_index_ && (request.previous < base_index_ || (request.previous == base_index_ && request.previous_term != base_term_))) {
			output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, false, 0, base_index_ + 1, request.read_probe, LogBoundary{base_index_, base_term_}}}); return;
		}
		if (request.previous > last_index() || log_term(request.previous) != request.previous_term) {
			auto hint = request.previous < base_index_ ? base_index_ + 1 : (request.previous > last_index() ? last_index() + 1 : request.previous);
			output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, false, 0, hint, request.read_probe}}); return;
		}
		for (const auto& entry : request.entries) {
			if (entry.index <= last_index() && log_entry(entry.index).term == entry.term && log_entry(entry.index) != entry) {
				throw std::runtime_error("same-index same-term content conflict");
			}
		}
		std::size_t first = 0;
		while (first < request.entries.size() && request.entries[first].index <= last_index() &&
			log_entry(request.entries[first].index) == request.entries[first]) { ++first; }
		if (first < request.entries.size()) {
			auto replace_from = request.entries[first].index;
			if (replace_from <= hard_.commit_index) {
				// Honest peers can never conflict with committed state. Fail closed.
				throw std::runtime_error("conflict with committed log prefix");
			}
			auto retained_bytes = log_bytes_ - entries_.suffix_bytes(static_cast<std::size_t>(replace_from - base_index_ - 1));
			auto additional = request.entries.size() - first;
			// R16: each entry against the limits in force at its position; retention against
			// the wider of those before and after the request's configurations.
			Limits running = limits_at(replace_from), widest = running;
			for (std::size_t i = first; i < request.entries.size(); ++i) {
				const auto& entry = request.entries[i];
				if (entry.payload.size() > running.command_bytes) {
					output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, false, 0, replace_from, request.read_probe}}); return;
				}
				if (entry.kind == EntryKind::Configuration) {
					if (auto m = decode_membership(entry.payload); m.limits) { running = *m.limits; }
					widest.log_entries = std::max(widest.log_entries, running.log_entries); widest.log_bytes = std::max(widest.log_bytes, running.log_bytes);
				}
			}
			if (replace_from - base_index_ - 1 > widest.log_entries || additional > widest.log_entries - (replace_from - base_index_ - 1)) {
				output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, false, 0, replace_from, request.read_probe}}); return;
			}
			for (std::size_t i = first; i < request.entries.size(); ++i) {
				if (request.entries[i].payload.size() > widest.log_bytes - retained_bytes) {
					output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, false, 0, replace_from, request.read_probe}}); return;
				}
				retained_bytes += request.entries[i].payload.size();
			}
			std::size_t incoming_bytes = 0;
			for (std::size_t i = first; i < request.entries.size(); ++i) { incoming_bytes += request.entries[i].payload.size(); }
			// Overflow-free charged bounds include one reserved replacement RPC.
			if (entries_.charged_entries() > widest.log_entries || additional > ceilings.rpc_entries ||
				entries_.charged_bytes() > widest.log_bytes || incoming_bytes > ceilings.rpc_bytes) {
				output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, false, 0, replace_from, request.read_probe}}); return;
			}
			std::vector<Entry> suffix(request.entries.begin() + static_cast<std::ptrdiff_t>(first), request.entries.end());
			entries_.resize(static_cast<std::size_t>(replace_from - base_index_ - 1)); note_truncate(replace_from);
			for (const auto& entry : suffix) { entries_.push_back(entry); note_append(entry); } log_bytes_ = retained_bytes;
			batch.log = LogMutation{replace_from, std::move(suffix)};
		}
		auto verified = request.previous + request.entries.size();
		witness_bound_ = std::max(witness_bound_, std::min(request.data_matched, verified));
		hard_.commit_index = std::max(hard_.commit_index, std::min(request.commit, verified));
		output.emplace_back(Send{source, AppendResponse{hard_.term, request.rpc, true, verified, verified + 1, request.read_probe}});
	}
	void receive_message(NodeId source, AppendResponse response, StorageBatch&, Actions& output) {
		if (role_ != Role::Leader || response.term != hard_.term) { return; }
		answered_[source] = now_;
		auto found = peers_.find(source);
		if (found == peers_.end()) { return; }
		auto& peer = found->second;
		if (!peer.flight || peer.flight->rpc != response.rpc) { return; }
		auto flight = *peer.flight;
		if (response.read_probe != flight.read_probe) { return; }
		// Preserve its send time across replies so fixed-time callbacks cannot
		// turn independent snapshot contact into an immediate request loop.
		if (flight.keepalive) { return; }
		if (response.success) {
			if (response.matched != flight.through) { return; }
			peer.matched = std::max(peer.matched, response.matched); peer.next = peer.matched + 1; peer.retry_after = 0;
			if (transfer_ && transfer_->target == source) { timeout_now(output); }
			if (flight.read_probe) {
				for (auto& read : reads_) { if (read.probe == flight.read_probe && read.term == hard_.term) { read.acknowledgements.insert(source); } }
			}
		} else {
			// A stale failure cannot reach this path. Durable peers cannot lose a
			// matched prefix on restart; never rewind below verified progress.
			auto previous_next = peer.next;
			if (response.compacted && response.compacted->index >= base_index_) {
				auto boundary = *response.compacted;
				if (boundary.index > last_index() || log_term(boundary.index) != boundary.term) {
					throw std::runtime_error("compacted peer boundary conflicts with leader log");
				}
				// This selects a prefix to verify in a new RPC; it is not an ACK.
				peer.next = std::max(peer.matched + 1, boundary.index + 1);
			} else {
				peer.next = std::max(peer.matched + 1, std::min(response.next_hint, peer.next > 1 ? peer.next - 1 : 1));
			}
			// Capacity rejection or an unusable hint must not form an immediate
			// request/response loop. Retry on a later monotonic tick.
			peer.retry_after = peer.next < previous_next ? 0 : deadline(timing_.rpc_timeout);
		}
		peer.flight.reset();
		auto needed = needed_probe(source);
		if (peer.next <= last_index() || needed) {
			replicate(source, peer, output, needed);
		}
	}
	void receive_message(NodeId source, SnapshotResponse response, StorageBatch&, Actions& output) {
		if (role_ != Role::Leader || response.term != hard_.term || response.key.leader_term != durable_.term) { return; }
		answered_[source] = now_;
		auto found = peers_.find(source);
		if (found == peers_.end()) { return; }
		auto& peer = found->second;
		if (!peer.snapshot || peer.snapshot->phase != SnapshotPhase::Sending || peer.snapshot->key != response.key) { return; }
		if (response.result == SnapshotReply::Installed) {
			auto boundary = peer.snapshot->key.boundary;
			output.emplace_back(SnapshotReleased{source, response.key, SnapshotReleaseReason::Installed}); peer.snapshot.reset();
			if (peer.flight && peer.flight->keepalive) { peer.flight.reset(); }
			peer.matched = std::max(peer.matched, boundary.index); peer.next = peer.matched + 1; peer.retry_after = 0; peer.snapshot_retry_clock.reset();
			replicate(source, peer, output, needed_probe(source));
		} else { release_snapshot(source, peer, response.result == SnapshotReply::CaughtUp ? SnapshotReleaseReason::CaughtUp : SnapshotReleaseReason::Rejected, output, response.result == SnapshotReply::CaughtUp); }
	}

	void drive(Actions& output, bool allow_campaign) {
		if (!started_ || busy()) { return; }
		if (role_ != Role::Leader) {
			// Campaigns require a separately admitted Tick. Slow persistence
			// must not start another election from its completion callback.
			if (!allow_campaign || now_ < election_deadline_ || hard_.term == maximum) { return; }
			if (!is_voter(configuration_.local)) { reset_election(); return; }  // learners and joiners never campaign
			if (quorum() == 1) { campaign(output); return; }
			// Pre-vote first (Track C, D7): only a quorum of grants starts a campaign, so a
			// node that cannot win never raises the group's term.
			prevote_ = hard_.term + 1; prevotes_ = {configuration_.local}; reset_election();
			for (NodeId voter : voters()) {
				if (voter != configuration_.local) { output.emplace_back(Send{voter, PreVoteRequest{*prevote_, last_index(), log_term(last_index())}}); }
			}
			return;
		}
		// CheckQuorum (Track C, D7): a leader whose voters have not answered within an
		// election timeout steps down.
		std::size_t answering = is_voter(configuration_.local);
		for (const auto& [id, at] : answered_) { answering += is_voter(id) && now_ - at < timing_.election_max; }
		if (answering < quorum()) { follower(0, output); return; }
		// D6: a transfer that has not ended in one election timeout aborts; proposals resume.
		if (transfer_ && now_ >= transfer_->until) { transfer_.reset(); }
		// D9: a witness that leads hands leadership to its most caught-up data voter.
		if (!transfer_ && witness_self()) {
			NodeId best = 0; Index matched = 0;
			for (const auto& [id, peer] : peers_) {
				auto member = find_member(membership(), id);
				if (member && member->role == MemberRole::Voter && (!best || peer.matched > matched)) { best = id; matched = peer.matched; }
			}
			if (best) { start_transfer(best, output); }
		}
		// D4: a leader whose own removal committed steps down, transfer or not. D3: a
		// committed replacement's leaving voter is removed next, by whichever leader holds
		// the committed membership, so the replacement completes across leader changes.
		const auto& committed = committed_membership();
		if (!votes_in(committed, configuration_.local) && committed == membership()) { follower(0, output); return; }
		if (committed.leaving && committed == membership() && log_term(durable_.commit_index) == hard_.term) {
			Membership next = committed;
			std::erase_if(next.members, [&](const Member& x) { return x.incarnation == committed.leaving; });
			next.leaving = 0;
			propose_membership(0, next, output);
			if (busy()) { return; }
		}
		bool heartbeat = now_ >= heartbeat_deadline_;
		for (auto& [id, peer] : peers_) {
			if (peer.snapshot && peer.snapshot->phase == SnapshotPhase::AwaitSource && peer.snapshot->key.boundary != LogBoundary{base_index_, base_term_}) { release_snapshot(id, peer, SnapshotReleaseReason::Stale, output); }
			if (peer.snapshot && now_ - peer.snapshot->started >= timing_.snapshot_timeout) { release_snapshot(id, peer, SnapshotReleaseReason::TimedOut, output, true); }
			if (peer.flight && !peer.flight->keepalive && now_ - peer.flight->sent >= timing_.rpc_timeout) { peer.flight.reset(); }
			if (peer.snapshot || (!peer.flight && (heartbeat || peer.next <= last_index()))) {
				auto probe = needed_probe(id);
				replicate(id, peer, output, probe);
			}
		}
		if (heartbeat) { heartbeat_deadline_ = deadline(timing_.heartbeat); }
	}
	void campaign(Actions& output, bool transfer = false) {
		++hard_.term; hard_.voted_for = configuration_.local; prevote_.reset(); prevotes_.clear();
		role_ = Role::Candidate; leader_ = 0; grants_ = {configuration_.local}; reset_election();
		Actions deferred; deferred.emplace_back(RoleChanged{role_, hard_.term, 0});
		for (NodeId voter : voters()) {
			if (voter != configuration_.local) { deferred.emplace_back(Send{voter, VoteRequest{hard_.term, last_index(), log_term(last_index()), transfer}}); }
		}
		persist(StorageBatch{hard_, std::nullopt}, std::move(deferred), output, quorum() == 1);
	}
	void become_leader(Actions& output) {
		if (busy()) { throw std::logic_error("leader election before vote persistence"); }
		if (last_index() >= maximum - 1 || entries_.charged_entries() >= limits_in_force().log_entries) {
			follower(0, output); return; // Control reserve exhausted: fail closed.
		}
		role_ = Role::Leader; leader_ = configuration_.local; peers_.clear(); grants_.clear(); answered_.clear(); transfer_.reset();
		for (NodeId replica : replicas()) {
			peers_.emplace(replica, Peer{last_index() + 1, 0, {}});
			if (is_voter(replica)) { answered_[replica] = now_; }
		}
		Entry noop{last_index() + 1, hard_.term, EntryKind::NoOp, {}}; entries_.push_back(noop);
		Actions deferred; deferred.emplace_back(RoleChanged{role_, hard_.term, leader_});
		persist(StorageBatch{std::nullopt, LogMutation{noop.index, {noop}}}, std::move(deferred), output);
		heartbeat_deadline_ = now_;
	}
	void snapshot_keepalive(NodeId id, Peer& peer, Actions& output) {
		if (peer.flight && (!peer.flight->keepalive || now_ - peer.flight->sent < timing_.heartbeat)) { return; }
		auto rpc = token(); peer.flight = Flight{rpc, 0, 0, 0, now_, true};
		output.emplace_back(Send{id, AppendRequest{hard_.term, rpc, 0, 0, 0, 0, {}}});
	}

	void replicate(NodeId id, Peer& peer, Actions& output, Token probe) {
		if (peer.snapshot) { snapshot_keepalive(id, peer, output); return; }
		if (peer.flight || now_ < peer.retry_after || (peer.snapshot_retry_clock && now_ <= *peer.snapshot_retry_clock)) { return; }
		if (peer.next <= base_index_) {
			SnapshotKey key{durable_.term, token(), {base_index_, base_term_}};
			peer.snapshot.emplace(SnapshotFlight{key, SnapshotPhase::AwaitSource, now_}); peer.snapshot_retry_clock.reset();
			output.emplace_back(SnapshotNeeded{id, key}); snapshot_keepalive(id, peer, output); return;
		}
		Index previous = peer.next - 1;
		std::vector<Entry> batch; std::size_t bytes = 0;
		const auto rpc_entries = std::min(limits_in_force().rpc_entries, limits_.rpc_entries), rpc_bytes = std::min(limits_in_force().rpc_bytes, limits_.rpc_bytes);
		for (Index index = peer.next; index <= last_index() && batch.size() < rpc_entries; ++index) {
			const auto& entry = log_entry(index);
			if (entry.payload.size() > rpc_bytes - bytes) { break; }
			bytes += entry.payload.size(); batch.push_back(entry);
		}
		auto rpc = token();
		peer.flight = Flight{rpc, previous, previous + batch.size(), probe, now_};
		output.emplace_back(Send{id, AppendRequest{hard_.term, rpc, previous, log_term(previous), durable_.commit_index, probe, std::move(batch), data_matched()}});
	}
	// D9: the least index every data voter of the membership in force has matched.
	Index data_matched() const {
		Index least = maximum;
		for (const auto& x : membership().members) {
			if (x.role != MemberRole::Voter) { continue; }
			if (x.incarnation == configuration_.local) { least = std::min(least, last_index()); continue; }
			auto found = peers_.find(x.incarnation);
			least = std::min(least, found == peers_.end() ? Index{0} : found->second.matched);
		}
		return least == maximum ? 0 : least;
	}
	void advance_commit(Actions& output) {
		if (busy() || role_ != Role::Leader) { return; }
		// Voters and witnesses of the membership in force count; the leader itself only while
		// it is one (D4), learners never.
		std::vector<Index> matches;
		if (is_voter(configuration_.local)) { matches.push_back(last_index()); }
		for (const auto& [id, peer] : peers_) {
			if (is_voter(id)) { matches.push_back(peer.matched); }
		}
		if (matches.size() < quorum()) { return; }
		std::sort(matches.begin(), matches.end(), std::greater<Index>());
		auto candidate = matches[quorum() - 1];
		if (candidate <= durable_.commit_index || log_term(candidate) != hard_.term) { return; }
		hard_.commit_index = candidate;
		persist(StorageBatch{hard_, std::nullopt}, {}, output);
	}
	void deliver(Actions& output) {
		if (busy() || delivered_through_ || applied_ >= durable_.commit_index) { return; }
		std::vector<Entry> batch; std::size_t bytes = 0;
		for (Index index = applied_ + 1; index <= durable_.commit_index && batch.size() < limits_.rpc_entries; ++index) {
			const auto& entry = log_entry(index);
			if (!batch.empty() && entry.payload.size() > limits_.rpc_bytes - bytes) { break; }  // one entry always fits
			bytes += entry.payload.size(); batch.push_back(entry);
		}
		delivered_through_ = batch.back().index;
		output.emplace_back(Committed{applied_ + 1, std::move(batch)});
	}
	void ready_reads(Actions& output) {
		if (busy() || role_ != Role::Leader) { return; }
		for (auto read = reads_.begin(); read != reads_.end();) {
			if (read->term == hard_.term && acknowledged(*read) >= quorum() && applied_ >= read->index) {
				output.emplace_back(ReadReady{read->request, read->index}); read = reads_.erase(read);
			} else { ++read; }
		}
	}

	FixedConfiguration configuration_;
	Membership base_membership_;           // in force at base_index
	std::map<Index, Membership> configs_;  // the Configuration entries in the retained log
	HardState hard_, durable_;
	detail::RetainedLog entries_;
	Index base_index_ = 0;
	Term base_term_ = 0;
	Limits limits_;
	Timing timing_;
	Role role_ = Role::Follower;
	NodeId leader_ = 0;
	std::size_t log_bytes_ = 0;
	std::uint64_t now_ = 0, election_delay_, election_deadline_ = 0, heartbeat_deadline_ = 0;
	Token next_token_ = 0;
	Index applied_ = 0, delivered_through_ = 0;
	std::uint64_t epoch_ = 0;
	bool started_ = false, capture_mode_ = false, retiring_ = false;
	std::optional<FrozenCheckpoint> captured_;
	std::shared_ptr<CaptureOwner> capture_owner_ = std::make_shared<CaptureOwner>(CaptureOwner{this});
	std::optional<Pending> pending_;
	std::optional<PendingInstall> install_;
	std::map<NodeId, Peer> peers_;
	std::set<NodeId> grants_;
	std::optional<Term> prevote_;  // the term a pre-vote in progress asks for
	std::set<NodeId> prevotes_;
	std::map<NodeId, std::uint64_t> answered_;  // a leader's last answer from each voter
	std::uint64_t leader_contact_ = 0;
	std::vector<PendingRead> reads_;
	std::optional<Transfer> transfer_;  // a leader's transfer in progress (D6)
	std::pair<NodeId, Token> acted_timeout_{};  // the last TimeoutNow this node acted on
	bool timed_out_ = false;
	Index witness_bound_ = 0;  // a witness truncates only through this (D9)
};

} // namespace kronuz::consensus
