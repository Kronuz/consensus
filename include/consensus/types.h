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

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <limits>
#include <string>
#include <variant>
#include <vector>

namespace kronuz::consensus {

using NodeId = std::uint64_t;
using Term = std::uint64_t;
using Index = std::uint64_t;
using Token = std::uint64_t;
using RequestId = std::uint64_t;
using Identity = std::array<char, 16>;

struct FixedConfiguration {
	Identity cluster{}, configuration{};
	NodeId local = 0;
	std::vector<NodeId> voters;
	bool operator==(const FixedConfiguration&) const = default;
};
struct HardState {
	Term term = 0;
	std::optional<NodeId> voted_for;
	Index commit_index = 0;
	bool operator==(const HardState&) const = default;
};
struct Limits {
	std::size_t voters = 7;
	std::size_t command_bytes = 1024 * 1024;
	std::size_t log_bytes = 64 * 1024 * 1024;
	std::size_t log_entries = 65536;
	std::size_t control_entries = 16;
	std::size_t rpc_bytes = 2 * 1024 * 1024;
	std::size_t rpc_entries = 256;
	std::size_t reads = 64;
	std::size_t uncommitted_entries = 1024;
	std::size_t members = 64;
	bool operator==(const Limits&) const = default;
};
// Configuration entries carry an encoded Membership (membership.h; Track C, D1).
enum class EntryKind : std::uint8_t { Command, NoOp, Configuration };
// A member of a group (Track C, D1, D5): a per-group incarnation id, never reused, on a
// node identity, in a role. Learners replicate and never vote; a witness votes and keeps
// the log but no application state.
enum class MemberRole : std::uint8_t { Voter, Learner, Witness };
struct Member {
	NodeId incarnation = 0;
	NodeId node = 0;
	MemberRole role = MemberRole::Voter;
	bool operator==(const Member&) const = default;
};
struct Membership {
	std::vector<Member> members;  // sorted by incarnation
	NodeId next_incarnation = 1;  // above every incarnation the group has used
	NodeId leaving = 0;           // a voter a replacement promoted beside; removed next
	// The group's limits from this entry on (R16); absent, those before it, else the node's.
	std::optional<Limits> limits{};
	bool operator==(const Membership&) const = default;
};
struct Entry {
	Index index = 0;
	Term term = 0;
	EntryKind kind = EntryKind::Command;
	std::string payload;
	bool operator==(const Entry&) const = default;
};
struct RecoveredState {
	FixedConfiguration configuration;
	HardState hard;
	Index base_index = 0;
	Term base_term = 0;
	std::vector<Entry> entries;
	Index applied_index = 0;
	// The membership in force at base_index; absent, the fixed voters, each its own node.
	std::optional<Membership> membership{};
};
struct LogMutation { Index replace_from; std::vector<Entry> entries; };
struct StorageBatch {
	std::optional<HardState> hard;
	std::optional<LogMutation> log;
};
// Milliseconds. The default is Track C's default profile (R4): the slowest platform
// measured, macOS x86_64 APFS at a barrier p99 of 80 ms. A host on faster storage derives
// its own by R4's rules.
struct Timing {
	std::uint64_t heartbeat = 100, rpc_timeout = 500;
	std::uint64_t election_min = 1000, election_max = 2000;
	std::uint64_t snapshot_timeout = 60000;
	// Quiescence (docs/mechanisms/quiescence.md): a group at rest stops its timers, and the
	// host's node-pair detector raises LeaderGone in their place.
	bool quiesce = false;
};

// Pipelined replication (Track C, D15): Append RPCs and bytes in flight per peer. One
// flight by default, today's behavior; a membership group's host raises it (D15: 4).
// frame_bytes caps an append's payload below rpc_bytes, or one entry when larger, so a
// control frame waits behind at most one such frame (R19: 256 KiB); 0 leaves rpc_bytes.
struct Flow {
	std::uint32_t pipeline = 1;
	std::uint64_t window_bytes = 4 << 20;
	std::size_t frame_bytes = 0;
};

// transfer: sent on TimeoutNow (Track C, D6); it bypasses the leader-contact guard.
struct VoteRequest { Term term; Index last_index; Term last_term; bool transfer = false; };
struct VoteResponse { Term term; bool granted; };
// Pre-vote (Track C, D7): `term` is the term the candidate would campaign at; neither side
// changes its term or ballot. A response carries the responder's own term.
struct PreVoteRequest { Term term; Index last_index; Term last_term; };
struct PreVoteResponse { Term term; Term request_term; bool granted; };
// A transferring leader tells a caught-up voter to campaign now (Track C, D6).
struct TimeoutNow { Term term; Index last_index; Token token; };
// Quiescence (docs/mechanisms/quiescence.md): the sender's replica of the group stopped in
// `term` (its Driver reports a fence); a follower of it drops it as on LeaderGone.
struct Gone { Term term; };
struct AppendRequest {
	Term term;
	Token rpc;
	Index previous;
	Term previous_term;
	Index commit;
	Token read_probe;
	std::vector<Entry> entries;
	Index data_matched = 0;  // what every data voter has matched, for a witness's truncation (D9)
};
struct LogBoundary { Index index; Term term; bool operator==(const LogBoundary&) const = default; };
struct SnapshotKey { Term leader_term; Token transfer; LogBoundary boundary; bool operator==(const SnapshotKey&) const = default; };
enum class SnapshotReply { Installed, CaughtUp, Rejected };
struct SnapshotResponse { Term term; SnapshotKey key; SnapshotReply result; };
inline bool valid_snapshot_key(const SnapshotKey& key) {
	return key.leader_term && key.transfer && key.boundary.index && key.boundary.index < std::numeric_limits<Index>::max() && key.boundary.term && key.boundary.term <= key.leader_term;
}
inline bool valid_snapshot_response(const SnapshotResponse& response) {
	if (!response.term || !valid_snapshot_key(response.key)) { return false; }
	switch (response.result) {
	case SnapshotReply::Installed: case SnapshotReply::CaughtUp: return response.term >= response.key.leader_term;
	case SnapshotReply::Rejected: return true;
	} return false;
}
struct AppendResponse {
	Term term;
	Token rpc;
	bool success;
	Index matched;
	Index next_hint;
	Token read_probe;
	std::optional<LogBoundary> compacted{};
};
// An install request (docs/mechanisms/install-request.md): a follower whose application state
// is lost asks its leader in `term` for the leader's image; `base` is the requester's own base
// boundary, which the leader compares with its own. The follower resends it each heartbeat
// until answered; `request` correlates the answer.
struct InstallRequest { Term term; Token request; LogBoundary base; };
// Started: the leader ships the requester its image through the snapshot channel. InFlight: an
// image to the requester is already under way. Current: the leader ships at most one image per
// requester per leader base, so it ships none when it holds no image, its base is below the
// requester's, or it already shipped the requester one at its current base; it keeps one entry
// per member, dropped at a leader change. NotLeader: the responder does not lead `term`;
// `leader` names the leader it knows, 0 for none. NotMember: the requester is no data member
// (a witness or a removed incarnation).
enum class InstallRequestReply : std::uint8_t { Started, InFlight, Current, NotLeader, NotMember };
struct InstallRequestResponse { Term term; Token request; InstallRequestReply result; NodeId leader = 0; };
// A follower's read forwarded to its leader in `term` (docs/mechanisms/follower-read.md):
// `start` names the follower's Core start (Start::nonce), `token` the read within it.
struct ReadIndexRequest { Term term; Token start; Token token; };
// Ready: `index` is the leader's commit index at the request's arrival, confirmed by a quorum
// that answered a probe sent after it. Otherwise the leader refused the read, as a local Read.
enum class ReadIndexReply : std::uint8_t { Ready, NotReady, Busy, NotLeader };
struct ReadIndexResponse { Term term; Token start; Token token; ReadIndexReply result; Index index = 0; };
using Message = std::variant<VoteRequest, VoteResponse, AppendRequest, AppendResponse, SnapshotResponse, PreVoteRequest, PreVoteResponse, TimeoutNow, Gone, InstallRequest, InstallRequestResponse, ReadIndexRequest, ReadIndexResponse>;

// nonce: a random value the host draws at each start, never 0 (the Driver does); it keeps a
// restarted follower from matching a read answered for its previous start. 0 leaves a
// follower's Read refused NotLeader.
struct Start { Token nonce = 0; };
// The host supplies absolute monotonic time and a fresh randomized election
// delay within Timing's bounds. Deliver a Tick before completions after I/O.
struct Tick { std::uint64_t now; std::uint64_t election_delay; };
// `more`: further Append requests from the same peer, each starting where the one before
// ended, received while a batch persisted (D15); one storage batch takes them all, and each
// is answered with its own token once it is durable.
struct Receive { NodeId authenticated_peer; Message message; std::vector<AppendRequest> more{}; };
struct Propose { RequestId request; std::string command; };
// At a leader, ReadReady once a quorum confirms its leadership and the index is applied; at a
// follower, forwarded to its leader (ReadIndexRequest) and ReadReady once its answer is applied.
struct Read { RequestId request; };
struct Persisted { Token token; };
struct Applied { Index through; };
// The host owns the immutable application capture identified by capture.
// epoch: the application epoch the capture was taken under; Repair advances it.
struct LocalCheckpoint { RequestId request; Token capture; Index through; Term term; Identity cluster, configuration; std::uint64_t epoch = 0; };
// Trusted adapter: prepared identifies a staged, semantically validated image.
struct InstallPrepared {
	RequestId request; Token prepared; NodeId authenticated_peer; Term leader_term;
	Identity cluster, configuration; LogBoundary boundary;
	std::optional<Membership> membership{};  // the image's base membership; absent, the fixed voters
};
struct InstallActivated { Token token; };
struct InstallActivationFailed { Token token; std::string error; };
struct SnapshotSourceReady { NodeId peer; SnapshotKey key; };
struct SnapshotTransferFailed { NodeId peer; SnapshotKey key; };
struct StorageFault { std::string error; };
enum class FailureSource { Storage, Application };
struct Failed { FailureSource source; Token token; std::string error; };
// The host restored its application image at `boundary` (base_index <= index <= commit),
// verified whole: from a local checkpoint, or from a member's image when its own state
// stalled behind. The core moves its applied cursor there and delivers the committed
// entries after it from its own log. Hard state, role, leader, and commit index stay as
// they are. A host reports damage this way, never as Failed{Application}, which fences.
struct Repair { RequestId request; LogBoundary boundary; };
// The host's application state is lost below what Repair can rewind to (docs/mechanisms/
// install-request.md): the follower asks its leader for an install, accepts the image even at
// or below its commit index while it retains the boundary's entry, and answers with
// RepairCompleted once the image is activated, its epoch advanced.
struct RequestInstall { RequestId request; };
// A leader proposes the membership that follows its current one (Track C, D3): at most one
// voter or witness added or removed, any learners; a learner's promotion beside a voter
// it replaces names that voter as `leaving`, and the core removes it once that commits.
struct ChangeMembership { RequestId request; Membership next; };
// A leader hands leadership to a voter of its membership in force (D6). Proposals are
// refused until the target wins or one election timeout passes.
struct TransferLeadership { RequestId request; NodeId target; };
// The host reports a member's node limits, learned when its node connects (R16). A leader
// keeps commands and RPCs within every reported member's; one not reported is taken to
// match the leader's own node.
struct PeerLimits { NodeId incarnation; Limits limits; };
// Quiescence: the host ends a group's rest (a request or a frame is due for it), or reports
// that a node (Member::node) stopped answering on its connection, for every group it hosts.
struct Wake {};
struct LeaderGone { NodeId node; };
using Event = std::variant<Start, Tick, Receive, Propose, Read, Persisted, Applied, Failed, LocalCheckpoint, StorageFault, InstallPrepared, InstallActivated, InstallActivationFailed, SnapshotSourceReady, SnapshotTransferFailed, Repair, ChangeMembership, TransferLeadership, PeerLimits, Wake, LeaderGone, RequestInstall>;

enum class Role : std::uint8_t { Follower, Candidate, Leader, Fenced };
// Current: the leader refused an install request: one image per requester per leader base.
enum class RejectReason { Busy, NotLeader, NotReady, LogFull, TooLarge, DuplicateRequest, InvalidCheckpoint, InvalidRepair, LeaderMustTransfer, InvalidMembership, InvalidTarget, Current };
struct Persist { Token token; StorageBatch batch; };
struct PersistCheckpoint { Token token; Token capture; RecoveredState state; };
struct PersistInstall { Token token, prepared; RecoveredState state; };
struct ActivateInstall { Token token, prepared; LogBoundary boundary; };
enum class InstallRejectReason { Busy, Invalid, StaleTerm, CaughtUp };
struct InstallRejected { RequestId request; Token prepared; InstallRejectReason reason; };
struct InstallCompleted { RequestId request; Token prepared; LogBoundary boundary; };
struct CheckpointPublished { RequestId request; Index through; };
struct SnapshotNeeded { NodeId peer; SnapshotKey key; };
struct SnapshotTransmit { NodeId peer; SnapshotKey key; };
enum class SnapshotReleaseReason { Installed, CaughtUp, Rejected, Unavailable, Stale, TimedOut, LeadershipLost, Fenced, Removed };
struct SnapshotReleased { NodeId peer; SnapshotKey key; SnapshotReleaseReason reason; };
struct Send { NodeId peer; Message message; };
struct ProposalPlaced { RequestId request; Term term; Index index; };
struct Committed { Index first; std::vector<Entry> entries; };
struct ReadReady { RequestId request; Index index; };
struct RoleChanged { Role role; Term term; NodeId leader; };
struct Reject { RequestId request; RejectReason reason; };
struct RepairCompleted { RequestId request; LogBoundary boundary; std::uint64_t epoch; };
struct Fenced { std::string reason; };
using Action = std::variant<Persist, Send, ProposalPlaced, Committed, ReadReady, RoleChanged, Reject, Fenced, PersistCheckpoint, CheckpointPublished, SnapshotNeeded, PersistInstall, ActivateInstall, InstallRejected, InstallCompleted, SnapshotTransmit, SnapshotReleased, RepairCompleted>;
using Actions = std::vector<Action>;

} // namespace kronuz::consensus
