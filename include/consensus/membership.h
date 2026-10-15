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

// A group's membership (strata Track C, D1, D3): its encoding in Configuration entries,
// its validity, and the single-step rule between one membership and the next.
#pragma once

#include "types.h"
#include "journal/codec.h"
#include <algorithm>
#include <string>
#include <string_view>

namespace kronuz::consensus {

// Node-level ceilings no group limit may exceed (R16). Decoders bound their input by
// these, never by a group's limits, so a follower under old limits decodes a raise.
inline constexpr Limits ceilings{31, std::size_t{64} << 20, std::size_t{16} << 30, std::size_t{8} << 20, 4096,
	std::size_t{256} << 20, 65536, 65536, std::size_t{8} << 20, 255};

// A Configuration entry's largest encoding with `members` members, with limits.
// Configuration entries are bounded by this, never by command_bytes, so lowering it cannot
// wedge a group; an RPC limit below it could, so validate_limits refuses one.
inline constexpr std::size_t membership_bytes(std::size_t members) { return 4 + 8 + 8 + 24 * members + 8 + 8 + 80; }
inline constexpr std::size_t maximum_membership_bytes = membership_bytes(ceilings.members);

// Throws std::invalid_argument when a group's limits are inconsistent or above the ceilings.
inline void validate_limits(const Limits& l) {
	if (l.voters == 0 || l.command_bytes == 0 || l.rpc_entries == 0 || l.log_entries == 0 || l.reads == 0 || l.uncommitted_entries == 0 ||
		l.members == 0 || l.control_entries == 0 || l.control_entries > l.log_entries || l.command_bytes > l.rpc_bytes || l.rpc_bytes > l.log_bytes ||
		l.voters > ceilings.voters || l.command_bytes > ceilings.command_bytes || l.log_bytes > ceilings.log_bytes ||
		l.log_entries > ceilings.log_entries || l.control_entries > ceilings.control_entries || l.rpc_bytes > ceilings.rpc_bytes ||
		l.rpc_entries > ceilings.rpc_entries || l.reads > ceilings.reads || l.uncommitted_entries > ceilings.uncommitted_entries ||
		l.members > ceilings.members || l.rpc_bytes < membership_bytes(l.members)) {
		throw std::invalid_argument("membership limits");
	}
}

// Whether a group's limits stay within a node's, field by field (R16).
inline bool within(const Limits& group, const Limits& node) noexcept {
	return group.voters <= node.voters && group.command_bytes <= node.command_bytes && group.log_bytes <= node.log_bytes &&
		group.log_entries <= node.log_entries && group.control_entries <= node.control_entries && group.rpc_bytes <= node.rpc_bytes &&
		group.rpc_entries <= node.rpc_entries && group.reads <= node.reads && group.uncommitted_entries <= node.uncommitted_entries &&
		group.members <= node.members;
}

inline bool voting(MemberRole role) noexcept { return role == MemberRole::Voter || role == MemberRole::Witness; }

// The founding membership of a fixed configuration: every voter its own node and incarnation.
inline Membership fixed_membership(const FixedConfiguration& configuration) {
	Membership m;
	for (NodeId id : configuration.voters) { m.members.push_back({id, id, MemberRole::Voter}); }
	std::sort(m.members.begin(), m.members.end(), [](const Member& a, const Member& b) { return a.incarnation < b.incarnation; });
	for (const auto& member : m.members) { m.next_incarnation = std::max(m.next_incarnation, member.incarnation + 1); }
	return m;
}

inline const Member* find_member(const Membership& m, NodeId incarnation) {
	auto it = std::lower_bound(m.members.begin(), m.members.end(), incarnation, [](const Member& a, NodeId id) { return a.incarnation < id; });
	return it != m.members.end() && it->incarnation == incarnation ? &*it : nullptr;
}
inline bool votes_in(const Membership& m, NodeId incarnation) {
	auto member = find_member(m, incarnation);
	return member && voting(member->role);
}
inline std::size_t voter_count(const Membership& m) {
	return std::size_t(std::count_if(m.members.begin(), m.members.end(), [](const Member& x) { return voting(x.role); }));
}

// Throws std::invalid_argument when the membership cannot be in force.
// limits: those in force when the membership carries none of its own (R16).
inline void validate_membership(const Membership& m, const Limits& in_force) {
	const Limits& limits = m.limits ? *m.limits : in_force;
	if (m.members.empty() || m.members.size() > limits.members) { throw std::invalid_argument("membership size"); }
	NodeId previous = 0;
	std::size_t witnesses = 0;
	for (const auto& x : m.members) {
		if (x.incarnation <= previous || x.incarnation >= m.next_incarnation || x.node == 0 ||
			static_cast<unsigned>(x.role) > static_cast<unsigned>(MemberRole::Witness)) {
			throw std::invalid_argument("membership member");
		}
		previous = x.incarnation;
		witnesses += x.role == MemberRole::Witness;
	}
	const auto voters = voter_count(m);
	if (voters == 0 || voters > limits.voters || voters > 31) { throw std::invalid_argument("membership voters"); }
	if (m.leaving && !votes_in(m, m.leaving)) { throw std::invalid_argument("membership leaving"); }
	if (m.limits) { validate_limits(*m.limits); }
	// D9: a witness only in rf = 2 + witness; inside a replacement's window, 3 + 1 while a
	// data voter leaves, or 2 + 2 while the witness does.
	if (witnesses) {
		const auto data = voters - witnesses;
		const bool steady = data == 2 && witnesses == 1;
		const bool window = m.leaving && ((data == 3 && witnesses == 1) || (data == 2 && witnesses == 2));
		if (!steady && !window) { throw std::invalid_argument("membership witnesses"); }
	}
}

// The single-step rule (D3): at most one voter or witness added or removed, any learners;
// no incarnation reused, none moved to another node, and a new member only as a learner.
// The leader checks a promoted learner's progress itself.
inline void validate_change(const Membership& current, const Membership& next, const Limits& limits) {
	validate_membership(next, limits);
	if (next.next_incarnation < current.next_incarnation) { throw std::invalid_argument("membership incarnation counter"); }
	std::size_t changed = 0;
	for (const auto& x : next.members) {
		auto old = find_member(current, x.incarnation);
		if (!old) {
			if (x.incarnation < current.next_incarnation) { throw std::invalid_argument("membership reuses an incarnation"); }
			// A new member joins as a learner, so it votes only once it has caught up (D3(c)).
			if (voting(x.role)) { throw std::invalid_argument("a new member joins as a learner"); }
		} else {
			if (old->node != x.node) { throw std::invalid_argument("membership moves an incarnation"); }
			changed += voting(old->role) != voting(x.role);
			if (voting(old->role) && voting(x.role) && old->role != x.role) { changed += 2; }  // voter <-> witness
		}
	}
	for (const auto& x : current.members) { changed += !find_member(next, x.incarnation) && voting(x.role); }
	if (changed > 1) { throw std::invalid_argument("membership changes more than one voter"); }
	if (next.leaving && (next.leaving != current.leaving && changed != 1)) { throw std::invalid_argument("membership leaving without a promotion"); }
}

inline std::string encode_membership(const Membership& m) {
	using kronuz::journal::put64;
	std::string bytes("RFTM", 4);
	put64(bytes, m.limits ? 2 : 1);  // version 2 carries limits (R16)
	put64(bytes, m.members.size());
	for (const auto& x : m.members) {
		put64(bytes, x.incarnation);
		put64(bytes, x.node);
		put64(bytes, static_cast<unsigned>(x.role));
	}
	put64(bytes, m.next_incarnation);
	put64(bytes, m.leaving);
	if (m.limits) {
		const auto& l = *m.limits;
		for (auto v : {l.voters, l.command_bytes, l.log_bytes, l.log_entries, l.control_entries, l.rpc_bytes, l.rpc_entries, l.reads, l.uncommitted_entries, l.members}) { put64(bytes, v); }
	}
	return bytes;
}
// Structural: bounded by the ceilings; the core checks a membership against the limits in force.
inline Membership decode_membership(std::string_view bytes) {
	using kronuz::journal::Corruption;
	using kronuz::journal::get64;
	if (!bytes.starts_with("RFTM")) { throw Corruption("membership magic"); }
	bytes.remove_prefix(4);
	const auto version = get64(bytes);
	if (version != 1 && version != 2) { throw Corruption("membership version"); }
	const auto count = get64(bytes);
	if (count > ceilings.members || count > bytes.size() / 24) { throw Corruption("membership count"); }
	Membership m;
	for (std::uint64_t i = 0; i < count; ++i) {
		const auto incarnation = get64(bytes), node = get64(bytes), role = get64(bytes);
		if (role > static_cast<unsigned>(MemberRole::Witness)) { throw Corruption("membership role"); }
		m.members.push_back({incarnation, node, static_cast<MemberRole>(role)});
	}
	m.next_incarnation = get64(bytes);
	m.leaving = get64(bytes);
	if (version == 2) {
		Limits l;
		for (auto* v : {&l.voters, &l.command_bytes, &l.log_bytes, &l.log_entries, &l.control_entries, &l.rpc_bytes, &l.rpc_entries, &l.reads, &l.uncommitted_entries, &l.members}) {
			const auto value = get64(bytes);
			if (value > std::numeric_limits<std::size_t>::max()) { throw Corruption("membership limit"); }
			*v = static_cast<std::size_t>(value);
		}
		m.limits = l;
	}
	if (!bytes.empty()) { throw Corruption("trailing membership bytes"); }
	try {
		validate_membership(m, ceilings);
	} catch (const std::invalid_argument& error) {
		throw Corruption(error.what());
	}
	return m;
}

}  // namespace kronuz::consensus
