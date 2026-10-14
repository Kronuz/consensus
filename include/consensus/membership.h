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
inline void validate_membership(const Membership& m, const Limits& limits) {
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
	// Outside a replacement's window a group has at most one witness (D9); inside it, two.
	if (witnesses > 2) { throw std::invalid_argument("membership witnesses"); }
}

// The single-step rule (D3): at most one voter or witness added or removed, any learners;
// no incarnation reused, none moved to another node. The leader checks a promoted
// learner's progress itself.
inline void validate_change(const Membership& current, const Membership& next, const Limits& limits) {
	validate_membership(next, limits);
	if (next.next_incarnation < current.next_incarnation) { throw std::invalid_argument("membership incarnation counter"); }
	std::size_t changed = 0;
	for (const auto& x : next.members) {
		auto old = find_member(current, x.incarnation);
		if (!old) {
			if (x.incarnation < current.next_incarnation) { throw std::invalid_argument("membership reuses an incarnation"); }
			changed += voting(x.role);
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
	put64(bytes, 1);  // version
	put64(bytes, m.members.size());
	for (const auto& x : m.members) {
		put64(bytes, x.incarnation);
		put64(bytes, x.node);
		put64(bytes, static_cast<unsigned>(x.role));
	}
	put64(bytes, m.next_incarnation);
	put64(bytes, m.leaving);
	return bytes;
}
inline Membership decode_membership(std::string_view bytes, const Limits& limits) {
	using kronuz::journal::Corruption;
	using kronuz::journal::get64;
	if (!bytes.starts_with("RFTM")) { throw Corruption("membership magic"); }
	bytes.remove_prefix(4);
	if (get64(bytes) != 1) { throw Corruption("membership version"); }
	const auto count = get64(bytes);
	if (count > limits.members || count > bytes.size() / 24) { throw Corruption("membership count"); }
	Membership m;
	for (std::uint64_t i = 0; i < count; ++i) {
		const auto incarnation = get64(bytes), node = get64(bytes), role = get64(bytes);
		if (role > static_cast<unsigned>(MemberRole::Witness)) { throw Corruption("membership role"); }
		m.members.push_back({incarnation, node, static_cast<MemberRole>(role)});
	}
	m.next_incarnation = get64(bytes);
	m.leaving = get64(bytes);
	if (!bytes.empty()) { throw Corruption("trailing membership bytes"); }
	try {
		validate_membership(m, limits);
	} catch (const std::invalid_argument& error) {
		throw Corruption(error.what());
	}
	return m;
}

}  // namespace kronuz::consensus
