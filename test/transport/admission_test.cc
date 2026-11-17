// The per-frame rule (R7) and the group open's checks, as values: each refusal, and what each
// admits (docs/mechanisms/host-transport.md, Identity).
#include "consensus/transport/host_transport.h"
#include <cassert>
#include <cstdio>

using namespace kronuz::consensus::transport;
using kronuz::consensus::Member;
using kronuz::consensus::MemberRole;
using kronuz::consensus::NodeId;

namespace {
const char *name(Admission a) {
	switch (a) {
	case Admission::admitted:
		return "admitted";
	case Admission::unstored:
		return "unstored";
	case Admission::unopened:
		return "unopened";
	case Admission::misrouted:
		return "misrouted";
	case Admission::spoofed:
		return "spoofed";
	}
	return "?";
}
// The receiver's membership: incarnation 1 on node 1, 2 on node 2, a learner 5 on node 3; 9 unnamed.
std::optional<Member> member_of(GroupId group, NodeId incarnation) {
	assert(group == 7);
	switch (incarnation) {
	case 1:
		return Member{1, 1, MemberRole::Voter};
	case 2:
		return Member{2, 2, MemberRole::Voter};
	case 5:
		return Member{5, 3, MemberRole::Learner};
	default:
		return std::nullopt;
	}
}
void expect(const char *what, Route route, NodeId node, NodeId held, bool opened, Admission want) {
	const auto got = admit(route, node, held, opened, member_of);
	std::printf("%-58s group %llu %llu->%llu on node %llu's connection, held %llu, opened %d: %s\n", what,
				(unsigned long long)route.group, (unsigned long long)route.source, (unsigned long long)route.destination,
				(unsigned long long)node, (unsigned long long)held, int(opened), name(got));
	assert(got == want);
}
GroupOpen base() {
	GroupOpen o;
	o.cluster.fill('C');
	o.group.fill('G');
	o.epoch.fill('E');
	o.profile.fill(0xAB);
	return o;
}
} // namespace

int main() {
	// Group 7 is stored here as incarnation 4.
	expect("a voter's frame on its own node's connection", {7, 1, 4}, 1, 4, true, Admission::admitted);
	expect("a learner's frame on its own node's connection", {7, 5, 4}, 3, 4, true, Admission::admitted);
	expect("an incarnation the membership does not name (a joiner's leader)", {7, 9, 4}, 3, 4, true, Admission::admitted);
	expect("a group not stored here", {7, 1, 4}, 1, 0, true, Admission::unstored);
	expect("a group not stored here, never opened", {7, 1, 4}, 1, 0, false, Admission::unstored);
	expect("no matching open on the connection", {7, 1, 4}, 1, 4, false, Admission::unopened);
	expect("a destination incarnation not held here", {7, 1, 3}, 1, 4, true, Admission::misrouted);
	expect("destination 0", {7, 1, 0}, 1, 4, true, Admission::misrouted);
	expect("node 2's connection naming incarnation 1 (node 1's)", {7, 1, 4}, 2, 4, true, Admission::spoofed);
	expect("node 1's connection naming learner 5 (node 3's)", {7, 5, 4}, 1, 4, true, Admission::spoofed);

	// The group open's fields, each alone.
	const auto local = base();
	assert(!open_fault(local, local));
	struct Case {
		const char *field;
		void (*change)(GroupOpen &);
	} cases[] = {
		{"cluster", [](GroupOpen &o) { o.cluster[15] = 'X'; }},
		{"group", [](GroupOpen &o) { o.group[0] = 'X'; }},
		{"epoch", [](GroupOpen &o) { o.epoch[7] = 'X'; }},
		{"profile", [](GroupOpen &o) { o.profile[31] ^= 1; }},
	};
	for (auto &c : cases) {
		auto remote = base();
		c.change(remote);
		const char *fault = open_fault(local, remote);
		std::printf("an open whose %s differs in one byte: %s\n", c.field, fault ? fault : "matches");
		assert(fault && std::string_view(fault) == c.field);
		// What travels is what is checked.
		const auto decoded = decode_open(encode_open(7, remote));
		assert(decoded && decoded->first == 7);
		assert(open_fault(local, decoded->second) && std::string_view(open_fault(local, decoded->second)) == c.field);
		assert(!open_fault(remote, decoded->second));
	}
	const auto frame = encode_open(0x0102030405060708ull, local);
	assert(frame.size() == open_frame && frame[0] == char(FrameKind::open));
	assert(decode_open(frame)->first == 0x0102030405060708ull);
	assert(!decode_open(frame.substr(0, open_frame - 1)));
	assert(!decode_open(frame + "x"));
	auto other = frame;
	other[0] = char(FrameKind::control);
	assert(!decode_open(other));
	std::printf("admission: ok\n");
	return 0;
}
