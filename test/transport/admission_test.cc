// The per-frame rule (R7) and the group open's checks, as values: each refusal, and what each
// admits (docs/mechanisms/host-transport.md, Identity). Then what a connection owes (Owing): a
// backlog that never empties frees its repaid prefix (feed-scan review N3), and a flood of
// refusals for distinct groups owes at most `closes` closed answers (identity review N1).
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
// A backlog that never empties, under the merge bound: 500 records owed, then rounds of a
// 1,000-record batch and 1,000 repaid. Without freeing its repaid prefix the vector would hold
// every record ever owed; it holds at most a few batches.
void owing_prefix() {
	Owing owing;
	auto batch = [](GroupId from, std::size_t n) {
		std::vector<Owing::Record> b;
		for (GroupId g = from; g != from + n; ++g)
			b.push_back({g, 64, false});
		return b;
	};
	owing.add(batch(1, 500), 4096);
	std::size_t most = 0;
	for (int round = 0; round != 200; ++round) {
		owing.add(batch(1000 + GroupId(round) * 1000, 1000), 4096);
		for (int i = 0; i != 1000; ++i)
			(void)owing.next();
		most = std::max(most, owing.held());
	}
	std::printf("owing prefix: 200 rounds, %zu owed, at most %zu records held\n", owing.owed(), most);
	assert(owing.owed() == 500 && most <= 4000);
}
// 100,000 refusals of distinct groups while nothing is repaid: at most 4,096 closed answers owed.
void owing_closes() {
	Owing owing;
	std::size_t dropped = 0;
	for (GroupId base = 0; base != 100'000; base += 1000) {
		std::vector<Owing::Record> batch;
		for (GroupId g = base; g != base + 1000; ++g)
			batch.push_back({g + 1, 0, true});
		dropped += owing.add(std::move(batch), 4096);
	}
	std::printf("owing closes: %zu owed, %zu closed answers, %zu dropped\n", owing.owed(), owing.closes(), dropped);
	assert(owing.owed() <= 4096 && owing.closes() <= 4096 && dropped == 100'000 - owing.closes());
	// A credit keeps its record, its closed dropped; once repaid, a closed is owed again.
	owing.add({{200'000, 64, true}}, 4096);
	assert(owing.owed() == 4097 && owing.closes() == 4096);
	while (!owing.empty())
		(void)owing.next();
	assert(!owing.closes() && !owing.held());
	owing.add({{7, 0, true}}, 4096);
	assert(owing.closes() == 1 && owing.next().close);
}
// Records merge per group past the bound; the bytes and the closed are kept whole.
void owing_merge() {
	Owing owing;
	std::uint64_t bytes = 0;
	for (int round = 0; round != 3; ++round) {
		std::vector<Owing::Record> batch;
		for (GroupId g = 1; g <= 3000; ++g)
			batch.push_back({g, 10, g == 5});
		bytes += 3000 * 10;
		owing.add(std::move(batch), 4096);
	}
	assert(owing.owed() == 3000 && owing.closes() == 1);
	std::uint64_t seen = 0;
	bool closed = false;
	while (!owing.empty()) {
		const auto r = owing.next();
		seen += r.bytes;
		closed |= r.group == 5 && r.close;
	}
	assert(seen == bytes && closed);
	std::printf("owing merge: 9,000 records of 3,000 groups owed as 3,000, every byte and the closed kept\n");
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
	owing_prefix();
	owing_closes();
	owing_merge();
	return 0;
}
