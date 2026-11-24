// DTHL0003, the host connection's hello: its bytes, each malformed field refused, the checks
// against this end's hello, and T1's version negotiation.
#include "consensus/transport/node_hello.h"
#include <cassert>
#include <cstdio>
#include <functional>

using namespace kronuz::consensus::transport;
using kronuz::journal::Corruption;

namespace {
kronuz::consensus::Identity id(char c) {
	kronuz::consensus::Identity result{};
	result.fill(c);
	return result;
}
NodeHello sample() { return {0x0102030405060708, LinkLane::Bulk, false, id('K'), id('c'), 1, 3}; }

void put32(std::string &bytes, std::size_t at, std::uint32_t v) {
	for (int i = 0; i != 4; ++i)
		bytes[at + i] = char(v >> (8 * i));
}
void put64(std::string &bytes, std::size_t at, std::uint64_t v) {
	for (int i = 0; i != 8; ++i)
		bytes[at + i] = char(v >> (8 * i));
}
template <class E> void refused(std::function<void()> f, std::string_view expect) {
	try {
		f();
	} catch (const E &error) {
		const std::string what = error.what();
		if (what.find(expect) == std::string::npos) {
			std::fprintf(stderr, "expected '%.*s', got '%s'\n", int(expect.size()), expect.data(), what.c_str());
			assert(false);
		}
		return;
	}
	std::fprintf(stderr, "not refused: %.*s\n", int(expect.size()), expect.data());
	assert(false);
}
void decode_refuses(std::string bytes, std::string_view expect) {
	refused<Corruption>([&] { (void)decode_node_hello(bytes); }, expect);
}
} // namespace

int main() {
	// The bytes, field by field.
	const auto bytes = encode_node_hello(sample());
	std::string expect = "DTHL0003";
	expect += std::string("\x08\x07\x06\x05\x04\x03\x02\x01", 8);
	expect += std::string("\x02\0\0\0", 4) + std::string(4, '\0');
	expect += std::string(16, 'K') + std::string(16, 'c');
	expect += std::string(16, '\0') + std::string(8, '\0') + std::string(4, '\0');  // no boot, directory 0, no reason
	expect += std::string("\x01\0\0\0\x03\0\0\0", 8);
	expect += std::string("\x01\0\0\0\x01\0\0\0", 8);  // wire versions 1-1; the default limits follow
	kronuz::consensus::Limits limits;
	for (auto *field : limit_fields(limits))
		kronuz::journal::put64(expect, *field);
	assert(bytes == expect && bytes.size() == node_hello_size);
	assert(decode_node_hello(bytes) == sample());
	auto refusal = sample();
	refusal.refused = true;
	assert(encode_node_hello(refusal)[20] == 1 && decode_node_hello(encode_node_hello(refusal)) == refusal);
	auto booted = sample();
	booted.boot = id('b');
	booted.directory = 0x1122334455667788;
	const auto booted_bytes = encode_node_hello(booted);
	assert(booted_bytes.substr(56, 16) == std::string(16, 'b') && booted_bytes[72] == '\x88' && booted_bytes[79] == '\x11');
	assert(decode_node_hello(booted_bytes) == booted);
	auto boot_refusal = booted;
	boot_refusal.refused = true;
	boot_refusal.reason = 7;
	assert(encode_node_hello(boot_refusal)[80] == 7 && decode_node_hello(encode_node_hello(boot_refusal)) == boot_refusal);

	// Each malformed field.
	decode_refuses(bytes.substr(0, 63), "wrong size");
	decode_refuses(bytes + '\0', "wrong size");
	decode_refuses({}, "wrong size");
	auto v1 = bytes;
	v1.replace(0, 8, "DTHL0001");
	decode_refuses(v1, "not DTHL0003");
	auto v2 = bytes;
	v2.replace(0, 8, "DTHL0002");
	decode_refuses(v2, "not DTHL0003");
	decode_refuses(std::string("DTHL0001") + std::string(112, 'x'), "wrong size");  // a whole DTHL0001 hello
	auto patched = [&](auto patch) {
		auto b = bytes;
		patch(b);
		return b;
	};
	decode_refuses(patched([](auto &b) { put64(b, 8, 0); }), "node 0");
	decode_refuses(patched([](auto &b) { put32(b, 16, 0); }), "unknown lane");
	decode_refuses(patched([](auto &b) { put32(b, 16, 3); }), "unknown lane");
	decode_refuses(patched([](auto &b) { put32(b, 20, 2); }), "reserved flags");
	decode_refuses(patched([](auto &b) { put32(b, 20, 0x80000001u); }), "reserved flags");
	decode_refuses(patched([](auto &b) { b.replace(24, 16, std::string(16, '\0')); }), "zero cluster");
	decode_refuses(patched([](auto &b) { b.replace(40, 16, std::string(16, '\0')); }), "zero connection");
	decode_refuses(patched([](auto &b) { put32(b, 80, 3); }), "a reason without a refusal");
	decode_refuses(patched([](auto &b) { put32(b, 84, 0); }), "invalid RPC version range");
	decode_refuses(patched([](auto &b) { put32(b, 84, 4); }), "invalid RPC version range");  // min 4 > max 3
	decode_refuses(patched([](auto &b) { put32(b, 88, 10000); }), "invalid RPC version range");
	assert(decode_node_hello(patched([](auto &b) { put32(b, 88, 9999); })).rpc_max == 9999);
	auto bad = sample();
	bad.node = 0;
	refused<std::invalid_argument>([&] { (void)encode_node_hello(bad); }, "node 0");

	// Against this end's hello: node 2 dialed node 1's host.
	NodeHello local{1, LinkLane::Bulk, false, id('K'), id('a'), 1, 1};
	NodeHello remote{2, LinkLane::Bulk, false, id('K'), id('b'), 1, 1};
	check_node_hello(remote, local, 2, false);
	refused<std::invalid_argument>([&] { check_node_hello(remote, local, 3, false); }, "not the authenticated node");
	auto self = remote;
	self.node = 1;
	refused<std::invalid_argument>([&] { check_node_hello(self, local, 1, false); }, "from this node");
	auto lane = remote;
	lane.lane = LinkLane::Control;
	refused<std::invalid_argument>([&] { check_node_hello(lane, local, 2, false); }, "another lane");
	auto cluster = remote;
	cluster.cluster = id('X');
	refused<std::invalid_argument>([&] { check_node_hello(cluster, local, 2, false); }, "another cluster");
	auto echo = remote;
	echo.connection = local.connection;
	refused<std::invalid_argument>([&] { check_node_hello(echo, local, 2, false); }, "connection id");
	auto refusing = remote;
	refusing.refused = true;
	refused<std::invalid_argument>([&] { check_node_hello(refusing, local, 2, false); }, "a dialer refused");
	check_node_hello(refusing, local, 2, true);

	// T1: min(max_a, max_b) when it is at least max(min_a, min_b); otherwise 0, the lane off.
	auto range = [](std::uint32_t lo, std::uint32_t hi) { return NodeHello{1, LinkLane::Bulk, false, id('K'), id('a'), lo, hi}; };
	assert(negotiate_rpc(range(1, 3), range(2, 5)) == 3);
	assert(negotiate_rpc(range(2, 5), range(1, 3)) == 3);
	assert(negotiate_rpc(range(1, 1), range(1, 1)) == 1);
	assert(negotiate_rpc(range(1, 1), range(2, 2)) == 0);
	assert(negotiate_rpc(range(2, 4), range(1, 2)) == 2);
	assert(negotiate_rpc(range(5, 9), range(1, 4)) == 0);
	std::printf("node_hello: DTHL0003 bytes, 16 malformed inputs refused, 7 checks, negotiation passed\n");
	return 0;
}
