#pragma once
#include <algorithm>
#include <consensus/membership.h>
#include <consensus/types.h>
#include <cstdint>
#include <array>
#include <journal/codec.h>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

// The host connection's hello, DTHL0003 (docs/mechanisms/hello.md). Each end sends one after
// the TLS handshake: the dialer first, then the acceptor, whose reply may refuse the connection,
// as a duplicate lane or, with a reason, the dialer's boot. Little-endian, 180 bytes:
//   0  magic "DTHL0003"     8  node u64         16 lane u32        20 flags u32
//   24 cluster (16 bytes)   40 connection (16)  56 boot (16)       72 directory u64
//   80 reason u32           84 rpc_min u32      88 rpc_max u32     92 wire_min u32
//   96 wire_max u32         100 the node's ten Limits, u64 each, in types.h order
namespace kronuz::consensus::transport {
enum class LinkLane : std::uint32_t { Control = 1, Bulk = 2 };
struct NodeHello {
	kronuz::consensus::NodeId node = 0;
	LinkLane lane = LinkLane::Control;
	bool refused = false;  // the acceptor refuses: another connection of this lane, or `reason`
	kronuz::consensus::Identity cluster{}, connection{};
	std::uint32_t rpc_min = 1, rpc_max = 1;  // the RPC lane versions this end speaks (DTRPnnnn)
	std::uint32_t wire_min = 1, wire_max = 1;  // the host transport's frame versions it speaks
	kronuz::consensus::Limits limits{};  // the node's own: what its peers' leaders send it within
	// This process's start (M7), zero for none, and the directory version that admitted it, 0
	// before admission.
	kronuz::consensus::Identity boot{};
	std::uint64_t directory = 0;
	std::uint32_t reason = 0;  // a refusing acceptor's check of the dialer's boot: nonzero, the caller's code
	bool operator==(const NodeHello &) const = default;
};
inline constexpr std::size_t node_hello_size = 180;
inline constexpr std::uint32_t wire_version = 1;  // this build's host transport frames and DTC2 messages
// A peer whose wire versions share none with this end's: refused before any frame.
struct WireVersionRefused : std::invalid_argument {
	using std::invalid_argument::invalid_argument;
};
// The ten Limits in their wire order.
inline auto limit_fields(kronuz::consensus::Limits &l) {
	return std::array{&l.voters, &l.command_bytes, &l.log_bytes, &l.log_entries, &l.control_entries,
					  &l.rpc_bytes, &l.rpc_entries, &l.reads, &l.uncommitted_entries, &l.members};
}
inline constexpr std::uint32_t rpc_version_limit = 9999;  // four digits in the lane's magic
inline constexpr std::uint32_t hello_refused = 1;

// Why a hello is malformed, or null when it is well formed.
inline const char *node_hello_fault(const NodeHello &hello) noexcept {
	if (!hello.node)
		return "node 0";
	if (hello.lane != LinkLane::Control && hello.lane != LinkLane::Bulk)
		return "unknown lane";
	if (hello.cluster == kronuz::consensus::Identity{})
		return "zero cluster";
	if (hello.connection == kronuz::consensus::Identity{})
		return "zero connection";
	if (!hello.rpc_min || hello.rpc_max < hello.rpc_min || hello.rpc_max > rpc_version_limit)
		return "invalid RPC version range";
	if (!hello.wire_min || hello.wire_max < hello.wire_min)
		return "invalid wire version range";
	if (hello.reason && !hello.refused)
		return "a reason without a refusal";
	try {
		kronuz::consensus::validate_limits(hello.limits);
	} catch (const std::invalid_argument &) {
		return "invalid limits";
	}
	return nullptr;
}
inline std::string encode_node_hello(const NodeHello &hello) {
	if (auto fault = node_hello_fault(hello))
		throw std::invalid_argument(std::string("invalid node hello: ") + fault);
	std::string bytes("DTHL0003", 8);
	kronuz::journal::put64(bytes, hello.node);
	kronuz::journal::put32(bytes, static_cast<std::uint32_t>(hello.lane));
	kronuz::journal::put32(bytes, hello.refused ? hello_refused : 0);
	bytes.append(hello.cluster.data(), hello.cluster.size());
	bytes.append(hello.connection.data(), hello.connection.size());
	bytes.append(hello.boot.data(), hello.boot.size());
	kronuz::journal::put64(bytes, hello.directory);
	kronuz::journal::put32(bytes, hello.reason);
	kronuz::journal::put32(bytes, hello.rpc_min);
	kronuz::journal::put32(bytes, hello.rpc_max);
	kronuz::journal::put32(bytes, hello.wire_min);
	kronuz::journal::put32(bytes, hello.wire_max);
	auto limits = hello.limits;
	for (auto *field : limit_fields(limits))
		kronuz::journal::put64(bytes, *field);
	return bytes;
}
inline NodeHello decode_node_hello(std::string_view bytes) {
	using kronuz::journal::Corruption;
	if (bytes.size() != node_hello_size)
		throw Corruption("node hello: wrong size");
	if (bytes.substr(0, 8) != "DTHL0003")
		throw Corruption("node hello: not DTHL0003");
	bytes.remove_prefix(8);
	NodeHello hello;
	hello.node = kronuz::journal::get64(bytes);
	hello.lane = static_cast<LinkLane>(kronuz::journal::get32(bytes));
	const auto flags = kronuz::journal::get32(bytes);
	if (flags & ~hello_refused)
		throw Corruption("node hello: reserved flags");
	hello.refused = flags & hello_refused;
	for (auto *id : {&hello.cluster, &hello.connection, &hello.boot}) {
		std::copy_n(bytes.begin(), id->size(), id->begin());
		bytes.remove_prefix(id->size());
	}
	hello.directory = kronuz::journal::get64(bytes);
	hello.reason = kronuz::journal::get32(bytes);
	hello.rpc_min = kronuz::journal::get32(bytes);
	hello.rpc_max = kronuz::journal::get32(bytes);
	hello.wire_min = kronuz::journal::get32(bytes);
	hello.wire_max = kronuz::journal::get32(bytes);
	for (auto *field : limit_fields(hello.limits)) {
		const auto value = kronuz::journal::get64(bytes);
		if (value > std::numeric_limits<std::size_t>::max())
			throw Corruption("node hello: a limit past size_t");
		*field = std::size_t(value);
	}
	if (auto fault = node_hello_fault(hello))
		throw Corruption(std::string("node hello: ") + fault);
	return hello;
}

// The peer's hello against this end's, once TLS authenticated the peer's node. A dialer's hello
// never refuses; an acceptor's may.
inline void check_node_hello(const NodeHello &remote, const NodeHello &local,
							 kronuz::consensus::NodeId authenticated, bool remote_accepted) {
	if (remote.node != authenticated)
		throw std::invalid_argument("node hello: not the authenticated node");
	if (remote.node == local.node)
		throw std::invalid_argument("node hello: from this node");
	if (remote.lane != local.lane)
		throw std::invalid_argument("node hello: another lane");
	if (remote.cluster != local.cluster)
		throw std::invalid_argument("node hello: another cluster");
	if (remote.connection == local.connection)
		throw std::invalid_argument("node hello: this end's connection id");
	if (remote.refused && !remote_accepted)
		throw std::invalid_argument("node hello: a dialer refused");
	if (std::max(remote.wire_min, local.wire_min) > std::min(remote.wire_max, local.wire_max))
		throw WireVersionRefused("node hello: wire versions " + std::to_string(remote.wire_min) + "-" +
								 std::to_string(remote.wire_max) + " share none with " + std::to_string(local.wire_min) +
								 "-" + std::to_string(local.wire_max));
}

// T1: the RPC lane's version on a connection, min(max_a, max_b) when both speak it, else 0
// (protocol_not_supported: the lane is off on this connection; group frames are unaffected).
inline std::uint32_t negotiate_rpc(const NodeHello &a, const NodeHello &b) noexcept {
	const auto version = std::min(a.rpc_max, b.rpc_max);
	return version >= std::max(a.rpc_min, b.rpc_min) ? version : 0;
}
} // namespace kronuz::consensus::transport
