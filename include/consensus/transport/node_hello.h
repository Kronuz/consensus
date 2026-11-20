#pragma once
#include <algorithm>
#include <consensus/types.h>
#include <cstdint>
#include <journal/codec.h>
#include <stdexcept>
#include <string>
#include <string_view>

// The host connection's hello, DTHL0002 (docs/mechanisms/host-connection.md). Each end sends
// one after the TLS handshake: the dialer first, then the acceptor, whose reply may refuse the
// connection as a duplicate. Little-endian, 64 bytes:
//   0  magic "DTHL0002"     8  node u64         16 lane u32        20 flags u32
//   24 cluster (16 bytes)   40 connection (16)  56 rpc_min u32     60 rpc_max u32
namespace kronuz::consensus::transport {
enum class LinkLane : std::uint32_t { Control = 1, Bulk = 2 };
struct NodeHello {
	kronuz::consensus::NodeId node = 0;
	LinkLane lane = LinkLane::Control;
	bool refused = false;  // the acceptor keeps another connection of this lane (the tie-break)
	kronuz::consensus::Identity cluster{}, connection{};
	std::uint32_t rpc_min = 1, rpc_max = 1;  // the RPC lane versions this end speaks (DTRPnnnn)
	bool operator==(const NodeHello &) const = default;
};
inline constexpr std::size_t node_hello_size = 64;
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
	return nullptr;
}
inline std::string encode_node_hello(const NodeHello &hello) {
	if (auto fault = node_hello_fault(hello))
		throw std::invalid_argument(std::string("invalid node hello: ") + fault);
	std::string bytes("DTHL0002", 8);
	kronuz::journal::put64(bytes, hello.node);
	kronuz::journal::put32(bytes, static_cast<std::uint32_t>(hello.lane));
	kronuz::journal::put32(bytes, hello.refused ? hello_refused : 0);
	bytes.append(hello.cluster.data(), hello.cluster.size());
	bytes.append(hello.connection.data(), hello.connection.size());
	kronuz::journal::put32(bytes, hello.rpc_min);
	kronuz::journal::put32(bytes, hello.rpc_max);
	return bytes;
}
inline NodeHello decode_node_hello(std::string_view bytes) {
	using kronuz::journal::Corruption;
	if (bytes.size() != node_hello_size)
		throw Corruption("node hello: wrong size");
	if (bytes.substr(0, 8) != "DTHL0002")
		throw Corruption("node hello: not DTHL0002");
	bytes.remove_prefix(8);
	NodeHello hello;
	hello.node = kronuz::journal::get64(bytes);
	hello.lane = static_cast<LinkLane>(kronuz::journal::get32(bytes));
	const auto flags = kronuz::journal::get32(bytes);
	if (flags & ~hello_refused)
		throw Corruption("node hello: reserved flags");
	hello.refused = flags & hello_refused;
	for (auto *id : {&hello.cluster, &hello.connection}) {
		std::copy_n(bytes.begin(), id->size(), id->begin());
		bytes.remove_prefix(id->size());
	}
	hello.rpc_min = kronuz::journal::get32(bytes);
	hello.rpc_max = kronuz::journal::get32(bytes);
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
}

// T1: the RPC lane's version on a connection, min(max_a, max_b) when both speak it, else 0
// (protocol_not_supported: the lane is off on this connection; group frames are unaffected).
inline std::uint32_t negotiate_rpc(const NodeHello &a, const NodeHello &b) noexcept {
	const auto version = std::min(a.rpc_max, b.rpc_max);
	return version >= std::max(a.rpc_min, b.rpc_min) ? version : 0;
}
} // namespace kronuz::consensus::transport
