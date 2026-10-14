#pragma once
#include <algorithm>
#include <array>
#include <consensus/types.h>
#include <journal/codec.h>
#include <string>

namespace kronuz::consensus::transport {
using Fingerprint = std::array<unsigned char, 32>;
enum class Lane : std::uint64_t { Control = 1, Bulk = 2 };
struct Hello {
	kronuz::consensus::Identity cluster{}, group{}, epoch{}, connection{};
	kronuz::consensus::NodeId node = 0;
	Lane lane = Lane::Control;
	Fingerprint profile{};
};
inline constexpr std::size_t hello_size = 120;
inline std::string encode_hello(const Hello &hello) {
	using namespace kronuz::consensus;
	if (!hello.node || hello.cluster == Identity{} || hello.group == Identity{} ||
		hello.epoch == Identity{} || hello.connection == Identity{} ||
		(hello.lane != Lane::Control && hello.lane != Lane::Bulk))
		throw std::invalid_argument("invalid peer hello");
	std::string bytes("DTHL0001", 8);
	kronuz::journal::put64(bytes, hello.node);
	kronuz::journal::put64(bytes, static_cast<std::uint64_t>(hello.lane));
	for (auto id : {hello.cluster, hello.group, hello.epoch, hello.connection})
		bytes.append(id.data(), id.size());
	bytes.append(reinterpret_cast<const char *>(hello.profile.data()), hello.profile.size());
	return bytes;
}
inline Hello decode_hello(std::string_view bytes) {
	if (bytes.size() != hello_size || !bytes.starts_with("DTHL0001"))
		throw kronuz::journal::Corruption("invalid peer hello frame");
	bytes.remove_prefix(8);
	Hello result;
	result.node = kronuz::journal::get64(bytes);
	result.lane = static_cast<Lane>(kronuz::journal::get64(bytes));
	for (auto *id : {&result.cluster, &result.group, &result.epoch, &result.connection}) {
		std::copy_n(bytes.begin(), id->size(), id->begin());
		bytes.remove_prefix(id->size());
	}
	std::copy_n(bytes.begin(), result.profile.size(), result.profile.begin());
	(void)encode_hello(result);
	return result;
}
// Certificate admission must have established authenticated_node independently.
inline void validate_hello(const Hello &remote, const Hello &local,
						   kronuz::consensus::NodeId authenticated_node, Lane lane) {
	if (remote.node != authenticated_node || remote.node == local.node || remote.lane != lane ||
		remote.cluster != local.cluster || remote.group != local.group || remote.epoch != local.epoch ||
		remote.profile != local.profile || remote.connection == local.connection)
		throw std::invalid_argument("incompatible authenticated peer hello");
}
} // namespace kronuz::consensus::transport
