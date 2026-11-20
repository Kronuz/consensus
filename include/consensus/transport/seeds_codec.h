#pragma once
#include <compare>
#include <consensus/types.h>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

// The multicast seed datagram, DTSD0001 (docs/mechanisms/seeds.md): what a node announces. No
// I/O here, so the decoder fuzzes alone (fuzz/seeds_fuzz.cc).
namespace kronuz::consensus::transport {
// One service a node announces: a name its peers ask for and its listening port.
struct SeedService {
	std::string name;
	std::uint16_t port = 0;
	auto operator<=>(const SeedService &) const = default;
};
// What one datagram says: the cluster, the announcing process, and its services.
struct Announcement {
	kronuz::consensus::Identity cluster{}, sender{};
	std::vector<SeedService> services;
	bool operator==(const Announcement &) const = default;
};

// DTSD0001, little-endian: magic (8), cluster (16), sender (16), service count (u8), then per
// service a name length (u8), the name, and its port (u16). The version is the magic's digits.
inline constexpr std::uint32_t seed_version = 1;
inline constexpr std::size_t seed_header = 41;
inline constexpr std::size_t seed_services_limit = 8;  // Detent's voters and relays, Xapiand's node
inline constexpr std::size_t seed_name_limit = 32;
inline constexpr std::size_t seed_datagram_limit = 512;  // above the largest valid (321): never fragmented

// Why a datagram is not an announcement this build reads; every fault is a drop with a counter.
enum class SeedFault : std::uint8_t { none, oversized, malformed, version };

// Why a service is invalid, or null: a name of 1 to 32 bytes of [a-z0-9._-] and a nonzero port.
inline const char *seed_service_fault(const SeedService &service) noexcept {
	if (service.name.empty() || service.name.size() > seed_name_limit)
		return "name length";
	for (const char c : service.name)
		if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-'))
			return "name character";
	return service.port ? nullptr : "port 0";
}
// Why an announcement is invalid, or null: a nonzero cluster, 1 to 8 services, names distinct.
inline const char *announcement_fault(const Announcement &a) noexcept {
	if (a.cluster == kronuz::consensus::Identity{})
		return "zero cluster";
	if (a.services.empty() || a.services.size() > seed_services_limit)
		return "service count";
	for (std::size_t i = 0; i < a.services.size(); ++i) {
		if (auto fault = seed_service_fault(a.services[i]))
			return fault;
		for (std::size_t j = 0; j < i; ++j)
			if (a.services[j].name == a.services[i].name)
				return "repeated service";
	}
	return nullptr;
}

inline std::string encode_announcement(const Announcement &a, std::uint32_t version = seed_version) {
	if (auto fault = announcement_fault(a))
		throw std::invalid_argument(std::string("invalid announcement: ") + fault);
	if (!version || version > 9999)
		throw std::invalid_argument("invalid announcement: version");
	std::string bytes = "DTSD" + std::to_string(10000 + version).substr(1);
	bytes.append(a.cluster.data(), a.cluster.size());
	bytes.append(a.sender.data(), a.sender.size());
	bytes.push_back(char(a.services.size()));
	for (const auto &s : a.services) {
		bytes.push_back(char(s.name.size()));
		bytes += s.name;
		bytes.push_back(char(s.port & 0xff));
		bytes.push_back(char(s.port >> 8));
	}
	return bytes;
}

// Decodes `bytes` into `out` when it is a well-formed DTSD0001 announcement; the fault otherwise,
// with `out` unspecified. A DTSD magic with other digits is another version.
inline SeedFault decode_announcement(std::string_view bytes, Announcement &out) {
	if (bytes.size() > seed_datagram_limit)
		return SeedFault::oversized;
	if (bytes.size() < 8 || bytes.substr(0, 4) != "DTSD")
		return SeedFault::malformed;
	for (const char c : bytes.substr(4, 4))
		if (c < '0' || c > '9')
			return SeedFault::malformed;
	if (bytes.substr(4, 4) != "0001")
		return SeedFault::version;
	if (bytes.size() < seed_header)
		return SeedFault::malformed;
	std::memcpy(out.cluster.data(), bytes.data() + 8, out.cluster.size());
	std::memcpy(out.sender.data(), bytes.data() + 24, out.sender.size());
	const std::size_t count = std::uint8_t(bytes[40]);
	if (count > seed_services_limit)
		return SeedFault::malformed;
	bytes.remove_prefix(seed_header);
	out.services.clear();
	for (std::size_t i = 0; i < count; ++i) {
		if (bytes.empty())
			return SeedFault::malformed;
		const std::size_t length = std::uint8_t(bytes[0]);
		if (bytes.size() < 1 + length + 2)
			return SeedFault::malformed;
		SeedService s{std::string(bytes.substr(1, length)),
					  std::uint16_t(std::uint8_t(bytes[1 + length]) | std::uint8_t(bytes[2 + length]) << 8)};
		out.services.push_back(std::move(s));
		bytes.remove_prefix(1 + length + 2);
	}
	return !bytes.empty() || announcement_fault(out) ? SeedFault::malformed : SeedFault::none;
}
} // namespace kronuz::consensus::transport
