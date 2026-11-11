#pragma once
#include <algorithm>
#include <asio/ip/address.hpp>
#include <asio/ip/tcp.hpp>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <vector>

// Peer names and their lookups (docs/mechanisms/peer-dns.md). A lookup runs on a thread of
// its own, so a resolver that hangs parks that thread and nothing else.
namespace kronuz::consensus::transport {
// A peer as configured: a host name or an IP literal, and its port.
struct PeerAddress {
	std::string host;
	std::uint16_t port = 0;
};
// Blocking: returns the host's addresses, or none when it does not resolve. Peers calls it
// on lookup threads, several at once, so it must be safe to call concurrently.
using PeerResolver = std::function<std::vector<asio::ip::address>(const std::string &host)>;

// getaddrinfo, as the system configures it (hosts file, DNS, its own timeouts).
inline std::vector<asio::ip::address> system_resolver(const std::string &host) {
	addrinfo hints{};
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	addrinfo *found = nullptr;
	if (::getaddrinfo(host.c_str(), nullptr, &hints, &found) != 0)
		return {};
	std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> owned(found, ::freeaddrinfo);
	std::vector<asio::ip::address> result;
	for (auto *entry = found; entry; entry = entry->ai_next) {
		if (entry->ai_family == AF_INET) {
			sockaddr_in v4;
			std::memcpy(&v4, entry->ai_addr, sizeof v4);
			result.push_back(asio::ip::address_v4(ntohl(v4.sin_addr.s_addr)));
		} else if (entry->ai_family == AF_INET6) {
			sockaddr_in6 v6;
			std::memcpy(&v6, entry->ai_addr, sizeof v6);
			asio::ip::address_v6::bytes_type bytes;
			std::memcpy(bytes.data(), &v6.sin6_addr, bytes.size());
			result.push_back(asio::ip::address_v6(bytes, v6.sin6_scope_id));
		}
	}
	return result;
}

// RFC 8305 section 4: the first address's family, then the other, alternating; each family
// keeps the resolver's order.
inline std::vector<asio::ip::tcp::endpoint> interleave(const std::vector<asio::ip::address> &addresses,
													   std::uint16_t port) {
	std::vector<asio::ip::tcp::endpoint> first, other, result;
	if (addresses.empty())
		return result;
	for (const auto &address : addresses)
		(address.is_v6() == addresses.front().is_v6() ? first : other).emplace_back(address, port);
	for (std::size_t i = 0; i != std::max(first.size(), other.size()); ++i) {
		if (i < first.size())
			result.push_back(first[i]);
		if (i < other.size())
			result.push_back(other[i]);
	}
	return result;
}

// One lookup on its own detached thread. The answer goes to `deliver` (called on that
// thread) unless abandon() ran first; after that the thread touches nothing of its
// caller's and ends when the resolver returns.
class Lookup {
  public:
	using Delivery = std::function<void(std::vector<asio::ip::address>)>;
	// Throws std::system_error when no thread can start.
	static std::shared_ptr<Lookup> start(PeerResolver resolver, std::string host, Delivery deliver) {
		auto lookup = std::make_shared<Lookup>();
		lookup->deliver_ = std::move(deliver);
		auto work = [lookup, resolver = std::move(resolver), host = std::move(host)] {
			std::vector<asio::ip::address> found;
			try {
				found = resolver(host);
			} catch (...) {
				found.clear();
			}
			std::lock_guard lock(lookup->mutex_);
			if (lookup->deliver_) {
				try {
					lookup->deliver_(std::move(found));
				} catch (...) {
				}
				lookup->deliver_ = nullptr;
			}
		};
		std::thread(std::move(work)).detach();
		return lookup;
	}
	void abandon() noexcept {
		std::lock_guard lock(mutex_);
		deliver_ = nullptr;
	}

  private:
	std::mutex mutex_;
	Delivery deliver_;
};
} // namespace kronuz::consensus::transport
