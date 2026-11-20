// Listen ports a test picks before its servers bind them and keeps across their restarts.
// A port read back from bind(0) and closed returns to the ephemeral pool, where any bind(0) or
// connect() beside the test (a self-connect included) may take it before the server binds.
// These ports lie outside that pool, and a claim keeps tests running side by side apart.
#pragma once

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/sysctl.h>
#endif

namespace kronuz::consensus::testing {

// The ports bind(0) and connect() hand out, as the system configures them.
inline std::pair<unsigned, unsigned> ephemeral_range() {
#if defined(__linux__)
	unsigned first = 0, last = 0;
	std::ifstream range("/proc/sys/net/ipv4/ip_local_port_range");
	if (range >> first >> last)
		return {first, last};
	return {32768, 60999};
#elif defined(__APPLE__) || defined(__FreeBSD__)
	int first = 0, last = 0;
	std::size_t size = sizeof first;
	if (::sysctlbyname("net.inet.ip.portrange.first", &first, &size, nullptr, 0) == 0 &&
		::sysctlbyname("net.inet.ip.portrange.last", &last, &size, nullptr, 0) == 0)
		return {static_cast<unsigned>(first), static_cast<unsigned>(last)};
	return {49152, 65535};
#else
	return {49152, 65535};
#endif
}

inline bool ephemeral(std::uint16_t port) {
	static const auto range = ephemeral_range();
	return port >= range.first && port <= range.second;
}

// Claims `port` among the processes that pick here: a UDP socket bound to it for the rest of
// the process, without SO_REUSEADDR, so a picker beside this one fails to claim it too.
inline bool claim(std::uint16_t port) {
	const int fd = ::socket(AF_INET6, SOCK_DGRAM, 0);
	if (fd < 0) {
		std::perror("claim: socket");
		std::abort();
	}
	const int dual = 0;
	::setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &dual, sizeof dual);
	sockaddr_in6 at{};
	at.sin6_family = AF_INET6;
	at.sin6_port = htons(port);
	at.sin6_addr = in6addr_any;
	if (::bind(fd, reinterpret_cast<const sockaddr *>(&at), sizeof at) == 0)
		return true;
	::close(fd);
	return false;
}

inline bool bindable(asio::io_context &io, asio::ip::tcp::endpoint endpoint) {
	asio::error_code error;
	asio::ip::tcp::acceptor probe(io);
	probe.open(endpoint.protocol(), error);
	if (!error)
		probe.bind(endpoint, error);
	return !error;
}

// A port this process alone binds on `address` for its whole run, restarts included: outside
// the ephemeral range, claimed, and free now.
inline std::uint16_t free_port(asio::io_context &io, asio::ip::address address) {
	static const auto [low, high] = [] {
		const auto [first, last] = ephemeral_range();
		if (first >= 2048)
			return std::pair<unsigned, unsigned>{first > 16384 + 1024 ? first - 16384 : 1024, first};
		if (last > 65535 - 1024) {
			std::fprintf(stderr, "no port range outside the ephemeral one [%u, %u]\n", first, last);
			std::abort();
		}
		return std::pair<unsigned, unsigned>{last + 1, 65536};
	}();
	static unsigned next = static_cast<unsigned>(::getpid()) * 7919u;
	for (unsigned tried = 0; tried != high - low; ++tried) {
		const auto port = static_cast<std::uint16_t>(low + next++ % (high - low));
		if (claim(port) && bindable(io, {address, port}))
			return port;
	}
	std::fprintf(stderr, "no free port in [%u, %u)\n", low, high);
	std::abort();
}

} // namespace kronuz::consensus::testing
