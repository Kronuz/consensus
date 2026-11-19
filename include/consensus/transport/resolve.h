#pragma once
#include <algorithm>
#include <asio/ip/address.hpp>
#include <asio/ip/tcp.hpp>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

// Peer names and their lookups (docs/mechanisms/peer-dns.md). Lookups run on the process's
// lookup threads, at most LookupThreads::process_bound at once; a resolver that hangs parks
// one of them and nothing else.
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

class LookupThreads;

// One lookup on a thread of a LookupThreads. The answer goes to `deliver` (called on that
// thread) unless abandon() ran first. Abandoning a queued lookup frees it unstarted; a
// running one touches nothing of its caller's after abandon() and ends with its resolver.
// Neither the resolver nor `deliver` may call std::exit: exit waits for their thread.
class Lookup {
  public:
	using Delivery = std::function<void(std::vector<asio::ip::address>)>;
	// On the process's lookup threads. Throws std::system_error when no thread can take it.
	static std::shared_ptr<Lookup> start(PeerResolver resolver, std::string host, Delivery deliver);
	void abandon() noexcept;

  private:
	friend class LookupThreads;
	struct Owner;
	std::mutex mutex_;  // the delivery
	Delivery deliver_;
	std::shared_ptr<Owner> owner_;
	// Under the owner's mutex.
	std::list<std::shared_ptr<Lookup>>::iterator place_;
	bool queued_ = false;
	PeerResolver resolver_;
	std::string host_;
};

// A LookupThreads' state, shared with its lookups so that abandon() may outlive it.
struct Lookup::Owner {
	std::mutex mutex;
	std::condition_variable idle;
	std::size_t bound = 0;
	std::size_t busy = 0;  // threads inside a resolver or a delivery
	std::atomic<bool> closing{false};
	std::list<std::shared_ptr<Lookup>> queue;
	std::list<std::thread> running, exited;  // exited: returned, joined at the next start

	// A thread's loop: takes queued lookups until none is left, then moves itself to `exited`.
	void run(std::list<std::thread>::iterator self) {
		std::unique_lock lock(mutex);
		while (!queue.empty()) {
			auto lookup = std::move(queue.front());
			queue.pop_front();
			lookup->queued_ = false;
			auto resolver = std::exchange(lookup->resolver_, nullptr);
			auto host = std::exchange(lookup->host_, {});
			++busy;
			lock.unlock();
			std::vector<asio::ip::address> found;
			try {
				found = resolver(host);
			} catch (...) {
				found.clear();
			}
			resolver = nullptr;
			Delivery deliver;
			{
				std::lock_guard delivering(lookup->mutex_);
				deliver = std::exchange(lookup->deliver_, nullptr);
				if (deliver && !closing) {
					try {
						deliver(std::move(found));
					} catch (...) {
					}
				}
			}
			deliver = nullptr;
			lookup.reset();
			lock.lock();
			--busy;
		}
		exited.splice(exited.end(), running, self);
		if (running.empty())
			idle.notify_all();
	}
};

// The threads lookups run on: at most `bound` at once, later lookups queue in order. A thread
// starts when a lookup queues and no thread is free, and returns once the queue is empty.
// getaddrinfo cannot be cancelled, so closing (destruction, or the process's at exit) drops the
// queue, stops every delivery, and waits for each running resolver to return before it joins its
// thread; start() throws from then on.
class LookupThreads {
  public:
	// Lookups past this many at once in the process queue for a thread.
	static constexpr std::size_t process_bound = 16;
	// The process's, which Lookup::start uses. Never destroyed: an atexit handler registered when
	// it is built closes it, so a start() during or after exit throws operation_canceled.
	static LookupThreads &process() {
		static LookupThreads &threads = []() -> LookupThreads & {
			auto built = std::make_unique<LookupThreads>(process_bound);
			if (std::atexit([] { process().close(); }) != 0)
				throw std::system_error(std::make_error_code(std::errc::not_enough_memory), "LookupThreads: atexit");
			return *built.release();
		}();
		return threads;
	}

	explicit LookupThreads(std::size_t bound) : owner_(std::make_shared<Lookup::Owner>()) {
		if (bound == 0)
			throw std::invalid_argument("LookupThreads: bound 0");
		owner_->bound = bound;
	}
	LookupThreads(const LookupThreads &) = delete;
	LookupThreads &operator=(const LookupThreads &) = delete;
	~LookupThreads() { close(); }

	// Queues the lookup and starts a thread for it when none is free and fewer than `bound`
	// run. Throws std::system_error when closed, or when no thread runs and none can start.
	std::shared_ptr<Lookup> start(PeerResolver resolver, std::string host, Lookup::Delivery deliver) {
		auto lookup = std::make_shared<Lookup>();
		lookup->deliver_ = std::move(deliver);
		lookup->owner_ = owner_;
		lookup->resolver_ = std::move(resolver);
		lookup->host_ = std::move(host);
		auto &owner = *owner_;
		std::lock_guard lock(owner.mutex);
		if (owner.closing)
			throw std::system_error(std::make_error_code(std::errc::operation_canceled), "LookupThreads closed");
		owner.queue.push_back(lookup);
		lookup->place_ = std::prev(owner.queue.end());
		lookup->queued_ = true;
		// Each running thread not busy takes one queued lookup before it can return.
		if (owner.running.size() >= owner.queue.size() + owner.busy || owner.running.size() >= owner.bound)
			return lookup;
		for (auto &thread : owner.exited)
			thread.join();  // past its last use of the mutex, which this thread holds
		owner.exited.clear();
		auto self = owner.running.emplace(owner.running.end());
		try {
			*self = std::thread([&owner, self] { owner.run(self); });
		} catch (...) {
			owner.running.erase(self);
			if (!owner.running.empty())
				return lookup;  // a running thread takes it once its resolver returns
			owner.queue.erase(lookup->place_);
			lookup->queued_ = false;
			throw;
		}
		return lookup;
	}

	// Threads not yet joined, and lookups waiting for one.
	std::size_t threads() const {
		std::lock_guard lock(owner_->mutex);
		return owner_->running.size() + owner_->exited.size();
	}
	std::size_t queued() const {
		std::lock_guard lock(owner_->mutex);
		return owner_->queue.size();
	}

  private:
	std::shared_ptr<Lookup::Owner> owner_;

	// Destruction's, and the process's at exit; idempotent.
	void close() {
		auto &owner = *owner_;
		std::list<std::shared_ptr<Lookup>> dropped;
		std::list<std::thread> threads;
		{
			std::unique_lock lock(owner.mutex);
			owner.closing = true;
			for (auto &lookup : owner.queue)
				lookup->queued_ = false;
			dropped.swap(owner.queue);
			owner.idle.wait(lock, [&] { return owner.running.empty(); });
			threads.swap(owner.exited);
		}
		for (auto &thread : threads)
			thread.join();
		for (auto &lookup : dropped) {
			// No thread holds these any longer: unqueued above, under the owner's mutex.
			lookup->resolver_ = nullptr;
			lookup->host_.clear();
			std::lock_guard lock(lookup->mutex_);
			lookup->deliver_ = nullptr;
		}
	}
};

inline std::shared_ptr<Lookup> Lookup::start(PeerResolver resolver, std::string host, Delivery deliver) {
	return LookupThreads::process().start(std::move(resolver), std::move(host), std::move(deliver));
}

// A queued lookup leaves the queue with its resolver and host; then the delivery is cleared,
// after any delivery in progress returns.
inline void Lookup::abandon() noexcept {
	std::shared_ptr<Lookup> queued;
	PeerResolver resolver;
	std::string host;
	if (owner_) {
		std::lock_guard lock(owner_->mutex);
		if (queued_) {
			queued = std::move(*place_);
			owner_->queue.erase(place_);
			queued_ = false;
			resolver = std::exchange(resolver_, nullptr);  // a moved-from std::function may keep its target
			host = std::exchange(host_, {});
		}
	}
	Delivery deliver;
	std::lock_guard lock(mutex_);
	deliver = std::exchange(deliver_, nullptr);
}
} // namespace kronuz::consensus::transport
