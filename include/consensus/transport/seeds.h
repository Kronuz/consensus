#pragma once
#include "seeds_codec.h"
#include <algorithm>
#include <array>
#include <cassert>
#include <asio/any_io_executor.hpp>
#include <asio/ip/multicast.hpp>
#include <asio/ip/udp.hpp>
#include <asio/steady_timer.hpp>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <net/if.h>
#include <random>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <vector>

// Multicast seeds (docs/mechanisms/seeds.md): a node announces its services by UDP multicast
// and learns other nodes' addresses as seeds. An announcement is a hint, never an identity:
// whoever answers at a seed is authenticated by the transport's TLS hello and node pins.
namespace kronuz::consensus::transport {
struct SeedOptions {
	std::string group = "239.192.168.1";  // the Xapian-era discovery group and port
	std::uint16_t port = 58880;
	// Where to send and join: empty is the system's choice (the default route's interface); an
	// IPv4 address for an IPv4 group, an interface name for an IPv6 one.
	std::string interface;
	int ttl = 3;  // hops: 0 keeps announcements on this host, 1 on the link
	// One announcement per interval plus up to `jitter`, after a burst of `burst` spaced
	// `burst_spacing` at start, so a node starting is found at once and a quiet one costs little.
	std::chrono::milliseconds interval{2000}, jitter{1000}, burst_spacing{100};
	std::size_t burst = 3;
	std::chrono::milliseconds expire{10000};  // a seed not heard this long leaves the set
	std::size_t seeds = 256;  // the set's bound: a flood of senders costs this many entries, no more
	// A seed whose dial failed waits backoff, doubling per failure up to backoff_max.
	std::chrono::milliseconds backoff{1000}, backoff_max{60000};
};
inline void validate_seed_options(const SeedOptions &o) {
	asio::error_code error;
	const auto group = asio::ip::make_address(o.group, error);
	if (error || !group.is_multicast())
		throw std::invalid_argument("seeds: group is not a multicast address");
	if (!o.port)
		throw std::invalid_argument("seeds: port 0");
	if (o.ttl < 0 || o.ttl > 255)
		throw std::invalid_argument("seeds: ttl outside 0..255");
	if (o.interval < std::chrono::milliseconds(10) || o.jitter.count() < 0 || o.jitter > o.interval)
		throw std::invalid_argument("seeds: interval below 10 ms or jitter outside 0..interval");
	if (o.burst > 16 || (o.burst && o.burst_spacing < std::chrono::milliseconds(1)))
		throw std::invalid_argument("seeds: burst above 16 or spaced below 1 ms");
	if (o.expire < o.interval + o.jitter)
		throw std::invalid_argument("seeds: expire below one interval and its jitter");
	if (!o.seeds || o.seeds > 65536)
		throw std::invalid_argument("seeds: set bound outside 1..65536");
	if (o.backoff < std::chrono::milliseconds(1) || o.backoff_max < o.backoff)
		throw std::invalid_argument("seeds: backoff below 1 ms or above backoff_max");
}

// A node another process announced: where it was heard from and the asked-for services there.
struct Seed {
	std::uint64_t id = 0;  // names the seed to failed() and joined()
	kronuz::consensus::Identity sender{};
	asio::ip::address address;
	std::vector<SeedService> services;
	std::uint16_t port(std::string_view name) const noexcept {
		for (const auto &s : services)
			if (s.name == name)
				return s.port;
		return 0;
	}
};
struct SeedStats {
	std::uint64_t announced = 0, send_failures = 0, received = 0;
	std::uint64_t oversized = 0, malformed = 0, version = 0, cluster = 0;  // dropped, never a seed
	std::uint64_t own = 0, unwanted = 0, full = 0;  // well formed, no seed: this process, no asked-for service, set full
	std::uint64_t dials = 0, failures = 0, expired = 0;
};

// The multicast seed source on one loop's executor: it announces `services` and seeds the
// services in `wanted`. Unavailable multicast (no route, a sandbox) is reported once through
// on_unavailable; the source then stays idle and the node goes on with its explicit seeds.
class Seeds {
  public:
	using Clock = std::chrono::steady_clock;

	Seeds(asio::any_io_executor executor, kronuz::consensus::Identity cluster, std::vector<SeedService> services,
		  std::set<std::string> wanted, SeedOptions options = {})
		: options_(std::move(options)), receiver_(executor), sender_(executor), timer_(executor),
		  wanted_(std::move(wanted)), random_(std::random_device{}()) {
		validate_seed_options(options_);
		self_.cluster = cluster;
		for (auto &c : self_.sender)
			c = char(random_());
		self_.services = std::move(services);
		if (self_.cluster == kronuz::consensus::Identity{})
			throw std::invalid_argument("seeds: zero cluster");
		if (!self_.services.empty())
			encode_announcement(self_);  // throws on an invalid service
		for (const auto &name : wanted_)
			if (auto fault = seed_service_fault({name, 1}))
				throw std::invalid_argument(std::string("seeds: wanted service ") + fault);
		if (self_.services.empty() && wanted_.empty())
			throw std::invalid_argument("seeds: nothing to announce or seed");
	}
	Seeds(const Seeds &) = delete;
	Seeds &operator=(const Seeds &) = delete;
	~Seeds() { assert(drained() && "Seeds destroyed with operations pending: stop() and run the loop until drained()"); }

	// Called once, when multicast cannot be used here; the reason names the failing step.
	std::function<void(const std::string &reason)> on_unavailable;
	// A seed to dial: a new one, or one whose backoff passed and which is still announced. The
	// caller answers each with failed(id) or joined(id); none comes for a seed until then.
	std::function<void(const Seed &)> on_dial;

	const SeedStats &stats() const noexcept { return stats_; }
	const std::string &unavailable() const noexcept { return unavailable_; }
	const kronuz::consensus::Identity &sender() const noexcept { return self_.sender; }
	int sending_handle() noexcept { return sender_.is_open() ? int(sender_.native_handle()) : -1; }
	bool drained() const noexcept { return !pending_; }

	// Opens the sockets, joins the group, and starts announcing and listening.
	void start() {
		if (started_)
			return;
		started_ = true;
		asio::error_code error;
		const auto group = asio::ip::make_address(options_.group);
		const asio::ip::udp::endpoint to(group, options_.port);
		const char *step = "open";
		if (!error)
			receiver_.open(to.protocol(), error), step = "open";
		if (!error)
			receiver_.set_option(asio::socket_base::reuse_address(true), error), step = "reuse address";
#ifdef SO_REUSEPORT
		if (!error) {
			const int one = 1;
			if (::setsockopt(receiver_.native_handle(), SOL_SOCKET, SO_REUSEPORT, &one, sizeof one))
				error = asio::error_code(errno, asio::system_category()), step = "reuse port";
		}
#endif
		if (!error)
			receiver_.bind(to, error), step = "bind";
		if (!error && !wanted_.empty())
			join(group, error), step = "join";
		if (!error)
			sender_.open(to.protocol(), error), step = "open sender";
		if (!error)
			outbound(group, error), step = "interface";
		if (!error)
			sender_.set_option(asio::ip::multicast::hops(options_.ttl), error), step = "ttl";
		if (!error)
			sender_.set_option(asio::ip::multicast::enable_loopback(true), error), step = "loopback";
		if (error)
			return fail(std::string(step) + ": " + error.message());
		group_ = to;
		if (!wanted_.empty())
			receive();
		if (!self_.services.empty()) {
			datagram_ = encode_announcement(self_);
			burst_left_ = options_.burst;
			announce();
		}
	}
	// Closes the sockets and cancels the timer; run the loop until drained() before destroying.
	void stop() noexcept {
		stopping_ = true;
		asio::error_code ignored;
		timer_.cancel();
		receiver_.close(ignored);
		sender_.close(ignored);
	}

	// The seeds heard within `expire`, ordered by address and sender.
	std::vector<Seed> seeds() const {
		std::vector<Seed> r;
		const auto now = Clock::now();
		for (const auto &[key, e] : entries_)
			if (now - e.heard < options_.expire)
				r.push_back(e.seed);
		return r;
	}
	// The caller's dial of a seed failed (no connection, a failed TLS hello, an unpinned node), or
	// the node it joined was lost: its next dial waits backoff, doubled per failure since its last join.
	void failed(std::uint64_t id) {
		auto *e = find(id);
		if (!e || e->state == State::idle)
			return;
		++stats_.failures;
		e->state = State::idle;
		auto wait = options_.backoff;
		for (std::uint32_t i = 0; i < e->failures && wait < options_.backoff_max; ++i)
			wait *= 2;
		++e->failures;
		e->next = Clock::now() + std::min(wait, options_.backoff_max);
	}
	// The caller's dial of a seed authenticated a node: no further dials unless failed() says it was lost.
	void joined(std::uint64_t id) {
		if (auto *e = find(id); e && e->state == State::dialing) {
			e->state = State::joined;
			e->failures = 0;
		}
	}

  private:
	enum class State : std::uint8_t { idle, dialing, joined };
	struct Entry {
		Seed seed;
		Clock::time_point heard, next;
		std::uint32_t failures = 0;
		State state = State::idle;
	};
	using Key = std::pair<asio::ip::address, kronuz::consensus::Identity>;

	struct Pending {
		Seeds &self;
		explicit Pending(Seeds &s) : self(s) { ++self.pending_; }
		Pending(const Pending &o) noexcept : self(o.self) { ++self.pending_; }
		~Pending() { --self.pending_; }
	};

	void fail(std::string reason) {
		asio::error_code ignored;
		receiver_.close(ignored);
		sender_.close(ignored);
		report(std::move(reason));
	}
	void report(std::string reason) {
		if (!unavailable_.empty())
			return;
		unavailable_ = std::move(reason);
		if (on_unavailable)
			on_unavailable(unavailable_);
	}
	void join(const asio::ip::address &group, asio::error_code &error) {
		if (group.is_v4()) {
			auto local = asio::ip::address_v4::any();
			if (!options_.interface.empty())
				local = asio::ip::make_address_v4(options_.interface, error);
			if (!error)
				receiver_.set_option(asio::ip::multicast::join_group(group.to_v4(), local), error);
		} else {
			receiver_.set_option(asio::ip::multicast::join_group(group.to_v6(), index(error)), error);
		}
	}
	void outbound(const asio::ip::address &group, asio::error_code &error) {
		if (options_.interface.empty())
			return;
		if (group.is_v4()) {
			const auto local = asio::ip::make_address_v4(options_.interface, error);
			if (!error)
				sender_.set_option(asio::ip::multicast::outbound_interface(local), error);
		} else if (const auto i = index(error); !error) {
			sender_.set_option(asio::ip::multicast::outbound_interface(i), error);
		}
	}
	unsigned index(asio::error_code &error) const {
		if (options_.interface.empty())
			return 0;
		const unsigned i = ::if_nametoindex(options_.interface.c_str());
		if (!i)
			error = asio::error::no_such_device;
		return i;
	}

	void announce() {
		if (stopping_)
			return;
		Pending pending(*this);
		sender_.async_send_to(asio::buffer(datagram_), group_, [this, pending](asio::error_code error, std::size_t) {
			if (stopping_)
				return;
			if (error) {
				++stats_.send_failures;
				report("send: " + error.message());
			} else {
				++stats_.announced;
			}
			Clock::duration wait = options_.burst_spacing;
			if (burst_left_ > 1) {
				--burst_left_;
			} else {
				burst_left_ = 0;
				const auto jitter = std::uniform_int_distribution<std::int64_t>(0, options_.jitter.count())(random_);
				wait = options_.interval + std::chrono::milliseconds(jitter);
			}
			sweep();
			timer_.expires_after(wait);
			timer_.async_wait([this, pending](asio::error_code e) {
				if (!e)
					announce();
			});
		});
	}

	void receive() {
		if (stopping_)
			return;
		Pending pending(*this);
		receiver_.async_receive_from(asio::buffer(buffer_), from_, [this, pending](asio::error_code error, std::size_t n) {
			if (stopping_)
				return;
			if (!error)
				received(std::string_view(buffer_.data(), n));
			else if (error == asio::error::operation_aborted || !receiver_.is_open())
				return;
			receive();
		});
	}

	void received(std::string_view bytes) {
		++stats_.received;
		Announcement a;
		switch (decode_announcement(bytes, a)) {
		case SeedFault::oversized: ++stats_.oversized; return;
		case SeedFault::malformed: ++stats_.malformed; return;
		case SeedFault::version: ++stats_.version; return;
		case SeedFault::none: break;
		}
		if (a.cluster != self_.cluster)
			return void(++stats_.cluster);
		if (a.sender == self_.sender)
			return void(++stats_.own);
		std::vector<SeedService> services;
		for (auto &s : a.services)
			if (wanted_.contains(s.name))
				services.push_back(std::move(s));
		if (services.empty())
			return void(++stats_.unwanted);
		const auto now = Clock::now();
		sweep();
		const Key key{from_.address(), a.sender};
		auto it = entries_.find(key);
		if (it == entries_.end()) {
			if (entries_.size() >= options_.seeds)
				return void(++stats_.full);
			it = entries_.emplace(key, Entry{Seed{++ids_, a.sender, from_.address(), {}}, now, now}).first;
			index_[it->second.seed.id] = key;
		}
		auto &e = it->second;
		e.heard = now;
		e.seed.services = std::move(services);
		if (e.state == State::idle && now >= e.next && on_dial) {
			e.state = State::dialing;
			++stats_.dials;
			on_dial(e.seed);  // may call failed() or joined() at once
		}
	}

	// Drops seeds not heard within `expire`, except one being dialed or still backing off, so an
	// announcement that comes back resumes the backoff instead of starting a fresh one.
	void sweep() {
		const auto now = Clock::now();
		for (auto it = entries_.begin(); it != entries_.end();) {
			const auto &e = it->second;
			if (now - e.heard >= options_.expire && e.state != State::dialing && now >= e.next) {
				++stats_.expired;
				index_.erase(e.seed.id);
				it = entries_.erase(it);
			} else {
				++it;
			}
		}
	}
	Entry *find(std::uint64_t id) {
		const auto i = index_.find(id);
		return i == index_.end() ? nullptr : &entries_.at(i->second);
	}

	SeedOptions options_;
	asio::ip::udp::socket receiver_, sender_;
	asio::steady_timer timer_;
	asio::ip::udp::endpoint group_, from_;
	Announcement self_;
	std::set<std::string> wanted_;
	std::minstd_rand random_;
	std::string datagram_, unavailable_;
	std::array<char, seed_datagram_limit + 1> buffer_{};  // one byte over the limit reads as oversized
	std::map<Key, Entry> entries_;
	std::map<std::uint64_t, Key> index_;
	std::uint64_t ids_ = 0;
	std::size_t burst_left_ = 0, pending_ = 0;
	bool started_ = false, stopping_ = false;
	SeedStats stats_;
};
} // namespace kronuz::consensus::transport
