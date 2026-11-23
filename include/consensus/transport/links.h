#pragma once
#include "admission_join.h"
#include "connection.h"
#include "resolve.h"
#include <algorithm>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <atomic>
#include <map>
#include <tuple>

// A host's connections on one loop, at most one per peer node and lane, and how they start
// (docs/mechanisms/host-connection.md): a dial resolves the peer's name and races its addresses
// (RFC 8305), an accept takes any pinned node, and the DTHL0003 hellos settle duplicate dials
// before any frame. Nodes are routed and removed while it runs, and an accept whose first byte is
// the join magic goes to the join service (docs/mechanisms/admission.md).
// Everything runs on the loop.
namespace kronuz::consensus::transport {
struct LinkOptions {
	ConnectionOptions connection;
	std::chrono::milliseconds dial{5000};  // the TCP connect race
	// A name with no cached address waits for its lookup this long, which also spaces lookups.
	std::chrono::milliseconds resolve_deadline{1000}, stagger{250};
	PeerResolver resolver = system_resolver;  // on a thread that may outlive Links: capture only what it owns
	// Incoming connections before authentication: at most `handshakes`, and `handshakes_per_source`
	// from one address (a node dials each lane once at a time); a newcomer replaces the oldest,
	// and one from an unknown address never replaces one from a peer's (N9).
	std::size_t handshakes = 4, handshakes_per_source = 2;
	std::uint32_t rpc_min = 1, rpc_max = 1;
	std::uint32_t wire_min = wire_version, wire_max = wire_version;
	kronuz::consensus::Limits limits{};  // the node's own, in its hellos (docs/mechanisms/hello.md)
	// This process's boot (M7) and the directory version that admitted it (0 before), in its
	// hellos; Links::boot() changes them while it runs. A zero boot is none.
	kronuz::consensus::Identity boot{};
	std::uint64_t directory = 0;
	// Each end's check of the peer's hello against the directory's admitted boot, on `loop`:
	// 0 admits it, any other value refuses the connection with that code, which an acceptor's
	// reply carries to the dialer. Asynchronous, so it may fetch the directory first when behind
	// the hello's version, within the handshake deadline. None admits every boot.
	std::function<task::task<std::uint32_t>(const NodeHello &peer, task::scheduler_ref loop)> check_boot;
	// On the dialer's loop: the peer's acceptor refused this node's boot with `reason`.
	std::function<void(kronuz::consensus::NodeId peer, std::uint32_t reason)> boot_refused;
};
struct LinkStats {
	std::uint64_t established = 0, refused = 0, abandoned = 0, rejected = 0, evicted = 0;
	std::uint64_t version_refused = 0;  // hellos whose wire versions shared none with this node's
	std::uint64_t unverified = 0;       // links a reload closed: their peer's chain no longer verifies
	std::uint64_t joins = 0;            // accepts whose first byte was the join magic's
	std::uint64_t boot_refused = 0;     // hellos whose boot a check here, or the peer's, refused
	std::uint64_t lookups = 0, lookup_failures = 0, lookup_timeouts = 0;
};
// An established connection and the hellos that made it.
struct Link {
	kronuz::consensus::NodeId peer = 0;
	LinkLane lane = LinkLane::Control;
	std::shared_ptr<Connection> connection;
	NodeHello local, remote;
	bool dialed = false;
	std::uint32_t rpc = 0;  // the RPC lane's negotiated version, 0 when off (T1)
};

class Links {
  public:
	using FrameHandler = std::function<void(const Link &, std::string_view)>;
	using LinkHandler = std::function<void(const Link &)>;

	Links(asio::any_io_executor executor, task::scheduler_ref loop, std::shared_ptr<asio::ssl::context> tls,
		  NodePins pins, kronuz::consensus::Identity cluster, std::map<kronuz::consensus::NodeId, PeerAddress> addresses,
		  LinkOptions options = {})
		: executor_(std::move(executor)), loop_(loop), tls_(std::move(tls)), pins_(std::move(pins)), cluster_(cluster),
		  options_(std::move(options)), state_(std::make_shared<Shared>()) {
		if (!options_.handshakes || !options_.handshakes_per_source)
			throw std::invalid_argument("links: handshakes");
		if (tls_)
			cache_certificates(*tls_);  // before any loop handshakes with it: a host's loops share it
		for (auto &[node, address] : addresses)
			route(node, address);
	}
	Links(const Links &) = delete;
	Links &operator=(const Links &) = delete;
	~Links() {
		assert(!tasks_ && "Links destroyed with tasks running: stop() and run the loop until drained()");
		for (auto &[node, route] : state_->routes)
			if (route.lookup)
				route.lookup->abandon();  // a cached name's refresh may still run: it must not post
	}

	// What established links deliver, and when a link starts and ends.
	FrameHandler on_frame;
	LinkHandler on_established, on_ended;
	// The Links that keeps `peer`'s connections when a host runs one per loop; null keeps them here.
	// An accept authenticated here moves to it for its hellos (Connection::rehome).
	std::function<Links *(kronuz::consensus::NodeId)> owner;
	// Before serve(): the joins this host serves on its port, or none (every accept is a node's).
	std::shared_ptr<JoinService> joins;

	const LinkStats &stats() const noexcept { return stats_; }
	// The pins every connection authenticates against, shared with every copy (node_pins.h).
	NodePins &pins() noexcept { return pins_; }
	bool routed(kronuz::consensus::NodeId peer) const noexcept { return state_->routes.contains(peer); }
	// Dials to `peer` go to `address` from now on; a lookup of its old name is abandoned.
	void route(kronuz::consensus::NodeId peer, PeerAddress address) {
		auto &route = state_->routes[peer];
		if (route.lookup)
			route.lookup->abandon();
		route.lookup.reset();
		asio::error_code error;
		const auto literal = asio::ip::make_address(address.host, error);
		route.endpoints = error ? std::vector<asio::ip::tcp::endpoint>{} : interleave({literal}, address.port);
		route.literal = !error;
		route.address = std::move(address);
		route.answer_by = 0;
		++route.serial;
		route.waiting.wake_all();
	}
	// Removes `peer` (NodePins::remove): its route goes, its links and its dial's hellos abort, and
	// a connection of it that began authenticating before now never becomes a link.
	void remove(kronuz::consensus::NodeId peer) {
		pins_.remove(peer);
		if (const auto it = state_->routes.find(peer); it != state_->routes.end()) {
			if (it->second.lookup)
				it->second.lookup->abandon();
			it->second.waiting.wake_all();
			state_->routes.erase(it);
		}
		for (auto lane : {LinkLane::Control, LinkLane::Bulk})
			if (const auto it = slots_.find({peer, lane}); it != slots_.end())
				for (const auto *link : {it->second.link.get(), it->second.offered.get()})
					if (link)
						link->connection->abort();
	}
	// Takes `tls` for new connections (HostTransport::reload, after NodePins::reload) and aborts
	// every link, established or offered, whose peer's chain no longer verifies.
	void reload(std::shared_ptr<asio::ssl::context> tls) {
		tls_ = std::move(tls);
		for (auto &[key, slot] : slots_)
			for (const auto *link : {slot.link.get(), slot.offered.get()})
				if (link && !pins_.verifies(link->connection->native_handle())) {
					++stats_.unverified;
					link->connection->abort();
				}
	}
	// The boot and directory version this host's hellos carry from now on.
	void boot(kronuz::consensus::Identity id, std::uint64_t directory) {
		options_.boot = id;
		options_.directory = directory;
	}
	bool drained() const noexcept { return !tasks_ && !incoming_.load(std::memory_order_acquire); }
	// The established, undrained link to `peer` on `lane`, or null.
	const Link *find(kronuz::consensus::NodeId peer, LinkLane lane) const noexcept {
		const auto it = slots_.find({peer, lane});
		return it != slots_.end() && it->second.link && !it->second.link->connection->draining() ? &*it->second.link
																							   : nullptr;
	}

	// Dials `lane` to `peer` unless a link or a dial of it exists: true once established, false
	// when the peer's own dial won the tie-break, the dial failed, or this host is stopping.
	task::task<bool> dial(kronuz::consensus::NodeId peer, LinkLane lane) {
		auto connection = co_await connect(peer, lane);
		if (!connection)
			co_return false;
		co_return co_await offer(peer, lane, std::move(connection));
	}
	// The first half of a dial: the name, the TCP race and the TLS handshake to `peer`; null when
	// a link or another dial of the lane exists, or it failed.
	task::task<std::shared_ptr<Connection>> connect(kronuz::consensus::NodeId peer, LinkLane lane) {
		auto &slot = slots_[{peer, lane}];
		if (stopping_ || !state_->routes.contains(peer) || slot.dialing || slot.offered || find(peer, lane))
			co_return nullptr;
		Task running(*this);
		slot.dialing = true;
		const auto since = pins_.epoch();  // before the handshake authenticates against the pins
		std::shared_ptr<Connection> connection;
		try {
			auto socket = co_await race(peer);
			if (socket && !stopping_) {
				connection = std::make_shared<Connection>(loop_, std::move(*socket), tls_, true, options_.connection);
				live(connection, since);
				late_deadline deadline(loop_, options_.connection.handshake);
				co_await connection->handshake(pins_, peer, deadline.token());
			}
		} catch (...) {
			forget(connection);
			connection = nullptr;
		}
		slots_[{peer, lane}].dialing = false;
		if (stopping_)
			forget(connection);
		co_return stopping_ ? nullptr : connection;
	}
	// The second half: the hellos. The dialer's hello counts in the tie-break from here.
	task::task<bool> offer(kronuz::consensus::NodeId peer, LinkLane lane, std::shared_ptr<Connection> connection) {
		Task running(*this);
		auto &slot = slots_[{peer, lane}];
		if (stopping_ || find(peer, lane) || slot.offered) {
			++stats_.abandoned;  // an incoming connection won while this one handshook
			connection->abort();
			forget(connection);
			co_return false;
		}
		auto link = std::make_shared<Link>(Link{peer, lane, connection, hello(lane), {}, true, 0});
		slot.offered = link;
		bool established = false;
		try {
			late_deadline deadline(loop_, options_.connection.handshake);
			co_await connection->write_hello(link->local, deadline.token());
			link->remote = co_await connection->read_hello(deadline.token());
			check_node_hello(link->remote, link->local, peer, true);
			std::uint32_t reason = 0;
			if (!link->remote.refused && options_.check_boot)
				reason = co_await options_.check_boot(link->remote, loop_);  // the acceptor's boot, as every end checks its peer's
			auto &now = slots_[{peer, lane}];
			if (link->remote.reason || reason) {
				++stats_.boot_refused;
				if (link->remote.reason && options_.boot_refused)
					options_.boot_refused(peer, link->remote.reason);
			} else if (link->remote.refused || now.offered != link) {
				++stats_.refused;  // the peer's dial won: its link arrives as an accept
			} else {
				now.offered = nullptr;
				established = reserve(link);
				if (established)
					start(std::move(link));
			}
		} catch (const WireVersionRefused &) {
			++stats_.version_refused;
		} catch (...) {
		}
		auto &now = slots_[{peer, lane}];
		if (now.offered && now.offered->connection == connection)
			now.offered = nullptr;
		if (!established) {
			connection->abort();
			forget(connection);
		}
		co_return established;
	}
	// Takes one accepted socket: the TLS handshake against every pin, then adopt() here or on the
	// peer's owner; false when refused, failed or moved.
	task::task<bool> accept(asio::ip::tcp::socket socket) {
		asio::error_code error;
		const auto source = socket.remote_endpoint(error).address();
		if (stopping_ || error) {
			++stats_.rejected;
			co_return false;
		}
		Task running(*this);
		late_deadline deadline(loop_, options_.connection.handshake);  // the first byte, TLS and the pins
		if (joins) {
			// In the handshakes' pool until its first byte: the join magic's leaves it for the
			// join service's pool; a TLS record's goes on as a node's, in the same place.
			socket.non_blocking(true, error);
			admit(source, &socket, [&socket] {
				asio::error_code ignored;
				socket.shutdown(asio::socket_base::shutdown_both, ignored);
				socket.cancel(ignored);
			});
			const int first = co_await first_byte(socket, deadline.token());
			authenticated(&socket);
			if (first < 0 || stopping_) {
				++stats_.rejected;
				co_return false;
			}
			if (char(first) == join_magic[0]) {
				++stats_.joins;
				co_await joins->serve(std::move(socket), loop_, stop_.token());
				co_return false;
			}
		}
		auto connection = std::make_shared<Connection>(loop_, std::move(socket), tls_, false, options_.connection);
		const auto since = pins_.epoch();
		live(connection, since);
		admit(source, connection.get(), [connection] { connection->abort(); });
		kronuz::consensus::NodeId peer = 0;
		try {
			peer = co_await connection->handshake(pins_, 0, deadline.token());
		} catch (...) {
		}
		authenticated(connection.get());
		if (!peer || stopping_) {
			connection->abort();
			forget(connection);
			co_return false;
		}
		Links *home = owner ? owner(peer) : nullptr;
		if (!home || home == this)
			co_return co_await adopt(peer, std::move(connection));
		forget(connection);
		connection->rehome(home->loop_, home->executor_);
		home->incoming_.fetch_add(1, std::memory_order_acq_rel);
		asio::post(home->executor_, [home, peer, since, connection = std::move(connection)]() mutable {
			task::spawn(home->loop_, home->adopt(peer, std::move(connection), since), [](std::exception_ptr, std::optional<bool>) {});
		});
		co_return false;
	}
	// The accepted side once `peer` is authenticated: the dialer's hello, the tie-break, the reply.
	// `moved`: the connection came from another loop's accept, which began authenticating it at
	// that pins epoch.
	task::task<bool> adopt(kronuz::consensus::NodeId peer, std::shared_ptr<Connection> connection,
						   std::optional<std::uint64_t> moved = std::nullopt) {
		Task running(*this);
		if (moved) {
			live(connection, *moved);
			incoming_.fetch_sub(1, std::memory_order_acq_rel);
		}
		std::shared_ptr<Link> reserved;
		bool established = false;
		try {
			if (stopping_)
				throw std::runtime_error("links: stopping");
			late_deadline deadline(loop_, options_.connection.handshake);
			auto remote = co_await connection->read_hello(deadline.token());
			auto link = std::make_shared<Link>(Link{peer, remote.lane, connection, hello(remote.lane), remote, false, 0});
			check_node_hello(remote, link->local, peer, false);
			std::uint32_t reason = 0;
			if (options_.check_boot)
				reason = co_await options_.check_boot(remote, loop_);  // the dialer's boot against the directory's (M7)
			if (!current(*link))
				throw std::runtime_error("links: the peer was removed");
			link->local.reason = reason;
			link->local.refused = reason || !wins(peer, remote.lane);
			if (reason) {
				++stats_.boot_refused;
			} else if (link->local.refused) {
				++stats_.refused;
			} else {
				slots_[{peer, remote.lane}].offered = nullptr;  // this host's own dial lost, if any
				if (reserve(link))  // before the reply, so nothing else takes the slot
					reserved = link;
			}
			co_await connection->write_hello(link->local, deadline.token());
			if (reserved && !stopping_) {
				start(reserved);
				established = true;
			}
		} catch (const WireVersionRefused &) {
			++stats_.version_refused;
		} catch (...) {
		}
		if (!established) {
			if (reserved)
				release(*reserved);
			connection->abort();
			forget(connection);
		}
		co_return established;
	}
	// Accepts on `acceptor` until stop(); a host may serve several (an IPv4 and an IPv6 address).
	task::task<void> serve(asio::ip::tcp::acceptor &acceptor) {
		Task running(*this);
		acceptors_.push_back(&acceptor);
		while (!stopping_) {
			auto [error, socket] = co_await task::await_asio<void(asio::error_code, asio::ip::tcp::socket)>(
				[&](auto h) { acceptor.async_accept(std::move(h)); });
			if (error || stopping_ || !socket)
				continue;
			task::spawn(loop_, accept(std::move(*socket)), [](std::exception_ptr, std::optional<bool>) {});
		}
		std::erase(acceptors_, &acceptor);
	}
	// Starts draining the link: its queued frames, then close_notify. It stops counting at once.
	void retire(kronuz::consensus::NodeId peer, LinkLane lane) {
		if (auto *link = find(peer, lane))
			link->connection->close();
	}
	// Stops accepting and dialing, abandons lookups, and aborts every connection; the loop
	// then runs until drained().
	void stop() noexcept {
		stopping_ = true;
		stop_.request();  // the joins being served
		std::vector<std::function<void()>> pending;
		for (auto &h : handshaking_)
			pending.push_back(h.abort);
		for (auto &abort : pending)
			abort();
		asio::error_code ignored;
		for (auto *acceptor : acceptors_)
			acceptor->close(ignored);
		for (auto &[node, route] : state_->routes) {
			if (route.lookup)
				route.lookup->abandon();
			route.waiting.wake_all();
		}
		for (auto &race : races_)
			race->stop();
		for (auto &connection : live_)
			connection->abort();
	}

  private:
	struct Route {
		PeerAddress address;
		bool literal = false;
		std::vector<asio::ip::tcp::endpoint> endpoints;
		std::shared_ptr<Lookup> lookup;
		std::uint64_t answer_by = 0;  // the last lookup's deadline, on the loop's clock
		std::uint64_t serial = 0;     // raised by each route(): an older lookup's answer is stale
		wait_list waiting;
	};
	// What a lookup's thread reaches through a weak reference.
	struct Shared {
		std::map<kronuz::consensus::NodeId, Route> routes;
	};
	struct Slot {
		bool dialing = false;
		std::shared_ptr<Link> offered;  // this host's dial whose hello is out: it counts in the tie-break
		std::shared_ptr<Link> link;
	};
	// One TCP connect race (RFC 8305): each attempt's completion wakes the loop's wait.
	struct Race {
		std::vector<std::shared_ptr<asio::ip::tcp::socket>> sockets;
		std::optional<std::size_t> winner;
		std::size_t pending = 0;
		wait_list waiting;
		void stop() noexcept {
			asio::error_code ignored;
			for (auto &s : sockets)
				s->close(ignored);
			waiting.wake_all();
		}
	};
	// An incoming connection not yet authenticated, oldest first.
	struct Handshake {
		asio::ip::address source;
		const void *key = nullptr;    // the connection, or an accepted socket before its first byte
		std::function<void()> abort;  // ends it: its handshake or its wait fails
		bool peer = false;            // from an address a route names
	};
	struct Task {
		Links &links;
		explicit Task(Links &l) : links(l) { ++links.tasks_; }
		~Task() { --links.tasks_; }
	};

	NodeHello hello(LinkLane lane) const {
		return {pins_.local(), lane, false, cluster_, fresh_connection_identity(), options_.rpc_min, options_.rpc_max,
				options_.wire_min, options_.wire_max, options_.limits, options_.boot, options_.directory, 0};
	}
	// The tie-break (T6) for an incoming hello from `peer`: an undrained link of the lane wins;
	// else, when this host's own dial already offered its hello, the dial of the lower node wins.
	bool wins(kronuz::consensus::NodeId peer, LinkLane lane) const {
		if (find(peer, lane))
			return false;
		const auto it = slots_.find({peer, lane});
		const bool offered = it != slots_.end() && it->second.offered;
		return !offered || peer < pins_.local();
	}
	// Whether the link's peer's chain verifies against the CAs trusted now, and the peer is pinned
	// and was not removed since its connection began authenticating.
	bool current(const Link &link) const {
		if (!pins_.verifies(link.connection->native_handle()))
			return false;
		const auto it = since_.find(link.connection.get());
		return it != since_.end() && pins_.current(link.peer, it->second);
	}
	// Makes `link` the lane's link: false when stopping, or its peer is not current().
	bool reserve(const std::shared_ptr<Link> &link) {
		if (stopping_ || !current(*link))
			return false;
		link->rpc = negotiate_rpc(link->local, link->remote);
		slots_[{link->peer, link->lane}].link = link;
		return true;
	}
	void release(const Link &link) {
		auto &slot = slots_[{link.peer, link.lane}];
		if (slot.link && slot.link.get() == &link)
			slot.link = nullptr;
	}
	// Runs a reserved link once both hellos are through: frames may flow from here.
	void start(std::shared_ptr<Link> link) {
		++stats_.established;
		if (on_established)
			on_established(*link);
		task::spawn(loop_, run(std::move(link)), [](std::exception_ptr) {});
	}
	task::task<void> run(std::shared_ptr<Link> link) {
		Task running(*this);
		co_await link->connection->run([this, link = link.get()](std::string_view frame) {
			if (on_frame)
				on_frame(*link, frame);
		});
		release(*link);
		forget(link->connection);
		if (on_ended)
			on_ended(*link);
	}
	void live(const std::shared_ptr<Connection> &connection, std::uint64_t since) {
		live_.push_back(connection);
		since_[connection.get()] = since;
	}
	void forget(const std::shared_ptr<Connection> &connection) {
		if (connection) {
			std::erase(live_, connection);
			since_.erase(connection.get());
		}
	}
	// Makes room for an incoming connection before it authenticates: when its source is full,
	// its oldest ends at once; when the pool is, its oldest from an unknown address, else, for a
	// newcomer from a peer's address, its oldest, else the newcomer (host-connection.md, Before
	// authentication).
	void admit(asio::ip::address source, const void *key, std::function<void()> abort) {
		if (source.is_v6() && source.to_v6().is_v4_mapped())
			source = asio::ip::make_address_v4(asio::ip::v4_mapped, source.to_v6());
		const bool peer = routed(source);
		const auto from = [&](const Handshake &h) { return h.source == source; };
		auto drop = handshaking_.end();
		if (std::size_t(std::count_if(handshaking_.begin(), handshaking_.end(), from)) >= options_.handshakes_per_source)
			drop = std::find_if(handshaking_.begin(), handshaking_.end(), from);
		else if (handshaking_.size() >= options_.handshakes) {
			drop = std::find_if(handshaking_.begin(), handshaking_.end(), [](const Handshake &h) { return !h.peer; });
			if (drop == handshaking_.end() && peer)
				drop = handshaking_.begin();
		}
		if (drop == handshaking_.end() && handshaking_.size() >= options_.handshakes) {
			++stats_.evicted;
			abort();
			return;
		}
		if (drop != handshaking_.end()) {
			++stats_.evicted;
			const auto evicted = std::move(drop->abort);
			handshaking_.erase(drop);
			evicted();
		}
		handshaking_.push_back({source, key, std::move(abort), peer});
	}
	// Whether a route names `source`: a literal address, or a name's cached answer.
	bool routed(const asio::ip::address &source) const {
		for (auto &[node, route] : state_->routes)
			for (auto &e : route.endpoints)
				if (e.address() == source)
					return true;
		return false;
	}
	void authenticated(const void *key) {
		std::erase_if(handshaking_, [&](const Handshake &h) { return h.key == key; });
	}
	// The first byte an accepted socket sends, peeked within `deadline`: -1 when none came.
	static task::task<int> first_byte(asio::ip::tcp::socket &socket, task::cancel_token deadline) {
		for (;;) {
			unsigned char byte = 0;
			const auto n = ::recv(socket.native_handle(), &byte, 1, MSG_PEEK);
			if (n == 1)
				co_return byte;
			if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR))
				co_return -1;
			const auto ec = co_await task::await_asio<void(asio::error_code)>(
				[&](auto h) { socket.async_wait(asio::socket_base::wait_read, std::move(h)); }, deadline);
			if (ec || deadline.requested())
				co_return -1;
		}
	}

	// The peer's endpoints: a literal's, the cache's (a lookup refreshes it behind the dial), or
	// a name's first answer, waited for until the lookup's deadline.
	task::task<std::vector<asio::ip::tcp::endpoint>> resolve(kronuz::consensus::NodeId peer) {
		auto &route = state_->routes.at(peer);
		if (route.literal)
			co_return route.endpoints;
		const auto now = loop_.now();
		if (!route.lookup && route.answer_by <= now) {
			++stats_.lookups;
			const auto window = std::chrono::nanoseconds(options_.resolve_deadline).count();
			route.answer_by = now + std::uint64_t(window);
			std::weak_ptr<Shared> weak = state_;
			try {
				route.lookup = Lookup::start(options_.resolver, route.address.host,
											 [executor = executor_, weak, peer, serial = route.serial, this](auto found) {
												 asio::post(executor, [weak, peer, serial, this, found = std::move(found)]() mutable {
													 if (auto shared = weak.lock())
														 resolved(*shared, peer, serial, std::move(found));
												 });
											 });
			} catch (const std::system_error &) {
				++stats_.lookup_failures;
				co_return route.endpoints;
			}
		}
		if (!route.endpoints.empty() || !route.lookup)
			co_return route.endpoints;
		if (route.answer_by <= now) {
			++stats_.lookup_timeouts;
			co_return route.endpoints;
		}
		late_deadline answer(loop_, std::chrono::nanoseconds(route.answer_by - now));
		answer.join(route.waiting);
		if (co_await answer.wait() == late_deadline::outcome::expired)
			++stats_.lookup_timeouts;
		co_return state_->routes.at(peer).endpoints;
	}
	// A lookup's answer, on the loop: a changed set replaces the cache; an empty one keeps it. An
	// answer for a route since removed or moved is dropped.
	void resolved(Shared &shared, kronuz::consensus::NodeId peer, std::uint64_t serial, std::vector<asio::ip::address> found) {
		const auto it = shared.routes.find(peer);
		if (it == shared.routes.end() || it->second.serial != serial)
			return;
		auto &route = it->second;
		route.lookup.reset();
		if (found.empty())
			++stats_.lookup_failures;
		else
			route.endpoints = interleave(found, route.address.port);
		route.waiting.wake_all();
	}
	// Connects to the peer's endpoints in turn, starting the next a stagger after the one before
	// or once every started attempt failed, all within one dial deadline; the first wins.
	task::task<std::optional<asio::ip::tcp::socket>> race(kronuz::consensus::NodeId peer) {
		const auto endpoints = co_await resolve(peer);
		auto race = std::make_shared<Race>();
		races_.push_back(race);
		struct Forget {
			Links &links;
			std::shared_ptr<Race> race;
			~Forget() { std::erase(links.races_, race); }
		} forget{*this, race};
		late_deadline total(loop_, options_.dial);
		while (!stopping_ && !race->winner && total.state() == late_deadline::outcome::pending) {
			const auto started = race->sockets.size();
			if (started < endpoints.size()) {
				auto socket = std::make_shared<asio::ip::tcp::socket>(executor_);
				race->sockets.push_back(socket);
				++race->pending;
				socket->async_connect(endpoints[started], [race, started](asio::error_code error) {
					--race->pending;
					if (!error && !race->winner)
						race->winner = started;
					race->waiting.wake_all();
				});
			} else if (!race->pending) {
				break;  // every address failed
			}
			late_deadline step(loop_, started + 1 < endpoints.size() ? options_.stagger : options_.dial);
			step.join(race->waiting);
			task::cancel_callback expired(total.token(), [&step] { step.wake(); });
			co_await step.wait();
		}
		asio::error_code ignored;
		for (std::size_t i = 0; i != race->sockets.size(); ++i)
			if (i != race->winner)
				race->sockets[i]->close(ignored);
		if (stopping_ || !race->winner)
			co_return std::nullopt;
		co_return std::move(*race->sockets[*race->winner]);
	}

	asio::any_io_executor executor_;
	task::scheduler_ref loop_;
	std::shared_ptr<asio::ssl::context> tls_;
	NodePins pins_;
	kronuz::consensus::Identity cluster_;
	LinkOptions options_;
	std::shared_ptr<Shared> state_;
	std::map<std::pair<kronuz::consensus::NodeId, LinkLane>, Slot> slots_;
	std::vector<std::shared_ptr<Connection>> live_;
	std::map<const Connection *, std::uint64_t> since_;  // the pins epoch each live connection began authenticating at
	std::vector<std::shared_ptr<Race>> races_;
	task::cancel_source stop_;  // requested by stop(): the joins being served end
	std::vector<Handshake> handshaking_;
	std::vector<asio::ip::tcp::acceptor *> acceptors_;  // serve()'s, one per listening address
	LinkStats stats_;
	std::size_t tasks_ = 0;
	std::atomic<std::size_t> incoming_{0};  // accepts moving here from another loop's Links
	bool stopping_ = false;
};
} // namespace kronuz::consensus::transport
