#pragma once
#include "codec.h"
#include "fetch.h"
#include "hello.h"
#include "resolve.h"
#include "consensus/snapshot_channel.h"
#include "consensus/worker.h"
#include "rpc_observation.h"
#include "snapshot_write.h"
#include <deque>
#include <metrics.hh>

namespace kronuz::consensus::transport {
// The group's identity and limits as the transport needs them. profile is the
// fingerprint both ends compare in their hellos (see profile_fingerprint).
struct PeerConfiguration {
	kronuz::consensus::FixedConfiguration configuration;
	kronuz::consensus::Limits consensus;
	kronuz::consensus::Identity cluster{}, group{}, epoch{};
	Fingerprint profile{};
};
// What the transport drives: the consensus worker that takes decoded inputs, and the
// snapshot channel for sessions and bulk transfer. Any host with these two members works.
template <class Host>
concept PeerHost = requires(Host& host) {
	{ host.worker() } -> std::same_as<kronuz::consensus::Worker&>;
	{ host.channel() } -> std::same_as<kronuz::consensus::SnapshotChannel&>;
};
struct PeerOptions {
	reactor::tls::Options tls;
	asio::ip::tcp::endpoint listen;
	// Each peer by host name or IP literal; a dial resolves a name first (resolve.h).
	std::map<kronuz::consensus::NodeId, PeerAddress> addresses;
	std::map<kronuz::consensus::NodeId, Fingerprint> pins;
	std::size_t incoming_bytes = 16 * 1024 * 1024, outgoing_bytes = 16 * 1024 * 1024;
	std::size_t queued_messages = 4, handshakes = 2;
	// Appends carrying entries queue apart from control frames (Track C, D15, R19): at most
	// a pipeline of frames and a window of bytes, and every control frame goes first.
	std::size_t pipeline_frames = 4, window_bytes = 4 * 1024 * 1024;
	std::chrono::milliseconds deadline{5000};
	// A dial goes to the cached addresses at once while a lookup refreshes them; a name with
	// none waits up to resolve_deadline, which also spaces its lookups. A dial starts each
	// further address a stagger after the one before (RFC 8305).
	std::chrono::milliseconds resolve_deadline{1000}, stagger{250};
	PeerResolver resolver = system_resolver;
	// The fetch lane (fetch.h): `reader` serves this voter's names (none answers not-found). At
	// most `fetches` of a voter's fetches await each peer, and as many of the peer's wait here,
	// so voters agree on it; a reply holds at most fetch_bytes and arrives within fetch_deadline.
	FetchReader reader;
	std::size_t fetches = 4, fetch_bytes = 64 * 1024 * 1024;
	std::chrono::milliseconds fetch_deadline{30000};
};
struct PeerStats {
	std::uint64_t admitted = 0, rejected = 0, dropped = 0, reconnects = 0;
	std::uint64_t received = 0, accepted = 0, maximum_admission_wait_us = 0;
	// Lookups started, answered with no address, and uncached dials whose lookup was late.
	std::uint64_t lookups = 0, lookup_failures = 0, lookup_timeouts = 0;
	// Fetches answered (bytes or not-found) and failed, and replies served to peers.
	std::uint64_t fetched = 0, fetch_failures = 0, served = 0;
	kronuz::metrics::DurationHistogram<> decoded_to_admitted;
	AppendTimings append;
};
struct PeerOccupancy {
	std::size_t incoming_bytes = 0, outgoing_bytes = 0;
	std::size_t incoming_high_water = 0, outgoing_high_water = 0;
	std::size_t connections = 0, handshakes = 0, queued_control = 0, retained_inputs = 0, queued_data = 0;
	std::size_t lookups = 0;  // in flight: at most one per peer name
	std::size_t fetches = 0, serving = 0;  // this voter's awaiting replies, and peers' awaiting ours
};
// All calls and completions run on the authority owner executor. start()/stop()
// are admission boundaries; retain this object until drained() becomes true.
template <PeerHost Host>
class Peers : public std::enable_shared_from_this<Peers<Host>> {
  public:
	Peers(asio::any_io_executor executor, std::shared_ptr<Host> node, PeerConfiguration configuration,
		  PeerOptions options)
		: executor_(executor), node_(std::move(node)), configuration_(std::move(configuration)),
		  options_(std::move(options)),
		  context_(std::make_shared<asio::ssl::context>(asio::ssl::context::tls)),
		  incoming_(std::make_shared<ByteBudget>(options_.incoming_bytes)),
		  outgoing_(std::make_shared<ByteBudget>(options_.outgoing_bytes)), acceptor_(executor) {
		if (!node_ || !options_.queued_messages || !options_.handshakes || !options_.pipeline_frames || !options_.window_bytes ||
			options_.deadline <= std::chrono::milliseconds::zero() ||
			options_.resolve_deadline <= std::chrono::milliseconds::zero() ||
			options_.stagger <= std::chrono::milliseconds::zero() || !options_.fetches || !options_.fetch_bytes ||
			options_.fetch_deadline <= std::chrono::milliseconds::zero())
			throw std::invalid_argument("peer bounds");
		reactor::tls::configure(*context_, options_.tls, reactor::tls::Role::Peer);
		validate_peer_identity(*context_, configuration_.configuration, options_.pins);
		if (options_.addresses.size() + 1 != configuration_.configuration.voters.size())
			throw std::invalid_argument("peer address membership");
		for (auto voter : configuration_.configuration.voters) {
			if (voter == configuration_.configuration.local)
				continue;
			if (!options_.addresses.contains(voter) || !options_.addresses.at(voter).port ||
				options_.addresses.at(voter).host.empty())
				throw std::invalid_argument("missing peer address");
			auto &route = routes_[voter];
			const auto &address = options_.addresses.at(voter);
			asio::error_code error;
			const auto literal = asio::ip::make_address(address.host, error);
			route.literal = !error;
			if (route.literal)
				route.endpoints.emplace_back(literal, address.port);
			else if (!options_.resolver)
				throw std::invalid_argument("missing peer resolver");
		}
		prototype_.node = configuration_.configuration.local;
		prototype_.cluster = configuration_.cluster;
		prototype_.group = configuration_.group;
		prototype_.epoch = configuration_.epoch;
		prototype_.profile = configuration_.profile;
		acceptor_.open(options_.listen.protocol());
		// `::` serves IPv4 too, whatever the OS default (FreeBSD is v6-only).
		if (options_.listen.address() == asio::ip::address_v6::any())
			acceptor_.set_option(asio::ip::v6_only(false));
		acceptor_.set_option(asio::socket_base::reuse_address(true));
		acceptor_.bind(options_.listen);
		acceptor_.listen();
	}
	void start() {
		if (started_ || stopping_)
			throw std::logic_error("peer runtime already started");
		started_ = true;
		launch(accept());
		for (const auto &[peer, address] : options_.addresses) {
			if (prototype_.node < peer)
				for (auto lane : {Lane::Control, Lane::Bulk})
					launch(connect(peer, lane));
		}
	}
	void stop() noexcept {
		stopping_ = true;
		asio::error_code ignored;
		acceptor_.close(ignored);
		for (auto &[id, socket] : connecting_)
			socket->close(ignored);
		for (auto &connection : connections_)
			close(connection);
		for (auto &[id, timer] : retries_)
			timer->cancel();
		abandon_lookups();
	}
	~Peers() { abandon_lookups(); }
	bool drained() const noexcept { return tasks_ == 0; }
	const PeerStats &stats() const noexcept { return stats_; }
	PeerOccupancy occupancy() const noexcept {
		PeerOccupancy result;
		result.incoming_bytes = incoming_->used();
		result.outgoing_bytes = outgoing_->used();
		result.incoming_high_water = incoming_->high_water();
		result.outgoing_high_water = outgoing_->high_water();
		result.connections = connections_.size();
		for (const auto &[peer, route] : routes_)
			result.lookups += route.lookup != nullptr;
		for (const auto &connection : connections_) {
			result.handshakes += !connection->ready && !connection->closing;
			result.queued_control += connection->queue.size();
			result.queued_data += connection->data.size();
			result.retained_inputs += connection->input.has_value();
			result.fetches += connection->fetching.size();
			result.serving += connection->serving.size();
		}
		return result;
	}
	asio::ip::tcp::endpoint local_endpoint() const { return acceptor_.local_endpoint(); }
	void send(kronuz::consensus::Send action) {
		auto connection = find(action.peer, Lane::Control);
		auto request = std::get_if<kronuz::consensus::AppendRequest>(&action.message);
		const bool data = request && !request->entries.empty();
		if (stopping_ || !connection || (!data && connection->queue.size() >= options_.queued_messages) ||
			(data && connection->data.size() >= options_.pipeline_frames)) {
			++stats_.dropped;
			return;
		}
		try {
			ByteCredit credit(outgoing_, control_frame_bound(configuration_.consensus));
			auto bytes = encode(action.message, configuration_.consensus);
			std::optional<kronuz::consensus::AppendRequest> observation;
			if (request)
				observation.emplace(kronuz::consensus::AppendRequest{
					request->term, request->rpc, 0, 0, 0, request->read_probe, {}});
			if (data) {
				// The first queued append always fits; others only within the window.
				if (!connection->data.empty() && connection->data_bytes + bytes.size() > options_.window_bytes) {
					++stats_.dropped;
					return;
				}
				connection->data_bytes += bytes.size();
				connection->data.push_back({Frame(std::move(credit), std::move(bytes)), std::move(observation)});
			} else {
				connection->queue.push_back({Frame(std::move(credit), std::move(bytes)), std::move(observation)});
			}
			connection->wake.cancel();
		} catch (const std::runtime_error &) {
			++stats_.dropped;
		}
	}
	void pump() {
		if (stopping_)
			return;
		if (auto output = node_->channel().outbound()) {
			auto connection = find(output->session.peer, Lane::Bulk);
			if (connection && !connection->writing && connection->trusted == output->session)
				connection->wake.cancel();
		}
	}
	// Asks `peer` for `name` over the bulk lane. `done` runs once, on the executor and never
	// within this call; it fails at once without a bulk lane to the peer, or with `fetches` of
	// this voter's awaiting it.
	void fetch(kronuz::consensus::NodeId peer, std::string name, FetchDone done) {
		if (!done || name.empty() || name.size() > fetch_chunk_bytes)
			throw std::invalid_argument("invalid fetch");
		auto connection = stopping_ ? nullptr : find(peer, Lane::Bulk);
		if (!connection || connection->fetching.size() >= options_.fetches) {
			complete(std::move(done),
					 std::make_error_code(connection ? std::errc::resource_unavailable_try_again : std::errc::not_connected),
					 std::nullopt);
			return;
		}
		const auto id = ++fetch_id_;
		connection->requests.push_back(encode_fetch({FetchKind::Request, id, 0, name}));
		auto timer = std::make_shared<asio::steady_timer>(executor_, options_.fetch_deadline);
		connection->fetching.emplace(id, Fetching{std::move(done), timer});
		++tasks_;
		timer->async_wait([owner = this->shared_from_this(), weak = std::weak_ptr<Connection>(connection),
						   id](asio::error_code error) {
			--owner->tasks_;
			if (auto connection = weak.lock(); connection && !error)
				if (auto found = connection->fetching.find(id); found != connection->fetching.end())
					owner->fail(found->second, std::errc::timed_out);
		});
		connection->wake.cancel();
	}
	// One bounded owner pass before Node.service. Original readers retain their
	// frame and decoded credit until this scheduler admits their event.
	void admit() {
		if (stopping_ || connections_.empty())
			return;
		const auto now = std::chrono::steady_clock::now();
		for (const auto &connection : connections_)
			connection->append.expire(now, options_.deadline, stats_.append);
		const auto count = connections_.size();
		for (std::size_t offset = 0; offset != count; ++offset) {
			const auto index = (admission_cursor_ + offset) % count;
			auto &connection = connections_[index];
			if (connection->closing || !connection->input)
				continue;
			auto result = node_->worker().try_submit_retained(*connection->input);
			if (result == kronuz::consensus::SubmitResult::Busy)
				return;
			if (result == kronuz::consensus::SubmitResult::Pressure)
				continue;
			if (result == kronuz::consensus::SubmitResult::Fenced)
				close(connection);
			else {
				++stats_.accepted;
				const auto waited_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
										   std::chrono::steady_clock::now() - connection->received_at)
										   .count();
				stats_.decoded_to_admitted.observe(
					static_cast<std::uint64_t>(std::max<std::int64_t>(waited_ns, 0)));
				const auto waited = std::chrono::duration_cast<std::chrono::microseconds>(
										std::chrono::steady_clock::now() - connection->received_at)
										.count();
				stats_.maximum_admission_wait_us =
					std::max(stats_.maximum_admission_wait_us, static_cast<std::uint64_t>(waited));
			}
			connection->input.reset();
			connection->admission_wake.cancel();
			admission_cursor_ = (index + 1) % count;
			return;
		}
		admission_cursor_ = (admission_cursor_ + 1) % count;
	}

  private:
	struct ControlFrame {
		Frame frame;
		std::optional<kronuz::consensus::AppendRequest> observation;
	};
	// One of this voter's fetches awaiting its reply. A failed one (deadline, size) has no `done`
	// and absorbs the rest of its reply, so a peer never holds more than `fetches` of ours.
	struct Fetching {
		FetchDone done;
		std::shared_ptr<asio::steady_timer> timer;
		std::string bytes;
		std::uint64_t total = 0, received = 0;
		bool started = false;
	};
	// A peer's request, answered in order; the reader runs as its reply starts.
	struct Serving {
		std::uint64_t id = 0;
		std::string name, bytes;
		std::size_t offset = 0;
		std::optional<FetchKind> kind;
	};
	struct Connection {
		std::shared_ptr<Session> session;
		asio::steady_timer wake;
		asio::steady_timer admission_wake;
		std::optional<kronuz::consensus::Event> input;
		std::chrono::steady_clock::time_point received_at;
		std::deque<ControlFrame> queue;  // control frames: votes, heartbeats, responses
		std::deque<ControlFrame> data;   // appends carrying entries
		std::size_t data_bytes = 0;
		AppendObservation append;
		kronuz::consensus::NodeId peer = 0;
		Lane lane = Lane::Control;
		std::optional<kronuz::consensus::TrustedSession> trusted;
		std::map<std::uint64_t, Fetching> fetching;  // bulk: this voter's fetches, by id
		std::deque<std::string> requests;           // bulk: their encoded requests, not yet written
		std::deque<Serving> serving;                // bulk: the peer's fetches
		bool ready = false, closing = false, writing = false, incoming = false, notified = false;
		bool fetch_turn = false;  // bulk: fetch and snapshot frames alternate while both wait
		Connection(asio::any_io_executor executor, std::shared_ptr<Session> stream)
			: session(std::move(stream)), wake(executor), admission_wake(executor) {}
		void close() noexcept {
			closing = true;
			session->close();
			wake.cancel();
			admission_wake.cancel();
		}
	};
	void close(const std::shared_ptr<Connection> &connection) {
		connection->append.abandon(std::chrono::steady_clock::now(), stats_.append);
		connection->close();
		if (connection->trusted && !connection->notified) {
			connection->notified = true;
			node_->channel().session_closed(*connection->trusted);
		}
		for (auto &[id, fetching] : connection->fetching) {
			fetching.timer->cancel();
			fail(fetching, std::errc::connection_aborted);
		}
		connection->fetching.clear();
		connection->requests.clear();
		connection->serving.clear();
	}
	void complete(FetchDone done, std::error_code error, std::optional<std::string> bytes) {
		++(error ? stats_.fetch_failures : stats_.fetched);
		asio::post(executor_, [done = std::move(done), error, bytes = std::move(bytes)]() mutable {
			done(error, std::move(bytes));
		});
	}
	void fail(Fetching &fetching, std::errc error) {
		if (!fetching.done)
			return;
		complete(std::exchange(fetching.done, nullptr), std::make_error_code(error), std::nullopt);
		std::string().swap(fetching.bytes);
	}
	// A fetch frame from the peer: a request to serve, or the next frame of one of our replies.
	// A frame outside the protocol throws, which closes the bulk lane.
	void received(Connection &connection, const FetchFrame &frame) {
		if (frame.kind == FetchKind::Request) {
			if (connection.serving.size() >= options_.fetches)
				throw std::length_error("peer fetch bound");
			connection.serving.push_back({frame.id, std::string(frame.body)});
			connection.wake.cancel();
			return;
		}
		auto found = connection.fetching.find(frame.id);
		if (found == connection.fetching.end())
			throw std::invalid_argument("unrequested fetch reply");
		auto &fetching = found->second;
		if (frame.kind == FetchKind::Data) {
			if (!fetching.started) {
				fetching.started = true;
				fetching.total = frame.total;
				if (frame.total > options_.fetch_bytes)
					fail(fetching, std::errc::message_size);
				else if (fetching.done)
					fetching.bytes.reserve(frame.total);
			}
			if (frame.total != fetching.total || frame.body.size() > fetching.total - fetching.received)
				throw std::invalid_argument("fetch reply framing");
			fetching.received += frame.body.size();
			if (fetching.done)
				fetching.bytes.append(frame.body);
			if (fetching.received != fetching.total)
				return;
		} else if (fetching.started)
			throw std::invalid_argument("fetch reply framing");
		fetching.timer->cancel();
		if (fetching.done) {
			std::optional<std::string> bytes;
			if (frame.kind == FetchKind::Data)
				bytes = std::move(fetching.bytes);
			complete(std::move(fetching.done),
					 frame.kind == FetchKind::Failed ? std::make_error_code(std::errc::io_error) : std::error_code{},
					 std::move(bytes));
		}
		connection.fetching.erase(found);
	}
	// The bulk lane's next fetch frame: a request first, else the next frame of the oldest reply.
	std::string next_fetch(Connection &connection) {
		if (!connection.requests.empty()) {
			auto bytes = std::move(connection.requests.front());
			connection.requests.pop_front();
			return bytes;
		}
		auto &reply = connection.serving.front();
		if (!reply.kind)
			reply.kind = answer(reply);
		const auto chunk = std::min(fetch_chunk_bytes, reply.bytes.size() - reply.offset);
		auto bytes = encode_fetch({*reply.kind, reply.id, *reply.kind == FetchKind::Data ? reply.bytes.size() : 0,
								   std::string_view(reply.bytes).substr(reply.offset, chunk)});
		reply.offset += chunk;
		if (reply.offset == reply.bytes.size()) {
			connection.serving.pop_front();
			++stats_.served;
		}
		return bytes;
	}
	FetchKind answer(Serving &reply) {
		try {
			auto bytes = options_.reader ? options_.reader(reply.name) : std::nullopt;
			if (!bytes)
				return FetchKind::Missing;
			if (bytes->size() > options_.fetch_bytes)
				return FetchKind::Failed;
			reply.bytes = std::move(*bytes);
			return FetchKind::Data;
		} catch (const std::exception &) {
			return FetchKind::Failed;
		}
	}
	std::shared_ptr<Connection> find(kronuz::consensus::NodeId peer, Lane lane) const {
		for (auto &connection : connections_)
			if (connection->ready && !connection->closing && connection->peer == peer &&
				connection->lane == lane)
				return connection;
		return {};
	}
	void launch(asio::awaitable<void> operation) {
		++tasks_;
		auto owner = this->shared_from_this();
		try {
			asio::co_spawn(executor_, std::move(operation), [owner](std::exception_ptr error) {
				--owner->tasks_;
				if (error && !owner->stopping_)
					++owner->stats_.rejected;
			});
		} catch (...) {
			--tasks_;
			throw;
		}
	}
	asio::awaitable<void> pause(std::chrono::milliseconds duration) {
		if (stopping_)
			co_return;  // stop() cancels only the timers that exist when it runs
		auto timer = std::make_shared<asio::steady_timer>(executor_, duration);
		const auto id = ++retry_id_;
		retries_.emplace(id, timer);
		asio::error_code error;
		co_await timer->async_wait(asio::redirect_error(asio::use_awaitable, error));
		retries_.erase(id);
	}
	asio::awaitable<void> accept() {
		while (!stopping_) {
			asio::ip::tcp::socket socket(executor_);
			asio::error_code error;
			co_await acceptor_.async_accept(socket, asio::redirect_error(asio::use_awaitable, error));
			if (stopping_)
				co_return;
			if (error) {
				co_await pause(std::chrono::milliseconds(20));
				continue;
			}
			std::size_t pending = 0;
			for (auto &connection : connections_)
				if (connection->incoming && !connection->ready)
					++pending;
			if (pending >= options_.handshakes ||
				connections_.size() >= 2 * options_.addresses.size() + options_.handshakes) {
				++stats_.rejected;
				continue;
			}
			auto connection = make(std::move(socket), Lane::Control);
			connection->incoming = true;
			launch(incoming(connection));
		}
	}
	std::shared_ptr<Connection> make(asio::ip::tcp::socket socket, Lane lane) {
		// Incoming admission learns the lane from the hello before choosing its
		// payload policy. Session's common wire cap is narrowed after admission.
		auto stream = std::make_shared<Session>(std::move(socket), context_, incoming_,
												std::max(control_frame_bound(configuration_.consensus),
														 kronuz::consensus::maximum_transfer_bytes),
												options_.deadline);
		auto connection = std::make_shared<Connection>(executor_, std::move(stream));
		connection->lane = lane;
		connections_.push_back(connection);
		return connection;
	}
	asio::awaitable<void> connect(kronuz::consensus::NodeId peer, Lane lane) {
		while (!stopping_) {
			co_await resolve(peer);
			if (stopping_)
				co_return;
			const auto &route = routes_.at(peer);
			std::vector<asio::ip::tcp::endpoint> order(route.endpoints.size());
			std::rotate_copy(route.endpoints.begin(), route.endpoints.begin() + route.first, route.endpoints.end(),
							 order.begin());
			if (auto dialed = co_await dial(std::move(order))) {
				auto connection = make(std::move(dialed->socket), lane);
				co_await run(connection, false, peer);
				if (!connection->ready && !stopping_)
					skip_past(peer, dialed->endpoint);
			}
			++stats_.reconnects;
			co_await pause(std::chrono::milliseconds(
				100 + (peer * 17 + static_cast<unsigned>(lane) * 31 + stats_.reconnects * 13) % 100));
		}
	}
	// Starts a lookup unless one is in flight or the last started within resolve_deadline.
	// A cached name dials at once and the lookup refreshes the cache behind it; a name with
	// no cached address waits for the answer until the lookup's deadline.
	asio::awaitable<void> resolve(kronuz::consensus::NodeId peer) {
		auto &route = routes_.at(peer);
		if (route.literal)
			co_return;
		const auto now = std::chrono::steady_clock::now();
		if (!route.lookup && route.answer_by <= now) {
			++stats_.lookups;
			route.answer_by = now + options_.resolve_deadline;
			try {
				route.lookup = Lookup::start(
					options_.resolver, options_.addresses.at(peer).host,
					[executor = executor_, owner = this->weak_from_this(), peer](auto found) {
						asio::post(executor, [owner, peer, found = std::move(found)]() mutable {
							if (auto self = owner.lock())
								self->resolved(peer, std::move(found));
						});
					});
			} catch (const std::system_error &) {
				++stats_.lookup_failures;
				co_return;
			}
		}
		if (!route.endpoints.empty() || !route.lookup)
			co_return;
		if (route.answer_by <= now) {
			++stats_.lookup_timeouts;
			co_return;
		}
		auto timer = std::make_shared<asio::steady_timer>(executor_, route.answer_by);
		route.waiting.push_back(timer);
		asio::error_code error;
		co_await timer->async_wait(asio::redirect_error(asio::use_awaitable, error));
		std::erase(route.waiting, timer);
		if (!error)
			++stats_.lookup_timeouts;
	}
	// A lookup's answer, on the executor, however late: a changed set replaces the cached
	// addresses and dials from its first again; an empty one keeps them. Wakes waiting dials.
	void resolved(kronuz::consensus::NodeId peer, std::vector<asio::ip::address> found) {
		auto &route = routes_.at(peer);
		route.lookup.reset();
		if (found.empty())
			++stats_.lookup_failures;
		else if (auto endpoints = interleave(found, options_.addresses.at(peer).port); endpoints != route.endpoints) {
			route.endpoints = std::move(endpoints);
			route.first = 0;
		}
		for (auto &timer : route.waiting)
			timer->cancel();
	}
	// A dialed endpoint that failed authentication (another voter, a foreign service): the
	// peer's next dial starts after it, so an earlier wrong address cannot win every race.
	void skip_past(kronuz::consensus::NodeId peer, const asio::ip::tcp::endpoint &endpoint) {
		auto &route = routes_.at(peer);
		const auto found = std::find(route.endpoints.begin(), route.endpoints.end(), endpoint);
		if (found != route.endpoints.end())
			route.first = static_cast<std::size_t>(found - route.endpoints.begin() + 1) % route.endpoints.size();
	}
	void abandon_lookups() noexcept {
		for (auto &[peer, route] : routes_) {
			if (route.lookup)
				route.lookup->abandon();
			for (auto &timer : route.waiting)
				timer->cancel();
		}
	}
	struct Dialed {
		asio::ip::tcp::socket socket;
		asio::ip::tcp::endpoint endpoint;
	};
	// Dials the addresses in turn, starting the next a stagger after the one before or as
	// soon as every started attempt failed, all within one deadline; the first to connect wins.
	asio::awaitable<std::optional<Dialed>> dial(std::vector<asio::ip::tcp::endpoint> endpoints) {
		struct Race {
			explicit Race(asio::any_io_executor executor) : wake(executor) {}
			asio::steady_timer wake;
			std::vector<std::shared_ptr<asio::ip::tcp::socket>> sockets;
			std::optional<std::size_t> winner;
			std::size_t pending = 0;
		};
		auto race = std::make_shared<Race>(executor_);
		const auto now = std::chrono::steady_clock::now();
		const auto deadline = now + options_.deadline;
		auto next_start = now;
		while (!stopping_ && !race->winner && std::chrono::steady_clock::now() < deadline) {
			const auto started = race->sockets.size();
			if (started < endpoints.size() && (!race->pending || std::chrono::steady_clock::now() >= next_start)) {
				auto socket = std::make_shared<asio::ip::tcp::socket>(executor_);
				const auto attempt = ++retry_id_;
				connecting_.emplace(attempt, socket);
				race->sockets.push_back(socket);
				++race->pending;
				++tasks_;
				socket->async_connect(endpoints[started], [owner = this->shared_from_this(), race, started,
															 attempt](asio::error_code error) {
					--owner->tasks_;
					--race->pending;
					owner->connecting_.erase(attempt);
					if (!error && !race->winner)
						race->winner = started;
					race->wake.cancel();
				});
				next_start = std::chrono::steady_clock::now() + options_.stagger;
				continue;
			}
			if (!race->pending)
				break;
			race->wake.expires_at(started < endpoints.size() ? std::min(next_start, deadline) : deadline);
			asio::error_code error;
			co_await race->wake.async_wait(asio::redirect_error(asio::use_awaitable, error));
		}
		asio::error_code ignored;
		for (std::size_t i = 0; i != race->sockets.size(); ++i)
			if (i != race->winner)
				race->sockets[i]->close(ignored);
		if (stopping_ || !race->winner)
			co_return std::nullopt;
		co_return Dialed{std::move(*race->sockets[*race->winner]), endpoints[*race->winner]};
	}
	asio::awaitable<void> incoming(std::shared_ptr<Connection> connection) {
		co_await run(connection, true, 0);
	}
	asio::awaitable<void> run(std::shared_ptr<Connection> connection, bool server,
							  kronuz::consensus::NodeId expected) {
		try {
			auto local = prototype_;
			local.lane = connection->lane;
			local.connection = fresh_connection_identity();
			// A dial pins only the peer it dialed: an address that reaches another voter fails
			// at the handshake, before either end admits a hello.
			auto pins = server ? options_.pins
							   : std::map<kronuz::consensus::NodeId, Fingerprint>{
									 {local.node, options_.pins.at(local.node)}, {expected, options_.pins.at(expected)}};
			auto remote = co_await connection->session->authenticate(server, local, std::move(pins), server);
			if (stopping_ || (expected && remote.node != expected) || (server && remote.node >= local.node) ||
				find(remote.node, remote.lane))
				throw std::invalid_argument("peer dial ownership or duplicate lane");
			connection->peer = remote.node;
			connection->lane = remote.lane;
			connection->ready = true;
			connection->session->set_frame_bound(remote.lane == Lane::Bulk
													 ? kronuz::consensus::maximum_transfer_bytes
													 : control_frame_bound(configuration_.consensus));
			if (remote.lane == Lane::Bulk) {
				connection->trusted =
					kronuz::consensus::TrustedSession{remote.node, local.connection, remote.connection};
				node_->channel().register_session(*connection->trusted);
			}
			++stats_.admitted;
			using namespace asio::experimental::awaitable_operators;
			co_await (reader(connection) && writer(connection));
		} catch (...) {
			if (!stopping_)
				++stats_.rejected;
		}
		close(connection);
		std::erase(connections_, connection);
	}
	asio::awaitable<void> reader(std::shared_ptr<Connection> connection) {
		try {
			while (!stopping_ && !connection->closing) {
				auto frame = co_await connection->session->receive();
				if (stopping_ || connection->closing)
					throw std::runtime_error("peer admission closed");
				if (connection->lane == Lane::Control) {
					ByteCredit decoded(incoming_, control_frame_bound(configuration_.consensus) +
													  configuration_.consensus.rpc_entries *
														  sizeof(kronuz::consensus::Entry));
					connection->input.emplace(kronuz::consensus::Receive{
						connection->peer, decode(frame.bytes(), configuration_.consensus)});
					connection->received_at = std::chrono::steady_clock::now();
					const auto &message = std::get<kronuz::consensus::Receive>(*connection->input).message;
					if (auto response = std::get_if<kronuz::consensus::AppendResponse>(&message)) {
						connection->append.expire(connection->received_at, options_.deadline, stats_.append);
						connection->append.receive(*response, connection->received_at, stats_.append);
					}
					++stats_.received;
					while (connection->input) {
						if (stopping_ || connection->closing)
							throw std::runtime_error("peer admission closed");
						connection->admission_wake.expires_at(std::chrono::steady_clock::time_point::max());
						asio::error_code error;
						co_await connection->admission_wake.async_wait(
							asio::redirect_error(asio::use_awaitable, error));
					}
				} else if (is_fetch_frame(frame.bytes())) {
					received(*connection, decode_fetch(frame.bytes()));
				} else {
					if (frame.bytes().size() > kronuz::consensus::maximum_transfer_bytes)
						throw std::invalid_argument("bulk frame bound");
					std::span<const char> bytes(frame.bytes().data(), frame.bytes().size());
					bool busy = false;
					while (!bytes.empty() || busy) {
						if (stopping_ || connection->closing)
							throw std::runtime_error("bulk session closed");
						auto result = node_->channel().receive(*connection->trusted, bytes);
						bytes = bytes.subspan(result.consumed);
						if (result.result == kronuz::consensus::TransferReceive::Closed || stopping_ ||
							connection->closing)
							throw std::runtime_error("bulk session closed");
						busy = result.result == kronuz::consensus::TransferReceive::Busy;
						if (busy)
							co_await pause(std::chrono::milliseconds(1));
					}
				}
			}
		} catch (...) {
			close(connection);
			throw;
		}
	}
	asio::awaitable<void> writer(std::shared_ptr<Connection> connection) {
		try {
			while (!stopping_ && !connection->closing) {
				if (connection->lane == Lane::Control && (!connection->queue.empty() || !connection->data.empty())) {
					// Control first (R19): a data frame goes only when no control frame waits.
					const bool control = !connection->queue.empty();
					auto& from = control ? connection->queue : connection->data;
					auto frame = std::move(from.front());
					from.pop_front();
					if (!control) { connection->data_bytes -= frame.frame.bytes().size(); }
					auto generation =
						frame.observation
							? connection->append.emit(*frame.observation, std::chrono::steady_clock::now(),
													  stats_.append)
							: 0;
					try {
						co_await connection->session->send(std::move(frame.frame));
					} catch (...) {
						connection->append.write_failed(generation, std::chrono::steady_clock::now(),
														stats_.append);
						throw;
					}
					continue;
				}
				if (connection->lane == Lane::Bulk) {
					auto output = node_->channel().outbound();
					const bool snapshot = output && output->session == connection->trusted;
					if ((!connection->requests.empty() || !connection->serving.empty()) &&
						(!snapshot || connection->fetch_turn)) {
						// One frame's credit at a time, from the budget every lane shares.
						connection->fetch_turn = false;
						auto bytes = next_fetch(*connection);
						ByteCredit credit(outgoing_, bytes.size());
						co_await connection->session->send(Frame(std::move(credit), std::move(bytes)));
						continue;
					}
					if (snapshot) {
						connection->fetch_turn = true;
						SnapshotWrite flight(*output, outgoing_);
						connection->writing = true;
						co_await connection->session->send(flight.take_frame());
						connection->writing = false;
						flight.consume_if_current(node_->channel());
						continue;
					}
				}
				connection->wake.expires_at(std::chrono::steady_clock::time_point::max());
				asio::error_code error;
				co_await connection->wake.async_wait(asio::redirect_error(asio::use_awaitable, error));
			}
		} catch (...) {
			close(connection);
			throw;
		}
	}
	asio::any_io_executor executor_;
	std::shared_ptr<Host> node_;
	PeerConfiguration configuration_;
	PeerOptions options_;
	std::shared_ptr<asio::ssl::context> context_;
	std::shared_ptr<ByteBudget> incoming_, outgoing_;
	asio::ip::tcp::acceptor acceptor_;
	Hello prototype_;
	PeerStats stats_;
	std::vector<std::shared_ptr<Connection>> connections_;
	std::map<std::uint64_t, std::shared_ptr<asio::steady_timer>> retries_;
	std::map<std::uint64_t, std::shared_ptr<asio::ip::tcp::socket>> connecting_;
	// Per peer: its endpoints (the IP literal's, or the last lookup's that answered) and where
	// the next dial starts in them, and its lookup in flight with the dials waiting for it.
	struct Route {
		std::vector<asio::ip::tcp::endpoint> endpoints;
		std::size_t first = 0;
		bool literal = false;
		std::shared_ptr<Lookup> lookup;
		// The last lookup's deadline; no other lookup of the name starts before it.
		std::chrono::steady_clock::time_point answer_by;
		std::vector<std::shared_ptr<asio::steady_timer>> waiting;
	};
	std::map<kronuz::consensus::NodeId, Route> routes_;
	std::size_t tasks_ = 0;
	std::size_t admission_cursor_ = 0;
	std::uint64_t retry_id_ = 0, fetch_id_ = 0;
	bool started_ = false, stopping_ = false;
};
} // namespace kronuz::consensus::transport
