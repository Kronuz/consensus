#pragma once
#include "codec.h"
#include "hello.h"
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
	std::map<kronuz::consensus::NodeId, asio::ip::tcp::endpoint> addresses;
	std::map<kronuz::consensus::NodeId, Fingerprint> pins;
	std::size_t incoming_bytes = 16 * 1024 * 1024, outgoing_bytes = 16 * 1024 * 1024;
	std::size_t queued_messages = 4, handshakes = 2;
	// Appends carrying entries queue apart from control frames (Track C, D15, R19): at most
	// a pipeline of frames and a window of bytes, and every control frame goes first.
	std::size_t pipeline_frames = 4, window_bytes = 4 * 1024 * 1024;
	std::chrono::milliseconds deadline{5000};
};
struct PeerStats {
	std::uint64_t admitted = 0, rejected = 0, dropped = 0, reconnects = 0;
	std::uint64_t received = 0, accepted = 0, maximum_admission_wait_us = 0;
	kronuz::metrics::DurationHistogram<> decoded_to_admitted;
	AppendTimings append;
};
struct PeerOccupancy {
	std::size_t incoming_bytes = 0, outgoing_bytes = 0;
	std::size_t incoming_high_water = 0, outgoing_high_water = 0;
	std::size_t connections = 0, handshakes = 0, queued_control = 0, retained_inputs = 0, queued_data = 0;
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
			options_.deadline <= std::chrono::milliseconds::zero())
			throw std::invalid_argument("peer bounds");
		reactor::tls::configure(*context_, options_.tls, reactor::tls::Role::Peer);
		validate_peer_identity(*context_, configuration_.configuration, options_.pins);
		if (options_.addresses.size() + 1 != configuration_.configuration.voters.size())
			throw std::invalid_argument("peer address membership");
		for (auto voter : configuration_.configuration.voters) {
			if (voter == configuration_.configuration.local)
				continue;
			if (!options_.addresses.contains(voter) || !options_.addresses.at(voter).port())
				throw std::invalid_argument("missing peer address");
		}
		prototype_.node = configuration_.configuration.local;
		prototype_.cluster = configuration_.cluster;
		prototype_.group = configuration_.group;
		prototype_.epoch = configuration_.epoch;
		prototype_.profile = configuration_.profile;
		acceptor_.open(options_.listen.protocol());
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
					launch(connect(peer, lane, address));
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
	}
	bool drained() const noexcept { return tasks_ == 0; }
	const PeerStats &stats() const noexcept { return stats_; }
	PeerOccupancy occupancy() const noexcept {
		PeerOccupancy result;
		result.incoming_bytes = incoming_->used();
		result.outgoing_bytes = outgoing_->used();
		result.incoming_high_water = incoming_->high_water();
		result.outgoing_high_water = outgoing_->high_water();
		result.connections = connections_.size();
		for (const auto &connection : connections_) {
			result.handshakes += !connection->ready && !connection->closing;
			result.queued_control += connection->queue.size();
			result.queued_data += connection->data.size();
			result.retained_inputs += connection->input.has_value();
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
		bool ready = false, closing = false, writing = false, incoming = false, notified = false;
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
	asio::awaitable<void> connect(kronuz::consensus::NodeId peer, Lane lane,
								  asio::ip::tcp::endpoint address) {
		while (!stopping_) {
			auto socket = std::make_shared<asio::ip::tcp::socket>(executor_);
			const auto attempt = ++retry_id_;
			connecting_.emplace(attempt, socket);
			asio::steady_timer timer(executor_, options_.deadline);
			asio::error_code error, timeout_error;
			using namespace asio::experimental::awaitable_operators;
			auto result =
				co_await (socket->async_connect(address, asio::redirect_error(asio::use_awaitable, error)) ||
						  timer.async_wait(asio::redirect_error(asio::use_awaitable, timeout_error)));
			connecting_.erase(attempt);
			if (stopping_)
				co_return;
			if (result.index() == 0 && !error) {
				auto connection = make(std::move(*socket), lane);
				co_await run(connection, false, peer);
			}
			++stats_.reconnects;
			co_await pause(std::chrono::milliseconds(
				100 + (peer * 17 + static_cast<unsigned>(lane) * 31 + stats_.reconnects * 13) % 100));
		}
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
			auto remote = co_await connection->session->authenticate(server, local, options_.pins, server);
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
				if (connection->lane == Lane::Bulk)
					if (auto output = node_->channel().outbound();
						output && output->session == connection->trusted) {
						SnapshotWrite flight(*output, outgoing_);
						connection->writing = true;
						co_await connection->session->send(flight.take_frame());
						connection->writing = false;
						flight.consume_if_current(node_->channel());
						continue;
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
	std::size_t tasks_ = 0;
	std::size_t admission_cursor_ = 0;
	std::uint64_t retry_id_ = 0;
	bool started_ = false, stopping_ = false;
};
} // namespace kronuz::consensus::transport
