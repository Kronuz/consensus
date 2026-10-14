#pragma once
#include "tls.h"
#include <algorithm>
#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <chrono>
#include <memory>

namespace kronuz::consensus::transport {
// These objects belong to the authority executor. A frame owns its credit until
// the consumer has finished with its bytes, including original socket writes.
class ByteBudget {
  public:
	explicit ByteBudget(std::size_t limit) : limit_(limit) {}
	std::size_t used() const noexcept { return used_; }
	std::size_t high_water() const noexcept { return high_water_; }

  private:
	friend class Frame;
	friend class ByteCredit;
	bool acquire(std::size_t size) {
		if (size > limit_ - used_)
			return false;
		used_ += size;
		high_water_ = std::max(high_water_, used_);
		return true;
	}
	void release(std::size_t size) noexcept { used_ -= size; }

  private:
	std::size_t limit_, used_ = 0, high_water_ = 0;
};
class ByteCredit {
  public:
	ByteCredit(std::shared_ptr<ByteBudget> budget, std::size_t size) : budget_(std::move(budget)) {
		if (!budget_ || !budget_->acquire(size))
			throw std::runtime_error("peer byte admission");
		charge_ = size;
	}
	ByteCredit(ByteCredit &&other) noexcept
		: budget_(std::move(other.budget_)), charge_(std::exchange(other.charge_, 0)) {}
	ByteCredit(const ByteCredit &) = delete;
	~ByteCredit() {
		if (charge_)
			budget_->release(charge_);
	}

  private:
	friend class Frame;
	std::shared_ptr<ByteBudget> budget_;
	std::size_t charge_ = 0;
};
class Frame {
  public:
	Frame(ByteCredit &&credit, std::string bytes) : budget_(credit.budget_), bytes_(std::move(bytes)) {
		if (bytes_.size() > credit.charge_)
			throw std::invalid_argument("frame credit mismatch");
		budget_->release(credit.charge_ - bytes_.size());
		charge_ = bytes_.size();
		credit.charge_ = 0;
	}
	Frame(std::shared_ptr<ByteBudget> budget, std::size_t size) : budget_(std::move(budget)) {
		if (!budget_ || !budget_->acquire(size))
			throw std::runtime_error("peer byte admission");
		charge_ = size;
		try {
			bytes_.resize(size);
		} catch (...) {
			release();
			throw;
		}
	}
	Frame(Frame &&other) noexcept
		: budget_(std::move(other.budget_)), charge_(std::exchange(other.charge_, 0)),
		  bytes_(std::move(other.bytes_)) {}
	Frame &operator=(Frame &&) = delete;
	Frame(const Frame &) = delete;
	~Frame() { release(); }
	const std::string &bytes() const noexcept { return bytes_; }
	char *data() noexcept { return bytes_.data(); }
	void assign(std::string_view bytes) {
		if (bytes.size() != bytes_.size())
			throw std::invalid_argument("frame credit mismatch");
		std::copy(bytes.begin(), bytes.end(), bytes_.begin());
	}

  private:
	void release() noexcept {
		if (charge_) {
			budget_->release(charge_);
			charge_ = 0;
		}
	}
	std::shared_ptr<ByteBudget> budget_;
	std::size_t charge_ = 0;
	std::string bytes_;
};

template <class Socket> class BasicSession {
  public:
	using Stream = asio::ssl::stream<Socket>;
	BasicSession(Socket socket, std::shared_ptr<asio::ssl::context> context,
				 std::shared_ptr<ByteBudget> incoming, std::size_t maximum_frame,
				 std::chrono::milliseconds timeout)
		: context_(std::move(context)), stream_(std::move(socket), checked_context(context_)),
		  incoming_(std::move(incoming)), maximum_frame_(maximum_frame), timeout_(timeout) {
		if (!incoming_ || !maximum_frame_ || maximum_frame_ > UINT32_MAX ||
			timeout_ <= std::chrono::milliseconds::zero())
			throw std::invalid_argument("invalid peer session bounds");
	}
	asio::awaitable<Hello> authenticate(bool server, Hello local,
										std::map<kronuz::consensus::NodeId, Fingerprint> voters,
										bool infer_lane = false) {
		if (ready_ || closed_)
			throw std::logic_error("peer authentication state");
		OperationGuard guard(authenticating_);
		AuthenticationGuard failure{*this};
		kronuz::consensus::FixedConfiguration membership;
		membership.local = local.node;
		for (const auto &[node, pin] : voters)
			membership.voters.push_back(node);
		validate_peer_identity(*context_, membership, voters);
		const auto deadline = std::chrono::steady_clock::now() + timeout_;
		co_await timed(handshake(server), deadline);
		auto node = authenticated_voter(stream_.native_handle(), voters);
		std::array<char, hello_size> bytes{};
		if (!server) {
			auto outgoing = encode_hello(local);
			co_await timed(write(asio::buffer(outgoing)), deadline);
		}
		co_await timed(read(asio::buffer(bytes)), deadline);
		auto remote = decode_hello({bytes.data(), bytes.size()});
		if (server && infer_lane)
			local.lane = remote.lane;
		validate_hello(remote, local, node, local.lane);
		if (server) {
			auto outgoing = encode_hello(local);
			co_await timed(write(asio::buffer(outgoing)), deadline);
		}
		ready_ = true;
		co_return remote;
	}
	void set_frame_bound(std::size_t bound) {
		check_ready();
		if (!bound || bound > maximum_frame_ || reading_ || writing_)
			throw std::logic_error("peer frame policy");
		maximum_frame_ = bound;
	}
	asio::awaitable<Frame> receive() {
		check_ready();
		OperationGuard guard(reading_);
		std::array<unsigned char, 4> header{};
		// An idle bulk lane is healthy. Start one absolute frame deadline when
		// its first byte arrives; trickled header/body bytes never renew it.
		if (auto error = co_await read(asio::buffer(header.data(), 1))) {
			close();
			throw asio::system_error(error);
		}
		const auto deadline = std::chrono::steady_clock::now() + timeout_;
		co_await timed(read(asio::buffer(header.data() + 1, header.size() - 1)), deadline);
		std::uint64_t size = 0;
		for (unsigned i = 0; i != 4; ++i)
			size |= std::uint64_t(header[i]) << (i * 8);
		if (!size || size > maximum_frame_) {
			close();
			throw std::invalid_argument("peer frame bound");
		}
		Frame frame(incoming_, static_cast<std::size_t>(size));
		co_await timed(read(asio::buffer(frame.data(), frame.bytes().size())), deadline);
		co_return std::move(frame);
	}
	// Take the frame by value so its original write retains both bytes and credit.
	asio::awaitable<void> send(Frame frame) {
		check_ready();
		OperationGuard guard(writing_);
		const auto size = frame.bytes().size();
		if (!size || size > maximum_frame_ || size > UINT32_MAX)
			throw std::invalid_argument("peer frame bound");
		std::array<unsigned char, 4> header{};
		for (unsigned i = 0; i != 4; ++i)
			header[i] = static_cast<unsigned char>(size >> (i * 8));
		std::array<asio::const_buffer, 2> buffers{asio::buffer(header), asio::buffer(frame.bytes())};
		co_await timed(write(buffers));
	}
	void close() noexcept {
		closed_ = true;
		asio::error_code ignored;
		stream_.lowest_layer().close(ignored);
	}

  private:
	struct OperationGuard {
		bool &active;
		explicit OperationGuard(bool &flag) : active(flag) {
			if (active)
				throw std::logic_error("concurrent peer operation");
			active = true;
		}
		~OperationGuard() { active = false; }
	};
	struct AuthenticationGuard {
		BasicSession &session;
		~AuthenticationGuard() {
			if (!session.ready_)
				session.close();
		}
	};
	void check_ready() const {
		if (!ready_ || closed_)
			throw std::logic_error("unauthenticated peer operation");
	}
	static asio::ssl::context &checked_context(const std::shared_ptr<asio::ssl::context> &context) {
		if (!context)
			throw std::invalid_argument("missing TLS context");
		return *context;
	}
	asio::awaitable<asio::error_code> handshake(bool server) {
		asio::error_code error;
		co_await stream_.async_handshake(server ? Stream::server : Stream::client,
										 asio::redirect_error(asio::use_awaitable, error));
		co_return error;
	}
	template <class Buffers> asio::awaitable<asio::error_code> read(Buffers buffers) {
		asio::error_code error;
		co_await asio::async_read(stream_, buffers, asio::redirect_error(asio::use_awaitable, error));
		co_return error;
	}
	template <class Buffers> asio::awaitable<asio::error_code> write(Buffers buffers) {
		asio::error_code error;
		co_await asio::async_write(stream_, buffers, asio::redirect_error(asio::use_awaitable, error));
		co_return error;
	}
	asio::awaitable<void>
	timed(asio::awaitable<asio::error_code> operation,
		  std::chrono::steady_clock::time_point deadline = std::chrono::steady_clock::time_point::max()) {
		using namespace asio::experimental::awaitable_operators;
		asio::steady_timer timer(co_await asio::this_coro::executor);
		timer.expires_at(deadline == std::chrono::steady_clock::time_point::max()
							 ? std::chrono::steady_clock::now() + timeout_
							 : deadline);
		asio::error_code ignored;
		// || cancels and reaps the losing original operation before returning.
		// Errors are values so an I/O failure wins immediately, rather than waiting
		// for the deadline because the combinator considers exceptions failures.
		auto result = co_await (std::move(operation) ||
								timer.async_wait(asio::redirect_error(asio::use_awaitable, ignored)));
		if (result.index() != 0) {
			close();
			throw asio::system_error(ignored ? ignored : asio::error::timed_out);
		}
		if (auto error = std::get<0>(result)) {
			close();
			throw asio::system_error(error);
		}
	}
	std::shared_ptr<asio::ssl::context> context_;
	Stream stream_;
	std::shared_ptr<ByteBudget> incoming_;
	std::size_t maximum_frame_;
	std::chrono::milliseconds timeout_;
	bool ready_ = false, closed_ = false, authenticating_ = false, reading_ = false, writing_ = false;
};
using Session = BasicSession<asio::ip::tcp::socket>;
} // namespace kronuz::consensus::transport
